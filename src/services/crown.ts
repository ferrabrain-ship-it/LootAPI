import { formatEther, getAddress, type Address } from 'viem'
import crownAbi from '../abis/Crown.json' with { type: 'json' }
import autoCrownAbi from '../abis/AutoCrown.json' with { type: 'json' }
import { CONTRACTS } from '../config/contracts.js'
import { hasProtocolIndexDatabase, getProtocolIndexPool } from '../lib/protocolIndexDb.js'
import { publicClient } from '../lib/client.js'

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000' as Address
const CROWN_CACHE_TTL_MS = 2_500
const CROWN_HISTORY_CACHE_TTL_MS = 10_000
const MAX_LIVE_HOLDER_SCAN = 250

type CurrentRoundInfo = readonly [
  bigint,
  boolean,
  boolean,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  Address,
  bigint,
  bigint,
]

type RoundStorageInfo = readonly [
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  Address,
  Address,
  Address,
  boolean,
  boolean,
  boolean,
]

type HolderInfo = readonly [bigint, bigint, bigint]

type AutoCrownConfigInfo = readonly [
  boolean,
  boolean,
  boolean,
  boolean,
  number,
  number,
  number,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
]

type AutoCrownStateInfo = readonly [
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
]

type CrownRoundRow = {
  round_id: string
  start_time: string
  end_time: string
  next_roll_at: string
  total_sold: string
  prize_pool: string
  holder_count: string
  winning_roll: string
  current_leader: string
  leader_snapshot: string
  winner: string
  active: boolean
  settled: boolean
  vrf_pending: boolean
  settled_block_number: string | null
  settled_tx_hash: string | null
  settled_at: Date | string | null
}

type CrownHolderRow = {
  user_address: string
  chests: string
  spent: string
}

type CrownActivityRow = {
  tx_hash: string
  log_index: number
  round_id: string
  user_address: string
  price: string
  total_sold_after: string
  block_number: string
  block_timestamp: Date | string | null
}

type CrownStatsRow = {
  past_winners: string
  prize_paid: string
  chests_sold: string
  buyback_fees: string
  lock_fees: string
  admin_fees: string
  dividend_fees: string
}

type CrownBurnRow = {
  loot_burned: string
}

const cache = new Map<string, { expiresAt: number; value: unknown }>()
const inflight = new Map<string, Promise<unknown>>()

function toBigInt(value: string | number | bigint | null | undefined) {
  if (value == null) return 0n
  if (typeof value === 'bigint') return value
  return BigInt(String(value))
}

function toNumber(value: string | number | bigint | null | undefined) {
  const parsed = Number(value ?? 0)
  return Number.isFinite(parsed) ? parsed : 0
}

function toIsoString(value: Date | string | null | undefined) {
  if (!value) return null
  if (value instanceof Date) return value.toISOString()
  const parsed = new Date(value)
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null
}

function eth(value: bigint) {
  return formatEther(value)
}

function ethFixed(value: bigint, digits = 6) {
  return Number(formatEther(value)).toFixed(digits)
}

function clampLimit(limit: number, fallback = 10, max = 100) {
  return Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), max)) : fallback
}

function safeAddress(value: string | null | undefined): Address {
  if (!value || value === ZERO_ADDRESS) return ZERO_ADDRESS
  return getAddress(value)
}

async function withCache<T>(key: string, ttlMs: number, loader: () => Promise<T>): Promise<T> {
  const now = Date.now()
  const cached = cache.get(key)
  if (cached && cached.expiresAt > now) return cached.value as T

  const existing = inflight.get(key)
  if (existing) return existing as Promise<T>

  const promise = loader()
    .then((value) => {
      cache.set(key, { expiresAt: Date.now() + ttlMs, value })
      inflight.delete(key)
      return value
    })
    .catch((error) => {
      inflight.delete(key)
      throw error
    })

  inflight.set(key, promise)
  return promise
}

async function readCurrentRoundInfo() {
  return publicClient.readContract({
    address: CONTRACTS.crown,
    abi: crownAbi,
    functionName: 'getCurrentRoundInfo',
  }) as Promise<CurrentRoundInfo>
}

async function readRoundStorage(roundId: bigint) {
  return publicClient.readContract({
    address: CONTRACTS.crown,
    abi: crownAbi,
    functionName: 'rounds',
    args: [roundId],
  }) as Promise<RoundStorageInfo>
}

async function readHolderInfo(roundId: bigint, user: Address) {
  return publicClient.readContract({
    address: CONTRACTS.crown,
    abi: crownAbi,
    functionName: 'getHolderInfo',
    args: [roundId, user],
  }) as Promise<HolderInfo>
}

async function readClaimablePrize(user: Address) {
  return publicClient.readContract({
    address: CONTRACTS.crown,
    abi: crownAbi,
    functionName: 'claimablePrize',
    args: [user],
  }) as Promise<bigint>
}

async function readAutoCrownConfig(user: Address) {
  return publicClient.readContract({
    address: CONTRACTS.autoCrown,
    abi: autoCrownAbi,
    functionName: 'configs',
    args: [user],
  }) as Promise<AutoCrownConfigInfo>
}

async function readAutoCrownState(user: Address) {
  return publicClient.readContract({
    address: CONTRACTS.autoCrown,
    abi: autoCrownAbi,
    functionName: 'states',
    args: [user],
  }) as Promise<AutoCrownStateInfo>
}

async function readAutoCrownCanExecute(user: Address) {
  return publicClient.readContract({
    address: CONTRACTS.autoCrown,
    abi: autoCrownAbi,
    functionName: 'canExecute',
    args: [user],
  }) as Promise<boolean>
}

async function readCrownVaults() {
  const [buybackVault, lockRewardsVault, adminVault] = await Promise.all([
    publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'buybackVault' }) as Promise<Address>,
    publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'lockRewardsVault' }) as Promise<Address>,
    publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'adminVault' }) as Promise<Address>,
  ])

  return {
    buybackVault: getAddress(buybackVault),
    lockRewardsVault: getAddress(lockRewardsVault),
    adminVault: getAddress(adminVault),
  }
}

function mapRoundStorage(roundId: bigint, round: RoundStorageInfo) {
  return {
    roundId: roundId.toString(),
    startTime: Number(round[0]),
    endTime: Number(round[1]),
    nextRollAt: Number(round[2]),
    totalSold: round[3].toString(),
    prizePool: round[4].toString(),
    prizePoolFormatted: ethFixed(round[4], 6),
    holderCount: round[6].toString(),
    winningRoll: round[9].toString(),
    currentLeader: round[10],
    leaderSnapshot: round[11],
    winner: round[12],
    active: round[13],
    settled: round[14],
    vrfPending: round[15],
  }
}

function mapRoundRow(row: CrownRoundRow) {
  const prizePool = toBigInt(row.prize_pool)
  return {
    roundId: row.round_id,
    startTime: toNumber(row.start_time),
    endTime: toNumber(row.end_time),
    nextRollAt: toNumber(row.next_roll_at),
    totalSold: row.total_sold,
    prizePool: prizePool.toString(),
    prizePoolFormatted: ethFixed(prizePool, 6),
    holderCount: row.holder_count,
    winningRoll: row.winning_roll,
    currentLeader: safeAddress(row.current_leader),
    leaderSnapshot: safeAddress(row.leader_snapshot),
    winner: safeAddress(row.winner),
    active: row.active,
    settled: row.settled,
    vrfPending: row.vrf_pending,
    settledBlockNumber: row.settled_block_number,
    settledTxHash: row.settled_tx_hash,
    settledAt: toIsoString(row.settled_at),
  }
}

async function queryIndexedRound(roundId: bigint) {
  if (!hasProtocolIndexDatabase()) return null
  const pool = getProtocolIndexPool()
  const result = await pool.query<CrownRoundRow>(
    `
      select *
      from crown_rounds
      where round_id = $1
      limit 1
    `,
    [roundId.toString()]
  )
  return result.rows[0] ? mapRoundRow(result.rows[0]) : null
}

async function queryIndexedRounds(page: number, limit: number) {
  if (!hasProtocolIndexDatabase()) return null
  const safePage = Number.isFinite(page) ? Math.max(1, Math.floor(page)) : 1
  const safeLimit = clampLimit(limit, 10, 50)
  const offset = (safePage - 1) * safeLimit
  const pool = getProtocolIndexPool()
  const result = await pool.query<CrownRoundRow>(
    `
      select *
      from crown_rounds
      order by round_id desc
      limit $1 offset $2
    `,
    [safeLimit, offset]
  )
  if (result.rows.length === 0) return null
  return {
    page: safePage,
    limit: safeLimit,
    source: 'indexed' as const,
    rounds: result.rows.map(mapRoundRow),
  }
}

async function queryIndexedHolders(roundId: bigint, limit: number) {
  if (!hasProtocolIndexDatabase()) return null
  const safeLimit = clampLimit(limit, 10, 100)
  const pool = getProtocolIndexPool()
  const result = await pool.query<CrownHolderRow>(
    `
      select user_address, count(*)::text as chests, coalesce(sum(price), 0)::text as spent
      from crown_purchases
      where round_id = $1
      group by user_address
      order by count(*) desc, coalesce(sum(price), 0) desc
      limit $2
    `,
    [roundId.toString(), safeLimit]
  )

  if (result.rows.length === 0) return null

  const holders = await Promise.all(result.rows.map(async (row, index) => {
    const address = getAddress(row.user_address)
    const holderInfo = await readHolderInfo(roundId, address)
    return {
      rank: index + 1,
      address,
      chests: Number(holderInfo[0] || toBigInt(row.chests)),
      pendingDividends: holderInfo[2].toString(),
      pendingDividendsFormatted: ethFixed(holderInfo[2], 6),
      spent: row.spent,
      spentFormatted: ethFixed(toBigInt(row.spent), 6),
    }
  }))

  return {
    roundId: roundId.toString(),
    limit: safeLimit,
    source: 'indexed' as const,
    holders,
  }
}

async function getLiveHolders(roundId: bigint, limit: number) {
  const safeLimit = clampLimit(limit, 10, 100)
  const round = await readRoundStorage(roundId)
  const holderCount = Number(round[6])
  const scanCount = Math.min(holderCount, MAX_LIVE_HOLDER_SCAN)

  const addresses = await Promise.all(Array.from({ length: scanCount }, (_, index) => (
    publicClient.readContract({
      address: CONTRACTS.crown,
      abi: crownAbi,
      functionName: 'getHolderAt',
      args: [roundId, BigInt(index)],
    }) as Promise<Address>
  )))

  const rows = await Promise.all(addresses
    .filter((address) => address !== ZERO_ADDRESS)
    .map(async (address) => {
      const holderInfo = await readHolderInfo(roundId, address)
      return {
        rank: 0,
        address,
        chests: Number(holderInfo[0]),
        pendingDividends: holderInfo[2].toString(),
        pendingDividendsFormatted: ethFixed(holderInfo[2], 6),
        spent: '0',
        spentFormatted: '0.000000',
      }
    }))

  return {
    roundId: roundId.toString(),
    limit: safeLimit,
    source: 'live' as const,
    holderCount,
    scanned: scanCount,
    holders: rows
      .filter((row) => row.chests > 0)
      .sort((a, b) => {
        if (b.chests !== a.chests) return b.chests - a.chests
        return Number.parseFloat(b.pendingDividendsFormatted) - Number.parseFloat(a.pendingDividendsFormatted)
      })
      .slice(0, safeLimit)
      .map((row, index) => ({ ...row, rank: index + 1 })),
  }
}

export async function getCrownHolders(roundIdInput?: string | number | bigint, limit = 10) {
  const roundId = roundIdInput === undefined
    ? (await readCurrentRoundInfo())[0]
    : toBigInt(roundIdInput)

  return withCache(`crown:holders:${roundId}:${limit}`, CROWN_CACHE_TTL_MS, async () => (
    await queryIndexedHolders(roundId, limit) ?? await getLiveHolders(roundId, limit)
  ))
}

export async function getCrownCurrent(user?: string) {
  const userAddress = user ? getAddress(user) : null
  return withCache(`crown:current:${userAddress ?? 'global'}`, CROWN_CACHE_TTL_MS, async () => {
    const [
      info,
      gameStarted,
      canRequestRoll,
      basePrice,
      priceStep,
      maxPrice,
    ] = await Promise.all([
      readCurrentRoundInfo(),
      publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'gameStarted' }) as Promise<boolean>,
      publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'canRequestRoll' }) as Promise<boolean>,
      publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'basePrice' }) as Promise<bigint>,
      publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'priceStep' }) as Promise<bigint>,
      publicClient.readContract({ address: CONTRACTS.crown, abi: crownAbi, functionName: 'maxPrice' }) as Promise<bigint>,
    ])

    const topHolders = info[0] > 0n ? await getCrownHolders(info[0], 10) : null
    const userInfo = userAddress && info[0] > 0n
      ? await getCrownUser(userAddress)
      : null

    return {
      game: 'Crown',
      contracts: {
        crown: CONTRACTS.crown,
        autoCrown: CONTRACTS.autoCrown,
      },
      roundId: info[0].toString(),
      active: info[1],
      vrfPending: info[2],
      startTime: Number(info[3]),
      nextRollAt: Number(info[4]),
      totalSold: info[5].toString(),
      prizePool: info[6].toString(),
      prizePoolFormatted: ethFixed(info[6], 6),
      currentPrice: info[7].toString(),
      currentPriceFormatted: ethFixed(info[7], 6),
      currentLeader: info[8],
      leaderChests: info[9].toString(),
      timeRemaining: Number(info[10]),
      gameStarted,
      canRequestRoll,
      pricing: {
        basePrice: basePrice.toString(),
        basePriceFormatted: ethFixed(basePrice, 6),
        priceStep: priceStep.toString(),
        priceStepFormatted: ethFixed(priceStep, 6),
        maxPrice: maxPrice.toString(),
        maxPriceFormatted: ethFixed(maxPrice, 6),
      },
      topHolders,
      user: userInfo,
    }
  })
}

export async function getCrownRound(roundIdInput: string | number | bigint) {
  const roundId = toBigInt(roundIdInput)
  return withCache(`crown:round:${roundId}`, CROWN_HISTORY_CACHE_TTL_MS, async () => {
    const indexed = await queryIndexedRound(roundId)
    if (indexed) return { source: 'indexed' as const, ...indexed }

    const round = await readRoundStorage(roundId)
    return {
      source: 'live' as const,
      ...mapRoundStorage(roundId, round),
    }
  })
}

export async function getCrownRounds(page = 1, limit = 10) {
  return withCache(`crown:rounds:${page}:${limit}`, CROWN_HISTORY_CACHE_TTL_MS, async () => {
    const indexed = await queryIndexedRounds(page, limit)
    if (indexed) return indexed

    const current = await readCurrentRoundInfo()
    const safeLimit = clampLimit(limit, 10, 50)
    const start = current[0]
    const ids: bigint[] = []
    for (let offset = 0n; offset < BigInt(safeLimit) && start > offset; offset += 1n) {
      ids.push(start - offset)
    }

    const rounds = await Promise.all(ids.map((id) => getCrownRound(id)))
    return {
      page: 1,
      limit: safeLimit,
      source: 'live' as const,
      rounds,
    }
  })
}

export async function getCrownActivity(roundIdInput?: string | number | bigint, limit = 20) {
  if (!hasProtocolIndexDatabase()) {
    return { source: 'unavailable' as const, activity: [] }
  }

  const safeLimit = clampLimit(limit, 20, 100)
  const roundId = roundIdInput === undefined ? null : toBigInt(roundIdInput)
  const pool = getProtocolIndexPool()
  const result = await pool.query<CrownActivityRow>(
    roundId
      ? `
          select *
          from crown_purchases
          where round_id = $1
          order by block_number desc, log_index desc
          limit $2
        `
      : `
          select *
          from crown_purchases
          order by block_number desc, log_index desc
          limit $1
        `,
    roundId ? [roundId.toString(), safeLimit] : [safeLimit]
  )

  return {
    source: 'indexed' as const,
    roundId: roundId?.toString() ?? null,
    limit: safeLimit,
    activity: result.rows.map((row) => ({
      txHash: row.tx_hash,
      logIndex: row.log_index,
      roundId: row.round_id,
      user: getAddress(row.user_address),
      price: row.price,
      priceFormatted: ethFixed(toBigInt(row.price), 6),
      totalSoldAfter: row.total_sold_after,
      blockNumber: row.block_number,
      timestamp: toIsoString(row.block_timestamp),
    })),
  }
}

export async function getCrownStats(limit = 10) {
  const safeLimit = clampLimit(limit, 10, 50)

  return withCache(`crown:stats:${safeLimit}`, CROWN_HISTORY_CACHE_TTL_MS, async () => {
    const vaults = await readCrownVaults()

    const emptyTotals = {
      pastWinners: 0,
      chestsSold: '0',
      prizePaid: '0',
      prizePaidFormatted: ethFixed(0n, 6),
      buybackFees: '0',
      buybackFeesFormatted: ethFixed(0n, 6),
      lockFees: '0',
      lockFeesFormatted: ethFixed(0n, 6),
      adminFees: '0',
      adminFeesFormatted: ethFixed(0n, 6),
      dividendFees: '0',
      dividendFeesFormatted: ethFixed(0n, 6),
      lootBurnedFromBuybackVault: '0',
      lootBurnedFromBuybackVaultFormatted: ethFixed(0n, 4),
    }

    if (!hasProtocolIndexDatabase()) {
      return {
        source: 'live_unindexed' as const,
        indexReady: false,
        vaults,
        totals: emptyTotals,
        recentWinners: [],
      }
    }

    try {
      const pool = getProtocolIndexPool()
      const [statsResult, burnsResult, winnersResult] = await Promise.all([
        pool.query<CrownStatsRow>(
          `
            with round_totals as (
              select
                count(*)::text as past_winners,
                coalesce(sum(prize_pool), 0)::text as prize_paid
              from crown_rounds
              where settled = true
                and lower(winner) <> lower($1)
            ),
            fee_totals as (
              select
                count(*)::text as chests_sold,
                coalesce(sum(buyback_amount), 0)::text as buyback_fees,
                coalesce(sum(lock_amount), 0)::text as lock_fees,
                coalesce(sum(admin_amount), 0)::text as admin_fees,
                coalesce(sum(dividend_amount), 0)::text as dividend_fees
              from crown_purchases
            )
            select *
            from round_totals, fee_totals
          `,
          [ZERO_ADDRESS]
        ),
        pool.query<CrownBurnRow>(
          `
            select coalesce(sum(value), 0)::text as loot_burned
            from protocol_direct_burns
            where lower(from_address) = lower($1)
          `,
          [vaults.buybackVault]
        ),
        pool.query<CrownRoundRow>(
          `
            select *
            from crown_rounds
            where settled = true
              and lower(winner) <> lower($1)
            order by round_id desc
            limit $2
          `,
          [ZERO_ADDRESS, safeLimit]
        ),
      ])

      const row = statsResult.rows[0]
      const lootBurned = toBigInt(burnsResult.rows[0]?.loot_burned)
      const prizePaid = toBigInt(row?.prize_paid)
      const buybackFees = toBigInt(row?.buyback_fees)
      const lockFees = toBigInt(row?.lock_fees)
      const adminFees = toBigInt(row?.admin_fees)
      const dividendFees = toBigInt(row?.dividend_fees)

      return {
        source: 'indexed' as const,
        indexReady: true,
        vaults,
        totals: {
          pastWinners: toNumber(row?.past_winners),
          chestsSold: row?.chests_sold ?? '0',
          prizePaid: prizePaid.toString(),
          prizePaidFormatted: ethFixed(prizePaid, 6),
          buybackFees: buybackFees.toString(),
          buybackFeesFormatted: ethFixed(buybackFees, 6),
          lockFees: lockFees.toString(),
          lockFeesFormatted: ethFixed(lockFees, 6),
          adminFees: adminFees.toString(),
          adminFeesFormatted: ethFixed(adminFees, 6),
          dividendFees: dividendFees.toString(),
          dividendFeesFormatted: ethFixed(dividendFees, 6),
          lootBurnedFromBuybackVault: lootBurned.toString(),
          lootBurnedFromBuybackVaultFormatted: ethFixed(lootBurned, 4),
        },
        recentWinners: winnersResult.rows.map(mapRoundRow),
      }
    } catch (error) {
      console.warn('[crown] indexed stats unavailable', error)
      return {
        source: 'live_unindexed' as const,
        indexReady: false,
        vaults,
        totals: emptyTotals,
        recentWinners: [],
      }
    }
  })
}

export async function getCrownUser(addressInput: string) {
  const user = getAddress(addressInput)
  return withCache(`crown:user:${user}`, CROWN_CACHE_TTL_MS, async () => {
    const current = await readCurrentRoundInfo()
    const [holderInfo, claimablePrize, autoConfig, autoState, autoCanExecute] = await Promise.all([
      current[0] > 0n ? readHolderInfo(current[0], user) : Promise.resolve([0n, 0n, 0n] as HolderInfo),
      readClaimablePrize(user),
      readAutoCrownConfig(user).catch(() => null),
      readAutoCrownState(user).catch(() => null),
      readAutoCrownCanExecute(user).catch(() => false),
    ])

    return {
      address: user,
      currentRoundId: current[0].toString(),
      chests: holderInfo[0].toString(),
      realizedDividends: holderInfo[1].toString(),
      pendingDividends: holderInfo[2].toString(),
      pendingDividendsFormatted: ethFixed(holderInfo[2], 6),
      claimablePrize: claimablePrize.toString(),
      claimablePrizeFormatted: ethFixed(claimablePrize, 6),
      autoCrown: autoConfig && autoState ? {
        active: autoConfig[0],
        openNewRound: autoConfig[1],
        defendLead: autoConfig[2],
        snipeWhenOutbid: autoConfig[3],
        buyWindowSeconds: autoConfig[4],
        maxBuysPerTick: autoConfig[5],
        maxBuysPerRound: autoConfig[6],
        maxBuildPrice: autoConfig[7].toString(),
        maxBuildPriceFormatted: ethFixed(autoConfig[7], 6),
        maxBattlePrice: autoConfig[8].toString(),
        maxBattlePriceFormatted: ethFixed(autoConfig[8], 6),
        minPrizePool: autoConfig[9].toString(),
        targetChests: autoConfig[10].toString(),
        maxRoundSpend: autoConfig[11].toString(),
        totalBudget: autoConfig[12].toString(),
        canExecute: autoCanExecute,
        state: {
          lastRoundId: autoState[0].toString(),
          lastTickMarker: autoState[1].toString(),
          buysThisTick: autoState[2].toString(),
          buysThisRound: autoState[3].toString(),
          spentThisRound: autoState[4].toString(),
          totalSpent: autoState[5].toString(),
          totalFeesPaid: autoState[6].toString(),
          depositBalance: autoState[7].toString(),
          depositBalanceFormatted: ethFixed(autoState[7], 6),
        },
      } : null,
    }
  })
}

export async function getCrownSkillContext(addressInput?: string) {
  const current = await getCrownCurrent(addressInput)
  const recentRounds = await getCrownRounds(1, 8).catch(() => null)
  const activity = await getCrownActivity(undefined, 12).catch(() => null)

  return {
    generatedAt: new Date().toISOString(),
    game: 'Crown',
    objective: 'Buy chests to become the current leader. If the executor roll hits 1000, the current leader wins the ETH prize pool.',
    contracts: {
      crown: CONTRACTS.crown,
      autoCrown: CONTRACTS.autoCrown,
    },
    mechanics: {
      rollIntervalSeconds: 60,
      rollRange: '1-1000',
      winningRoll: 1000,
      chestFeeSplit: {
        prizePoolBps: 7000,
        holderDividendsBps: 1500,
        lootBuybackBps: 800,
        lockRewardsBps: 400,
        adminBps: 300,
      },
      note: 'Do not route gameplay transactions through the API. The API is read-only context; users and agents submit transactions directly to Crown or AutoCrown.',
    },
    current,
    recentRounds,
    recentActivity: activity,
  }
}
