import type { PoolClient } from 'pg'
import { formatEther, getAddress, parseEther, type Address } from 'viem'
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'
import { getAgentStatsPool } from '../lib/agentStatsDb.js'
import { decodeBlockMask, safeAddressEq, toBigInt } from '../lib/format.js'
import { getAllDeploymentLogs, getRound, getRoundMiners, type DeploymentLog } from './protocol.js'

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>

interface StoredAgentWalletStatsRow {
  wallet_address: string
  rounds_played: number
  wins: number
  losses: number
  total_deployed_eth: string
  total_rewards_eth: string
  loot_earned: string
  loot_value_eth: string
  eth_pnl: string
  true_pnl_eth: string
  best_round_eth: string
  worst_round_eth: string
  average_bet_eth: string
  win_rate: string
  net_roi: string
  total_rounds_won_eth: string
  total_loot_value_eth: string
  last_active_at: Date | string | null
  last_processed_round: string
  updated_at: Date | string
}

interface StoredAgentRecentRoundRow {
  wallet_address: string
  round_id: string
  block_number: number
  blocks_covered: number
  deployed_eth: string
  rewards_eth: string
  loot_earned: string
  loot_value_eth: string
  pnl_eth: string
  true_pnl_eth: string
  pnl_pct: string
  outcome: string
  mode: string
  round_timestamp: Date | string | null
}

interface EnrichedAgentRound {
  roundId: number
  blockNumber: number
  blocksCovered: number
  deployedEth: number
  rewardsEth: number
  lootEarned: number
  lootValueEth: number
  pnlEth: number
  truePnlEth: number
  pnlPct: number
  outcome: 'Win' | 'Miss' | 'Lootpot'
  mode: string
  roundTimestamp: string | null
  totalAmountWei: bigint
  rewardsEthWei: bigint
  lootEarnedWei: bigint
  wonWinningBlock: boolean
}

export interface AgentStatsApiRound {
  roundId: number
  block: number
  blocksCovered: number
  deployedEth: number
  rewardsEth: number
  lootEarned: number
  lootValueEth: number
  pnlEth: number
  truePnlEth: number
  pnlPct: number
  outcome: 'Win' | 'Miss' | 'Probe' | 'Lootpot'
  mode: string
  minutesAgo: number
}

export interface AgentStatsApiResponse {
  walletAddress: string
  status: 'syncing' | 'ready' | 'error'
  updatedAt: string | null
  dataWindowLabel: string
  roundsPlayed: number
  wins: number
  losses: number
  winRate: number
  netRoi: number
  totalDeployedEth: number
  totalRewardsEth: number
  lootEarned: number
  lootValueEth: number
  pnlEth: number
  truePnlEth: number
  bestRoundEth: number
  worstRoundEth: number
  averageBetEth: number
  lastActiveMinutes: number
  recentRounds: AgentStatsApiRound[]
}

const AGENT_STATS_STALE_MS = 90_000
const AGENT_STATS_RECENT_DEFAULT = 12
const AGENT_STATS_RECENT_STORE_LIMIT = 80
const AGENT_STATS_RECENT_MAX_LIMIT = 80
const AGENT_STATS_SYNC_CONCURRENCY = 6

declare global {
  // eslint-disable-next-line no-var
  var __mineLootAgentStatsInflight__: Map<string, Promise<void>> | undefined
}

function getInflightMap() {
  if (!globalThis.__mineLootAgentStatsInflight__) {
    globalThis.__mineLootAgentStatsInflight__ = new Map()
  }

  return globalThis.__mineLootAgentStatsInflight__
}

function numericToNumber(value: string | number | null | undefined) {
  const parsed = typeof value === 'number' ? value : Number.parseFloat(value ?? '')
  return Number.isFinite(parsed) ? parsed : 0
}

function toIsoString(value: Date | string | null | undefined) {
  if (!value) return null
  if (value instanceof Date) return value.toISOString()
  const parsed = new Date(value)
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null
}

function relativeMinutes(timestamp: string | null) {
  if (!timestamp) return 0
  const diffMs = Date.now() - new Date(timestamp).getTime()
  if (!Number.isFinite(diffMs) || diffMs <= 0) return 0
  return Math.floor(diffMs / 60_000)
}

function parseNumericToWei(value: string | number | null | undefined) {
  const normalized = typeof value === 'number' ? value.toString() : String(value ?? '0')
  return parseEther(normalized)
}

function getTrackedWallets() {
  return env.agentStatsWallets
    .split(',')
    .map((wallet) => wallet.trim())
    .filter(Boolean)
    .map((wallet) => getAddress(wallet))
}

async function mapWithConcurrency<T, R>(items: T[], concurrency: number, mapper: (item: T) => Promise<R>) {
  if (items.length === 0) return [] as R[]

  const safeConcurrency = Math.max(1, Math.min(concurrency, items.length))
  const results = new Array<R>(items.length)
  let nextIndex = 0

  async function worker() {
    while (true) {
      const index = nextIndex
      nextIndex += 1
      if (index >= items.length) return
      results[index] = await mapper(items[index])
    }
  }

  await Promise.all(Array.from({ length: safeConcurrency }, () => worker()))
  return results
}

async function getLootPriceNativeEth() {
  const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${CONTRACTS.loot}`, {
    cache: 'no-store',
  })

  if (!response.ok) {
    throw new Error(`DexScreener ${response.status}`)
  }

  const data = await response.json()
  const pairs: Array<{ priceNative?: string; liquidity?: { usd?: number } }> = data.pairs ?? []
  const best = pairs.sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0))[0]
  return numericToNumber(best?.priceNative)
}

async function enrichDeploymentLog(
  walletAddress: Address,
  log: DeploymentLog,
  lootPriceNativeEth: number,
  roundCache: Map<bigint, Promise<Awaited<ReturnType<typeof getRound>>>>,
  minerCache: Map<bigint, Promise<Awaited<ReturnType<typeof getRoundMiners>>>>
): Promise<EnrichedAgentRound> {
  const roundId = toBigInt(log.args.roundId)
  const loadRound = () => {
    const cached = roundCache.get(roundId)
    if (cached) return cached
    const promise = getRound(roundId)
    roundCache.set(roundId, promise)
    return promise
  }
  const loadMiners = () => {
    const cached = minerCache.get(roundId)
    if (cached) return cached
    const promise = getRoundMiners(roundId)
    minerCache.set(roundId, promise)
    return promise
  }

  const [round, miners] = await Promise.all([loadRound(), loadMiners()])
  const blockMask = toBigInt(log.args.blockMask)
  const selectedBlocks = decodeBlockMask(blockMask)
  const totalAmountWei = toBigInt(log.args.totalAmount)
  const userMiner = miners.miners.find((miner) => safeAddressEq(miner.address, walletAddress))
  const rewardsEthWei = userMiner ? BigInt(userMiner.ethReward) : 0n
  const lootEarnedWei = userMiner ? BigInt(userMiner.lootReward) : 0n
  const deployedEth = Number(formatEther(totalAmountWei))
  const rewardsEth = Number(formatEther(rewardsEthWei))
  const lootEarned = Number(formatEther(lootEarnedWei))
  const lootValueEth = lootEarned * lootPriceNativeEth
  const pnlEth = rewardsEth - deployedEth
  const truePnlEth = pnlEth + lootValueEth
  const wonWinningBlock = selectedBlocks.includes(round.winningBlock)
  const outcome = Number(round.lootpotAmount) > 0 ? 'Lootpot' : wonWinningBlock ? 'Win' : 'Miss'

  return {
    roundId: Number(roundId),
    blockNumber: round.winningBlock + 1,
    blocksCovered: selectedBlocks.length,
    deployedEth,
    rewardsEth,
    lootEarned,
    lootValueEth,
    pnlEth,
    truePnlEth,
    pnlPct: deployedEth > 0 ? (truePnlEth / deployedEth) * 100 : 0,
    outcome,
    mode: outcome === 'Lootpot' ? 'Lootpot hit' : wonWinningBlock ? 'Winning entry' : 'No win',
    roundTimestamp: round.settledAt ?? null,
    totalAmountWei,
    rewardsEthWei,
    lootEarnedWei,
    wonWinningBlock,
  }
}

async function loadStatsRow(client: PoolClient, walletAddress: Address) {
  const result = await client.query<StoredAgentWalletStatsRow>(
    'select * from agent_wallet_stats where wallet_address = $1',
    [walletAddress.toLowerCase()]
  )

  return result.rows[0] ?? null
}

async function loadRecentRows(client: PoolClient, walletAddress: Address, limit = AGENT_STATS_RECENT_STORE_LIMIT) {
  const result = await client.query<StoredAgentRecentRoundRow>(
    `
      select *
      from agent_recent_rounds
      where wallet_address = $1
      order by round_id desc
      limit $2
    `,
    [walletAddress.toLowerCase(), limit]
  )

  return result.rows
}

function mapRecentRow(row: StoredAgentRecentRoundRow): AgentStatsApiRound {
  const roundTimestamp = toIsoString(row.round_timestamp)

  return {
    roundId: Number(row.round_id),
    block: row.block_number,
    blocksCovered: row.blocks_covered,
    deployedEth: numericToNumber(row.deployed_eth),
    rewardsEth: numericToNumber(row.rewards_eth),
    lootEarned: numericToNumber(row.loot_earned),
    lootValueEth: numericToNumber(row.loot_value_eth),
    pnlEth: numericToNumber(row.pnl_eth),
    truePnlEth: numericToNumber(row.true_pnl_eth),
    pnlPct: numericToNumber(row.pnl_pct),
    outcome: row.outcome as AgentStatsApiRound['outcome'],
    mode: row.mode,
    minutesAgo: relativeMinutes(roundTimestamp),
  }
}

function buildApiResponse(
  walletAddress: Address,
  row: StoredAgentWalletStatsRow | null,
  recentRows: StoredAgentRecentRoundRow[]
): AgentStatsApiResponse | null {
  if (!row) {
    return null
  }

  const updatedAt = toIsoString(row.updated_at)
  const lastActiveAt = toIsoString(row.last_active_at)

  return {
    walletAddress: walletAddress.toLowerCase(),
    status: 'ready',
    updatedAt,
    dataWindowLabel: `Tracked ${row.rounds_played} rounds`,
    roundsPlayed: row.rounds_played,
    wins: row.wins,
    losses: row.losses,
    winRate: numericToNumber(row.win_rate),
    netRoi: numericToNumber(row.net_roi),
    totalDeployedEth: numericToNumber(row.total_deployed_eth),
    totalRewardsEth: numericToNumber(row.total_rewards_eth),
    lootEarned: numericToNumber(row.loot_earned),
    lootValueEth: numericToNumber(row.loot_value_eth),
    pnlEth: numericToNumber(row.eth_pnl),
    truePnlEth: numericToNumber(row.true_pnl_eth),
    bestRoundEth: numericToNumber(row.best_round_eth),
    worstRoundEth: numericToNumber(row.worst_round_eth),
    averageBetEth: numericToNumber(row.average_bet_eth),
    lastActiveMinutes: relativeMinutes(lastActiveAt),
    recentRounds: recentRows.map(mapRecentRow),
  }
}

async function refreshCurrentValueFields(client: PoolClient, walletAddress: Address, lootPriceNativeEth: number) {
  const price = lootPriceNativeEth.toString()

  await client.query(
    `
      update agent_wallet_stats
      set
        loot_value_eth = loot_earned * $2::numeric,
        true_pnl_eth = eth_pnl + (loot_earned * $2::numeric),
        net_roi = case
          when total_deployed_eth > 0 then ((eth_pnl + (loot_earned * $2::numeric)) / total_deployed_eth) * 100
          else 0
        end,
        total_loot_value_eth = loot_earned * $2::numeric,
        updated_at = now()
      where wallet_address = $1
    `,
    [walletAddress.toLowerCase(), price]
  )

  await client.query(
    `
      update agent_recent_rounds
      set
        loot_value_eth = loot_earned * $2::numeric,
        true_pnl_eth = pnl_eth + (loot_earned * $2::numeric),
        pnl_pct = case
          when deployed_eth > 0 then ((pnl_eth + (loot_earned * $2::numeric)) / deployed_eth) * 100
          else 0
        end
      where wallet_address = $1
    `,
    [walletAddress.toLowerCase(), price]
  )
}

async function replaceRecentRows(client: PoolClient, walletAddress: Address, rounds: EnrichedAgentRound[]) {
  await client.query('delete from agent_recent_rounds where wallet_address = $1', [walletAddress.toLowerCase()])

  if (rounds.length === 0) {
    return
  }

  const values: unknown[] = []
  const placeholders = rounds.map((round, index) => {
    const offset = index * 14
    values.push(
      walletAddress.toLowerCase(),
      round.roundId,
      round.blockNumber,
      round.blocksCovered,
      round.deployedEth.toString(),
      round.rewardsEth.toString(),
      round.lootEarned.toString(),
      round.lootValueEth.toString(),
      round.pnlEth.toString(),
      round.truePnlEth.toString(),
      round.pnlPct.toString(),
      round.outcome,
      round.mode,
      round.roundTimestamp
    )

    return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14})`
  })

  await client.query(
    `
      insert into agent_recent_rounds (
        wallet_address,
        round_id,
        block_number,
        blocks_covered,
        deployed_eth,
        rewards_eth,
        loot_earned,
        loot_value_eth,
        pnl_eth,
        true_pnl_eth,
        pnl_pct,
        outcome,
        mode,
        round_timestamp
      )
      values ${placeholders.join(', ')}
    `,
    values
  )
}

async function syncAgentWalletStats(walletAddress: Address, logger: Logger = console) {
  const address = getAddress(walletAddress)
  const pool = getAgentStatsPool()
  const client = await pool.connect()

  try {
    const [allLogs, lootPriceNativeEth] = await Promise.all([
      getAllDeploymentLogs(),
      getLootPriceNativeEth(),
    ])

    const walletLogs = allLogs.filter((log) => safeAddressEq(log.args.user, address))
    const existing = await loadStatsRow(client, address)

    if (walletLogs.length === 0) {
      await client.query(
        `
          insert into agent_wallet_stats (
            wallet_address,
            updated_at
          ) values ($1, now())
          on conflict (wallet_address) do update
          set updated_at = now()
        `,
        [address.toLowerCase()]
      )
      await client.query('delete from agent_recent_rounds where wallet_address = $1', [address.toLowerCase()])
      return
    }

    const lastProcessedRound = existing ? BigInt(existing.last_processed_round) : 0n
    const newLogs = existing
      ? walletLogs.filter((log) => toBigInt(log.args.roundId) > lastProcessedRound)
      : walletLogs

    if (newLogs.length === 0 && existing) {
      await refreshCurrentValueFields(client, address, lootPriceNativeEth)
      return
    }

    const roundCache = new Map<bigint, Promise<Awaited<ReturnType<typeof getRound>>>>()
    const minerCache = new Map<bigint, Promise<Awaited<ReturnType<typeof getRoundMiners>>>>()
    const enrichedNew = await mapWithConcurrency(newLogs, AGENT_STATS_SYNC_CONCURRENCY, (log) =>
      enrichDeploymentLog(address, log, lootPriceNativeEth, roundCache, minerCache)
    )

    const existingTotalDeployedWei = existing ? parseNumericToWei(existing.total_deployed_eth) : 0n
    const existingTotalRewardsWei = existing ? parseNumericToWei(existing.total_rewards_eth) : 0n
    const existingLootEarnedWei = existing ? parseNumericToWei(existing.loot_earned) : 0n
    const nextRoundsPlayed = (existing?.rounds_played ?? 0) + enrichedNew.length
    const nextWins = (existing?.wins ?? 0) + enrichedNew.filter((entry) => entry.wonWinningBlock).length
    const nextLosses = Math.max(nextRoundsPlayed - nextWins, 0)
    const nextTotalDeployedWei = existingTotalDeployedWei + enrichedNew.reduce((sum, entry) => sum + entry.totalAmountWei, 0n)
    const nextTotalRewardsWei = existingTotalRewardsWei + enrichedNew.reduce((sum, entry) => sum + entry.rewardsEthWei, 0n)
    const nextLootEarnedWei = existingLootEarnedWei + enrichedNew.reduce((sum, entry) => sum + entry.lootEarnedWei, 0n)
    const nextTotalDeployedEth = Number(formatEther(nextTotalDeployedWei))
    const nextTotalRewardsEth = Number(formatEther(nextTotalRewardsWei))
    const nextLootEarned = Number(formatEther(nextLootEarnedWei))
    const nextEthPnl = nextTotalRewardsEth - nextTotalDeployedEth
    const nextLootValueEth = nextLootEarned * lootPriceNativeEth
    const nextTruePnlEth = nextEthPnl + nextLootValueEth
    const nextAverageBetEth = nextRoundsPlayed > 0 ? nextTotalDeployedEth / nextRoundsPlayed : 0
    const nextWinRate = nextRoundsPlayed > 0 ? (nextWins / nextRoundsPlayed) * 100 : 0
    const nextNetRoi = nextTotalDeployedEth > 0 ? (nextTruePnlEth / nextTotalDeployedEth) * 100 : 0
    const nextBestRoundEth = Math.max(
      existing ? numericToNumber(existing.best_round_eth) : Number.NEGATIVE_INFINITY,
      ...enrichedNew.map((entry) => entry.truePnlEth)
    )
    const nextWorstRoundEth = Math.min(
      existing ? numericToNumber(existing.worst_round_eth) : Number.POSITIVE_INFINITY,
      ...enrichedNew.map((entry) => entry.truePnlEth)
    )
    const lastRound = walletLogs[walletLogs.length - 1]
    const nextLastProcessedRound = Number(toBigInt(lastRound.args.roundId))
    const latestActivity = enrichedNew.at(-1)?.roundTimestamp
      ?? toIsoString(existing?.last_active_at)

    await client.query(
      `
        insert into agent_wallet_stats (
          wallet_address,
          rounds_played,
          wins,
          losses,
          total_deployed_eth,
          total_rewards_eth,
          loot_earned,
          loot_value_eth,
          eth_pnl,
          true_pnl_eth,
          best_round_eth,
          worst_round_eth,
          average_bet_eth,
          win_rate,
          net_roi,
          total_rounds_won_eth,
          total_loot_value_eth,
          last_active_at,
          last_processed_round,
          updated_at
        ) values (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14, $15, $16, $17, $18, $19, now()
        )
        on conflict (wallet_address) do update set
          rounds_played = excluded.rounds_played,
          wins = excluded.wins,
          losses = excluded.losses,
          total_deployed_eth = excluded.total_deployed_eth,
          total_rewards_eth = excluded.total_rewards_eth,
          loot_earned = excluded.loot_earned,
          loot_value_eth = excluded.loot_value_eth,
          eth_pnl = excluded.eth_pnl,
          true_pnl_eth = excluded.true_pnl_eth,
          best_round_eth = excluded.best_round_eth,
          worst_round_eth = excluded.worst_round_eth,
          average_bet_eth = excluded.average_bet_eth,
          win_rate = excluded.win_rate,
          net_roi = excluded.net_roi,
          total_rounds_won_eth = excluded.total_rounds_won_eth,
          total_loot_value_eth = excluded.total_loot_value_eth,
          last_active_at = excluded.last_active_at,
          last_processed_round = excluded.last_processed_round,
          updated_at = now()
      `,
      [
        address.toLowerCase(),
        nextRoundsPlayed,
        nextWins,
        nextLosses,
        nextTotalDeployedEth.toString(),
        nextTotalRewardsEth.toString(),
        nextLootEarned.toString(),
        nextLootValueEth.toString(),
        nextEthPnl.toString(),
        nextTruePnlEth.toString(),
        Number.isFinite(nextBestRoundEth) ? nextBestRoundEth.toString() : '0',
        Number.isFinite(nextWorstRoundEth) ? nextWorstRoundEth.toString() : '0',
        nextAverageBetEth.toString(),
        nextWinRate.toString(),
        nextNetRoi.toString(),
        nextTotalRewardsEth.toString(),
        nextLootValueEth.toString(),
        latestActivity,
        nextLastProcessedRound,
      ]
    )

    const existingRecentRows = await loadRecentRows(client, address, AGENT_STATS_RECENT_STORE_LIMIT)
    const existingRecentMap = new Map<number, EnrichedAgentRound>(
      existingRecentRows.map((row) => {
        const lootEarned = numericToNumber(row.loot_earned)
        const lootValueEth = lootEarned * lootPriceNativeEth
        const deployedEth = numericToNumber(row.deployed_eth)
        const pnlEth = numericToNumber(row.pnl_eth)
        const truePnlEth = pnlEth + lootValueEth
        return [
          Number(row.round_id),
          {
            roundId: Number(row.round_id),
            blockNumber: row.block_number,
            blocksCovered: row.blocks_covered,
            deployedEth,
            rewardsEth: numericToNumber(row.rewards_eth),
            lootEarned,
            lootValueEth,
            pnlEth,
            truePnlEth,
            pnlPct: deployedEth > 0 ? (truePnlEth / deployedEth) * 100 : 0,
            outcome: row.outcome as EnrichedAgentRound['outcome'],
            mode: row.mode,
            roundTimestamp: toIsoString(row.round_timestamp),
            totalAmountWei: 0n,
            rewardsEthWei: 0n,
            lootEarnedWei: 0n,
            wonWinningBlock: row.outcome === 'Win' || row.outcome === 'Lootpot',
          },
        ]
      })
    )

    for (const entry of enrichedNew) {
      existingRecentMap.set(entry.roundId, entry)
    }

    const nextRecent = [...existingRecentMap.values()]
      .sort((left, right) => right.roundId - left.roundId)
      .slice(0, AGENT_STATS_RECENT_STORE_LIMIT)

    await replaceRecentRows(client, address, nextRecent)
    logger.info(`[agent-stats] synced ${address} (${enrichedNew.length} new rounds)`)
  } finally {
    client.release()
  }
}

function triggerAgentWalletSync(walletAddress: Address, logger: Logger = console) {
  const inflight = getInflightMap()
  const key = walletAddress.toLowerCase()
  const existing = inflight.get(key)
  if (existing) {
    return existing
  }

  const promise = syncAgentWalletStats(walletAddress, logger)
    .catch((error) => {
      logger.error(`[agent-stats] sync failed for ${walletAddress}`, error)
      throw error
    })
    .finally(() => {
      inflight.delete(key)
    })

  inflight.set(key, promise)
  return promise
}

export async function getAgentWalletStats(walletAddress: Address, recentLimit = AGENT_STATS_RECENT_DEFAULT): Promise<AgentStatsApiResponse> {
  const address = getAddress(walletAddress)
  const safeRecentLimit = Math.max(1, Math.min(Math.floor(recentLimit), AGENT_STATS_RECENT_MAX_LIMIT))
  const pool = getAgentStatsPool()
  const client = await pool.connect()

  try {
    const statsRow = await loadStatsRow(client, address)
    const recentRows = await loadRecentRows(client, address, safeRecentLimit)
    const snapshot = buildApiResponse(address, statsRow, recentRows)

    if (!snapshot) {
      void triggerAgentWalletSync(address)
      return {
        walletAddress: address.toLowerCase(),
        status: 'syncing',
        updatedAt: null,
        dataWindowLabel: 'Tracked history',
        roundsPlayed: 0,
        wins: 0,
        losses: 0,
        winRate: 0,
        netRoi: 0,
        totalDeployedEth: 0,
        totalRewardsEth: 0,
        lootEarned: 0,
        lootValueEth: 0,
        pnlEth: 0,
        truePnlEth: 0,
        bestRoundEth: 0,
        worstRoundEth: 0,
        averageBetEth: 0,
        lastActiveMinutes: 0,
        recentRounds: [],
      }
    }

    if (!snapshot.updatedAt || Date.now() - new Date(snapshot.updatedAt).getTime() > AGENT_STATS_STALE_MS) {
      void triggerAgentWalletSync(address)
    }

    return snapshot
  } finally {
    client.release()
  }
}

export async function runAgentStatsSyncOnce(options?: { wallets?: Address[]; logger?: Logger }) {
  const logger = options?.logger ?? console
  const wallets = options?.wallets?.length ? options.wallets : getTrackedWallets()

  if (wallets.length === 0) {
    logger.warn('[agent-stats] no wallets configured in AGENT_STATS_WALLETS')
    return { ok: true, synced: 0 }
  }

  await mapWithConcurrency(wallets, 2, async (wallet) => {
    await syncAgentWalletStats(wallet, logger)
  })

  return {
    ok: true,
    synced: wallets.length,
  }
}
