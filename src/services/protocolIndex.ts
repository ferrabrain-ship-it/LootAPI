import type { PoolClient } from 'pg'
import { formatEther, formatUnits, getAddress, type Address } from 'viem'
import gridMiningAbi from '../abis/GridMining.json' with { type: 'json' }
import { CONTRACTS, PROTOCOL_CONSTANTS } from '../config/contracts.js'
import { hasProtocolIndexDatabase, getProtocolIndexPool } from '../lib/protocolIndexDb.js'
import { decodeBlockMask, etherString, relativeTime, safeAddressEq } from '../lib/format.js'
import { publicClient } from '../lib/client.js'

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
const ONE_ADDRESS = '0x0000000000000000000000000000000000000001'
const STAKING_APR_WINDOW_DAYS = 7
const INDEX_ROUNDS_MAX_LAG_BLOCKS = 900n
const INDEX_TREASURY_MAX_LAG_BLOCKS = 1200n
const INDEX_TREASURY_AGENT_MAX_LAG_BLOCKS = 1200n
const INDEX_UNFORGED_MAX_LAG_BLOCKS = 1200n
const INDEX_HEAD_CACHE_TTL_MS = 5000
let latestHeadCache: { expiresAt: number; value: bigint } | null = null
let currentRoundIdCache: { expiresAt: number; value: bigint } | null = null

type RoundRow = {
  round_id: string
  start_time: string
  end_time: string
  total_deployed: string
  total_winnings: string
  winners_deployed: string
  winning_block: number
  top_miner: string
  top_miner_reward: string
  lootpot_amount: string
  vrf_request_id: string
  top_miner_seed: string
  settled: boolean
  miner_count: string
  is_split: boolean
  settled_block_number: string | null
  settled_tx_hash: string | null
  settled_at: Date | string | null
}

type DeploymentRow = {
  tx_hash: string
  log_index: number
  event_name: string
  round_id: string
  user_address: string
  executor_address: string | null
  amount_per_block: string
  block_mask: string
  total_amount: string
  block_number: string
  block_timestamp: Date | string | null
}

type BuybackRow = {
  type: 'buyback' | 'burn'
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: Date | string | null
  eth_spent: string | null
  loot_received: string | null
  loot_burned: string
  loot_to_stakers: string | null
  burned_by: string | null
}

type CheckpointAggRow = {
  address: string
  gross: string
}

type SyncStateRow = {
  stream_name: string
  last_synced_block: string
}

type TreasuryAgentLeaderboardRow = {
  user_address: string
  rank: number
  deposited: string
  pending: string
  rewards: string
}

type TreasuryAgentHoldingRow = {
  wallet_address: string
  token_key: string
  token_address: string | null
  symbol: string
  name: string
  protocol: string | null
  location_label: string | null
  balance: string
  balance_formatted: string
  usd_value: number
  usd_value_formatted: string
  allocation: number
  decimals: number
  logo_url: string | null
  coingecko_url: string | null
  is_native: boolean
}

function formatIndexedDecimal(value: string, digits = 4) {
  const parsed = Number.parseFloat(value)
  if (!Number.isFinite(parsed)) {
    return (0).toFixed(digits)
  }

  return parsed.toFixed(digits)
}

function toBigInt(value: string | number | bigint | null | undefined) {
  if (value == null) return 0n
  if (typeof value === 'bigint') return value
  return BigInt(String(value))
}

function toIsoString(value: Date | string | null | undefined) {
  if (!value) return null
  if (value instanceof Date) return value.toISOString()
  const parsed = new Date(value)
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  mapper: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  if (items.length === 0) return []
  const safeConcurrency = Math.max(1, Math.min(concurrency, items.length))
  const results = new Array<R>(items.length)
  let nextIndex = 0

  const workers = Array.from({ length: safeConcurrency }, async () => {
    while (nextIndex < items.length) {
      const current = nextIndex
      nextIndex += 1
      results[current] = await mapper(items[current], current)
    }
  })

  await Promise.all(workers)
  return results
}

async function getLatestHeadBlock() {
  const now = Date.now()
  if (latestHeadCache && latestHeadCache.expiresAt > now) {
    return latestHeadCache.value
  }

  const latestHead = await publicClient.getBlockNumber()
  latestHeadCache = { expiresAt: now + INDEX_HEAD_CACHE_TTL_MS, value: latestHead }
  return latestHead
}

async function getCurrentRoundId() {
  const now = Date.now()
  if (currentRoundIdCache && currentRoundIdCache.expiresAt > now) {
    return currentRoundIdCache.value
  }

  const currentRoundId = await publicClient.readContract({
    address: CONTRACTS.gridMining,
    abi: gridMiningAbi,
    functionName: 'currentRoundId',
  }) as bigint

  currentRoundIdCache = { expiresAt: now + INDEX_HEAD_CACHE_TTL_MS, value: currentRoundId }
  return currentRoundId
}

async function withProtocolIndex<T>(
  requiredStreams: string[],
  loader: (client: PoolClient) => Promise<T>,
  options?: {
    maxLagBlocks?: bigint
  }
): Promise<T | null> {
  if (!hasProtocolIndexDatabase()) return null

  const pool = getProtocolIndexPool()
  const client = await pool.connect()

  try {
    const result = await client.query<SyncStateRow>(
      `
        select stream_name, last_synced_block::text
        from protocol_sync_state
        where stream_name = any($1::text[])
      `,
      [requiredStreams]
    )
    const rowsByStream = new Map(result.rows.map((row) => [row.stream_name, row.last_synced_block]))
    for (const stream of requiredStreams) {
      if (!rowsByStream.has(stream)) {
        return null
      }
    }

    if (options?.maxLagBlocks !== undefined) {
      let minSynced: bigint | null = null
      for (const stream of requiredStreams) {
        const block = BigInt(rowsByStream.get(stream) ?? '0')
        if (minSynced === null || block < minSynced) {
          minSynced = block
        }
      }
      if (minSynced === null) {
        return null
      }

      const now = Date.now()
      let latestHead: bigint
      if (latestHeadCache && latestHeadCache.expiresAt > now) {
        latestHead = latestHeadCache.value
      } else {
        latestHead = await getLatestHeadBlock()
      }

      const lag = latestHead > minSynced ? latestHead - minSynced : 0n
      if (lag > options.maxLagBlocks) {
        return null
      }
    }

    return await loader(client)
  } finally {
    client.release()
  }
}

function computeVaultedAmount(totalDeployed: bigint, winnersDeployed: bigint, totalWinnings: bigint) {
  if (winnersDeployed === 0n && totalDeployed > 0n && totalWinnings === 0n) {
    const adminFee = (totalDeployed * PROTOCOL_CONSTANTS.adminFeeBps) / PROTOCOL_CONSTANTS.bpsDenominator
    return totalDeployed - adminFee
  }
  const losersPool = totalDeployed - winnersDeployed
  const losersAdminShare = (losersPool * PROTOCOL_CONSTANTS.adminFeeBps) / PROTOCOL_CONSTANTS.bpsDenominator
  const losersAfterAdmin = losersPool - losersAdminShare
  return (losersAfterAdmin * PROTOCOL_CONSTANTS.vaultFeeBps) / PROTOCOL_CONSTANTS.bpsDenominator
}

function mapDeploymentRow(row: DeploymentRow) {
  return {
    address: getAddress(row.user_address),
    amountPerBlock: toBigInt(row.amount_per_block),
    blockMask: toBigInt(row.block_mask),
    totalAmount: toBigInt(row.total_amount),
    isAutoMine: row.event_name === 'DeployedFor',
    txHash: row.tx_hash,
    blockNumber: toBigInt(row.block_number),
    logIndex: row.log_index,
    timestamp: toIsoString(row.block_timestamp),
  }
}

function groupDeploymentsByRound(rows: DeploymentRow[]) {
  const grouped = new Map<bigint, DeploymentRow[]>()

  for (const row of rows) {
    const roundId = toBigInt(row.round_id)
    const existing = grouped.get(roundId)
    if (existing) {
      existing.push(row)
    } else {
      grouped.set(roundId, [row])
    }
  }

  for (const entries of grouped.values()) {
    entries.sort((a, b) => {
      const blockDelta = Number(toBigInt(a.block_number) - toBigInt(b.block_number))
      if (blockDelta !== 0) return blockDelta
      return a.log_index - b.log_index
    })
  }

  return grouped
}

function buildRoundWinnerAddress(round: RoundRow, deployRows: DeploymentRow[]) {
  const topMiner = getAddress(round.top_miner)
  const isSplit = round.is_split
  const winnersDeployed = toBigInt(round.winners_deployed)

  if (isSplit || winnersDeployed === 0n) return null
  if (!safeAddressEq(topMiner, ONE_ADDRESS)) return topMiner

  const winningDeploys = deployRows
    .map(mapDeploymentRow)
    .filter((log) => decodeBlockMask(log.blockMask).includes(round.winning_block))

  let cumulative = 0n
  const sample = toBigInt(round.top_miner_seed) % winnersDeployed

  for (const log of winningDeploys) {
    const next = cumulative + log.amountPerBlock
    if (sample >= cumulative && sample < next) return log.address
    cumulative = next
  }

  return null
}

function buildRoundMinersFromRows(round: RoundRow, deployRows: DeploymentRow[]) {
  const totalDeployed = toBigInt(round.total_deployed)
  const totalWinnings = toBigInt(round.total_winnings)
  const winnersDeployed = toBigInt(round.winners_deployed)
  const topMinerReward = toBigInt(round.top_miner_reward)
  const lootpotAmount = toBigInt(round.lootpot_amount)
  const mapped = deployRows.map(mapDeploymentRow)
  const winners = mapped.filter((log) => decodeBlockMask(log.blockMask).includes(round.winning_block))
  const claimablePool = totalDeployed === 0n
    ? 0n
    : totalDeployed
        - ((totalDeployed * PROTOCOL_CONSTANTS.adminFeeBps) / PROTOCOL_CONSTANTS.bpsDenominator)
        - computeVaultedAmount(totalDeployed, winnersDeployed, totalWinnings)
  const singleWinner = buildRoundWinnerAddress(round, deployRows)

  return winners.map((winner) => {
    const ethReward = winnersDeployed > 0n
      ? (claimablePool * winner.amountPerBlock) / winnersDeployed
      : 0n

    let lootReward = 0n
    if (round.is_split) {
      lootReward = winnersDeployed > 0n
        ? (topMinerReward * winner.amountPerBlock) / winnersDeployed
        : 0n
    } else if (safeAddressEq(singleWinner, winner.address)) {
      lootReward = topMinerReward
    }

    if (lootpotAmount > 0n && winnersDeployed > 0n) {
      lootReward += (lootpotAmount * winner.amountPerBlock) / winnersDeployed
    }

    return {
      address: winner.address,
      deployed: winner.amountPerBlock.toString(),
      deployedFormatted: etherString(winner.amountPerBlock),
      ethReward: ethReward.toString(),
      ethRewardFormatted: etherString(ethReward),
      lootReward: lootReward.toString(),
      lootRewardFormatted: etherString(lootReward),
    }
  })
}

function buildRoundFromRows(round: RoundRow, deployRows: DeploymentRow[]) {
  const totalDeployed = toBigInt(round.total_deployed)
  const totalWinnings = toBigInt(round.total_winnings)
  const winnersDeployed = toBigInt(round.winners_deployed)
  const lootpotAmount = toBigInt(round.lootpot_amount)
  const vaultedAmount = computeVaultedAmount(totalDeployed, winnersDeployed, totalWinnings)
  const winnerCount = deployRows
    .map(mapDeploymentRow)
    .filter((log) => decodeBlockMask(log.blockMask).includes(round.winning_block))
    .length
  const settledAt = toIsoString(round.settled_at)
  const topMiner = buildRoundWinnerAddress(round, deployRows)

  return {
    roundId: Number(round.round_id),
    winningBlock: round.winning_block,
    topMiner: round.is_split ? null : topMiner,
    isSplit: round.is_split,
    winnerCount,
    totalDeployed: totalDeployed.toString(),
    totalDeployedFormatted: etherString(totalDeployed),
    vaultedAmount: vaultedAmount.toString(),
    vaultedAmountFormatted: etherString(vaultedAmount),
    totalWinnings: totalWinnings.toString(),
    totalWinningsFormatted: etherString(totalWinnings),
    lootpotAmount: lootpotAmount.toString(),
    lootpotAmountFormatted: etherString(lootpotAmount),
    startTime: Number(round.start_time),
    endTime: Number(round.end_time),
    settledAt: settledAt ?? new Date(Number(round.end_time) * 1000).toISOString(),
    txHash: round.settled_tx_hash,
  }
}

async function getRoundRows(client: PoolClient, roundIds: bigint[]) {
  if (roundIds.length === 0) return new Map<bigint, RoundRow>()
  const unique = [...new Set(roundIds.map((value) => value.toString()))]
  const result = await client.query<RoundRow>(
    `
      select
        round_id::text,
        start_time::text,
        end_time::text,
        total_deployed::text,
        total_winnings::text,
        winners_deployed::text,
        winning_block,
        top_miner,
        top_miner_reward::text,
        lootpot_amount::text,
        vrf_request_id::text,
        top_miner_seed::text,
        settled,
        miner_count::text,
        is_split,
        settled_block_number::text,
        settled_tx_hash,
        settled_at
      from protocol_rounds
      where round_id = any($1::bigint[])
    `,
    [unique]
  )

  return new Map(result.rows.map((row) => [toBigInt(row.round_id), row]))
}

async function getDeploymentsForRounds(client: PoolClient, roundIds: bigint[]) {
  if (roundIds.length === 0) return [] as DeploymentRow[]
  const unique = [...new Set(roundIds.map((value) => value.toString()))]
  const result = await client.query<DeploymentRow>(
    `
      select
        tx_hash,
        log_index,
        event_name,
        round_id::text,
        user_address,
        executor_address,
        amount_per_block::text,
        block_mask::text,
        total_amount::text,
        block_number::text,
        block_timestamp
      from protocol_deployments
      where round_id = any($1::bigint[])
      order by round_id desc, block_number asc, log_index asc
    `,
    [unique]
  )

  return result.rows
}

export async function getIndexedTreasuryStats() {
  return withProtocolIndex(['treasury_vault', 'treasury_buybacks', 'direct_burns'], async (client) => {
    const result = await client.query<{
      total_vaulted: string
      current_vaulted: string
      buyback_burned: string
      direct_burned: string
      total_distributed_to_stakers: string
      total_buybacks: string
    }>(`
      with vault as (
        select
          coalesce(sum(amount), 0) as total_vaulted,
          coalesce((array_agg(total_vaulted order by block_number desc, log_index desc))[1], 0) as current_vaulted
        from protocol_treasury_vault_events
      ),
      buybacks as (
        select
          coalesce(sum(loot_burned), 0) as buyback_burned,
          coalesce(sum(loot_to_stakers), 0) as total_distributed_to_stakers,
          count(*)::text as total_buybacks
        from protocol_treasury_buybacks
      ),
      burns as (
        select coalesce(sum(value), 0) as direct_burned
        from protocol_direct_burns b
        where not exists (
          select 1 from protocol_treasury_buybacks bb
          where bb.tx_hash = b.tx_hash
        )
      )
      select
        vault.total_vaulted::text,
        vault.current_vaulted::text,
        buybacks.buyback_burned::text,
        burns.direct_burned::text,
        buybacks.total_distributed_to_stakers::text,
        buybacks.total_buybacks
      from vault, buybacks, burns
    `)

    const row = result.rows[0]
    const totalVaulted = toBigInt(row.total_vaulted)
    const currentVaulted = toBigInt(row.current_vaulted)
    const buybackBurned = toBigInt(row.buyback_burned)
    const directBurned = toBigInt(row.direct_burned)
    const totalBurned = buybackBurned + directBurned

    return {
      totalVaulted: totalVaulted.toString(),
      totalVaultedFormatted: etherString(totalVaulted),
      currentVaulted: currentVaulted.toString(),
      currentVaultedFormatted: etherString(currentVaulted),
      totalBurned: totalBurned.toString(),
      totalBurnedFormatted: etherString(totalBurned),
      buybackBurned: buybackBurned.toString(),
      buybackBurnedFormatted: etherString(buybackBurned),
      directBurned: directBurned.toString(),
      directBurnedFormatted: etherString(directBurned),
      totalDistributedToStakers: row.total_distributed_to_stakers,
      totalDistributedToStakersFormatted: etherString(toBigInt(row.total_distributed_to_stakers)),
      totalBuybacks: Number(row.total_buybacks),
    }
  }, {
    maxLagBlocks: INDEX_TREASURY_MAX_LAG_BLOCKS,
  }).then(async (indexed) => {
    if (!indexed) return null

    const pool = getProtocolIndexPool()
    const lagResult = await pool.query<{ latest_block: string }>(`
      with latest as (
        select coalesce(max(block_number), 0) as latest_block from protocol_treasury_vault_events
        union all
        select coalesce(max(block_number), 0) as latest_block from protocol_treasury_buybacks
        union all
        select coalesce(max(block_number), 0) as latest_block from protocol_direct_burns
      )
      select coalesce(max(latest_block), 0)::text as latest_block from latest
    `)

    const latestIndexedBlock = toBigInt(lagResult.rows[0]?.latest_block ?? '0')
    const latestHead = await getLatestHeadBlock()
    const lag = latestHead > latestIndexedBlock ? latestHead - latestIndexedBlock : 0n
    if (lag > INDEX_TREASURY_MAX_LAG_BLOCKS) {
      return null
    }

    return indexed
  })
}

export async function getIndexedBuybacks(page = 1, limit = 12) {
  return withProtocolIndex(['treasury_buybacks', 'direct_burns'], async (client) => {
    const safePage = Number.isFinite(page) ? Math.max(1, Math.floor(page)) : 1
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), 100)) : 12
    const offset = (safePage - 1) * safeLimit

    const totalResult = await client.query<{ total: string }>(`
      with events as (
        select tx_hash, log_index from protocol_treasury_buybacks
        union all
        select b.tx_hash, b.log_index
        from protocol_direct_burns b
        where not exists (
          select 1 from protocol_treasury_buybacks bb
          where bb.tx_hash = b.tx_hash
        )
      )
      select count(*)::text as total from events
    `)

    const rows = await client.query<BuybackRow>(
      `
        with direct_burns as (
          select
            'burn'::text as type,
            b.tx_hash,
            b.log_index,
            b.block_number::text,
            b.block_timestamp,
            null::text as eth_spent,
            null::text as loot_received,
            b.value::text as loot_burned,
            null::text as loot_to_stakers,
            b.from_address as burned_by
          from protocol_direct_burns b
          where not exists (
            select 1 from protocol_treasury_buybacks bb
            where bb.tx_hash = b.tx_hash
          )
        ),
        buybacks as (
          select
            'buyback'::text as type,
            tx_hash,
            log_index,
            block_number::text,
            block_timestamp,
            eth_spent::text,
            loot_received::text,
            loot_burned::text,
            loot_to_stakers::text,
            null::text as burned_by
          from protocol_treasury_buybacks
        )
        select *
        from (
          select * from buybacks
          union all
          select * from direct_burns
        ) events
        order by block_number::bigint desc, log_index desc
        limit $1 offset $2
      `,
      [safeLimit, offset]
    )

    const total = Number(totalResult.rows[0]?.total ?? '0')
    const pages = Math.max(1, Math.ceil(total / safeLimit))

    return {
      buybacks: rows.rows.map((row) => ({
        type: row.type,
        ethSpent: row.eth_spent,
        ethSpentFormatted: row.eth_spent ? etherString(toBigInt(row.eth_spent)) : null,
        lootReceived: row.loot_received,
        lootReceivedFormatted: row.loot_received ? etherString(toBigInt(row.loot_received)) : null,
        lootBurned: row.loot_burned,
        lootBurnedFormatted: etherString(toBigInt(row.loot_burned)),
        lootToStakers: row.loot_to_stakers,
        lootToStakersFormatted: row.loot_to_stakers ? etherString(toBigInt(row.loot_to_stakers)) : null,
        txHash: row.tx_hash,
        blockNumber: Number(row.block_number),
        timestamp: toIsoString(row.block_timestamp),
        burnedBy: row.burned_by ? getAddress(row.burned_by) : undefined,
      })),
      pagination: { page: safePage, limit: safeLimit, total, pages },
    }
  })
}

export async function getIndexedTreasuryAgentLeaderboard(limit = 12) {
  return withProtocolIndex(['treasury_agent_leaderboard'], async (client) => {
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), 500)) : 12
    const result = await client.query<TreasuryAgentLeaderboardRow>(
      `
        select user_address, rank, deposited::text, pending::text, rewards::text
        from protocol_treasury_agent_leaderboard
        order by rank asc, deposited::numeric desc, pending::numeric desc, rewards::numeric desc
        limit $1
      `,
      [safeLimit]
    )

    return {
      entries: result.rows.map((row) => ({
        rank: row.rank,
        address: getAddress(row.user_address),
        deposited: row.deposited,
        depositedFormatted: formatIndexedDecimal(row.deposited),
        pending: row.pending,
        pendingFormatted: formatIndexedDecimal(row.pending),
        rewards: row.rewards,
        rewardsFormatted: formatIndexedDecimal(row.rewards),
      })),
    }
  }, {
    maxLagBlocks: INDEX_TREASURY_AGENT_MAX_LAG_BLOCKS,
  })
}

export async function getIndexedTreasuryAgentHoldings() {
  return withProtocolIndex(['treasury_agent_holdings'], async (client) => {
    const result = await client.query<TreasuryAgentHoldingRow>(
      `
        select
          wallet_address,
          token_key,
          token_address,
          symbol,
          name,
          protocol,
          location_label,
          balance::text,
          balance_formatted,
          usd_value,
          usd_value_formatted,
          allocation,
          decimals,
          logo_url,
          coingecko_url,
          is_native
        from protocol_treasury_agent_holdings
        order by allocation desc, symbol asc
      `
    )

    return {
      walletAddress: result.rows[0]?.wallet_address ?? null,
      entries: result.rows.map((row) => ({
        tokenKey: row.token_key,
        symbol: row.symbol,
        name: row.name,
        address: row.token_address ? getAddress(row.token_address) : null,
        protocol: row.protocol,
        locationLabel: row.location_label,
        balance: row.balance,
        balanceFormatted: row.balance_formatted,
        usdValue: Number(row.usd_value),
        usdValueFormatted: row.usd_value_formatted,
        allocation: Number(row.allocation),
        decimals: Number(row.decimals),
        logoUrl: row.logo_url,
        coingeckoUrl: row.coingecko_url,
        isNative: row.is_native,
      })),
    }
  }, {
    maxLagBlocks: INDEX_TREASURY_AGENT_MAX_LAG_BLOCKS,
  })
}

export async function getIndexedStakingSnapshot() {
  return withProtocolIndex(['staking_deposits', 'staking_withdrawals', 'staking_compounds', 'staking_yield_distributions'], async (client) => {
    const result = await client.query<{
      total_staked: string
      total_yield_distributed: string
      yield_in_window: string
    }>(
      `
        with deltas as (
          select user_address, amount as delta_amount from protocol_staking_deposits
          union all
          select user_address, amount as delta_amount from protocol_staking_compounds
          union all
          select user_address, -amount as delta_amount from protocol_staking_withdrawals
        ),
        staked as (
          select coalesce(sum(delta_amount), 0) as total_staked from deltas
        ),
        distributed as (
          select
            coalesce(sum(amount), 0) as total_yield_distributed,
            coalesce(sum(case when block_timestamp >= now() - ($1::text || ' days')::interval then amount else 0 end), 0) as yield_in_window
          from protocol_staking_yield_distributions
        )
        select
          staked.total_staked::text,
          distributed.total_yield_distributed::text,
          distributed.yield_in_window::text
        from staked, distributed
      `,
      [String(STAKING_APR_WINDOW_DAYS)]
    )

    const row = result.rows[0]
    const totalStaked = toBigInt(row.total_staked)
    const yieldInWindow = toBigInt(row.yield_in_window)
    const apr = totalStaked > 0n
      ? (Number(yieldInWindow) / Number(totalStaked)) * (365 / STAKING_APR_WINDOW_DAYS) * 100
      : 0

    return {
      totalStaked: totalStaked.toString(),
      totalStakedFormatted: etherString(totalStaked),
      totalYieldDistributed: row.total_yield_distributed,
      totalYieldDistributedFormatted: etherString(toBigInt(row.total_yield_distributed)),
      apr: apr.toFixed(2),
    }
  })
}

export async function getIndexedLockSnapshot() {
  return withProtocolIndex(['lock_reward_notified', 'locker_locked', 'locker_added', 'locker_extended', 'locker_unlocked'], async (client) => {
    const result = await client.query<{
      protocol_locked: string
      lockers: string
      total_notified: string
      protocol_weight: string
    }>(`
      with locked as (
        select user_address, coalesce(sum(amount_delta), 0) as locked_amount
        from protocol_locker_events
        group by user_address
      ),
      latest_weight as (
        select new_total_weight::text as protocol_weight
        from protocol_locker_events
        where new_total_weight is not null
        order by block_number desc, log_index desc
        limit 1
      ),
      rewards as (
        select coalesce(sum(amount), 0)::text as total_notified
        from protocol_lock_reward_notified
      )
      select
        coalesce(sum(case when locked_amount > 0 then locked_amount else 0 end), 0)::text as protocol_locked,
        (count(*) filter (where locked_amount > 0))::text as lockers,
        (select total_notified from rewards),
        coalesce((select protocol_weight from latest_weight), '0') as protocol_weight
      from locked
    `)

    const row = result.rows[0]
    return {
      protocolLocked: row.protocol_locked,
      protocolLockedFormatted: etherString(toBigInt(row.protocol_locked)),
      lockers: Number(row.lockers),
      totalNotified: row.total_notified,
      totalNotifiedFormatted: etherString(toBigInt(row.total_notified)),
      protocolWeight: row.protocol_weight,
      protocolWeightFormatted: etherString(toBigInt(row.protocol_weight)),
    }
  })
}

export async function getIndexedLockDistributions(page = 1, limit = 12) {
  return withProtocolIndex(['lock_reward_notified', 'locker_locked', 'locker_added', 'locker_extended', 'locker_unlocked'], async (client) => {
    const safePage = Number.isFinite(page) ? Math.max(1, Math.floor(page)) : 1
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), 100)) : 12
    const offset = (safePage - 1) * safeLimit

    const snapshot = await getIndexedLockSnapshot()
    if (!snapshot) return null

    const totalResult = await client.query<{ total: string }>('select count(*)::text as total from protocol_lock_reward_notified')
    const rows = await client.query<{
      tx_hash: string
      block_number: string
      block_timestamp: Date | string | null
      amount: string
      distributed_amount: string
      unallocated_amount: string
    }>(
      `
        select
          tx_hash,
          block_number::text,
          block_timestamp,
          amount::text,
          distributed_amount::text,
          unallocated_amount::text
        from protocol_lock_reward_notified
        order by block_number desc, log_index desc
        limit $1 offset $2
      `,
      [safeLimit, offset]
    )

    const total = Number(totalResult.rows[0]?.total ?? '0')
    const pages = Math.max(1, Math.ceil(total / safeLimit))

    return {
      distributions: rows.rows.map((row) => {
        const timestamp = toIsoString(row.block_timestamp)
        const timestampMs = timestamp ? new Date(timestamp).getTime() : 0
        return {
          time: timestamp ? relativeTime(timestampMs) : 'just now',
          timestamp,
          amount: row.amount,
          amountFormatted: etherString(toBigInt(row.amount)),
          ethDistributed: row.distributed_amount,
          ethDistributedFormatted: etherString(toBigInt(row.distributed_amount)),
          unallocatedAmount: row.unallocated_amount,
          unallocatedAmountFormatted: etherString(toBigInt(row.unallocated_amount)),
          lockers: snapshot.lockers,
          lockedSupply: snapshot.protocolLocked,
          lockedSupplyFormatted: snapshot.protocolLockedFormatted,
          txHash: row.tx_hash,
          blockNumber: Number(row.block_number),
        }
      }),
      pagination: { page: safePage, limit: safeLimit, total, pages },
    }
  })
}

export async function getIndexedLeaderboardMiners(limit = 12) {
  return withProtocolIndex(['deployments_direct', 'deployments_for'], async (client) => {
    const result = await client.query<{
      address: string
      total_deployed: string
      rounds_played: string
    }>(
      `
        select
          user_address as address,
          sum(total_amount)::text as total_deployed,
          count(distinct round_id)::text as rounds_played
        from protocol_deployments
        group by user_address
        order by sum(total_amount) desc
        limit $1
      `,
      [limit]
    )

    const deployers = result.rows.map((row) => ({
      address: getAddress(row.address),
      totalDeployed: row.total_deployed,
      totalDeployedFormatted: etherString(toBigInt(row.total_deployed)),
      roundsPlayed: Number(row.rounds_played),
    }))

    return {
      period: 'all',
      miners: deployers,
      deployers,
    }
  })
}

export async function getIndexedLeaderboardStakers(limit = 12) {
  return withProtocolIndex(['staking_deposits', 'staking_withdrawals', 'staking_compounds'], async (client) => {
    const result = await client.query<{
      address: string
      staked_balance: string
    }>(
      `
        with deltas as (
          select user_address, amount as delta_amount from protocol_staking_deposits
          union all
          select user_address, amount as delta_amount from protocol_staking_compounds
          union all
          select user_address, -amount as delta_amount from protocol_staking_withdrawals
        )
        select
          user_address as address,
          sum(delta_amount)::text as staked_balance
        from deltas
        group by user_address
        having sum(delta_amount) > 0
        order by sum(delta_amount) desc
        limit $1
      `,
      [limit]
    )

    return {
      stakers: result.rows.map((row) => ({
        address: getAddress(row.address),
        balance: row.staked_balance,
        balanceFormatted: etherString(toBigInt(row.staked_balance)),
        stakedBalance: row.staked_balance,
        stakedBalanceFormatted: etherString(toBigInt(row.staked_balance)),
      })),
    }
  })
}

export async function getIndexedLeaderboardLockers(limit = 12) {
  return withProtocolIndex(['locker_locked', 'locker_added', 'locker_extended', 'locker_unlocked'], async (client) => {
    const result = await client.query<{
      address: string
      locked: string
      weight: string
    }>(
      `
        with locked as (
          select user_address, sum(amount_delta) as locked_amount
          from protocol_locker_events
          group by user_address
        ),
        latest_weights as (
          select distinct on (user_address)
            user_address,
            coalesce(new_user_weight, 0)::text as weight
          from protocol_locker_events
          order by user_address, block_number desc, log_index desc
        )
        select
          locked.user_address as address,
          locked.locked_amount::text as locked,
          coalesce(latest_weights.weight, '0') as weight
        from locked
        left join latest_weights on latest_weights.user_address = locked.user_address
        where locked.locked_amount > 0
        order by locked.locked_amount desc
        limit $1
      `,
      [limit]
    )

    return {
      lockers: result.rows.map((row) => ({
        address: getAddress(row.address),
        locked: row.locked,
        lockedFormatted: etherString(toBigInt(row.locked)),
        weight: row.weight,
        weightFormatted: etherString(toBigInt(row.weight)),
      })),
    }
  })
}

export async function getIndexedLeaderboardEarners(limit = 12) {
  return withProtocolIndex(['deployments_direct', 'deployments_for'], async (client) => {
    const candidateLimit = Math.max(limit * 40, 400)
    const result = await client.query<CheckpointAggRow>(
      `
        with deployers as (
          select
            user_address as address,
            sum(total_amount) as total_deployed
          from protocol_deployments
          group by user_address
          order by total_deployed desc
          limit $1
        ),
        checkpointers as (
          select
            user_address as address,
            sum(loot_reward) as total_loot
          from protocol_checkpoints
          group by user_address
          order by total_loot desc
          limit $2
        ),
        candidates as (
          select address from deployers
          union
          select address from checkpointers
        )
        select address, '0'::text as gross
        from candidates
      `,
      [candidateLimit, candidateLimit]
    )

    const addresses = result.rows
      .map((row) => row.address)
      .filter((address) => !!address)
      .map((address) => getAddress(address))

    const batchSize = 100
    const chunks: Address[][] = []
    for (let index = 0; index < addresses.length; index += batchSize) {
      chunks.push(addresses.slice(index, index + batchSize))
    }

    const allPending = (await mapWithConcurrency(chunks, 4, async (chunk) => {
      const response = await publicClient.multicall({
        contracts: chunk.map((address) => ({
          address: CONTRACTS.gridMining,
          abi: gridMiningAbi as never,
          functionName: 'getTotalPendingRewards',
          args: [address],
        })),
        allowFailure: true,
      })

      return response.flatMap((item, idx) => {
        if (item.status !== 'success') return []
        const values = item.result as readonly [bigint, bigint, bigint, bigint]
        const unforged = values[1]
        if (unforged <= 0n) return []
        return [{
          address: chunk[idx],
          unforged,
        }]
      })
    })).flat()

    allPending.sort((a, b) => (b.unforged > a.unforged ? 1 : -1))

    const earners = allPending.slice(0, limit).map((item) => ({
      address: item.address,
      unforged: item.unforged.toString(),
      unforgedFormatted: etherString(item.unforged),
      gross: item.unforged.toString(),
      grossFormatted: etherString(item.unforged),
    }))

    return {
      earners,
      pagination: { page: 1, limit, total: allPending.length, pages: 1 },
    }
  }, {
    maxLagBlocks: INDEX_UNFORGED_MAX_LAG_BLOCKS,
  })
}

export async function getIndexedRound(roundIdInput: string | number | bigint) {
  const roundId = BigInt(roundIdInput)
  return withProtocolIndex(['rounds', 'deployments_direct', 'deployments_for'], async (client) => {
    const rounds = await getRoundRows(client, [roundId])
    const round = rounds.get(roundId)
    if (!round) return null
    const deployRows = await getDeploymentsForRounds(client, [roundId])
    return buildRoundFromRows(round, deployRows)
  })
}

export async function getIndexedRoundMiners(roundIdInput: string | number | bigint) {
  const roundId = BigInt(roundIdInput)
  return withProtocolIndex(['rounds', 'deployments_direct', 'deployments_for'], async (client) => {
    const rounds = await getRoundRows(client, [roundId])
    const round = rounds.get(roundId)
    if (!round) return null
    const deployRows = await getDeploymentsForRounds(client, [roundId])
    return {
      roundId: Number(round.round_id),
      winningBlock: round.winning_block,
      miners: buildRoundMinersFromRows(round, deployRows),
    }
  })
}

export async function getIndexedRounds(page = 1, limit = 12, lootpotOnly = false) {
  return withProtocolIndex(['rounds', 'deployments_direct', 'deployments_for'], async (client) => {
    const safePage = Number.isFinite(page) ? Math.max(1, Math.floor(page)) : 1
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), 100)) : 12
    const offset = (safePage - 1) * safeLimit

    const params: Array<number> = [safeLimit, offset]
    const where = lootpotOnly ? 'where lootpot_amount > 0' : ''
    const totalResult = await client.query<{ total: string }>(
      `select count(*)::text as total from protocol_rounds ${where}`
    )
    const rows = await client.query<RoundRow>(
      `
        select
          round_id::text,
          start_time::text,
          end_time::text,
          total_deployed::text,
          total_winnings::text,
          winners_deployed::text,
          winning_block,
          top_miner,
          top_miner_reward::text,
          lootpot_amount::text,
          vrf_request_id::text,
          top_miner_seed::text,
          settled,
          miner_count::text,
          is_split,
          settled_block_number::text,
          settled_tx_hash,
          settled_at
        from protocol_rounds
        ${where}
        order by round_id desc
        limit $1 offset $2
      `,
      params
    )

    const roundIds = rows.rows.map((row) => toBigInt(row.round_id))
    const deployRows = await getDeploymentsForRounds(client, roundIds)
    const grouped = groupDeploymentsByRound(deployRows)
    const total = Number(totalResult.rows[0]?.total ?? '0')
    const pages = Math.max(1, Math.ceil(total / safeLimit))

    const mapped = rows.rows.map((row) => buildRoundFromRows(row, grouped.get(toBigInt(row.round_id)) ?? []))

    if (!lootpotOnly && safePage === 1) {
      const latestIndexedRoundId = mapped.length > 0 ? BigInt(mapped[0].roundId) : 0n
      const currentRoundId = await getCurrentRoundId()
      const latestSettledRoundId = currentRoundId > 0n ? currentRoundId - 1n : 0n
      const roundLag = latestSettledRoundId > latestIndexedRoundId ? latestSettledRoundId - latestIndexedRoundId : 0n
      if (roundLag > 25n) {
        return null
      }
    }

    const latestIndexedSettledBlock = rows.rows.reduce((max, row) => {
      const value = row.settled_block_number ? toBigInt(row.settled_block_number) : 0n
      return value > max ? value : max
    }, 0n)
    if (latestIndexedSettledBlock > 0n) {
      const latestHead = await getLatestHeadBlock()
      const lag = latestHead > latestIndexedSettledBlock ? latestHead - latestIndexedSettledBlock : 0n
      if (lag > INDEX_ROUNDS_MAX_LAG_BLOCKS) {
        return null
      }
    }

    return {
      rounds: mapped,
      pagination: { page: safePage, limit: safeLimit, total, pages },
    }
  }, {
    maxLagBlocks: INDEX_ROUNDS_MAX_LAG_BLOCKS,
  })
}

export async function getIndexedUserHistory(address: Address, limit = 100, roundIdFilter?: bigint) {
  const normalized = getAddress(address)
  return withProtocolIndex(['rounds', 'deployments_direct', 'deployments_for'], async (client) => {
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), 200)) : 100
    const values: Array<string | number> = [normalized]
    const clauses = ['user_address = $1']

    if (roundIdFilter) {
      values.push(roundIdFilter.toString())
      clauses.push(`round_id = $${values.length}`)
    }

    values.push(safeLimit)

    const result = await client.query<DeploymentRow>(
      `
        select
          tx_hash,
          log_index,
          event_name,
          round_id::text,
          user_address,
          executor_address,
          amount_per_block::text,
          block_mask::text,
          total_amount::text,
          block_number::text,
          block_timestamp
        from protocol_deployments
        where ${clauses.join(' and ')}
        order by round_id desc, block_number desc, log_index desc
        limit $${values.length}
      `,
      values
    )

    if (result.rows.length === 0) {
      return {
        history: [],
        totals: {
          totalETHWonFormatted: '0',
          totalLOOTWonFormatted: '0',
          totalETHDeployedFormatted: '0',
          totalPNL: '0',
          roundsPlayed: 0,
          roundsWon: 0,
        },
      }
    }

    const roundIds = result.rows.map((row) => toBigInt(row.round_id))
    const rounds = await getRoundRows(client, roundIds)
    const roundDeployments = await getDeploymentsForRounds(client, roundIds)
    const grouped = groupDeploymentsByRound(roundDeployments)

    const history = result.rows.flatMap((row) => {
      const roundId = toBigInt(row.round_id)
      const round = rounds.get(roundId)
      if (!round) return []

      const miners = buildRoundMinersFromRows(round, grouped.get(roundId) ?? [])
      const blockMask = toBigInt(row.block_mask)
      const totalAmount = toBigInt(row.total_amount)
      const wonWinningBlock = decodeBlockMask(blockMask).includes(round.winning_block)
      const userMiner = miners.find((miner) => safeAddressEq(miner.address, normalized))
      const ethWon = userMiner ? toBigInt(userMiner.ethReward) : 0n
      const lootWon = userMiner ? toBigInt(userMiner.lootReward) : 0n
      const pnl = Number(ethWon) / 1e18 - Number(totalAmount) / 1e18

      return [{
        roundId: Number(row.round_id),
        totalAmount: totalAmount.toString(),
        blockMask: blockMask.toString(),
        txHash: row.tx_hash,
        isAutoMine: row.event_name === 'DeployedFor',
        timestamp: toIsoString(row.block_timestamp),
        roundResult: {
          settled: true,
          wonWinningBlock,
          lootpotHit: toBigInt(round.lootpot_amount) > 0n,
          winningBlock: round.winning_block,
          ethWon: ethWon.toString(),
          ethWonFormatted: etherString(ethWon),
          lootWon: lootWon.toString(),
          lootWonFormatted: etherString(lootWon),
          pnl: pnl.toString(),
        },
      }]
    })

    const totals = history.reduce((acc, entry) => {
      acc.totalETHWon += toBigInt(entry.roundResult.ethWon)
      acc.totalLOOTWon += toBigInt(entry.roundResult.lootWon)
      acc.totalETHDeployed += toBigInt(entry.totalAmount)
      acc.totalPNL += Number(entry.roundResult.pnl)
      if (entry.roundResult.wonWinningBlock) acc.roundsWon += 1
      return acc
    }, {
      totalETHWon: 0n,
      totalLOOTWon: 0n,
      totalETHDeployed: 0n,
      totalPNL: 0,
      roundsWon: 0,
    })

    return {
      history,
      totals: {
        totalETHWonFormatted: etherString(totals.totalETHWon),
        totalLOOTWonFormatted: etherString(totals.totalLOOTWon),
        totalETHDeployedFormatted: etherString(totals.totalETHDeployed),
        totalPNL: totals.totalPNL.toString(),
        roundsPlayed: history.length,
        roundsWon: totals.roundsWon,
      },
    }
  })
}
