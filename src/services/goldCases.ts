import { formatUnits, getAddress, type Address } from 'viem'
import { CONTRACTS } from '../config/contracts.js'
import { getProtocolIndexPool, hasProtocolIndexDatabase } from '../lib/protocolIndexDb.js'

const GOLD_DECIMALS = 18
const ACTIVE_CASES_CONTRACT = getAddress(CONTRACTS.goldCases)
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000' as Address

type GoldCaseBestWinRow = {
  request_id: string
  chest_index: number
  tx_hash: string
  player_address: string
  tier: number
  tier_name: string
  multiplier_label: string
  multiplier_bps: string | null
  payout: string
  total_payout: string
  jackpot_payout: string
  chest_count: number
  block_number: string
  block_timestamp: Date | string | null
}

type GoldCaseOpenRow = {
  request_id: string
  tx_hash: string
  player_address: string
  chest_count: number
  chest_price: string
  total_cost: string
  paid_from_claimable: boolean
  total_payout: string | null
  jackpot_payout: string | null
  best_tier: number | null
  best_tier_name: string | null
  block_number: string
  block_timestamp: Date | string | null
}

type GoldCaseStatsRow = {
  opens: string
  chests_opened: string
  total_spent: string
  total_payout: string
  jackpot_payout: string
  wins: string
  jackpot_hits: string
  best_tier: number | null
}

const tierLabels = ['Empty', 'Common', 'Rare', 'Epic', 'Legendary', '25x', 'Mythic', '100x', 'Jackpot']

function toBigInt(value: string | number | bigint | null | undefined) {
  if (value == null) return 0n
  if (typeof value === 'bigint') return value
  return BigInt(String(value))
}

function gold(value: string | number | bigint | null | undefined) {
  return formatUnits(toBigInt(value), GOLD_DECIMALS)
}

function goldFixed(value: string | number | bigint | null | undefined, digits = 4) {
  return Number(gold(value)).toLocaleString(undefined, {
    maximumFractionDigits: digits,
  })
}

function clampLimit(limit: number, fallback = 10, max = 100) {
  return Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), max)) : fallback
}

function safeAddress(value: string | null | undefined): Address {
  if (!value || value === ZERO_ADDRESS) return ZERO_ADDRESS
  return getAddress(value)
}

function toIsoString(value: Date | string | null | undefined) {
  if (!value) return null
  if (value instanceof Date) return value.toISOString()
  const parsed = new Date(value)
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null
}

function formatMultiplier(row: { multiplier_label: string; multiplier_bps: string | null }) {
  if (row.multiplier_label) return row.multiplier_label
  if (!row.multiplier_bps) return '--'
  const bps = Number(row.multiplier_bps)
  if (!Number.isFinite(bps)) return '--'
  return `${bps / 10_000}x`
}

function mapBestWin(row: GoldCaseBestWinRow) {
  const payout = toBigInt(row.payout)
  return {
    requestId: row.request_id,
    chestIndex: row.chest_index,
    txHash: row.tx_hash,
    player: safeAddress(row.player_address),
    wallet: safeAddress(row.player_address),
    tier: row.tier,
    tierName: row.tier_name || tierLabels[row.tier] || 'Unknown',
    result: row.tier_name || tierLabels[row.tier] || 'Unknown',
    multiplier: formatMultiplier(row),
    payout: payout.toString(),
    payoutFormatted: goldFixed(payout, 4),
    totalPayout: toBigInt(row.total_payout).toString(),
    jackpotPayout: toBigInt(row.jackpot_payout).toString(),
    chestCount: row.chest_count,
    blockNumber: row.block_number,
    timestamp: toIsoString(row.block_timestamp),
  }
}

function mapOpen(row: GoldCaseOpenRow) {
  return {
    requestId: row.request_id,
    txHash: row.tx_hash,
    player: safeAddress(row.player_address),
    wallet: safeAddress(row.player_address),
    chestCount: row.chest_count,
    chestPrice: toBigInt(row.chest_price).toString(),
    chestPriceFormatted: goldFixed(row.chest_price, 4),
    totalCost: toBigInt(row.total_cost).toString(),
    totalCostFormatted: goldFixed(row.total_cost, 4),
    paidFromClaimable: row.paid_from_claimable,
    resolved: row.total_payout !== null,
    totalPayout: toBigInt(row.total_payout).toString(),
    totalPayoutFormatted: goldFixed(row.total_payout, 4),
    jackpotPayout: toBigInt(row.jackpot_payout).toString(),
    jackpotPayoutFormatted: goldFixed(row.jackpot_payout, 4),
    bestTier: row.best_tier ?? 0,
    bestTierName: row.best_tier_name ?? tierLabels[row.best_tier ?? 0] ?? 'Empty',
    blockNumber: row.block_number,
    timestamp: toIsoString(row.block_timestamp),
  }
}

export async function getGoldCaseBestWins(limit = 10) {
  const safeLimit = clampLimit(limit, 10, 100)
  if (!hasProtocolIndexDatabase()) {
    return { indexed: false, wins: [] }
  }

  const pool = getProtocolIndexPool()
  const result = await pool.query<GoldCaseBestWinRow>(
    `
      select *
      from gold_case_results
      where contract_address = $1
        and tier > 0
      order by tier desc, payout desc, block_number desc, log_index desc, chest_index asc
      limit $2
    `,
    [ACTIVE_CASES_CONTRACT, safeLimit],
  )

  return {
    indexed: true,
    wins: result.rows.map(mapBestWin),
  }
}

export async function getGoldCaseActivity(limit = 20) {
  const safeLimit = clampLimit(limit, 20, 100)
  if (!hasProtocolIndexDatabase()) {
    return { indexed: false, opens: [] }
  }

  const pool = getProtocolIndexPool()
  const result = await pool.query<GoldCaseOpenRow>(
    `
      select
        o.request_id::text,
        o.tx_hash,
        o.player_address,
        o.chest_count,
        o.chest_price::text,
        o.total_cost::text,
        o.paid_from_claimable,
        r.total_payout::text,
        r.jackpot_payout::text,
        best.tier as best_tier,
        best.tier_name as best_tier_name,
        o.block_number::text,
        o.block_timestamp
      from gold_case_opens o
      left join gold_case_resolutions r on r.contract_address = o.contract_address and r.request_id = o.request_id
      left join lateral (
        select tier, tier_name
        from gold_case_results gr
        where gr.contract_address = o.contract_address
          and gr.request_id = o.request_id
        order by tier desc, payout desc, chest_index asc
        limit 1
      ) best on true
      where o.contract_address = $2
      order by o.block_number desc, o.log_index desc
      limit $1
    `,
    [safeLimit, ACTIVE_CASES_CONTRACT],
  )

  return {
    indexed: true,
    opens: result.rows.map(mapOpen),
  }
}

export async function getGoldCaseStats() {
  if (!hasProtocolIndexDatabase()) {
    return { indexed: false, stats: null }
  }

  const pool = getProtocolIndexPool()
  const result = await pool.query<GoldCaseStatsRow>(
    `
      select
        count(distinct o.request_id)::text as opens,
        coalesce(sum(o.chest_count), 0)::text as chests_opened,
        coalesce(sum(o.total_cost), 0)::text as total_spent,
        coalesce(sum(r.total_payout), 0)::text as total_payout,
        coalesce(sum(r.jackpot_payout), 0)::text as jackpot_payout,
        count(gr.*)::text as wins,
        count(*) filter (where gr.tier = 6)::text as jackpot_hits,
        max(gr.tier) as best_tier
      from gold_case_opens o
      left join gold_case_resolutions r on r.contract_address = o.contract_address and r.request_id = o.request_id
      left join gold_case_results gr on gr.contract_address = o.contract_address and gr.request_id = o.request_id and gr.tier > 0
      where o.contract_address = $1
    `,
    [ACTIVE_CASES_CONTRACT],
  )

  const row = result.rows[0]
  return {
    indexed: true,
    stats: {
      opens: row?.opens ?? '0',
      chestsOpened: row?.chests_opened ?? '0',
      totalSpent: toBigInt(row?.total_spent).toString(),
      totalSpentFormatted: goldFixed(row?.total_spent, 2),
      totalPayout: toBigInt(row?.total_payout).toString(),
      totalPayoutFormatted: goldFixed(row?.total_payout, 2),
      jackpotPayout: toBigInt(row?.jackpot_payout).toString(),
      jackpotPayoutFormatted: goldFixed(row?.jackpot_payout, 2),
      wins: row?.wins ?? '0',
      jackpotHits: row?.jackpot_hits ?? '0',
      bestTier: row?.best_tier ?? 0,
      bestTierName: tierLabels[row?.best_tier ?? 0] ?? 'Empty',
    },
  }
}
