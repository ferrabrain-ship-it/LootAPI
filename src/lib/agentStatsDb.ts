import { Pool } from 'pg'
import { env } from '../config/env.js'

const AGENT_STATS_SCHEMA_SQL = `
create table if not exists agent_wallet_stats (
  wallet_address text primary key,
  rounds_played integer not null default 0,
  wins integer not null default 0,
  losses integer not null default 0,
  total_deployed_eth numeric(38,18) not null default 0,
  total_rewards_eth numeric(38,18) not null default 0,
  loot_earned numeric(38,18) not null default 0,
  loot_value_eth numeric(38,18) not null default 0,
  eth_pnl numeric(38,18) not null default 0,
  true_pnl_eth numeric(38,18) not null default 0,
  best_round_eth numeric(38,18) not null default 0,
  worst_round_eth numeric(38,18) not null default 0,
  average_bet_eth numeric(38,18) not null default 0,
  win_rate numeric(12,6) not null default 0,
  net_roi numeric(12,6) not null default 0,
  total_rounds_won_eth numeric(38,18) not null default 0,
  total_loot_value_eth numeric(38,18) not null default 0,
  last_active_at timestamptz,
  last_processed_round bigint not null default 0,
  updated_at timestamptz not null default now()
);

create table if not exists agent_recent_rounds (
  wallet_address text not null,
  round_id bigint not null,
  block_number integer not null,
  blocks_covered integer not null default 0,
  deployed_eth numeric(38,18) not null default 0,
  rewards_eth numeric(38,18) not null default 0,
  loot_earned numeric(38,18) not null default 0,
  loot_value_eth numeric(38,18) not null default 0,
  pnl_eth numeric(38,18) not null default 0,
  true_pnl_eth numeric(38,18) not null default 0,
  pnl_pct numeric(12,6) not null default 0,
  outcome text not null,
  mode text not null,
  round_timestamp timestamptz,
  created_at timestamptz not null default now(),
  primary key (wallet_address, round_id)
);

create index if not exists idx_agent_recent_rounds_wallet_round
  on agent_recent_rounds(wallet_address, round_id desc);

create index if not exists idx_agent_wallet_stats_last_processed_round
  on agent_wallet_stats(last_processed_round);
`

let agentStatsPool: Pool | null = null

export function getAgentStatsDatabaseUrl() {
  const databaseUrl = env.agentStatsDatabaseUrl.trim()

  if (!databaseUrl) {
    throw new Error('AGENT_STATS_DATABASE_URL is not configured')
  }

  return databaseUrl
}

export function getAgentStatsPool() {
  if (!agentStatsPool) {
    agentStatsPool = new Pool({
      connectionString: getAgentStatsDatabaseUrl(),
      max: 4,
    })
  }

  return agentStatsPool
}

export async function initAgentStatsSchema() {
  const pool = getAgentStatsPool()
  const client = await pool.connect()

  try {
    await client.query(AGENT_STATS_SCHEMA_SQL)
  } finally {
    client.release()
  }
}

export async function closeAgentStatsPool() {
  if (!agentStatsPool) {
    return
  }

  const pool = agentStatsPool
  agentStatsPool = null
  await pool.end()
}

