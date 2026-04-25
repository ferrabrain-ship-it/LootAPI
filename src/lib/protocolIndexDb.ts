import { Pool } from 'pg'
import { env } from '../config/env.js'

const PROTOCOL_INDEX_SCHEMA_SQL = `
create table if not exists protocol_sync_state (
  stream_name text primary key,
  last_synced_block bigint not null default 0,
  updated_at timestamptz not null default now()
);

create table if not exists protocol_rounds (
  round_id bigint primary key,
  start_time bigint not null,
  end_time bigint not null,
  total_deployed numeric(78,0) not null,
  total_winnings numeric(78,0) not null,
  winners_deployed numeric(78,0) not null,
  winning_block integer not null,
  top_miner text not null,
  top_miner_reward numeric(78,0) not null,
  lootpot_amount numeric(78,0) not null,
  vrf_request_id numeric(78,0) not null,
  top_miner_seed numeric(78,0) not null,
  settled boolean not null default false,
  miner_count numeric(78,0) not null default 0,
  is_split boolean not null default false,
  settled_block_number bigint,
  settled_tx_hash text,
  settled_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists protocol_deployments (
  tx_hash text not null,
  log_index integer not null,
  event_name text not null,
  round_id bigint not null,
  user_address text not null,
  executor_address text,
  amount_per_block numeric(78,0) not null,
  block_mask numeric(78,0) not null,
  total_amount numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_checkpoints (
  tx_hash text not null,
  log_index integer not null,
  round_id bigint not null,
  user_address text not null,
  eth_reward numeric(78,0) not null,
  loot_reward numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_claimed_loot (
  tx_hash text not null,
  log_index integer not null,
  user_address text not null,
  mined_loot numeric(78,0) not null,
  forged_loot numeric(78,0) not null,
  fee numeric(78,0) not null,
  net numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_treasury_vault_events (
  tx_hash text not null,
  log_index integer not null,
  amount numeric(78,0) not null,
  total_vaulted numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_treasury_buybacks (
  tx_hash text not null,
  log_index integer not null,
  eth_spent numeric(78,0) not null,
  loot_received numeric(78,0) not null,
  loot_burned numeric(78,0) not null,
  loot_to_stakers numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_direct_burns (
  tx_hash text not null,
  log_index integer not null,
  from_address text not null,
  value numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_staking_deposits (
  tx_hash text not null,
  log_index integer not null,
  user_address text not null,
  amount numeric(78,0) not null,
  new_balance numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_staking_withdrawals (
  tx_hash text not null,
  log_index integer not null,
  user_address text not null,
  amount numeric(78,0) not null,
  new_balance numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_staking_compounds (
  tx_hash text not null,
  log_index integer not null,
  user_address text not null,
  compounder_address text not null,
  amount numeric(78,0) not null,
  fee numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_staking_yield_distributions (
  tx_hash text not null,
  log_index integer not null,
  amount numeric(78,0) not null,
  new_acc_yield_per_share numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_lock_reward_notified (
  tx_hash text not null,
  log_index integer not null,
  amount numeric(78,0) not null,
  distributed_amount numeric(78,0) not null,
  unallocated_amount numeric(78,0) not null,
  acc_reward_per_weight numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_locker_events (
  tx_hash text not null,
  log_index integer not null,
  event_name text not null,
  user_address text not null,
  lock_id bigint not null,
  amount_delta numeric(78,0) not null,
  unlock_time bigint,
  duration_id integer,
  new_user_weight numeric(78,0),
  new_total_weight numeric(78,0),
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists protocol_treasury_agent_leaderboard (
  user_address text primary key,
  rank integer not null default 0,
  deposited numeric(78,18) not null default 0,
  pending numeric(78,18) not null default 0,
  rewards numeric(78,18) not null default 0,
  snapshot_block bigint not null default 0,
  updated_at timestamptz not null default now()
);

create table if not exists protocol_treasury_agent_holdings (
  wallet_address text not null,
  token_key text not null,
  token_address text,
  symbol text not null,
  name text not null,
  protocol text,
  location_label text,
  balance numeric(78,18) not null,
  balance_formatted text not null,
  usd_value double precision not null default 0,
  usd_value_formatted text not null,
  allocation double precision not null default 0,
  decimals integer not null,
  logo_url text,
  coingecko_url text,
  is_native boolean not null default false,
  snapshot_block bigint not null default 0,
  updated_at timestamptz not null default now(),
  primary key (wallet_address, token_key)
);

alter table protocol_treasury_agent_holdings
  add column if not exists protocol text;

alter table protocol_treasury_agent_holdings
  add column if not exists location_label text;

create table if not exists crown_rounds (
  round_id bigint primary key,
  start_time bigint not null default 0,
  end_time bigint not null default 0,
  next_roll_at bigint not null default 0,
  total_sold numeric(78,0) not null default 0,
  prize_pool numeric(78,0) not null default 0,
  acc_dividend_per_chest numeric(78,0) not null default 0,
  holder_count numeric(78,0) not null default 0,
  vrf_request_id numeric(78,0) not null default 0,
  vrf_requested_at bigint not null default 0,
  winning_roll numeric(78,0) not null default 0,
  current_leader text not null default '0x0000000000000000000000000000000000000000',
  leader_snapshot text not null default '0x0000000000000000000000000000000000000000',
  winner text not null default '0x0000000000000000000000000000000000000000',
  active boolean not null default false,
  settled boolean not null default false,
  vrf_pending boolean not null default false,
  activated_block_number bigint,
  settled_block_number bigint,
  settled_tx_hash text,
  settled_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists crown_purchases (
  tx_hash text not null,
  log_index integer not null,
  round_id bigint not null,
  user_address text not null,
  price numeric(78,0) not null,
  total_sold_after numeric(78,0) not null,
  leader text not null,
  prize_amount numeric(78,0) not null,
  dividend_amount numeric(78,0) not null,
  buyback_amount numeric(78,0) not null,
  lock_amount numeric(78,0) not null,
  admin_amount numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists crown_batch_purchases (
  tx_hash text not null,
  log_index integer not null,
  round_id bigint not null,
  user_address text not null,
  amount numeric(78,0) not null,
  total_price numeric(78,0) not null,
  total_sold_after numeric(78,0) not null,
  leader text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists crown_roll_events (
  tx_hash text not null,
  log_index integer not null,
  event_name text not null,
  round_id bigint not null,
  request_id numeric(78,0),
  leader_snapshot text,
  roll numeric(78,0),
  next_roll_at bigint,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists crown_claims (
  tx_hash text not null,
  log_index integer not null,
  event_name text not null,
  user_address text not null,
  amount numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists autocrown_configs (
  user_address text primary key,
  active boolean not null default true,
  open_new_round boolean not null,
  defend_lead boolean not null,
  snipe_when_outbid boolean not null,
  buy_window_seconds integer not null,
  max_buys_per_tick integer not null,
  max_buys_per_round integer not null,
  max_build_price numeric(78,0) not null,
  max_battle_price numeric(78,0) not null,
  min_prize_pool numeric(78,0) not null,
  target_chests numeric(78,0) not null,
  max_round_spend numeric(78,0) not null,
  total_budget numeric(78,0) not null,
  block_number bigint not null,
  tx_hash text not null,
  updated_at timestamptz
);

create table if not exists autocrown_deposits (
  tx_hash text not null,
  log_index integer not null,
  user_address text not null,
  amount numeric(78,0) not null,
  new_balance numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists autocrown_executions (
  tx_hash text not null,
  log_index integer not null,
  event_name text not null,
  user_address text not null,
  round_id bigint not null,
  amount numeric(78,0) not null,
  total_price numeric(78,0) not null,
  executor_fee numeric(78,0) not null,
  battle_phase boolean not null,
  deposit_balance numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create table if not exists autocrown_stops (
  tx_hash text not null,
  log_index integer not null,
  user_address text not null,
  refunded numeric(78,0) not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  primary key (tx_hash, log_index)
);

create index if not exists idx_protocol_rounds_settled_block
  on protocol_rounds (settled_block_number desc);

create index if not exists idx_protocol_rounds_lootpot_round
  on protocol_rounds (round_id desc)
  where lootpot_amount > 0;

create index if not exists idx_protocol_deployments_round
  on protocol_deployments (round_id desc, block_number desc, log_index desc);

create index if not exists idx_protocol_deployments_user_round
  on protocol_deployments (user_address, round_id desc, block_number desc, log_index desc);

create index if not exists idx_protocol_checkpoints_user_block
  on protocol_checkpoints (user_address, block_number desc, log_index desc);

create index if not exists idx_protocol_claimed_loot_user_block
  on protocol_claimed_loot (user_address, block_number desc, log_index desc);

create index if not exists idx_protocol_treasury_buybacks_block
  on protocol_treasury_buybacks (block_number desc, log_index desc);

create index if not exists idx_protocol_treasury_vault_block
  on protocol_treasury_vault_events (block_number desc, log_index desc);

create index if not exists idx_protocol_direct_burns_block
  on protocol_direct_burns (block_number desc, log_index desc);

create index if not exists idx_protocol_staking_deposits_user
  on protocol_staking_deposits (user_address, block_number desc, log_index desc);

create index if not exists idx_protocol_staking_withdrawals_user
  on protocol_staking_withdrawals (user_address, block_number desc, log_index desc);

create index if not exists idx_protocol_staking_compounds_user
  on protocol_staking_compounds (user_address, block_number desc, log_index desc);

create index if not exists idx_protocol_staking_yield_block
  on protocol_staking_yield_distributions (block_number desc, log_index desc);

create index if not exists idx_protocol_lock_reward_notified_block
  on protocol_lock_reward_notified (block_number desc, log_index desc);

create index if not exists idx_protocol_locker_events_user_block
  on protocol_locker_events (user_address, block_number desc, log_index desc);

create index if not exists idx_protocol_treasury_agent_leaderboard_rank
  on protocol_treasury_agent_leaderboard (rank asc, deposited desc, pending desc, rewards desc);

create index if not exists idx_protocol_treasury_agent_holdings_wallet_allocation
  on protocol_treasury_agent_holdings (wallet_address, allocation desc);

create index if not exists idx_crown_rounds_round_desc
  on crown_rounds (round_id desc);

create index if not exists idx_crown_purchases_round
  on crown_purchases (round_id desc, block_number desc, log_index desc);

create index if not exists idx_crown_purchases_user
  on crown_purchases (user_address, round_id desc, block_number desc, log_index desc);

create index if not exists idx_crown_roll_events_round
  on crown_roll_events (round_id desc, block_number desc, log_index desc);

create index if not exists idx_crown_claims_user
  on crown_claims (user_address, block_number desc, log_index desc);

create index if not exists idx_autocrown_configs_active
  on autocrown_configs (active, updated_at desc);

create index if not exists idx_autocrown_executions_user
  on autocrown_executions (user_address, round_id desc, block_number desc, log_index desc);
`

let protocolIndexPool: Pool | null = null

export function getProtocolIndexDatabaseUrl() {
  const databaseUrl = env.protocolIndexDatabaseUrl.trim()

  if (!databaseUrl) {
    throw new Error('PROTOCOL_INDEX_DATABASE_URL is not configured')
  }

  return databaseUrl
}

export function hasProtocolIndexDatabase() {
  return env.protocolIndexDatabaseUrl.trim().length > 0
}

export function getProtocolIndexPool() {
  if (!protocolIndexPool) {
    protocolIndexPool = new Pool({
      connectionString: getProtocolIndexDatabaseUrl(),
      max: 4,
    })
  }

  return protocolIndexPool
}

export async function initProtocolIndexSchema() {
  const pool = getProtocolIndexPool()
  const client = await pool.connect()

  try {
    await client.query(PROTOCOL_INDEX_SCHEMA_SQL)
  } finally {
    client.release()
  }
}

export async function closeProtocolIndexPool() {
  if (!protocolIndexPool) return

  const pool = protocolIndexPool
  protocolIndexPool = null
  await pool.end()
}
