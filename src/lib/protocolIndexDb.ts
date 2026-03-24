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
