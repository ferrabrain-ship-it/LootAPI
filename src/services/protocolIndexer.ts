import type { AbiEvent, Address, Log } from 'viem'
import { getAddress, parseAbiItem } from 'viem'
import type { PoolClient } from 'pg'
import gridMiningAbi from '../abis/GridMining.json' with { type: 'json' }
import crownAbi from '../abis/Crown.json' with { type: 'json' }
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'
import { publicClient } from '../lib/client.js'
import { closeProtocolIndexPool, getProtocolIndexPool, initProtocolIndexSchema } from '../lib/protocolIndexDb.js'
import { toBigInt } from '../lib/format.js'
import { getTreasuryAgentHoldings, getTreasuryAgentLeaderboard } from './treasuryAgent.js'

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>

const DEPLOYED_EVENT = parseAbiItem(
  'event Deployed(uint64 indexed roundId, address indexed user, uint256 amountPerBlock, uint256 blockMask, uint256 totalAmount)'
)
const DEPLOYED_FOR_EVENT = parseAbiItem(
  'event DeployedFor(uint64 indexed roundId, address indexed user, address indexed executor, uint256 amountPerBlock, uint256 blockMask, uint256 totalAmount)'
)
const ROUND_SETTLED_EVENT = parseAbiItem(
  'event RoundSettled(uint64 indexed roundId, uint8 winningBlock, address topMiner, uint256 totalWinnings, uint256 topMinerReward, uint256 lootpotAmount, bool isSplit, uint256 topMinerSeed, uint256 winnersDeployed)'
)
const CHECKPOINTED_EVENT = parseAbiItem('event Checkpointed(uint64 indexed roundId, address indexed user, uint256 ethReward, uint256 lootReward)')
const CLAIMED_LOOT_EVENT = parseAbiItem('event ClaimedLOOT(address indexed user, uint256 minedLoot, uint256 forgedLoot, uint256 fee, uint256 net)')
const VAULT_EVENT = parseAbiItem('event VaultReceived(uint256 amount, uint256 totalVaulted)')
const BUYBACK_EVENT = parseAbiItem(
  'event BuybackExecuted(uint256 ethSpent, uint256 lootReceived, uint256 lootBurned, uint256 lootToStakers)'
)
const TRANSFER_EVENT = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)')
const STAKE_DEPOSIT_EVENT = parseAbiItem('event Deposited(address indexed user, uint256 amount, uint256 newBalance)')
const STAKE_WITHDRAW_EVENT = parseAbiItem('event Withdrawn(address indexed user, uint256 amount, uint256 newBalance)')
const STAKE_COMPOUND_EVENT = parseAbiItem('event YieldCompounded(address indexed user, uint256 amount, address indexed compounder, uint256 fee)')
const YIELD_DISTRIBUTED_EVENT = parseAbiItem('event YieldDistributed(uint256 amount, uint256 newAccYieldPerShare)')
const LOCK_REWARD_NOTIFIED_EVENT = parseAbiItem(
  'event RewardNotified(uint256 amount, uint256 distributedAmount, uint256 unallocatedAmount, uint256 accRewardPerWeight)'
)
const LOCKED_EVENT = parseAbiItem(
  'event Locked(address indexed user, uint256 indexed lockId, uint256 amount, uint8 durationId, uint256 unlockTime, uint256 newUserWeight, uint256 newTotalWeight)'
)
const ADDED_TO_LOCK_EVENT = parseAbiItem(
  'event AddedToLock(address indexed user, uint256 indexed lockId, uint256 amountAdded, uint256 newAmount, uint256 newUnlockTime, uint256 newUserWeight, uint256 newTotalWeight)'
)
const EXTENDED_LOCK_EVENT = parseAbiItem(
  'event Extended(address indexed user, uint256 indexed lockId, uint8 oldDurationId, uint8 newDurationId, uint256 newUnlockTime, uint256 newUserWeight, uint256 newTotalWeight)'
)
const UNLOCKED_EVENT = parseAbiItem(
  'event Unlocked(address indexed user, uint256 indexed lockId, uint256 amount, uint256 newUserWeight, uint256 newTotalWeight)'
)
const CROWN_ROUND_ACTIVATED_EVENT = parseAbiItem('event RoundActivated(uint64 indexed roundId, uint256 startTime, uint256 nextRollAt)')
const CROWN_CHEST_PURCHASED_EVENT = parseAbiItem(
  'event ChestPurchased(uint64 indexed roundId, address indexed user, uint256 price, uint256 totalSold, address indexed leader, uint256 prizeAmount, uint256 dividendAmount, uint256 buybackAmount, uint256 lockAmount, uint256 adminAmount)'
)
const CROWN_CHESTS_PURCHASED_EVENT = parseAbiItem(
  'event ChestsPurchased(uint64 indexed roundId, address indexed user, uint256 amount, uint256 totalPrice, uint256 totalSold, address indexed leader)'
)
const CROWN_ROLL_REQUESTED_EVENT = parseAbiItem(
  'event RollRequested(uint64 indexed roundId, uint256 requestId, address indexed leaderSnapshot, uint256 nextRollAt)'
)
const CROWN_ROLL_RESOLVED_EVENT = parseAbiItem('event RollResolved(uint64 indexed roundId, uint256 roll, uint256 nextRollAt)')
const CROWN_ROUND_SETTLED_EVENT = parseAbiItem(
  'event RoundSettled(uint64 indexed roundId, address indexed winner, uint256 prize, uint256 totalSold, uint256 roll)'
)
const CROWN_CLAIMED_DIVIDENDS_EVENT = parseAbiItem('event ClaimedDividends(address indexed user, uint256 amount)')
const CROWN_CLAIMED_PRIZE_EVENT = parseAbiItem('event ClaimedPrize(address indexed user, uint256 amount)')
const AUTOCROWN_CONFIG_UPDATED_EVENT = parseAbiItem(
  'event ConfigUpdated(address indexed user, bool openNewRound, bool defendLead, bool snipeWhenOutbid, uint32 buyWindowSeconds, uint32 maxBuysPerTick, uint32 maxBuysPerRound, uint256 maxBuildPrice, uint256 maxBattlePrice, uint256 minPrizePool, uint256 targetChests, uint256 maxRoundSpend, uint256 totalBudget)'
)
const AUTOCROWN_DEPOSIT_ADDED_EVENT = parseAbiItem('event DepositAdded(address indexed user, uint256 amount, uint256 newBalance)')
const AUTOCROWN_EXECUTED_FOR_EVENT = parseAbiItem(
  'event ExecutedFor(address indexed user, uint64 indexed roundId, uint256 price, uint256 executorFee, bool battlePhase, uint256 depositBalance)'
)
const AUTOCROWN_BATCH_EXECUTED_FOR_EVENT = parseAbiItem(
  'event BatchExecutedFor(address indexed user, uint64 indexed roundId, uint256 amount, uint256 totalPrice, uint256 executorFee, bool battlePhase, uint256 depositBalance)'
)
const AUTOCROWN_STOPPED_EVENT = parseAbiItem('event Stopped(address indexed user, uint256 refunded)')

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
const LOG_BLOCK_RANGE = 10_000n
const MIN_LOG_BLOCK_RANGE = 1_000n
const LOG_FETCH_CONCURRENCY = 4
const RPC_RETRY_ATTEMPTS = 3
const ROW_UPSERT_CONCURRENCY = 12
const ROUND_READ_CONCURRENCY = 6

const blockTimestampCache = new Map<bigint, number>()

type DeploymentLog =
  | Log<bigint, number, false, typeof DEPLOYED_EVENT>
  | Log<bigint, number, false, typeof DEPLOYED_FOR_EVENT>

type LockerStateLog =
  | Log<bigint, number, false, typeof LOCKED_EVENT>
  | Log<bigint, number, false, typeof ADDED_TO_LOCK_EVENT>
  | Log<bigint, number, false, typeof EXTENDED_LOCK_EVENT>
  | Log<bigint, number, false, typeof UNLOCKED_EVENT>

type CrownRoundStorage = [
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

function normalizeAddress(address: string | undefined, label = 'address') {
  if (!address) {
    throw new Error(`Missing ${label} in indexed event log`)
  }

  return getAddress(address)
}

function isRangeLimitErrorMessage(message: string) {
  return (
    message.includes('eth_getLogs is limited to a 10,000 range') ||
    message.includes('limited to a 10000 range') ||
    message.includes('limited to 0 - 10000 blocks range') ||
    message.includes('query returned more than') ||
    message.includes('block range is too wide') ||
    message.includes('range limit')
  )
}

function isRetryableRpcErrorMessage(message: string) {
  const m = message.toLowerCase()
  return (
    m.includes('fetch failed') ||
    m.includes('http request failed') ||
    m.includes('timeout') ||
    m.includes('timed out') ||
    m.includes('socket hang up') ||
    m.includes('econnreset') ||
    m.includes('etimedout') ||
    m.includes('429') ||
    m.includes('too many requests') ||
    m.includes('rate limit') ||
    m.includes('quota') ||
    m.includes('credits') ||
    m.includes('resource not found') ||
    m.includes('gateway timeout') ||
    m.includes('expected double-quoted property name') ||
    m.includes('unexpected token') ||
    m.includes('syntaxerror')
  )
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function withRpcRetries<T>(loader: () => Promise<T>): Promise<T> {
  let lastError: unknown

  for (let attempt = 0; attempt < RPC_RETRY_ATTEMPTS; attempt += 1) {
    try {
      return await loader()
    } catch (error) {
      lastError = error
      const message = error instanceof Error ? error.message : String(error)
      const isLastAttempt = attempt >= RPC_RETRY_ATTEMPTS - 1

      if (!isRetryableRpcErrorMessage(message) || isLastAttempt) {
        throw error
      }

      await sleep(150 * (attempt + 1))
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError))
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
      const index = nextIndex
      nextIndex += 1
      results[index] = await mapper(items[index], index)
    }
  })

  await Promise.all(workers)
  return results
}

async function getBlockTimestampMs(blockNumber: bigint) {
  const cached = blockTimestampCache.get(blockNumber)
  if (cached) return cached

  const block = await withRpcRetries(() => publicClient.getBlock({ blockNumber }))
  const timestampMs = Number(block.timestamp) * 1000
  blockTimestampCache.set(blockNumber, timestampMs)
  if (blockTimestampCache.size > 20_000) {
    const oldest = blockTimestampCache.keys().next().value
    if (oldest !== undefined) blockTimestampCache.delete(oldest)
  }
  return timestampMs
}

async function fetchLogsRange<TEvent extends AbiEvent | undefined>(
  params: {
    address: Address
    event: TEvent
    args?: Record<string, unknown>
  },
  fromBlock: bigint,
  toBlock: bigint
): Promise<Log<bigint, number, false, TEvent>[]> {
  try {
    let logs: Log<bigint, number, false, TEvent>[] | null = null

    for (let attempt = 0; attempt < RPC_RETRY_ATTEMPTS; attempt += 1) {
      try {
        logs = await publicClient.getLogs({
          address: params.address,
          event: params.event as never,
          args: params.args as never,
          fromBlock,
          toBlock,
        }) as Log<bigint, number, false, TEvent>[]
        break
      } catch (rpcError) {
        const rpcMessage = rpcError instanceof Error ? rpcError.message : String(rpcError)
        const isRangeLimitError = isRangeLimitErrorMessage(rpcMessage)
        const isRetryableRpcError = isRetryableRpcErrorMessage(rpcMessage)
        const isLastAttempt = attempt >= RPC_RETRY_ATTEMPTS - 1

        if (isRangeLimitError || !isRetryableRpcError || isLastAttempt) {
          throw rpcError
        }

        await sleep(120 * (attempt + 1))
      }
    }

    if (!logs) {
      throw new Error('Failed to fetch logs after retries')
    }

    return logs
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    const isRangeLimitError = isRangeLimitErrorMessage(message)
    const blockSpan = toBlock - fromBlock + 1n

    if (!isRangeLimitError || blockSpan <= MIN_LOG_BLOCK_RANGE) {
      throw error
    }

    const mid = fromBlock + (blockSpan / 2n) - 1n
    const leftToBlock = mid < fromBlock ? fromBlock : mid
    const rightFromBlock = leftToBlock + 1n
    const [left, right] = await Promise.all([
      fetchLogsRange(params, fromBlock, leftToBlock),
      rightFromBlock <= toBlock
        ? fetchLogsRange(params, rightFromBlock, toBlock)
        : Promise.resolve([] as Log<bigint, number, false, TEvent>[]),
    ])

    return [...left, ...right]
  }
}

async function getLogsPaged<TEvent extends AbiEvent | undefined>(
  params: {
    address: Address
    event: TEvent
    args?: Record<string, unknown>
    fromBlock: bigint
    toBlock?: bigint | 'latest'
  }
) {
  const latestBlock = params.toBlock === 'latest' || params.toBlock === undefined
    ? await withRpcRetries(() => publicClient.getBlockNumber())
    : params.toBlock
  const startBlock = params.fromBlock

  if (startBlock > latestBlock) {
    return [] as Log<bigint, number, false, TEvent>[]
  }

  const ranges: Array<{ fromBlock: bigint; toBlock: bigint }> = []
  for (let cursor = startBlock; cursor <= latestBlock; cursor += LOG_BLOCK_RANGE) {
    const endBlock = cursor + LOG_BLOCK_RANGE - 1n > latestBlock
      ? latestBlock
      : cursor + LOG_BLOCK_RANGE - 1n
    ranges.push({ fromBlock: cursor, toBlock: endBlock })
  }

  const chunks = await mapWithConcurrency(ranges, LOG_FETCH_CONCURRENCY, async ({ fromBlock, toBlock }) => {
    return fetchLogsRange(params, fromBlock, toBlock)
  })

  return chunks.flat()
}

async function getLastSyncedBlock(client: PoolClient, streamName: string, startBlock = env.scanStartBlock) {
  const result = await client.query<{ last_synced_block: string }>(
    'select last_synced_block::text from protocol_sync_state where stream_name = $1',
    [streamName]
  )

  if (!result.rowCount) {
    return startBlock > 0n ? startBlock - 1n : 0n
  }

  return BigInt(result.rows[0].last_synced_block)
}

async function updateSyncState(client: PoolClient, streamName: string, lastSyncedBlock: bigint) {
  await client.query(
    `
      insert into protocol_sync_state (stream_name, last_synced_block, updated_at)
      values ($1, $2, now())
      on conflict (stream_name) do update
      set last_synced_block = excluded.last_synced_block,
          updated_at = excluded.updated_at
    `,
    [streamName, lastSyncedBlock.toString()]
  )
}

async function upsertRows<T>(
  items: T[],
  concurrency: number,
  inserter: (item: T) => Promise<void>
) {
  await mapWithConcurrency(items, concurrency, async (item) => {
    await inserter(item)
    return null
  })
}

async function runWriteQuery(query: string, values: unknown[]) {
  const pool = getProtocolIndexPool()
  await pool.query(query, values)
}

async function getLatestUpdatedAtMs(client: PoolClient, tableName: 'protocol_treasury_agent_leaderboard' | 'protocol_treasury_agent_holdings') {
  const result = await client.query<{ updated_at_ms: string | null }>(
    `
      select floor(extract(epoch from max(updated_at)) * 1000)::bigint::text as updated_at_ms
      from ${tableName}
    `
  )

  const raw = result.rows[0]?.updated_at_ms
  return raw ? Number(raw) : 0
}

async function shouldRefreshTreasurySnapshot(
  client: PoolClient,
  tableName: 'protocol_treasury_agent_leaderboard' | 'protocol_treasury_agent_holdings'
) {
  const latestUpdatedAtMs = await getLatestUpdatedAtMs(client, tableName)
  if (!latestUpdatedAtMs) {
    return true
  }

  return Date.now() - latestUpdatedAtMs >= env.treasuryAgentSnapshotSyncIntervalMs
}

async function syncDeployments(client: PoolClient, eventName: 'Deployed' | 'DeployedFor', latestBlock: bigint) {
  const streamName = eventName === 'Deployed' ? 'deployments_direct' : 'deployments_for'
  const event = eventName === 'Deployed' ? DEPLOYED_EVENT : DEPLOYED_FOR_EVENT
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n

  if (fromBlock > latestBlock) {
    return { streamName, inserted: 0, latestBlock }
  }

  const logs = await getLogsPaged({
    address: CONTRACTS.gridMining,
    event,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_deployments (
          tx_hash, log_index, event_name, round_id, user_address, executor_address,
          amount_per_block, block_mask, total_amount, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,to_timestamp($11 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            round_id = excluded.round_id,
            user_address = excluded.user_address,
            executor_address = excluded.executor_address,
            amount_per_block = excluded.amount_per_block,
            block_mask = excluded.block_mask,
            total_amount = excluded.total_amount,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        eventName,
        toBigInt(log.args.roundId).toString(),
        normalizeAddress(log.args.user, 'deployment user'),
        log.eventName === 'DeployedFor' ? normalizeAddress(log.args.executor, 'deployment executor') : null,
        toBigInt(log.args.amountPerBlock).toString(),
        toBigInt(log.args.blockMask).toString(),
        toBigInt(log.args.totalAmount).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncRoundSettled(client: PoolClient, latestBlock: bigint) {
  const streamName = 'rounds'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n

  if (fromBlock > latestBlock) {
    return { streamName, inserted: 0, latestBlock }
  }

  const logs = await getLogsPaged({
    address: CONTRACTS.gridMining,
    event: ROUND_SETTLED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROUND_READ_CONCURRENCY, async (log) => {
    const roundId = toBigInt(log.args.roundId)
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const roundStruct = await withRpcRetries(() => publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'rounds',
      args: [roundId],
    }) as Promise<[bigint, bigint, bigint, bigint, bigint, number, Address, bigint, bigint, bigint, bigint, boolean, bigint]>)

    await runWriteQuery(
      `
        insert into protocol_rounds (
          round_id, start_time, end_time, total_deployed, total_winnings, winners_deployed,
          winning_block, top_miner, top_miner_reward, lootpot_amount, vrf_request_id,
          top_miner_seed, settled, miner_count, is_split, settled_block_number,
          settled_tx_hash, settled_at, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,to_timestamp($18 / 1000.0),now())
        on conflict (round_id) do update
        set start_time = excluded.start_time,
            end_time = excluded.end_time,
            total_deployed = excluded.total_deployed,
            total_winnings = excluded.total_winnings,
            winners_deployed = excluded.winners_deployed,
            winning_block = excluded.winning_block,
            top_miner = excluded.top_miner,
            top_miner_reward = excluded.top_miner_reward,
            lootpot_amount = excluded.lootpot_amount,
            vrf_request_id = excluded.vrf_request_id,
            top_miner_seed = excluded.top_miner_seed,
            settled = excluded.settled,
            miner_count = excluded.miner_count,
            is_split = excluded.is_split,
            settled_block_number = excluded.settled_block_number,
            settled_tx_hash = excluded.settled_tx_hash,
            settled_at = excluded.settled_at,
            updated_at = excluded.updated_at
      `,
      [
        roundId.toString(),
        toBigInt(roundStruct[0]).toString(),
        toBigInt(roundStruct[1]).toString(),
        toBigInt(roundStruct[2]).toString(),
        toBigInt(roundStruct[3]).toString(),
        toBigInt(roundStruct[4]).toString(),
        roundStruct[5],
        normalizeAddress(roundStruct[6]),
        toBigInt(roundStruct[7]).toString(),
        toBigInt(roundStruct[8]).toString(),
        toBigInt(roundStruct[9]).toString(),
        toBigInt(roundStruct[10]).toString(),
        roundStruct[11],
        toBigInt(roundStruct[12]).toString(),
        Boolean(log.args.isSplit ?? false),
        log.blockNumber.toString(),
        log.transactionHash,
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCheckpoints(client: PoolClient, latestBlock: bigint) {
  const streamName = 'checkpoints'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.gridMining,
    event: CHECKPOINTED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_checkpoints (
          tx_hash, log_index, round_id, user_address, eth_reward, loot_reward, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set round_id = excluded.round_id,
            user_address = excluded.user_address,
            eth_reward = excluded.eth_reward,
            loot_reward = excluded.loot_reward,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        toBigInt(log.args.roundId).toString(),
        normalizeAddress(log.args.user, 'checkpoint user'),
        toBigInt(log.args.ethReward).toString(),
        toBigInt(log.args.lootReward).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncClaimedLoot(client: PoolClient, latestBlock: bigint) {
  const streamName = 'claimed_loot'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.gridMining,
    event: CLAIMED_LOOT_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_claimed_loot (
          tx_hash, log_index, user_address, mined_loot, forged_loot, fee, net, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,to_timestamp($9 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set user_address = excluded.user_address,
            mined_loot = excluded.mined_loot,
            forged_loot = excluded.forged_loot,
            fee = excluded.fee,
            net = excluded.net,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        normalizeAddress(log.args.user, 'claimed loot user'),
        toBigInt(log.args.minedLoot).toString(),
        toBigInt(log.args.forgedLoot).toString(),
        toBigInt(log.args.fee).toString(),
        toBigInt(log.args.net).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function upsertCrownRoundSnapshot(
  roundId: bigint,
  options?: {
    activatedBlockNumber?: bigint | null
    settledBlockNumber?: bigint | null
    settledTxHash?: string | null
    settledAtMs?: number | null
  }
) {
  const round = await withRpcRetries(() => publicClient.readContract({
    address: CONTRACTS.crown,
    abi: crownAbi,
    functionName: 'rounds',
    args: [roundId],
  }) as Promise<CrownRoundStorage>)

  await runWriteQuery(
    `
      insert into crown_rounds (
        round_id, start_time, end_time, next_roll_at, total_sold, prize_pool,
        acc_dividend_per_chest, holder_count, vrf_request_id, vrf_requested_at,
        winning_roll, current_leader, leader_snapshot, winner, active, settled,
        vrf_pending, activated_block_number, settled_block_number, settled_tx_hash,
        settled_at, updated_at
      )
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,now())
      on conflict (round_id) do update
      set start_time = excluded.start_time,
          end_time = excluded.end_time,
          next_roll_at = excluded.next_roll_at,
          total_sold = excluded.total_sold,
          prize_pool = excluded.prize_pool,
          acc_dividend_per_chest = excluded.acc_dividend_per_chest,
          holder_count = excluded.holder_count,
          vrf_request_id = excluded.vrf_request_id,
          vrf_requested_at = excluded.vrf_requested_at,
          winning_roll = excluded.winning_roll,
          current_leader = excluded.current_leader,
          leader_snapshot = excluded.leader_snapshot,
          winner = excluded.winner,
          active = excluded.active,
          settled = excluded.settled,
          vrf_pending = excluded.vrf_pending,
          activated_block_number = coalesce(excluded.activated_block_number, crown_rounds.activated_block_number),
          settled_block_number = coalesce(excluded.settled_block_number, crown_rounds.settled_block_number),
          settled_tx_hash = coalesce(excluded.settled_tx_hash, crown_rounds.settled_tx_hash),
          settled_at = coalesce(excluded.settled_at, crown_rounds.settled_at),
          updated_at = excluded.updated_at
    `,
    [
      roundId.toString(),
      toBigInt(round[0]).toString(),
      toBigInt(round[1]).toString(),
      toBigInt(round[2]).toString(),
      toBigInt(round[3]).toString(),
      toBigInt(round[4]).toString(),
      toBigInt(round[5]).toString(),
      toBigInt(round[6]).toString(),
      toBigInt(round[7]).toString(),
      toBigInt(round[8]).toString(),
      toBigInt(round[9]).toString(),
      normalizeAddress(round[10]),
      normalizeAddress(round[11]),
      normalizeAddress(round[12]),
      round[13],
      round[14],
      round[15],
      options?.activatedBlockNumber?.toString() ?? null,
      options?.settledBlockNumber?.toString() ?? null,
      options?.settledTxHash ?? null,
      options?.settledAtMs ? new Date(options.settledAtMs).toISOString() : null,
    ]
  )
}

async function syncCrownRoundActivated(client: PoolClient, latestBlock: bigint) {
  const streamName = 'crown_round_activated'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event: CROWN_ROUND_ACTIVATED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    await upsertCrownRoundSnapshot(toBigInt(log.args.roundId), {
      activatedBlockNumber: log.blockNumber,
    })
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCrownPurchases(client: PoolClient, latestBlock: bigint) {
  const streamName = 'crown_purchases'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event: CROWN_CHEST_PURCHASED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })
  const affectedRoundIds = new Set<string>()

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const roundId = toBigInt(log.args.roundId)
    affectedRoundIds.add(roundId.toString())
    await runWriteQuery(
      `
        insert into crown_purchases (
          tx_hash, log_index, round_id, user_address, price, total_sold_after,
          leader, prize_amount, dividend_amount, buyback_amount, lock_amount,
          admin_amount, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,to_timestamp($14 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set round_id = excluded.round_id,
            user_address = excluded.user_address,
            price = excluded.price,
            total_sold_after = excluded.total_sold_after,
            leader = excluded.leader,
            prize_amount = excluded.prize_amount,
            dividend_amount = excluded.dividend_amount,
            buyback_amount = excluded.buyback_amount,
            lock_amount = excluded.lock_amount,
            admin_amount = excluded.admin_amount,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        roundId.toString(),
        normalizeAddress(log.args.user, 'crown purchase user'),
        toBigInt(log.args.price).toString(),
        toBigInt(log.args.totalSold).toString(),
        normalizeAddress(log.args.leader, 'crown purchase leader'),
        toBigInt(log.args.prizeAmount).toString(),
        toBigInt(log.args.dividendAmount).toString(),
        toBigInt(log.args.buybackAmount).toString(),
        toBigInt(log.args.lockAmount).toString(),
        toBigInt(log.args.adminAmount).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await mapWithConcurrency([...affectedRoundIds], ROUND_READ_CONCURRENCY, async (roundId) => {
    await upsertCrownRoundSnapshot(BigInt(roundId))
    return null
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCrownBatchPurchases(client: PoolClient, latestBlock: bigint) {
  const streamName = 'crown_batch_purchases'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event: CROWN_CHESTS_PURCHASED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into crown_batch_purchases (
          tx_hash, log_index, round_id, user_address, amount, total_price,
          total_sold_after, leader, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($10 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set round_id = excluded.round_id,
            user_address = excluded.user_address,
            amount = excluded.amount,
            total_price = excluded.total_price,
            total_sold_after = excluded.total_sold_after,
            leader = excluded.leader,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        toBigInt(log.args.roundId).toString(),
        normalizeAddress(log.args.user, 'crown batch user'),
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.totalPrice).toString(),
        toBigInt(log.args.totalSold).toString(),
        normalizeAddress(log.args.leader, 'crown batch leader'),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCrownRollRequested(client: PoolClient, latestBlock: bigint) {
  const streamName = 'crown_roll_requested'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event: CROWN_ROLL_REQUESTED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const roundId = toBigInt(log.args.roundId)
    await runWriteQuery(
      `
        insert into crown_roll_events (
          tx_hash, log_index, event_name, round_id, request_id, leader_snapshot,
          roll, next_roll_at, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($10 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            round_id = excluded.round_id,
            request_id = excluded.request_id,
            leader_snapshot = excluded.leader_snapshot,
            next_roll_at = excluded.next_roll_at,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        'RollRequested',
        roundId.toString(),
        toBigInt(log.args.requestId).toString(),
        normalizeAddress(log.args.leaderSnapshot, 'crown roll leader snapshot'),
        null,
        toBigInt(log.args.nextRollAt).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
    await upsertCrownRoundSnapshot(roundId)
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCrownRollResolved(client: PoolClient, latestBlock: bigint) {
  const streamName = 'crown_roll_resolved'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event: CROWN_ROLL_RESOLVED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const roundId = toBigInt(log.args.roundId)
    await runWriteQuery(
      `
        insert into crown_roll_events (
          tx_hash, log_index, event_name, round_id, request_id, leader_snapshot,
          roll, next_roll_at, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($10 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            round_id = excluded.round_id,
            roll = excluded.roll,
            next_roll_at = excluded.next_roll_at,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        'RollResolved',
        roundId.toString(),
        null,
        null,
        toBigInt(log.args.roll).toString(),
        toBigInt(log.args.nextRollAt).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
    await upsertCrownRoundSnapshot(roundId)
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCrownRoundSettled(client: PoolClient, latestBlock: bigint) {
  const streamName = 'crown_round_settled'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event: CROWN_ROUND_SETTLED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const roundId = toBigInt(log.args.roundId)
    await runWriteQuery(
      `
        insert into crown_roll_events (
          tx_hash, log_index, event_name, round_id, request_id, leader_snapshot,
          roll, next_roll_at, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($10 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            round_id = excluded.round_id,
            roll = excluded.roll,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        'RoundSettled',
        roundId.toString(),
        null,
        normalizeAddress(log.args.winner, 'crown winner'),
        toBigInt(log.args.roll).toString(),
        null,
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
    await upsertCrownRoundSnapshot(roundId, {
      settledBlockNumber: log.blockNumber,
      settledTxHash: log.transactionHash,
      settledAtMs: timestampMs,
    })
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncCrownClaims(client: PoolClient, eventName: 'ClaimedDividends' | 'ClaimedPrize', latestBlock: bigint) {
  const streamName = eventName === 'ClaimedDividends' ? 'crown_claimed_dividends' : 'crown_claimed_prize'
  const event = eventName === 'ClaimedDividends' ? CROWN_CLAIMED_DIVIDENDS_EVENT : CROWN_CLAIMED_PRIZE_EVENT
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.crown,
    event,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into crown_claims (
          tx_hash, log_index, event_name, user_address, amount, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,to_timestamp($7 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            user_address = excluded.user_address,
            amount = excluded.amount,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        eventName,
        normalizeAddress(log.args.user, 'crown claim user'),
        toBigInt(log.args.amount).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncAutoCrownConfigUpdated(client: PoolClient, latestBlock: bigint) {
  const streamName = 'autocrown_config_updated'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.autoCrown,
    event: AUTOCROWN_CONFIG_UPDATED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into autocrown_configs (
          user_address, active, open_new_round, defend_lead, snipe_when_outbid,
          buy_window_seconds, max_buys_per_tick, max_buys_per_round,
          max_build_price, max_battle_price, min_prize_pool, target_chests,
          max_round_spend, total_budget, block_number, tx_hash, updated_at
        )
        values ($1,true,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,to_timestamp($16 / 1000.0))
        on conflict (user_address) do update
        set active = excluded.active,
            open_new_round = excluded.open_new_round,
            defend_lead = excluded.defend_lead,
            snipe_when_outbid = excluded.snipe_when_outbid,
            buy_window_seconds = excluded.buy_window_seconds,
            max_buys_per_tick = excluded.max_buys_per_tick,
            max_buys_per_round = excluded.max_buys_per_round,
            max_build_price = excluded.max_build_price,
            max_battle_price = excluded.max_battle_price,
            min_prize_pool = excluded.min_prize_pool,
            target_chests = excluded.target_chests,
            max_round_spend = excluded.max_round_spend,
            total_budget = excluded.total_budget,
            block_number = excluded.block_number,
            tx_hash = excluded.tx_hash,
            updated_at = excluded.updated_at
      `,
      [
        normalizeAddress(log.args.user, 'autocrown config user'),
        Boolean(log.args.openNewRound),
        Boolean(log.args.defendLead),
        Boolean(log.args.snipeWhenOutbid),
        Number(toBigInt(log.args.buyWindowSeconds)),
        Number(toBigInt(log.args.maxBuysPerTick)),
        Number(toBigInt(log.args.maxBuysPerRound)),
        toBigInt(log.args.maxBuildPrice).toString(),
        toBigInt(log.args.maxBattlePrice).toString(),
        toBigInt(log.args.minPrizePool).toString(),
        toBigInt(log.args.targetChests).toString(),
        toBigInt(log.args.maxRoundSpend).toString(),
        toBigInt(log.args.totalBudget).toString(),
        log.blockNumber.toString(),
        log.transactionHash,
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncAutoCrownDeposits(client: PoolClient, latestBlock: bigint) {
  const streamName = 'autocrown_deposits'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.autoCrown,
    event: AUTOCROWN_DEPOSIT_ADDED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into autocrown_deposits (
          tx_hash, log_index, user_address, amount, new_balance, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,to_timestamp($7 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set user_address = excluded.user_address,
            amount = excluded.amount,
            new_balance = excluded.new_balance,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        normalizeAddress(log.args.user, 'autocrown deposit user'),
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.newBalance).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncAutoCrownExecutedFor(client: PoolClient, eventName: 'ExecutedFor' | 'BatchExecutedFor', latestBlock: bigint) {
  const isBatch = eventName === 'BatchExecutedFor'
  const streamName = isBatch ? 'autocrown_batch_executed_for' : 'autocrown_executed_for'
  const event = isBatch ? AUTOCROWN_BATCH_EXECUTED_FOR_EVENT : AUTOCROWN_EXECUTED_FOR_EVENT
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.autoCrown,
    event,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const args = log.args as Record<string, unknown>
    await runWriteQuery(
      `
        insert into autocrown_executions (
          tx_hash, log_index, event_name, user_address, round_id, amount,
          total_price, executor_fee, battle_phase, deposit_balance,
          block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,to_timestamp($12 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            user_address = excluded.user_address,
            round_id = excluded.round_id,
            amount = excluded.amount,
            total_price = excluded.total_price,
            executor_fee = excluded.executor_fee,
            battle_phase = excluded.battle_phase,
            deposit_balance = excluded.deposit_balance,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        eventName,
        normalizeAddress(args.user as string | undefined, 'autocrown execution user'),
        toBigInt(args.roundId).toString(),
        isBatch ? toBigInt(args.amount).toString() : '1',
        isBatch ? toBigInt(args.totalPrice).toString() : toBigInt(args.price).toString(),
        toBigInt(args.executorFee).toString(),
        Boolean(args.battlePhase),
        toBigInt(args.depositBalance).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncAutoCrownStops(client: PoolClient, latestBlock: bigint) {
  const streamName = 'autocrown_stops'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName, env.crownScanStartBlock)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.autoCrown,
    event: AUTOCROWN_STOPPED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    const user = normalizeAddress(log.args.user, 'autocrown stopped user')
    await runWriteQuery(
      `
        insert into autocrown_stops (
          tx_hash, log_index, user_address, refunded, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,to_timestamp($6 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set user_address = excluded.user_address,
            refunded = excluded.refunded,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        user,
        toBigInt(log.args.refunded).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
    await runWriteQuery(
      `
        update autocrown_configs
        set active = false,
            block_number = $2,
            tx_hash = $3,
            updated_at = to_timestamp($4 / 1000.0)
        where user_address = $1
      `,
      [user, log.blockNumber.toString(), log.transactionHash, timestampMs]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncVaultEvents(client: PoolClient, latestBlock: bigint) {
  const streamName = 'treasury_vault'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.treasury,
    event: VAULT_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_treasury_vault_events (
          tx_hash, log_index, amount, total_vaulted, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,to_timestamp($6 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set amount = excluded.amount,
            total_vaulted = excluded.total_vaulted,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.totalVaulted).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncBuybacks(client: PoolClient, latestBlock: bigint) {
  const streamName = 'treasury_buybacks'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.treasury,
    event: BUYBACK_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_treasury_buybacks (
          tx_hash, log_index, eth_spent, loot_received, loot_burned, loot_to_stakers, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set eth_spent = excluded.eth_spent,
            loot_received = excluded.loot_received,
            loot_burned = excluded.loot_burned,
            loot_to_stakers = excluded.loot_to_stakers,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        toBigInt(log.args.ethSpent).toString(),
        toBigInt(log.args.lootReceived).toString(),
        toBigInt(log.args.lootBurned).toString(),
        toBigInt(log.args.lootToStakers).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncDirectBurns(client: PoolClient, latestBlock: bigint) {
  const streamName = 'direct_burns'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.loot,
    event: TRANSFER_EVENT,
    args: { to: ZERO_ADDRESS },
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_direct_burns (
          tx_hash, log_index, from_address, value, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,to_timestamp($6 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set from_address = excluded.from_address,
            value = excluded.value,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        normalizeAddress(log.args.from, 'direct burn sender'),
        toBigInt(log.args.value).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncStakeDeposits(client: PoolClient, latestBlock: bigint) {
  const streamName = 'staking_deposits'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.staking,
    event: STAKE_DEPOSIT_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_staking_deposits (
          tx_hash, log_index, user_address, amount, new_balance, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,to_timestamp($7 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set user_address = excluded.user_address,
            amount = excluded.amount,
            new_balance = excluded.new_balance,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        normalizeAddress(log.args.user, 'staking deposit user'),
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.newBalance).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncStakeWithdrawals(client: PoolClient, latestBlock: bigint) {
  const streamName = 'staking_withdrawals'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.staking,
    event: STAKE_WITHDRAW_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_staking_withdrawals (
          tx_hash, log_index, user_address, amount, new_balance, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,to_timestamp($7 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set user_address = excluded.user_address,
            amount = excluded.amount,
            new_balance = excluded.new_balance,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        normalizeAddress(log.args.user, 'staking withdrawal user'),
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.newBalance).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncStakeCompounds(client: PoolClient, latestBlock: bigint) {
  const streamName = 'staking_compounds'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.staking,
    event: STAKE_COMPOUND_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_staking_compounds (
          tx_hash, log_index, user_address, compounder_address, amount, fee, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set user_address = excluded.user_address,
            compounder_address = excluded.compounder_address,
            amount = excluded.amount,
            fee = excluded.fee,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        normalizeAddress(log.args.user, 'staking compound user'),
        normalizeAddress(log.args.compounder, 'staking compounder'),
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.fee).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncYieldDistributions(client: PoolClient, latestBlock: bigint) {
  const streamName = 'staking_yield_distributions'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.staking,
    event: YIELD_DISTRIBUTED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_staking_yield_distributions (
          tx_hash, log_index, amount, new_acc_yield_per_share, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,to_timestamp($6 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set amount = excluded.amount,
            new_acc_yield_per_share = excluded.new_acc_yield_per_share,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.newAccYieldPerShare).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncLockRewards(client: PoolClient, latestBlock: bigint) {
  const streamName = 'lock_reward_notified'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.lockerRewards,
    event: LOCK_REWARD_NOTIFIED_EVENT,
    fromBlock,
    toBlock: latestBlock,
  })

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)
    await runWriteQuery(
      `
        insert into protocol_lock_reward_notified (
          tx_hash, log_index, amount, distributed_amount, unallocated_amount,
          acc_reward_per_weight, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set amount = excluded.amount,
            distributed_amount = excluded.distributed_amount,
            unallocated_amount = excluded.unallocated_amount,
            acc_reward_per_weight = excluded.acc_reward_per_weight,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        toBigInt(log.args.amount).toString(),
        toBigInt(log.args.distributedAmount).toString(),
        toBigInt(log.args.unallocatedAmount).toString(),
        toBigInt(log.args.accRewardPerWeight).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncLockerState(client: PoolClient, streamName: string, logs: LockerStateLog[]) {
  if (logs.length === 0) {
    return { streamName, inserted: 0 }
  }

  await upsertRows(logs, ROW_UPSERT_CONCURRENCY, async (log) => {
    const timestampMs = await getBlockTimestampMs(log.blockNumber)

    let amountDelta = 0n
    let unlockTime: bigint | null = null
    let durationId: number | null = null
    let newUserWeight: bigint | null = null
    let newTotalWeight: bigint | null = null

    if (log.eventName === 'Locked') {
      amountDelta = toBigInt(log.args.amount)
      unlockTime = toBigInt(log.args.unlockTime)
      durationId = Number(log.args.durationId)
      newUserWeight = toBigInt(log.args.newUserWeight)
      newTotalWeight = toBigInt(log.args.newTotalWeight)
    } else if (log.eventName === 'AddedToLock') {
      amountDelta = toBigInt(log.args.amountAdded)
      unlockTime = toBigInt(log.args.newUnlockTime)
      newUserWeight = toBigInt(log.args.newUserWeight)
      newTotalWeight = toBigInt(log.args.newTotalWeight)
    } else if (log.eventName === 'Extended') {
      amountDelta = 0n
      unlockTime = toBigInt(log.args.newUnlockTime)
      durationId = Number(log.args.newDurationId)
      newUserWeight = toBigInt(log.args.newUserWeight)
      newTotalWeight = toBigInt(log.args.newTotalWeight)
    } else if (log.eventName === 'Unlocked') {
      amountDelta = -toBigInt(log.args.amount)
      newUserWeight = toBigInt(log.args.newUserWeight)
      newTotalWeight = toBigInt(log.args.newTotalWeight)
    }

    await runWriteQuery(
      `
        insert into protocol_locker_events (
          tx_hash, log_index, event_name, user_address, lock_id, amount_delta,
          unlock_time, duration_id, new_user_weight, new_total_weight, block_number, block_timestamp
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,to_timestamp($12 / 1000.0))
        on conflict (tx_hash, log_index) do update
        set event_name = excluded.event_name,
            user_address = excluded.user_address,
            lock_id = excluded.lock_id,
            amount_delta = excluded.amount_delta,
            unlock_time = excluded.unlock_time,
            duration_id = excluded.duration_id,
            new_user_weight = excluded.new_user_weight,
            new_total_weight = excluded.new_total_weight,
            block_number = excluded.block_number,
            block_timestamp = excluded.block_timestamp
      `,
      [
        log.transactionHash,
        log.logIndex,
        log.eventName,
        normalizeAddress(log.args.user, 'locker event user'),
        toBigInt(log.args.lockId).toString(),
        amountDelta.toString(),
        unlockTime?.toString() ?? null,
        durationId,
        newUserWeight?.toString() ?? null,
        newTotalWeight?.toString() ?? null,
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  return { streamName, inserted: logs.length }
}

async function syncLockerStateEvent(
  client: PoolClient,
  streamName: string,
  event: typeof LOCKED_EVENT | typeof ADDED_TO_LOCK_EVENT | typeof EXTENDED_LOCK_EVENT | typeof UNLOCKED_EVENT,
  latestBlock: bigint
) {
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const fromBlock = lastSyncedBlock + 1n
  if (fromBlock > latestBlock) return { streamName, inserted: 0, latestBlock }

  const logs = await getLogsPaged({
    address: CONTRACTS.lootLocker,
    event,
    fromBlock,
    toBlock: latestBlock,
  })

  const result = await syncLockerState(client, streamName, logs as LockerStateLog[])
  await updateSyncState(client, streamName, latestBlock)
  return { ...result, latestBlock }
}

async function syncTreasuryAgentLeaderboardSnapshot(client: PoolClient, latestBlock: bigint) {
  const streamName = 'treasury_agent_leaderboard'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  if (latestBlock <= lastSyncedBlock) {
    return { streamName, inserted: 0, latestBlock }
  }

  const shouldRefresh = await shouldRefreshTreasurySnapshot(client, 'protocol_treasury_agent_leaderboard')
  if (!shouldRefresh) {
    return { streamName, inserted: 0, latestBlock: lastSyncedBlock }
  }

  const payload = await getTreasuryAgentLeaderboard(500)

  await runWriteQuery('delete from protocol_treasury_agent_leaderboard', [])

  await upsertRows(payload.entries, ROW_UPSERT_CONCURRENCY, async (entry) => {
    await runWriteQuery(
      `
        insert into protocol_treasury_agent_leaderboard (
          user_address, rank, deposited, pending, rewards, snapshot_block, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,now())
        on conflict (user_address) do update
        set rank = excluded.rank,
            deposited = excluded.deposited,
            pending = excluded.pending,
            rewards = excluded.rewards,
            snapshot_block = excluded.snapshot_block,
            updated_at = excluded.updated_at
      `,
      [
        entry.address,
        entry.rank,
        entry.depositedFormatted,
        entry.pendingFormatted,
        entry.rewardsFormatted,
        latestBlock.toString(),
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: payload.entries.length, latestBlock }
}

async function syncTreasuryAgentHoldingsSnapshot(client: PoolClient, latestBlock: bigint) {
  const streamName = 'treasury_agent_holdings'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  if (latestBlock <= lastSyncedBlock) {
    return { streamName, inserted: 0, latestBlock }
  }

  const shouldRefresh = await shouldRefreshTreasurySnapshot(client, 'protocol_treasury_agent_holdings')
  if (!shouldRefresh) {
    return { streamName, inserted: 0, latestBlock: lastSyncedBlock }
  }

  const payload = await getTreasuryAgentHoldings()

  if (payload.walletAddress) {
    await runWriteQuery(
      'delete from protocol_treasury_agent_holdings where wallet_address = $1',
      [payload.walletAddress]
    )
  } else {
    await runWriteQuery('delete from protocol_treasury_agent_holdings', [])
  }

  await upsertRows(payload.entries, ROW_UPSERT_CONCURRENCY, async (entry) => {
    await runWriteQuery(
      `
        insert into protocol_treasury_agent_holdings (
          wallet_address, token_key, token_address, symbol, name, protocol, location_label,
          balance, balance_formatted, usd_value, usd_value_formatted, allocation, decimals, logo_url, coingecko_url,
          is_native, snapshot_block, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,now())
        on conflict (wallet_address, token_key) do update
        set token_address = excluded.token_address,
            symbol = excluded.symbol,
            name = excluded.name,
            protocol = excluded.protocol,
            location_label = excluded.location_label,
            balance = excluded.balance,
            balance_formatted = excluded.balance_formatted,
            usd_value = excluded.usd_value,
            usd_value_formatted = excluded.usd_value_formatted,
            allocation = excluded.allocation,
            decimals = excluded.decimals,
            logo_url = excluded.logo_url,
            coingecko_url = excluded.coingecko_url,
            is_native = excluded.is_native,
            snapshot_block = excluded.snapshot_block,
            updated_at = excluded.updated_at
      `,
      [
        payload.walletAddress,
        entry.tokenKey,
        entry.address,
        entry.symbol,
        entry.name,
        entry.protocol,
        entry.locationLabel,
        entry.balance,
        entry.balanceFormatted,
        entry.usdValue,
        entry.usdValueFormatted,
        entry.allocation,
        entry.decimals,
        entry.logoUrl,
        entry.coingeckoUrl,
        entry.isNative,
        latestBlock.toString(),
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: payload.entries.length, latestBlock }
}

export async function runProtocolIndexSyncOnce(options?: {
  logger?: Logger
}) {
  const logger = options?.logger ?? console
  await initProtocolIndexSchema()

  const pool = getProtocolIndexPool()
  const client = await pool.connect()

  try {
    const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
    const results: Array<{ streamName: string; inserted: number; latestBlock: bigint }> = []
    const runStream = async (
      label: string,
      syncer: () => Promise<{ streamName: string; inserted: number; latestBlock: bigint }>
    ) => {
      const startedAt = Date.now()
      logger.info(`[protocol-indexer] syncing ${label}...`)
      const result = await syncer()
      logger.info(`[protocol-indexer] synced ${label}`, {
        inserted: result.inserted,
        latestBlock: result.latestBlock.toString(),
        durationMs: Date.now() - startedAt,
      })
      results.push(result)
    }

    await runStream('rounds', () => syncRoundSettled(client, latestBlock))
    await runStream('deployments_direct', () => syncDeployments(client, 'Deployed', latestBlock))
    await runStream('deployments_for', () => syncDeployments(client, 'DeployedFor', latestBlock))
    await runStream('checkpoints', () => syncCheckpoints(client, latestBlock))
    await runStream('claimed_loot', () => syncClaimedLoot(client, latestBlock))
    await runStream('crown_round_activated', () => syncCrownRoundActivated(client, latestBlock))
    await runStream('crown_purchases', () => syncCrownPurchases(client, latestBlock))
    await runStream('crown_batch_purchases', () => syncCrownBatchPurchases(client, latestBlock))
    await runStream('crown_roll_requested', () => syncCrownRollRequested(client, latestBlock))
    await runStream('crown_roll_resolved', () => syncCrownRollResolved(client, latestBlock))
    await runStream('crown_round_settled', () => syncCrownRoundSettled(client, latestBlock))
    await runStream('crown_claimed_dividends', () => syncCrownClaims(client, 'ClaimedDividends', latestBlock))
    await runStream('crown_claimed_prize', () => syncCrownClaims(client, 'ClaimedPrize', latestBlock))
    await runStream('autocrown_config_updated', () => syncAutoCrownConfigUpdated(client, latestBlock))
    await runStream('autocrown_deposits', () => syncAutoCrownDeposits(client, latestBlock))
    await runStream('autocrown_executed_for', () => syncAutoCrownExecutedFor(client, 'ExecutedFor', latestBlock))
    await runStream('autocrown_batch_executed_for', () => syncAutoCrownExecutedFor(client, 'BatchExecutedFor', latestBlock))
    await runStream('autocrown_stops', () => syncAutoCrownStops(client, latestBlock))
    await runStream('treasury_vault', () => syncVaultEvents(client, latestBlock))
    await runStream('treasury_buybacks', () => syncBuybacks(client, latestBlock))
    await runStream('direct_burns', () => syncDirectBurns(client, latestBlock))
    await runStream('staking_deposits', () => syncStakeDeposits(client, latestBlock))
    await runStream('staking_withdrawals', () => syncStakeWithdrawals(client, latestBlock))
    await runStream('staking_compounds', () => syncStakeCompounds(client, latestBlock))
    await runStream('staking_yield_distributions', () => syncYieldDistributions(client, latestBlock))
    await runStream('lock_reward_notified', () => syncLockRewards(client, latestBlock))
    await runStream('locker_locked', () => syncLockerStateEvent(client, 'locker_locked', LOCKED_EVENT, latestBlock))
    await runStream('locker_added', () => syncLockerStateEvent(client, 'locker_added', ADDED_TO_LOCK_EVENT, latestBlock))
    await runStream('locker_extended', () => syncLockerStateEvent(client, 'locker_extended', EXTENDED_LOCK_EVENT, latestBlock))
    await runStream('locker_unlocked', () => syncLockerStateEvent(client, 'locker_unlocked', UNLOCKED_EVENT, latestBlock))
    const latestSyncedBlock = results.reduce((max, result) => (
      result.latestBlock > max ? result.latestBlock : max
    ), 0n)

    logger.info('[protocol-indexer] cycle complete', {
      latestSyncedBlock: latestSyncedBlock.toString(),
      streams: results.reduce<Record<string, number>>((acc, item) => {
        acc[item.streamName] = item.inserted
        return acc
      }, {}),
    })

    return {
      latestSyncedBlock: latestSyncedBlock.toString(),
      streams: results.reduce<Record<string, number>>((acc, item) => {
        acc[item.streamName] = item.inserted
        return acc
      }, {}),
    }
  } finally {
    client.release()
  }
}

export async function runTreasuryAgentSnapshotSyncOnce(options?: {
  logger?: Logger
}) {
  const logger = options?.logger ?? console
  await initProtocolIndexSchema()

  const pool = getProtocolIndexPool()
  const client = await pool.connect()

  try {
    const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
    const results: Array<{ streamName: string; inserted: number; latestBlock: bigint }> = []
    const runStream = async (
      label: string,
      syncer: () => Promise<{ streamName: string; inserted: number; latestBlock: bigint }>
    ) => {
      const startedAt = Date.now()
      logger.info(`[treasury-agent-snapshots] syncing ${label}...`)
      const result = await syncer()
      logger.info(`[treasury-agent-snapshots] synced ${label}`, {
        inserted: result.inserted,
        latestBlock: result.latestBlock.toString(),
        durationMs: Date.now() - startedAt,
      })
      results.push(result)
    }

    await runStream('treasury_agent_leaderboard', () => syncTreasuryAgentLeaderboardSnapshot(client, latestBlock))
    await runStream('treasury_agent_holdings', () => syncTreasuryAgentHoldingsSnapshot(client, latestBlock))

    const latestSyncedBlock = results.reduce((max, result) => (
      result.latestBlock > max ? result.latestBlock : max
    ), 0n)

    logger.info('[treasury-agent-snapshots] cycle complete', {
      latestSyncedBlock: latestSyncedBlock.toString(),
      streams: results.reduce<Record<string, number>>((acc, item) => {
        acc[item.streamName] = item.inserted
        return acc
      }, {}),
    })

    return {
      latestSyncedBlock: latestSyncedBlock.toString(),
      streams: results.reduce<Record<string, number>>((acc, item) => {
        acc[item.streamName] = item.inserted
        return acc
      }, {}),
    }
  } finally {
    client.release()
  }
}

export async function closeProtocolIndexerResources() {
  await closeProtocolIndexPool().catch(() => {})
}
