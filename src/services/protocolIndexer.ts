import type { AbiEvent, Address, Log } from 'viem'
import { getAddress, parseAbiItem } from 'viem'
import type { PoolClient } from 'pg'
import gridMiningAbi from '../abis/GridMining.json' with { type: 'json' }
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'
import { publicClient } from '../lib/client.js'
import { closeProtocolIndexPool, getProtocolIndexPool, initProtocolIndexSchema } from '../lib/protocolIndexDb.js'
import { toBigInt } from '../lib/format.js'

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
const VAULT_EVENT = parseAbiItem('event VaultReceived(uint256 amount, uint256 totalVaulted)')
const BUYBACK_EVENT = parseAbiItem(
  'event BuybackExecuted(uint256 ethSpent, uint256 lootReceived, uint256 lootBurned, uint256 lootToStakers)'
)
const TRANSFER_EVENT = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)')
const STAKE_DEPOSIT_EVENT = parseAbiItem('event Deposited(address indexed user, uint256 amount, uint256 newBalance)')
const STAKE_WITHDRAW_EVENT = parseAbiItem('event Withdrawn(address indexed user, uint256 amount, uint256 newBalance)')
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

function normalizeAddress(address: string) {
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

async function getLastSyncedBlock(client: PoolClient, streamName: string) {
  const result = await client.query<{ last_synced_block: string }>(
    'select last_synced_block::text from protocol_sync_state where stream_name = $1',
    [streamName]
  )

  if (!result.rowCount) {
    return env.scanStartBlock > 0n ? env.scanStartBlock - 1n : 0n
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

async function syncDeployments(client: PoolClient, eventName: 'Deployed' | 'DeployedFor') {
  const streamName = eventName === 'Deployed' ? 'deployments_direct' : 'deployments_for'
  const event = eventName === 'Deployed' ? DEPLOYED_EVENT : DEPLOYED_FOR_EVENT
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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
        normalizeAddress(log.args.user),
        log.eventName === 'DeployedFor' ? normalizeAddress(log.args.executor) : null,
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

async function syncRoundSettled(client: PoolClient) {
  const streamName = 'rounds'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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

    await client.query(
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

async function syncCheckpoints(client: PoolClient) {
  const streamName = 'checkpoints'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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
        normalizeAddress(log.args.user),
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

async function syncVaultEvents(client: PoolClient) {
  const streamName = 'treasury_vault'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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

async function syncBuybacks(client: PoolClient) {
  const streamName = 'treasury_buybacks'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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

async function syncDirectBurns(client: PoolClient) {
  const streamName = 'direct_burns'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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
        normalizeAddress(log.args.from),
        toBigInt(log.args.value).toString(),
        log.blockNumber.toString(),
        timestampMs,
      ]
    )
  })

  await updateSyncState(client, streamName, latestBlock)
  return { streamName, inserted: logs.length, latestBlock }
}

async function syncStakeDeposits(client: PoolClient) {
  const streamName = 'staking_deposits'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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
        normalizeAddress(log.args.user),
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

async function syncStakeWithdrawals(client: PoolClient) {
  const streamName = 'staking_withdrawals'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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
        normalizeAddress(log.args.user),
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

async function syncYieldDistributions(client: PoolClient) {
  const streamName = 'staking_yield_distributions'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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

async function syncLockRewards(client: PoolClient) {
  const streamName = 'lock_reward_notified'
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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
    await client.query(
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

    await client.query(
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
        normalizeAddress(log.args.user),
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

async function syncLockerStateEvent(client: PoolClient, streamName: string, event: typeof LOCKED_EVENT | typeof ADDED_TO_LOCK_EVENT | typeof EXTENDED_LOCK_EVENT | typeof UNLOCKED_EVENT) {
  const lastSyncedBlock = await getLastSyncedBlock(client, streamName)
  const latestBlock = await withRpcRetries(() => publicClient.getBlockNumber())
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

export async function runProtocolIndexSyncOnce(options?: {
  logger?: Logger
}) {
  const logger = options?.logger ?? console
  await initProtocolIndexSchema()

  const pool = getProtocolIndexPool()
  const client = await pool.connect()

  try {
    const results: Array<{ streamName: string; inserted: number; latestBlock: bigint }> = []

    results.push(await syncRoundSettled(client))
    results.push(await syncDeployments(client, 'Deployed'))
    results.push(await syncDeployments(client, 'DeployedFor'))
    results.push(await syncCheckpoints(client))
    results.push(await syncVaultEvents(client))
    results.push(await syncBuybacks(client))
    results.push(await syncDirectBurns(client))
    results.push(await syncStakeDeposits(client))
    results.push(await syncStakeWithdrawals(client))
    results.push(await syncYieldDistributions(client))
    results.push(await syncLockRewards(client))
    results.push(await syncLockerStateEvent(client, 'locker_locked', LOCKED_EVENT))
    results.push(await syncLockerStateEvent(client, 'locker_added', ADDED_TO_LOCK_EVENT))
    results.push(await syncLockerStateEvent(client, 'locker_extended', EXTENDED_LOCK_EVENT))
    results.push(await syncLockerStateEvent(client, 'locker_unlocked', UNLOCKED_EVENT))

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

export async function closeProtocolIndexerResources() {
  await closeProtocolIndexPool().catch(() => {})
}
