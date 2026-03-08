import type { AbiEvent, Address, Log } from 'viem'
import { formatEther, getAddress, isAddress, parseAbiItem } from 'viem'
import lootAbi from '../abis/Loot.json' with { type: 'json' }
import gridMiningAbi from '../abis/GridMining.json' with { type: 'json' }
import treasuryAbi from '../abis/Treasury.json' with { type: 'json' }
import stakingAbi from '../abis/Staking.json' with { type: 'json' }
import autoMinerAbi from '../abis/AutoMiner.json' with { type: 'json' }
import { CONTRACTS, PROTOCOL_CONSTANTS } from '../config/contracts.js'
import { env } from '../config/env.js'
import { publicClient } from '../lib/client.js'
import { countSelectedBlocks, decodeBlockMask, etherFixed, etherString, relativeTime, safeAddressEq, toBigInt } from '../lib/format.js'

const DEPLOYED_EVENT = parseAbiItem(
  'event Deployed(uint64 indexed roundId, address indexed user, uint256 amountPerBlock, uint256 blockMask, uint256 totalAmount)'
)
const DEPLOYED_FOR_EVENT = parseAbiItem(
  'event DeployedFor(uint64 indexed roundId, address indexed user, address indexed executor, uint256 amountPerBlock, uint256 blockMask, uint256 totalAmount)'
)
const ROUND_SETTLED_EVENT = parseAbiItem(
  'event RoundSettled(uint64 indexed roundId, uint8 winningBlock, address topMiner, uint256 totalWinnings, uint256 topMinerReward, uint256 lootpotAmount, bool isSplit, uint256 topMinerSeed, uint256 winnersDeployed)'
)
const GAME_STARTED_EVENT = parseAbiItem('event GameStarted(uint64 indexed roundId, uint256 startTime, uint256 endTime)')
const BUYBACK_EVENT = parseAbiItem(
  'event BuybackExecuted(uint256 ethSpent, uint256 lootReceived, uint256 lootBurned, uint256 lootToStakers)'
)
const VAULT_EVENT = parseAbiItem('event VaultReceived(uint256 amount, uint256 totalVaulted)')
const STAKE_DEPOSIT_EVENT = parseAbiItem('event Deposited(address indexed user, uint256 amount, uint256 newBalance)')
const STAKE_WITHDRAW_EVENT = parseAbiItem('event Withdrawn(address indexed user, uint256 amount, uint256 newBalance)')
const YIELD_DISTRIBUTED_EVENT = parseAbiItem('event YieldDistributed(uint256 amount, uint256 newAccYieldPerShare)')
const CHECKPOINTED_EVENT = parseAbiItem('event Checkpointed(uint64 indexed roundId, address indexed user, uint256 ethReward, uint256 lootReward)')
const CLAIMED_LOOT_EVENT = parseAbiItem('event ClaimedLOOT(address indexed user, uint256 minedLoot, uint256 forgedLoot, uint256 fee, uint256 net)')

type DeploymentLog = Log<bigint, number, false, typeof DEPLOYED_EVENT> | Log<bigint, number, false, typeof DEPLOYED_FOR_EVENT>

const blockTimestampCache = new Map<string, number>()
const LOG_BLOCK_RANGE = 45_000n
let scanStartBlockPromise: Promise<bigint> | null = null
let protocolStatusCache: { value: Promise<{ gameStarted: boolean; currentRoundId: bigint }>; expiresAt: number } | null = null

function normalizeAddress(value: unknown): Address {
  if (typeof value !== 'string') {
    throw new Error(`Invalid address value: ${String(value)}`)
  }
  return getAddress(value)
}

async function getBlockTimestampMs(blockNumber: bigint): Promise<number> {
  const key = blockNumber.toString()
  const cached = blockTimestampCache.get(key)
  if (cached) return cached
  const block = await publicClient.getBlock({ blockNumber })
  const ts = Number(block.timestamp) * 1000
  blockTimestampCache.set(key, ts)
  return ts
}

async function contractExistsAt(address: Address, blockNumber: bigint) {
  const code = await publicClient.getBytecode({ address, blockNumber })
  return !!code && code !== '0x'
}

async function findContractDeploymentBlock(address: Address) {
  let low = 0n
  let high = await publicClient.getBlockNumber()

  if (!(await contractExistsAt(address, high))) {
    throw new Error(`Contract not found on chain: ${address}`)
  }

  while (low < high) {
    const mid = (low + high) / 2n
    if (await contractExistsAt(address, mid)) {
      high = mid
    } else {
      low = mid + 1n
    }
  }

  return low
}

async function getScanStartBlock() {
  if (env.scanStartBlock > 0n) {
    return env.scanStartBlock
  }

  if (!scanStartBlockPromise) {
    scanStartBlockPromise = Promise.all([
      findContractDeploymentBlock(CONTRACTS.gridMining),
      findContractDeploymentBlock(CONTRACTS.treasury),
      findContractDeploymentBlock(CONTRACTS.staking),
      findContractDeploymentBlock(CONTRACTS.autoMiner),
    ]).then((blocks) => {
      const earliest = blocks.reduce((min, block) => block < min ? block : min, blocks[0])
      return earliest > 100n ? earliest - 100n : 0n
    })
  }

  return scanStartBlockPromise
}

async function getLogsPaged<TEvent extends AbiEvent | undefined>(
  params: {
    address: Address
    event: TEvent
    args?: Record<string, unknown>
    fromBlock?: bigint
    toBlock?: bigint | 'latest'
  }
): Promise<Log<bigint, number, false, TEvent>[]> {
  const latestBlock = params.toBlock === 'latest' || params.toBlock === undefined
    ? await publicClient.getBlockNumber()
    : params.toBlock
  const startBlock = params.fromBlock && params.fromBlock > 0n
    ? params.fromBlock
    : await getScanStartBlock()

  if (startBlock > latestBlock) {
    return []
  }

  const chunks: Log<bigint, number, false, TEvent>[] = []
  let cursor = startBlock

  while (cursor <= latestBlock) {
    const endBlock = cursor + LOG_BLOCK_RANGE - 1n > latestBlock
      ? latestBlock
      : cursor + LOG_BLOCK_RANGE - 1n

    const logs = await publicClient.getLogs({
      address: params.address,
      event: params.event as never,
      args: params.args as never,
      fromBlock: cursor,
      toBlock: endBlock,
    }) as Log<bigint, number, false, TEvent>[]
    chunks.push(...logs)
    cursor = endBlock + 1n
  }

  return chunks
}

function getLogBlockNumber(log: { blockNumber: bigint | null }): bigint {
  if (log.blockNumber == null) {
    throw new Error('Log missing blockNumber')
  }
  return log.blockNumber
}

function getLogIndex(log: { logIndex: number | null }): number {
  return log.logIndex ?? 0
}

function compareLogsAsc(a: { blockNumber: bigint | null; logIndex: number | null }, b: { blockNumber: bigint | null; logIndex: number | null }) {
  return Number(getLogBlockNumber(a) - getLogBlockNumber(b)) || getLogIndex(a) - getLogIndex(b)
}

function compareLogsDesc(a: { blockNumber: bigint | null; logIndex: number | null }, b: { blockNumber: bigint | null; logIndex: number | null }) {
  return Number(getLogBlockNumber(b) - getLogBlockNumber(a)) || getLogIndex(b) - getLogIndex(a)
}

function emptyPricePayload() {
  return {
    priceUsd: 0,
    payload: formatPricePayload(0, []),
  }
}

function formatPricePayload(priceUsd: number, pairs: Array<{ priceUsd?: string; priceNative?: string; volume?: { h24?: number }; liquidity?: { usd?: number }; priceChange?: { h24?: number }; fdv?: number }>) {
  const best = [...pairs].sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0))[0]
  return {
    loot: {
      priceUsd: best?.priceUsd ?? priceUsd.toString(),
      priceNative: best?.priceNative ?? '0',
      volume24h: ((best?.volume?.h24 ?? 0)).toString(),
      liquidity: ((best?.liquidity?.usd ?? 0)).toString(),
      priceChange24h: ((best?.priceChange?.h24 ?? 0)).toString(),
      fdv: ((best?.fdv ?? 0)).toString(),
    },
    fetchedAt: new Date().toISOString(),
  }
}

async function getProtocolStatus() {
  const now = Date.now()
  if (protocolStatusCache && protocolStatusCache.expiresAt > now) {
    return protocolStatusCache.value
  }

  const value = Promise.all([
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'gameStarted',
    }) as Promise<boolean>,
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'currentRoundId',
    }) as Promise<bigint>,
  ]).then(([gameStarted, currentRoundId]) => ({ gameStarted, currentRoundId }))

  protocolStatusCache = {
    value,
    expiresAt: now + 2000,
  }

  return value
}

export async function getLootPrice() {
  try {
    const res = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${CONTRACTS.loot}`, { cache: 'no-store' })
    if (!res.ok) {
      return emptyPricePayload()
    }
    const data = await res.json()
    const pairs = (data.pairs ?? []) as Array<{ priceUsd?: string; priceNative?: string; volume?: { h24?: number }; liquidity?: { usd?: number }; priceChange?: { h24?: number }; fdv?: number }>
    if (!pairs.length) {
      return emptyPricePayload()
    }
    const best = [...pairs].sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0))[0]
    return {
      priceUsd: Number(best?.priceUsd ?? 0),
      payload: formatPricePayload(Number(best?.priceUsd ?? 0), pairs),
    }
  } catch {
    return emptyPricePayload()
  }
}

export async function getStats() {
  const [totalMinted, price] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.loot,
      abi: lootAbi,
      functionName: 'totalMinted',
    }) as Promise<bigint>,
    getLootPrice(),
  ])

  return {
    totalSupply: totalMinted.toString(),
    totalSupplyFormatted: etherString(totalMinted),
    totalMinted: totalMinted.toString(),
    totalMintedFormatted: etherString(totalMinted),
    ...price.payload,
  }
}

async function getRoundDeployLogs(roundId: bigint) {
  const [direct, delegated] = await Promise.all([
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: DEPLOYED_EVENT,
      args: { roundId },
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: DEPLOYED_FOR_EVENT,
      args: { roundId },
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
  ])
  return [...direct, ...delegated].sort(compareLogsAsc) as DeploymentLog[]
}

function buildBlockStats(deployLogs: DeploymentLog[]) {
  const blocks = Array.from({ length: PROTOCOL_CONSTANTS.gridSize }, (_, id) => ({
    id,
    deployed: 0n,
    deployedFormatted: '0.0',
    minerCount: 0,
  }))

  for (const log of deployLogs) {
    const blockMask = toBigInt(log.args.blockMask)
    const amountPerBlock = toBigInt(log.args.amountPerBlock)
    const selected = decodeBlockMask(blockMask)
    for (const blockId of selected) {
      blocks[blockId].deployed += amountPerBlock
      blocks[blockId].minerCount += 1
    }
  }

  return blocks.map((block) => ({
    ...block,
    deployed: block.deployed.toString(),
    deployedFormatted: etherString(block.deployed),
  }))
}

function computeUserDeployed(mask: bigint, amountPerBlock: bigint) {
  return BigInt(countSelectedBlocks(mask)) * amountPerBlock
}

export async function getCurrentRound(user?: string) {
  const [roundInfo, lootpotPool] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'getCurrentRoundInfo',
    }) as Promise<[bigint, bigint, bigint, bigint, bigint, boolean]>,
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'lootpotPool',
    }) as Promise<bigint>,
  ])

  const [roundId, startTime, endTime, totalDeployed] = roundInfo
  if (roundId === 0n) {
    return {
      roundId: '0',
      startTime: Number(startTime),
      endTime: Number(endTime),
      totalDeployed: '0',
      totalDeployedFormatted: '0',
      lootpotPool: lootpotPool.toString(),
      lootpotPoolFormatted: etherString(lootpotPool),
      settled: false,
      blocks: Array.from({ length: PROTOCOL_CONSTANTS.gridSize }, (_, id) => ({
        id,
        deployed: '0',
        deployedFormatted: '0',
        minerCount: 0,
      })),
      userDeployed: '0',
      userDeployedFormatted: '0',
    }
  }

  const deployLogs = await getRoundDeployLogs(roundId)
  const blocks = buildBlockStats(deployLogs)

  let userDeployed = 0n
  if (user && isAddress(user)) {
    const minerInfo = await publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'getMinerInfo',
      args: [roundId, getAddress(user)],
    }) as [bigint, bigint, boolean]
    userDeployed = computeUserDeployed(minerInfo[0], minerInfo[1])
  }

  return {
    roundId: roundId.toString(),
    startTime: Number(startTime),
    endTime: Number(endTime),
    totalDeployed: totalDeployed.toString(),
    totalDeployedFormatted: etherString(totalDeployed),
    lootpotPool: lootpotPool.toString(),
    lootpotPoolFormatted: etherString(lootpotPool),
    settled: false,
    blocks,
    userDeployed: userDeployed.toString(),
    userDeployedFormatted: etherString(userDeployed),
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

function mapDeployment(log: DeploymentLog) {
  return {
    address: normalizeAddress(log.args.user),
    amountPerBlock: toBigInt(log.args.amountPerBlock),
    blockMask: toBigInt(log.args.blockMask),
    totalAmount: toBigInt(log.args.totalAmount),
    isAutoMine: log.eventName === 'DeployedFor',
    txHash: log.transactionHash,
    blockNumber: log.blockNumber,
    logIndex: log.logIndex,
  }
}

async function resolveRoundWinnerAddress(roundId: bigint, roundState: Awaited<ReturnType<typeof getRoundState>>, deployLogs: DeploymentLog[]) {
  if (roundState.isSplit || roundState.winnersDeployed === 0n) return null
  if (roundState.topMiner !== '0x0000000000000000000000000000000000000001') return roundState.topMiner

  const winningDeploys = deployLogs
    .map(mapDeployment)
    .filter((log) => decodeBlockMask(log.blockMask).includes(Number(roundState.winningBlock)))

  let cumulative = 0n
  const sample = roundState.topMinerSeed % roundState.winnersDeployed

  for (const log of winningDeploys) {
    const next = cumulative + log.amountPerBlock
    if (sample >= cumulative && sample < next) return log.address
    cumulative = next
  }
  return null
}

async function getRoundState(roundId: bigint) {
  const [
    roundStruct,
    roundView,
    settledLog,
  ] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'rounds',
      args: [roundId],
    }) as Promise<[bigint, bigint, bigint, bigint, bigint, number, Address, bigint, bigint, bigint, bigint, boolean, bigint]>,
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'getRound',
      args: [roundId],
    }) as Promise<[bigint, bigint, bigint, bigint, number, Address, bigint, bigint, boolean]>,
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: ROUND_SETTLED_EVENT,
      args: { roundId },
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }).then((logs) => logs.at(-1) ?? null),
  ])

  return {
    startTime: toBigInt(roundStruct[0]),
    endTime: toBigInt(roundStruct[1]),
    totalDeployed: toBigInt(roundStruct[2]),
    totalWinnings: toBigInt(roundStruct[3]),
    winnersDeployed: toBigInt(roundStruct[4]),
    winningBlock: roundStruct[5],
    topMiner: roundStruct[6],
    topMinerReward: toBigInt(roundStruct[7]),
    lootpotAmount: toBigInt(roundStruct[8]),
    vrfRequestId: toBigInt(roundStruct[9]),
    topMinerSeed: toBigInt(roundStruct[10]),
    settled: roundStruct[11],
    minerCount: toBigInt(roundStruct[12]),
    settledLog,
    roundView,
    isSplit: settledLog?.args.isSplit ?? roundStruct[6] === '0x0000000000000000000000000000000000000000',
  }
}

export async function getRound(roundIdInput: string | number | bigint) {
  const roundId = BigInt(roundIdInput)
  const [roundState, deployLogs] = await Promise.all([
    getRoundState(roundId),
    getRoundDeployLogs(roundId),
  ])

  const winnerAddress = await resolveRoundWinnerAddress(roundId, roundState, deployLogs)
  const winnerCount = deployLogs
    .map(mapDeployment)
    .filter((log) => decodeBlockMask(log.blockMask).includes(Number(roundState.winningBlock)))
    .length

  const vaultedAmount = computeVaultedAmount(roundState.totalDeployed, roundState.winnersDeployed, roundState.totalWinnings)
  const settledAtMs = roundState.settledLog?.blockNumber ? await getBlockTimestampMs(roundState.settledLog.blockNumber) : Number(roundState.endTime) * 1000

  return {
    roundId: Number(roundId),
    winningBlock: Number(roundState.winningBlock),
    topMiner: roundState.isSplit ? null : winnerAddress,
    isSplit: roundState.isSplit,
    winnerCount,
    totalDeployed: roundState.totalDeployed.toString(),
    totalDeployedFormatted: etherString(roundState.totalDeployed),
    vaultedAmount: vaultedAmount.toString(),
    vaultedAmountFormatted: etherString(vaultedAmount),
    totalWinnings: roundState.totalWinnings.toString(),
    totalWinningsFormatted: etherString(roundState.totalWinnings),
    lootpotAmount: roundState.lootpotAmount.toString(),
    lootpotAmountFormatted: etherString(roundState.lootpotAmount),
    startTime: Number(roundState.startTime),
    endTime: Number(roundState.endTime),
    settledAt: new Date(settledAtMs).toISOString(),
    txHash: roundState.settledLog?.transactionHash ?? null,
  }
}

export async function getRoundMiners(roundIdInput: string | number | bigint) {
  const roundId = BigInt(roundIdInput)
  const [roundState, deployLogs] = await Promise.all([
    getRoundState(roundId),
    getRoundDeployLogs(roundId),
  ])

  const mapped = deployLogs.map(mapDeployment)
  const winners = mapped.filter((log) => decodeBlockMask(log.blockMask).includes(Number(roundState.winningBlock)))
  const claimablePool = roundState.totalDeployed === 0n
    ? 0n
    : roundState.totalDeployed
        - ((roundState.totalDeployed * PROTOCOL_CONSTANTS.adminFeeBps) / PROTOCOL_CONSTANTS.bpsDenominator)
        - computeVaultedAmount(roundState.totalDeployed, roundState.winnersDeployed, roundState.totalWinnings)

  const singleWinner = await resolveRoundWinnerAddress(roundId, roundState, deployLogs)

  return {
    roundId: Number(roundId),
    winningBlock: Number(roundState.winningBlock),
    miners: winners.map((winner) => {
      const ethReward = roundState.winnersDeployed > 0n
        ? (claimablePool * winner.amountPerBlock) / roundState.winnersDeployed
        : 0n

      let lootReward = 0n
      if (roundState.isSplit) {
        lootReward = roundState.winnersDeployed > 0n
          ? (roundState.topMinerReward * winner.amountPerBlock) / roundState.winnersDeployed
          : 0n
      } else if (safeAddressEq(singleWinner, winner.address)) {
        lootReward = roundState.topMinerReward
      }

      if (roundState.lootpotAmount > 0n && roundState.winnersDeployed > 0n) {
        lootReward += (roundState.lootpotAmount * winner.amountPerBlock) / roundState.winnersDeployed
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
    }),
  }
}

export async function getRounds(page = 1, limit = 12, lootpotOnly = false) {
  const status = await getProtocolStatus()
  if (!status.gameStarted || status.currentRoundId === 0n) {
    return {
      rounds: [],
      pagination: { page, limit, total: 0, pages: 1 },
    }
  }

  const logs = await getLogsPaged({
    address: CONTRACTS.gridMining,
    event: ROUND_SETTLED_EVENT,
    fromBlock: env.scanStartBlock,
    toBlock: 'latest',
  })

  let roundIds = logs.map((log) => Number(log.args.roundId))
  if (lootpotOnly) {
    roundIds = logs.filter((log) => toBigInt(log.args.lootpotAmount ?? 0n) > 0n).map((log) => Number(log.args.roundId))
  }

  roundIds = [...new Set(roundIds)].sort((a, b) => b - a)
  const total = roundIds.length
  const pages = Math.max(1, Math.ceil(total / limit))
  const slice = roundIds.slice((page - 1) * limit, page * limit)

  const rounds = await Promise.all(slice.map((roundId) => getRound(roundId)))
  return {
    rounds,
    pagination: { page, limit, total, pages },
  }
}

export async function getTreasuryStats() {
  const status = await getProtocolStatus()
  const [stats, vaultLogs] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.treasury,
      abi: treasuryAbi,
      functionName: 'getStats',
    }) as Promise<[bigint, bigint, bigint, bigint]>,
    status.currentRoundId === 0n
      ? Promise.resolve([])
      : getLogsPaged({
      address: CONTRACTS.treasury,
      event: VAULT_EVENT,
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
      }),
  ])

  const totalVaultedLifetime = vaultLogs.reduce((sum, log) => sum + toBigInt(log.args.amount), 0n)

  return {
    totalVaulted: totalVaultedLifetime.toString(),
    totalVaultedFormatted: etherString(totalVaultedLifetime),
    currentVaulted: stats[0].toString(),
    currentVaultedFormatted: etherString(stats[0]),
    totalBurned: stats[1].toString(),
    totalBurnedFormatted: etherString(stats[1]),
    totalDistributedToStakers: stats[2].toString(),
    totalDistributedToStakersFormatted: etherString(stats[2]),
    totalBuybacks: Number(stats[3]),
  }
}

export async function getBuybacks(page = 1, limit = 12) {
  const logs = await getLogsPaged({
    address: CONTRACTS.treasury,
    event: BUYBACK_EVENT,
    fromBlock: env.scanStartBlock,
    toBlock: 'latest',
  })

  const ordered = [...logs].sort((a, b) =>
    compareLogsDesc(a, b)
  )
  const total = ordered.length
  const pages = Math.max(1, Math.ceil(total / limit))
  const slice = ordered.slice((page - 1) * limit, page * limit)

  const buybacks = await Promise.all(slice.map(async (log) => {
    const timestampMs = await getBlockTimestampMs(getLogBlockNumber(log))
    return {
      ethSpent: toBigInt(log.args.ethSpent).toString(),
      ethSpentFormatted: etherString(toBigInt(log.args.ethSpent)),
      lootReceived: toBigInt(log.args.lootReceived).toString(),
      lootReceivedFormatted: etherString(toBigInt(log.args.lootReceived)),
      lootBurned: toBigInt(log.args.lootBurned).toString(),
      lootBurnedFormatted: etherString(toBigInt(log.args.lootBurned)),
      lootToStakers: toBigInt(log.args.lootToStakers).toString(),
      lootToStakersFormatted: etherString(toBigInt(log.args.lootToStakers)),
      txHash: log.transactionHash,
      blockNumber: Number(log.blockNumber),
      timestamp: new Date(timestampMs).toISOString(),
    }
  }))

  return {
    buybacks,
    pagination: { page, limit, total, pages },
  }
}

export async function getStakingStats() {
  const [globalStats, stakeTokenBalance, price, yieldLogs] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.staking,
      abi: stakingAbi,
      functionName: 'getGlobalStats',
    }) as Promise<[bigint, bigint, bigint]>,
    publicClient.readContract({
      address: CONTRACTS.loot,
      abi: lootAbi,
      functionName: 'balanceOf',
      args: [CONTRACTS.staking],
    }) as Promise<bigint>,
    getLootPrice(),
    getLogsPaged({
      address: CONTRACTS.staking,
      event: YIELD_DISTRIBUTED_EVENT,
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
  ])

  const totalStaked = globalStats[0]
  const tvlUsd = Number(formatEther(totalStaked)) * price.priceUsd

  const now = Date.now()
  let yield30d = 0n
  for (const log of yieldLogs) {
    const ts = await getBlockTimestampMs(log.blockNumber)
    if (ts >= now - (30 * 24 * 60 * 60 * 1000)) {
      yield30d += toBigInt(log.args.amount)
    }
  }
  const apr = totalStaked > 0n ? (Number(formatEther(yield30d)) * (365 / 30) / Number(formatEther(totalStaked))) * 100 : 0

  return {
    totalStaked: totalStaked.toString(),
    totalStakedFormatted: etherString(totalStaked),
    totalYieldDistributed: globalStats[1].toString(),
    totalYieldDistributedFormatted: etherString(globalStats[1]),
    contractLootBalance: stakeTokenBalance.toString(),
    contractLootBalanceFormatted: etherString(stakeTokenBalance),
    apr: apr.toFixed(2),
    tvlUsd: tvlUsd.toFixed(2),
  }
}

export async function getUserStake(address: Address) {
  const [stakeInfo, pendingRewards] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.staking,
      abi: stakingAbi,
      functionName: 'getStakeInfo',
      args: [address],
    }) as Promise<[bigint, bigint, bigint, bigint, bigint, bigint, boolean]>,
    publicClient.readContract({
      address: CONTRACTS.staking,
      abi: stakingAbi,
      functionName: 'getPendingRewards',
      args: [address],
    }) as Promise<bigint>,
  ])

  return {
    balance: stakeInfo[0].toString(),
    balanceFormatted: etherString(stakeInfo[0]),
    pendingRewards: pendingRewards.toString(),
    pendingRewardsFormatted: etherString(pendingRewards),
    compoundFeeReserve: stakeInfo[2].toString(),
    compoundFeeReserveFormatted: etherString(stakeInfo[2]),
    lastClaimAt: Number(stakeInfo[3]),
    lastDepositAt: Number(stakeInfo[4]),
    lastWithdrawAt: Number(stakeInfo[5]),
    canCompound: stakeInfo[6],
  }
}

export async function getUserRewards(address: Address) {
  const [pending, pendingLoot] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'getTotalPendingRewards',
      args: [address],
    }) as Promise<[bigint, bigint, bigint, bigint]>,
    publicClient.readContract({
      address: CONTRACTS.gridMining,
      abi: gridMiningAbi,
      functionName: 'getPendingLOOT',
      args: [address],
    }) as Promise<[bigint, bigint, bigint]>,
  ])

  const pendingEth = pending[0]
  const unforged = pending[1]
  const forged = pending[2]
  const gross = pendingLoot[0]
  const fee = pendingLoot[1]
  const net = pendingLoot[2]

  return {
    pendingETH: pendingEth.toString(),
    pendingETHFormatted: etherString(pendingEth),
    pendingLOOT: {
      unforged: unforged.toString(),
      unforgedFormatted: etherString(unforged),
      forged: forged.toString(),
      forgedFormatted: etherString(forged),
      gross: gross.toString(),
      grossFormatted: etherString(gross),
      fee: fee.toString(),
      feeFormatted: etherString(fee),
      net: net.toString(),
      netFormatted: etherString(net),
    },
    uncheckpointedRound: pending[3].toString(),
  }
}

export async function getAutoMine(address: Address) {
  const [state, progress] = await Promise.all([
    publicClient.readContract({
      address: CONTRACTS.autoMiner,
      abi: autoMinerAbi,
      functionName: 'getUserState',
      args: [address],
    }) as Promise<[
      {
        strategyId: number
        numBlocks: number
        active: boolean
        executorFeeBps: number
        selectedBlockMask: number
        amountPerBlock: bigint
        numRounds: bigint
        roundsExecuted: bigint
        depositAmount: bigint
        depositTimestamp: number
        executorFlatFee: bigint
      },
      bigint,
      bigint,
      bigint,
      bigint,
    ]>,
    publicClient.readContract({
      address: CONTRACTS.autoMiner,
      abi: autoMinerAbi,
      functionName: 'getConfigProgress',
      args: [address],
    }) as Promise<[boolean, bigint, bigint, bigint, bigint]>,
  ])

  const [config, lastRound, costPerRound, roundsRemaining, totalRefundable] = state
  const selectedBlockMask = Number(config.selectedBlockMask)
  const selectedBlocks = selectedBlockMask > 0 ? decodeBlockMask(BigInt(selectedBlockMask)) : []
  return {
    config: {
      strategyId: Number(config.strategyId),
      numBlocks: Number(config.numBlocks),
      amountPerBlock: config.amountPerBlock.toString(),
      amountPerBlockFormatted: etherString(config.amountPerBlock),
      active: config.active,
      numRounds: Number(config.numRounds),
      roundsExecuted: Number(config.roundsExecuted),
      depositAmount: config.depositAmount.toString(),
      depositAmountFormatted: etherString(config.depositAmount),
      selectedBlockMask,
      selectedBlocks,
    },
    lastRound: Number(lastRound),
    costPerRound: costPerRound.toString(),
    costPerRoundFormatted: etherString(costPerRound),
    roundsRemaining: Number(roundsRemaining),
    totalRefundable: totalRefundable.toString(),
    totalRefundableFormatted: etherString(totalRefundable),
    progress: {
      active: progress[0],
      numRounds: Number(progress[1]),
      roundsExecuted: Number(progress[2]),
      roundsRemaining: Number(progress[3]),
      percentComplete: Number(progress[4]),
    },
  }
}

export async function getUserHistory(address: Address, limit = 100, roundIdFilter?: bigint) {
  const status = await getProtocolStatus()
  if (!status.gameStarted || status.currentRoundId === 0n) {
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

  const [deploys, delegated] = await Promise.all([
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: DEPLOYED_EVENT,
      args: { user: address, ...(roundIdFilter ? { roundId: roundIdFilter } : {}) },
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: DEPLOYED_FOR_EVENT,
      args: { user: address, ...(roundIdFilter ? { roundId: roundIdFilter } : {}) },
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
  ])

  const ordered = [...deploys, ...delegated].sort((a, b) =>
    compareLogsDesc(a, b)
  ).slice(0, limit)

  const history = await Promise.all(ordered.map(async (log) => {
    const roundId = toBigInt(log.args.roundId)
    const round = await getRound(roundId)
    const miners = await getRoundMiners(roundId)
    const blockMask = toBigInt(log.args.blockMask)
    const totalAmount = toBigInt(log.args.totalAmount)
    const wonWinningBlock = decodeBlockMask(blockMask).includes(round.winningBlock)
    const userMiner = miners.miners.find((miner) => safeAddressEq(miner.address, address))
    const ethWon = userMiner ? BigInt(userMiner.ethReward) : 0n
    const lootWon = userMiner ? BigInt(userMiner.lootReward) : 0n
    const pnl = Number(formatEther(ethWon)) - Number(formatEther(totalAmount))
    const timestamp = await getBlockTimestampMs(getLogBlockNumber(log))

    return {
      roundId: Number(roundId),
      totalAmount: totalAmount.toString(),
      blockMask: blockMask.toString(),
      txHash: log.transactionHash,
      isAutoMine: log.eventName === 'DeployedFor',
      timestamp: new Date(timestamp).toISOString(),
      roundResult: {
        settled: true,
        wonWinningBlock,
        lootpotHit: BigInt(round.lootpotAmount) > 0n,
        winningBlock: round.winningBlock,
        ethWon: ethWon.toString(),
        ethWonFormatted: etherString(ethWon),
        lootWon: lootWon.toString(),
        lootWonFormatted: etherString(lootWon),
        pnl: pnl.toString(),
      },
    }
  }))

  const totals = history.reduce((acc, entry) => {
    acc.totalETHWon += BigInt(entry.roundResult.ethWon)
    acc.totalLOOTWon += BigInt(entry.roundResult.lootWon)
    acc.totalETHDeployed += BigInt(entry.totalAmount)
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
}

export async function getLeaderboardMiners(limit = 12) {
  const status = await getProtocolStatus()
  if (!status.gameStarted || status.currentRoundId === 0n) {
    return { period: 'all', miners: [], deployers: [] }
  }

  const logs = await Promise.all([
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: DEPLOYED_EVENT,
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
    getLogsPaged({
      address: CONTRACTS.gridMining,
      event: DEPLOYED_FOR_EVENT,
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
  ])
  const totals = new Map<string, bigint>()
  for (const log of [...logs[0], ...logs[1]]) {
    const address = normalizeAddress(log.args.user)
    totals.set(address, (totals.get(address) ?? 0n) + toBigInt(log.args.totalAmount))
  }
  const deployers = [...totals.entries()]
      .sort((a, b) => (b[1] > a[1] ? 1 : -1))
      .slice(0, limit)
      .map(([address, totalDeployed]) => ({
        address,
        totalDeployed: totalDeployed.toString(),
        totalDeployedFormatted: etherString(totalDeployed),
        roundsPlayed: 0,
      }))

  return {
    period: 'all',
    miners: deployers,
    deployers,
  }
}

export async function getLeaderboardStakers(limit = 12) {
  const [deposits, withdrawals] = await Promise.all([
    getLogsPaged({
      address: CONTRACTS.staking,
      event: STAKE_DEPOSIT_EVENT,
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
    getLogsPaged({
      address: CONTRACTS.staking,
      event: STAKE_WITHDRAW_EVENT,
      fromBlock: env.scanStartBlock,
      toBlock: 'latest',
    }),
  ])
  const balances = new Map<string, bigint>()
  for (const log of deposits) {
    const address = normalizeAddress(log.args.user)
    balances.set(address, (balances.get(address) ?? 0n) + toBigInt(log.args.amount))
  }
  for (const log of withdrawals) {
    const address = normalizeAddress(log.args.user)
    balances.set(address, (balances.get(address) ?? 0n) - toBigInt(log.args.amount))
  }
  const stakers = [...balances.entries()]
      .filter(([, balance]) => balance > 0n)
      .sort((a, b) => (b[1] > a[1] ? 1 : -1))
      .slice(0, limit)
      .map(([address, balance]) => ({
        address,
        balance: balance.toString(),
        balanceFormatted: etherString(balance),
        stakedBalance: balance.toString(),
        stakedBalanceFormatted: etherString(balance),
      }))

  return { stakers }
}

export async function getLeaderboardEarners(limit = 12) {
  const status = await getProtocolStatus()
  if (!status.gameStarted || status.currentRoundId === 0n) {
    return {
      earners: [],
      pagination: { page: 1, limit, total: 0, pages: 1 },
    }
  }

  const checkpointLogs = await getLogsPaged({
    address: CONTRACTS.gridMining,
    event: CHECKPOINTED_EVENT,
    fromBlock: env.scanStartBlock,
    toBlock: 'latest',
  })
  const users = [...new Set(checkpointLogs.map((log) => normalizeAddress(log.args.user)))]
  const rewards = await Promise.all(users.map(async (address) => ({
    address,
    rewards: await getUserRewards(address),
  })))

  const earners = rewards
      .map(({ address, rewards }) => ({
        address,
        unforged: rewards.pendingLOOT.unforged,
        unforgedFormatted: rewards.pendingLOOT.unforgedFormatted,
        gross: rewards.pendingLOOT.gross,
        grossFormatted: rewards.pendingLOOT.grossFormatted,
      }))
      .filter((item) => BigInt(item.unforged) > 0n)
      .sort((a, b) => (BigInt(b.unforged) > BigInt(a.unforged) ? 1 : -1))
      .slice(0, limit)

  return {
    earners,
    pagination: { page: 1, limit, total: earners.length, pages: 1 },
  }
}

export async function getLatestRoundTransition() {
  const current = await getCurrentRound()
  const roundId = BigInt(current.roundId)
  const previousRoundId = roundId > 1n ? roundId - 1n : 0n
  const settled = previousRoundId > 0n ? await getRound(previousRoundId).catch(() => null) : null
  return {
    settled: settled ? {
      roundId: settled.roundId.toString(),
      winningBlock: settled.winningBlock.toString(),
      topMiner: settled.topMiner ?? '0x0000000000000000000000000000000000000000',
      totalWinnings: settled.totalWinnings,
      topMinerReward: '0',
      lootpotAmount: settled.lootpotAmount,
      isSplit: settled.isSplit,
    } : null,
    newRound: {
      roundId: current.roundId,
      startTime: current.startTime,
      endTime: current.endTime,
      lootpotPool: current.lootpotPool,
      lootpotPoolFormatted: current.lootpotPoolFormatted,
    },
  }
}

export function asAddress(value: string): Address {
  if (!isAddress(value)) throw new Error(`Invalid address: ${value}`)
  return getAddress(value)
}
