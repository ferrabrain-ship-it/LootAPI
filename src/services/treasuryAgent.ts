import { formatUnits, parseAbi, parseAbiItem, type AbiEvent, type Address, type Log } from 'viem'
import { CONTRACTS } from '../config/contracts.js'
import { publicClient } from '../lib/client.js'

type UserPosition = readonly [bigint, bigint, bigint, bigint, bigint, bigint]

export type TreasuryLeaderboardEntry = {
  rank: number
  address: string
  depositedFormatted: string
  pendingFormatted: string
  rewardsFormatted: string
}

export type TreasuryHoldingEntry = {
  symbol: string
  name: string
  address: string | null
  balance: string
  balanceFormatted: string
  usdValue: number
  usdValueFormatted: string
  allocation: number
  decimals: number
  logoUrl: string | null
  coingeckoUrl: string | null
  isNative: boolean
}

const ZERO = 0n
const ONE = 1n
const BASE_WETH = '0x4200000000000000000000000000000000000006' as Address
const USDC = '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913' as Address
const LOG_BLOCK_RANGE = 20_000n
const MIN_LOG_BLOCK_RANGE = 1_000n
const TREASURY_AGENT_CACHE_TTL_MS = 15_000
const METADATA_CACHE_TTL_MS = 6 * 60 * 60 * 1000
const PRICE_CACHE_TTL_MS = 5 * 60 * 1000
const MIN_HOLDING_USD = 5
const START_BLOCK_PADDING = 250n
const RPC_RETRY_ATTEMPTS = 3

const configAbi = parseAbi([
  'function multisig() view returns (address)',
  'function asset() view returns (address)',
  'function loot() view returns (address)',
  'function getUserPosition(address user) view returns (uint256 shares, uint256 pendingDepositAssets, uint256 pendingWithdrawShares, uint256 claimableWithdrawAssets, uint256 claimableLoot, uint256 redeemableAssets)',
])

const erc20ReadAbi = parseAbi([
  'function balanceOf(address) view returns (uint256)',
  'function symbol() view returns (string)',
  'function name() view returns (string)',
  'function decimals() view returns (uint8)',
])

const depositQueuedEvent = parseAbiItem(
  'event UserDepositQueued(address indexed caller, address indexed receiver, uint256 assets)'
)
const lootClaimedEvent = parseAbiItem(
  'event LootClaimed(address indexed user, uint256 amount)'
)
const transferEvent = parseAbiItem(
  'event Transfer(address indexed from, address indexed to, uint256 value)'
)

const cache = new Map<string, { expiresAt: number; value: unknown }>()
const inflight = new Map<string, Promise<unknown>>()

const COINGECKO_FALLBACKS: Record<string, { logoUrl: string; coingeckoUrl: string; name: string; symbol: string }> = {
  native_eth: {
    logoUrl: 'https://coin-images.coingecko.com/coins/images/279/small/ethereum.png?1696501628',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/ethereum',
    name: 'Ethereum',
    symbol: 'ETH',
  },
  [USDC.toLowerCase()]: {
    logoUrl: 'https://coin-images.coingecko.com/coins/images/6319/small/USDC.png?1769615602',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/usdc',
    name: 'USD Coin',
    symbol: 'USDC',
  },
  [BASE_WETH.toLowerCase()]: {
    logoUrl: 'https://coin-images.coingecko.com/coins/images/39810/small/weth.png?1724139790',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/l2-standard-bridged-weth-base',
    name: 'Wrapped Ether',
    symbol: 'WETH',
  },
}

function getCachedValue<T>(key: string) {
  const cached = cache.get(key)
  if (!cached || cached.expiresAt <= Date.now()) return undefined
  return cached.value as T
}

async function withCache<T>(key: string, ttlMs: number, loader: () => Promise<T>): Promise<T> {
  const cached = getCachedValue<T>(key)
  if (cached !== undefined) {
    return cached
  }

  const current = inflight.get(key)
  if (current) {
    return current as Promise<T>
  }

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

function toNumber(value: bigint, decimals: number) {
  return Number.parseFloat(formatUnits(value, decimals))
}

function formatDecimal(value: bigint, decimals: number, digits = 4) {
  return toNumber(value, decimals).toFixed(digits)
}

function formatBalance(value: bigint, decimals: number) {
  const parsed = toNumber(value, decimals)
  const maximumFractionDigits = parsed >= 1000 ? 2 : parsed >= 1 ? 4 : 6
  return parsed.toFixed(maximumFractionDigits)
}

function formatUsdValue(value: number) {
  return value.toLocaleString('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: value >= 100 ? 0 : 2,
    maximumFractionDigits: value >= 100 ? 0 : 2,
  })
}

function isRangeLimitError(message: string) {
  const lower = message.toLowerCase()
  return (
    lower.includes('range') ||
    lower.includes('max allowed range') ||
    lower.includes('exceeded') ||
    lower.includes('10,000') ||
    lower.includes('10000')
  )
}

function isRetryableRpcError(message: string) {
  const lower = message.toLowerCase()
  return (
    lower.includes('fetch failed') ||
    lower.includes('timeout') ||
    lower.includes('timed out') ||
    lower.includes('socket hang up') ||
    lower.includes('econnreset') ||
    lower.includes('etimedout') ||
    lower.includes('429') ||
    lower.includes('too many requests') ||
    lower.includes('rate limit') ||
    lower.includes('quota') ||
    lower.includes('credits') ||
    lower.includes('gateway timeout')
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

      if (!isRetryableRpcError(message) || isLastAttempt) {
        throw error
      }

      await sleep(150 * (attempt + 1))
    }
  }

  throw lastError
}

async function contractExistsAt(address: Address, blockNumber: bigint) {
  const code = await withRpcRetries(() => publicClient.getBytecode({ address, blockNumber }))
  return Boolean(code && code !== '0x')
}

async function findContractDeploymentBlock(address: Address) {
  let low = ZERO
  let high = await withRpcRetries(() => publicClient.getBlockNumber())

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

async function getTreasuryAgentStartBlock() {
  return withCache('treasury-agent:start-block', METADATA_CACHE_TTL_MS, async () => {
    const deploymentBlock = await findContractDeploymentBlock(CONTRACTS.treasuryAgent)
    return deploymentBlock > START_BLOCK_PADDING ? deploymentBlock - START_BLOCK_PADDING : ZERO
  })
}

async function getLogsPaged<TEvent extends AbiEvent | undefined>(params: {
  address?: Address
  event: TEvent
  args?: Record<string, unknown>
  fromBlock: bigint
  toBlock?: bigint | 'latest'
}) {
  const latestBlock = params.toBlock === undefined || params.toBlock === 'latest'
    ? await withRpcRetries(() => publicClient.getBlockNumber())
    : params.toBlock

  const logs: Log<bigint, number, false, TEvent>[] = []
  let cursor = params.fromBlock
  let blockRange = LOG_BLOCK_RANGE

  while (cursor <= latestBlock) {
    const endBlock = cursor + blockRange - ONE > latestBlock
      ? latestBlock
      : cursor + blockRange - ONE

    try {
      const batch = await withRpcRetries(() => publicClient.getLogs({
        address: params.address,
        event: params.event as never,
        args: params.args as never,
        fromBlock: cursor,
        toBlock: endBlock,
      }) as Promise<Log<bigint, number, false, TEvent>[]>)

      logs.push(...batch)
      cursor = endBlock + ONE

      if (blockRange < LOG_BLOCK_RANGE) {
        blockRange = LOG_BLOCK_RANGE
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      const shouldShrinkRange = isRangeLimitError(message) || isRetryableRpcError(message)
      if (!shouldShrinkRange || blockRange <= MIN_LOG_BLOCK_RANGE) {
        throw error
      }

      blockRange = blockRange / 2n
      if (blockRange < MIN_LOG_BLOCK_RANGE) {
        blockRange = MIN_LOG_BLOCK_RANGE
      }
    }
  }

  return logs
}

async function getTreasuryAgentConfig() {
  return withCache('treasury-agent:config', 5 * 60 * 1000, async () => {
    const [multisig, asset, loot] = await Promise.all([
      withRpcRetries(() => publicClient.readContract({
        address: CONTRACTS.treasuryAgent,
        abi: configAbi,
        functionName: 'multisig',
      }) as Promise<Address>),
      withRpcRetries(() => publicClient.readContract({
        address: CONTRACTS.treasuryAgent,
        abi: configAbi,
        functionName: 'asset',
      }) as Promise<Address>),
      withRpcRetries(() => publicClient.readContract({
        address: CONTRACTS.treasuryAgent,
        abi: configAbi,
        functionName: 'loot',
      }) as Promise<Address>),
    ])

    return { multisig, asset, loot }
  })
}

async function getCoinGeckoMeta(address: Address | null, native = false) {
  const fallback = native
    ? COINGECKO_FALLBACKS.native_eth
    : (address ? COINGECKO_FALLBACKS[address.toLowerCase()] : undefined)
  const cacheKey = native ? 'coingecko:native:eth' : `coingecko:base:${address?.toLowerCase() ?? 'unknown'}`

  return withCache(cacheKey, METADATA_CACHE_TTL_MS, async () => {
    try {
      const endpoint = native
        ? 'https://api.coingecko.com/api/v3/coins/ethereum?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false&sparkline=false'
        : `https://api.coingecko.com/api/v3/coins/base/contract/${address?.toLowerCase()}`

      const response = await fetch(endpoint, { cache: 'no-store' })
      if (!response.ok) {
        return {
          logoUrl: fallback?.logoUrl ?? null,
          coingeckoUrl: fallback?.coingeckoUrl ?? null,
          name: fallback?.name ?? null,
          symbol: fallback?.symbol ?? null,
        }
      }

      const data = await response.json() as {
        id?: string
        web_slug?: string
        name?: string
        symbol?: string
        image?: { small?: string | null; thumb?: string | null }
      }

      return {
        logoUrl: data.image?.small ?? data.image?.thumb ?? fallback?.logoUrl ?? null,
        coingeckoUrl: data.web_slug
          ? `https://www.coingecko.com/en/coins/${data.web_slug}`
          : data.id
            ? `https://www.coingecko.com/en/coins/${data.id}`
            : fallback?.coingeckoUrl ?? null,
        name: data.name ?? fallback?.name ?? null,
        symbol: data.symbol ? data.symbol.toUpperCase() : fallback?.symbol ?? null,
      }
    } catch {
      return {
        logoUrl: fallback?.logoUrl ?? null,
        coingeckoUrl: fallback?.coingeckoUrl ?? null,
        name: fallback?.name ?? null,
        symbol: fallback?.symbol ?? null,
      }
    }
  })
}

async function getTokenPriceUsd(address: Address | null, native = false) {
  const resolvedAddress = native ? BASE_WETH : address
  if (!resolvedAddress) {
    return 0
  }

  return withCache(`dexscreener:price:${resolvedAddress.toLowerCase()}`, PRICE_CACHE_TTL_MS, async () => {
    try {
      const response = await fetch(`https://api.dexscreener.com/token-pairs/v1/base/${resolvedAddress.toLowerCase()}`, {
        cache: 'no-store',
      })

      if (!response.ok) {
        return resolvedAddress.toLowerCase() === USDC.toLowerCase() ? 1 : 0
      }

      const pairs = await response.json() as Array<{
        priceUsd?: string
        liquidity?: { usd?: number }
        baseToken?: { address?: string | null }
      }>
      if (!Array.isArray(pairs) || pairs.length === 0) {
        return resolvedAddress.toLowerCase() === USDC.toLowerCase() ? 1 : 0
      }

      const target = resolvedAddress.toLowerCase()
      const candidatePairs = pairs.filter((pair) => pair.baseToken?.address?.toLowerCase() === target)
      const bestPair = [...(candidatePairs.length > 0 ? candidatePairs : pairs)]
        .sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0))[0]
      const parsed = Number.parseFloat(bestPair?.priceUsd ?? '0')

      return Number.isFinite(parsed) && parsed > 0
        ? parsed
        : (resolvedAddress.toLowerCase() === USDC.toLowerCase() ? 1 : 0)
    } catch {
      return resolvedAddress.toLowerCase() === USDC.toLowerCase() ? 1 : 0
    }
  })
}

export async function getTreasuryAgentLeaderboard(limit = 12) {
  return withCache(`treasury-agent:leaderboard:${limit}`, TREASURY_AGENT_CACHE_TTL_MS, async () => {
    const startBlock = await getTreasuryAgentStartBlock()

    const [queuedDeposits, claimedRewards] = await Promise.all([
      getLogsPaged({
        address: CONTRACTS.treasuryAgent,
        event: depositQueuedEvent,
        fromBlock: startBlock,
        toBlock: 'latest',
      }),
      getLogsPaged({
        address: CONTRACTS.treasuryAgent,
        event: lootClaimedEvent,
        fromBlock: startBlock,
        toBlock: 'latest',
      }),
    ])

    const claimedLootByUser = new Map<string, bigint>()
    for (const log of claimedRewards) {
      const user = log.args.user?.toLowerCase()
      const amount = log.args.amount ?? ZERO
      if (!user) continue
      claimedLootByUser.set(user, (claimedLootByUser.get(user) ?? ZERO) + amount)
    }

    const users = Array.from(new Set(
      queuedDeposits
        .map((log) => log.args.receiver?.toLowerCase())
        .filter((value): value is string => Boolean(value))
    ))

    if (users.length === 0) {
      return { entries: [] as TreasuryLeaderboardEntry[] }
    }

    const positionResults = await withRpcRetries(() => publicClient.multicall({
      allowFailure: true,
      contracts: users.map((user) => ({
        address: CONTRACTS.treasuryAgent,
        abi: configAbi,
        functionName: 'getUserPosition',
        args: [user as Address],
      })),
    }))

    const entries = users
      .map((user, index) => {
        const result = positionResults[index]
        if (result.status !== 'success') return null

        const position = result.result as unknown as UserPosition
        const pendingDepositAssets = position[1]
        const claimableLoot = position[4]
        const redeemableAssets = position[5]
        const totalRewards = (claimedLootByUser.get(user) ?? ZERO) + claimableLoot

        const deposited = toNumber(redeemableAssets, 6)
        const pending = toNumber(pendingDepositAssets, 6)
        const rewards = toNumber(totalRewards, 18)

        if (deposited <= 0 && pending <= 0 && rewards <= 0) {
          return null
        }

        return {
          address: user,
          deposited,
          pending,
          rewards,
        }
      })
      .filter((entry): entry is { address: string; deposited: number; pending: number; rewards: number } => Boolean(entry))
      .sort((a, b) => {
        if (b.deposited !== a.deposited) return b.deposited - a.deposited
        if (b.pending !== a.pending) return b.pending - a.pending
        return b.rewards - a.rewards
      })
      .slice(0, limit)
      .map((entry, index) => ({
        rank: index + 1,
        address: entry.address,
        depositedFormatted: entry.deposited.toFixed(4),
        pendingFormatted: entry.pending.toFixed(4),
        rewardsFormatted: entry.rewards.toFixed(4),
      }))

    return { entries }
  })
}

export async function getTreasuryAgentHoldings() {
  return withCache('treasury-agent:holdings', TREASURY_AGENT_CACHE_TTL_MS, async () => {
    const config = await getTreasuryAgentConfig()
    const startBlock = await getTreasuryAgentStartBlock()

    const [nativeBalance, incomingLogs, outgoingLogs] = await Promise.all([
      withRpcRetries(() => publicClient.getBalance({ address: config.multisig })),
      getLogsPaged({
        event: transferEvent,
        args: { to: config.multisig },
        fromBlock: startBlock,
        toBlock: 'latest',
      }),
      getLogsPaged({
        event: transferEvent,
        args: { from: config.multisig },
        fromBlock: startBlock,
        toBlock: 'latest',
      }),
    ])

    const candidateAddresses = new Set<string>([
      config.asset.toLowerCase(),
      config.loot.toLowerCase(),
      BASE_WETH.toLowerCase(),
    ])
    incomingLogs.forEach((log) => {
      if (log.address) candidateAddresses.add(log.address.toLowerCase())
    })
    outgoingLogs.forEach((log) => {
      if (log.address) candidateAddresses.add(log.address.toLowerCase())
    })

    const tokenResults = await Promise.all(
      Array.from(candidateAddresses).map(async (address) => {
        try {
          const [balance, symbol, name, decimals] = await Promise.all([
            withRpcRetries(() => publicClient.readContract({
              address: address as Address,
              abi: erc20ReadAbi,
              functionName: 'balanceOf',
              args: [config.multisig],
            }) as Promise<bigint>),
            withRpcRetries(() => publicClient.readContract({
              address: address as Address,
              abi: erc20ReadAbi,
              functionName: 'symbol',
            }) as Promise<string>),
            withRpcRetries(() => publicClient.readContract({
              address: address as Address,
              abi: erc20ReadAbi,
              functionName: 'name',
            }) as Promise<string>),
            withRpcRetries(() => publicClient.readContract({
              address: address as Address,
              abi: erc20ReadAbi,
              functionName: 'decimals',
            }) as Promise<number>),
          ])

          return {
            address: address as Address,
            balance,
            symbol,
            name,
            decimals: Number(decimals),
          }
        } catch {
          return null
        }
      })
    )

    const resolvedTokens = tokenResults.filter((entry): entry is {
      address: Address
      balance: bigint
      symbol: string
      name: string
      decimals: number
    } => Boolean(entry) && (entry?.balance ?? ZERO) > ZERO)

    const prioritizedAddresses = new Set<string>([
      config.asset.toLowerCase(),
      config.loot.toLowerCase(),
      BASE_WETH.toLowerCase(),
    ])

    const entries: TreasuryHoldingEntry[] = []

    if (nativeBalance > ZERO) {
      const nativeMeta = await getCoinGeckoMeta(null, true)
      const nativePriceUsd = await getTokenPriceUsd(null, true)
      const balance = toNumber(nativeBalance, 18)
      const usdValue = balance * nativePriceUsd

      if (usdValue >= MIN_HOLDING_USD) {
        entries.push({
          symbol: 'ETH',
          name: nativeMeta.name ?? 'Ethereum',
          address: null,
          balance: balance.toString(),
          balanceFormatted: formatBalance(nativeBalance, 18),
          usdValue,
          usdValueFormatted: formatUsdValue(usdValue),
          allocation: usdValue,
          decimals: 18,
          logoUrl: nativeMeta.logoUrl,
          coingeckoUrl: nativeMeta.coingeckoUrl,
          isNative: true,
        })
      }
    }

    const tokenEntries = await Promise.all(
      resolvedTokens.map(async (token) => {
        const meta = await getCoinGeckoMeta(token.address)
        const priceUsd = await getTokenPriceUsd(token.address)
        const balance = toNumber(token.balance, token.decimals)
        const usdValue = balance * priceUsd

        if (!meta.logoUrl && !prioritizedAddresses.has(token.address.toLowerCase())) {
          return null
        }

        if (usdValue < MIN_HOLDING_USD) {
          return null
        }

        return {
          symbol: meta.symbol ?? token.symbol.toUpperCase(),
          name: meta.name ?? token.name,
          address: token.address,
          balance: balance.toString(),
          balanceFormatted: formatBalance(token.balance, token.decimals),
          usdValue,
          usdValueFormatted: formatUsdValue(usdValue),
          allocation: usdValue,
          decimals: token.decimals,
          logoUrl: meta.logoUrl,
          coingeckoUrl: meta.coingeckoUrl,
          isNative: false,
        } satisfies TreasuryHoldingEntry
      })
    )

    entries.push(...(tokenEntries.filter(Boolean) as TreasuryHoldingEntry[]))

    const totalUsdValue = entries.reduce((sum, entry) => sum + entry.allocation, 0)
    const normalizedEntries = entries
      .map((entry) => ({
        ...entry,
        allocation: totalUsdValue > 0 ? (entry.allocation / totalUsdValue) * 100 : 0,
      }))
      .sort((a, b) => {
        if (b.allocation !== a.allocation) return b.allocation - a.allocation
        return a.symbol.localeCompare(b.symbol)
      })

    return {
      walletAddress: config.multisig,
      entries: normalizedEntries,
    }
  })
}
