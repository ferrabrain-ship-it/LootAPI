import { encodeAbiParameters, formatUnits, keccak256, parseAbi, parseAbiItem, parseAbiParameters, type AbiEvent, type Address, type Log } from 'viem'
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
  tokenKey: string
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
  protocol: string | null
  locationLabel: string | null
}

const ZERO = 0n
const ONE = 1n
const BASE_WETH = '0x4200000000000000000000000000000000000006' as Address
const USDC = '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913' as Address
const USDT = '0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2' as Address
const AERO = '0x940181a94A35A4569E4529A3CDfB74e38FD98631' as Address
const CBBTC = '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf' as Address
const AERODROME_AERO_USDC_POOL = '0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d' as Address
const AERODROME_AERO_USDC_GAUGE = '0x4F09bAb2f0E15e2A078A227FE1537665F55b8360' as Address
const AERODROME_SLIPSTREAM_MANAGER = '0x827922686190790b37229fd06084350e74485b72' as Address
const AERODROME_SLIPSTREAM_FACTORY = '0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A' as Address
const UNISWAP_V3_MANAGER = '0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1' as Address
const UNISWAP_V3_FACTORY = '0x33128a8fC17869897dcE68Ed026d694621f6FDfD' as Address
const UNISWAP_V4_MANAGER = '0x7C5f5A4bBd8fD63184577525326123B519429bDc' as Address
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

const aavePositionAbi = parseAbi([
  'function UNDERLYING_ASSET_ADDRESS() view returns (address)',
])

const morphoVaultAbi = parseAbi([
  'function MORPHO() view returns (address)',
  'function asset() view returns (address)',
  'function convertToAssets(uint256 shares) view returns (uint256)',
])

const aerodromeGaugeAbi = parseAbi([
  'function balanceOf(address) view returns (uint256)',
])

const aerodromePoolAbi = parseAbi([
  'function token0() view returns (address)',
  'function token1() view returns (address)',
  'function totalSupply() view returns (uint256)',
  'function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)',
])

const concentratedPositionTransferEvent = parseAbiItem(
  'event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)'
)

const uniswapV3ManagerAbi = parseAbi([
  'function ownerOf(uint256 tokenId) view returns (address)',
  'function positions(uint256 tokenId) view returns (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)',
])

const aerodromeSlipstreamManagerAbi = parseAbi([
  'function ownerOf(uint256 tokenId) view returns (address)',
  'function positions(uint256 tokenId) view returns (uint96 nonce, address operator, address token0, address token1, int24 tickSpacing, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)',
])

const uniswapV3FactoryAbi = parseAbi([
  'function getPool(address tokenA, address tokenB, uint24 fee) view returns (address)',
])

const aerodromeSlipstreamFactoryAbi = parseAbi([
  'function getPool(address tokenA, address tokenB, int24 tickSpacing) view returns (address)',
])

const uniswapV3PoolAbi = parseAbi([
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)',
])

const aerodromeSlipstreamPoolAbi = parseAbi([
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked)',
])

const uniswapV4ManagerAbi = parseAbi([
  'function ownerOf(uint256 tokenId) view returns (address)',
  'function poolManager() view returns (address)',
  'function getPoolAndPositionInfo(uint256 tokenId) view returns ((address,address,uint24,int24,address), uint256)',
  'function getPositionLiquidity(uint256 tokenId) view returns (uint128)',
])

const uniswapV4PoolManagerAbi = parseAbi([
  'function getSlot0(bytes32 poolId) view returns (uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)',
])

const v4PoolKeyAbiParameters = parseAbiParameters('address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks')

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
  [USDT.toLowerCase()]: {
    logoUrl: 'https://coin-images.coingecko.com/coins/images/325/small/Tether.png?1696501661',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/tether',
    name: 'Tether USD',
    symbol: 'USDT',
  },
  [BASE_WETH.toLowerCase()]: {
    logoUrl: 'https://coin-images.coingecko.com/coins/images/39810/small/weth.png?1724139790',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/l2-standard-bridged-weth-base',
    name: 'Wrapped Ether',
    symbol: 'WETH',
  },
  [AERO.toLowerCase()]: {
    logoUrl: 'https://cdn.dexscreener.com/cms/images/f950c4e0a9ded4f6b5c811cf33570a65c6f7ea8630d937cd69065269e9dc3a03?width=800&height=800&quality=95&format=auto',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/aerodrome-finance',
    name: 'Aerodrome Finance',
    symbol: 'AERO',
  },
  [CBBTC.toLowerCase()]: {
    logoUrl: 'https://coin-images.coingecko.com/coins/images/40143/small/cbbtc.webp?1726136727',
    coingeckoUrl: 'https://www.coingecko.com/en/coins/coinbase-wrapped-btc',
    name: 'Coinbase Wrapped BTC',
    symbol: 'CBBTC',
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

const Q96 = 1n << 96n
const MAX_UINT256 = (1n << 256n) - 1n

function mulDiv(a: bigint, b: bigint, denominator: bigint) {
  if (denominator === ZERO) return ZERO
  return (a * b) / denominator
}

function getSqrtRatioAtTick(tick: number) {
  let absTick = BigInt(tick < 0 ? -tick : tick)
  let ratio = (absTick & 0x1n) !== 0n
    ? 0xfffcb933bd6fad37aa2d162d1a594001n
    : 0x100000000000000000000000000000000n

  if (absTick & 0x2n) ratio = (ratio * 0xfff97272373d413259a46990580e213an) >> 128n
  if (absTick & 0x4n) ratio = (ratio * 0xfff2e50f5f656932ef12357cf3c7fdccn) >> 128n
  if (absTick & 0x8n) ratio = (ratio * 0xffe5caca7e10e4e61c3624eaa0941cd0n) >> 128n
  if (absTick & 0x10n) ratio = (ratio * 0xffcb9843d60f6159c9db58835c926644n) >> 128n
  if (absTick & 0x20n) ratio = (ratio * 0xff973b41fa98c081472e6896dfb254c0n) >> 128n
  if (absTick & 0x40n) ratio = (ratio * 0xff2ea16466c96a3843ec78b326b52861n) >> 128n
  if (absTick & 0x80n) ratio = (ratio * 0xfe5dee046a99a2a811c461f1969c3053n) >> 128n
  if (absTick & 0x100n) ratio = (ratio * 0xfcbe86c7900a88aedcffc83b479aa3a4n) >> 128n
  if (absTick & 0x200n) ratio = (ratio * 0xf987a7253ac413176f2b074cf7815e54n) >> 128n
  if (absTick & 0x400n) ratio = (ratio * 0xf3392b0822b70005940c7a398e4b70f3n) >> 128n
  if (absTick & 0x800n) ratio = (ratio * 0xe7159475a2c29b7443b29c7fa6e889d9n) >> 128n
  if (absTick & 0x1000n) ratio = (ratio * 0xd097f3bdfd2022b8845ad8f792aa5825n) >> 128n
  if (absTick & 0x2000n) ratio = (ratio * 0xa9f746462d870fdf8a65dc1f90e061e5n) >> 128n
  if (absTick & 0x4000n) ratio = (ratio * 0x70d869a156d2a1b890bb3df62baf32f7n) >> 128n
  if (absTick & 0x8000n) ratio = (ratio * 0x31be135f97d08fd981231505542fcfa6n) >> 128n
  if (absTick & 0x10000n) ratio = (ratio * 0x09aa508b5b7a84e1c677de54f3e99bc9n) >> 128n
  if (absTick & 0x20000n) ratio = (ratio * 0x005d6af8dedb81196699c329225ee604n) >> 128n
  if (absTick & 0x40000n) ratio = (ratio * 0x0002216e584f5fa1ea926041bedfe98n) >> 128n
  if (absTick & 0x80000n) ratio = (ratio * 0x000048a170391f7dc42444e8fa2n) >> 128n

  if (tick > 0) {
    ratio = MAX_UINT256 / ratio
  }

  return (ratio >> 32n) + ((ratio & ((1n << 32n) - 1n)) === 0n ? 0n : 1n)
}

function getAmountsForLiquidity(
  sqrtPriceX96: bigint,
  sqrtPriceAX96: bigint,
  sqrtPriceBX96: bigint,
  liquidity: bigint,
) {
  if (sqrtPriceAX96 > sqrtPriceBX96) {
    [sqrtPriceAX96, sqrtPriceBX96] = [sqrtPriceBX96, sqrtPriceAX96]
  }

  let amount0 = ZERO
  let amount1 = ZERO

  if (sqrtPriceX96 <= sqrtPriceAX96) {
    amount0 = mulDiv(liquidity << 96n, sqrtPriceBX96 - sqrtPriceAX96, sqrtPriceBX96 * sqrtPriceAX96)
  } else if (sqrtPriceX96 < sqrtPriceBX96) {
    amount0 = mulDiv(liquidity << 96n, sqrtPriceBX96 - sqrtPriceX96, sqrtPriceBX96 * sqrtPriceX96)
    amount1 = mulDiv(liquidity, sqrtPriceX96 - sqrtPriceAX96, Q96)
  } else {
    amount1 = mulDiv(liquidity, sqrtPriceBX96 - sqrtPriceAX96, Q96)
  }

  return [amount0, amount1] as const
}

function decodeSigned24(value: bigint) {
  const masked = Number(value & 0xffffffn)
  return masked >= 0x800000 ? masked - 0x1000000 : masked
}

function isStablePairSymbol(symbol: string | null | undefined) {
  const normalized = (symbol ?? '').toUpperCase()
  return normalized === 'USDC' || normalized === 'USDT' || normalized === 'DAI'
}

function pickPreferredPairMeta(params: {
  token0Symbol: string
  token1Symbol: string
  token0Meta: { logoUrl: string | null; coingeckoUrl: string | null }
  token1Meta: { logoUrl: string | null; coingeckoUrl: string | null }
  token0UsdValue: number
  token1UsdValue: number
}) {
  const token0IsStable = isStablePairSymbol(params.token0Symbol)
  const token1IsStable = isStablePairSymbol(params.token1Symbol)

  if (params.token0Symbol === 'WETH' || params.token0Symbol === 'ETH') {
    return params.token0Meta
  }

  if (params.token1Symbol === 'WETH' || params.token1Symbol === 'ETH') {
    return params.token1Meta
  }

  if (!token0IsStable && token1IsStable) {
    return params.token0Meta
  }

  if (!token1IsStable && token0IsStable) {
    return params.token1Meta
  }

  return params.token0UsdValue >= params.token1UsdValue ? params.token0Meta : params.token1Meta
}

async function readTokenDescriptor(address: Address) {
  const [symbol, name, decimals] = await Promise.all([
    withRpcRetries(() => publicClient.readContract({
      address,
      abi: erc20ReadAbi,
      functionName: 'symbol',
    }) as Promise<string>),
    withRpcRetries(() => publicClient.readContract({
      address,
      abi: erc20ReadAbi,
      functionName: 'name',
    }) as Promise<string>),
    withRpcRetries(() => publicClient.readContract({
      address,
      abi: erc20ReadAbi,
      functionName: 'decimals',
    }) as Promise<number>),
  ])

  return {
    symbol,
    name,
    decimals: Number(decimals),
  }
}

async function buildProtocolHoldingEntry(params: {
  tokenAddress: Address
  balance: bigint
  protocol: 'Aave' | 'Morpho' | 'Aerodrome'
  locationLabel: string
  underlyingAddress: Address
  underlyingBalance: bigint
}) {
  const [{ symbol, name, decimals }, meta, priceUsd] = await Promise.all([
    readTokenDescriptor(params.underlyingAddress),
    getCoinGeckoMeta(params.underlyingAddress),
    getTokenPriceUsd(params.underlyingAddress),
  ])

  const balance = toNumber(params.underlyingBalance, decimals)
  const usdValue = balance * priceUsd

  if (usdValue < MIN_HOLDING_USD) {
    return null
  }

  return {
    tokenKey: `${params.protocol.toLowerCase()}:${params.tokenAddress.toLowerCase()}`,
    symbol: meta.symbol ?? symbol.toUpperCase(),
    name: meta.name ?? name,
    address: params.underlyingAddress,
    balance: balance.toString(),
    balanceFormatted: formatBalance(params.underlyingBalance, decimals),
    usdValue,
    usdValueFormatted: formatUsdValue(usdValue),
    allocation: usdValue,
    decimals,
    logoUrl: meta.logoUrl,
    coingeckoUrl: meta.coingeckoUrl,
    isNative: false,
    protocol: params.protocol,
    locationLabel: params.locationLabel,
  } satisfies TreasuryHoldingEntry
}

async function getManagedPositionTokenIds(
  managerAddress: Address,
  walletAddress: Address,
  startBlock: bigint,
) {
  return withCache(`treasury-agent:position-ids:${managerAddress.toLowerCase()}:${walletAddress.toLowerCase()}`, TREASURY_AGENT_CACHE_TTL_MS, async () => {
    const [incoming, outgoing] = await Promise.all([
      getLogsPaged({
        address: managerAddress,
        event: concentratedPositionTransferEvent,
        args: { to: walletAddress },
        fromBlock: startBlock,
        toBlock: 'latest',
      }),
      getLogsPaged({
        address: managerAddress,
        event: concentratedPositionTransferEvent,
        args: { from: walletAddress },
        fromBlock: startBlock,
        toBlock: 'latest',
      }),
    ])

    const tokenIds = new Set<bigint>()
    for (const log of [...incoming, ...outgoing]) {
      if (typeof log.args.tokenId === 'bigint') {
        tokenIds.add(log.args.tokenId)
      }
    }

    return [...tokenIds]
  })
}

async function getManagedPositionOwnerContext(
  managerAddress: Address,
  tokenId: bigint,
  walletAddress: Address,
) {
  try {
    const owner = await withRpcRetries(() => publicClient.readContract({
      address: managerAddress,
      abi: parseAbi(['function ownerOf(uint256 tokenId) view returns (address)']),
      functionName: 'ownerOf',
      args: [tokenId],
    }) as Promise<Address>)

    if (owner.toLowerCase() === walletAddress.toLowerCase()) {
      return {
        owner,
        heldDirectly: true,
      }
    }

    const code = await withRpcRetries(() => publicClient.getBytecode({ address: owner }))
    if (!code || code === '0x') {
      return null
    }

    return {
      owner,
      heldDirectly: false,
    }
  } catch {
    return null
  }
}

async function buildConcentratedLiquidityEntry(params: {
  tokenKey: string
  symbol: string
  name: string
  address: Address
  managerAddress: Address
  tokenId: bigint
  token0Address: Address
  token1Address: Address
  amount0: bigint
  amount1: bigint
  protocol: string
  locationLabel: string
}) {
  const [
    token0Descriptor,
    token1Descriptor,
    token0Meta,
    token1Meta,
    token0PriceUsd,
    token1PriceUsd,
  ] = await Promise.all([
    readTokenDescriptor(params.token0Address),
    readTokenDescriptor(params.token1Address),
    getCoinGeckoMeta(params.token0Address),
    getCoinGeckoMeta(params.token1Address),
    getTokenPriceUsd(params.token0Address),
    getTokenPriceUsd(params.token1Address),
  ])

  const token0UsdValue = toNumber(params.amount0, token0Descriptor.decimals) * token0PriceUsd
  const token1UsdValue = toNumber(params.amount1, token1Descriptor.decimals) * token1PriceUsd
  const usdValue = token0UsdValue + token1UsdValue

  if (usdValue < MIN_HOLDING_USD) {
    return null
  }

  const token0Symbol = token0Meta.symbol ?? token0Descriptor.symbol.toUpperCase()
  const token1Symbol = token1Meta.symbol ?? token1Descriptor.symbol.toUpperCase()
  const preferredMeta = pickPreferredPairMeta({
    token0Symbol,
    token1Symbol,
    token0Meta,
    token1Meta,
    token0UsdValue,
    token1UsdValue,
  })

  return {
    tokenKey: params.tokenKey,
    symbol: params.symbol,
    name: params.name,
    address: params.address,
    balance: '1',
    balanceFormatted: '1',
    usdValue,
    usdValueFormatted: formatUsdValue(usdValue),
    allocation: usdValue,
    decimals: 0,
    logoUrl: preferredMeta.logoUrl ?? token0Meta.logoUrl ?? token1Meta.logoUrl,
    coingeckoUrl: preferredMeta.coingeckoUrl ?? token0Meta.coingeckoUrl ?? token1Meta.coingeckoUrl,
    isNative: false,
    protocol: params.protocol,
    locationLabel: params.locationLabel,
  } satisfies TreasuryHoldingEntry
}

async function buildV3LikePositionEntry(params: {
  managerAddress: Address
  poolAddress: Address
  tokenId: bigint
  token0Address: Address
  token1Address: Address
  tickLower: number
  tickUpper: number
  liquidity: bigint
  tokensOwed0: bigint
  tokensOwed1: bigint
  sqrtPriceX96: bigint
  protocol: 'Uniswap V3' | 'Aerodrome'
  locationLabel: string
  symbolSuffix: string
}) {
  const [amount0Principal, amount1Principal] = getAmountsForLiquidity(
    params.sqrtPriceX96,
    getSqrtRatioAtTick(params.tickLower),
    getSqrtRatioAtTick(params.tickUpper),
    params.liquidity,
  )

  const amount0 = amount0Principal + params.tokensOwed0
  const amount1 = amount1Principal + params.tokensOwed1

  const token0Meta = await getCoinGeckoMeta(params.token0Address)
  const token1Meta = await getCoinGeckoMeta(params.token1Address)
  const token0Symbol = token0Meta.symbol ?? (await readTokenDescriptor(params.token0Address)).symbol.toUpperCase()
  const token1Symbol = token1Meta.symbol ?? (await readTokenDescriptor(params.token1Address)).symbol.toUpperCase()

  return buildConcentratedLiquidityEntry({
    tokenKey: `${params.protocol.toLowerCase().replace(/\s+/g, '-')}:${params.tokenId.toString()}`,
    symbol: `${token0Symbol}/${token1Symbol} ${params.symbolSuffix}`,
    name: `${params.protocol} #${params.tokenId.toString()}`,
    address: params.poolAddress,
    managerAddress: params.managerAddress,
    tokenId: params.tokenId,
    token0Address: params.token0Address,
    token1Address: params.token1Address,
    amount0,
    amount1,
    protocol: params.protocol,
    locationLabel: params.locationLabel,
  })
}

async function getManagedUniswapV3PositionEntries(walletAddress: Address, startBlock: bigint) {
  const tokenIds = await getManagedPositionTokenIds(UNISWAP_V3_MANAGER, walletAddress, startBlock)
  if (tokenIds.length === 0) {
    return []
  }

  const entries = await Promise.all(tokenIds.map(async (tokenId) => {
    const ownerContext = await getManagedPositionOwnerContext(UNISWAP_V3_MANAGER, tokenId, walletAddress)
    if (!ownerContext) {
      return null
    }

    try {
      const position = await withRpcRetries(() => publicClient.readContract({
        address: UNISWAP_V3_MANAGER,
        abi: uniswapV3ManagerAbi,
        functionName: 'positions',
        args: [tokenId],
      }) as Promise<readonly [bigint, Address, Address, Address, number, number, number, bigint, bigint, bigint, bigint, bigint]>)

      const poolAddress = await withRpcRetries(() => publicClient.readContract({
        address: UNISWAP_V3_FACTORY,
        abi: uniswapV3FactoryAbi,
        functionName: 'getPool',
        args: [position[2], position[3], position[4]],
      }) as Promise<Address>)

      if (!poolAddress || poolAddress === '0x0000000000000000000000000000000000000000') {
        return null
      }

      const slot0 = await withRpcRetries(() => publicClient.readContract({
        address: poolAddress,
        abi: uniswapV3PoolAbi,
        functionName: 'slot0',
      }) as Promise<readonly [bigint, number, number, number, number, number, boolean]>)

      return buildV3LikePositionEntry({
        managerAddress: UNISWAP_V3_MANAGER,
        poolAddress,
        tokenId,
        token0Address: position[2],
        token1Address: position[3],
        tickLower: position[5],
        tickUpper: position[6],
        liquidity: position[7],
        tokensOwed0: position[10],
        tokensOwed1: position[11],
        sqrtPriceX96: slot0[0],
        protocol: 'Uniswap V3',
        locationLabel: ownerContext.heldDirectly ? 'Held on Uniswap V3' : 'Managed in external contract',
        symbolSuffix: 'V3',
      })
    } catch {
      return null
    }
  }))

  return entries.filter(Boolean) as TreasuryHoldingEntry[]
}

async function getManagedAerodromeSlipstreamPositionEntries(walletAddress: Address, startBlock: bigint) {
  const tokenIds = await getManagedPositionTokenIds(AERODROME_SLIPSTREAM_MANAGER, walletAddress, startBlock)
  if (tokenIds.length === 0) {
    return []
  }

  const entries = await Promise.all(tokenIds.map(async (tokenId) => {
    const ownerContext = await getManagedPositionOwnerContext(AERODROME_SLIPSTREAM_MANAGER, tokenId, walletAddress)
    if (!ownerContext) {
      return null
    }

    try {
      const position = await withRpcRetries(() => publicClient.readContract({
        address: AERODROME_SLIPSTREAM_MANAGER,
        abi: aerodromeSlipstreamManagerAbi,
        functionName: 'positions',
        args: [tokenId],
      }) as Promise<readonly [bigint, Address, Address, Address, number, number, number, bigint, bigint, bigint, bigint, bigint]>)

      const poolAddress = await withRpcRetries(() => publicClient.readContract({
        address: AERODROME_SLIPSTREAM_FACTORY,
        abi: aerodromeSlipstreamFactoryAbi,
        functionName: 'getPool',
        args: [position[2], position[3], position[4]],
      }) as Promise<Address>)

      if (!poolAddress || poolAddress === '0x0000000000000000000000000000000000000000') {
        return null
      }

      const slot0 = await withRpcRetries(() => publicClient.readContract({
        address: poolAddress,
        abi: aerodromeSlipstreamPoolAbi,
        functionName: 'slot0',
      }) as Promise<readonly [bigint, number, number, number, number, boolean]>)

      return buildV3LikePositionEntry({
        managerAddress: AERODROME_SLIPSTREAM_MANAGER,
        poolAddress,
        tokenId,
        token0Address: position[2],
        token1Address: position[3],
        tickLower: position[5],
        tickUpper: position[6],
        liquidity: position[7],
        tokensOwed0: position[10],
        tokensOwed1: position[11],
        sqrtPriceX96: slot0[0],
        protocol: 'Aerodrome',
        locationLabel: ownerContext.heldDirectly ? 'Held on Aerodrome Slipstream' : 'Staked on Aerodrome Slipstream',
        symbolSuffix: 'CL',
      })
    } catch {
      return null
    }
  }))

  return entries.filter(Boolean) as TreasuryHoldingEntry[]
}

async function getManagedUniswapV4PositionEntries(walletAddress: Address, startBlock: bigint) {
  const tokenIds = await getManagedPositionTokenIds(UNISWAP_V4_MANAGER, walletAddress, startBlock)
  if (tokenIds.length === 0) {
    return []
  }

  const poolManager = await withRpcRetries(() => publicClient.readContract({
    address: UNISWAP_V4_MANAGER,
    abi: uniswapV4ManagerAbi,
    functionName: 'poolManager',
  }) as Promise<Address>)

  const entries = await Promise.all(tokenIds.map(async (tokenId) => {
    const ownerContext = await getManagedPositionOwnerContext(UNISWAP_V4_MANAGER, tokenId, walletAddress)
    if (!ownerContext) {
      return null
    }

    try {
      const [[poolKey, info], liquidity] = await Promise.all([
        withRpcRetries(() => publicClient.readContract({
          address: UNISWAP_V4_MANAGER,
          abi: uniswapV4ManagerAbi,
          functionName: 'getPoolAndPositionInfo',
          args: [tokenId],
        }) as Promise<readonly [readonly [Address, Address, number, number, Address], bigint]>),
        withRpcRetries(() => publicClient.readContract({
          address: UNISWAP_V4_MANAGER,
          abi: uniswapV4ManagerAbi,
          functionName: 'getPositionLiquidity',
          args: [tokenId],
        }) as Promise<bigint>),
      ])

      const [currency0, currency1, fee, tickSpacing, hooks] = poolKey
      const poolId = keccak256(encodeAbiParameters(v4PoolKeyAbiParameters, [currency0, currency1, fee, tickSpacing, hooks]))
      const slot0 = await withRpcRetries(() => publicClient.readContract({
        address: poolManager,
        abi: uniswapV4PoolManagerAbi,
        functionName: 'getSlot0',
        args: [poolId],
      }) as Promise<readonly [bigint, number, number, number]>)

      const tickLower = decodeSigned24(info >> 8n)
      const tickUpper = decodeSigned24(info >> 32n)

      const [amount0, amount1] = getAmountsForLiquidity(
        slot0[0],
        getSqrtRatioAtTick(tickLower),
        getSqrtRatioAtTick(tickUpper),
        liquidity,
      )

      const token0Meta = await getCoinGeckoMeta(currency0)
      const token1Meta = await getCoinGeckoMeta(currency1)
      const token0Symbol = token0Meta.symbol ?? (await readTokenDescriptor(currency0)).symbol.toUpperCase()
      const token1Symbol = token1Meta.symbol ?? (await readTokenDescriptor(currency1)).symbol.toUpperCase()

      return buildConcentratedLiquidityEntry({
        tokenKey: `uniswap-v4:${tokenId.toString()}`,
        symbol: `${token0Symbol}/${token1Symbol} V4`,
        name: `Uniswap V4 #${tokenId.toString()}`,
        address: UNISWAP_V4_MANAGER,
        managerAddress: UNISWAP_V4_MANAGER,
        tokenId,
        token0Address: currency0,
        token1Address: currency1,
        amount0,
        amount1,
        protocol: 'Uniswap V4',
        locationLabel: ownerContext.heldDirectly ? 'Held on Uniswap V4' : 'Managed in external contract',
      })
    } catch {
      return null
    }
  }))

  return entries.filter(Boolean) as TreasuryHoldingEntry[]
}

async function tryResolveAaveHolding(token: {
  address: Address
  balance: bigint
}) {
  try {
    const underlyingAddress = await withRpcRetries(() => publicClient.readContract({
      address: token.address,
      abi: aavePositionAbi,
      functionName: 'UNDERLYING_ASSET_ADDRESS',
    }) as Promise<Address>)

    return buildProtocolHoldingEntry({
      tokenAddress: token.address,
      balance: token.balance,
      protocol: 'Aave',
      locationLabel: 'Supplied on Aave',
      underlyingAddress,
      underlyingBalance: token.balance,
    })
  } catch {
    return null
  }
}

async function tryResolveMorphoHolding(token: {
  address: Address
  balance: bigint
}) {
  try {
    await withRpcRetries(() => publicClient.readContract({
      address: token.address,
      abi: morphoVaultAbi,
      functionName: 'MORPHO',
    }) as Promise<Address>)

    const [underlyingAddress, underlyingBalance] = await Promise.all([
      withRpcRetries(() => publicClient.readContract({
        address: token.address,
        abi: morphoVaultAbi,
        functionName: 'asset',
      }) as Promise<Address>),
      withRpcRetries(() => publicClient.readContract({
        address: token.address,
        abi: morphoVaultAbi,
        functionName: 'convertToAssets',
        args: [token.balance],
      }) as Promise<bigint>),
    ])

    return buildProtocolHoldingEntry({
      tokenAddress: token.address,
      balance: token.balance,
      protocol: 'Morpho',
      locationLabel: 'Supplied on Morpho',
      underlyingAddress,
      underlyingBalance,
    })
  } catch {
    return null
  }
}

async function tryResolveAerodromeHolding(token: {
  address: Address
  balance: bigint
  decimals: number
}, walletAddress: Address) {
  if (token.address.toLowerCase() !== AERODROME_AERO_USDC_POOL.toLowerCase()) {
    return null
  }

  try {
    const [stakedBalance, totalSupply, reserves, token0Address, token1Address] = await Promise.all([
      withRpcRetries(() => publicClient.readContract({
        address: AERODROME_AERO_USDC_GAUGE,
        abi: aerodromeGaugeAbi,
        functionName: 'balanceOf',
        args: [walletAddress],
      }) as Promise<bigint>),
      withRpcRetries(() => publicClient.readContract({
        address: AERODROME_AERO_USDC_POOL,
        abi: aerodromePoolAbi,
        functionName: 'totalSupply',
      }) as Promise<bigint>),
      withRpcRetries(() => publicClient.readContract({
        address: AERODROME_AERO_USDC_POOL,
        abi: aerodromePoolAbi,
        functionName: 'getReserves',
      }) as Promise<readonly [bigint, bigint, number]>),
      withRpcRetries(() => publicClient.readContract({
        address: AERODROME_AERO_USDC_POOL,
        abi: aerodromePoolAbi,
        functionName: 'token0',
      }) as Promise<Address>),
      withRpcRetries(() => publicClient.readContract({
        address: AERODROME_AERO_USDC_POOL,
        abi: aerodromePoolAbi,
        functionName: 'token1',
      }) as Promise<Address>),
    ])

    if (stakedBalance <= ZERO || totalSupply <= ZERO) {
      return null
    }

    const [reserve0, reserve1] = reserves
    const underlying0 = (reserve0 * stakedBalance) / totalSupply
    const underlying1 = (reserve1 * stakedBalance) / totalSupply

    const [
      token0Descriptor,
      token1Descriptor,
      token0Meta,
      token1Meta,
      token0PriceUsd,
      token1PriceUsd,
    ] = await Promise.all([
      readTokenDescriptor(token0Address),
      readTokenDescriptor(token1Address),
      getCoinGeckoMeta(token0Address),
      getCoinGeckoMeta(token1Address),
      getTokenPriceUsd(token0Address),
      getTokenPriceUsd(token1Address),
    ])

    const token0UsdValue = toNumber(underlying0, token0Descriptor.decimals) * token0PriceUsd
    const token1UsdValue = toNumber(underlying1, token1Descriptor.decimals) * token1PriceUsd
    const usdValue = token0UsdValue + token1UsdValue

    if (usdValue < MIN_HOLDING_USD) {
      return null
    }

    const token0Symbol = token0Meta.symbol ?? token0Descriptor.symbol.toUpperCase()
    const token1Symbol = token1Meta.symbol ?? token1Descriptor.symbol.toUpperCase()
    const pairSymbol = (
      token0Address.toLowerCase() === USDC.toLowerCase() && token1Address.toLowerCase() === AERO.toLowerCase()
        ? `${token1Symbol}/${token0Symbol} LP`
        : `${token0Symbol}/${token1Symbol} LP`
    )
    const primaryMeta = token1Address.toLowerCase() === AERO.toLowerCase() ? token1Meta : token0Meta

    return {
      tokenKey: `aerodrome:${AERODROME_AERO_USDC_GAUGE.toLowerCase()}`,
      symbol: pairSymbol,
      name: 'Aerodrome AERO/USDC LP',
      address: AERODROME_AERO_USDC_POOL,
      balance: toNumber(stakedBalance, token.decimals).toString(),
      balanceFormatted: formatBalance(stakedBalance, token.decimals),
      usdValue,
      usdValueFormatted: formatUsdValue(usdValue),
      allocation: usdValue,
      decimals: token.decimals,
      logoUrl: primaryMeta.logoUrl,
      coingeckoUrl: primaryMeta.coingeckoUrl,
      isNative: false,
      protocol: 'Aerodrome',
      locationLabel: 'Staked on Aerodrome',
    } satisfies TreasuryHoldingEntry
  } catch {
    return null
  }
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
      USDT.toLowerCase(),
      CBBTC.toLowerCase(),
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

    const discoveredTokens = tokenResults.filter((entry): entry is {
      address: Address
      balance: bigint
      symbol: string
      name: string
      decimals: number
    } => Boolean(entry))

    const prioritizedAddresses = new Set<string>([
      config.asset.toLowerCase(),
      config.loot.toLowerCase(),
      BASE_WETH.toLowerCase(),
      USDT.toLowerCase(),
      CBBTC.toLowerCase(),
    ])

    const entries: TreasuryHoldingEntry[] = []

    const [uniswapV3PositionEntries, uniswapV4PositionEntries, aerodromeSlipstreamEntries] = await Promise.all([
      getManagedUniswapV3PositionEntries(config.multisig, startBlock),
      getManagedUniswapV4PositionEntries(config.multisig, startBlock),
      getManagedAerodromeSlipstreamPositionEntries(config.multisig, startBlock),
    ])

    if (nativeBalance > ZERO) {
      const nativeMeta = await getCoinGeckoMeta(null, true)
      const nativePriceUsd = await getTokenPriceUsd(null, true)
      const balance = toNumber(nativeBalance, 18)
      const usdValue = balance * nativePriceUsd

      if (usdValue >= MIN_HOLDING_USD) {
        entries.push({
          tokenKey: 'native',
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
          protocol: null,
          locationLabel: null,
        })
      }
    }

    const tokenEntries = await Promise.all(
      discoveredTokens.map(async (token) => {
        const protocolEntry = (
          (token.balance > ZERO ? await tryResolveAaveHolding(token) ?? await tryResolveMorphoHolding(token) : null)
          ?? await tryResolveAerodromeHolding(token, config.multisig)
        )
        if (protocolEntry) {
          return protocolEntry
        }

        if (token.balance <= ZERO) {
          return null
        }

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
          tokenKey: token.address.toLowerCase(),
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
          protocol: null,
          locationLabel: null,
        } satisfies TreasuryHoldingEntry
      })
    )

    entries.push(...(tokenEntries.filter(Boolean) as TreasuryHoldingEntry[]))
    entries.push(...uniswapV3PositionEntries)
    entries.push(...uniswapV4PositionEntries)
    entries.push(...aerodromeSlipstreamEntries)

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
