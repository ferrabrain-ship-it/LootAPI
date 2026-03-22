import {
  ActionRowBuilder,
  AttachmentBuilder,
  ButtonBuilder,
  ButtonStyle,
  Client,
  EmbedBuilder,
  GatewayIntentBits,
  type Message,
} from 'discord.js'
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>

type Pair = {
  chainId?: string
  dexId?: string
  url?: string
  pairAddress?: string
  priceUsd?: string
  priceChange?: {
    h24?: number
    h6?: number
    h1?: number
  }
  volume?: {
    h24?: number
    h6?: number
    h1?: number
  }
  liquidity?: {
    usd?: number
  }
  info?: {
    openGraph?: string
  }
}

const SUPPORTED_WINDOWS = new Set([
  '15m',
  '1h',
  '4h',
  '12h',
  '24h',
  '1d',
  '7d',
  '1w',
])

type OhlcvTimeframe = 'minute' | 'hour' | 'day'

type OhlcvWindowConfig = {
  timeframe: OhlcvTimeframe
  aggregate: number
  limit: number
}

type OhlcvPoint = {
  timestampSec: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

const WINDOW_CONFIG: Record<string, OhlcvWindowConfig> = {
  '15m': { timeframe: 'minute', aggregate: 15, limit: 96 },
  '1h': { timeframe: 'hour', aggregate: 1, limit: 96 },
  '4h': { timeframe: 'hour', aggregate: 4, limit: 96 },
  '12h': { timeframe: 'hour', aggregate: 12, limit: 96 },
  '24h': { timeframe: 'day', aggregate: 1, limit: 90 },
  '1d': { timeframe: 'day', aggregate: 1, limit: 90 },
  '7d': { timeframe: 'day', aggregate: 7, limit: 90 },
  '1w': { timeframe: 'day', aggregate: 7, limit: 90 },
}

function numeric(value: unknown) {
  const parsed = typeof value === 'number' ? value : Number.parseFloat(String(value ?? ''))
  return Number.isFinite(parsed) ? parsed : 0
}

function formatSignedPercent(value: number) {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

function formatUsd(value: number) {
  if (!Number.isFinite(value)) return '$0.00'
  if (value >= 1) {
    return `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 4 }).format(value)}`
  }
  return `$${value.toFixed(6)}`
}

function parseCommandWindow(content: string) {
  const trimmed = content.trim().toLowerCase()
  const tokens = trimmed.split(/\s+/).filter(Boolean)
  if (!tokens.length) return null

  const prefix = env.discordPriceCommandPrefix || 'price'
  if (tokens[0] !== prefix && tokens[0] !== `!${prefix}` && tokens[0] !== `/${prefix}`) {
    return null
  }

  if (tokens.length < 2) return '4h'
  const requested = tokens[1]
  if (!SUPPORTED_WINDOWS.has(requested)) return null
  return requested
}

function decoratePairUrl(url: string, requestedWindow: string) {
  try {
    const parsed = new URL(url)
    parsed.searchParams.set('utm_source', 'mineloot-discord-bot')
    parsed.searchParams.set('interval', requestedWindow)
    return parsed.toString()
  } catch {
    return url
  }
}

async function getBestLootPair() {
  const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${CONTRACTS.loot}`, {
    cache: 'no-store',
  })

  if (!response.ok) {
    throw new Error(`DexScreener ${response.status}`)
  }

  const data = await response.json() as { pairs?: Pair[] }
  const pairs = data.pairs ?? []
  if (!pairs.length) {
    throw new Error('No LOOT pair available on DexScreener')
  }

  return [...pairs].sort(
    (left, right) => numeric(right.liquidity?.usd) - numeric(left.liquidity?.usd)
  )[0]
}

async function getOhlcvFromGecko(pairAddress: string, requestedWindow: string) {
  const config = WINDOW_CONFIG[requestedWindow]
  if (!config) {
    throw new Error(`Unsupported window: ${requestedWindow}`)
  }

  const url = new URL(
    `https://api.geckoterminal.com/api/v2/networks/base/pools/${pairAddress}/ohlcv/${config.timeframe}`
  )
  url.searchParams.set('aggregate', String(config.aggregate))
  url.searchParams.set('limit', String(config.limit))
  url.searchParams.set('currency', 'usd')

  const response = await fetch(url.toString(), { cache: 'no-store' })
  if (!response.ok) {
    throw new Error(`GeckoTerminal ${response.status}`)
  }

  const data = await response.json() as {
    data?: {
      attributes?: {
        ohlcv_list?: Array<[number, number, number, number, number, number]>
      }
    }
  }

  const raw = data.data?.attributes?.ohlcv_list ?? []
  const parsed: OhlcvPoint[] = raw
    .map((entry) => ({
      timestampSec: Number(entry[0]),
      open: Number(entry[1]),
      high: Number(entry[2]),
      low: Number(entry[3]),
      close: Number(entry[4]),
      volume: Number(entry[5]),
    }))
    .filter((entry) => (
      Number.isFinite(entry.timestampSec) &&
      Number.isFinite(entry.open) &&
      Number.isFinite(entry.high) &&
      Number.isFinite(entry.low) &&
      Number.isFinite(entry.close)
    ))
    .sort((left, right) => left.timestampSec - right.timestampSec)

  return parsed
}

function formatAxisTs(timestampSec: number) {
  const date = new Date(timestampSec * 1000)
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

function percentile(values: number[], p: number) {
  if (!values.length) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const clamped = Math.max(0, Math.min(1, p))
  const idx = (sorted.length - 1) * clamped
  const lo = Math.floor(idx)
  const hi = Math.ceil(idx)
  if (lo === hi) return sorted[lo]
  const weight = idx - lo
  return sorted[lo] * (1 - weight) + sorted[hi] * weight
}

function deriveYBounds(candles: OhlcvPoint[]) {
  const highs = candles.map((entry) => entry.high).filter(Number.isFinite)
  const lows = candles.map((entry) => entry.low).filter(Number.isFinite)

  if (!highs.length || !lows.length) {
    return { min: 0, max: 1 }
  }

  const absoluteMax = Math.max(...highs)
  const absoluteMin = Math.min(...lows)
  const p98 = percentile(highs, 0.98)
  const p95 = percentile(highs, 0.95)
  const p02 = percentile(lows, 0.02)

  // Trim extreme listing wick/outlier so recent action remains readable.
  const effectiveMax = absoluteMax > p95 * 1.8 ? p98 : absoluteMax
  const latest = candles[candles.length - 1]
  const max = Math.max(effectiveMax, latest?.high ?? effectiveMax)
  const minBase = Math.min(p02, latest?.low ?? p02, absoluteMin)
  const span = Math.max(max - minBase, max * 0.08, 1e-8)

  return {
    min: Math.max(0, minBase - span * 0.08),
    max: max + span * 0.12,
  }
}

async function buildQuickChartImage(candles: OhlcvPoint[], requestedWindow: string) {
  const y = deriveYBounds(candles)
  const labels = candles.map((entry) => formatAxisTs(entry.timestampSec))
  const candleData = candles.map((entry, index) => ({
    x: index + 1,
    o: entry.open,
    h: entry.high,
    l: entry.low,
    c: entry.close,
  }))

  const volumeData = candles.map((entry, index) => ({
    x: index + 1,
    y: entry.volume,
  }))

  const chart = {
    type: 'candlestick',
    data: {
      labels,
      datasets: [
        {
          label: 'LOOT / USD',
          data: candleData,
          color: {
            up: '#2bd67b',
            down: '#ff5d5d',
            unchanged: '#9aa4b2',
          },
          borderColor: '#2b3345',
        },
        {
          type: 'bar',
          label: 'Volume',
          data: volumeData,
          yAxisID: 'volume',
          backgroundColor: 'rgba(72, 114, 189, 0.35)',
          borderWidth: 0,
          barPercentage: 1.0,
          categoryPercentage: 0.95,
        },
      ],
    },
    options: {
      plugins: {
        legend: { display: false },
        title: {
          display: true,
          text: `LOOT / USD • ${requestedWindow.toUpperCase()} • Uniswap`,
          color: '#f5f7fb',
          font: { size: 20, weight: 'bold' },
          padding: { top: 10, bottom: 16 },
        },
      },
      layout: {
        padding: { left: 10, right: 10, top: 6, bottom: 6 },
      },
      scales: {
        x: {
          grid: { color: 'rgba(123, 142, 184, 0.22)' },
          ticks: {
            color: '#9fb0cf',
          maxTicksLimit: 10,
          minRotation: 0,
          maxRotation: 0,
        },
      },
      y: {
        position: 'right',
        min: y.min,
        max: y.max,
        grid: { color: 'rgba(123, 142, 184, 0.22)' },
        ticks: {
          color: '#c6d2ea',
          callback: 'function(value){ return Number(value).toFixed(6); }',
          maxTicksLimit: 8,
        },
      },
      volume: {
        position: 'left',
        beginAtZero: true,
          display: false,
        },
      },
    },
  }

  const createResponse = await fetch('https://quickchart.io/chart/create', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      version: '4',
      width: 1280,
      height: 960,
      devicePixelRatio: 2,
      format: 'png',
      backgroundColor: '#0a1020',
      chart,
    }),
  })

  if (!createResponse.ok) {
    throw new Error(`QuickChart create ${createResponse.status}`)
  }

  const payload = await createResponse.json() as { success?: boolean; url?: string }
  if (!payload?.url) {
    throw new Error('QuickChart did not return a render URL')
  }

  const imageResponse = await fetch(payload.url, { cache: 'no-store' })
  if (!imageResponse.ok) {
    throw new Error(`QuickChart render ${imageResponse.status}`)
  }

  const mime = imageResponse.headers.get('content-type') || ''
  if (!mime.includes('image/')) {
    throw new Error(`QuickChart render returned non-image content-type: ${mime}`)
  }

  const bytes = Buffer.from(await imageResponse.arrayBuffer())
  return bytes
}

async function respondPrice(message: Message, requestedWindow: string, logger: Logger) {
  const pair = await getBestLootPair()

  const priceUsd = numeric(pair.priceUsd)
  const change24h = numeric(pair.priceChange?.h24)
  const change6h = numeric(pair.priceChange?.h6)
  const change1h = numeric(pair.priceChange?.h1)
  const vol24h = numeric(pair.volume?.h24)
  const liqUsd = numeric(pair.liquidity?.usd)

  const title = `LOOT / USD • ${requestedWindow.toUpperCase()}`
  const openUrl = pair.url ? decoratePairUrl(pair.url, requestedWindow) : 'https://dexscreener.com/base'
  let chartAttachment: AttachmentBuilder | null = null

  if (pair.pairAddress) {
    try {
      const ohlcv = await getOhlcvFromGecko(pair.pairAddress, requestedWindow)
      if (ohlcv.length >= 2) {
        const chartBytes = await buildQuickChartImage(ohlcv, requestedWindow)
        chartAttachment = new AttachmentBuilder(chartBytes, { name: 'loot-chart.png' })
      }
    } catch (error) {
      logger.warn(
        {
          requestedWindow,
          pairAddress: pair.pairAddress,
          error: error instanceof Error ? error.message : String(error),
        },
        '[discord-price-command-bot] failed to build candle chart; fallback to openGraph'
      )
    }
  }

  const embed = new EmbedBuilder()
    .setColor(0x2ed66f)
    .setTitle(title)
    .setURL(openUrl)
    .setDescription('DexScreener pair + live candle chart.')
    .addFields(
      { name: 'Price', value: formatUsd(priceUsd), inline: true },
      { name: '24h', value: formatSignedPercent(change24h), inline: true },
      { name: 'Liquidity', value: formatUsd(liqUsd), inline: true },
      { name: '1h', value: formatSignedPercent(change1h), inline: true },
      { name: '6h', value: formatSignedPercent(change6h), inline: true },
      { name: '24h Volume', value: formatUsd(vol24h), inline: true },
    )
    .setFooter({ text: `Requested by ${message.author.username}` })
    .setTimestamp(new Date())

  if (chartAttachment) {
    embed.setImage('attachment://loot-chart.png')
  } else if (pair.info?.openGraph) {
    embed.setImage(pair.info.openGraph)
  }

  const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder()
      .setStyle(ButtonStyle.Link)
      .setLabel(`Open ${requestedWindow.toUpperCase()} Chart`)
      .setURL(openUrl)
  )

  await message.reply(
    chartAttachment
      ? {
          embeds: [embed],
          components: [row],
          files: [chartAttachment],
          allowedMentions: { repliedUser: false },
        }
      : {
          embeds: [embed],
          components: [row],
          allowedMentions: { repliedUser: false },
        }
  )

  logger.info(
    {
      channelId: message.channelId,
      guildId: message.guildId,
      window: requestedWindow,
      openUrl,
    },
    '[discord-price-command-bot] responded'
  )
}

export function createDiscordPriceCommandBot(options?: { logger?: Logger }) {
  const logger = options?.logger ?? console
  const enabled = env.enableDiscordPriceCommandBot
  const token = env.discordPriceCommandBotToken.trim()
  const allowedGuildId = env.discordPriceCommandAllowedGuildId.trim()
  const allowedChannelId = env.discordPriceCommandAllowedChannelId.trim()
  let started = false
  let stopping = false

  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent,
    ],
  })

  client.on('messageCreate', async (message) => {
    if (stopping) return
    if (message.author.bot) return
    if (!message.inGuild()) return
    if (allowedGuildId && message.guildId !== allowedGuildId) return
    if (allowedChannelId && message.channelId !== allowedChannelId) return

    const requestedWindow = parseCommandWindow(message.content)
    if (!requestedWindow) return

    try {
      await respondPrice(message, requestedWindow, logger)
    } catch (error) {
      logger.error(error, '[discord-price-command-bot] reply failed')
      await message.reply({
        content: 'Price bot temporary unavailable. Retry in a few seconds.',
        allowedMentions: { repliedUser: false },
      }).catch(() => {})
    }
  })

  const stop = async () => {
    stopping = true
    if (client.isReady()) {
      client.destroy()
    }
  }

  const start = async () => {
    if (started) return
    started = true

    if (!enabled) {
      logger.info('[discord-price-command-bot] disabled (ENABLE_DISCORD_PRICE_COMMAND_BOT != true)')
      return
    }

    if (!token) {
      logger.warn('[discord-price-command-bot] missing DISCORD_PRICE_COMMAND_BOT_TOKEN; bot not started')
      return
    }

    await client.login(token)
    logger.info(
      {
        allowedGuildId: allowedGuildId || null,
        allowedChannelId: allowedChannelId || null,
        prefix: env.discordPriceCommandPrefix || 'price',
      },
      '[discord-price-command-bot] ready'
    )
  }

  return {
    enabled,
    start,
    stop,
  }
}
