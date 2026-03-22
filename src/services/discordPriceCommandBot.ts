import {
  ActionRowBuilder,
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

async function respondPrice(message: Message, requestedWindow: string, logger: Logger) {
  const pair = await getBestLootPair()

  const priceUsd = numeric(pair.priceUsd)
  const change24h = numeric(pair.priceChange?.h24)
  const change6h = numeric(pair.priceChange?.h6)
  const change1h = numeric(pair.priceChange?.h1)
  const vol24h = numeric(pair.volume?.h24)
  const liqUsd = numeric(pair.liquidity?.usd)

  const title = `LOOT / USD • ${requestedWindow.toUpperCase()} • DexScreener`
  const openUrl = pair.url ? decoratePairUrl(pair.url, requestedWindow) : 'https://dexscreener.com/base'

  const embed = new EmbedBuilder()
    .setColor(0xd4a72c)
    .setTitle(title)
    .setURL(openUrl)
    .setDescription('Live pair overview from DexScreener.')
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

  if (pair.info?.openGraph) {
    embed.setImage(pair.info.openGraph)
  }

  const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder()
      .setStyle(ButtonStyle.Link)
      .setLabel(`Open ${requestedWindow.toUpperCase()} Chart`)
      .setURL(openUrl)
  )

  await message.reply({
    embeds: [embed],
    components: [row],
    allowedMentions: { repliedUser: false },
  })

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
