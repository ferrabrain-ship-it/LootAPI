import { ActivityType, Client, GatewayIntentBits } from 'discord.js'
import { Pool } from 'pg'
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>

type LootMarketSnapshot = {
  priceUsd: number
  change24hPct: number
}

export type DiscordPriceTickResult = {
  priceUsd: number
  change24hPct: number
  change7dPct: number | null
  statusLabel: string
  nickname: string
}

const PRICE_SNAPSHOT_SCHEMA_SQL = `
create table if not exists loot_price_snapshots (
  ts timestamptz not null default now(),
  price_usd numeric(38,18) not null
);

create index if not exists idx_loot_price_snapshots_ts_desc
  on loot_price_snapshots (ts desc);
`

let priceSnapshotPool: Pool | null = null

function numeric(value: unknown) {
  const parsed = typeof value === 'number' ? value : Number.parseFloat(String(value ?? ''))
  return Number.isFinite(parsed) ? parsed : 0
}

function formatPriceForNickname(priceUsd: number) {
  const absolute = Math.abs(priceUsd)

  if (absolute >= 1000) {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(priceUsd)
  }

  if (absolute >= 100) {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    }).format(priceUsd)
  }

  if (absolute >= 1) {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(priceUsd)
  }

  if (absolute >= 0.1) {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 3,
      maximumFractionDigits: 3,
    }).format(priceUsd)
  }

  if (absolute >= 0.01) {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 4,
      maximumFractionDigits: 4,
    }).format(priceUsd)
  }

  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 6,
    maximumFractionDigits: 6,
  }).format(priceUsd)
}

function formatSignedPercent(value: number) {
  if (!Number.isFinite(value)) return '0.00%'
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

async function getLootMarketSnapshot(): Promise<LootMarketSnapshot> {
  const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${CONTRACTS.loot}`, {
    cache: 'no-store',
  })

  if (!response.ok) {
    throw new Error(`DexScreener ${response.status}`)
  }

  const data = await response.json()
  const pairs: Array<{
    priceUsd?: string | number
    priceChange?: { h24?: string | number }
    liquidity?: { usd?: string | number }
  }> = data.pairs ?? []

  if (!pairs.length) {
    throw new Error('No LOOT pairs found on DexScreener')
  }

  const best = [...pairs].sort(
    (left, right) => numeric(right.liquidity?.usd) - numeric(left.liquidity?.usd)
  )[0]

  return {
    priceUsd: numeric(best.priceUsd),
    change24hPct: numeric(best.priceChange?.h24),
  }
}

function getPriceSnapshotPool() {
  const databaseUrl = env.discordPriceDatabaseUrl.trim()
  if (!databaseUrl) return null

  if (!priceSnapshotPool) {
    priceSnapshotPool = new Pool({
      connectionString: databaseUrl,
      max: 2,
    })
  }

  return priceSnapshotPool
}

async function getSevenDayChangePct(priceUsd: number, logger: Logger): Promise<number | null> {
  const pool = getPriceSnapshotPool()
  if (!pool) return null

  const client = await pool.connect()

  try {
    await client.query(PRICE_SNAPSHOT_SCHEMA_SQL)
    await client.query(
      'insert into loot_price_snapshots(price_usd) values ($1)',
      [priceUsd.toString()]
    )

    const reference = await client.query<{ price_usd: string }>(
      `
        select price_usd
        from loot_price_snapshots
        where ts <= now() - interval '7 days'
        order by ts desc
        limit 1
      `
    )

    await client.query(
      `
        delete from loot_price_snapshots
        where ts < now() - interval '120 days'
      `
    )

    if (!reference.rows.length) {
      return null
    }

    const baseline = numeric(reference.rows[0].price_usd)
    if (baseline <= 0) return null

    return ((priceUsd - baseline) / baseline) * 100
  } catch (error) {
    logger.warn('[discord-price-worker] failed to persist/read price snapshots, using 24h fallback')
    logger.warn(error)
    return null
  } finally {
    client.release()
  }
}

async function applyDiscordProfile(
  client: Client,
  snapshot: LootMarketSnapshot,
  change7dPct: number | null,
  logger: Logger
): Promise<DiscordPriceTickResult> {
  if (!client.user) {
    throw new Error('Discord client user not available')
  }

  const trend = snapshot.change24hPct >= 0 ? '↗' : '↘'
  const nickname = `$${formatPriceForNickname(snapshot.priceUsd)} (${trend})`
  const statusLabel = `24h: ${formatSignedPercent(snapshot.change24hPct)}`

  const guild = await client.guilds.fetch(env.discordGuildId)
  const me = await guild.members.fetchMe()

  try {
    if (me.nickname !== nickname) {
      await me.setNickname(nickname)
    }
  } catch (error) {
    logger.warn('[discord-price-worker] failed to update bot nickname (check Manage Nicknames permission and role order)')
    logger.warn(error)
  }

  client.user.setPresence({
    activities: [{ type: ActivityType.Watching, name: statusLabel }],
    status: 'online',
  })

  return {
    priceUsd: snapshot.priceUsd,
    change24hPct: snapshot.change24hPct,
    change7dPct,
    statusLabel,
    nickname,
  }
}

async function runSingleTick(client: Client, logger: Logger) {
  const snapshot = await getLootMarketSnapshot()
  const change7dPct = await getSevenDayChangePct(snapshot.priceUsd, logger)
  return applyDiscordProfile(client, snapshot, change7dPct, logger)
}

export function createDiscordPriceWorker(options?: { logger?: Logger }) {
  const logger = options?.logger ?? console
  const enabled = env.enableDiscordPriceWorker
  const token = env.discordBotToken.trim()
  const guildId = env.discordGuildId.trim()

  let stopping = false
  let started = false
  let timer: NodeJS.Timeout | null = null

  const client = new Client({
    intents: [GatewayIntentBits.Guilds],
  })

  const stop = async () => {
    stopping = true
    if (timer) {
      clearTimeout(timer)
      timer = null
    }

    if (client.isReady()) {
      client.destroy()
    }

    if (priceSnapshotPool) {
      const pool = priceSnapshotPool
      priceSnapshotPool = null
      await pool.end()
    }
  }

  const start = async () => {
    if (started) return
    started = true

    if (!enabled) {
      logger.info('[discord-price-worker] disabled (ENABLE_DISCORD_PRICE_WORKER != true)')
      return
    }

    if (!token || !guildId) {
      logger.warn('[discord-price-worker] missing DISCORD_BOT_TOKEN or DISCORD_GUILD_ID; worker not started')
      return
    }

    logger.info(`[discord-price-worker] inline mode enabled, poll interval ${env.discordPricePollIntervalMs}ms`)
    await client.login(token)

    const tick = async () => {
      if (stopping) return

      try {
        const result = await runSingleTick(client, logger)
        logger.info({ result }, '[discord-price-worker] cycle complete')
      } catch (error) {
        logger.error(error, '[discord-price-worker] cycle failed')
      }

      if (stopping) return
      timer = setTimeout(() => {
        void tick()
      }, env.discordPricePollIntervalMs)
    }

    void tick()
  }

  return {
    enabled,
    start,
    stop,
  }
}

export async function runDiscordPriceWorkerOnce(options?: { logger?: Logger }) {
  const logger = options?.logger ?? console
  const token = env.discordBotToken.trim()
  const guildId = env.discordGuildId.trim()

  if (!token || !guildId) {
    throw new Error('DISCORD_BOT_TOKEN and DISCORD_GUILD_ID are required')
  }

  const client = new Client({
    intents: [GatewayIntentBits.Guilds],
  })

  await client.login(token)

  try {
    return await runSingleTick(client, logger)
  } finally {
    client.destroy()
    if (priceSnapshotPool) {
      const pool = priceSnapshotPool
      priceSnapshotPool = null
      await pool.end()
    }
  }
}
