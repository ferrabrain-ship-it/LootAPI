import { ActivityType, Client, GatewayIntentBits } from 'discord.js'
import { formatEther } from 'viem'
import { env } from '../config/env.js'
import { getStats, getTreasuryStats } from './protocol.js'

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>
type MetricKey = 'circulating' | 'burned'

type MetricSnapshot = {
  circulatingLoot: number
  burnedLoot: number
}

type TickResult = {
  circulatingLoot: number
  burnedLoot: number
  circulatingNickname: string | null
  burnedNickname: string | null
}

function errorMessage(error: unknown) {
  if (error instanceof Error) return error.message
  return String(error)
}

function toLootNumber(valueWei: string | bigint | null | undefined) {
  try {
    const value = typeof valueWei === 'bigint' ? valueWei : BigInt(valueWei || '0')
    const asNumber = Number(formatEther(value))
    return Number.isFinite(asNumber) ? asNumber : 0
  } catch {
    return 0
  }
}

function formatLootInteger(value: number) {
  const safe = Number.isFinite(value) ? Math.max(0, value) : 0
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Math.floor(safe))
}

function buildNickname(metric: MetricKey, snapshot: MetricSnapshot) {
  if (metric === 'circulating') {
    return `${env.discordCirculatingEmoji} ${formatLootInteger(snapshot.circulatingLoot)}`
  }
  return `${env.discordBurnedEmoji} ${formatLootInteger(snapshot.burnedLoot)}`
}

function buildStatus(metric: MetricKey) {
  return metric === 'circulating'
    ? env.discordCirculatingStatus
    : env.discordBurnedStatus
}

async function getMetricSnapshot(): Promise<MetricSnapshot> {
  const [stats, treasuryStats] = await Promise.all([
    getStats(),
    getTreasuryStats(),
  ])

  return {
    circulatingLoot: toLootNumber(stats.totalSupply),
    burnedLoot: toLootNumber(treasuryStats.totalBurned),
  }
}

async function applyMetricBot(
  client: Client,
  metric: MetricKey,
  snapshot: MetricSnapshot,
  logger: Logger
) {
  if (!client.user) {
    throw new Error(`Discord ${metric} bot client user not available`)
  }

  const guildId = env.discordMetricsGuildId.trim()
  if (!guildId) {
    throw new Error('DISCORD_METRIC_BOTS_GUILD_ID (or DISCORD_GUILD_ID) is required')
  }

  const nickname = buildNickname(metric, snapshot)
  const statusLabel = buildStatus(metric)

  let guild
  try {
    guild = await client.guilds.fetch(guildId)
  } catch (error) {
    throw new Error(`[${metric}] failed to fetch guild ${guildId}: ${errorMessage(error)}`)
  }

  let me
  try {
    me = await guild.members.fetchMe()
  } catch (error) {
    throw new Error(`[${metric}] failed to fetch bot member in guild ${guildId}: ${errorMessage(error)}`)
  }

  try {
    if (me.nickname !== nickname) {
      await me.setNickname(nickname)
    }
  } catch (error) {
    logger.warn(`[discord-metric-bots-worker] failed to update ${metric} nickname`)
    logger.warn(error)
  }

  client.user.setPresence({
    activities: [{ type: ActivityType.Watching, name: statusLabel }],
    status: 'online',
  })

  return nickname
}

export function createDiscordMetricBotsWorker(options?: { logger?: Logger }) {
  const logger = options?.logger ?? console
  const enabled = env.enableDiscordMetricBotsWorker

  const tokenMap: Record<MetricKey, string> = {
    circulating: env.discordCirculatingBotToken.trim(),
    burned: env.discordBurnedBotToken.trim(),
  }

  const clients = new Map<MetricKey, Client>()
  let started = false
  let stopping = false
  let timer: NodeJS.Timeout | null = null

  const stop = async () => {
    stopping = true
    if (timer) {
      clearTimeout(timer)
      timer = null
    }
    for (const client of clients.values()) {
      if (client.isReady()) {
        client.destroy()
      }
    }
    clients.clear()
  }

  const start = async () => {
    if (started) return
    started = true

    if (!enabled) {
      logger.info('[discord-metric-bots-worker] disabled (ENABLE_DISCORD_METRIC_BOTS_WORKER != true)')
      return
    }

    const activeMetrics = (Object.keys(tokenMap) as MetricKey[]).filter((metric) => Boolean(tokenMap[metric]))
    if (!activeMetrics.length) {
      logger.warn('[discord-metric-bots-worker] no bot tokens configured (DISCORD_CIRCULATING_BOT_TOKEN / DISCORD_BURNED_BOT_TOKEN)')
      return
    }

    for (const metric of activeMetrics) {
      const client = new Client({
        intents: [GatewayIntentBits.Guilds],
      })
      await client.login(tokenMap[metric])
      clients.set(metric, client)
      logger.info(`[discord-metric-bots-worker] ${metric} bot logged in`)
    }

    logger.info(`[discord-metric-bots-worker] inline mode enabled, poll interval ${env.discordMetricsPollIntervalMs}ms`)

    const tick = async () => {
      if (stopping) return

      try {
        const snapshot = await getMetricSnapshot()
        let circulatingNickname: string | null = null
        let burnedNickname: string | null = null

        if (clients.has('circulating')) {
          try {
            circulatingNickname = await applyMetricBot(
              clients.get('circulating') as Client,
              'circulating',
              snapshot,
              logger
            )
          } catch (error) {
            logger.error(
              {
                metric: 'circulating',
                guildId: env.discordMetricsGuildId,
                error: errorMessage(error),
              },
              '[discord-metric-bots-worker] metric bot update failed'
            )
          }
        }

        if (clients.has('burned')) {
          try {
            burnedNickname = await applyMetricBot(
              clients.get('burned') as Client,
              'burned',
              snapshot,
              logger
            )
          } catch (error) {
            logger.error(
              {
                metric: 'burned',
                guildId: env.discordMetricsGuildId,
                error: errorMessage(error),
              },
              '[discord-metric-bots-worker] metric bot update failed'
            )
          }
        }

        const result: TickResult = {
          circulatingLoot: snapshot.circulatingLoot,
          burnedLoot: snapshot.burnedLoot,
          circulatingNickname,
          burnedNickname,
        }
        logger.info({ result }, '[discord-metric-bots-worker] cycle complete')
      } catch (error) {
        logger.error(error, '[discord-metric-bots-worker] cycle failed')
      }

      if (stopping) return
      timer = setTimeout(() => {
        void tick()
      }, env.discordMetricsPollIntervalMs)
    }

    void tick()
  }

  return {
    enabled,
    start,
    stop,
  }
}
