import Fastify from 'fastify'
import cors from '@fastify/cors'
import type { FastifyReply, FastifyRequest } from 'fastify'
import { env } from './config/env.js'
import {
  getAgentWalletStats,
  runAgentStatsSyncOnce,
} from './services/agentStats.js'
import {
  asAddress,
  getAutoMine,
  getBuybacks,
  getCopilotContext,
  getCurrentRound,
  getLockDistributions,
  getLatestRoundTransition,
  getLeaderboardEarners,
  getLeaderboardLockers,
  getLeaderboardMiners,
  getLeaderboardStakers,
  getLootPriceCached,
  getLeaderboardTreasury,
  getLockStats,
  getRound,
  getRoundMiners,
  getRounds,
  getStakingStats,
  getStats,
  getTreasuryStats,
  getUserHistory,
  getUserRewards,
  getUserStake,
  warmProtocolCaches,
} from './services/protocol.js'
import { getProfile, getProfilesBatch } from './services/profiles.js'
import { runLootpotNotifierOnce } from './services/lootpotNotifier.js'
import { createDiscordPriceWorker } from './services/discordPriceWorker.js'
import { createDiscordMetricBotsWorker } from './services/discordMetricBotsWorker.js'
import { createDiscordPriceCommandBot } from './services/discordPriceCommandBot.js'

const app = Fastify({ logger: true })
let lootpotWorkerStopping = false
let lootpotWorkerTimer: NodeJS.Timeout | null = null
let cacheWarmTimer: NodeJS.Timeout | null = null
let cacheWarmerStopping = false
let cacheWarmerRunning = false
let agentStatsWorkerStopping = false
let agentStatsWorkerTimer: NodeJS.Timeout | null = null
const discordPriceWorker = createDiscordPriceWorker({ logger: console })
const discordMetricBotsWorker = createDiscordMetricBotsWorker({ logger: console })
const discordPriceCommandBot = createDiscordPriceCommandBot({ logger: console })

await app.register(cors, {
  origin: true,
})

app.get('/health', async () => ({ ok: true }))

app.get('/api/price', async () => {
  const price = await getLootPriceCached()
  return {
    ...price.payload,
  }
})

app.get('/api/stats', async () => getStats())
app.get('/api/treasury/stats', async () => getTreasuryStats())
app.get('/api/lock/stats', async () => getLockStats())
app.get('/api/lock/distributions', async (req) => {
  const { page = '1', limit = '12' } = req.query as Record<string, string | undefined>
  return getLockDistributions(Number(page), Number(limit))
})

app.get('/api/treasury/buybacks', async (req) => {
  const { page = '1', limit = '12' } = req.query as Record<string, string | undefined>
  return getBuybacks(Number(page), Number(limit))
})

app.get('/api/staking/stats', async () => getStakingStats())

app.get('/api/staking/:address', async (req) => {
  const { address } = req.params as { address: string }
  return getUserStake(asAddress(address))
})

app.get('/api/automine/:address', async (req) => {
  const { address } = req.params as { address: string }
  return getAutoMine(asAddress(address))
})

app.get('/api/round/current', async (req) => {
  const { user } = req.query as { user?: string }
  return getCurrentRound(user)
})

app.get('/api/round/:id', async (req) => {
  const { id } = req.params as { id: string }
  return getRound(id)
})

app.get('/api/round/:id/miners', async (req) => {
  const { id } = req.params as { id: string }
  return getRoundMiners(id)
})

app.get('/api/rounds', async (req) => {
  const { page = '1', limit = '12', lootpot } = req.query as Record<string, string | undefined>
  return getRounds(Number(page), Number(limit), lootpot === 'true')
})

app.get('/api/copilot/context', async (req) => {
  const { lookback = '1000' } = req.query as Record<string, string | undefined>
  return getCopilotContext(Number(lookback))
})

app.get('/api/user/:address/rewards', async (req) => {
  const { address } = req.params as { address: string }
  return getUserRewards(asAddress(address))
})

app.get('/api/user/:address/history', async (req) => {
  const { address } = req.params as { address: string }
  const { type = 'deploy', limit = '100', roundId } = req.query as Record<string, string | undefined>
  if (type !== 'deploy') {
    return { history: [], totals: null }
  }
  const parsedLimit = Number(limit)
  const safeLimit = Number.isFinite(parsedLimit) ? Math.max(1, Math.min(parsedLimit, 200)) : 100
  return getUserHistory(asAddress(address), safeLimit, roundId ? BigInt(roundId) : undefined)
})

app.get('/api/user/:address/profile', async (req) => {
  const { address } = req.params as { address: string }
  return getProfile(asAddress(address))
})

app.get('/api/agent-stats/:address', async (req) => {
  const { address } = req.params as { address: string }
  const { recent = '12' } = req.query as Record<string, string | undefined>
  return getAgentWalletStats(asAddress(address), Number(recent))
})

app.get('/api/profiles/batch', async (req) => {
  const { addresses = '' } = req.query as { addresses?: string }
  const parsed = addresses
    .split(',')
    .map((address) => address.trim())
    .filter(Boolean)
    .map(asAddress)
  return getProfilesBatch(parsed)
})

app.get('/api/leaderboard/miners', async (req) => {
  const { limit = '12' } = req.query as { limit?: string }
  return getLeaderboardMiners(Number(limit))
})

app.get('/api/leaderboard/stakers', async (req) => {
  const { limit = '12' } = req.query as { limit?: string }
  return getLeaderboardStakers(Number(limit))
})

app.get('/api/leaderboard/earners', async (req) => {
  const { limit = '12' } = req.query as { limit?: string }
  return getLeaderboardEarners(Number(limit))
})

app.get('/api/leaderboard/lockers', async (req) => {
  const { limit = '12' } = req.query as { limit?: string }
  return getLeaderboardLockers(Number(limit))
})

app.get('/api/leaderboard/treasury', async (req) => {
  const { limit = '12' } = req.query as { limit?: string }
  return getLeaderboardTreasury(Number(limit))
})

function startSse(reply: FastifyReply) {
  reply.raw.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  })
}

function sendSse(reply: FastifyReply, event: string, data: unknown) {
  reply.raw.write(`event: ${event}\n`)
  reply.raw.write(`data: ${JSON.stringify(data)}\n\n`)
}

app.get('/api/events/rounds', async (_req, reply) => {
  startSse(reply)

  let previous = await getCurrentRound().catch(() => null)
  sendSse(reply, 'heartbeat', { timestamp: new Date().toISOString() })
  if (previous) {
    sendSse(reply, 'roundTransition', await getLatestRoundTransition().catch(() => ({ settled: null, newRound: previous })))
  }

  const timer = setInterval(async () => {
    try {
      const current = await getCurrentRound()
      sendSse(reply, 'heartbeat', { timestamp: new Date().toISOString() })

      if (!previous || previous.roundId !== current.roundId) {
        sendSse(reply, 'roundTransition', await getLatestRoundTransition())
      } else if (previous.totalDeployed !== current.totalDeployed || JSON.stringify(previous.blocks) !== JSON.stringify(current.blocks)) {
        sendSse(reply, 'deployed', {
          roundId: current.roundId,
          user: '',
          totalAmount: '0',
          isAutoMine: false,
          totalDeployed: current.totalDeployed,
          totalDeployedFormatted: current.totalDeployedFormatted,
          userDeployed: '0',
          userDeployedFormatted: '0',
          blocks: current.blocks,
        })
      }

      previous = current
    } catch (error) {
      sendSse(reply, 'heartbeat', { timestamp: new Date().toISOString(), error: String(error) })
    }
  }, 1000)

  reply.raw.on('close', () => {
    clearInterval(timer)
  })

  return reply
})

app.get('/api/user/:address/events', async (_req: FastifyRequest, reply) => {
  startSse(reply)
  sendSse(reply, 'heartbeat', { timestamp: new Date().toISOString() })
  const timer = setInterval(() => {
    sendSse(reply, 'heartbeat', { timestamp: new Date().toISOString() })
  }, 30000)
  reply.raw.on('close', () => clearInterval(timer))
  return reply
})

app.setErrorHandler((error, _req, reply) => {
  app.log.error(error)
  const message = error instanceof Error ? error.message : String(error)
  reply.status(500).send({ error: message })
})

function stopLootpotWorker() {
  lootpotWorkerStopping = true
  if (lootpotWorkerTimer) {
    clearTimeout(lootpotWorkerTimer)
    lootpotWorkerTimer = null
  }
}

function stopCacheWarmer() {
  cacheWarmerStopping = true
  if (cacheWarmTimer) {
    clearTimeout(cacheWarmTimer)
    cacheWarmTimer = null
  }
}

function stopAgentStatsWorker() {
  agentStatsWorkerStopping = true
  if (agentStatsWorkerTimer) {
    clearTimeout(agentStatsWorkerTimer)
    agentStatsWorkerTimer = null
  }
}

function startCacheWarmer() {
  if (!env.enableCacheWarmer) {
    app.log.info('[cache-warmer] disabled')
    return
  }

  cacheWarmerStopping = false
  const schedule = (delayMs: number) => {
    if (cacheWarmerStopping) return
    cacheWarmTimer = setTimeout(() => {
      void run()
    }, delayMs)
  }

  const run = async () => {
    if (cacheWarmerStopping) return
    if (cacheWarmerRunning) {
      app.log.warn('[cache-warmer] previous cycle still running, skipping overlap')
      schedule(env.cacheWarmIntervalMs)
      return
    }

    cacheWarmerRunning = true
    try {
      await warmProtocolCaches()
      app.log.info('[cache-warmer] cycle complete')
    } catch (error) {
      app.log.error(error, '[cache-warmer] cycle failed')
    } finally {
      cacheWarmerRunning = false
      schedule(env.cacheWarmIntervalMs)
    }
  }

  schedule(env.cacheWarmIntervalMs)
}

function startLootpotWorker() {
  if (!env.enableLootpotWorker) return

  app.log.info(`[lootpot-worker] inline mode enabled, poll interval ${env.lootpotPollIntervalMs}ms`)

  const tick = async () => {
    if (lootpotWorkerStopping) return

    try {
      const result = await runLootpotNotifierOnce({ logger: console })
      app.log.info({ result }, '[lootpot-worker] cycle complete')
    } catch (error) {
      app.log.error(error, '[lootpot-worker] cycle failed')
    }

    if (lootpotWorkerStopping) return
    lootpotWorkerTimer = setTimeout(() => {
      void tick()
    }, env.lootpotPollIntervalMs)
  }

  void tick()
}

function startAgentStatsWorker() {
  if (!env.agentStatsWallets.trim()) return

  app.log.info(`[agent-stats-worker] inline mode enabled, poll interval ${env.agentStatsSyncIntervalMs}ms`)

  const tick = async () => {
    if (agentStatsWorkerStopping) return

    try {
      const result = await runAgentStatsSyncOnce({ logger: console })
      app.log.info({ result }, '[agent-stats-worker] cycle complete')
    } catch (error) {
      app.log.error(error, '[agent-stats-worker] cycle failed')
    }

    if (agentStatsWorkerStopping) return
    agentStatsWorkerTimer = setTimeout(() => {
      void tick()
    }, env.agentStatsSyncIntervalMs)
  }

  void tick()
}

async function startDiscordPriceWorker() {
  if (!discordPriceWorker.enabled) return
  try {
    await discordPriceWorker.start()
  } catch (error) {
    app.log.error(error, '[discord-price-worker] failed to start')
  }
}

async function startDiscordMetricBotsWorker() {
  if (!discordMetricBotsWorker.enabled) return
  try {
    await discordMetricBotsWorker.start()
  } catch (error) {
    app.log.error(error, '[discord-metric-bots-worker] failed to start')
  }
}

async function startDiscordPriceCommandBot() {
  if (!discordPriceCommandBot.enabled) return
  try {
    await discordPriceCommandBot.start()
  } catch (error) {
    app.log.error(error, '[discord-price-command-bot] failed to start')
  }
}

process.on('SIGTERM', () => {
  stopLootpotWorker()
  stopAgentStatsWorker()
  stopCacheWarmer()
  void discordPriceWorker.stop()
  void discordMetricBotsWorker.stop()
  void discordPriceCommandBot.stop()
})
process.on('SIGINT', () => {
  stopLootpotWorker()
  stopAgentStatsWorker()
  stopCacheWarmer()
  void discordPriceWorker.stop()
  void discordMetricBotsWorker.stop()
  void discordPriceCommandBot.stop()
})

app.listen({ port: env.port, host: '0.0.0.0' })
  .then(() => {
    app.log.info(`mineloot-api listening on :${env.port}`)
    startCacheWarmer()
    startLootpotWorker()
    startAgentStatsWorker()
    void startDiscordPriceWorker()
    void startDiscordMetricBotsWorker()
    void startDiscordPriceCommandBot()
  })
  .catch((error) => {
    app.log.error(error)
    process.exit(1)
  })
