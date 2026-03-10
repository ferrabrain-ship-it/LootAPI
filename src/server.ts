import Fastify from 'fastify'
import cors from '@fastify/cors'
import type { FastifyReply, FastifyRequest } from 'fastify'
import { env } from './config/env.js'
import {
  asAddress,
  getAutoMine,
  getBuybacks,
  getCurrentRound,
  getLockDistributions,
  getLatestRoundTransition,
  getLeaderboardEarners,
  getLeaderboardLockers,
  getLeaderboardMiners,
  getLeaderboardStakers,
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
} from './services/protocol.js'
import { getProfile, getProfilesBatch } from './services/profiles.js'

const app = Fastify({ logger: true })

await app.register(cors, {
  origin: true,
})

app.get('/health', async () => ({ ok: true }))

app.get('/api/price', async () => {
  const stats = await getStats()
  return {
    loot: stats.loot,
    fetchedAt: stats.fetchedAt,
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
  }, 2500)

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

app.listen({ port: env.port, host: '0.0.0.0' })
  .then(() => {
    app.log.info(`mineloot-api listening on :${env.port}`)
  })
  .catch((error) => {
    app.log.error(error)
    process.exit(1)
  })
