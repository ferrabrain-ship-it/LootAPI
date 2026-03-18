import { getAddress } from 'viem'
import { env } from '../config/env.js'
import { runAgentStatsSyncOnce } from '../services/agentStats.js'

let shuttingDown = false

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

function getWalletsFromEnv() {
  return env.agentStatsWallets
    .split(',')
    .map((wallet) => wallet.trim())
    .filter(Boolean)
    .map((wallet) => getAddress(wallet))
}

async function tick() {
  const result = await runAgentStatsSyncOnce({
    wallets: getWalletsFromEnv(),
    logger: console,
  })

  console.info('[agent-stats-worker] cycle complete', result)
}

async function main() {
  const runOnce = process.argv.includes('--once')

  if (runOnce) {
    await tick()
    process.exit(0)
  }

  console.info(`[agent-stats-worker] starting, poll interval ${env.agentStatsSyncIntervalMs}ms`)

  while (!shuttingDown) {
    try {
      await tick()
    } catch (error) {
      console.error('[agent-stats-worker] cycle failed', error)
    }

    if (shuttingDown) break
    await sleep(env.agentStatsSyncIntervalMs)
  }
}

process.on('SIGTERM', () => {
  shuttingDown = true
  console.info('[agent-stats-worker] received SIGTERM')
})

process.on('SIGINT', () => {
  shuttingDown = true
  console.info('[agent-stats-worker] received SIGINT')
})

main().catch((error) => {
  console.error('[agent-stats-worker] fatal', error)
  process.exit(1)
})

