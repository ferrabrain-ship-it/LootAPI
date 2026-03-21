import { createDiscordMetricBotsWorker } from '../services/discordMetricBotsWorker.js'

const worker = createDiscordMetricBotsWorker({ logger: console })

if (!worker.enabled) {
  console.info('[discord-metric-bots-worker] disabled (set ENABLE_DISCORD_METRIC_BOTS_WORKER=true to start)')
  process.exit(0)
}

let shuttingDown = false

async function shutdown() {
  if (shuttingDown) return
  shuttingDown = true
  await worker.stop()
  process.exit(0)
}

process.on('SIGTERM', () => {
  console.info('[discord-metric-bots-worker] received SIGTERM')
  void shutdown()
})

process.on('SIGINT', () => {
  console.info('[discord-metric-bots-worker] received SIGINT')
  void shutdown()
})

worker.start().catch((error) => {
  console.error('[discord-metric-bots-worker] fatal', error)
  process.exit(1)
})

