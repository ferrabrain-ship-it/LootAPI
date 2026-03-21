import { env } from '../config/env.js'
import { createDiscordPriceWorker, runDiscordPriceWorkerOnce } from '../services/discordPriceWorker.js'

let shuttingDown = false

async function main() {
  const runOnce = process.argv.includes('--once')

  if (runOnce) {
    const result = await runDiscordPriceWorkerOnce({ logger: console })
    console.info('[discord-price-worker] cycle complete', result)
    process.exit(0)
  }

  const worker = createDiscordPriceWorker({ logger: console })

  if (!env.enableDiscordPriceWorker) {
    console.info('[discord-price-worker] disabled (set ENABLE_DISCORD_PRICE_WORKER=true to start)')
    process.exit(0)
  }

  await worker.start()

  while (!shuttingDown) {
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  await worker.stop()
}

process.on('SIGTERM', () => {
  shuttingDown = true
  console.info('[discord-price-worker] received SIGTERM')
})

process.on('SIGINT', () => {
  shuttingDown = true
  console.info('[discord-price-worker] received SIGINT')
})

main().catch((error) => {
  console.error('[discord-price-worker] fatal', error)
  process.exit(1)
})
