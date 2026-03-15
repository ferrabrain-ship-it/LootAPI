import { env } from '../config/env.js'
import { runLootpotNotifierOnce } from '../services/lootpotNotifier.js'

let shuttingDown = false

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function tick() {
  const result = await runLootpotNotifierOnce({
    test: env.lootpotWorkerTest,
    logger: console,
  })

  console.info('[lootpot-worker] cycle complete', result)
  return result
}

async function main() {
  if (env.lootpotWorkerTest) {
    await tick()
    process.exit(0)
  }

  console.info(`[lootpot-worker] starting, poll interval ${env.lootpotPollIntervalMs}ms`)

  while (!shuttingDown) {
    try {
      await tick()
    } catch (error) {
      console.error('[lootpot-worker] cycle failed', error)
    }

    if (shuttingDown) break
    await sleep(env.lootpotPollIntervalMs)
  }
}

process.on('SIGTERM', () => {
  shuttingDown = true
  console.info('[lootpot-worker] received SIGTERM')
})

process.on('SIGINT', () => {
  shuttingDown = true
  console.info('[lootpot-worker] received SIGINT')
})

main().catch((error) => {
  console.error('[lootpot-worker] fatal', error)
  process.exit(1)
})
