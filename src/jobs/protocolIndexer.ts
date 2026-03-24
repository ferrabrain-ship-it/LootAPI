import { env } from '../config/env.js'
import { closeProtocolIndexerResources, runProtocolIndexSyncOnce } from '../services/protocolIndexer.js'

let shuttingDown = false

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function tick() {
  const result = await runProtocolIndexSyncOnce({ logger: console })
  console.info('[protocol-indexer] cycle complete', result)
}

async function main() {
  const runOnce = process.argv.includes('--once')

  if (runOnce) {
    await tick()
    process.exit(0)
  }

  console.info(`[protocol-indexer] starting, poll interval ${env.protocolIndexSyncIntervalMs}ms`)

  while (!shuttingDown) {
    try {
      await tick()
    } catch (error) {
      console.error('[protocol-indexer] cycle failed', error)
    }

    if (shuttingDown) break
    await sleep(env.protocolIndexSyncIntervalMs)
  }
}

process.on('SIGTERM', () => {
  shuttingDown = true
  console.info('[protocol-indexer] received SIGTERM')
})

process.on('SIGINT', () => {
  shuttingDown = true
  console.info('[protocol-indexer] received SIGINT')
})

main()
  .catch((error) => {
    console.error('[protocol-indexer] fatal', error)
    process.exit(1)
  })
  .finally(async () => {
    await closeProtocolIndexerResources()
  })
