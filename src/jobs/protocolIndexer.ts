import { env } from '../config/env.js'
import {
  closeProtocolIndexerResources,
  runProtocolIndexSyncOnce,
  runTreasuryAgentSnapshotSyncOnce,
} from '../services/protocolIndexer.js'

let shuttingDown = false

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function tick() {
  const result = await runProtocolIndexSyncOnce({ logger: console })
  console.info('[protocol-indexer] cycle complete', result)
}

async function tickTreasurySnapshots() {
  const result = await runTreasuryAgentSnapshotSyncOnce({ logger: console })
  console.info('[treasury-agent-snapshots] cycle complete', result)
}

async function protocolLoop() {
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

async function treasurySnapshotLoop() {
  console.info(
    `[treasury-agent-snapshots] starting, poll interval ${env.treasuryAgentSnapshotSyncIntervalMs}ms`
  )

  while (!shuttingDown) {
    try {
      await tickTreasurySnapshots()
    } catch (error) {
      console.error('[treasury-agent-snapshots] cycle failed', error)
    }

    if (shuttingDown) break
    await sleep(env.treasuryAgentSnapshotSyncIntervalMs)
  }
}

async function main() {
  const runOnce = process.argv.includes('--once')

  if (runOnce) {
    await tick()
    await tickTreasurySnapshots()
    process.exit(0)
  }

  if (!env.enableProtocolIndexer) {
    console.info('[protocol-indexer] disabled via ENABLE_PROTOCOL_INDEXER=false')
    process.exit(0)
  }

  await Promise.all([protocolLoop(), treasurySnapshotLoop()])
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
