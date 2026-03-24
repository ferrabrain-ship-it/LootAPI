import { closeProtocolIndexPool, getProtocolIndexDatabaseUrl, initProtocolIndexSchema } from '../lib/protocolIndexDb.js'

function maskDatabaseUrl(databaseUrl: string) {
  try {
    const parsed = new URL(databaseUrl)
    const host = parsed.hostname || 'unknown-host'
    const databaseName = parsed.pathname.replace(/^\//, '') || 'unknown-db'
    return `${host}/${databaseName}`
  } catch {
    return 'configured-database'
  }
}

async function main() {
  const databaseUrl = getProtocolIndexDatabaseUrl()
  console.log(`[protocol-index-db] initializing schema on ${maskDatabaseUrl(databaseUrl)}`)
  await initProtocolIndexSchema()
  console.log('[protocol-index-db] schema ready')
}

main()
  .catch((error) => {
    console.error('[protocol-index-db] init failed', error)
    process.exitCode = 1
  })
  .finally(async () => {
    await closeProtocolIndexPool().catch(() => {})
  })
