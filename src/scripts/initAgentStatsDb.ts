import { closeAgentStatsPool, getAgentStatsDatabaseUrl, initAgentStatsSchema } from '../lib/agentStatsDb.js'

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
  const databaseUrl = getAgentStatsDatabaseUrl()
  console.log(`[agent-stats-db] initializing schema on ${maskDatabaseUrl(databaseUrl)}`)
  await initAgentStatsSchema()
  console.log('[agent-stats-db] schema ready')
}

main()
  .catch((error) => {
    console.error('[agent-stats-db] init failed', error)
    process.exitCode = 1
  })
  .finally(async () => {
    await closeAgentStatsPool().catch(() => {})
  })

