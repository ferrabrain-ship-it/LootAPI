import { spawnSync } from 'node:child_process'

const isRailway = Boolean(
  process.env.RAILWAY_PROJECT_ID ||
  process.env.RAILWAY_SERVICE_ID ||
  process.env.RAILWAY_ENVIRONMENT_ID
)

if (!isRailway) {
  console.log('[postbuild] skipping Playwright browser install (not Railway)')
  process.exit(0)
}

// Railway final image always ships /app reliably, so keep browsers inside /app.
const browsersPath = '/app/ms-playwright'
console.log(`[postbuild] installing Playwright browsers into ${browsersPath}`)

const result = spawnSync(
  'npx',
  ['playwright', 'install', '--with-deps', 'chromium', 'chromium-headless-shell'],
  {
    stdio: 'inherit',
    env: {
      ...process.env,
      PLAYWRIGHT_BROWSERS_PATH: browsersPath,
    },
  }
)

if (result.status !== 0) {
  console.error('[postbuild] Playwright browser install failed')
  process.exit(result.status ?? 1)
}

console.log('[postbuild] Playwright browser install completed')
