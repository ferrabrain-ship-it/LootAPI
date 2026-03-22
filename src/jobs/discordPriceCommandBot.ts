import { createDiscordPriceCommandBot } from '../services/discordPriceCommandBot.js'

const bot = createDiscordPriceCommandBot({ logger: console })

if (!bot.enabled) {
  console.info('[discord-price-command-bot] disabled (set ENABLE_DISCORD_PRICE_COMMAND_BOT=true to start)')
  process.exit(0)
}

let shuttingDown = false

async function shutdown() {
  if (shuttingDown) return
  shuttingDown = true
  await bot.stop()
  process.exit(0)
}

process.on('SIGTERM', () => {
  console.info('[discord-price-command-bot] received SIGTERM')
  void shutdown()
})

process.on('SIGINT', () => {
  console.info('[discord-price-command-bot] received SIGINT')
  void shutdown()
})

bot.start().catch((error) => {
  console.error('[discord-price-command-bot] fatal', error)
  process.exit(1)
})

