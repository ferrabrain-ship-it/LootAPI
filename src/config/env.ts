import 'dotenv/config'

function toNumber(value: string | undefined, fallback: number) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function toBigInt(value: string | undefined, fallback: bigint) {
  if (!value) return fallback
  try {
    return BigInt(value)
  } catch {
    return fallback
  }
}

export const env = {
  port: toNumber(process.env.PORT, 3001),
  corsOrigin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  appUrl: process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000',
  agentStatsDatabaseUrl: process.env.AGENT_STATS_DATABASE_URL || process.env.DATABASE_URL || '',
  agentStatsWallets: process.env.AGENT_STATS_WALLETS || '',
  agentStatsSyncIntervalMs: toNumber(process.env.AGENT_STATS_SYNC_INTERVAL_MS, 120000),
  enableLootpotWorker: process.env.ENABLE_LOOTPOT_WORKER === 'true',
  enableDiscordPriceWorker: process.env.ENABLE_DISCORD_PRICE_WORKER === 'true',
  enableDiscordMetricBotsWorker: process.env.ENABLE_DISCORD_METRIC_BOTS_WORKER === 'true',
  enableDiscordPriceCommandBot: process.env.ENABLE_DISCORD_PRICE_COMMAND_BOT === 'true',
  discordBotToken: process.env.DISCORD_BOT_TOKEN || '',
  discordGuildId: process.env.DISCORD_GUILD_ID || '',
  discordPricePollIntervalMs: toNumber(process.env.DISCORD_PRICE_POLL_INTERVAL_MS, 600000),
  discordPriceDatabaseUrl: process.env.DISCORD_PRICE_DATABASE_URL || process.env.AGENT_STATS_DATABASE_URL || process.env.DATABASE_URL || '',
  discordPriceCommandBotToken: process.env.DISCORD_PRICE_COMMAND_BOT_TOKEN || '',
  discordPriceCommandAllowedGuildId: process.env.DISCORD_PRICE_COMMAND_ALLOWED_GUILD_ID || '',
  discordPriceCommandAllowedChannelId: process.env.DISCORD_PRICE_COMMAND_ALLOWED_CHANNEL_ID || '',
  discordPriceCommandPrefix: (process.env.DISCORD_PRICE_COMMAND_PREFIX || 'price').trim().toLowerCase(),
  discordPriceCommandRenderMode: (process.env.DISCORD_PRICE_COMMAND_RENDER_MODE || 'dexscreener').trim().toLowerCase(),
  discordPriceCommandRenderTimeoutMs: toNumber(process.env.DISCORD_PRICE_COMMAND_RENDER_TIMEOUT_MS, 14000),
  playwrightChromiumExecutablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || '',
  discordMetricsGuildId: process.env.DISCORD_METRIC_BOTS_GUILD_ID || process.env.DISCORD_GUILD_ID || '',
  discordCirculatingBotToken: process.env.DISCORD_CIRCULATING_BOT_TOKEN || '',
  discordBurnedBotToken: process.env.DISCORD_BURNED_BOT_TOKEN || '',
  discordMetricsPollIntervalMs: toNumber(process.env.DISCORD_METRIC_BOTS_POLL_INTERVAL_MS, 120000),
  discordCirculatingEmoji: process.env.DISCORD_CIRCULATING_EMOJI || '',
  discordBurnedEmoji: process.env.DISCORD_BURNED_EMOJI || '',
  discordCirculatingStatus: process.env.DISCORD_CIRCULATING_STATUS || 'Circulating Supply',
  discordBurnedStatus: process.env.DISCORD_BURNED_STATUS || 'Burned',
  discordLootpotWebhookUrl: process.env.DISCORD_LOOTPOT_WEBHOOK_URL || '',
  discordBotAssetBaseUrl: process.env.DISCORD_BOT_ASSET_BASE_URL || process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000',
  discordLootpotEmoji: process.env.DISCORD_LOOTPOT_EMOJI || '🪙',
  discordLootEmoji: process.env.DISCORD_LOOT_EMOJI || '🪙',
  discordUsdEmoji: process.env.DISCORD_USD_EMOJI || '💵',
  lootpotLookbackBlocks: toBigInt(process.env.LOOTPOT_LOOKBACK_BLOCKS, 21600n),
  lootpotPollIntervalMs: toNumber(process.env.LOOTPOT_POLL_INTERVAL_MS, 120000),
  lootpotWorkerTest: process.env.LOOTPOT_WORKER_TEST === 'true',
  cacheWarmIntervalMs: toNumber(process.env.CACHE_WARM_INTERVAL_MS, 120000),
  rpcPrimary: process.env.RPC_URL_PRIMARY || 'https://mainnet.base.org',
  rpcFallback1: process.env.RPC_URL_FALLBACK_1 || 'https://base.llamarpc.com',
  rpcFallback2: process.env.RPC_URL_FALLBACK_2 || 'https://rpc.ankr.com/base',
  rpcFallback3: process.env.RPC_URL_FALLBACK_3 || 'https://base-rpc.publicnode.com',
  scanStartBlock: toBigInt(process.env.LOYALTY_SCAN_START_BLOCK || process.env.DEPLOY_SCAN_START_BLOCK, 0n),
  supabaseUrl: process.env.SUPABASE_URL || '',
  supabaseServiceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY || '',
}
