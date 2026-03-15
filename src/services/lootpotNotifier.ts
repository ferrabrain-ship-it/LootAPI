import { parseAbi } from 'viem'
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'
import { publicClient } from '../lib/client.js'
import { supabaseAdmin } from '../lib/supabase.js'

const ROUND_SETTLED = parseAbi([
  'event RoundSettled(uint64 indexed roundId, uint8 winningBlock, address topMiner, uint256 totalWinnings, uint256 topMinerReward, uint256 lootpotAmount, bool isSplit, uint256 topMinerSeed, uint256 winnersDeployed)',
])

const LOOT_LOGO_URL = `${env.discordBotAssetBaseUrl.replace(/\/$/, '')}/loot-logo-icon.png`

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>

function isMissingLootpotAnnouncementsTable(message: string) {
  return message.includes("Could not find the table 'public.lootpot_announcements'")
}

function formatLoot(value: number) {
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 3,
    maximumFractionDigits: 3,
  }).format(value)
}

function formatUsd(value: number) {
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value)
}

async function getLootPriceUsd() {
  const response = await fetch(
    `https://api.dexscreener.com/latest/dex/tokens/${CONTRACTS.loot}`,
    { cache: 'no-store' }
  )

  if (!response.ok) {
    throw new Error(`DexScreener ${response.status}`)
  }

  const data = await response.json()
  const pairs: Array<{ priceUsd: string; liquidity?: { usd?: number } }> = data.pairs ?? []
  if (!pairs.length) {
    throw new Error('No LOOT pairs found on DexScreener')
  }

  const best = pairs.sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0))[0]
  return Number(best.priceUsd)
}

async function postLootpotEmbed(roundId: number, winningBlock: number, lootAmount: number, lootUsd: number, txHash?: `0x${string}`) {
  if (!env.discordLootpotWebhookUrl) {
    throw new Error('DISCORD_LOOTPOT_WEBHOOK_URL not set')
  }

  const totalUsdValue = lootUsd * lootAmount
  const embed = {
    title: `${env.discordLootpotEmoji} LOOTPOT — Round #${roundId}`,
    description: `**Block #${winningBlock}** just hit the lootpot!`,
    color: 0xe2ab2f,
    fields: [
      { name: `${env.discordLootEmoji} Total LOOT`, value: formatLoot(lootAmount), inline: false },
      { name: `${env.discordUsdEmoji} USD Value`, value: `~$${formatUsd(totalUsdValue)}`, inline: false },
    ],
    footer: { text: 'mineloot.app' },
    timestamp: new Date().toISOString(),
    url: txHash ? `https://basescan.org/tx/${txHash}` : env.appUrl,
  }

  const response = await fetch(env.discordLootpotWebhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      username: 'MineLoot Bot',
      avatar_url: LOOT_LOGO_URL,
      allowed_mentions: { parse: [] },
      embeds: [embed],
    }),
  })

  if (!response.ok) {
    throw new Error(`Discord webhook failed: ${await response.text()}`)
  }
}

function getLookbackBlocks(requested?: bigint) {
  if (requested && requested > 0n) return requested
  if (env.lootpotLookbackBlocks > 0n) return env.lootpotLookbackBlocks
  return 21600n
}

async function requireSupabase() {
  if (!supabaseAdmin) {
    throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required for lootpot dedupe')
  }
  return supabaseAdmin
}

export async function runLootpotNotifierOnce(options?: {
  test?: boolean
  lookbackBlocks?: bigint
  logger?: Logger
}) {
  const logger = options?.logger ?? console

  if (options?.test) {
    const lootUsd = await getLootPriceUsd()
    await postLootpotEmbed(9999, 16, 30.921, lootUsd)
    return { ok: true, test: true, lootUsd, announced: 1 }
  }

  const supabase = await requireSupabase()
  const lootUsd = await getLootPriceUsd()
  const latestBlock = await publicClient.getBlockNumber()
  const lookbackBlocks = getLookbackBlocks(options?.lookbackBlocks)
  const fromBlock = latestBlock > lookbackBlocks ? latestBlock - lookbackBlocks : 0n

  const logs = await publicClient.getLogs({
    address: CONTRACTS.gridMining,
    event: ROUND_SETTLED[0],
    fromBlock,
    toBlock: latestBlock,
  })

  const lootpotLogs = logs
    .filter((log) => (log.args.lootpotAmount ?? 0n) > 0n)
    .sort((a, b) => Number(a.blockNumber - b.blockNumber))

  let announced = 0

  for (const log of lootpotLogs) {
    const roundId = Number(log.args.roundId)
    const winningBlock = Number(log.args.winningBlock) + 1
    const lootAmount = Number(log.args.lootpotAmount ?? 0n) / 1e18

    const { data: existing, error: selectError } = await supabase
      .from('lootpot_announcements')
      .select('round_id')
      .eq('round_id', roundId)
      .maybeSingle()

    if (selectError) {
      if (isMissingLootpotAnnouncementsTable(selectError.message)) {
        throw new Error(
          "Missing Supabase table public.lootpot_announcements. Run sql/lootpot_announcements.sql in Supabase, then redeploy or restart the worker."
        )
      }
      throw new Error(`Supabase select failed for round #${roundId}: ${selectError.message}`)
    }

    if (existing) continue

    await postLootpotEmbed(roundId, winningBlock, lootAmount, lootUsd, log.transactionHash)

    const { error: insertError } = await supabase
      .from('lootpot_announcements')
      .insert({ round_id: roundId })

    if (insertError) {
      if (isMissingLootpotAnnouncementsTable(insertError.message)) {
        throw new Error(
          "Missing Supabase table public.lootpot_announcements. Run sql/lootpot_announcements.sql in Supabase, then redeploy or restart the worker."
        )
      }
      throw new Error(`Supabase insert failed for round #${roundId}: ${insertError.message}`)
    }

    announced += 1
    logger.info(`[lootpot-worker] announced round #${roundId}`)
  }

  return {
    ok: true,
    announced,
    scannedFromBlock: fromBlock.toString(),
    scannedToBlock: latestBlock.toString(),
    lookbackBlocks: lookbackBlocks.toString(),
  }
}
