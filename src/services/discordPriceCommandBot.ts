import {
  ActionRowBuilder,
  AttachmentBuilder,
  ButtonBuilder,
  ButtonStyle,
  Client,
  EmbedBuilder,
  GatewayIntentBits,
  type Message,
} from 'discord.js'
import { existsSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { chromium } from 'playwright'
import { CONTRACTS } from '../config/contracts.js'
import { env } from '../config/env.js'

type Logger = Pick<typeof console, 'info' | 'warn' | 'error'>

type Pair = {
  chainId?: string
  dexId?: string
  url?: string
  pairAddress?: string
  priceUsd?: string
  priceChange?: {
    h24?: number
    h6?: number
    h1?: number
  }
  volume?: {
    h24?: number
    h6?: number
    h1?: number
  }
  liquidity?: {
    usd?: number
  }
  info?: {
    openGraph?: string
  }
}

const SUPPORTED_WINDOWS = new Set([
  '15m',
  '1h',
  '4h',
  '12h',
  '24h',
  '1d',
  '7d',
  '1w',
])

type OhlcvTimeframe = 'minute' | 'hour' | 'day'

type OhlcvWindowConfig = {
  timeframe: OhlcvTimeframe
  aggregate: number
  limit: number
}

type OhlcvPoint = {
  timestampSec: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

const WINDOW_CONFIG: Record<string, OhlcvWindowConfig> = {
  '15m': { timeframe: 'minute', aggregate: 15, limit: 96 },
  '1h': { timeframe: 'hour', aggregate: 1, limit: 96 },
  '4h': { timeframe: 'hour', aggregate: 4, limit: 96 },
  '12h': { timeframe: 'hour', aggregate: 12, limit: 96 },
  '24h': { timeframe: 'day', aggregate: 1, limit: 90 },
  '1d': { timeframe: 'day', aggregate: 1, limit: 90 },
  '7d': { timeframe: 'day', aggregate: 7, limit: 90 },
  '1w': { timeframe: 'day', aggregate: 7, limit: 90 },
}

const PLAYWRIGHT_BROWSERS_PATH_CANDIDATES = [
  '/app/ms-playwright',
  '/ms-playwright',
  '/root/.cache/ms-playwright',
]

function resolvePlaywrightExecutablePathFromBrowsersPath(browsersPath: string) {
  if (!browsersPath || !existsSync(browsersPath)) return undefined

  try {
    const entries = readdirSync(browsersPath, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)

    const chromiumDir = entries.find((entry) => entry.startsWith('chromium-'))
    if (chromiumDir) {
      const candidate = join(browsersPath, chromiumDir, 'chrome-linux', 'chrome')
      if (existsSync(candidate)) return candidate
    }

    const headlessDir = entries.find((entry) => entry.startsWith('chromium_headless_shell-'))
    if (headlessDir) {
      const candidate = join(
        browsersPath,
        headlessDir,
        'chrome-headless-shell-linux64',
        'chrome-headless-shell'
      )
      if (existsSync(candidate)) return candidate
    }
  } catch {
    return undefined
  }

  return undefined
}

function ensurePlaywrightRuntimeConfig() {
  const explicitExecutable = env.playwrightChromiumExecutablePath.trim()
  if (explicitExecutable) {
    return {
      browsersPath: process.env.PLAYWRIGHT_BROWSERS_PATH || '',
      executablePath: explicitExecutable,
    }
  }

  const configuredPath = process.env.PLAYWRIGHT_BROWSERS_PATH?.trim()
  if (configuredPath) {
    const executable = resolvePlaywrightExecutablePathFromBrowsersPath(configuredPath)
    if (executable) {
      process.env.PLAYWRIGHT_BROWSERS_PATH = configuredPath
      return { browsersPath: configuredPath, executablePath: executable }
    }
  }

  for (const candidate of PLAYWRIGHT_BROWSERS_PATH_CANDIDATES) {
    const executable = resolvePlaywrightExecutablePathFromBrowsersPath(candidate)
    if (executable) {
      process.env.PLAYWRIGHT_BROWSERS_PATH = candidate
      return { browsersPath: candidate, executablePath: executable }
    }
  }

  // Last-resort fallback, Playwright may still resolve bundled executable if available.
  const fallbackPath = configuredPath || '/app/ms-playwright'
  process.env.PLAYWRIGHT_BROWSERS_PATH = fallbackPath
  return {
    browsersPath: fallbackPath,
    executablePath: undefined,
  }
}

function numeric(value: unknown) {
  const parsed = typeof value === 'number' ? value : Number.parseFloat(String(value ?? ''))
  return Number.isFinite(parsed) ? parsed : 0
}

function formatSignedPercent(value: number) {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

function formatUsd(value: number) {
  if (!Number.isFinite(value)) return '$0.00'
  if (value >= 1) {
    return `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 4 }).format(value)}`
  }
  return `$${value.toFixed(6)}`
}

function parseCommandWindow(content: string) {
  const trimmed = content.trim().toLowerCase()
  const tokens = trimmed.split(/\s+/).filter(Boolean)
  if (!tokens.length) return null

  const prefix = env.discordPriceCommandPrefix || 'price'
  if (tokens[0] !== prefix && tokens[0] !== `!${prefix}` && tokens[0] !== `/${prefix}`) {
    return null
  }

  if (tokens.length < 2) return '4h'
  const requested = tokens[1]
  if (!SUPPORTED_WINDOWS.has(requested)) return null
  return requested
}

function decoratePairUrl(url: string, requestedWindow: string) {
  try {
    const parsed = new URL(url)
    parsed.searchParams.set('utm_source', 'mineloot-discord-bot')
    parsed.searchParams.set('interval', requestedWindow)
    return parsed.toString()
  } catch {
    return url
  }
}

function toDexScreenerInterval(requestedWindow: string) {
  switch (requestedWindow) {
    case '15m':
      return '15'
    case '1h':
      return '60'
    case '4h':
      return '240'
    case '12h':
      return '720'
    case '24h':
    case '1d':
      return 'D'
    case '7d':
    case '1w':
      return 'W'
    default:
      return '240'
  }
}

function buildDexScreenerEmbedUrl(pairUrl: string, requestedWindow: string) {
  const parsed = new URL(pairUrl)
  const interval = toDexScreenerInterval(requestedWindow)
  parsed.searchParams.set('embed', '1')
  parsed.searchParams.set('loadChartSettings', '0')
  parsed.searchParams.set('trades', '0')
  parsed.searchParams.set('tabs', '0')
  parsed.searchParams.set('info', '0')
  parsed.searchParams.set('chartLeftToolbar', '0')
  parsed.searchParams.set('chartTimeframesToolbar', '0')
  parsed.searchParams.set('chartDefaultOnMobile', '1')
  parsed.searchParams.set('chartTheme', 'dark')
  parsed.searchParams.set('theme', 'dark')
  parsed.searchParams.set('chartStyle', '1')
  parsed.searchParams.set('chartType', 'usd')
  parsed.searchParams.set('interval', interval)
  parsed.searchParams.set('utm_source', 'mineloot-discord-bot')
  return parsed.toString()
}

async function getBestLootPair() {
  const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${CONTRACTS.loot}`, {
    cache: 'no-store',
  })

  if (!response.ok) {
    throw new Error(`DexScreener ${response.status}`)
  }

  const data = await response.json() as { pairs?: Pair[] }
  const pairs = data.pairs ?? []
  if (!pairs.length) {
    throw new Error('No LOOT pair available on DexScreener')
  }

  return [...pairs].sort(
    (left, right) => numeric(right.liquidity?.usd) - numeric(left.liquidity?.usd)
  )[0]
}

async function getOhlcvFromGecko(pairAddress: string, requestedWindow: string) {
  const config = WINDOW_CONFIG[requestedWindow]
  if (!config) {
    throw new Error(`Unsupported window: ${requestedWindow}`)
  }

  const url = new URL(
    `https://api.geckoterminal.com/api/v2/networks/base/pools/${pairAddress}/ohlcv/${config.timeframe}`
  )
  url.searchParams.set('aggregate', String(config.aggregate))
  url.searchParams.set('limit', String(config.limit))
  url.searchParams.set('currency', 'usd')

  const response = await fetch(url.toString(), { cache: 'no-store' })
  if (!response.ok) {
    throw new Error(`GeckoTerminal ${response.status}`)
  }

  const data = await response.json() as {
    data?: {
      attributes?: {
        ohlcv_list?: Array<[number, number, number, number, number, number]>
      }
    }
  }

  const raw = data.data?.attributes?.ohlcv_list ?? []
  const parsed: OhlcvPoint[] = raw
    .map((entry) => ({
      timestampSec: Number(entry[0]),
      open: Number(entry[1]),
      high: Number(entry[2]),
      low: Number(entry[3]),
      close: Number(entry[4]),
      volume: Number(entry[5]),
    }))
    .filter((entry) => (
      Number.isFinite(entry.timestampSec) &&
      Number.isFinite(entry.open) &&
      Number.isFinite(entry.high) &&
      Number.isFinite(entry.low) &&
      Number.isFinite(entry.close)
    ))
    .sort((left, right) => left.timestampSec - right.timestampSec)

  return parsed
}

function formatAxisTs(timestampSec: number, requestedWindow: string) {
  const config = WINDOW_CONFIG[requestedWindow]
  const date = new Date(timestampSec * 1000)
  if (config?.timeframe === 'day') {
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
    }).format(date)
  }

  return new Intl.DateTimeFormat('en-US', {
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

function percentile(values: number[], p: number) {
  if (!values.length) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const clamped = Math.max(0, Math.min(1, p))
  const idx = (sorted.length - 1) * clamped
  const lo = Math.floor(idx)
  const hi = Math.ceil(idx)
  if (lo === hi) return sorted[lo]
  const weight = idx - lo
  return sorted[lo] * (1 - weight) + sorted[hi] * weight
}

function getPriceTickPrecision(candles: OhlcvPoint[]) {
  const latest = candles[candles.length - 1]?.close ?? 0
  if (latest >= 1000) return 2
  if (latest >= 100) return 3
  if (latest >= 1) return 4
  if (latest >= 0.1) return 5
  return 6
}

function pickCandlesForRender(candles: OhlcvPoint[], requestedWindow: string) {
  if (candles.length <= 90) return candles

  const window = requestedWindow.toLowerCase()
  const targetCount =
    window === '15m'
      ? 160
      : window === '1h'
        ? 120
        : window === '4h'
          ? 100
          : 90

  const clampedTarget = Math.max(60, Math.min(targetCount, candles.length))
  return candles.slice(-clampedTarget)
}

function buildEmaSeries(candles: OhlcvPoint[], period: number) {
  if (!candles.length) return [] as Array<{ x: number; y: number }>
  const alpha = 2 / (period + 1)
  const series: Array<{ x: number; y: number }> = []
  let ema = candles[0].close

  for (let index = 0; index < candles.length; index += 1) {
    const close = candles[index].close
    ema = index === 0 ? close : (close * alpha) + (ema * (1 - alpha))
    series.push({ x: index + 1, y: ema })
  }

  return series
}

function deriveYBounds(candles: OhlcvPoint[]) {
  const focusWindow = Math.min(candles.length, 72)
  const focusCandles = candles.slice(-focusWindow)
  const highs = focusCandles.map((entry) => entry.high).filter(Number.isFinite)
  const lows = focusCandles.map((entry) => entry.low).filter(Number.isFinite)

  if (!highs.length || !lows.length) {
    return { min: 0, max: 1 }
  }

  const p95 = percentile(highs, 0.95)
  const p05 = percentile(lows, 0.05)
  const latest = focusCandles[focusCandles.length - 1]
  const max = Math.max(p95, latest?.high ?? p95)
  const minBase = Math.min(p05, latest?.low ?? p05)
  const span = Math.max(max - minBase, max * 0.08, 1e-8)

  return {
    min: Math.max(0, minBase - span * 0.1),
    max: max + span * 0.14,
  }
}

async function buildQuickChartImage(rawCandles: OhlcvPoint[], requestedWindow: string) {
  const candles = pickCandlesForRender(rawCandles, requestedWindow)
  if (candles.length < 2) {
    throw new Error('Not enough candles to render chart')
  }

  const y = deriveYBounds(candles)
  const labels = candles.map((entry) => formatAxisTs(entry.timestampSec, requestedWindow))
  const xTickStep = Math.max(1, Math.floor(labels.length / 7))
  const yPrecision = getPriceTickPrecision(candles)
  const emaPeriod = requestedWindow === '15m' ? 21 : requestedWindow === '1h' ? 21 : 14
  const emaSeries = buildEmaSeries(candles, emaPeriod)
  const lastClose = candles[candles.length - 1]?.close ?? 0
  const maxVolume = Math.max(1, ...candles.map((entry) => entry.volume))
  const candleData = candles.map((entry, index) => ({
    x: index + 1,
    o: entry.open,
    h: entry.high,
    l: entry.low,
    c: entry.close,
  }))

  const volumeData = candles.map((entry, index) => ({
    x: index + 1,
    y: entry.volume,
  }))
  const volumeColors = candles.map((entry) =>
    entry.close >= entry.open ? 'rgba(53, 212, 127, 0.26)' : 'rgba(255, 107, 107, 0.24)'
  )

  const chart = {
    type: 'candlestick',
    plugins: [
      {
        id: 'timeStrip',
        beforeDraw: `function(chart){
          const x = chart.scales && chart.scales.x;
          const area = chart.chartArea;
          if (!x || !area) return;
          const ctx = chart.ctx;
          const top = area.bottom + 1;
          const bottom = x.bottom;
          if (bottom <= top) return;
          ctx.save();
          ctx.fillStyle = 'rgba(8, 15, 31, 0.92)';
          ctx.fillRect(area.left, top, area.right - area.left, bottom - top);
          ctx.strokeStyle = 'rgba(123, 142, 184, 0.26)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(area.left, top);
          ctx.lineTo(area.right, top);
          ctx.stroke();
          ctx.restore();
        }`,
      },
    ],
    data: {
      labels,
      datasets: [
        {
          label: 'Price',
          data: candleData,
          yAxisID: 'yPrice',
          color: {
            up: '#35d47f',
            down: '#ff6b6b',
            unchanged: '#8ea3c6',
          },
          borderColor: '#22304f',
          borderWidth: 1,
          order: 3,
        },
        {
          type: 'bar',
          label: 'Volume',
          data: volumeData,
          yAxisID: 'yVolume',
          backgroundColor: volumeColors,
          borderWidth: 0,
          barPercentage: 1.0,
          categoryPercentage: 0.95,
          order: 1,
        },
        {
          type: 'line',
          label: `EMA ${emaPeriod}`,
          data: emaSeries,
          parsing: false,
          yAxisID: 'yPrice',
          borderColor: '#4da3ff',
          borderWidth: 1.6,
          pointRadius: 0,
          tension: 0.18,
          order: 4,
        },
        {
          type: 'line',
          label: 'Last',
          data: [
            { x: 1, y: lastClose },
            { x: candles.length, y: lastClose },
          ],
          parsing: false,
          yAxisID: 'yPrice',
          borderColor: '#f0b90b',
          borderWidth: 1.2,
          borderDash: [6, 5],
          pointRadius: 0,
          tension: 0,
          order: 2,
        },
      ],
    },
    options: {
      animation: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        legend: { display: false },
        title: {
          display: true,
          text: `LOOT / USD • ${requestedWindow.toUpperCase()}`,
          color: '#f5f7fb',
          font: { size: 21, weight: 'bold' },
          padding: { top: 10, bottom: 10 },
        },
      },
      layout: {
        padding: { left: 12, right: 14, top: 8, bottom: 34 },
      },
      scales: {
        x: {
          offset: true,
          grid: {
            color: 'rgba(123, 142, 184, 0.14)',
            drawOnChartArea: false,
            drawTicks: false,
          },
          border: {
            display: true,
            color: 'rgba(123, 142, 184, 0.28)',
          },
          ticks: {
            color: '#96a8cb',
            autoSkip: false,
            maxTicksLimit: 10,
            minRotation: 0,
            maxRotation: 0,
            callback: `function(value){
              const idx = Number(value);
              if (!Number.isFinite(idx)) return this.getLabelForValue(value);
              if (idx % ${xTickStep} !== 0 && idx !== ${labels.length - 1}) return '';
              return this.getLabelForValue(idx);
            }`,
            font: { size: 11, weight: '600' },
            padding: 12,
          },
        },
        y: {
          position: 'right',
          min: y.min,
          max: y.max,
          grid: { color: 'rgba(123, 142, 184, 0.16)' },
          ticks: {
            color: '#d2def4',
            callback: `function(value){
              const n = Number(value);
              if (!Number.isFinite(n)) return '';
              if (Math.abs(n) >= 1000) return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
              return n.toFixed(${yPrecision});
            }`,
            maxTicksLimit: 7,
            font: { size: 10 },
          },
        },
        volume: {
          position: 'left',
          beginAtZero: true,
          max: maxVolume * 4,
          display: false,
        },
      },
    },
  }

  const createResponse = await fetch('https://quickchart.io/chart/create', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      version: '4',
      width: 1400,
      height: 860,
      devicePixelRatio: 2,
      format: 'png',
      backgroundColor: '#0a1020',
      chart,
    }),
  })

  if (!createResponse.ok) {
    throw new Error(`QuickChart create ${createResponse.status}`)
  }

  const payload = await createResponse.json() as { success?: boolean; url?: string }
  if (!payload?.url) {
    throw new Error('QuickChart did not return a render URL')
  }

  const imageResponse = await fetch(payload.url, { cache: 'no-store' })
  if (!imageResponse.ok) {
    throw new Error(`QuickChart render ${imageResponse.status}`)
  }

  const mime = imageResponse.headers.get('content-type') || ''
  if (!mime.includes('image/')) {
    throw new Error(`QuickChart render returned non-image content-type: ${mime}`)
  }

  const bytes = Buffer.from(await imageResponse.arrayBuffer())
  return bytes
}

async function buildDexScreenerChartImage(pairUrl: string, requestedWindow: string, logger: Logger) {
  const runtime = ensurePlaywrightRuntimeConfig()
  const embedUrl = buildDexScreenerEmbedUrl(pairUrl, requestedWindow)
  const browser = await chromium.launch({
    headless: true,
    executablePath: runtime.executablePath,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-zygote',
      '--single-process',
      '--disable-background-networking',
      '--disable-background-timer-throttling',
      '--disable-renderer-backgrounding',
    ],
  })

  try {
    const context = await browser.newContext({
      viewport: { width: 1400, height: 920 },
      deviceScaleFactor: 2,
      colorScheme: 'dark',
      userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
    })
    const page = await context.newPage()

    await page.goto(embedUrl, {
      waitUntil: 'domcontentloaded',
      timeout: env.discordPriceCommandRenderTimeoutMs,
    })

    await page.waitForTimeout(3000)

    // Force chart mode to Price + USD when the toggle bar is visible.
    await page.evaluate(() => {
      const clickToggleLeft = (pattern: RegExp) => {
        const nodes = Array.from(document.querySelectorAll('button,div,span')) as HTMLElement[]
        const target = nodes.find((node) => pattern.test((node.textContent || '').replace(/\s+/g, ' ').trim()))
        if (!target) return
        const rect = target.getBoundingClientRect()
        if (!rect.width || !rect.height) return
        const x = rect.left + Math.max(8, rect.width * 0.22)
        const y = rect.top + rect.height / 2
        const element = document.elementFromPoint(x, y) as HTMLElement | null
        const receiver = element || target
        receiver.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, clientX: x, clientY: y, view: window }))
        receiver.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, clientX: x, clientY: y, view: window }))
        receiver.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, clientX: x, clientY: y, view: window }))
        receiver.dispatchEvent(new MouseEvent('click', { bubbles: true, clientX: x, clientY: y, view: window }))
      }

      clickToggleLeft(/Price\s*\/\s*MCAP/i)
      clickToggleLeft(/USD\s*\/\s*ETH/i)
    })
    await page.waitForTimeout(450)

    try {
      await page.locator('canvas').first().waitFor({
        state: 'visible',
        timeout: 6000,
      })
    } catch (error) {
      logger.warn(
        {
          embedUrl,
          error: error instanceof Error ? error.message : String(error),
        },
        '[discord-price-command-bot] canvas wait timeout, taking fallback body screenshot'
      )
    }

    // Focus chart and zoom in a bit so candles remain readable inside Discord embeds.
    const viewport = page.viewportSize() ?? { width: 1400, height: 920 }
    const centerX = Math.floor(viewport.width * 0.52)
    const centerY = Math.floor(viewport.height * 0.55)
    await page.mouse.move(centerX, centerY)
    await page.mouse.click(centerX, centerY)
    const zoomSteps = requestedWindow === '15m'
      ? 22
      : requestedWindow === '1h'
        ? 18
        : requestedWindow === '4h'
          ? 14
          : 12
    for (let i = 0; i < zoomSteps; i += 1) {
      await page.mouse.wheel(0, -760)
      await page.waitForTimeout(45)
    }
    await page.waitForTimeout(500)

    const clipX = 20
    const clipY = 66
    const clipWidth = Math.max(100, viewport.width - clipX * 2)
    const clipHeight = Math.max(100, viewport.height - clipY - 24)

    const image = await page.screenshot({
      type: 'png',
      animations: 'disabled',
      caret: 'hide',
      fullPage: false,
      clip: {
        x: clipX,
        y: clipY,
        width: clipWidth,
        height: clipHeight,
      },
      timeout: env.discordPriceCommandRenderTimeoutMs,
    })

    await context.close()
    return image
  } finally {
    await browser.close()
  }
}

async function respondPrice(message: Message, requestedWindow: string, logger: Logger) {
  const pair = await getBestLootPair()

  const priceUsd = numeric(pair.priceUsd)
  const change24h = numeric(pair.priceChange?.h24)
  const change6h = numeric(pair.priceChange?.h6)
  const change1h = numeric(pair.priceChange?.h1)
  const vol24h = numeric(pair.volume?.h24)
  const liqUsd = numeric(pair.liquidity?.usd)

  const title = `LOOT / USD • ${requestedWindow.toUpperCase()}`
  const openUrl = pair.url ? decoratePairUrl(pair.url, requestedWindow) : 'https://dexscreener.com/base'
  let chartAttachment: AttachmentBuilder | null = null
  let renderSource: 'dexscreener' | 'quickchart' | 'openGraph' = 'openGraph'
  let renderNote: string | null = null

  if (env.discordPriceCommandRenderMode === 'dexscreener' && pair.url) {
    try {
      const image = await buildDexScreenerChartImage(pair.url, requestedWindow, logger)
      chartAttachment = new AttachmentBuilder(image, { name: 'loot-chart.png' })
      renderSource = 'dexscreener'
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      renderNote = `dex fail: ${message}`.slice(0, 80)
      logger.warn(
        {
          requestedWindow,
          pairUrl: pair.url,
          error: message,
        },
        '[discord-price-command-bot] dexscreener screenshot failed; fallback to quickchart'
      )
    }
  }

  if (!chartAttachment && pair.pairAddress) {
    try {
      const ohlcv = await getOhlcvFromGecko(pair.pairAddress, requestedWindow)
      if (ohlcv.length >= 2) {
        const chartBytes = await buildQuickChartImage(ohlcv, requestedWindow)
        chartAttachment = new AttachmentBuilder(chartBytes, { name: 'loot-chart.png' })
        renderSource = 'quickchart'
      }
    } catch (error) {
      logger.warn(
        {
          requestedWindow,
          pairAddress: pair.pairAddress,
          error: error instanceof Error ? error.message : String(error),
        },
        '[discord-price-command-bot] failed to build candle chart; fallback to openGraph'
      )
    }
  }

  const embed = new EmbedBuilder()
    .setColor(0x2ed66f)
    .setTitle(title)
    .setURL(openUrl)
    .setDescription(
      renderSource === 'dexscreener'
        ? 'DexScreener pair + screenshot chart.'
        : renderSource === 'quickchart'
          ? 'DexScreener pair + live candle chart.'
          : 'DexScreener pair overview.'
    )
    .addFields(
      { name: 'Price', value: formatUsd(priceUsd), inline: true },
      { name: '24h', value: formatSignedPercent(change24h), inline: true },
      { name: 'Liquidity', value: formatUsd(liqUsd), inline: true },
      { name: '1h', value: formatSignedPercent(change1h), inline: true },
      { name: '6h', value: formatSignedPercent(change6h), inline: true },
      { name: '24h Volume', value: formatUsd(vol24h), inline: true },
    )
    .setFooter({
      text: `Requested by ${message.author.username} • render: ${renderSource}${renderNote ? ` • ${renderNote}` : ''}`,
    })
    .setTimestamp(new Date())

  if (chartAttachment) {
    embed.setImage('attachment://loot-chart.png')
  } else if (pair.info?.openGraph) {
    embed.setImage(pair.info.openGraph)
  }

  const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder()
      .setStyle(ButtonStyle.Link)
      .setLabel(`Open ${requestedWindow.toUpperCase()} Chart`)
      .setURL(openUrl)
  )

  await message.reply(
    chartAttachment
      ? {
          embeds: [embed],
          components: [row],
          files: [chartAttachment],
          allowedMentions: { repliedUser: false },
        }
      : {
          embeds: [embed],
          components: [row],
          allowedMentions: { repliedUser: false },
        }
  )

  logger.info(
    {
      channelId: message.channelId,
      guildId: message.guildId,
      window: requestedWindow,
      openUrl,
    },
    '[discord-price-command-bot] responded'
  )
}

export function createDiscordPriceCommandBot(options?: { logger?: Logger }) {
  const logger = options?.logger ?? console
  const enabled = env.enableDiscordPriceCommandBot
  const token = env.discordPriceCommandBotToken.trim()
  const allowedGuildId = env.discordPriceCommandAllowedGuildId.trim()
  const allowedChannelId = env.discordPriceCommandAllowedChannelId.trim()
  let started = false
  let stopping = false

  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent,
    ],
  })

  client.on('messageCreate', async (message) => {
    if (stopping) return
    if (message.author.bot) return
    if (!message.inGuild()) return
    if (allowedGuildId && message.guildId !== allowedGuildId) return
    if (allowedChannelId && message.channelId !== allowedChannelId) return

    const requestedWindow = parseCommandWindow(message.content)
    if (!requestedWindow) return

    try {
      await respondPrice(message, requestedWindow, logger)
    } catch (error) {
      logger.error(error, '[discord-price-command-bot] reply failed')
      await message.reply({
        content: 'Price bot temporary unavailable. Retry in a few seconds.',
        allowedMentions: { repliedUser: false },
      }).catch(() => {})
    }
  })

  const stop = async () => {
    stopping = true
    if (client.isReady()) {
      client.destroy()
    }
  }

  const start = async () => {
    if (started) return
    started = true

    if (!enabled) {
      logger.info('[discord-price-command-bot] disabled (ENABLE_DISCORD_PRICE_COMMAND_BOT != true)')
      return
    }

    if (!token) {
      logger.warn('[discord-price-command-bot] missing DISCORD_PRICE_COMMAND_BOT_TOKEN; bot not started')
      return
    }

    await client.login(token)
    logger.info(
      {
        allowedGuildId: allowedGuildId || null,
        allowedChannelId: allowedChannelId || null,
        prefix: env.discordPriceCommandPrefix || 'price',
      },
      '[discord-price-command-bot] ready'
    )
  }

  return {
    enabled,
    start,
    stop,
  }
}
