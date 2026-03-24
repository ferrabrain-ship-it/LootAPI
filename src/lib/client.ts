import { createPublicClient, fallback, http } from 'viem'
import { base } from 'viem/chains'
import { env } from '../config/env.js'

const rpcUrls = [...new Set([
  env.rpcPrimary,
  env.rpcFallback1,
  env.rpcFallback2,
  env.rpcFallback3,
].filter((url): url is string => Boolean(url)))]

const transports = rpcUrls.map((url) => http(url, { timeout: env.rpcTimeoutMs, retryCount: 0 }))

export const publicClient = createPublicClient({
  chain: base,
  transport: transports.length === 1
    ? transports[0]
    : fallback(transports, { rank: false }),
})
