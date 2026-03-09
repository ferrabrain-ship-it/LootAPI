import { createPublicClient, fallback, http } from 'viem'
import { base } from 'viem/chains'
import { env } from '../config/env.js'

const RPC_TIMEOUT_MS = 4_000
const rpcUrls = [
  env.rpcPrimary,
  env.rpcFallback1,
  env.rpcFallback2,
  env.rpcFallback3,
].filter((url): url is string => Boolean(url))

export const publicClient = createPublicClient({
  chain: base,
  transport: fallback(
    rpcUrls.map((url) => http(url, { timeout: RPC_TIMEOUT_MS, retryCount: 0 }))
  ),
})
