import { createPublicClient, fallback, http } from 'viem';
import { base } from 'viem/chains';
import { env } from '../config/env.js';
export const publicClient = createPublicClient({
    chain: base,
    transport: fallback([
        http(env.rpcPrimary),
        http(env.rpcFallback1),
        http(env.rpcFallback2),
        http(env.rpcFallback3),
    ]),
});
