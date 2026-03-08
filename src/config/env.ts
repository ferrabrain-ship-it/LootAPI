import 'dotenv/config'

export const env = {
  port: Number(process.env.PORT || 3001),
  corsOrigin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  appUrl: process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000',
  rpcPrimary: process.env.RPC_URL_PRIMARY || 'https://mainnet.base.org',
  rpcFallback1: process.env.RPC_URL_FALLBACK_1 || 'https://base.llamarpc.com',
  rpcFallback2: process.env.RPC_URL_FALLBACK_2 || 'https://rpc.ankr.com/base',
  rpcFallback3: process.env.RPC_URL_FALLBACK_3 || 'https://base-rpc.publicnode.com',
  scanStartBlock: BigInt(process.env.LOYALTY_SCAN_START_BLOCK || process.env.DEPLOY_SCAN_START_BLOCK || '0'),
  supabaseUrl: process.env.SUPABASE_URL || '',
  supabaseServiceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY || '',
}
