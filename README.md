# mineloot-api

Backend fork MVP for the Mineloot frontend.

What it covers:
- on-chain reads for `stats`, `price`, `round/current`, `round/:id`, `round/:id/miners`
- user rewards/history
- staking and autominer reads
- treasury stats and buyback history
- basic leaderboards
- SSE polling stream for round changes
- optional profile reads via Supabase

What it does not replicate yet:
- the original private indexer / webhook infra from the upstream project
- push-driven per-user SSE events
- auth/OAuth routes

## Run

```bash
npm install
cp .env.example .env
npm run dev
```

Default API base:
- `http://localhost:3001`

Recommended frontend env:

```env
NEXT_PUBLIC_API_URL=http://localhost:3001
NEXT_PUBLIC_APP_URL=http://localhost:3000
```

Recommended backend env:

```env
LOYALTY_SCAN_START_BLOCK=43103600
```

If you omit `LOYALTY_SCAN_START_BLOCK`, the API auto-detects the deployment block of the Mineloot contracts and scans from there. That works, but the first requests are slower.

## Deploy on Railway

This repo is ready for Railway with:
- [railway.json](/Users/brain/.openclaw/workspace/mineloot-api/railway.json)
- `npm run build`
- `npm run start`

Use these Railway environment variables:

```env
CORS_ORIGIN=https://mineloot.app
NEXT_PUBLIC_APP_URL=https://mineloot.app
RPC_URL_PRIMARY=https://mainnet.base.org
RPC_URL_FALLBACK_1=https://base.llamarpc.com
RPC_URL_FALLBACK_2=https://rpc.ankr.com/base
RPC_URL_FALLBACK_3=https://base-rpc.publicnode.com
LOYALTY_SCAN_START_BLOCK=43103600
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
```

Notes:
- `PORT` is injected by Railway automatically.
- Supabase is optional. If omitted, profile endpoints still work but return empty profile data.
- Healthcheck path is `/health`.
