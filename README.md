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
- the original private indexer infra from the upstream project
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
ENABLE_LOOTPOT_WORKER=false
ENABLE_DISCORD_PRICE_WORKER=false
ENABLE_DISCORD_METRIC_BOTS_WORKER=false
RPC_URL_PRIMARY=https://mainnet.base.org
RPC_URL_FALLBACK_1=https://base.llamarpc.com
RPC_URL_FALLBACK_2=https://rpc.ankr.com/base
RPC_URL_FALLBACK_3=https://base-rpc.publicnode.com
LOYALTY_SCAN_START_BLOCK=43103600
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
DISCORD_BOT_TOKEN=
DISCORD_GUILD_ID=
DISCORD_PRICE_POLL_INTERVAL_MS=600000
DISCORD_PRICE_DATABASE_URL=
DISCORD_METRIC_BOTS_GUILD_ID=
DISCORD_CIRCULATING_BOT_TOKEN=
DISCORD_BURNED_BOT_TOKEN=
DISCORD_METRIC_BOTS_POLL_INTERVAL_MS=120000
DISCORD_CIRCULATING_EMOJI=🪙
DISCORD_BURNED_EMOJI=🔥
DISCORD_CIRCULATING_STATUS=Circulating Supply
DISCORD_BURNED_STATUS=Burned
DISCORD_LOOTPOT_WEBHOOK_URL=
DISCORD_BOT_ASSET_BASE_URL=https://mineloot.app
DISCORD_LOOTPOT_EMOJI=🪙
DISCORD_LOOT_EMOJI=🪙
DISCORD_USD_EMOJI=💵
LOOTPOT_LOOKBACK_BLOCKS=21600
LOOTPOT_POLL_INTERVAL_MS=120000
```

Notes:
- `PORT` is injected by Railway automatically.
- Supabase is optional. If omitted, profile endpoints still work but return empty profile data.
- Healthcheck path is `/health`.

## Lootpot bot on Railway

Run the Discord lootpot notifier as a separate Railway service from the same repo:

```bash
npm run worker:lootpot
```

Recommended setup:
- keep the HTTP API service with start command `npm run start`
- create a second Railway service from the same repo with start command `npm run worker:lootpot`

If you prefer a single Railway service, you can also enable the notifier inline on the API service:

```env
ENABLE_LOOTPOT_WORKER=true
```

In that mode `npm run start` will launch the HTTP API and the lootpot polling loop in the same container.

The worker:
- polls Base for `RoundSettled` events with a non-zero lootpot
- deduplicates announcements through Supabase table `lootpot_announcements`
- posts the Discord embed directly through your webhook

Before enabling the worker, create the dedupe table in Supabase:

```sql
create table if not exists public.lootpot_announcements (
  round_id bigint primary key,
  created_at timestamptz not null default now()
);

create index if not exists lootpot_announcements_created_at_idx
  on public.lootpot_announcements (created_at desc);
```

For a one-shot validation run:

```bash
npm run worker:lootpot:test
```

## Discord price bot worker

You can run a Discord price-status bot from this same repo:

```bash
npm run worker:discord-price
```

Required environment variables:

```env
ENABLE_DISCORD_PRICE_WORKER=true
DISCORD_BOT_TOKEN=...
DISCORD_GUILD_ID=...
DISCORD_PRICE_POLL_INTERVAL_MS=600000
```

Optional:

```env
# If set, stores hourly snapshots and computes a 7d change for status text.
# If omitted, worker falls back to 24h change from DexScreener.
DISCORD_PRICE_DATABASE_URL=...
```

You can also enable it inline with the API service (single Railway service):

```env
ENABLE_DISCORD_PRICE_WORKER=true
```

The worker updates:
- bot nickname: `$<price> (↗/↘)`
- bot presence: `Watching 24h: +/-X.XX%`

If using snapshots DB, create this table first:

```sql
create table if not exists public.loot_price_snapshots (
  ts timestamptz not null default now(),
  price_usd numeric(38,18) not null
);

create index if not exists loot_price_snapshots_ts_desc_idx
  on public.loot_price_snapshots (ts desc);
```

## Discord metric bots worker (circulating + burned)

Run two dedicated Discord bots that update nickname/status with live protocol metrics:

```bash
npm run worker:discord-metrics
```

Required environment variables:

```env
ENABLE_DISCORD_METRIC_BOTS_WORKER=true
DISCORD_METRIC_BOTS_GUILD_ID=... # optional if DISCORD_GUILD_ID already set
DISCORD_CIRCULATING_BOT_TOKEN=...
DISCORD_BURNED_BOT_TOKEN=...
DISCORD_METRIC_BOTS_POLL_INTERVAL_MS=120000
```

Optional branding:

```env
DISCORD_CIRCULATING_EMOJI=🪙
DISCORD_BURNED_EMOJI=🔥
DISCORD_CIRCULATING_STATUS=Circulating Supply
DISCORD_BURNED_STATUS=Burned
```

You can run it inline on the API service too:

```env
ENABLE_DISCORD_METRIC_BOTS_WORKER=true
```
