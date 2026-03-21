create table if not exists public.loot_price_snapshots (
  ts timestamptz not null default now(),
  price_usd numeric(38,18) not null
);

create index if not exists loot_price_snapshots_ts_desc_idx
  on public.loot_price_snapshots (ts desc);
