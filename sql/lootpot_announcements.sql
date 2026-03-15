create table if not exists public.lootpot_announcements (
  round_id bigint primary key,
  created_at timestamptz not null default now()
);

create index if not exists lootpot_announcements_created_at_idx
  on public.lootpot_announcements (created_at desc);
