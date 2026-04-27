create table if not exists public.chat_messages (
  id uuid primary key default gen_random_uuid(),
  wallet_address text not null check (wallet_address ~ '^0x[a-f0-9]{40}$'),
  text text not null check (char_length(text) between 1 and 280),
  reply_to_id uuid references public.chat_messages(id) on delete set null,
  reactions jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

alter table public.chat_messages
  add column if not exists reply_to_id uuid references public.chat_messages(id) on delete set null;

alter table public.chat_messages
  add column if not exists reactions jsonb not null default '{}'::jsonb;

create index if not exists chat_messages_created_at_idx
  on public.chat_messages (created_at desc);

create index if not exists chat_messages_wallet_address_idx
  on public.chat_messages (wallet_address);

create index if not exists chat_messages_reply_to_id_idx
  on public.chat_messages (reply_to_id);

alter table public.chat_messages enable row level security;

-- API writes with SUPABASE_SERVICE_ROLE_KEY. No public insert policy is needed.
