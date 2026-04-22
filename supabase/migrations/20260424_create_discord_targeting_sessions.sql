create table if not exists public.discord_targeting_sessions (
  id uuid primary key default gen_random_uuid(),
  session_key text unique not null,
  discord_user_id text not null,
  guild_id text,
  channel_id text,
  message_id text,
  state jsonb not null default '{}'::jsonb,
  status text not null default 'active',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  expires_at timestamptz
);

create index if not exists idx_discord_targeting_sessions_user
  on public.discord_targeting_sessions (discord_user_id);

create index if not exists idx_discord_targeting_sessions_status
  on public.discord_targeting_sessions (status);
