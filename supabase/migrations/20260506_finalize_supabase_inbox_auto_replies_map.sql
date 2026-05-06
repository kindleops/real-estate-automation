-- Finalize Supabase-first inbox, enrichment, auto-replies, queue automation, and map support.

alter table if exists public.message_events
  add column if not exists updated_at timestamptz default now(),
  add column if not exists thread_key text,
  add column if not exists seller_display_name text,
  add column if not exists property_address text,
  add column if not exists market text,
  add column if not exists auto_reply_status text,
  add column if not exists auto_reply_queue_id text,
  add column if not exists detected_intent text,
  add column if not exists metadata jsonb default '{}'::jsonb;

alter table if exists public.message_events
  alter column metadata set default '{}'::jsonb;

create index if not exists idx_message_events_created_at_desc on public.message_events (created_at desc);
create index if not exists idx_message_events_direction_created_at_desc on public.message_events (direction, created_at desc);
create index if not exists idx_message_events_thread_key on public.message_events (thread_key);
create index if not exists idx_message_events_phone_pair_created_at_desc on public.message_events (from_phone_number, to_phone_number, created_at desc);
create index if not exists idx_message_events_property_id on public.message_events (property_id);
create index if not exists idx_message_events_master_owner_id on public.message_events (master_owner_id);

alter table if exists public.send_queue
  add column if not exists type text,
  add column if not exists thread_key text,
  add column if not exists seller_first_name text,
  add column if not exists seller_display_name text,
  add column if not exists market text,
  add column if not exists timezone text,
  add column if not exists textgrid_message_id text,
  add column if not exists template_source text,
  add column if not exists rendered_message text;

create index if not exists idx_send_queue_status_type_created_at_desc on public.send_queue (queue_status, type, created_at desc);
create index if not exists idx_send_queue_phone_pair_created_at_desc on public.send_queue (from_phone_number, to_phone_number, created_at desc);
create index if not exists idx_send_queue_thread_key on public.send_queue (thread_key);
create index if not exists idx_send_queue_provider_message_id on public.send_queue (provider_message_id);
create index if not exists idx_send_queue_textgrid_message_id on public.send_queue (textgrid_message_id);
