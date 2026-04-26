-- Track unknown inbound phone numbers and their triage lifecycle.
create table if not exists public.unknown_inbound_contacts (
  id uuid primary key default gen_random_uuid(),
  phone_e164 text not null unique,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  message_count integer not null default 0,
  last_message_body text,
  unknown_bucket text,
  classification_confidence numeric,
  resolved_status text not null default 'unresolved',
  linked_master_owner_id text,
  linked_property_id text,
  linked_prospect_id text,
  auto_reply_sent_at timestamptz,
  auto_reply_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_unknown_inbound_contacts_last_seen_at
  on public.unknown_inbound_contacts (last_seen_at desc nulls last);

create index if not exists idx_unknown_inbound_contacts_unknown_bucket
  on public.unknown_inbound_contacts (unknown_bucket);

create index if not exists idx_unknown_inbound_contacts_resolved_status
  on public.unknown_inbound_contacts (resolved_status);
