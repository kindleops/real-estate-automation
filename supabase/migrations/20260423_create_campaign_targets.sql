-- Migration: create campaign_targets table for Targeting Console v1
-- Tracks market-level targeting campaigns used by the Discord command center.

create table if not exists campaign_targets (
  id                           uuid        primary key default gen_random_uuid(),
  campaign_key                 text        unique not null,
  campaign_name                text,
  market                       text        not null,
  asset_type                   text        not null,
  strategy                     text        not null,
  language                     text        default 'auto',
  source_view_id               bigint,
  source_view_name             text,
  daily_cap                    int         default 50,
  status                       text        default 'draft',
  created_by_discord_user_id   text,
  approved_by_discord_user_id  text,
  last_scan_summary            jsonb       default '{}'::jsonb,
  last_scan_at                 timestamptz,
  last_launched_at             timestamptz,
  metadata                     jsonb       default '{}'::jsonb,
  created_at                   timestamptz default now(),
  updated_at                   timestamptz default now()
);

create index if not exists campaign_targets_status_idx
  on campaign_targets (status);

create index if not exists campaign_targets_market_idx
  on campaign_targets (market);

create index if not exists campaign_targets_asset_type_idx
  on campaign_targets (asset_type);

create index if not exists campaign_targets_strategy_idx
  on campaign_targets (strategy);
