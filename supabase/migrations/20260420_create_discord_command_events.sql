-- Migration: create discord_command_events audit table
-- Date: 2026-04-20
--
-- Records every Discord slash command and button interaction processed by
-- POST /api/webhooks/discord/interactions.
--
-- Design notes:
--   - Append-only: rows are never updated after insert (except for the
--     approval / executed_at columns on pending records).
--   - role_ids is stored as text[] to preserve the exact Discord snowflake
--     strings without any schema coupling to a roles table.
--   - options is JSONB for flexible per-command payload storage.
--   - approval_token is UUIDv4 for pending-approval rows; NULL otherwise.

CREATE TABLE IF NOT EXISTS discord_command_events (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Discord interaction identifiers
  interaction_id       TEXT        NOT NULL,
  guild_id             TEXT,
  channel_id           TEXT,
  user_id              TEXT        NOT NULL,
  username             TEXT,

  -- Command routing
  command_name         TEXT        NOT NULL,
  subcommand           TEXT,
  options              JSONB,
  role_ids             TEXT[],

  -- Access control outcome
  -- allowed | denied
  permission_outcome   TEXT        NOT NULL DEFAULT 'allowed',

  -- Execution outcome
  -- executed | approval_pending | approved | rejected | started | error | responded
  action_outcome       TEXT,

  -- Approval workflow
  approval_token       TEXT        UNIQUE,
  approved_by_user_id  TEXT,

  -- Result tracking (secrets must never be stored here)
  result_summary       TEXT,
  error_message        TEXT,

  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  executed_at          TIMESTAMPTZ
);

-- Index: look up by user for per-user audit queries
CREATE INDEX IF NOT EXISTS discord_command_events_user_id_idx
  ON discord_command_events (user_id);

-- Index: look up pending approvals by token (approval flow)
CREATE INDEX IF NOT EXISTS discord_command_events_approval_token_idx
  ON discord_command_events (approval_token)
  WHERE approval_token IS NOT NULL;

-- Index: time-series queries (most-recent-first)
CREATE INDEX IF NOT EXISTS discord_command_events_created_at_idx
  ON discord_command_events (created_at DESC);

-- RLS: service role only — the Discord webhook runs with the service role key.
ALTER TABLE discord_command_events ENABLE ROW LEVEL SECURITY;

-- Allow the service role (used by the Next.js backend) full access.
-- Authenticated users and anon role have no access.
CREATE POLICY discord_command_events_service_role_only
  ON discord_command_events
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
