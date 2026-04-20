-- Migration: create Email Layer v1 tables for Brevo + Discord cockpit
-- Date: 2026-04-21

-- ---------------------------------------------------------------------------
-- Shared updated_at trigger helper
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- 1) email_templates
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_templates (
  id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  template_key text        UNIQUE NOT NULL,
  use_case     text        NOT NULL,
  stage_code   text,
  stage_label  text,
  language     text        DEFAULT 'English',
  subject      text        NOT NULL,
  html_body    text        NOT NULL,
  text_body    text,
  variables    jsonb       DEFAULT '[]'::jsonb,
  is_active    boolean     DEFAULT true,
  metadata     jsonb       DEFAULT '{}'::jsonb,
  created_at   timestamptz DEFAULT now(),
  updated_at   timestamptz DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 2) email_send_queue
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_send_queue (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_id         text        UNIQUE NOT NULL,
  owner_id         bigint,
  property_id      bigint,
  prospect_id      bigint,
  email_address    text        NOT NULL,
  template_key     text,
  use_case         text,
  stage_code       text,
  subject          text        NOT NULL,
  html_body        text        NOT NULL,
  text_body        text,
  status           text        DEFAULT 'queued',
  scheduled_for    timestamptz,
  sent_at          timestamptz,
  brevo_message_id text,
  failure_reason   text,
  campaign_key     text,
  metadata         jsonb       DEFAULT '{}'::jsonb,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 3) email_events
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_events (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  event_key        text        UNIQUE,
  brevo_message_id text,
  email_address    text,
  event_type       text        NOT NULL,
  subject          text,
  template_key     text,
  campaign_key     text,
  raw_payload      jsonb       DEFAULT '{}'::jsonb,
  created_at       timestamptz DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 4) email_suppression
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_suppression (
  id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  email_address text        UNIQUE NOT NULL,
  reason        text        NOT NULL,
  source        text,
  raw_payload   jsonb       DEFAULT '{}'::jsonb,
  created_at    timestamptz DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 5) email_identities
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_identities (
  id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  brand_key      text        UNIQUE NOT NULL,
  sender_name    text        NOT NULL,
  sender_email   text        NOT NULL,
  reply_to_email text,
  domain         text,
  is_active      boolean     DEFAULT true,
  metadata       jsonb       DEFAULT '{}'::jsonb,
  created_at     timestamptz DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS email_templates_use_case_idx
  ON email_templates (use_case);

CREATE INDEX IF NOT EXISTS email_templates_template_key_idx
  ON email_templates (template_key);

CREATE INDEX IF NOT EXISTS email_send_queue_status_idx
  ON email_send_queue (status);

CREATE INDEX IF NOT EXISTS email_send_queue_email_address_idx
  ON email_send_queue (email_address);

CREATE INDEX IF NOT EXISTS email_send_queue_use_case_idx
  ON email_send_queue (use_case);

CREATE INDEX IF NOT EXISTS email_send_queue_template_key_idx
  ON email_send_queue (template_key);

CREATE INDEX IF NOT EXISTS email_send_queue_campaign_key_idx
  ON email_send_queue (campaign_key);

CREATE INDEX IF NOT EXISTS email_events_event_type_idx
  ON email_events (event_type);

CREATE INDEX IF NOT EXISTS email_events_email_address_idx
  ON email_events (email_address);

CREATE INDEX IF NOT EXISTS email_events_template_key_idx
  ON email_events (template_key);

CREATE INDEX IF NOT EXISTS email_events_campaign_key_idx
  ON email_events (campaign_key);

CREATE INDEX IF NOT EXISTS email_suppression_email_address_idx
  ON email_suppression (email_address);

-- ---------------------------------------------------------------------------
-- updated_at triggers
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_email_templates_updated_at'
  ) THEN
    CREATE TRIGGER trg_email_templates_updated_at
      BEFORE UPDATE ON email_templates
      FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_email_send_queue_updated_at'
  ) THEN
    CREATE TRIGGER trg_email_send_queue_updated_at
      BEFORE UPDATE ON email_send_queue
      FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- RLS: service role only
-- ---------------------------------------------------------------------------

ALTER TABLE email_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE email_send_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE email_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE email_suppression ENABLE ROW LEVEL SECURITY;
ALTER TABLE email_identities ENABLE ROW LEVEL SECURITY;

CREATE POLICY email_templates_service_role_only
  ON email_templates
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

CREATE POLICY email_send_queue_service_role_only
  ON email_send_queue
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

CREATE POLICY email_events_service_role_only
  ON email_events
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

CREATE POLICY email_suppression_service_role_only
  ON email_suppression
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

CREATE POLICY email_identities_service_role_only
  ON email_identities
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
