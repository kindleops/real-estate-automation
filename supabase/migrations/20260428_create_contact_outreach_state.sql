-- Migration: Create contact_outreach_state
-- Date: 2026-04-28
-- Purpose: Track last outreach per owner/property/channel so email and SMS
--          cannot overlap within 24 hours for the same seller.

CREATE TABLE IF NOT EXISTS public.contact_outreach_state (
  id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  master_owner_id          text        NOT NULL,
  property_id              text,
  channel                  text        NOT NULL CHECK (channel IN ('sms','email','discord_manual_sms')),
  last_outreach_at         timestamptz NOT NULL DEFAULT now(),
  last_queue_id            uuid,
  last_provider_message_id text,
  created_at               timestamptz NOT NULL DEFAULT now(),
  updated_at               timestamptz NOT NULL DEFAULT now()
);

-- Unique on the combination that determines "is this seller being reached via this channel?".
-- property_id can be NULL so we use a partial index per case.
CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_outreach_state_owner_property_channel
  ON public.contact_outreach_state (master_owner_id, property_id, channel)
  WHERE property_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_outreach_state_owner_null_property_channel
  ON public.contact_outreach_state (master_owner_id, channel)
  WHERE property_id IS NULL;

-- Lookup index for overlap checks.
CREATE INDEX IF NOT EXISTS idx_contact_outreach_state_lookup
  ON public.contact_outreach_state (master_owner_id, property_id, last_outreach_at DESC);

-- Auto-update updated_at.
CREATE OR REPLACE FUNCTION public.set_contact_outreach_state_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_contact_outreach_updated_at ON public.contact_outreach_state;
CREATE TRIGGER trg_contact_outreach_updated_at
  BEFORE UPDATE ON public.contact_outreach_state
  FOR EACH ROW EXECUTE FUNCTION public.set_contact_outreach_state_updated_at();

-- RLS: service role owns writes; authenticated can read.
ALTER TABLE public.contact_outreach_state ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS contact_outreach_state_service   ON public.contact_outreach_state;
DROP POLICY IF EXISTS contact_outreach_state_authed    ON public.contact_outreach_state;

CREATE POLICY contact_outreach_state_service ON public.contact_outreach_state
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY contact_outreach_state_authed ON public.contact_outreach_state
  FOR SELECT TO authenticated USING (true);
