-- Migration: Repair system_control to key/value/updated_at schema
-- Date: 2026-04-28
-- Purpose: Idempotent repair migration that guarantees the production schema
--          (key text PK, value text NOT NULL, updated_at timestamptz) and seeds
--          all expected flags using key/value only.
--          Safe to run against a DB that has an old schema with enabled/reason/updated_by
--          OR against a fresh DB with no table yet.

-- 1. Create table if it does not exist (matches production schema exactly).
CREATE TABLE IF NOT EXISTS public.system_control (
  key        text        PRIMARY KEY,
  value      text        NOT NULL DEFAULT 'false',
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- 2. Drop legacy columns that do not exist in production (if present from old local migration).
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'enabled'
  ) THEN
    ALTER TABLE public.system_control DROP COLUMN enabled;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'reason'
  ) THEN
    ALTER TABLE public.system_control DROP COLUMN reason;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'updated_by'
  ) THEN
    ALTER TABLE public.system_control DROP COLUMN updated_by;
  END IF;

  -- Ensure value column exists and is NOT NULL.
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'value'
  ) THEN
    ALTER TABLE public.system_control ADD COLUMN value text NOT NULL DEFAULT 'false';
  END IF;

  -- Backfill any null values before enforcing NOT NULL.
  UPDATE public.system_control SET value = 'false' WHERE value IS NULL;
  ALTER TABLE public.system_control ALTER COLUMN value SET NOT NULL;
END $$;

-- 3. Seed expected flags (key/value only — no enabled/reason/updated_by).
--    ON CONFLICT DO NOTHING preserves any manually-set production values.
INSERT INTO public.system_control (key, value) VALUES
  ('outbound_sms_enabled',    'false'),
  ('feeder_enabled',          'false'),
  ('queue_runner_enabled',    'false'),
  ('retry_enabled',           'false'),
  ('reconcile_enabled',       'true'),
  ('podio_sync_enabled',      'true'),
  ('discord_alerts_enabled',  'true'),
  ('discord_actions_enabled', 'false'),
  ('dashboard_live_enabled',  'true'),
  ('email_enabled',           'false'),
  ('verification_textgrid_send_enabled', 'false'),
  ('buyer_sms_blast_enabled', 'false')
ON CONFLICT (key) DO NOTHING;

-- 4. Recreate v_system_control view using key/value/updated_at only.
DROP VIEW IF EXISTS public.v_system_control;
CREATE VIEW public.v_system_control AS
  SELECT key, value, updated_at
  FROM public.system_control
  ORDER BY key;

-- 5. RLS: ensure policies exist (idempotent).
ALTER TABLE public.system_control ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS system_control_service_all  ON public.system_control;
DROP POLICY IF EXISTS system_control_authed_read  ON public.system_control;
DROP POLICY IF EXISTS system_control_anon_read    ON public.system_control;

CREATE POLICY system_control_service_all ON public.system_control
  FOR ALL TO service_role
  USING (true) WITH CHECK (true);

CREATE POLICY system_control_authed_read ON public.system_control
  FOR SELECT TO authenticated
  USING (true);

CREATE POLICY system_control_anon_read ON public.system_control
  FOR SELECT TO anon
  USING (true);
