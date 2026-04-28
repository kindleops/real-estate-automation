-- Migration: Create system_control table
-- Date: 2026-04-28
-- Purpose: Runtime feature flags that can be toggled without a deploy.
--          All critical send paths check these flags before executing.

CREATE TABLE IF NOT EXISTS public.system_control (
  key        text        PRIMARY KEY,
  value      text        NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Align any pre-existing variant schema to key/value/updated_at.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'value'
  ) THEN
    ALTER TABLE public.system_control ADD COLUMN value text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'enabled'
  ) THEN
    ALTER TABLE public.system_control DROP COLUMN enabled;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'reason'
  ) THEN
    ALTER TABLE public.system_control DROP COLUMN reason;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'system_control' AND column_name = 'updated_by'
  ) THEN
    ALTER TABLE public.system_control DROP COLUMN updated_by;
  END IF;

  UPDATE public.system_control SET value = 'false' WHERE value IS NULL;
  ALTER TABLE public.system_control ALTER COLUMN value SET NOT NULL;
END $$;

-- Ensure updated_at stays current.
CREATE OR REPLACE FUNCTION public.set_system_control_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_system_control_updated_at ON public.system_control;
CREATE TRIGGER trg_system_control_updated_at
  BEFORE UPDATE ON public.system_control
  FOR EACH ROW EXECUTE FUNCTION public.set_system_control_updated_at();

-- Seed default flags.
INSERT INTO public.system_control (key, value) VALUES
  ('outbound_sms_enabled',     'true'),
  ('feeder_enabled',           'true'),
  ('queue_runner_enabled',     'true'),
  ('retry_enabled',            'true'),
  ('reconcile_enabled',        'true'),
  ('podio_sync_enabled',       'true'),
  ('discord_alerts_enabled',   'true'),
  ('discord_actions_enabled',  'true'),
  ('dashboard_live_enabled',   'true'),
  ('email_enabled',            'false')
ON CONFLICT (key) DO NOTHING;

-- RLS: service role can manage; authenticated browser can read all.
ALTER TABLE public.system_control ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS system_control_service_all   ON public.system_control;
DROP POLICY IF EXISTS system_control_authed_read   ON public.system_control;
DROP POLICY IF EXISTS system_control_anon_read     ON public.system_control;

CREATE POLICY system_control_service_all ON public.system_control
  FOR ALL
  TO service_role
  USING (true) WITH CHECK (true);

CREATE POLICY system_control_authed_read ON public.system_control
  FOR SELECT
  TO authenticated
  USING (true);

-- Public (anon) can read flags so the Next.js browser client can gate UI.
CREATE POLICY system_control_anon_read ON public.system_control
  FOR SELECT
  TO anon
  USING (true);

-- Handy helper view that orders by key.
CREATE OR REPLACE VIEW public.v_system_control AS
  SELECT key, value, updated_at
  FROM public.system_control
  ORDER BY key;
