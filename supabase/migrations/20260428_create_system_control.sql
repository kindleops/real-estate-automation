-- Migration: Create system_control table
-- Date: 2026-04-28
-- Purpose: Runtime feature flags that can be toggled without a deploy.
--          All critical send paths check these flags before executing.

CREATE TABLE IF NOT EXISTS public.system_control (
  key        text        PRIMARY KEY,
  enabled    boolean     NOT NULL DEFAULT true,
  reason     text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by text
);

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
INSERT INTO public.system_control (key, enabled, reason) VALUES
  ('outbound_sms_enabled',     true,  'Global SMS send gate — false halts all outbound sends'),
  ('feeder_enabled',           true,  'Feeder queue generation gate'),
  ('queue_runner_enabled',     true,  'Send queue runner gate'),
  ('retry_enabled',            true,  'Retry runner gate'),
  ('reconcile_enabled',        true,  'Queue reconcile runner gate'),
  ('podio_sync_enabled',       true,  'Podio message-event sync gate'),
  ('discord_alerts_enabled',   true,  'Discord alert delivery gate'),
  ('discord_actions_enabled',  true,  'Discord slash-command action gate'),
  ('dashboard_live_enabled',   true,  'Use live Supabase data on dashboard — false disables mock fallback'),
  ('email_enabled',            false, 'Email send gate — kept false until provider config is confirmed')
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
  SELECT key, enabled, reason, updated_at, updated_by
  FROM public.system_control
  ORDER BY key;
