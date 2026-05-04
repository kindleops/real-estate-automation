-- Migration: Add auto-reply and auto-queue config flags to system_control
-- Date: 2026-05-04
-- Purpose: Add environment flags for controlling auto-queue and auto-reply behavior.

-- Seed default flags for auto-queue and auto-reply functionality.
INSERT INTO public.system_control (key, value) VALUES
  ('auto_queue_enabled',     'true'),
  ('auto_reply_enabled',    'true'),
  ('auto_reply_live_enabled', 'true'),
  ('auto_reply_dry_run',    'false'),
  ('require_local_routing', 'true'),
  ('offer_ai_mode',         'dry_run')
ON CONFLICT (key) DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE public.system_control IS 'Runtime feature flags that can be toggled without a deploy. All critical send paths check these flags before executing.';
