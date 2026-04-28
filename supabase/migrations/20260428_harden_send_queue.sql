-- Migration: Harden send_queue table
-- Date: 2026-04-28
-- Purpose: Add guard columns, dedupe protection, lock columns, and indexes.

-- ---------------------------------------------------------------------------
-- 1. Add columns (all use IF NOT EXISTS equivalents via DO block)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  -- Dedupe key — canonical hash for duplicate detection.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='dedupe_key') THEN
    ALTER TABLE public.send_queue ADD COLUMN dedupe_key text;
  END IF;

  -- Seller name columns for greetings guard.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='seller_first_name') THEN
    ALTER TABLE public.send_queue ADD COLUMN seller_first_name text;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='seller_display_name') THEN
    ALTER TABLE public.send_queue ADD COLUMN seller_display_name text;
  END IF;

  -- Timezone column for local send-window enforcement.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='timezone') THEN
    ALTER TABLE public.send_queue ADD COLUMN timezone text;
  END IF;

  -- Guard columns — set by pre-send gate.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='local_send_allowed') THEN
    ALTER TABLE public.send_queue ADD COLUMN local_send_allowed boolean;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='guard_status') THEN
    ALTER TABLE public.send_queue ADD COLUMN guard_status text;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='guard_reason') THEN
    ALTER TABLE public.send_queue ADD COLUMN guard_reason text;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='last_guard_checked_at') THEN
    ALTER TABLE public.send_queue ADD COLUMN last_guard_checked_at timestamptz;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='paused_reason') THEN
    ALTER TABLE public.send_queue ADD COLUMN paused_reason text;
  END IF;

  -- Lock columns (atomic row-level locking before send).
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='locked_at') THEN
    ALTER TABLE public.send_queue ADD COLUMN locked_at timestamptz;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='lock_token') THEN
    ALTER TABLE public.send_queue ADD COLUMN lock_token text;
  END IF;

  -- Provider tracking.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='provider_message_id') THEN
    ALTER TABLE public.send_queue ADD COLUMN provider_message_id text;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='sent_at') THEN
    ALTER TABLE public.send_queue ADD COLUMN sent_at timestamptz;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 2. Unique partial indexes for duplicate prevention
-- ---------------------------------------------------------------------------

-- Active dedupe_key uniqueness: same key cannot be active twice.
CREATE UNIQUE INDEX IF NOT EXISTS uq_send_queue_active_dedupe_key
  ON public.send_queue (dedupe_key)
  WHERE sent_at IS NULL
    AND queue_status IN ('queued','ready','runnable','scheduled','pending','paused','paused_after_hours')
    AND dedupe_key IS NOT NULL;

-- Provider message ID uniqueness: same provider message cannot log twice.
CREATE UNIQUE INDEX IF NOT EXISTS uq_send_queue_provider_message_id
  ON public.send_queue (provider_message_id)
  WHERE provider_message_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. Performance indexes for the queue runner
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_send_queue_queue_status
  ON public.send_queue (queue_status);

CREATE INDEX IF NOT EXISTS idx_send_queue_scheduled_for_utc
  ON public.send_queue (scheduled_for_utc)
  WHERE sent_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_timezone
  ON public.send_queue (timezone)
  WHERE sent_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_master_owner_id
  ON public.send_queue (master_owner_id)
  WHERE sent_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_property_id
  ON public.send_queue (property_id)
  WHERE sent_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_to_phone_number
  ON public.send_queue (to_phone_number)
  WHERE sent_at IS NULL;
