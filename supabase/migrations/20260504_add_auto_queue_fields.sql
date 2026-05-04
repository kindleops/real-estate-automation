-- Migration: Add auto-queue and auto-reply fields to send_queue
-- Date: 2026-05-04
-- Purpose: Add missing columns for Supabase-based auto-queue and auto-reply engine.

-- ---------------------------------------------------------------------------
-- 1. Add missing columns to send_queue
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  -- thread_key: stable thread identifier for conversation threading
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='thread_key') THEN
    ALTER TABLE public.send_queue ADD COLUMN thread_key text;
  END IF;

  -- owner_id: references master_owner or property owner
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='owner_id') THEN
    ALTER TABLE public.send_queue ADD COLUMN owner_id bigint;
  END IF;

  -- agent_id: the SMS agent assigned to this message
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='agent_id') THEN
    ALTER TABLE public.send_queue ADD COLUMN agent_id bigint;
  END IF;

  -- template_source: where the template was resolved from (podio, catalog, default)
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='template_source') THEN
    ALTER TABLE public.send_queue ADD COLUMN template_source text;
  END IF;

  -- rendered_message: the personalized message ready to send
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='rendered_message') THEN
    ALTER TABLE public.send_queue ADD COLUMN rendered_message text;
  END IF;

  -- priority: message priority level
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='priority') THEN
    ALTER TABLE public.send_queue ADD COLUMN priority text DEFAULT 'normal';
  END IF;

  -- risk: risk level assessment
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='risk') THEN
    ALTER TABLE public.send_queue ADD COLUMN risk text DEFAULT 'low';
  END IF;

  -- sms_eligible: whether the contact is eligible for SMS
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='sms_eligible') THEN
    ALTER TABLE public.send_queue ADD COLUMN sms_eligible boolean DEFAULT true;
  END IF;

  -- routing_allowed: whether routing to a TextGrid number is allowed
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='routing_allowed') THEN
    ALTER TABLE public.send_queue ADD COLUMN routing_allowed boolean DEFAULT true;
  END IF;

  -- safety_status: safety gate result (safe, blocked, review)
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='safety_status') THEN
    ALTER TABLE public.send_queue ADD COLUMN safety_status text DEFAULT 'pending';
  END IF;

  -- type: message type (outbound, auto_reply, manual)
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='type') THEN
    ALTER TABLE public.send_queue ADD COLUMN type text DEFAULT 'outbound';
  END IF;

  -- source_event_id: the inbound message event that triggered an auto-reply
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='source_event_id') THEN
    ALTER TABLE public.send_queue ADD COLUMN source_event_id uuid;
  END IF;

  -- inbound_message_id: the inbound message ID that triggered auto-reply
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='inbound_message_id') THEN
    ALTER TABLE public.send_queue ADD COLUMN inbound_message_id text;
  END IF;

  -- detected_intent: the classified intent of the inbound message
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='detected_intent') THEN
    ALTER TABLE public.send_queue ADD COLUMN detected_intent text;
  END IF;

  -- stage_before: conversation stage before processing inbound
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='stage_before') THEN
    ALTER TABLE public.send_queue ADD COLUMN stage_before text;
  END IF;

  -- stage_after: conversation stage after processing inbound
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='stage_after') THEN
    ALTER TABLE public.send_queue ADD COLUMN stage_after text;
  END IF;

  -- template_selected: the template ID or key selected for reply
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='template_selected') THEN
    ALTER TABLE public.send_queue ADD COLUMN template_selected text;
  END IF;

  -- textgrid_message_id: the TextGrid message ID after send
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='textgrid_message_id') THEN
    ALTER TABLE public.send_queue ADD COLUMN textgrid_message_id text;
  END IF;

  -- textgrid_number: the TextGrid number used to send
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='textgrid_number') THEN
    ALTER TABLE public.send_queue ADD COLUMN textgrid_number text;
  END IF;

  -- market: the market/region for this message
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='send_queue' AND column_name='market') THEN
    ALTER TABLE public.send_queue ADD COLUMN market text;
  END IF;

END $$;

-- ---------------------------------------------------------------------------
-- 2. Add new status values support
-- ---------------------------------------------------------------------------

-- Add CHECK constraint for valid queue_status values if not exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conname = 'send_queue_status_check' 
    AND conrelid = 'public.send_queue'::regclass
  ) THEN
    ALTER TABLE public.send_queue 
    ADD CONSTRAINT send_queue_status_check 
    CHECK (queue_status IN ('ready', 'scheduled', 'sent', 'delivered', 'failed', 'held', 'suppressed', 'blocked', 'pending_approval', 'queued', 'paused', 'paused_after_hours', 'paused_name_missing', 'paused_invalid_queue_row', 'paused_duplicate', 'paused_global_lock', 'paused_max_retries', 'cancelled', 'sending'));
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 3. Add message_events event_type constraint if not exists
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conname = 'message_events_event_type_check' 
    AND conrelid = 'public.message_events'::regclass
  ) THEN
    ALTER TABLE public.message_events 
    ADD CONSTRAINT message_events_event_type_check 
    CHECK (event_type IN ('inbound_received', 'outbound_queued', 'outbound_sent', 'delivered', 'failed', 'auto_reply_queued', 'auto_reply_blocked', 'suppression_applied'));
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 4. Create indexes for new columns
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_send_queue_thread_key
  ON public.send_queue (thread_key)
  WHERE thread_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_type
  ON public.send_queue (type)
  WHERE type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_safety_status
  ON public.send_queue (safety_status)
  WHERE safety_status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_source_event_id
  ON public.send_queue (source_event_id)
  WHERE source_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_send_queue_owner_id
  ON public.send_queue (owner_id)
  WHERE owner_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 5. Update existing rows to populate textgrid_message_id from provider_message_id
-- ---------------------------------------------------------------------------

UPDATE public.send_queue 
SET textgrid_message_id = provider_message_id 
WHERE textgrid_message_id IS NULL 
  AND provider_message_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 6. Add comments for documentation
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN public.send_queue.thread_key IS 'Stable thread identifier for conversation threading (owner:phone:property)';
COMMENT ON COLUMN public.send_queue.owner_id IS 'References master_owner or property owner ID';
COMMENT ON COLUMN public.send_queue.agent_id IS 'The SMS agent assigned to this message';
COMMENT ON COLUMN public.send_queue.template_source IS 'Where the template was resolved from (podio, catalog, default)';
COMMENT ON COLUMN public.send_queue.rendered_message IS 'The personalized message ready to send';
COMMENT ON COLUMN public.send_queue.priority IS 'Message priority level (high, normal, low)';
COMMENT ON COLUMN public.send_queue.risk IS 'Risk level assessment (low, medium, high)';
COMMENT ON COLUMN public.send_queue.sms_eligible IS 'Whether the contact is eligible for SMS';
COMMENT ON COLUMN public.send_queue.routing_allowed IS 'Whether routing to a TextGrid number is allowed';
COMMENT ON COLUMN public.send_queue.safety_status IS 'Safety gate result (safe, blocked, review, pending)';
COMMENT ON COLUMN public.send_queue.type IS 'Message type (outbound, auto_reply, manual)';
COMMENT ON COLUMN public.send_queue.source_event_id IS 'The inbound message event that triggered an auto-reply';
COMMENT ON COLUMN public.send_queue.inbound_message_id IS 'The inbound message ID that triggered auto-reply';
COMMENT ON COLUMN public.send_queue.detected_intent IS 'The classified intent of the inbound message';
COMMENT ON COLUMN public.send_queue.stage_before IS 'Conversation stage before processing inbound';
COMMENT ON COLUMN public.send_queue.stage_after IS 'Conversation stage after processing inbound';
COMMENT ON COLUMN public.send_queue.template_selected IS 'The template ID or key selected for reply';
COMMENT ON COLUMN public.send_queue.textgrid_message_id IS 'The TextGrid message ID after send';
COMMENT ON COLUMN public.send_queue.textgrid_number IS 'The TextGrid number used to send';
COMMENT ON COLUMN public.send_queue.market IS 'The market/region for this message';
