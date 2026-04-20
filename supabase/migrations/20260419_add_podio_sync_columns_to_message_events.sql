-- Migration: add Podio sync tracking columns to message_events
-- Created: 2026-04-19
-- Purpose: allow the Supabase → Podio Message Events async sync layer to track
--          per-row sync state without blocking the SMS send path.

ALTER TABLE public.message_events
  ADD COLUMN IF NOT EXISTS podio_sync_status    text         DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS podio_message_event_id text,
  ADD COLUMN IF NOT EXISTS podio_synced_at      timestamptz,
  ADD COLUMN IF NOT EXISTS podio_sync_error     text,
  ADD COLUMN IF NOT EXISTS podio_sync_attempts  int          DEFAULT 0;

-- Partial index for the sync worker to efficiently find un-synced rows.
CREATE INDEX IF NOT EXISTS idx_message_events_podio_sync_status
  ON public.message_events (podio_sync_status, created_at)
  WHERE podio_sync_status IN ('pending', 'failed');

-- Comment on columns for schema clarity.
COMMENT ON COLUMN public.message_events.podio_sync_status IS
  'Tracks async copy to Podio Message Events. Values: pending | synced | failed | skipped';
COMMENT ON COLUMN public.message_events.podio_message_event_id IS
  'Podio item_id of the created Message Events record, set after successful sync.';
COMMENT ON COLUMN public.message_events.podio_synced_at IS
  'Timestamp when the row was successfully synced to Podio.';
COMMENT ON COLUMN public.message_events.podio_sync_error IS
  'Last error message from a failed Podio sync attempt.';
COMMENT ON COLUMN public.message_events.podio_sync_attempts IS
  'Number of Podio sync attempts (incremented on each failure).';
