-- Migration: add offer-sync tracking columns to send_queue
--
-- cash_offer_snapshot_id : FK (logical) to property_cash_offer_snapshots.id
--   Set at queue-creation time when a cash offer number is baked into the
--   outbound message.  NULL means no offer was included in the message.
--   Stage-1 / ownership_check rows must never have this set.
--
-- offer_podio_item_id    : Podio Offers app item_id, back-filled after
--   successful Podio record creation.
--
-- offer_record_sync_status : lifecycle flag
--   NULL         — no offer sync needed (no snapshot)
--   'pending'    — snapshot present but sync not attempted yet
--   'synced'     — Podio Offer record created/updated successfully
--   'failed'     — Podio Offer creation failed; Discord alert was sent;
--                  data is preserved here for manual recovery
--
-- offer_record_sync_error : error message captured on 'failed' status
-- offer_record_synced_at  : timestamp of last sync attempt (success or fail)

ALTER TABLE public.send_queue
  ADD COLUMN IF NOT EXISTS cash_offer_snapshot_id    uuid,
  ADD COLUMN IF NOT EXISTS offer_podio_item_id        bigint,
  ADD COLUMN IF NOT EXISTS offer_record_sync_status   text,
  ADD COLUMN IF NOT EXISTS offer_record_sync_error    text,
  ADD COLUMN IF NOT EXISTS offer_record_synced_at     timestamptz;

-- Fast lookup: find queue rows that need offer sync recovery
CREATE INDEX IF NOT EXISTS idx_send_queue_offer_sync_status
  ON public.send_queue (offer_record_sync_status, created_at DESC)
  WHERE offer_record_sync_status IN ('pending', 'failed');

-- Allow tracing which queue items used a given snapshot
CREATE INDEX IF NOT EXISTS idx_send_queue_cash_offer_snapshot_id
  ON public.send_queue (cash_offer_snapshot_id)
  WHERE cash_offer_snapshot_id IS NOT NULL;
