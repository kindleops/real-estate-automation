-- Repair historical thread_keys across message_events, send_queue, and inbox_thread_state.
--
-- Canonical rule (matches application code in sms-engine.js and enrich-message-event-context.js):
--   outbound event: thread_key = normalize_phone(to_phone_number)
--   inbound event:  thread_key = normalize_phone(from_phone_number)
--   send_queue row: thread_key = normalize_phone(to_phone_number)
--
-- REVIEW BEFORE APPLYING. This migration is safe to run multiple times (idempotent WHERE guards).

-- ── 1. Phone normalization helper ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION normalize_e164(phone text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT
    CASE
      WHEN regexp_replace(phone, '\D', '', 'g') ~ '^\d{10}$'
        THEN '+1' || regexp_replace(phone, '\D', '', 'g')
      WHEN regexp_replace(phone, '\D', '', 'g') ~ '^1\d{10}$'
        THEN '+' || regexp_replace(phone, '\D', '', 'g')
      ELSE NULL
    END;
$$;

-- ── 2. Repair message_events.thread_key ───────────────────────────────────────────────────────
-- Outbound: canonical = normalize_e164(to_phone_number)
UPDATE public.message_events
SET
  thread_key = normalize_e164(to_phone_number),
  updated_at = now()
WHERE
  direction = 'outbound'
  AND to_phone_number IS NOT NULL
  AND normalize_e164(to_phone_number) IS NOT NULL
  AND (
    thread_key IS DISTINCT FROM normalize_e164(to_phone_number)
  );

-- Inbound: canonical = normalize_e164(from_phone_number)
UPDATE public.message_events
SET
  thread_key = normalize_e164(from_phone_number),
  updated_at = now()
WHERE
  direction = 'inbound'
  AND from_phone_number IS NOT NULL
  AND normalize_e164(from_phone_number) IS NOT NULL
  AND (
    thread_key IS DISTINCT FROM normalize_e164(from_phone_number)
  );

-- ── 3. Repair send_queue.thread_key ───────────────────────────────────────────────────────────
-- Canonical for queue rows = normalize_e164(to_phone_number)
UPDATE public.send_queue
SET
  thread_key = normalize_e164(to_phone_number),
  updated_at = now()
WHERE
  to_phone_number IS NOT NULL
  AND normalize_e164(to_phone_number) IS NOT NULL
  AND (
    thread_key IS DISTINCT FROM normalize_e164(to_phone_number)
  );

-- ── 4. Repair inbox_thread_state.thread_key ───────────────────────────────────────────────────
-- inbox_thread_state is UNIQUE on thread_key. Stale rows using non-canonical keys must be
-- merged into the canonical row (or inserted if the canonical row doesn't exist yet).
--
-- Strategy:
--   a) For each stale row whose canonical equivalent already exists: delete the stale row
--      (the canonical row wins; per-thread state like is_read/is_archived is preserved there).
--   b) For each stale row whose canonical equivalent does NOT exist: update the key in-place.
--
-- Step 4a: Delete stale rows that would conflict with an existing canonical row.
DELETE FROM public.inbox_thread_state stale
WHERE
  normalize_e164(stale.thread_key) IS NOT NULL
  AND stale.thread_key != normalize_e164(stale.thread_key)
  AND EXISTS (
    SELECT 1
    FROM public.inbox_thread_state canonical
    WHERE canonical.thread_key = normalize_e164(stale.thread_key)
  );

-- Step 4b: Update stale rows whose canonical row does not yet exist.
UPDATE public.inbox_thread_state
SET
  thread_key = normalize_e164(thread_key),
  updated_at = now()
WHERE
  normalize_e164(thread_key) IS NOT NULL
  AND thread_key != normalize_e164(thread_key);

-- ── 5. Cleanup helper ─────────────────────────────────────────────────────────────────────────
-- Drop the helper; application code normalizes phones in JS, not SQL.
DROP FUNCTION IF EXISTS normalize_e164(text);
