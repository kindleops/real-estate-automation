-- Repair historical thread_keys across message_events, send_queue, and inbox_thread_state.
--
-- Canonical rule (matches application code in sms-engine.js and enrich-message-event-context.js):
--   outbound event: thread_key = normalize_phone(to_phone_number)
--   inbound event:  thread_key = normalize_phone(from_phone_number)
--   send_queue row: thread_key = normalize_phone(to_phone_number)
--
-- SAFETY DESIGN:
--   - Backup/audit tables are created before any mutation (snapshot of rows that will change).
--   - All WHERE guards are idempotent: re-running only affects rows that still need repair.
--   - Canonical new_thread_key is computed and confirmed non-null before any update.
--   - Stale rows in inbox_thread_state are deleted only when a canonical row already exists.
--
-- REVIEW BEFORE APPLYING. Safe to run multiple times (idempotent WHERE guards).

-- ── 0. Phone normalization helper ──────────────────────────────────────────────────────────────
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

-- ── 1. Backup: message_events rows that will change ────────────────────────────────────────────
-- Snapshot captured before any UPDATE so the original values are preserved for audit/rollback.
CREATE TABLE IF NOT EXISTS public.message_events_thread_key_repair_backup_20260519 (
  id                uuid        NOT NULL,
  old_thread_key    text,
  new_thread_key    text,
  direction         text,
  from_phone_number text,
  to_phone_number   text,
  created_at        timestamptz,
  captured_at       timestamptz NOT NULL DEFAULT now(),
  pattern           text,
  PRIMARY KEY (id)
);

-- Insert outbound rows that will change (direction='outbound', thread_key != canonical).
INSERT INTO public.message_events_thread_key_repair_backup_20260519
  (id, old_thread_key, new_thread_key, direction, from_phone_number, to_phone_number, created_at, pattern)
SELECT
  id,
  thread_key                         AS old_thread_key,
  normalize_e164(to_phone_number)    AS new_thread_key,
  direction,
  from_phone_number,
  to_phone_number,
  created_at,
  CASE
    WHEN thread_key LIKE '%|%'        THEN 'pipe_composite'
    WHEN thread_key LIKE 'phone:%'    THEN 'phone_prefix'
    WHEN thread_key IS NULL           THEN 'null'
    ELSE 'other'
  END                                AS pattern
FROM public.message_events
WHERE
  direction = 'outbound'
  AND to_phone_number IS NOT NULL
  AND normalize_e164(to_phone_number) IS NOT NULL
  AND thread_key IS DISTINCT FROM normalize_e164(to_phone_number)
ON CONFLICT (id) DO NOTHING;

-- Insert inbound rows that will change (direction='inbound', thread_key != canonical).
INSERT INTO public.message_events_thread_key_repair_backup_20260519
  (id, old_thread_key, new_thread_key, direction, from_phone_number, to_phone_number, created_at, pattern)
SELECT
  id,
  thread_key                          AS old_thread_key,
  normalize_e164(from_phone_number)   AS new_thread_key,
  direction,
  from_phone_number,
  to_phone_number,
  created_at,
  CASE
    WHEN thread_key LIKE '%|%'         THEN 'pipe_composite'
    WHEN thread_key LIKE 'phone:%'     THEN 'phone_prefix'
    WHEN thread_key IS NULL            THEN 'null'
    ELSE 'other'
  END                                 AS pattern
FROM public.message_events
WHERE
  direction = 'inbound'
  AND from_phone_number IS NOT NULL
  AND normalize_e164(from_phone_number) IS NOT NULL
  AND thread_key IS DISTINCT FROM normalize_e164(from_phone_number)
ON CONFLICT (id) DO NOTHING;

-- ── 2. Repair message_events.thread_key ────────────────────────────────────────────────────────
-- Outbound: canonical = normalize_e164(to_phone_number)
UPDATE public.message_events
SET
  thread_key = normalize_e164(to_phone_number),
  updated_at = now()
WHERE
  direction = 'outbound'
  AND to_phone_number IS NOT NULL
  AND normalize_e164(to_phone_number) IS NOT NULL
  AND thread_key IS DISTINCT FROM normalize_e164(to_phone_number);

-- Inbound: canonical = normalize_e164(from_phone_number)
UPDATE public.message_events
SET
  thread_key = normalize_e164(from_phone_number),
  updated_at = now()
WHERE
  direction = 'inbound'
  AND from_phone_number IS NOT NULL
  AND normalize_e164(from_phone_number) IS NOT NULL
  AND thread_key IS DISTINCT FROM normalize_e164(from_phone_number);

-- ── 3. Backup: send_queue rows that will change ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.send_queue_thread_key_repair_backup_20260519 (
  id                uuid        NOT NULL,
  old_thread_key    text,
  new_thread_key    text,
  queue_status      text,
  from_phone_number text,
  to_phone_number   text,
  created_at        timestamptz,
  captured_at       timestamptz NOT NULL DEFAULT now(),
  pattern           text,
  PRIMARY KEY (id)
);

INSERT INTO public.send_queue_thread_key_repair_backup_20260519
  (id, old_thread_key, new_thread_key, queue_status, from_phone_number, to_phone_number, created_at, pattern)
SELECT
  id,
  thread_key                          AS old_thread_key,
  normalize_e164(to_phone_number)     AS new_thread_key,
  queue_status,
  from_phone_number,
  to_phone_number,
  created_at,
  CASE
    WHEN thread_key LIKE '%|%'         THEN 'pipe_composite'
    WHEN thread_key LIKE 'phone:%'     THEN 'phone_prefix'
    WHEN thread_key IS NULL            THEN 'null'
    ELSE 'other'
  END                                 AS pattern
FROM public.send_queue
WHERE
  to_phone_number IS NOT NULL
  AND normalize_e164(to_phone_number) IS NOT NULL
  AND thread_key IS DISTINCT FROM normalize_e164(to_phone_number)
ON CONFLICT (id) DO NOTHING;

-- ── 4. Repair send_queue.thread_key ───────────────────────────────────────────────────────────
-- Canonical for queue rows = normalize_e164(to_phone_number)
UPDATE public.send_queue
SET
  thread_key = normalize_e164(to_phone_number),
  updated_at = now()
WHERE
  to_phone_number IS NOT NULL
  AND normalize_e164(to_phone_number) IS NOT NULL
  AND thread_key IS DISTINCT FROM normalize_e164(to_phone_number);

-- ── 5. Backup: inbox_thread_state rows that will change ───────────────────────────────────────
-- inbox_thread_state is UNIQUE on thread_key. Capture both rows-to-delete and rows-to-update.
CREATE TABLE IF NOT EXISTS public.inbox_thread_state_thread_key_repair_backup_20260519 (
  id              bigint      NOT NULL GENERATED ALWAYS AS IDENTITY,
  thread_key      text,
  new_thread_key  text,
  master_owner_id text,
  property_id     text,
  is_read         boolean,
  is_archived     boolean,
  updated_at      timestamptz,
  captured_at     timestamptz NOT NULL DEFAULT now(),
  action          text,        -- 'delete' or 'update'
  pattern         text,
  PRIMARY KEY (id)
);

-- Capture rows that will be DELETED (canonical row already exists → stale loses).
INSERT INTO public.inbox_thread_state_thread_key_repair_backup_20260519
  (thread_key, new_thread_key, master_owner_id, property_id, is_read, is_archived, updated_at, action, pattern)
SELECT
  stale.thread_key,
  normalize_e164(stale.thread_key)   AS new_thread_key,
  stale.master_owner_id,
  stale.property_id,
  stale.is_read,
  stale.is_archived,
  stale.updated_at,
  'delete'                           AS action,
  CASE
    WHEN stale.thread_key LIKE '%|%'  THEN 'pipe_composite'
    WHEN stale.thread_key LIKE 'phone:%' THEN 'phone_prefix'
    ELSE 'other'
  END                                AS pattern
FROM public.inbox_thread_state stale
WHERE
  normalize_e164(stale.thread_key) IS NOT NULL
  AND stale.thread_key != normalize_e164(stale.thread_key)
  AND EXISTS (
    SELECT 1
    FROM public.inbox_thread_state canonical
    WHERE canonical.thread_key = normalize_e164(stale.thread_key)
  );

-- Capture rows that will be UPDATED in-place (no canonical row exists yet).
INSERT INTO public.inbox_thread_state_thread_key_repair_backup_20260519
  (thread_key, new_thread_key, master_owner_id, property_id, is_read, is_archived, updated_at, action, pattern)
SELECT
  thread_key,
  normalize_e164(thread_key)         AS new_thread_key,
  master_owner_id,
  property_id,
  is_read,
  is_archived,
  updated_at,
  'update'                           AS action,
  CASE
    WHEN thread_key LIKE '%|%'        THEN 'pipe_composite'
    WHEN thread_key LIKE 'phone:%'    THEN 'phone_prefix'
    ELSE 'other'
  END                                AS pattern
FROM public.inbox_thread_state
WHERE
  normalize_e164(thread_key) IS NOT NULL
  AND thread_key != normalize_e164(thread_key)
  AND NOT EXISTS (
    SELECT 1
    FROM public.inbox_thread_state canonical
    WHERE canonical.thread_key = normalize_e164(inbox_thread_state.thread_key)
  );

-- ── 6. Repair inbox_thread_state.thread_key ───────────────────────────────────────────────────
-- inbox_thread_state is UNIQUE on thread_key. Stale rows using non-canonical keys must be
-- merged into the canonical row (or inserted if the canonical row doesn't exist yet).
--
-- Strategy:
--   a) Delete stale rows that would conflict with an existing canonical row.
--      (The canonical row wins; its is_read/is_archived state is preserved.)
--   b) Update the key in-place for stale rows whose canonical row does not yet exist.

-- Step 6a: Delete stale rows that would conflict with an existing canonical row.
DELETE FROM public.inbox_thread_state stale
WHERE
  normalize_e164(stale.thread_key) IS NOT NULL
  AND stale.thread_key != normalize_e164(stale.thread_key)
  AND EXISTS (
    SELECT 1
    FROM public.inbox_thread_state canonical
    WHERE canonical.thread_key = normalize_e164(stale.thread_key)
  );

-- Step 6b: Update stale rows whose canonical row does not yet exist.
UPDATE public.inbox_thread_state
SET
  thread_key = normalize_e164(thread_key),
  updated_at = now()
WHERE
  normalize_e164(thread_key) IS NOT NULL
  AND thread_key != normalize_e164(thread_key);

-- ── 7. Cleanup helper ─────────────────────────────────────────────────────────────────────────
-- Drop the helper; application code normalizes phones in JS, not SQL.
DROP FUNCTION IF EXISTS normalize_e164(text);
