-- Cleanup: Move unsent rows with blank greeting to paused_name_missing.
-- Run with: psql "$DATABASE_URL" -f scripts/cleanup-blank-greeting-rows.sql
--
-- A "blank greeting" row has a message_body that begins with a greeting word
-- followed immediately by a comma with no name between them:
--   "Hello ," / "Hey ," / "Hi ," / "Hola ," / "Ola ," / "Marhaba ,"
--
-- These rows must never be sent. This script pauses them before any send attempt.

-- Step 1: Pause rows with blank greeting patterns.
UPDATE public.send_queue
SET
  queue_status  = 'paused_name_missing',
  guard_status  = 'blocked',
  guard_reason  = 'blank_greeting_cleanup',
  paused_reason = 'blank_greeting_cleanup',
  updated_at    = now()
WHERE
  sent_at IS NULL
  AND coalesce(provider_message_id, '') = ''
  AND queue_status IN (
    'queued', 'ready', 'runnable', 'scheduled', 'pending',
    'paused_global_lock', 'paused_after_hours', 'retry_pending'
  )
  AND (
    message_body ~* '^(hi|hey|hello|hola|ola|marhaba)\s*,'
    OR message_body LIKE 'Hello ,%'
    OR message_body LIKE 'Hey ,%'
    OR message_body LIKE 'Hi ,%'
    OR message_body LIKE 'Hola ,%'
    OR message_body LIKE 'Ola ,%'
    OR message_body LIKE 'Marhaba ,%'
  );

-- Step 2: Also pause rows where seller_first_name column is null/empty
--         and the row is in a sendable status.
UPDATE public.send_queue
SET
  queue_status  = 'paused_name_missing',
  guard_status  = 'blocked',
  guard_reason  = 'missing_seller_first_name_cleanup',
  paused_reason = 'missing_seller_first_name_cleanup',
  updated_at    = now()
WHERE
  sent_at IS NULL
  AND coalesce(provider_message_id, '') = ''
  AND queue_status IN (
    'queued', 'ready', 'runnable', 'scheduled', 'pending',
    'paused_global_lock', 'paused_after_hours', 'retry_pending'
  )
  AND (
    seller_first_name IS NULL
    OR trim(seller_first_name) = ''
  )
  AND queue_status != 'paused_name_missing';

-- Step 3: Verification — both counts must be 0 after this script.
SELECT
  'blank_greeting_sendable'         AS check_name,
  count(*)                          AS must_be_zero
FROM public.send_queue
WHERE
  sent_at IS NULL
  AND coalesce(provider_message_id, '') = ''
  AND queue_status IN ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  AND (
    message_body ~* '^(hi|hey|hello|hola|ola|marhaba)\s*,'
    OR message_body LIKE 'Hello ,%'
    OR message_body LIKE 'Hey ,%'
    OR message_body LIKE 'Hi ,%'
    OR message_body LIKE 'Hola ,%'
  )

UNION ALL

SELECT
  'missing_seller_first_name_sendable' AS check_name,
  count(*)                             AS must_be_zero
FROM public.send_queue
WHERE
  sent_at IS NULL
  AND coalesce(provider_message_id, '') = ''
  AND queue_status IN ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  AND (seller_first_name IS NULL OR trim(seller_first_name) = '');
