-- Verification: Name guard health checks.
-- Run with: psql "$DATABASE_URL" -f scripts/verify-name-guards.sql
-- All "must_be_zero" rows must return 0.

-- 1. Sendable rows with blank greeting (must be 0).
SELECT
  'sendable_blank_name_rows' AS check_name,
  count(*)                   AS must_be_zero
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
    OR message_body LIKE 'Ola ,%'
    OR message_body LIKE 'Marhaba ,%'
  );

-- 2. Sendable rows with missing seller_first_name (must be 0).
SELECT
  'sendable_missing_seller_first_name' AS check_name,
  count(*)                             AS must_be_zero
FROM public.send_queue
WHERE
  sent_at IS NULL
  AND coalesce(provider_message_id, '') = ''
  AND queue_status IN ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  AND (seller_first_name IS NULL OR trim(seller_first_name) = '');

-- 3. Sent messages today with blank greeting (must be 0).
SELECT
  'sent_today_blank_greeting' AS check_name,
  count(*)                    AS must_be_zero
FROM public.send_queue
WHERE
  sent_at >= date_trunc('day', now())
  AND queue_status = 'sent'
  AND (
    message_body ~* '^(hi|hey|hello|hola|ola|marhaba)\s*,'
    OR message_body LIKE 'Hello ,%'
    OR message_body LIKE 'Hey ,%'
    OR message_body LIKE 'Hi ,%'
    OR message_body LIKE 'Hola ,%'
  );

-- 4. paused_name_missing count (informational).
SELECT
  'paused_name_missing_total' AS check_name,
  count(*)                    AS count,
  max(updated_at)             AS latest_paused_at
FROM public.send_queue
WHERE queue_status = 'paused_name_missing';

-- 5. paused_name_missing breakdown by guard_reason.
SELECT
  coalesce(guard_reason, paused_reason, 'unknown') AS reason,
  count(*)                                          AS count
FROM public.send_queue
WHERE queue_status = 'paused_name_missing'
GROUP BY 1
ORDER BY 2 DESC;

-- 6. Most recently paused rows (last 20).
SELECT
  id,
  master_owner_id,
  property_id,
  seller_first_name,
  queue_status,
  guard_reason,
  paused_reason,
  updated_at,
  left(message_body, 80) AS body_preview
FROM public.send_queue
WHERE queue_status = 'paused_name_missing'
ORDER BY updated_at DESC
LIMIT 20;
