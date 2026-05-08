-- ===================================================================
-- Inbox Hydration Backfill SQL
-- Run in Supabase SQL Editor (production-safe, idempotent)
-- ===================================================================
-- Step 1: Count missing before backfill
-- ===================================================================
SELECT 'PRODUCTION BACKFILL - DRY RUN COUNTS' as phase;

SELECT 'send_queue.market missing' as metric, count(*) FROM public.send_queue WHERE market IS NULL
UNION ALL
SELECT 'message_events.market missing', count(*) FROM public.message_events WHERE market IS NULL
UNION ALL
SELECT 'send_queue.thread_key missing', count(*) FROM public.send_queue WHERE thread_key IS NULL
UNION ALL
SELECT 'message_events.thread_key missing', count(*) FROM public.message_events WHERE thread_key IS NULL
UNION ALL
SELECT 'send_queue.master_owner_id missing', count(*) FROM public.send_queue WHERE master_owner_id IS NULL
UNION ALL
SELECT 'message_events.master_owner_id missing', count(*) FROM public.message_events WHERE master_owner_id IS NULL
UNION ALL
SELECT 'send_queue.property_id missing', count(*) FROM public.send_queue WHERE property_id IS NULL
UNION ALL
SELECT 'message_events.property_id missing', count(*) FROM public.message_events WHERE property_id IS NULL
UNION ALL
SELECT 'send_queue.detected_intent missing', count(*) FROM public.send_queue WHERE detected_intent IS NULL
UNION ALL
SELECT 'message_events.detected_intent missing', count(*) FROM public.message_events WHERE detected_intent IS NULL;

-- ===================================================================
-- Step 2: Backfill market from properties
-- ===================================================================
SELECT 'BACKFILL START' as phase;

UPDATE public.send_queue sq
SET market = p.market
FROM public.properties p
WHERE sq.property_id IS NOT NULL
  AND sq.property_id::text = p.property_id::text
  AND sq.market IS NULL
  AND p.market IS NOT NULL;

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'send_queue.market backfilled: ' || v_count as result;

UPDATE public.message_events me
SET market = p.market
FROM public.properties p
WHERE me.property_id IS NOT NULL
  AND me.property_id::text = p.property_id::text
  AND me.market IS NULL
  AND p.market IS NOT NULL;

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'message_events.market backfilled (from props): ' || v_count as result;

UPDATE public.message_events me
SET market = sq.market
FROM public.send_queue sq
WHERE me.queue_id IS NOT NULL
  AND me.queue_id::text = sq.id::text
  AND me.market IS NULL
  AND sq.market IS NOT NULL;

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'message_events.market backfilled (from send_queue): ' || v_count as result;

-- ===================================================================
-- Step 3: Backfill thread_key from phone numbers
-- ===================================================================
UPDATE public.send_queue
SET thread_key = 'phone:' || public.normalize_phone(to_phone_number)
WHERE thread_key IS NULL
  AND to_phone_number IS NOT NULL;

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'send_queue.thread_key backfilled: ' || v_count as result;

UPDATE public.message_events
SET thread_key = 'phone:' || public.normalize_phone(from_phone_number)
WHERE thread_key IS NULL
  AND from_phone_number IS NOT NULL;

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'message_events.thread_key backfilled: ' || v_count as result;

-- ===================================================================
-- Step 4: Backfill master_owner_id/property_id from send_queue
-- ===================================================================
UPDATE public.message_events me
SET
  master_owner_id = COALESCE(me.master_owner_id, sq.master_owner_id),
  property_id = COALESCE(me.property_id, sq.property_id)
FROM public.send_queue sq
WHERE me.queue_id IS NOT NULL
  AND me.queue_id::text = sq.id::text
  AND (me.master_owner_id IS NULL OR me.property_id IS NULL)
  AND (sq.master_owner_id IS NOT NULL OR sq.property_id IS NOT NULL);

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'message_events mo/pid backfilled (from send_queue): ' || v_count as result;

-- ===================================================================
-- Step 5: Backfill detected_intent from metadata
-- ===================================================================
UPDATE public.send_queue
SET detected_intent = COALESCE(
  metadata->>'detected_intent',
  metadata->>'intent'
)
WHERE detected_intent IS NULL
  AND (metadata->>'detected_intent' IS NOT NULL OR metadata->>'intent' IS NOT NULL);

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'send_queue.detected_intent backfilled: ' || v_count as result;

UPDATE public.message_events me
SET detected_intent = sq.detected_intent
FROM public.send_queue sq
WHERE me.queue_id IS NOT NULL
  AND me.queue_id::text = sq.id::text
  AND me.detected_intent IS NULL
  AND sq.detected_intent IS NOT NULL;

GET DIAGNOSTICS v_count = ROW_COUNT;
SELECT 'message_events.detected_intent backfilled: ' || v_count as result;

-- ===================================================================
-- Step 6: Verify remaining gaps
-- ===================================================================
SELECT 'REMAINING GAPS AFTER BACKFILL' as phase;

SELECT 'send_queue.market still missing' as metric, count(*) FROM public.send_queue WHERE market IS NULL
UNION ALL
SELECT 'message_events.market still missing', count(*) FROM public.message_events WHERE market IS NULL
UNION ALL
SELECT 'send_queue.thread_key still missing', count(*) FROM public.send_queue WHERE thread_key IS NULL
UNION ALL
SELECT 'message_events.thread_key still missing', count(*) FROM public.message_events WHERE thread_key IS NULL
UNION ALL
SELECT 'send_queue.master_owner_id still missing', count(*) FROM public.send_queue WHERE master_owner_id IS NULL
UNION ALL
SELECT 'message_events.master_owner_id still missing', count(*) FROM public.message_events WHERE master_owner_id IS NULL
UNION ALL
SELECT 'send_queue.property_id still missing', count(*) FROM public.send_queue WHERE property_id IS NULL
UNION ALL
SELECT 'message_events.property_id still missing', count(*) FROM public.message_events WHERE property_id IS NULL
UNION ALL
SELECT 'send_queue.detected_intent still missing', count(*) FROM public.send_queue WHERE detected_intent IS NULL
UNION ALL
SELECT 'message_events.detected_intent still missing', count(*) FROM public.message_events WHERE detected_intent IS NULL;
