-- Production health checks for queue + messaging pipeline.
-- Run with: psql "$DATABASE_URL" -f scripts/check-production-health.sql

-- 1) System control flags (stored as key/value text).
select
  key,
  value,
  updated_at
from public.system_control
order by key;

-- 2) Runnable send_queue count (must be 0 when global lock is active).
select
  count(*) as runnable_now
from public.send_queue
where coalesce(sent_at, null) is null
  and queue_status in ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  and coalesce(provider_message_id, '') = '';

-- 3) Rows missing seller first name (should be zero for runnable rows).
select
  count(*) as blank_seller_first_name_count
from public.send_queue
where queue_status in ('queued', 'pending', 'retry_pending')
  and nullif(trim(coalesce(seller_first_name, '')), '') is null;

-- 4) Runnable rows outside local send window (should be zero).
select
  count(*) as after_hours_runnable_count
from public.send_queue
where queue_status in ('queued', 'pending', 'retry_pending')
  and coalesce(local_send_allowed, true) = false;

-- 5) Duplicate active dedupe keys (should be zero).
select
  dedupe_key,
  count(*) as dup_count
from public.send_queue
where dedupe_key is not null
  and sent_at is null
  and queue_status in ('queued', 'pending', 'retry_pending', 'processing')
group by dedupe_key
having count(*) > 1
order by dup_count desc, dedupe_key;

-- 6) SMS sent today (UTC day).
select
  count(*) as sms_sent_today
from public.message_events
where created_at >= date_trunc('day', now())
  and lower(coalesce(direction, '')) = 'outbound'
  and lower(coalesce(channel, 'sms')) = 'sms';

-- 7) SMS inbound today (UTC day).
select
  count(*) as inbound_sms_today
from public.message_events
where created_at >= date_trunc('day', now())
  and lower(coalesce(direction, '')) = 'inbound'
  and lower(coalesce(channel, 'sms')) = 'sms';

-- 8) Opt-outs today (UTC day).
select
  count(*) as opt_outs_today
from public.message_events
where created_at >= date_trunc('day', now())
  and (
    lower(coalesce(trigger_name, '')) like '%opt_out%'
    or lower(coalesce(message_body, '')) in ('stop', 'unsubscribe', 'cancel', 'quit')
  );
