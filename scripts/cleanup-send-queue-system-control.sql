-- DB-side emergency cleanup for queue safety and global lock enforcement.
-- Run with: psql "$DATABASE_URL" -f scripts/cleanup-send-queue-system-control.sql

-- 1) Pause rows with missing seller name patterns.
update public.send_queue
set
  queue_status = 'paused_name_missing',
  updated_at = now()
where queue_status in ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  and sent_at is null
  and provider_message_id is null
  and (
    nullif(trim(coalesce(seller_first_name, '')), '') is null
    or coalesce(message_body, '') ~* '(hello|hi|hey|hola)\s*,'
  );

-- 2) Apply global outbound lock to sendable rows when outbound_sms_enabled is false.
with outbound_flag as (
  select
    exists (
      select 1
      from public.system_control sc
      where sc.key = 'outbound_sms_enabled'
        and lower(coalesce(sc.value, '')) in ('true', '1', 'yes', 'on', 'enabled')
    ) as sms_enabled
)
update public.send_queue sq
set
  queue_status = 'paused_global_lock',
  updated_at = now()
from outbound_flag f
where f.sms_enabled = false
  and sq.queue_status in ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  and sq.sent_at is null
  and sq.provider_message_id is null;

-- 3) Verification (hard requirement): runnable_now must be 0.
select
  count(*) as runnable_now
from public.send_queue
where queue_status in ('queued', 'ready', 'runnable', 'scheduled', 'pending')
  and sent_at is null
  and provider_message_id is null;
