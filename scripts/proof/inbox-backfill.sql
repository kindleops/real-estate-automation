-- PROOF: INBOX BACKFILL & HYDRATION
-- TARGET THREAD: phone:+19102422956

-- 1. Verify Message Counts
SELECT 
    thread_key,
    message_count,
    inbound_count,
    outbound_count,
    failed_queue_count,
    pending_queue_count
FROM public.inbox_thread_state
WHERE thread_key = 'phone:+19102422956';

-- 2. Verify Hydration View (Identity)
SELECT 
    thread_key,
    owner_name,
    prospect_name,
    property_address_full,
    market
FROM public.inbox_threads_hydrated
WHERE thread_key = 'phone:+19102422956';

-- 3. Verify Hydration View (Property Details)
SELECT 
    thread_key,
    property_type,
    beds,
    baths,
    sqft,
    units,
    year_built,
    estimated_value,
    equity_amount
FROM public.inbox_threads_hydrated
WHERE thread_key = 'phone:+19102422956';

-- 4. Verify Latest Message Info
SELECT 
    thread_key,
    latest_message_body,
    latest_direction,
    latest_message_at,
    detected_intent
FROM public.inbox_threads_hydrated
WHERE thread_key = 'phone:+19102422956';

-- 5. Verify Dossier View completeness
SELECT count(*) as total_fields 
FROM information_schema.columns 
WHERE table_name = 'inbox_thread_dossier_hydrated';

-- 6. Verify Category Logic
SELECT 
    thread_key,
    inbox_category
FROM public.inbox_threads_hydrated
WHERE thread_key = 'phone:+19102422956';
