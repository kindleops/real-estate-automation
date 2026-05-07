
-- 1. NORMALIZE PHONE FUNCTION
CREATE OR REPLACE FUNCTION public.normalize_phone(phone text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  clean_phone text;
BEGIN
  IF phone IS NULL THEN RETURN NULL; END IF;
  -- Remove all non-numeric characters
  clean_phone := regexp_replace(phone, '[^0-9]', '', 'g');
  -- If it starts with 1 and is 11 digits, it's a US number, keep it (or add +)
  -- If it's 10 digits, add 1
  IF length(clean_phone) = 10 THEN
    clean_phone := '1' || clean_phone;
  END IF;
  IF length(clean_phone) >= 11 AND left(clean_phone, 1) = '1' THEN
    RETURN '+' || clean_phone;
  END IF;
  -- Fallback
  RETURN '+' || clean_phone;
END;
$$;

-- 2. THREAD KEY RESOLVER
CREATE OR REPLACE FUNCTION public.resolve_thread_key(
  p_existing_thread_key text DEFAULT NULL,
  p_property_id text DEFAULT NULL,
  p_master_owner_id text DEFAULT NULL,
  p_prospect_id text DEFAULT NULL,
  p_seller_phone text DEFAULT NULL,
  p_our_phone text DEFAULT NULL,
  p_provider_message_sid text DEFAULT NULL
) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_phone_a text;
  v_phone_b text;
  v_phones text;
BEGIN
  -- 1. existing thread_key
  IF p_existing_thread_key IS NOT NULL AND p_existing_thread_key <> '' THEN
    RETURN p_existing_thread_key;
  END IF;

  -- Normalize phones
  v_phone_a := public.normalize_phone(p_seller_phone);
  v_phone_b := public.normalize_phone(p_our_phone);
  
  -- Sort phones to ensure consistency
  IF v_phone_a IS NOT NULL AND v_phone_b IS NOT NULL THEN
    IF v_phone_a > v_phone_b THEN
      v_phones := v_phone_b || ':' || v_phone_a;
    ELSE
      v_phones := v_phone_a || ':' || v_phone_b;
    END IF;
  ELSE
    v_phones := COALESCE(v_phone_a, v_phone_b, p_provider_message_sid);
  END IF;

  -- Priority: property_id + phones, then master_owner_id + phones, then just phones
  RETURN COALESCE(p_property_id, p_master_owner_id, 'unknown') || ':' || COALESCE(v_phones, 'orphan');
END;
$$;

-- 3. INBOX MESSAGES HYDRATED VIEW
DROP VIEW IF EXISTS public.inbox_messages_hydrated CASCADE;
CREATE OR REPLACE VIEW public.inbox_messages_hydrated AS
SELECT 
    me.id AS message_event_id,
    COALESCE(me.thread_key, sq.thread_key, public.resolve_thread_key(
        NULL,
        COALESCE(me.property_id, sq.property_id),
        COALESCE(me.master_owner_id, sq.master_owner_id),
        COALESCE(me.prospect_id, sq.prospect_id),
        CASE WHEN me.direction = 'inbound' THEN me.from_phone_number ELSE me.to_phone_number END,
        CASE WHEN me.direction = 'inbound' THEN me.to_phone_number ELSE me.from_phone_number END,
        me.provider_message_sid
    )) AS thread_key,
    me.direction,
    me.message_body,
    me.created_at AS message_created_at,
    COALESCE(me.event_timestamp, me.sent_at, me.delivered_at, me.created_at) AS event_timestamp,
    me.delivery_status,
    me.event_type,
    me.provider_message_sid,
    me.to_phone_number,
    me.from_phone_number,
    CASE WHEN me.direction = 'inbound' THEN me.from_phone_number ELSE me.to_phone_number END AS seller_phone,
    CASE WHEN me.direction = 'inbound' THEN me.to_phone_number ELSE me.from_phone_number END AS textgrid_phone,
    me.queue_id,
    me.template_id,
    sq.use_case_template AS template_use_case,
    sq.message_body AS rendered_message,
    COALESCE(me.property_id, sq.property_id) AS property_id,
    COALESCE(me.master_owner_id, sq.master_owner_id) AS master_owner_id,
    COALESCE(me.prospect_id, sq.prospect_id) AS prospect_id,
    me.phone_number_id,
    me.market_id,
    COALESCE(me.metadata->>'detected_intent', sq.detected_intent) AS detected_intent,
    (me.metadata->>'classification_confidence')::numeric AS classification_confidence,
    me.metadata->>'language' AS language,
    (me.metadata->>'needs_human_review')::boolean AS needs_human_review,
    me.metadata
FROM public.message_events me
LEFT JOIN public.send_queue sq ON me.queue_id = sq.id OR me.provider_message_sid = sq.provider_message_id;

GRANT SELECT ON public.inbox_messages_hydrated TO authenticated, anon;

-- 4. INBOX THREADS HYDRATED VIEW
DROP VIEW IF EXISTS public.inbox_threads_hydrated CASCADE;
CREATE OR REPLACE VIEW public.inbox_threads_hydrated AS
WITH thread_latest AS (
    SELECT DISTINCT ON (thread_key)
        thread_key,
        event_timestamp AS latest_message_at,
        message_body AS latest_message_body,
        direction AS latest_direction,
        property_id,
        master_owner_id,
        prospect_id,
        phone_number_id,
        market_id,
        detected_intent,
        classification_confidence,
        needs_human_review,
        template_id,
        template_use_case
    FROM public.inbox_messages_hydrated
    ORDER BY thread_key, event_timestamp DESC
),
thread_stats AS (
    SELECT 
        thread_key,
        count(*) AS message_count,
        count(*) FILTER (WHERE direction = 'inbound') AS inbound_count,
        count(*) FILTER (WHERE direction = 'outbound') AS outbound_count,
        count(*) FILTER (WHERE delivery_status = 'failed') AS failed_count,
        count(*) FILTER (WHERE delivery_status = 'delivered') AS delivered_count,
        count(*) FILTER (WHERE delivery_status = 'sent') AS sent_count
    FROM public.inbox_messages_hydrated
    GROUP BY thread_key
)
SELECT 
    ts.thread_key,
    tl.latest_message_at,
    tl.latest_message_body,
    tl.latest_direction,
    ts.message_count,
    ts.inbound_count,
    ts.outbound_count,
    ts.failed_count,
    ts.delivered_count,
    ts.sent_count,
    its.status AS thread_status,
    its.priority AS thread_priority,
    its.is_archived,
    its.is_read,
    its.is_pinned,
    its.is_starred,
    its.is_hidden,
    its.is_urgent,
    its.is_suppressed,
    -- Identity Fields
    tl.property_id,
    tl.master_owner_id,
    tl.prospect_id,
    tl.phone_number_id,
    tl.market_id,
    mo.display_name AS owner_name,
    pr.full_name AS prospect_name,
    pr.first_name,
    pr.best_phone,
    ph.phone,
    ph.canonical_e164,
    ph.primary_display_name AS display_phone,
    pr.language_preference,
    -- Property Fields
    p.property_address_full,
    p.property_address,
    p.property_address_city,
    p.property_address_state,
    p.property_address_zip,
    p.property_address_county_name,
    p.market,
    p.latitude,
    p.longitude,
    p.podio_tags AS property_tags,
    p.property_flags_text,
    p.property_type,
    p.property_class,
    p.style AS property_style,
    p.total_bedrooms AS beds,
    p.total_baths AS baths,
    p.building_square_feet AS sqft,
    p.units_count AS units,
    p.year_built,
    p.effective_year_built,
    p.estimated_value,
    p.saleprice AS last_sale_price,
    p.recording_date AS last_sale_date,
    p.document_type AS last_sale_document,
    p.equity_percent,
    p.equity_amount AS estimated_equity_amount,
    p.ownership_years,
    p.building_condition AS condition,
    p.final_acquisition_score,
    p.stories,
    p.units_count AS number_of_units,
    p.sum_buildings_nbr AS number_of_buildings,
    p.avg_sqft_per_unit AS avg_square_feet_per_unit,
    p.beds_per_unit AS avg_beds_per_unit,
    p.sqft_range AS square_foot_range,
    p.construction_type,
    p.exterior_walls,
    p.floor_cover,
    p.basement,
    p.other_rooms,
    p.num_of_fireplaces AS number_of_fireplaces,
    p.patio,
    p.porch,
    p.deck,
    p.driveway,
    p.garage,
    p.sum_garage_sqft AS garage_square_feet,
    p.air_conditioning,
    p.heating_type,
    p.heating_fuel_type,
    p.interior_walls,
    p.roof_cover,
    p.roof_type,
    p.pool,
    -- Financial/Valuation
    p.total_loan_amt AS loan_amount,
    p.total_loan_balance AS loan_balance,
    p.total_loan_payment AS loan_payment,
    p.assd_total_value AS assessed_total_value,
    p.assd_land_value AS assessed_land_value,
    p.assd_improvement_value AS assessed_improvement_value,
    p.estimated_repair_cost,
    p.rehab_level,
    p.building_quality,
    p.tax_delinquent,
    p.tax_delinquent_year,
    p.tax_amt AS tax_amount,
    p.lot_acreage AS lot_size_acres,
    p.lot_square_feet AS lot_size_square_feet,
    p.sewer,
    p.water,
    p.zoning,
    p.flood_zone,
    -- Prospect/Contact
    pr.marital_status,
    pr.gender,
    pr.language_preference AS prospect_language,
    pr.education_model AS education,
    pr.est_household_income AS household_income,
    pr.net_asset_value,
    pr.occupation_group AS occupation,
    pr.seller_tags_text AS prospect_tags,
    -- Owner fields
    mo.display_name AS owner_full_name,
    mo.owner_type_guess AS owner_type,
    mo.primary_owner_address AS owner_address,
    mo.priority_tier,
    mo.priority_score,
    mo.best_contact_window,
    mo.best_language AS owner_language,
    -- Portfolio
    mo.property_count AS portfolio_property_count,
    mo.portfolio_total_units,
    mo.portfolio_total_value AS portfolio_value,
    mo.portfolio_total_equity,
    mo.portfolio_total_loan_balance AS portfolio_total_debt,
    mo.portfolio_total_loan_payment AS portfolio_total_debt_payment,
    -- Risk/Pressure
    mo.financial_pressure_score,
    mo.urgency_score AS urgency_count,
    mo.tax_delinquent_count AS portfolio_tax_delinquent_count,
    mo.active_lien_count AS portfolio_lien_count,
    mo.oldest_tax_delinquent_year,
    mo.portfolio_total_tax_amount AS total_tax_amount,
    -- Automation/Thread State
    tl.detected_intent,
    tl.classification_confidence,
    tl.needs_human_review,
    tl.template_id,
    tl.template_use_case,
    its.metadata->>'automation_state' AS automation_state,
    its.metadata->>'next_action' AS next_action,
    -- Category Logic (Deterministic)
    CASE 
        WHEN tl.latest_message_body ~* '(stop|unsubscribe|remove me|do not text|wrong number|legal|harassment)' THEN 'dnc_opt_out'
        WHEN tl.needs_human_review = true THEN 'needs_review'
        WHEN tl.latest_message_body ~* '(price|how much|yes|interested|open to selling|asking price)' THEN 'hot_leads'
        WHEN tl.latest_direction = 'inbound' AND (its.is_read IS FALSE OR its.is_read IS NULL) THEN 'new_inbound'
        WHEN its.metadata->>'auto_reply_status' = 'active' THEN 'automated'
        WHEN tl.latest_direction = 'outbound' AND tl.latest_message_at > now() - interval '48 hours' THEN 'outbound_active'
        WHEN tl.latest_direction = 'outbound' AND tl.latest_message_at <= now() - interval '48 hours' THEN 'cold_no_response'
        ELSE 'all'
    END AS inbox_category
FROM thread_stats ts
JOIN thread_latest tl ON ts.thread_key = tl.thread_key
LEFT JOIN public.inbox_thread_state its ON ts.thread_key = its.thread_key
LEFT JOIN public.properties p ON tl.property_id = p.property_id
LEFT JOIN public.master_owners mo ON tl.master_owner_id = mo.master_owner_id
LEFT JOIN public.prospects pr ON tl.prospect_id = pr.prospect_id
LEFT JOIN public.phones ph ON tl.phone_number_id::text = ph.phone_id;

-- 5. DOSSIER VIEW
DROP VIEW IF EXISTS public.inbox_thread_dossier_hydrated CASCADE;
CREATE OR REPLACE VIEW public.inbox_thread_dossier_hydrated AS
SELECT * FROM public.inbox_threads_hydrated;

-- 6. CATEGORY COUNTS
DROP VIEW IF EXISTS public.inbox_category_counts CASCADE;
CREATE OR REPLACE VIEW public.inbox_category_counts AS
SELECT 
    inbox_category,
    count(*) AS count
FROM public.inbox_threads_hydrated
GROUP BY inbox_category;

-- 7. REALTIME
-- Try to add tables to publication if they are not already there
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication_tables 
    WHERE pubname = 'supabase_realtime' AND tablename = 'message_events'
  ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.message_events;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_publication_tables 
    WHERE pubname = 'supabase_realtime' AND tablename = 'send_queue'
  ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.send_queue;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_publication_tables 
    WHERE pubname = 'supabase_realtime' AND tablename = 'inbox_thread_state'
  ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.inbox_thread_state;
  END IF;
END $$;
