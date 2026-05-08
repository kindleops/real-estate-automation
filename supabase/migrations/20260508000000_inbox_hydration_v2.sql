
-- 1. ADD SUMMARY COLUMNS TO inbox_thread_state
ALTER TABLE public.inbox_thread_state 
ADD COLUMN IF NOT EXISTS message_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS inbound_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS outbound_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS latest_message_event_id UUID,
ADD COLUMN IF NOT EXISTS latest_message_body TEXT,
ADD COLUMN IF NOT EXISTS latest_message_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS latest_direction TEXT,
ADD COLUMN IF NOT EXISTS latest_event_type TEXT,
ADD COLUMN IF NOT EXISTS latest_delivery_status TEXT,
ADD COLUMN IF NOT EXISTS last_inbound_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_outbound_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS pending_queue_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS failed_queue_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS blocked_queue_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS next_scheduled_for TIMESTAMPTZ;

-- 2. RECREATE HYDRATED VIEWS

-- inbox_messages_hydrated
DROP VIEW IF EXISTS public.inbox_messages_hydrated CASCADE;
CREATE OR REPLACE VIEW public.inbox_messages_hydrated AS
SELECT 
    me.id AS message_event_id,
    me.thread_key,
    me.direction,
    me.message_body,
    me.created_at AS message_created_at,
    COALESCE(me.event_timestamp, me.sent_at, me.delivered_at, me.created_at) AS event_timestamp,
    me.delivery_status,
    me.event_type,
    me.provider_message_sid,
    me.to_phone_number,
    me.from_phone_number,
    me.queue_id,
    me.template_id,
    me.master_owner_id,
    me.prospect_id,
    me.property_id,
    me.phone_number_id,
    me.market_id,
    me.detected_intent,
    me.safety_status,
    me.priority,
    me.risk,
    me.routing_allowed,
    me.language,
    me.classification_confidence,
    me.metadata
FROM public.message_events me;

-- inbox_threads_hydrated (Full Completeness)
DROP VIEW IF EXISTS public.inbox_threads_hydrated CASCADE;
CREATE OR REPLACE VIEW public.inbox_threads_hydrated AS
SELECT 
    its.id AS inbox_state_id,
    its.thread_key,
    its.seller_phone,
    its.our_number,
    its.master_owner_id,
    its.prospect_id,
    its.property_id,
    its.market,
    its.stage,
    its.status,
    its.priority,
    its.latest_message_event_id,
    its.latest_direction,
    its.latest_message_body,
    its.latest_message_at,
    its.latest_delivery_status,
    its.message_count,
    its.inbound_count,
    its.outbound_count,
    its.last_inbound_at,
    its.last_outbound_at,
    its.pending_queue_count,
    its.failed_queue_count,
    its.blocked_queue_count,
    its.next_scheduled_for,
    its.is_archived,
    its.is_read,
    its.is_pinned,
    its.is_starred,
    its.is_hidden,
    its.is_urgent,
    its.is_suppressed,

    -- Owner fields
    mo.display_name AS owner_display_name,
    mo.display_name AS owner_full_name, -- Alias
    mo.owner_type_guess AS owner_type_guess,
    mo.owner_type_guess AS owner_type, -- Alias
    mo.primary_owner_address AS owner_address,
    -- Add city/state/zip if possible, but master_owners might not have them separated
    -- We can try to extract or leave as null if not available
    NULL AS owner_city,
    NULL AS owner_state,
    NULL AS owner_zip,
    mo.priority_score,
    mo.contactability_score,
    mo.financial_pressure_score,
    mo.urgency_score,
    mo.priority_tier AS owner_priority_tier,
    mo.best_language,
    mo.best_contact_window,
    mo.agent_persona,
    mo.agent_family,

    -- Prospect fields
    pr.full_name AS prospect_full_name,
    pr.first_name AS prospect_first_name,
    pr.cnam AS prospect_cnam,
    NULL AS age, -- Not in table
    pr.marital_status,
    pr.gender,
    pr.language_preference,
    pr.education_model AS education,
    pr.est_household_income,
    pr.net_asset_value,
    pr.occupation_group AS occupation,
    pr.occupation_code AS occupation_group, -- Alias
    pr.seller_tags_text AS prospect_tags,
    pr.contact_score_final,
    pr.phone_score_final,
    NULL AS phone_carrier, -- Not in table

    -- Portfolio fields
    mo.property_count AS portfolio_property_count,
    NULL AS property_type_majority, -- Not easily computed in view
    NULL AS sfr_count,
    NULL AS mf_count,
    mo.portfolio_total_units,
    mo.portfolio_total_value AS portfolio_value,
    mo.portfolio_total_equity,
    mo.portfolio_total_loan_balance AS portfolio_total_debt,
    mo.portfolio_total_loan_payment AS portfolio_total_debt_payment,
    mo.urgency_score AS urgency_count,
    mo.tax_delinquent_count AS portfolio_tax_delinquent_count,
    CASE WHEN mo.tax_delinquent_count > 0 THEN 'TAX_DELINQUENT' ELSE NULL END AS tax_delinquent_badge,
    mo.active_lien_count AS portfolio_lien_count,
    CASE WHEN mo.active_lien_count > 0 THEN 'ACTIVE_LIENS' ELSE NULL END AS active_lien_badge,
    mo.oldest_tax_delinquent_year,
    mo.portfolio_total_tax_amount AS total_tax_amount,

    -- Property fields
    p.property_address_full,
    p.property_address_city,
    p.property_address_state,
    p.property_address_zip,
    p.property_address_county_name,
    p.market AS property_market,
    p.latitude,
    p.longitude,
    p.streetview_image,
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
    p.cash_offer,
    p.estimated_repair_cost,
    p.final_acquisition_score,
    p.deal_strength_score,
    p.structured_motivation_score,
    p.equity_amount,
    p.equity_percent,
    p.saleprice AS last_sale_price,
    p.recording_date AS last_sale_date,
    p.document_type AS last_sale_document,
    p.ownership_years,
    p.building_condition AS condition,
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

    -- Financial / tax / land
    p.total_loan_amt AS loan_amount,
    p.total_loan_balance AS loan_balance,
    p.total_loan_payment AS loan_payment,
    p.assd_total_value AS assessed_total_value,
    p.assd_land_value AS assessed_land_value,
    p.assd_improvement_value AS assessed_improvement_value,
    p.tax_delinquent,
    p.tax_delinquent_year,
    p.tax_amt AS tax_amount,
    p.lot_acreage AS lot_size_acres,
    p.lot_square_feet AS lot_size_square_feet,
    p.sewer,
    p.water,
    p.zoning,
    p.flood_zone,
    p.rehab_level,
    p.building_quality,

    -- Automation
    its.last_intent AS ui_intent,
    its.priority AS priority_bucket,
    (its.priority = 'high') AS show_in_priority_inbox,
    its.automation_state,
    its.next_action AS next_system_action,
    (SELECT queue_status FROM public.send_queue WHERE id = its.latest_message_event_id LIMIT 1) AS queue_status,
    its.last_intent AS detected_intent,
    (SELECT safety_status FROM public.message_events WHERE id = its.latest_message_event_id LIMIT 1) AS safety_status,
    (SELECT routing_allowed FROM public.message_events WHERE id = its.latest_message_event_id LIMIT 1) AS routing_allowed,
    its.latest_reply_template_id,
    -- Category Logic (Deterministic)
    CASE 
        WHEN its.is_suppressed THEN 'dnc_opt_out'
        WHEN its.status = 'unread' THEN 'new_inbound'
        WHEN its.priority = 'high' THEN 'hot_leads'
        WHEN its.stage = 'automated' THEN 'automated'
        WHEN its.latest_direction = 'outbound' AND its.latest_message_at > now() - interval '48 hours' THEN 'outbound_active'
        WHEN its.latest_direction = 'outbound' AND its.latest_message_at <= now() - interval '48 hours' THEN 'cold_no_response'
        ELSE 'all'
    END AS inbox_category

FROM public.inbox_thread_state its
LEFT JOIN public.master_owners mo ON its.master_owner_id = mo.master_owner_id
LEFT JOIN public.prospects pr ON its.prospect_id = pr.prospect_id
LEFT JOIN public.properties p ON its.property_id = p.property_id;

-- dossier view
DROP VIEW IF EXISTS public.inbox_thread_dossier_hydrated CASCADE;
CREATE VIEW public.inbox_thread_dossier_hydrated AS SELECT * FROM public.inbox_threads_hydrated;

-- category counts
DROP VIEW IF EXISTS public.inbox_category_counts CASCADE;
CREATE VIEW public.inbox_category_counts AS
SELECT 
    CASE 
        WHEN is_suppressed THEN 'suppressed'
        WHEN status = 'unread' THEN 'needs_response'
        WHEN priority = 'high' THEN 'hot_leads'
        ELSE 'all'
    END AS category,
    count(*) AS count
FROM public.inbox_threads_hydrated
GROUP BY 1;

-- map pins
DROP VIEW IF EXISTS public.inbox_map_pins CASCADE;
CREATE VIEW public.inbox_map_pins AS
SELECT 
    thread_key,
    latitude,
    longitude,
    property_address_full,
    status,
    priority
FROM public.inbox_threads_hydrated
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
