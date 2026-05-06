-- Create inbox_threads_hydrated view
CREATE OR REPLACE VIEW public.inbox_threads_hydrated AS
WITH me_with_joins AS (
  SELECT 
    me.id AS message_event_id,
    me.thread_key AS me_thread_key,
    me.queue_id AS me_queue_id,
    me.provider_message_sid,
    me.to_phone_number,
    me.from_phone_number,
    me.direction,
    me.body AS message_body,
    me.created_at AS message_created_at,
    me.event_type,
    me.property_id AS me_property_id,
    me.master_owner_id AS me_master_owner_id,
    me.phone_number_id AS me_phone_number_id,
    me.market_id AS me_market_id,
    me.metadata AS me_metadata,
    -- send_queue fields
    sq.thread_key AS sq_thread_key,
    sq.id AS sq_id,
    sq.property_id AS sq_property_id,
    sq.master_owner_id AS sq_master_owner_id,
    sq.phone_number_id AS sq_phone_number_id,
    sq.market_id AS sq_market_id,
    sq.queue_key,
    sq.queue_status,
    sq.queue_type,
    sq.scheduled_for,
    sq.sent_at AS sq_sent_at,
    sq.delivered_at AS sq_delivered_at,
    sq.failed_at AS sq_failed_at,
    sq.auto_reply_status,
    sq.auto_reply_last_attempt_at,
    sq.next_action,
    sq.next_automation_step,
    sq.stage_before,
    sq.stage_after,
    sq.thread_stage,
    sq.last_intent,
    sq.classification_confidence,
    sq.podio_sync_status,
    -- normalized phone pair
    LEAST(me.to_phone_number, me.from_phone_number) || ':' || GREATEST(me.to_phone_number, me.from_phone_number) AS phone_pair,
    -- properties fields
    p.id AS property_id,
    p.address_full AS property_address_full,
    p.address AS property_address,
    p.city AS property_address_city,
    p.state AS property_address_state,
    p.zip AS property_address_zip,
    p.county_name AS property_address_county_name,
    p.market,
    p.latitude,
    p.longitude,
    p.tags AS property_tags,
    p.flags_text AS property_flags_text,
    p.property_type,
    p.property_class,
    p.property_style,
    p.beds,
    p.baths,
    p.sqft,
    p.units,
    p.year_built,
    p.effective_year_built,
    p.estimated_value,
    p.last_sale_price,
    p.last_sale_date,
    p.last_sale_document,
    p.equity_percent,
    p.estimated_equity_amount,
    p.ownership_years,
    p.condition,
    p.final_acquisition_score,
    p.stories,
    p.number_of_units,
    p.number_of_buildings,
    p.avg_square_feet_per_unit,
    p.avg_beds_per_unit,
    p.square_foot_range,
    p.construction_type,
    p.exterior_walls,
    p.floor_cover,
    p.basement,
    p.other_rooms,
    p.number_of_fireplaces,
    p.patio,
    p.porch,
    p.deck,
    p.driveway,
    p.garage,
    p.garage_square_feet,
    p.air_conditioning,
    p.heating_type,
    p.heating_fuel_type,
    p.interior_walls,
    p.roof_cover,
    p.roof_type,
    p.pool,
    p.loan_amount,
    p.loan_balance,
    p.loan_payment,
    p.assessed_total_value,
    p.assessed_land_value,
    p.assessed_improvement_value,
    p.estimated_repair_cost,
    p.rehab_level,
    p.building_quality,
    p.tax_delinquent,
    p.tax_delinquent_year,
    p.tax_amount,
    p.lot_size_acres,
    p.lot_size_square_feet,
    p.sewer,
    p.water,
    p.zoning,
    p.flood_zone,
    p.market_id AS p_market_id,
    -- master_owners fields
    mo.id AS master_owner_id,
    mo.full_name AS owner_full_name,
    mo.owner_type,
    mo.address AS owner_address,
    mo.city AS owner_city,
    mo.state AS owner_state,
    mo.zip AS owner_zip,
    mo.priority_tier,
    mo.priority_score,
    mo.best_contact_window,
    mo.language AS owner_language,
    -- prospects fields
    pr.id AS prospect_id,
    pr.name AS prospect_name,
    pr.first_name AS prospect_first_name,
    pr.last_name AS prospect_last_name,
    pr.age,
    pr.marital_status,
    pr.gender,
    pr.language AS prospect_language,
    pr.education,
    pr.household_income,
    pr.net_asset_value,
    pr.occupation,
    pr.occupation_group,
    pr.tags AS prospect_tags,
    pr.phone_number AS prospect_phone_number,
    pr.phone_carrier,
    pr.contact_match_tags,
    pr.contact_match_badges,
    -- phone_numbers fields
    ph.id AS phone_number_id,
    ph.phone_number AS display_phone_number,
    -- portfolio fields
    port.property_count AS portfolio_property_count,
    port.property_type_majority,
    port.sfr_count,
    port.mf_count,
    port.total_units AS portfolio_total_units,
    port.total_value AS portfolio_value,
    port.total_equity AS portfolio_total_equity,
    port.total_debt AS portfolio_total_debt,
    port.total_debt_payment AS portfolio_total_debt_payment,
    -- financial pressure fields
    fp.financial_pressure_score,
    fp.urgency_count,
    fp.portfolio_tax_delinquent_count,
    fp.tax_delinquent_badge,
    fp.portfolio_lien_count,
    fp.active_lien_badge,
    fp.oldest_tax_delinquent_year,
    fp.total_tax_amount
  FROM public.message_events me
  LEFT JOIN public.send_queue sq ON me.queue_id = sq.id
  LEFT JOIN public.properties p ON COALESCE(me.property_id, sq.property_id) = p.id
  LEFT JOIN public.master_owners mo ON COALESCE(me.master_owner_id, sq.master_owner_id, p.master_owner_id) = mo.id
  LEFT JOIN public.prospects pr ON 
    pr.master_owner_id = COALESCE(me.master_owner_id, sq.master_owner_id, p.master_owner_id)
    OR pr.best_phone IN (me.to_phone_number, me.from_phone_number)
  LEFT JOIN public.phone_numbers ph ON 
    ph.id = COALESCE(me.phone_number_id, sq.phone_number_id)
    OR ph.phone_number IN (me.to_phone_number, me.from_phone_number)
  LEFT JOIN (
    SELECT 
      master_owner_id,
      COUNT(*) AS property_count,
      MODE(property_type) AS property_type_majority,
      COUNT(*) FILTER (WHERE property_type = 'SFR') AS sfr_count,
      COUNT(*) FILTER (WHERE property_type IN ('Multi-Family', 'MF')) AS mf_count,
      SUM(COALESCE(units, 1)) AS total_units,
      SUM(estimated_value) AS total_value,
      SUM(estimated_equity_amount) AS total_equity,
      SUM(loan_balance) AS total_debt,
      SUM(loan_payment) AS total_debt_payment
    FROM public.properties
    GROUP BY master_owner_id
  ) port ON port.master_owner_id = COALESCE(me.master_owner_id, sq.master_owner_id, p.master_owner_id)
  LEFT JOIN public.financial_pressure fp ON fp.master_owner_id = COALESCE(me.master_owner_id, sq.master_owner_id, p.master_owner_id)
),
resolved_threads AS (
  SELECT 
    *,
    COALESCE(
      me_thread_key,
      sq_thread_key,
      me_queue_id::TEXT,
      provider_message_sid,
      phone_pair,
      CONCAT(COALESCE(me_property_id, sq_property_id)::TEXT, ':', COALESCE(me_master_owner_id, sq_master_owner_id)::TEXT),
      message_event_id::TEXT
    ) AS resolved_thread_key
  FROM me_with_joins
),
thread_aggregates AS (
  SELECT 
    resolved_thread_key AS thread_key,
    resolved_thread_key AS thread_id,
    MAX(message_created_at) AS latest_message_at,
    (array_agg(message_event_id ORDER BY message_created_at DESC))[1] AS latest_message_event_id,
    (array_agg(message_body ORDER BY message_created_at DESC))[1] AS latest_message_body,
    (array_agg(direction ORDER BY message_created_at DESC))[1] AS latest_direction,
    MIN(message_created_at) AS first_message_at,
    COUNT(*) AS message_count,
    COUNT(*) FILTER (WHERE direction = 'inbound') AS inbound_count,
    COUNT(*) FILTER (WHERE direction = 'outbound') AS outbound_count,
    COUNT(*) FILTER (WHERE event_type = 'failed') AS failed_count,
    COUNT(*) FILTER (WHERE event_type = 'delivered') AS delivered_count,
    COUNT(*) FILTER (WHERE event_type = 'sent') AS sent_count,
    (array_agg(message_body ORDER BY message_created_at DESC) FILTER (WHERE direction = 'inbound'))[1] AS latest_inbound_body,
    (array_agg(message_created_at ORDER BY message_created_at DESC) FILTER (WHERE direction = 'inbound'))[1] AS latest_inbound_at,
    (array_agg(message_body ORDER BY message_created_at DESC) FILTER (WHERE direction = 'outbound'))[1] AS latest_outbound_body,
    (array_agg(message_created_at ORDER BY message_created_at DESC) FILTER (WHERE direction = 'outbound'))[1] AS latest_outbound_at,
    COALESCE(
      MAX(me_property_id) FILTER (WHERE me_property_id IS NOT NULL),
      MAX(sq_property_id) FILTER (WHERE sq_property_id IS NOT NULL),
      MAX(property_id) FILTER (WHERE property_id IS NOT NULL)
    ) AS resolved_property_id,
    COALESCE(
      MAX(me_master_owner_id) FILTER (WHERE me_master_owner_id IS NOT NULL),
      MAX(sq_master_owner_id) FILTER (WHERE sq_master_owner_id IS NOT NULL),
      MAX(master_owner_id) FILTER (WHERE master_owner_id IS NOT NULL)
    ) AS resolved_master_owner_id,
    COALESCE(
      MAX(me_phone_number_id) FILTER (WHERE me_phone_number_id IS NOT NULL),
      MAX(sq_phone_number_id) FILTER (WHERE sq_phone_number_id IS NOT NULL),
      MAX(phone_number_id) FILTER (WHERE phone_number_id IS NOT NULL)
    ) AS resolved_phone_number_id,
    COALESCE(
      MAX(me_market_id) FILTER (WHERE me_market_id IS NOT NULL),
      MAX(sq_market_id) FILTER (WHERE sq_market_id IS NOT NULL),
      MAX(p_market_id) FILTER (WHERE p_market_id IS NOT NULL)
    ) AS resolved_market_id,
    MAX(to_phone_number) AS to_phone_number,
    MAX(from_phone_number) AS from_phone_number,
    COALESCE(
      MAX(display_phone_number) FILTER (WHERE display_phone_number IS NOT NULL),
      CASE WHEN MAX(direction) = 'inbound' THEN MAX(from_phone_number) ELSE MAX(to_phone_number) END
    ) AS display_phone,
    CASE WHEN MAX(direction) = 'inbound' THEN MAX(from_phone_number) ELSE MAX(to_phone_number) END AS seller_phone,
    CASE 
      WHEN COALESCE(
        MAX(me_property_id) FILTER (WHERE me_property_id IS NOT NULL),
        MAX(sq_property_id),
        MAX(property_id)
      ) IS NOT NULL AND COALESCE(
        MAX(me_master_owner_id) FILTER (WHERE me_master_owner_id IS NOT NULL),
        MAX(sq_master_owner_id),
        MAX(master_owner_id)
      ) IS NOT NULL THEN 'full'
      WHEN COALESCE(
        MAX(me_property_id) FILTER (WHERE me_property_id IS NOT NULL),
        MAX(sq_property_id),
        MAX(property_id)
      ) IS NULL AND COALESCE(
        MAX(me_master_owner_id) FILTER (WHERE me_master_owner_id IS NOT NULL),
        MAX(sq_master_owner_id),
        MAX(master_owner_id)
      ) IS NULL THEN 'orphan'
      ELSE 'partial'
    END AS hydration_status,
    ARRAY_REMOVE(ARRAY[
      CASE WHEN COALESCE(
        MAX(me_property_id) FILTER (WHERE me_property_id IS NOT NULL),
        MAX(sq_property_id),
        MAX(property_id)
      ) IS NULL THEN 'property_id' END,
      CASE WHEN COALESCE(
        MAX(me_master_owner_id) FILTER (WHERE me_master_owner_id IS NOT NULL),
        MAX(sq_master_owner_id),
        MAX(master_owner_id)
      ) IS NULL THEN 'master_owner_id' END,
      CASE WHEN COALESCE(
        MAX(me_phone_number_id) FILTER (WHERE me_phone_number_id IS NOT NULL),
        MAX(sq_phone_number_id),
        MAX(phone_number_id)
      ) IS NULL THEN 'phone_number_id' END,
      CASE WHEN COALESCE(
        MAX(me_market_id) FILTER (WHERE me_market_id IS NOT NULL),
        MAX(sq_market_id),
        MAX(p_market_id)
      ) IS NULL THEN 'market_id' END
    ], NULL) AS missing_links,
    CASE 
      WHEN BOOL_OR(
        message_body ILIKE ANY(ARRAY['%stop%', '%unsubscribe%', '%wrong number%', '%wrong person%', '%do not text%', '%lawyer%', '%attorney%', '%legal%', '%harassment%', '%not interested%'])
        OR me_metadata->>'intent' IN ('stop', 'unsubscribe', 'dnc')
      ) THEN 'dnc_opt_out'
      WHEN BOOL_OR(
        message_body ILIKE ANY(ARRAY['%yes%', '%interested%', '%offer%', '%cash%', '%how much%', '%price%', '%asking price%', '%call me%', '%open to selling%', '%maybe%', '%depends%', '%what can you offer%'])
        OR me_metadata->>'intent' IN ('interested', 'hot_lead')
      ) THEN 'hot_leads'
      WHEN BOOL_OR(
        me_metadata->>'intent' IS NULL 
        OR (me_metadata->>'classification_confidence')::NUMERIC < 0.5
        OR resolved_master_owner_id IS NULL 
        OR resolved_property_id IS NULL
      ) THEN 'needs_review'
      WHEN MAX(direction) = 'inbound' AND NOT BOOL_OR(
        message_body ILIKE ANY(ARRAY['%stop%', '%unsubscribe%', '%wrong number%', '%wrong person%', '%do not text%', '%lawyer%', '%attorney%', '%legal%', '%harassment%', '%not interested%'])
      ) AND NOT BOOL_OR(
        message_body ILIKE ANY(ARRAY['%yes%', '%interested%', '%offer%', '%cash%', '%how much%', '%price%', '%asking price%', '%call me%', '%open to selling%', '%maybe%', '%depends%', '%what can you offer%'])
      ) THEN 'new_inbound'
      WHEN BOOL_OR(queue_type = 'auto_reply' OR auto_reply_status IS NOT NULL) THEN 'automated'
      WHEN MAX(direction) = 'outbound' AND NOT BOOL_OR(direction = 'inbound') THEN 'outbound_active'
      WHEN MAX(direction) = 'outbound' AND MAX(message_created_at) < NOW() - INTERVAL '48 hours' AND NOT BOOL_OR(direction = 'inbound') THEN 'cold_no_response'
      ELSE 'all'
    END AS inbox_category,
    MAX(thread_stage) AS thread_stage,
    MAX(last_intent) AS detected_intent,
    MAX(classification_confidence) AS ai_state,
    CASE WHEN BOOL_OR(queue_status IN ('scheduled', 'ready')) THEN 'active' ELSE 'inactive' END AS automation_state,
    BOOL_OR(me_metadata->>'needs_review' = 'true' OR classification_confidence < 0.5) AS needs_human_review,
    CASE WHEN inbox_category = 'hot_leads' THEN TRUE ELSE FALSE END AS is_hot_lead,
    CASE WHEN inbox_category = 'dnc_opt_out' THEN TRUE ELSE FALSE END AS is_dnc,
    CASE WHEN inbox_category = 'new_inbound' THEN TRUE ELSE FALSE END AS is_new_inbound,
    CASE WHEN MAX(direction) = 'outbound' AND NOT BOOL_OR(direction = 'inbound') THEN TRUE ELSE FALSE END AS is_waiting_on_seller,
    CASE WHEN inbox_category = 'automated' THEN TRUE ELSE FALSE END AS is_automated,
    BOOL_OR(event_type = 'failed') AS is_failed_automation,
    CASE 
      WHEN inbox_category = 'hot_leads' THEN 1000
      WHEN inbox_category = 'new_inbound' THEN 800
      WHEN inbox_category = 'needs_review' THEN 700
      WHEN inbox_category = 'automated' THEN 500
      WHEN inbox_category = 'outbound_active' THEN 300
      WHEN inbox_category = 'cold_no_response' THEN 100
      WHEN inbox_category = 'dnc_opt_out' THEN -500
      ELSE 0
    END + 
    CASE WHEN MAX(message_created_at) > NOW() - INTERVAL '1 hour' THEN 200 ELSE 0 END +
    CASE WHEN MAX(final_acquisition_score) > 80 THEN 100 ELSE 0 END AS priority_sort_score,
    MAX(sq_id) AS queue_id,
    MAX(queue_key) AS queue_key,
    MAX(queue_status) AS queue_status,
    MAX(queue_type) AS queue_type,
    MAX(scheduled_for) AS scheduled_for,
    MAX(sq_sent_at) AS sent_at,
    MAX(sq_delivered_at) AS delivered_at,
    MAX(sq_failed_at) AS failed_at,
    MAX(auto_reply_status) AS auto_reply_status,
    MAX(auto_reply_last_attempt_at) AS auto_reply_last_attempt_at,
    MAX(next_action) AS next_action,
    MAX(next_automation_step) AS next_automation_step,
    MAX(stage_before) AS stage_before,
    MAX(stage_after) AS stage_after,
    MAX(podio_sync_status) AS podio_sync_status
  FROM resolved_threads
  GROUP BY resolved_thread_key
)
SELECT 
  ta.thread_key,
  ta.thread_id,
  ta.latest_message_event_id,
  ta.latest_message_body,
  ta.latest_message_at,
  ta.latest_direction,
  ta.latest_inbound_body,
  ta.latest_inbound_at,
  ta.latest_outbound_body,
  ta.latest_outbound_at,
  ta.first_message_at,
  ta.message_count,
  ta.inbound_count,
  ta.outbound_count,
  ta.failed_count,
  ta.delivered_count,
  ta.sent_count,
  0 AS unread_count,
  ta.to_phone_number,
  ta.from_phone_number,
  ta.seller_phone,
  ta.display_phone,
  ta.resolved_property_id AS property_id,
  MAX(rt.property_address_full) AS property_address_full,
  MAX(rt.property_address) AS property_address,
  MAX(rt.property_address_city) AS property_address_city,
  MAX(rt.property_address_state) AS property_address_state,
  MAX(rt.property_address_zip) AS property_address_zip,
  MAX(rt.property_address_county_name) AS property_address_county_name,
  MAX(rt.market) AS market,
  MAX(rt.latitude) AS latitude,
  MAX(rt.longitude) AS longitude,
  MAX(rt.property_tags) AS property_tags,
  MAX(rt.property_flags_text) AS property_flags_text,
  MAX(rt.property_type) AS property_type,
  MAX(rt.property_class) AS property_class,
  MAX(rt.property_style) AS property_style,
  MAX(rt.beds) AS beds,
  MAX(rt.baths) AS baths,
  MAX(rt.sqft) AS sqft,
  MAX(rt.units) AS units,
  MAX(rt.year_built) AS year_built,
  MAX(rt.effective_year_built) AS effective_year_built,
  MAX(rt.estimated_value) AS estimated_value,
  MAX(rt.last_sale_price) AS last_sale_price,
  MAX(rt.last_sale_date) AS last_sale_date,
  MAX(rt.last_sale_document) AS last_sale_document,
  MAX(rt.equity_percent) AS equity_percent,
  MAX(rt.estimated_equity_amount) AS estimated_equity_amount,
  MAX(rt.ownership_years) AS ownership_years,
  MAX(rt.condition) AS condition,
  MAX(rt.final_acquisition_score) AS final_acquisition_score,
  MAX(rt.stories) AS stories,
  MAX(rt.number_of_units) AS property_number_of_units,
  MAX(rt.number_of_buildings) AS number_of_buildings,
  MAX(rt.avg_square_feet_per_unit) AS avg_square_feet_per_unit,
  MAX(rt.avg_beds_per_unit) AS avg_beds_per_unit,
  MAX(rt.square_foot_range) AS square_foot_range,
  MAX(rt.construction_type) AS construction_type,
  MAX(rt.exterior_walls) AS exterior_walls,
  MAX(rt.floor_cover) AS floor_cover,
  MAX(rt.basement) AS basement,
  MAX(rt.other_rooms) AS other_rooms,
  MAX(rt.number_of_fireplaces) AS number_of_fireplaces,
  MAX(rt.patio) AS patio,
  MAX(rt.porch) AS porch,
  MAX(rt.deck) AS deck,
  MAX(rt.driveway) AS driveway,
  MAX(rt.garage) AS garage,
  MAX(rt.garage_square_feet) AS garage_square_feet,
  MAX(rt.air_conditioning) AS air_conditioning,
  MAX(rt.heating_type) AS heating_type,
  MAX(rt.heating_fuel_type) AS heating_fuel_type,
  MAX(rt.interior_walls) AS interior_walls,
  MAX(rt.roof_cover) AS roof_cover,
  MAX(rt.roof_type) AS roof_type,
  MAX(rt.pool) AS pool,
  MAX(rt.loan_amount) AS loan_amount,
  MAX(rt.loan_balance) AS loan_balance,
  MAX(rt.loan_payment) AS loan_payment,
  MAX(rt.assessed_total_value) AS assessed_total_value,
  MAX(rt.assessed_land_value) AS assessed_land_value,
  MAX(rt.assessed_improvement_value) AS assessed_improvement_value,
  MAX(rt.estimated_repair_cost) AS estimated_repair_cost,
  MAX(rt.rehab_level) AS rehab_level,
  MAX(rt.building_quality) AS building_quality,
  MAX(rt.tax_delinquent) AS tax_delinquent,
  MAX(rt.tax_delinquent_year) AS tax_delinquent_year,
  MAX(rt.tax_amount) AS tax_amount,
  MAX(rt.lot_size_acres) AS lot_size_acres,
  MAX(rt.lot_size_square_feet) AS lot_size_square_feet,
  MAX(rt.sewer) AS sewer,
  MAX(rt.water) AS water,
  MAX(rt.zoning) AS zoning,
  MAX(rt.flood_zone) AS flood_zone,
  MAX(rt.prospect_id) AS prospect_id,
  MAX(rt.prospect_name) AS prospect_name,
  MAX(rt.prospect_first_name) AS prospect_first_name,
  MAX(rt.prospect_last_name) AS prospect_last_name,
  MAX(rt.age) AS age,
  MAX(rt.marital_status) AS marital_status,
  MAX(rt.gender) AS gender,
  MAX(rt.prospect_language) AS prospect_language,
  MAX(rt.education) AS education,
  MAX(rt.household_income) AS household_income,
  MAX(rt.net_asset_value) AS net_asset_value,
  MAX(rt.occupation) AS occupation,
  MAX(rt.occupation_group) AS occupation_group,
  MAX(rt.prospect_tags) AS prospect_tags,
  MAX(rt.prospect_phone_number) AS prospect_phone_number,
  MAX(rt.phone_carrier) AS phone_carrier,
  MAX(rt.contact_match_tags) AS contact_match_tags,
  MAX(rt.contact_match_badges) AS contact_match_badges,
  ta.resolved_master_owner_id AS master_owner_id,
  MAX(rt.owner_full_name) AS owner_full_name,
  MAX(rt.owner_type) AS owner_type,
  MAX(rt.owner_address) AS owner_address,
  MAX(rt.owner_city) AS owner_city,
  MAX(rt.owner_state) AS owner_state,
  MAX(rt.owner_zip) AS owner_zip,
  MAX(rt.priority_tier) AS priority_tier,
  MAX(rt.priority_score) AS priority_score,
  MAX(rt.best_contact_window) AS best_contact_window,
  MAX(rt.owner_language) AS owner_language,
  MAX(rt.portfolio_property_count) AS portfolio_property_count,
  MAX(rt.property_type_majority) AS property_type_majority,
  MAX(rt.sfr_count) AS sfr_count,
  MAX(rt.mf_count) AS mf_count,
  MAX(rt.portfolio_total_units) AS portfolio_total_units,
  MAX(rt.portfolio_value) AS portfolio_value,
  MAX(rt.portfolio_total_equity) AS portfolio_total_equity,
  MAX(rt.portfolio_total_debt) AS portfolio_total_debt,
  MAX(rt.portfolio_total_debt_payment) AS portfolio_total_debt_payment,
  MAX(rt.financial_pressure_score) AS financial_pressure_score,
  MAX(rt.urgency_count) AS urgency_count,
  MAX(rt.portfolio_tax_delinquent_count) AS portfolio_tax_delinquent_count,
  MAX(rt.tax_delinquent_badge) AS tax_delinquent_badge,
  MAX(rt.portfolio_lien_count) AS portfolio_lien_count,
  MAX(rt.active_lien_badge) AS active_lien_badge,
  MAX(rt.oldest_tax_delinquent_year) AS oldest_tax_delinquent_year,
  MAX(rt.total_tax_amount) AS total_tax_amount,
  ta.hydration_status,
  ta.missing_links,
  ta.inbox_category,
  ta.thread_stage,
  ta.detected_intent,
  ta.ai_state,
  ta.automation_state,
  ta.needs_human_review,
  ta.is_hot_lead,
  ta.is_dnc,
  ta.is_new_inbound,
  ta.is_waiting_on_seller,
  ta.is_automated,
  ta.is_failed_automation,
  ta.priority_sort_score,
  ta.queue_id,
  ta.queue_key,
  ta.queue_status,
  ta.queue_type,
  ta.scheduled_for,
  ta.sent_at,
  ta.delivered_at,
  ta.failed_at,
  ta.auto_reply_status,
  ta.auto_reply_last_attempt_at,
  ta.next_action,
  ta.next_automation_step,
  ta.stage_before,
  ta.stage_after,
  ta.podio_sync_status
FROM thread_aggregates ta
LEFT JOIN resolved_threads rt ON ta.thread_key = rt.resolved_thread_key
GROUP BY 
  ta.thread_key, ta.thread_id, ta.latest_message_event_id, ta.latest_message_body,
  ta.latest_message_at, ta.latest_direction, ta.latest_inbound_body, ta.latest_inbound_at,
  ta.latest_outbound_body, ta.latest_outbound_at, ta.first_message_at, ta.message_count,
  ta.inbound_count, ta.outbound_count, ta.failed_count, ta.delivered_count, ta.sent_count,
  ta.to_phone_number, ta.from_phone_number, ta.seller_phone, ta.display_phone,
  ta.resolved_property_id, ta.resolved_master_owner_id, ta.hydration_status, ta.missing_links,
  ta.inbox_category, ta.thread_stage, ta.detected_intent, ta.ai_state, ta.automation_state,
  ta.needs_human_review, ta.is_hot_lead, ta.is_dnc, ta.is_new_inbound, ta.is_waiting_on_seller,
  ta.is_automated, ta.is_failed_automation, ta.priority_sort_score, ta.queue_id, ta.queue_key,
  ta.queue_status, ta.queue_type, ta.scheduled_for, ta.sent_at, ta.delivered_at, ta.failed_at,
  ta.auto_reply_status, ta.auto_reply_last_attempt_at, ta.next_action, ta.next_automation_step,
  ta.stage_before, ta.stage_after, ta.podio_sync_status;

-- Create inbox_category_counts view
CREATE OR REPLACE VIEW public.inbox_category_counts AS
SELECT 
  inbox_category,
  COUNT(*) AS count,
  COUNT(*) FILTER (WHERE is_hot_lead) AS hot_count,
  COUNT(*) FILTER (WHERE is_new_inbound) AS new_inbound_count,
  COUNT(*) FILTER (WHERE needs_human_review) AS needs_review_count,
  COUNT(*) FILTER (WHERE is_automated) AS automated_count,
  COUNT(*) FILTER (WHERE inbox_category = 'outbound_active') AS outbound_active_count,
  COUNT(*) FILTER (WHERE inbox_category = 'cold_no_response') AS cold_count,
  COUNT(*) FILTER (WHERE is_dnc) AS dnc_count,
  (SELECT COUNT(*) FROM public.inbox_threads_hydrated) AS all_count
FROM public.inbox_threads_hydrated
GROUP BY inbox_category;

-- Optional helper function
CREATE OR REPLACE FUNCTION public.refresh_inbox_thread_state()
RETURNS VOID AS $$
BEGIN
  -- Regular views auto-refresh; no-op for materialized views (not used here)
  RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create indexes if missing
CREATE INDEX IF NOT EXISTS idx_message_events_thread_key ON public.message_events(thread_key);
CREATE INDEX IF NOT EXISTS idx_message_events_created_at ON public.message_events(created_at);
CREATE INDEX IF NOT EXISTS idx_message_events_event_timestamp ON public.message_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_message_events_direction ON public.message_events(direction);
CREATE INDEX IF NOT EXISTS idx_message_events_property_id ON public.message_events(property_id);
CREATE INDEX IF NOT EXISTS idx_message_events_master_owner_id ON public.message_events(master_owner_id);
CREATE INDEX IF NOT EXISTS idx_message_events_queue_id ON public.message_events(queue_id);
CREATE INDEX IF NOT EXISTS idx_message_events_provider_message_sid ON public.message_events(provider_message_sid);
CREATE INDEX IF NOT EXISTS idx_message_events_to_phone_number ON public.message_events(to_phone_number);
CREATE INDEX IF NOT EXISTS idx_message_events_from_phone_number ON public.message_events(from_phone_number);

CREATE INDEX IF NOT EXISTS idx_send_queue_thread_key ON public.send_queue(thread_key);
CREATE INDEX IF NOT EXISTS idx_send_queue_queue_key ON public.send_queue(queue_key);
CREATE INDEX IF NOT EXISTS idx_send_queue_property_id ON public.send_queue(property_id);
CREATE INDEX IF NOT EXISTS idx_send_queue_master_owner_id ON public.send_queue(master_owner_id);
CREATE INDEX IF NOT EXISTS idx_send_queue_phone_number_id ON public.send_queue(phone_number_id);
CREATE INDEX IF NOT EXISTS idx_send_queue_queue_status ON public.send_queue(queue_status);
CREATE INDEX IF NOT EXISTS idx_send_queue_scheduled_for ON public.send_queue(scheduled_for);

CREATE INDEX IF NOT EXISTS idx_properties_id ON public.properties(id);
CREATE INDEX IF NOT EXISTS idx_properties_master_owner_id ON public.properties(master_owner_id);

CREATE INDEX IF NOT EXISTS idx_prospects_master_owner_id ON public.prospects(master_owner_id);
CREATE INDEX IF NOT EXISTS idx_prospects_property_id ON public.prospects(property_id);
CREATE INDEX IF NOT EXISTS idx_prospects_best_phone ON public.prospects(best_phone);

CREATE INDEX IF NOT EXISTS idx_phone_numbers_master_owner_id ON public.phone_numbers(master_owner_id);
CREATE INDEX IF NOT EXISTS idx_phone_numbers_phone_number ON public.phone_numbers(phone_number);

-- Grant permissions
GRANT SELECT ON public.inbox_threads_hydrated TO authenticated, anon;
GRANT SELECT ON public.inbox_category_counts TO authenticated, anon;
