-- Rebuild v_sms_ready_contacts using production schema tables only:
-- master_owners, phones, properties, prospects, sub_owners.
--
-- Authoritative outbound phone rule:
--   Use master_owners.primary_phone_id first, then master_owners.best_phone_1.
--   Do not independently pick prospect/property-linked phones as primary.

alter table public.phones
  add column if not exists phone_first_name text,
  add column if not exists phone_full_name text,
  add column if not exists primary_display_name text;

create or replace view public.v_sms_ready_contacts as
select
  -- Property
  p.property_export_id,
  p.property_id,
  p.property_address_full,
  p.property_address_city,
  p.property_address_state,
  p.property_address_zip,
  p.property_county_name as property_address_county_name,
  p.market,
  coalesce(nullif(to_jsonb(p)->>'cash_offer', '')::numeric, null::numeric) as cash_offer,
  p.estimated_value,
  p.equity_amount,
  p.equity_percent,
  coalesce(
    nullif(to_jsonb(p)->>'final_acquisition_score', '')::numeric,
    nullif(to_jsonb(mo)->>'priority_score', '')::numeric,
    null::numeric
  ) as final_acquisition_score,
  p.seller_tags_text as podio_tags,

  -- Master owner
  mo.master_owner_id,
  mo.master_key,
  mo.display_name,
  mo.primary_owner_address,
  mo.priority_tier,
  mo.follow_up_cadence,
  mo.agent_persona,
  mo.agent_family,
  mo.best_language,

  -- Authoritative phone linkage (primary_phone_id -> best_phone_1)
  coalesce(nullif(mo.primary_phone_id, ''), nullif(mo.best_phone_1, '')) as best_phone_id,
  coalesce(ph.phone_id::text, coalesce(nullif(mo.primary_phone_id, ''), nullif(mo.best_phone_1, ''))) as phone_id,
  ph.phone,
  ph.canonical_e164,
  ph.best_phone_score,
  ph.phone_type,
  ph.activity_status,
  ph.usage_12_months,
  ph.usage_2_months,
  ph.contact_window,
  coalesce(ph.timezone, mo.routing_timezone) as timezone,
  ph.primary_prospect_id,
  ph.canonical_prospect_id,
  (coalesce(ph.canonical_e164, ph.phone) is not null) as sms_eligible,
  ph.phone_first_name,
  ph.phone_full_name,
  ph.primary_display_name,
  coalesce(
    nullif(ph.phone_first_name, ''),
    split_part(nullif(ph.phone_full_name, ''), ' ', 1)
  ) as seller_first_name,
  coalesce(
    nullif(ph.phone_full_name, ''),
    nullif(ph.primary_display_name, '')
  ) as seller_full_name,
  'properties.master_owner_id'::text as joined_property_source
from public.master_owners mo
left join public.phones ph
  on ph.phone_id::text = coalesce(nullif(mo.primary_phone_id, ''), nullif(mo.best_phone_1, ''))
left join public.properties p
  on p.master_owner_id = mo.master_owner_id;

comment on view public.v_sms_ready_contacts is
'Outbound-ready contacts sourced from master_owners + properties + phones. Authoritative phone selection is master_owners.primary_phone_id, with master_owners.best_phone_1 as fallback when primary is missing.';

-- Verification SQL:
-- select count(*) from phones where phone_first_name is not null or phone_full_name is not null;
-- select seller_first_name, seller_full_name, phone_first_name, phone_full_name, canonical_e164
-- from v_sms_ready_contacts
-- where seller_first_name is not null
-- limit 20;
