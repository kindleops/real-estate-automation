import { normalizeCandidateRow } from "./supabase-candidate-feeder.js";

const DEFAULT_CANDIDATE_SOURCE = "v_sms_campaign_queue_candidates";
const ALLOWED_CANDIDATE_SOURCE_OVERRIDES = new Set([
  "outbound_candidate_snapshot",
  "v_sms_ready_contacts",
  "v_sms_campaign_queue_candidates",
  "v_launch_sms_tier1",
]);

/**
 * loadSupabaseOutboundCandidates
 * Queries Supabase (not Podio) for candidate discovery.
 * 
 * Candidate requirements:
 * - has usable to_phone_number
 * - has property/address context
 * - not opted out / DNC / stopped
 * - not already pending in send_queue
 * - not recently contacted within suppression window
 * - has or can resolve market/timezone/contact_window
 * - can resolve seller_first_name or safely handle name_missing
 * - can resolve property_type/property_class/unit_count
 */
export async function loadSupabaseOutboundCandidates(
  {
    limit = 25,
    scan_limit = null,
    candidate_offset = 0,
    candidate_source = null,
    market = null,
    state = null,
    template_use_case = null,
    touch_number = 1,
    campaign_session_id = null,
  } = {},
  deps = {}
) {
  // Use provided supabase client or fallback to default
  const supabase = deps.supabase || (await import("../../supabase/client.js")).supabase;

  const requested_source = String(candidate_source || "").trim();
  let source_name = DEFAULT_CANDIDATE_SOURCE;

  if (requested_source && ALLOWED_CANDIDATE_SOURCE_OVERRIDES.has(requested_source)) {
    source_name = requested_source;
  } else if (requested_source) {
    throw new Error(`Candidate source override not allowed: ${requested_source}`);
  }

  const requestedLimit = Number(limit) > 0 ? Math.trunc(Number(limit)) : 25;
  const requestedScanLimit = Number(scan_limit) > 0 ? Math.trunc(Number(scan_limit)) : null;
  const effective_fetch_limit = requestedScanLimit !== null
    ? Math.min(requestedScanLimit, 5000)
    : Math.min(Math.max(requestedLimit * 5, 10), 2500);
  const effective_offset = Math.max(0, Math.trunc(Number(candidate_offset) || 0));

  let query = supabase.from(source_name).select("*");
  if (effective_offset > 0) {
    query = query.range(effective_offset, effective_offset + effective_fetch_limit - 1);
  } else {
    query = query.limit(effective_fetch_limit);
  }

  const { data, error } = await query;

  if (error) {
    throw new Error(`Failed to load Supabase candidates: ${error.message || String(error)}`);
  }

  const rows = Array.isArray(data) ? data : [];
  
  // Normalize candidates utilizing the shared helper
  const normalized = rows
    .map((row) =>
      normalizeCandidateRow(row, {
        template_use_case,
        touch_number,
        campaign_session_id,
        market,
        state,
      })
    )
    .filter((row) => {
      // Basic initial filters, further filtering happens in feeder
      if (market && String(row.market || "").toLowerCase().replace(/[^a-z0-9]+/g, "_") !== String(market).toLowerCase().replace(/[^a-z0-9]+/g, "_")) {
        return false;
      }
      if (state && String(row.state || "").toLowerCase() !== String(state).toLowerCase()) {
        return false;
      }
      return true;
    });

  return {
    ok: true,
    source: source_name,
    requested_source: requested_source || null,
    candidate_offset: effective_offset,
    scanned_count: normalized.length,
    rows: normalized,
    effective_fetch_limit,
  };
}
