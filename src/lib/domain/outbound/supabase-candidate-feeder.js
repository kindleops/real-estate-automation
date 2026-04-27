import crypto from "node:crypto";

import { child } from "@/lib/logging/logger.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import { hasSupabaseConfig, supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { evaluateContactWindow, insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";

const SEND_QUEUE_TABLE = "send_queue";
const TEXTGRID_NUMBERS_TABLE = "textgrid_numbers";
const DEFAULT_CANDIDATE_SOURCE = "v_sms_campaign_queue_candidates";
const ALLOWED_CANDIDATE_SOURCE_OVERRIDES = new Set([
  "outbound_candidate_snapshot",
  "v_sms_ready_contacts",
]);
const CANDIDATE_SOURCE_AVAILABLE_HINT = [
  "v_sms_campaign_queue_candidates",
  "v_sms_ready_contacts",
  "v_launch_sms_tier1",
];

const REASON_CODES = Object.freeze({
  NO_MASTER_OWNER: "NO_MASTER_OWNER",
  NO_BEST_PHONE: "NO_BEST_PHONE",
  NO_PROPERTY: "NO_PROPERTY",
  NO_PHONE: "NO_PHONE",
  NO_VALID_PHONE: "NO_VALID_PHONE",
  TRUE_OPT_OUT: "TRUE_OPT_OUT",
  SUPPRESSED: "SUPPRESSED",
  OUTSIDE_CONTACT_WINDOW: "OUTSIDE_CONTACT_WINDOW",
  PENDING_PRIOR_TOUCH: "PENDING_PRIOR_TOUCH",
  DUPLICATE_QUEUE_ITEM: "DUPLICATE_QUEUE_ITEM",
  NO_TEMPLATE: "NO_TEMPLATE",
  TEMPLATE_RENDER_FAILED: "TEMPLATE_RENDER_FAILED",
  NO_VALID_TEXTGRID_NUMBER: "NO_VALID_TEXTGRID_NUMBER",
  ROUTING_BLOCKED: "ROUTING_BLOCKED",
  CAMPAIGN_LIMIT_REACHED: "CAMPAIGN_LIMIT_REACHED",
});

const logger = child({ module: "domain.outbound.supabase_candidate_feeder" });

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = lower(value);
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function asNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asPositiveInteger(value, fallback = null) {
  const parsed = asNumber(value, fallback);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (!clean(value)) return [];
  return clean(value)
    .split(",")
    .map((entry) => clean(entry))
    .filter(Boolean);
}

function isTruthyDataFlag(value) {
  const normalized = lower(value);
  return ["1", "true", "yes", "active", "confirmed", "suppressed"].includes(normalized);
}

function pick(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && clean(value) !== "") return value;
  }
  return null;
}

function maskPhone(value) {
  const phone = normalizePhone(value);
  if (!phone) return null;
  return `${phone.slice(0, 2)}******${phone.slice(-2)}`;
}

function getSupabase(deps = {}) {
  if (deps.supabase) return deps.supabase;
  if (!hasSupabaseConfig()) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  return defaultSupabase;
}

function normalizeMarket(value) {
  return lower(value)
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

const REGIONAL_ROUTING_RULES = [
  {
    name: "ca_to_los_angeles",
    states: ["ca"],
    target_markets: ["Los Angeles, CA"],
  },
  {
    name: "west_mountain_to_los_angeles",
    states: ["or", "wa", "nv", "az", "id", "ut"],
    target_markets: ["Los Angeles, CA"],
  },
  {
    name: "midwest_to_minneapolis",
    states: ["mn", "wi", "ia", "nd", "sd", "ne", "il", "in", "mi", "oh", "mo"],
    target_markets: ["Minneapolis, MN"],
  },
  {
    name: "southern_plains_to_dallas",
    states: ["ok", "ar", "ks"],
    target_markets: ["Dallas, TX"],
  },
  {
    name: "louisiana_to_houston",
    states: ["la"],
    target_markets: ["Houston, TX"],
  },
  {
    name: "texas_to_dallas_then_houston",
    states: ["tx"],
    target_markets: ["Dallas, TX", "Houston, TX"],
  },
  {
    name: "georgia_to_atlanta",
    states: ["ga"],
    target_markets: ["Atlanta, GA"],
  },
  {
    name: "carolinas_to_charlotte",
    states: ["nc", "sc"],
    target_markets: ["Charlotte, NC"],
  },
  {
    name: "florida_to_jacksonville_then_miami",
    states: ["fl"],
    target_markets: ["Jacksonville, FL", "Miami, FL"],
  },
  {
    name: "northeast_to_miami",
    states: ["ny", "nj", "pa", "md", "va", "dc", "de", "ct", "ri", "ma", "nh", "vt", "me"],
    target_markets: ["Miami, FL"],
  },
  {
    name: "southeast_inland_to_atlanta_then_charlotte",
    states: ["al", "ms", "tn", "ky"],
    target_markets: ["Atlanta, GA", "Charlotte, NC"],
  },
];

function findRegionalRoutingRule(state) {
  const normalized_state = lower(state);
  return REGIONAL_ROUTING_RULES.find((rule) => rule.states.includes(normalized_state)) || null;
}

function buildIdempotencyKey({
  master_owner_id,
  property_id,
  phone_id,
  template_use_case,
  touch_number,
  campaign_session_id,
}) {
  return [
    clean(master_owner_id),
    clean(property_id),
    clean(phone_id),
    clean(template_use_case),
    clean(touch_number),
    clean(campaign_session_id),
  ].join(":");
}

function buildQueueKeyFromIdempotencyKey(idempotency_key) {
  return `feed:${crypto.createHash("sha1").update(clean(idempotency_key)).digest("hex")}`;
}

export function normalizeCandidateRow(row = {}, defaults = {}) {
  const normalized_touch_number = asPositiveInteger(
    pick(row.touch_number, row.next_touch_number, defaults.touch_number),
    1
  );
  const template_lookup_use_case =
    clean(pick(row.template_use_case, row.use_case, row.selected_use_case, defaults.template_use_case)) ||
    "ownership_check";
  const stage_code = clean(pick(row.stage_code, defaults.stage_code, "S1")) || "S1";
  const stage_label =
    clean(pick(row.stage_label, defaults.stage_label, "Ownership Confirmation")) ||
    "Ownership Confirmation";
  const is_first_touch = normalized_touch_number === 1;

  // Accept any non-empty string for IDs (mo_..., prop_..., ph_..., numeric, etc.)
  const master_owner_id =
    pick(row.master_owner_id, row.owner_id, row.owner_podio_item_id) || null;
  const property_id =
    pick(row.property_id, row.property_export_id, row.property_item_id) || null;
  const property_export_id = pick(row.property_export_id) || null;
  const best_phone_id = pick(row.best_phone_id) || null;
  const phone_id = pick(row.best_phone_id, row.phone_id, row.phone_item_id) || null;
  const primary_prospect_id = pick(row.primary_prospect_id) || null;
  const canonical_prospect_id = pick(row.canonical_prospect_id) || null;

  const canonical_e164 =
    normalizePhone(
      pick(
        row.canonical_e164,
        row.best_phone_e164,
        row.phone,
        row.best_phone,
        row.phone_e164,
        row.phone_hidden,
        row.phone_number
      )
    ) || "";

  const market = clean(
    pick(row.market, row.market_name, row.seller_market, row.canonical_market_slug, row.market_label, defaults.market)
  );
  const state = clean(
    pick(row.state_code, row.property_address_state, row.property_state, row.seller_state, row.state, defaults.state)
  );
  const owner_display_name = clean(pick(row.owner_display_name, row.display_name, row.owner_name));
  const phone_full_name = clean(pick(row.phone_full_name, row.seller_full_name));
  const phone_first_name = clean(
    pick(
      row.phone_first_name,
      row.seller_first_name,
      phone_full_name ? phone_full_name.split(/\s+/).filter(Boolean)[0] : null
    )
  );
  const seller_first_name = clean(pick(row.seller_first_name, row.phone_first_name, phone_first_name));
  const seller_full_name = clean(pick(row.seller_full_name, row.phone_full_name, phone_full_name));

  return {
    raw: row,
    master_owner_id,
    property_id,
    property_export_id,
    best_phone_id,
    phone_id,
    primary_prospect_id,
    canonical_prospect_id,
    canonical_e164,
    market,
    state,
    state_code: state,
    owner_display_name,
    display_name: owner_display_name,
    phone_first_name,
    phone_full_name,
    seller_first_name,
    seller_full_name,
    joined_property_source: clean(pick(row.joined_property_source, row.property_join_source, "master_owner_property_relation")),
    property_address_full: clean(pick(row.property_address_full, row.property_address, row.address, row.title)),
    property_city: clean(pick(row.property_address_city, row.property_city)),
    property_state: state,
    property_zip: clean(pick(row.property_address_zip, row.property_zip)),
    timezone: clean(pick(row.timezone, row.seller_timezone, "America/Chicago")) || "America/Chicago",
    contact_window: clean(
      pick(row.contact_window, row.contact_window_local, row.allowed_contact_window, "")
    ),
    true_post_contact_suppression: asBoolean(
      pick(row.true_post_contact_suppression, row.post_contact_suppression, row.is_suppressed),
      false
    ),
    active_opt_out: asBoolean(pick(row.active_opt_out, row.opt_out_active, row.is_opted_out), false),
    pending_prior_touch: asBoolean(pick(row.pending_prior_touch), false),
    template_use_case: template_lookup_use_case,
    template_lookup_use_case,
    stage_code,
    stage_label,
    touch_number: normalized_touch_number,
    is_first_touch,
    is_follow_up: !is_first_touch,
    campaign_session_id:
      clean(pick(row.campaign_session_id, defaults.campaign_session_id)) ||
      `session-${new Date().toISOString().slice(0, 10)}`,
    property_address: clean(pick(row.property_address_full, row.property_address, row.address, row.title)),
    owner_first_name: clean(pick(row.owner_first_name, row.first_name)),
    owner_last_name: clean(pick(row.owner_last_name, row.last_name)),
    language: clean(pick(row.best_language, row.language, row.preferred_language, "English")) || "English",
    seller_name: clean(pick(row.seller_name, row.owner_display_name, row.display_name, row.owner_name, "there")) || "there",
    market_id: asPositiveInteger(pick(row.market_id), null),
    // Agent
    agent_persona: clean(pick(row.agent_persona)),
    agent_family: clean(pick(row.agent_family)),
    best_language: clean(pick(row.best_language, row.language)),
    // Scores & financials
    final_acquisition_score: asNumber(row.final_acquisition_score, null),
    best_phone_score: asNumber(row.best_phone_score, null),
    cash_offer: asNumber(row.cash_offer, null),
    estimated_value: asNumber(row.estimated_value, null),
    equity_amount: asNumber(row.equity_amount, null),
    equity_percent: asNumber(row.equity_percent, null),
    // v_sms_campaign_queue_candidates extras
    property_address_county_name: clean(pick(row.property_address_county_name)),
    estimated_repair_cost: asNumber(row.estimated_repair_cost, null),
    rehab_level: clean(pick(row.rehab_level)),
    contact_status: clean(pick(row.contact_status)),
    ownership_years: asNumber(row.ownership_years, null),
    // Priority / campaign
    priority_tier: clean(pick(row.priority_tier)),
    follow_up_cadence: clean(pick(row.follow_up_cadence)),
    // Phone metadata
    phone_type: clean(pick(row.phone_type)),
    activity_status: clean(pick(row.activity_status)),
    sms_eligible: asBoolean(pick(row.sms_eligible), true),
  };
}

function mapReasonToDiagnosticCounter(reason) {
  if (reason === REASON_CODES.SUPPRESSED || reason === REASON_CODES.TRUE_OPT_OUT) {
    return "suppression_block_count";
  }
  if (reason === REASON_CODES.OUTSIDE_CONTACT_WINDOW) {
    return "contact_window_block_count";
  }
  if (reason === REASON_CODES.PENDING_PRIOR_TOUCH) {
    return "pending_prior_touch_block_count";
  }
  if (reason === REASON_CODES.DUPLICATE_QUEUE_ITEM) {
    return "duplicate_queue_block_count";
  }
  if (reason === REASON_CODES.NO_TEMPLATE || reason === REASON_CODES.TEMPLATE_RENDER_FAILED) {
    return "template_block_count";
  }
  if (reason === REASON_CODES.ROUTING_BLOCKED || reason === REASON_CODES.NO_VALID_TEXTGRID_NUMBER) {
    return "routing_block_count";
  }
  return null;
}

function buildCandidateNormalizedPreview(raw, candidate) {
  return {
    raw_keys: Object.keys(raw || {}).slice(0, 50),
    best_phone_id: candidate.best_phone_id || null,
    normalized_master_owner_id: candidate.master_owner_id,
    normalized_property_id: candidate.property_id,
    normalized_phone_id: candidate.phone_id,
    normalized_phone_e164: candidate.canonical_e164 || null,
    phone_first_name: candidate.phone_first_name || null,
    phone_full_name: candidate.phone_full_name || null,
    seller_first_name: candidate.seller_first_name || null,
    seller_full_name: candidate.seller_full_name || null,
    joined_property_source: candidate.joined_property_source || null,
    normalized_market: candidate.market || null,
    normalized_state: candidate.state || null,
  };
}

function buildTemplateContext(candidate = {}) {
  return {
    owner_name: candidate.seller_name || "there",
    owner_first_name: candidate.owner_first_name || null,
    owner_last_name: candidate.owner_last_name || null,
    property_address: candidate.property_address || "your property",
    market_name: candidate.market || null,
    state: candidate.state || null,
  };
}

function decodeBasicHtmlEntities(text = "") {
  return String(text)
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
}

function stripHtml(text = "") {
  return String(text).replace(/<[^>]*>/g, " ");
}

function formatCurrency(value) {
  const numeric = asNumber(value, null);
  if (!Number.isFinite(numeric)) return "";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(numeric);
}

function isLikelyCorporateName(name = "") {
  const normalized = lower(name);
  return ["llc", "inc", "property", "management", "trust", "holdings"].some((token) =>
    normalized.includes(token)
  );
}

function deriveSellerFirstName(candidate = {}) {
  const explicit_first = clean(pick(candidate.seller_first_name, candidate.phone_first_name));
  if (explicit_first) return explicit_first;

  const phone_full_name = clean(pick(candidate.seller_full_name, candidate.phone_full_name));
  if (phone_full_name) {
    const parts = phone_full_name.split(/\s+/).filter(Boolean);
    return clean(parts[0]);
  }

  return "";
}

function getFirstName(value = "") {
  const normalized = clean(String(value || "").replace(/\s+/g, " "));
  if (!normalized) return "";
  return clean(normalized.split(/\s+/)[0] || "");
}

function parseCityStateFromAddress(address = "") {
  const text = clean(address);
  if (!text) {
    return { city: "", state: "" };
  }

  // Common pattern: "123 Main St, Houston, TX 77002"
  const comma_parts = text.split(",").map((part) => clean(part)).filter(Boolean);
  if (comma_parts.length >= 2) {
    const city = clean(comma_parts[comma_parts.length - 2]);
    const trailing = clean(comma_parts[comma_parts.length - 1]);
    const state_match = trailing.match(/^([A-Za-z]{2})\b/);
    return {
      city,
      state: state_match ? clean(state_match[1]).toUpperCase() : "",
    };
  }

  return { city: "", state: "" };
}

function getStreetAddress(value = "") {
  const normalized = clean(String(value || "").replace(/\s+/g, " "));
  if (!normalized) return "";

  const comma_index = normalized.indexOf(",");
  if (comma_index === -1) {
    return normalized;
  }

  return clean(normalized.slice(0, comma_index).replace(/\s+/g, " "));
}

function hasRenderedFullAddressSuffix(rendered_text = "", full_address = "", street_address = "") {
  const rendered = lower(clean(rendered_text));
  const full = clean(full_address);
  const street = clean(street_address);

  if (!rendered || !full || !street) return false;
  if (full.length <= street.length) return false;

  const comma_index = full.indexOf(",");
  if (comma_index === -1) return false;

  const suffix = clean(full.slice(comma_index));
  if (!suffix) return false;

  return rendered.includes(lower(suffix));
}

function isColdOutboundS1OwnershipCheck(selector = {}) {
  const use_case = clean(selector.use_case);
  const is_first_touch = Boolean(selector.is_first_touch);
  const stage_code = clean(selector.stage_code).toUpperCase();
  return use_case === "ownership_check" && is_first_touch && stage_code === "S1";
}

function rewriteAddressPlaceholdersForColdS1(template_body = "", selector = {}) {
  if (!isColdOutboundS1OwnershipCheck(selector)) {
    return template_body;
  }

  return String(template_body || "")
    .replace(/\{\{\s*property_address_full\s*\}\}/gi, "{{property_street_address}}")
    .replace(/\{\s*property_address_full\s*\}/gi, "{property_street_address}");
}

function hasBlankLocationPattern(text = "") {
  const normalized = String(text || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

  if (!normalized) return false;

  const direct_patterns = [
    " en .",
    " em .",
    " in .",
    "zai zhao",
  ];

  if (direct_patterns.some((pattern) => normalized.includes(pattern))) {
    return true;
  }

  // Detect language-specific "in <blank>." patterns with optional spaces.
  if (/\b(en|em|in)\s*\./i.test(normalized)) {
    return true;
  }

  // Mandarin romanization pattern where location token disappears.
  if (/\bzai\s{2,}zhao\b/i.test(String(text || ""))) {
    return true;
  }

  return false;
}

function buildTemplateVariablePayload(candidate = {}) {
  const owner_display_name = clean(
    pick(candidate.raw?.display_name, candidate.owner_display_name, candidate.raw?.owner_display_name)
  );
  const language = clean(pick(candidate.best_language, candidate.language, "English")) || "English";
  const property_address_full = clean(
    pick(candidate.property_address_full, candidate.property_address, candidate.raw?.property_address_full)
  );
  const property_street_address = getStreetAddress(property_address_full);
  const parsed_location = parseCityStateFromAddress(
    property_address_full
  );

  const property_city = clean(
    pick(
      candidate.raw?.property_address_city,
      candidate.property_city,
      candidate.raw?.property_city,
      candidate.raw?.city,
      parsed_location.city,
      ""
    )
  );

  const seller_market = clean(
    pick(
      candidate.seller_market,
      candidate.market,
      candidate.normalized_market,
      candidate.raw?.seller_market,
      candidate.raw?.market,
      candidate.raw?.normalized_market,
      property_city,
      ""
    )
  );

  const market = clean(
    pick(
      candidate.market,
      candidate.seller_market,
      candidate.normalized_market,
      candidate.raw?.market,
      candidate.raw?.seller_market,
      candidate.raw?.normalized_market,
      property_city,
      ""
    )
  );

  const city = clean(pick(property_city, seller_market, market, ""));

  const property_state = clean(
    pick(
      candidate.property_state,
      candidate.state,
      candidate.raw?.property_address_state,
      candidate.raw?.property_state,
      candidate.raw?.seller_state,
      parsed_location.state,
      ""
    )
  );

  const agent_name_raw = clean(
    pick(
      candidate.agent_persona,
      candidate.agent_name,
      candidate.sms_agent_name,
      candidate.assigned_agent_name,
      candidate.raw?.agent_persona,
      candidate.raw?.agent_name,
      candidate.raw?.sms_agent_name,
      candidate.raw?.assigned_agent_name,
      candidate.raw?.acquisition_agent_name,
      candidate.raw?.agent_family,
      candidate.agent_family,
      "Alex"
    )
  ) || "Alex";
  const agent_first_name = getFirstName(agent_name_raw) || "Alex";

  const payload = {
    seller_first_name: deriveSellerFirstName(candidate),
    owner_display_name,
    owner_name: owner_display_name || candidate.seller_name || "",
    seller_full_name: clean(pick(candidate.seller_full_name, candidate.phone_full_name)),
    property_address_full,
    property_address: property_street_address || property_address_full,
    property_street_address: property_street_address || property_address_full,
    property_city,
    city,
    property_address_city: property_city,
    market,
    seller_market,
    normalized_market: clean(pick(candidate.normalized_market, candidate.raw?.normalized_market, market)),
    property_state,
    state: property_state,
    property_address_state: property_state,
    seller_state: property_state,
    property_zip: clean(candidate.property_zip),
    offer_price: formatCurrency(candidate.cash_offer),
    cash_offer: candidate.cash_offer,
    agent_name: agent_first_name,
    agent_first_name,
    sender_name: agent_first_name,
    sms_agent_name: agent_first_name,
    acquisition_agent_name: agent_first_name,
    agent_name_raw,
    language,
  };

  return payload;
}

function applyTemplatePlaceholders(template_text = "", payload = {}) {
  const missing = new Set();

  const resolveKey = (key) => {
    const normalized_key = clean(key);
    if (!normalized_key) return "";
    const value = payload[normalized_key];
    if (value === null || value === undefined || clean(value) === "") {
      missing.add(normalized_key);
      return "";
    }
    return String(value);
  };

  let rendered = String(template_text || "").replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key) => resolveKey(key));
  rendered = rendered.replace(/\{\s*([a-zA-Z0-9_]+)\s*\}/g, (_, key) => resolveKey(key));

  return {
    rendered_text: rendered,
    missing_variables: [...missing],
  };
}

function scoreTemplateCandidate(template = {}, selector = {}) {
  const preferred_language = lower(selector.preferred_language || "English");
  const template_language = lower(template.language || "English");
  const preferred_persona = lower(selector.preferred_agent_persona || "");
  const template_persona = lower(template.agent_persona || "");
  const persona_exact = Boolean(preferred_persona) && template_persona === preferred_persona;
  const persona_null = !clean(template.agent_persona);
  const use_case_exact = lower(template.use_case) === lower(selector.use_case);
  const language_exact = template_language === preferred_language;
  const language_english = template_language === "english";

  let ranking_group = 7;
  if (use_case_exact && language_exact && persona_exact) ranking_group = 1;
  else if (use_case_exact && language_exact && persona_null) ranking_group = 2;
  else if (use_case_exact && language_exact) ranking_group = 3;
  else if (use_case_exact && language_english && persona_exact) ranking_group = 4;
  else if (use_case_exact && language_english && persona_null) ranking_group = 5;
  else if (use_case_exact && language_english) ranking_group = 6;

  const stage_bonus =
    lower(selector.use_case) === "ownership_check" && lower(selector.stage_code) === lower(template.stage_code)
      ? 1
      : 0;
  const touch_bonus = selector.is_first_touch
    ? asBoolean(template.is_first_touch, false)
      ? 1
      : 0
    : asBoolean(template.is_follow_up, false)
      ? 1
      : 0;

  return {
    ranking_group,
    stage_bonus,
    touch_bonus,
    success_rate: asNumber(template.success_rate, 0),
    usage_count: asNumber(template.usage_count, 0),
    version: asNumber(template.version, 0),
    updated_at_ts: new Date(template.updated_at || template.created_at || 0).getTime() || 0,
  };
}

function sortTemplateCandidates(left, right, selector) {
  const l = scoreTemplateCandidate(left, selector);
  const r = scoreTemplateCandidate(right, selector);
  if (l.ranking_group !== r.ranking_group) return l.ranking_group - r.ranking_group;
  if (l.stage_bonus !== r.stage_bonus) return r.stage_bonus - l.stage_bonus;
  if (l.touch_bonus !== r.touch_bonus) return r.touch_bonus - l.touch_bonus;
  if (l.success_rate !== r.success_rate) return r.success_rate - l.success_rate;
  if (l.usage_count !== r.usage_count) return r.usage_count - l.usage_count;
  if (l.version !== r.version) return r.version - l.version;
  return r.updated_at_ts - l.updated_at_ts;
}

function computeTemplateQualityScore(template = {}, selector = {}) {
  const ranked = scoreTemplateCandidate(template, selector);
  const quality_floor = 200;
  const ranking_score = Math.max(0, 100 - ranked.ranking_group * 12);
  const stage_score = ranked.stage_bonus * 6;
  const touch_score = ranked.touch_bonus * 5;
  const success_score = Math.max(0, Math.min(30, Number(ranked.success_rate || 0) * 30));
  const usage_score = Math.min(10, Math.log10(Math.max(1, Number(ranked.usage_count || 0))) * 4);
  const recency_score = ranked.updated_at_ts > 0 ? 1 : 0;
  return quality_floor + ranking_score + stage_score + touch_score + success_score + usage_score + recency_score;
}

function buildTemplateRotationSeed({
  master_owner_id,
  property_id,
  phone_id,
  language,
  use_case,
  stage_code,
  campaign_key,
  day_bucket,
} = {}) {
  const utc_bucket = clean(day_bucket) || new Date().toISOString().slice(0, 10);
  return [
    clean(master_owner_id),
    clean(property_id),
    clean(phone_id),
    clean(language),
    clean(use_case),
    clean(stage_code),
    clean(campaign_key),
    utc_bucket,
  ].join("|");
}

function stableSeedModulo(seed = "", modulo = 1) {
  const safe_modulo = Math.max(1, Number(modulo) || 1);
  const digest = crypto.createHash("sha1").update(clean(seed)).digest("hex");
  const numeric = Number.parseInt(digest.slice(0, 8), 16);
  return Number.isFinite(numeric) ? numeric % safe_modulo : 0;
}

function chooseRotatingTemplate(candidates = [], seed = "") {
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return {
      selected: null,
      selected_index: -1,
      selected_hash_index: -1,
      rotation_pool: [],
    };
  }

  const index = stableSeedModulo(seed, candidates.length);
  const selected = candidates[index] || candidates[0] || null;
  return {
    selected,
    selected_index: candidates.findIndex((template) => {
      const left = clean(template?.template_id || template?.id || template?.item_id);
      const right = clean(selected?.template_id || selected?.id || selected?.item_id);
      return left && right ? left === right : template === selected;
    }),
    selected_hash_index: index,
    rotation_pool: candidates,
  };
}

function shouldEnableTemplateRotation(selector = {}) {
  return isColdOutboundS1OwnershipCheck(selector);
}

function normalizeLanguageToken(value = "") {
  return lower(clean(value));
}

function filterTemplatesByPreferredLanguage(templates = [], selector = {}) {
  const preferred_language = clean(pick(selector.preferred_language, selector.language, "English")) || "English";
  const preferred_language_normalized = normalizeLanguageToken(preferred_language);
  const rows = Array.isArray(templates) ? templates : [];
  const matching = rows.filter((template) =>
    normalizeLanguageToken(pick(template?.language, template?.metadata?.language, "")) === preferred_language_normalized
  );

  return {
    preferred_language,
    preferred_language_normalized,
    templates: matching,
    had_mixed_languages: new Set(
      rows
        .map((template) => clean(pick(template?.language, template?.metadata?.language, "")))
        .filter(Boolean)
    ).size > 1,
  };
}

function isS1OwnershipCheckRotation(selector = {}) {
  const use_case = lower(clean(pick(selector.use_case, selector.template_use_case, "")));
  const stage_code = clean(selector.stage_code).toUpperCase();
  const preferred_language = lower(clean(pick(selector.preferred_language, selector.language, "English")));
  return use_case === "ownership_check" && stage_code === "S1" && preferred_language === "english";
}

async function getRecentTemplateIds(candidate = {}, selector = {}, options = {}, deps = {}) {
  if (typeof deps.getRecentTemplateIds === "function") {
    const custom = await deps.getRecentTemplateIds(candidate, selector, options);
    return Array.isArray(custom) ? custom.map((value) => clean(value)).filter(Boolean) : [];
  }

  let supabase;
  try {
    supabase = getSupabase(deps);
  } catch {
    return [];
  }

  const phone = normalizePhone(candidate.canonical_e164);
  const use_case = clean(selector.use_case);
  const stage_code = clean(selector.stage_code);
  const lookback_ms = 30 * 24 * 60 * 60 * 1000;
  const cutoff_ms = Date.now() - lookback_ms;
  const recent_template_ids = new Set();

  const collectFromRows = (rows = []) => {
    for (const row of rows) {
      const created_at_raw = pick(row?.created_at, row?.sent_at, row?.scheduled_for, row?.inserted_at);
      const created_at_ms = created_at_raw ? new Date(created_at_raw).getTime() : 0;
      if (!created_at_ms || created_at_ms < cutoff_ms) continue;

      const row_phone = normalizePhone(pick(row?.to_phone_number, row?.phone_number, row?.recipient_phone));
      if (phone && row_phone && phone !== row_phone) continue;

      const row_use_case = clean(
        pick(row?.use_case_template, row?.metadata?.template_use_case, row?.template_use_case, row?.use_case)
      );
      if (use_case && row_use_case && lower(row_use_case) !== lower(use_case)) continue;

      const row_stage_code = clean(
        pick(row?.metadata?.template_stage_code, row?.stage_code, row?.metadata?.selected_template_stage_code)
      );
      if (stage_code && row_stage_code && lower(row_stage_code) !== lower(stage_code)) continue;

      const template_id = clean(
        pick(
          row?.template_id,
          row?.metadata?.template_id,
          row?.metadata?.selected_template_id,
          row?.metadata?.template?.id
        )
      );
      if (template_id) recent_template_ids.add(template_id);
    }
  };

  try {
    const queue_query = await supabase
      .from(SEND_QUEUE_TABLE)
      .select("template_id,metadata,to_phone_number,created_at,scheduled_for,use_case_template,stage_code")
      .eq("master_owner_id", candidate.master_owner_id)
      .limit(200);
    if (Array.isArray(queue_query?.data)) {
      collectFromRows(queue_query.data);
    }
  } catch {
    // Best-effort only.
  }

  try {
    const events_query = await supabase
      .from("message_events")
      .select("template_id,metadata,to_phone_number,created_at,sent_at,use_case,stage_code")
      .eq("master_owner_id", candidate.master_owner_id)
      .limit(200);
    if (Array.isArray(events_query?.data)) {
      collectFromRows(events_query.data);
    }
  } catch {
    // message_events may not exist in every environment.
  }

  return [...recent_template_ids];
}

function buildRotationPool(sorted_templates = [], selector = {}) {
  const is_s1_ownership_rotation = isS1OwnershipCheckRotation(selector);
  const strategy = is_s1_ownership_rotation
    ? "cold_s1_wide_window"
    : "default_top_window";
  const score_window = is_s1_ownership_rotation ? 35 : 10;
  const min_pool_size = is_s1_ownership_rotation ? 16 : 0;
  const cap = is_s1_ownership_rotation ? 35 : 50;
  const scored = sorted_templates.map((template) => ({
    template,
    score: computeTemplateQualityScore(template, selector),
  }));

  if (!scored.length) {
    return {
      pool: [],
      best_score: null,
      min_score: null,
      strategy,
      min_pool_size,
      cap,
    };
  }

  const has_scores = scored.some((entry) => Number.isFinite(entry.score));
  if (!has_scores) {
    const fallback_pool = sorted_templates.slice(0, Math.min(Math.max(min_pool_size, 25), cap));
    return {
      pool: fallback_pool,
      best_score: null,
      min_score: null,
      strategy,
      min_pool_size,
      cap,
    };
  }

  const best_score = Math.max(...scored.map((entry) => Number(entry.score || 0)));
  const min_score = best_score - score_window;
  let pool = scored
    .filter((entry) => Number(entry.score || 0) >= min_score)
    .map((entry) => entry.template);

  if (is_s1_ownership_rotation && pool.length < min_pool_size) {
    const existing_ids = new Set(
      pool
        .map((template) => clean(template?.template_id || template?.id || template?.item_id))
        .filter(Boolean)
    );

    for (const entry of scored) {
      if (pool.length >= min_pool_size) break;
      const template = entry.template;
      const template_id = clean(template?.template_id || template?.id || template?.item_id);
      if (template_id && existing_ids.has(template_id)) continue;
      pool.push(template);
      if (template_id) existing_ids.add(template_id);
    }
  }

  if (!pool.length) {
    pool = sorted_templates.slice(0, Math.min(Math.max(min_pool_size, 25), cap));
  }

  return {
    pool: pool.slice(0, cap),
    best_score,
    min_score,
    strategy,
    min_pool_size,
    cap,
  };
}

function buildTemplateRotationDiagnostics({
  rotation_enabled = false,
  rotation_seed = "",
  eligible_template_count = 0,
  excluded_recent_template_ids = [],
  rotation_choice = {},
  selected_template = null,
  pool_result = {},
  preferred_language = "English",
  fetch_diagnostics = {},
} = {}) {
  const rotation_pool = Array.isArray(rotation_choice?.rotation_pool)
    ? rotation_choice.rotation_pool
    : [];
  const rotation_candidate_languages = [...new Set(
    rotation_pool
      .map((template) => clean(pick(template?.language, template?.metadata?.language, "")))
      .filter(Boolean)
  )];
  const selected_template_language = clean(
    pick(selected_template?.language, selected_template?.metadata?.language, "")
  ) || null;
  const preferred_language_normalized = normalizeLanguageToken(preferred_language);
  const mismatch_detected = rotation_candidate_languages.some(
    (language) => normalizeLanguageToken(language) !== preferred_language_normalized
  );

  return {
    enabled: rotation_enabled,
    seed: rotation_seed,
    preferred_language: clean(preferred_language) || "English",
    requested_language: clean(preferred_language) || "English",
    selected_template_language,
    rotation_candidate_languages,
    rotation_language_mismatch_detected: mismatch_detected,
    rotation_strategy: clean(pool_result?.strategy) || "default_top_window",
    rotation_best_score: Number.isFinite(Number(pool_result?.best_score))
      ? Number(pool_result.best_score)
      : null,
    rotation_min_score: Number.isFinite(Number(pool_result?.min_score))
      ? Number(pool_result.min_score)
      : null,
    eligible_template_count: Number(eligible_template_count || 0),
    rotation_pool_size: rotation_pool.length,
    rotation_candidate_template_ids: rotation_pool
      .map((template) => clean(template?.template_id || template?.id || template?.item_id))
      .filter(Boolean),
    excluded_recent_template_ids: Array.isArray(excluded_recent_template_ids)
      ? excluded_recent_template_ids
      : [],
    selected_template_id:
      clean(selected_template?.template_id || selected_template?.id || selected_template?.item_id) || null,
    selected_index: Number.isFinite(Number(rotation_choice?.selected_index))
      ? Number(rotation_choice.selected_index)
      : -1,
    template_fetch_limit: Number(fetch_diagnostics?.template_fetch_limit) || null,
    template_fetch_language_filter_applied: Boolean(fetch_diagnostics?.template_fetch_language_filter_applied),
    template_fetch_use_case_filter_applied: fetch_diagnostics?.template_fetch_use_case_filter_applied !== false,
    template_fetch_stage_filter_applied: Boolean(fetch_diagnostics?.template_fetch_stage_filter_applied),
    template_fetch_fallback_used: Boolean(fetch_diagnostics?.template_fetch_fallback_used),
    raw_template_count_before_language_filter: Number(fetch_diagnostics?.raw_template_count_before_language_filter || 0),
    template_count_after_language_filter: Number(fetch_diagnostics?.template_count_after_language_filter || 0),
  };
}

function parseWindowTime(value) {
  const matched = clean(value).match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!matched) return null;
  let hours = Number(matched[1]);
  const minutes = Number(matched[2]);
  const period = matched[3].toUpperCase();
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return null;
  if (hours < 1 || hours > 12 || minutes < 0 || minutes > 59) return null;
  if (period === "AM" && hours === 12) hours = 0;
  if (period === "PM" && hours !== 12) hours += 12;
  return hours * 60 + minutes;
}

function parseContactWindowRange(window_text = "") {
  const cleaned = clean(window_text).replace(/\s+/g, " ");
  if (!cleaned) return null;
  const parts = cleaned.split(/\s*-\s*|\s+to\s+/i);
  if (!Array.isArray(parts) || parts.length !== 2) return null;
  const start = parseWindowTime(parts[0]);
  const end = parseWindowTime(parts[1]);
  if (start === null || end === null) return null;
  return { start, end };
}

function getLocalMinutesNow(timezone) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone || "America/Chicago",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).formatToParts(new Date());
    const hh = Number(parts.find((p) => p.type === "hour")?.value ?? 0);
    const mm = Number(parts.find((p) => p.type === "minute")?.value ?? 0);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    return hh * 60 + mm;
  } catch {
    return null;
  }
}

function computeNextSchedulableTime(candidate = {}, now_iso = new Date().toISOString()) {
  const range = parseContactWindowRange(candidate.contact_window);
  if (!range) return now_iso;
  const local_minutes = getLocalMinutesNow(candidate.timezone);
  if (local_minutes === null) return now_iso;
  if (local_minutes <= range.start) {
    const now = new Date();
    now.setUTCMinutes(now.getUTCMinutes() + Math.max(range.start - local_minutes, 0));
    return now.toISOString();
  }
  const next_day = new Date();
  next_day.setUTCDate(next_day.getUTCDate() + 1);
  next_day.setUTCHours(14, 0, 0, 0);
  return next_day.toISOString();
}

function getLocalMinutesAt(date, timezone) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone || "America/Chicago",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).formatToParts(date);
    const hh = Number(parts.find((p) => p.type === "hour")?.value ?? 0);
    const mm = Number(parts.find((p) => p.type === "minute")?.value ?? 0);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    return hh * 60 + mm;
  } catch {
    return null;
  }
}

function parseTimeLocal(value) {
  const cleaned = clean(value);
  const ampm_result = parseWindowTime(cleaned);
  if (ampm_result !== null) return ampm_result;
  const hhmm = cleaned.match(/^(\d{1,2}):(\d{2})$/);
  if (hhmm) {
    const hours = Number(hhmm[1]);
    const minutes = Number(hhmm[2]);
    if (hours >= 0 && hours <= 23 && minutes >= 0 && minutes <= 59) {
      return hours * 60 + minutes;
    }
  }
  return null;
}

function snapToNextWindowOpen(date_ms, timezone, start_minutes, end_minutes) {
  const date = new Date(date_ms);
  const local_minutes = getLocalMinutesAt(date, timezone);
  if (local_minutes === null) return date;
  if (local_minutes >= start_minutes && local_minutes < end_minutes) {
    return date;
  }
  if (local_minutes < start_minutes) {
    const delta_minutes = start_minutes - local_minutes;
    return new Date(date_ms + delta_minutes * 60 * 1000);
  }
  const minutes_to_tomorrow_start = (24 * 60 - local_minutes) + start_minutes;
  return new Date(date_ms + minutes_to_tomorrow_start * 60 * 1000);
}

function createSpreadScheduler({
  now_iso,
  schedule_start_local = "09:00",
  schedule_end_local = "20:00",
  schedule_interval_seconds_min = 45,
  schedule_interval_seconds_max = 180,
} = {}) {
  const default_start_minutes = parseTimeLocal(schedule_start_local) ?? (9 * 60);
  const default_end_minutes = parseTimeLocal(schedule_end_local) ?? (20 * 60);
  const min_interval_ms = Math.max(1, Number(schedule_interval_seconds_min) || 45) * 1000;
  const max_interval_ms = Math.max(min_interval_ms, Number(schedule_interval_seconds_max) || 180) * 1000;
  let cursor_ms = new Date(now_iso).getTime();

  return {
    nextScheduledFor(candidate) {
      const timezone = candidate.timezone || "America/Chicago";
      const range = parseContactWindowRange(candidate.contact_window);
      const start_minutes = range?.start ?? default_start_minutes;
      const end_minutes = range?.end ?? default_end_minutes;
      const jitter_ms = min_interval_ms + Math.random() * (max_interval_ms - min_interval_ms);
      cursor_ms += jitter_ms;
      const snapped = snapToNextWindowOpen(cursor_ms, timezone, start_minutes, end_minutes);
      cursor_ms = snapped.getTime();
      return snapped.toISOString();
    },
  };
}

export async function getSupabaseFeederCandidates(
  {
    limit = 25,
    scan_limit = null,
    candidate_source = null,
    market = null,
    state = null,
    template_use_case = null,
    touch_number = 1,
    campaign_session_id = null,
  } = {},
  deps = {}
) {
  const supabase = getSupabase(deps);

  const requested_source = clean(candidate_source);
  let source_name = DEFAULT_CANDIDATE_SOURCE;

  if (requested_source && ALLOWED_CANDIDATE_SOURCE_OVERRIDES.has(requested_source)) {
    source_name = requested_source;
  }

  if (requested_source && !ALLOWED_CANDIDATE_SOURCE_OVERRIDES.has(requested_source)) {
    return {
      ok: false,
      error: "CANDIDATE_SOURCE_UNAVAILABLE",
      source: source_name,
      requested_source,
      scanned_count: 0,
      rows: [],
      candidate_source_error: `candidate_source override not allowed: ${requested_source}`,
      available_hint: [...CANDIDATE_SOURCE_AVAILABLE_HINT],
    };
  }

  const requestedLimit = asPositiveInteger(limit, 25);
  const requestedScanLimit = asPositiveInteger(scan_limit, null);
  const effective_fetch_limit = requestedScanLimit !== null
    ? Math.min(requestedScanLimit, 5000)
    : Math.min(Math.max(requestedLimit * 5, 10), 2500);
  const { data, error } = await supabase
    .from(source_name)
    .select("*")
    .limit(effective_fetch_limit);

  if (error) {
    return {
      ok: false,
      error: "CANDIDATE_SOURCE_UNAVAILABLE",
      source: source_name,
      requested_source: requested_source || null,
      scanned_count: 0,
      rows: [],
      candidate_source_error: error?.message || String(error),
      available_hint: [...CANDIDATE_SOURCE_AVAILABLE_HINT],
    };
  }

  const rows = Array.isArray(data) ? data : [];
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
      if (market && normalizeMarket(row.market) !== normalizeMarket(market)) return false;
      if (state && lower(row.state) !== lower(state)) return false;
      return true;
    });

  return {
    ok: true,
    source: source_name,
    requested_source: requested_source || null,
    scanned_count: normalized.length,
    rows: normalized,
    effective_fetch_limit,
  };
}

async function hasDuplicateQueueItem(candidate = {}, options = {}, deps = {}) {
  if (typeof deps.hasDuplicateQueueItem === "function") {
    return deps.hasDuplicateQueueItem(candidate, options);
  }

  const supabase = getSupabase(deps);
  const statuses = ["queued", "sending", "sent"];

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .select("id,queue_status,touch_number,to_phone_number,metadata")
    .eq("master_owner_id", candidate.master_owner_id)
    .eq("property_id", candidate.property_id)
    .in("queue_status", statuses)
    .eq("touch_number", candidate.touch_number)
    .limit(20);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const phone = normalizePhone(candidate.canonical_e164);

  const matched = rows.find((row) => {
    const row_phone = normalizePhone(row?.to_phone_number);
    const template_use_case = clean(
      row?.metadata?.template_use_case || row?.metadata?.selected_use_case || row?.use_case_template
    );
    return row_phone === phone && template_use_case === clean(options.template_use_case);
  });

  return Boolean(matched);
}

export async function evaluateCandidateEligibility(candidate = {}, options = {}, deps = {}) {
  if (!candidate.master_owner_id) {
    return { ok: false, reason_code: REASON_CODES.NO_MASTER_OWNER, reason: "missing_master_owner_id" };
  }
  if (!candidate.property_id) {
    return { ok: false, reason_code: REASON_CODES.NO_PROPERTY, reason: "missing_property_id" };
  }
  if (!candidate.best_phone_id && !options.allow_phone_fallback) {
    return { ok: false, reason_code: REASON_CODES.NO_BEST_PHONE, reason: "missing_best_phone_id" };
  }
  if (!candidate.phone_id) {
    return { ok: false, reason_code: REASON_CODES.NO_VALID_PHONE, reason: "missing_phone_id" };
  }
  if (!candidate.canonical_e164) {
    return { ok: false, reason_code: REASON_CODES.NO_VALID_PHONE, reason: "missing_phone_e164" };
  }
  if (candidate.true_post_contact_suppression) {
    return { ok: false, reason_code: REASON_CODES.SUPPRESSED, reason: "true_post_contact_suppression" };
  }
  if (candidate.active_opt_out) {
    return { ok: false, reason_code: REASON_CODES.TRUE_OPT_OUT, reason: "active_opt_out" };
  }
  if (candidate.pending_prior_touch) {
    return { ok: false, reason_code: REASON_CODES.PENDING_PRIOR_TOUCH, reason: "pending_prior_touch" };
  }

  const duplicate = await hasDuplicateQueueItem(candidate, options, deps);
  if (duplicate) {
    return { ok: false, reason_code: REASON_CODES.DUPLICATE_QUEUE_ITEM, reason: "duplicate_queue_item" };
  }

  const window_check = evaluateContactWindow(
    {
      contact_window: candidate.contact_window || null,
      timezone: candidate.timezone || "America/Chicago",
      scheduled_for: options.now || new Date().toISOString(),
    },
    { now: options.now }
  );

  if (options.within_contact_window_now && !window_check.allowed) {
    return {
      ok: false,
      reason_code: REASON_CODES.OUTSIDE_CONTACT_WINDOW,
      reason: "outside_contact_window",
      contact_window: window_check,
    };
  }

  return {
    ok: true,
    reason_code: "OK",
    reason: "eligible",
    contact_window: window_check,
    scheduled_for: options.within_contact_window_now
      ? options.now || new Date().toISOString()
      : computeNextSchedulableTime(candidate, options.now || new Date().toISOString()),
  };
}

function isRoutingConfigEnabled(value) {
  return asBoolean(value, false) || isTruthyDataFlag(value);
}

function normalizeRoutingAliases(value) {
  return asArray(value).map((entry) => normalizeMarket(entry));
}

function normalizeTextgridNumberRow(row = {}) {
  const market = clean(pick(row.market_name, row.market, row.seller_market));
  const state_aliases = asArray(pick(row.allowed_states, row.cluster_states, row.routing_states));
  return {
    raw: row,
    id: pick(row.id, row.textgrid_number_id),
    phone_number: normalizePhone(pick(row.phone_number, row.number, row.e164)),
    market,
    market_normalized: normalizeMarket(market),
    aliases: normalizeRoutingAliases(pick(row.approved_market_aliases, row.routing_aliases)),
    status: lower(pick(row.status, row.number_status, "active")),
    allow_nationwide_fallback: isRoutingConfigEnabled(row.allow_nationwide_fallback),
    allow_cluster_fallback: isRoutingConfigEnabled(row.allow_cluster_fallback),
    is_nationwide: isRoutingConfigEnabled(pick(row.is_nationwide, row.nationwide, false)),
    state_aliases: state_aliases.map((entry) => lower(entry)),
    messages_sent_today: asNumber(pick(row.messages_sent_today, row.sent_today), 0),
    last_used_at: clean(row.last_used_at) || null,
  };
}

function buildRoutingSelection({
  selected,
  routing_tier,
  selection_reason,
  routing_rule_name = null,
  seller_market = null,
  seller_state = null,
} = {}) {
  return {
    ok: true,
    reason_code: "OK",
    routing_allowed: true,
    routing_tier,
    selection_reason,
    routing_rule_name,
    selected_textgrid_market: selected?.market || null,
    selected_textgrid_number: selected?.phone_number || null,
    seller_market: seller_market || null,
    seller_state: seller_state || null,
    routing_block_reason: null,
    selected: {
      id: selected?.id || null,
      phone_number: selected?.phone_number || null,
      market: selected?.market || null,
    },
  };
}

function byUsageThenRecency(left, right) {
  const left_sent = asNumber(left.messages_sent_today, 0);
  const right_sent = asNumber(right.messages_sent_today, 0);
  if (left_sent !== right_sent) return left_sent - right_sent;

  const left_ts = left.last_used_at ? new Date(left.last_used_at).getTime() : 0;
  const right_ts = right.last_used_at ? new Date(right.last_used_at).getTime() : 0;
  return left_ts - right_ts;
}

export async function chooseTextgridNumber(candidate = {}, options = {}, deps = {}) {
  if (typeof deps.chooseTextgridNumber === "function") {
    return deps.chooseTextgridNumber(candidate, options);
  }

  const supabase = getSupabase(deps);
  const { data, error } = await supabase
    .from(TEXTGRID_NUMBERS_TABLE)
    .select("*")
    .limit(200);

  if (error) throw error;

  const numbers = (Array.isArray(data) ? data : [])
    .map(normalizeTextgridNumberRow)
    .filter((row) => row.id && row.phone_number && (!row.status || row.status === "active"));

  if (!numbers.length) {
    return {
      ok: false,
      reason_code: REASON_CODES.NO_VALID_TEXTGRID_NUMBER,
      routing_allowed: false,
      routing_tier: "none",
      selection_reason: null,
      routing_rule_name: null,
      selected_textgrid_market: null,
      selected_textgrid_number: null,
      seller_market: candidate.market || null,
      seller_state: candidate.state || null,
      routing_block_reason: "NO_ACTIVE_TEXTGRID_NUMBERS",
      selected: null,
    };
  }

  const seller_market = normalizeMarket(candidate.market);
  const seller_state = lower(candidate.state);

  const exact = numbers.filter((row) => row.market_normalized === seller_market).sort(byUsageThenRecency);
  const alias = numbers
    .filter((row) => row.aliases.includes(seller_market) && row.market_normalized !== seller_market)
    .sort(byUsageThenRecency);
  const regional_rule = findRegionalRoutingRule(seller_state);
  const regional = regional_rule
    ? regional_rule.target_markets
        .map((target_market) => {
          const target_market_normalized = normalizeMarket(target_market);
          const candidates = numbers
            .filter((row) => row.market_normalized === target_market_normalized)
            .sort(byUsageThenRecency);
          return candidates.length
            ? {
                target_market,
                selected: candidates[0],
              }
            : null;
        })
        .filter(Boolean)
    : [];
  const nationwide = numbers
    .filter((row) => row.is_nationwide && row.allow_nationwide_fallback)
    .sort(byUsageThenRecency);

  if (exact.length) {
    return buildRoutingSelection({
      selected: exact[0],
      routing_tier: "exact_market_match",
      selection_reason: "exact_market_match",
      routing_rule_name: "exact_market_match",
      seller_market: candidate.market,
      seller_state: candidate.state,
    });
  }

  if (alias.length) {
    return buildRoutingSelection({
      selected: alias[0],
      routing_tier: "approved_alias_match",
      selection_reason: "approved_alias_match",
      routing_rule_name: "approved_alias_match",
      seller_market: candidate.market,
      seller_state: candidate.state,
    });
  }

  if (regional.length) {
    return buildRoutingSelection({
      selected: regional[0].selected,
      routing_tier: "approved_regional_fallback",
      selection_reason: `approved_regional_fallback:${regional[0].target_market}`,
      routing_rule_name: regional_rule?.name || null,
      seller_market: candidate.market,
      seller_state: candidate.state,
    });
  }

  if (!options.routing_safe_only && nationwide.length) {
    return buildRoutingSelection({
      selected: nationwide[0],
      routing_tier: "approved_nationwide_fallback",
      selection_reason: "approved_nationwide_fallback",
      routing_rule_name: "approved_nationwide_fallback",
      seller_market: candidate.market,
      seller_state: candidate.state,
    });
  }

  return {
      ok: false,
      reason_code: REASON_CODES.ROUTING_BLOCKED,
      routing_allowed: false,
      routing_tier: "blocked",
      selection_reason: null,
      routing_rule_name: regional_rule?.name || null,
      selected_textgrid_market: null,
      selected_textgrid_number: null,
      seller_market: candidate.market || null,
      seller_state: candidate.state || null,
      routing_block_reason: "NO_APPROVED_ROUTING_PATH",
      selected: null,
    };
}

export async function renderOutboundTemplate(candidate = {}, options = {}, deps = {}) {
  if (typeof deps.renderOutboundTemplate === "function") {
    return deps.renderOutboundTemplate(candidate, options);
  }

  const selector = {
    use_case: clean(options.template_use_case || candidate.template_lookup_use_case || candidate.template_use_case) || "ownership_check",
    stage_code: clean(candidate.stage_code) || "S1",
    stage_label: clean(candidate.stage_label) || "Ownership Confirmation",
    touch_number: asPositiveInteger(candidate.touch_number, 1),
    is_first_touch: Number(candidate.touch_number || 1) === 1,
    preferred_language: clean(pick(candidate.best_language, candidate.language, "English")) || "English",
    preferred_agent_persona: clean(candidate.agent_persona) || "",
    property_type_scope: clean(candidate.raw?.property_type_scope) || null,
    deal_strategy: clean(candidate.raw?.deal_strategy) || null,
  };

  const is_cold_s1_fetch = isS1OwnershipCheckRotation(selector);
  const fetch_limit = is_cold_s1_fetch ? 500 : 200;
  const fetch_language = clean(selector.preferred_language) || null;

  let fetch_diagnostics = {
    template_fetch_limit: fetch_limit,
    template_fetch_language_filter_applied: Boolean(fetch_language),
    template_fetch_use_case_filter_applied: true,
    template_fetch_stage_filter_applied: false,
    template_fetch_fallback_used: false,
    raw_template_count_before_language_filter: 0,
    template_count_after_language_filter: 0,
  };

  let templates = [];
  if (typeof deps.fetchSmsTemplates === "function") {
    templates = await deps.fetchSmsTemplates(selector, candidate, options);
    fetch_diagnostics = {
      ...fetch_diagnostics,
      raw_template_count_before_language_filter: templates.length,
    };
  } else {
    const supabase = getSupabase(deps);

    let primary_query = supabase
      .from("sms_templates")
      .select("*")
      .eq("is_active", true)
      .eq("use_case", selector.use_case);

    if (fetch_language) {
      primary_query = primary_query.ilike("language", fetch_language);
    }

    primary_query = primary_query.limit(fetch_limit);

    const { data, error } = await primary_query;

    if (error) {
      return {
        ok: false,
        reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
        reason: "template_query_failed",
        render_error_message: error?.message || String(error),
        template: null,
        rendered_message_body: null,
        missing_variables: [],
        variable_payload_preview: {},
        selected_template_preview: null,
      };
    }

    const primary_rows = Array.isArray(data) ? data : [];
    fetch_diagnostics.raw_template_count_before_language_filter = primary_rows.length;
    templates = primary_rows;

    if (!templates.length) {
      let fallback_query = supabase
        .from("sms_templates")
        .select("*")
        .eq("is_active", true);

      if (fetch_language) {
        fallback_query = fallback_query.ilike("language", fetch_language);
      }

      fallback_query = fallback_query.limit(fetch_limit);

      const fallback_any_use_case = await fallback_query;
      const fallback_rows = Array.isArray(fallback_any_use_case?.data) ? fallback_any_use_case.data : [];

      if (fallback_rows.length) {
        fetch_diagnostics.template_fetch_fallback_used = true;
        fetch_diagnostics.raw_template_count_before_language_filter = fallback_rows.length;
      }
      templates = fallback_rows;
    }
  }

  const language_filtered = filterTemplatesByPreferredLanguage(templates, selector);
  fetch_diagnostics.template_count_after_language_filter = language_filtered.templates.length;
  templates = language_filtered.templates;

  if (!templates.length) {
    return {
      ok: false,
      reason_code: REASON_CODES.NO_TEMPLATE,
      reason: "no_template_for_preferred_language",
      template: null,
      rendered_message_body: null,
      missing_variables: [],
      variable_payload_preview: buildTemplateVariablePayload(candidate),
      selected_template_preview: null,
      template_rotation: {
        enabled: false,
        preferred_language: language_filtered.preferred_language,
        requested_language: language_filtered.preferred_language,
        selected_template_language: null,
        rotation_candidate_languages: [],
        rotation_language_mismatch_detected: false,
        rotation_strategy: "no_template_for_preferred_language",
        rotation_best_score: null,
        rotation_min_score: null,
        eligible_template_count: 0,
        rotation_pool_size: 0,
        rotation_candidate_template_ids: [],
        excluded_recent_template_ids: [],
        selected_template_id: null,
        selected_index: -1,
        ...fetch_diagnostics,
      },
    };
  }

  const sorted_templates = [...templates].sort((left, right) => sortTemplateCandidates(left, right, selector));
  let selected_template = sorted_templates[0] || null;
  const rotation_enabled = shouldEnableTemplateRotation(selector);
  const recent_template_ids = rotation_enabled
    ? await getRecentTemplateIds(candidate, selector, options, deps)
    : [];

  let eligible_rotation_candidates = sorted_templates;
  if (rotation_enabled && recent_template_ids.length) {
    const recent_id_set = new Set(recent_template_ids.map((value) => clean(value)));
    const without_recent = sorted_templates.filter((template) => {
      const template_id = clean(template?.template_id || template?.id || template?.item_id);
      return !template_id || !recent_id_set.has(template_id);
    });
    if (without_recent.length) {
      eligible_rotation_candidates = without_recent;
    }
  }

  const pool_result = rotation_enabled
    ? buildRotationPool(eligible_rotation_candidates, selector)
    : { pool: [selected_template].filter(Boolean), best_score: null };
  const rotation_seed = buildTemplateRotationSeed({
    master_owner_id: candidate.master_owner_id,
    property_id: candidate.property_id,
    phone_id: candidate.best_phone_id || candidate.phone_id,
    language: selector.preferred_language,
    use_case: selector.use_case,
    stage_code: selector.stage_code,
    campaign_key: clean(options.campaign_key || options.campaign_session_id || candidate.campaign_session_id),
    day_bucket: clean(options.day_bucket) || clean(options.now).slice(0, 10),
  });
  const rotation_choice = rotation_enabled
    ? chooseRotatingTemplate(pool_result.pool, rotation_seed)
    : {
        selected: selected_template,
        selected_index: selected_template ? 0 : -1,
        selected_hash_index: selected_template ? 0 : -1,
        rotation_pool: [selected_template].filter(Boolean),
      };

  selected_template = rotation_choice.selected || selected_template;
  const template_rotation = buildTemplateRotationDiagnostics({
    rotation_enabled,
    rotation_seed,
    eligible_template_count: sorted_templates.length,
    excluded_recent_template_ids: recent_template_ids,
    rotation_choice,
    selected_template,
    pool_result,
    preferred_language: language_filtered.preferred_language,
    fetch_diagnostics,
  });

  const selected_template_with_source = selected_template
    ? { ...selected_template, source: "sms_templates" }
    : null;
  const source_body = clean(pick(selected_template?.template_body, selected_template?.english_translation));
  const rewritten_source_body = rewriteAddressPlaceholdersForColdS1(source_body, selector);

  if (!rewritten_source_body) {
    return {
      ok: false,
      reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
      reason: "rendered_message_empty",
      render_error_message: "rendered_message_empty",
      template: selected_template_with_source,
      rendered_message_body: null,
      missing_variables: [],
      variable_payload_preview: buildTemplateVariablePayload(candidate),
      selected_template_preview: {
        id: selected_template?.id || null,
        template_id: selected_template?.template_id || null,
        podio_template_id: selected_template?.podio_template_id || null,
        template_name: selected_template?.template_name || null,
        use_case: selected_template?.use_case || null,
        language: selected_template?.language || null,
        stage_code: selected_template?.stage_code || null,
        stage_label: selected_template?.stage_label || null,
      },
      template_rotation,
    };
  }

  const variable_payload = buildTemplateVariablePayload(candidate);
  const rendered = applyTemplatePlaceholders(rewritten_source_body, variable_payload);
  const normalized_rendered = clean(decodeBasicHtmlEntities(stripHtml(rendered.rendered_text || "")));

  if (!normalized_rendered) {
    return {
      ok: false,
      reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
      reason: "rendered_message_empty",
      render_error_message: "rendered_message_empty",
      template: selected_template_with_source,
      rendered_message_body: null,
      missing_variables: rendered.missing_variables,
      variable_payload_preview: variable_payload,
      selected_template_preview: {
        id: selected_template?.id || null,
        template_id: selected_template?.template_id || null,
        podio_template_id: selected_template?.podio_template_id || null,
        template_name: selected_template?.template_name || null,
        use_case: selected_template?.use_case || null,
        language: selected_template?.language || null,
        stage_code: selected_template?.stage_code || null,
        stage_label: selected_template?.stage_label || null,
      },
      template_rotation,
    };
  }

  if (hasBlankLocationPattern(normalized_rendered)) {
    return {
      ok: false,
      reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
      reason: "template_render_failed",
      missing_placeholder_reason: "blank_location_placeholder",
      render_error_message: "blank_location_placeholder",
      template: selected_template_with_source,
      template_id: selected_template?.template_id || selected_template?.id || null,
      rendered_message_body: null,
      missing_variables: rendered.missing_variables,
      variable_payload_preview: variable_payload,
      selected_template_preview: {
        id: selected_template?.id || null,
        template_id: selected_template?.template_id || null,
        podio_template_id: selected_template?.podio_template_id || null,
        template_name: selected_template?.template_name || null,
        use_case: selected_template?.use_case || null,
        language: selected_template?.language || null,
        stage_code: selected_template?.stage_code || null,
        stage_label: selected_template?.stage_label || null,
      },
      template_rotation,
    };
  }

  if (
    isColdOutboundS1OwnershipCheck(selector) &&
    hasRenderedFullAddressSuffix(
      normalized_rendered,
      variable_payload.property_address_full,
      variable_payload.property_street_address
    )
  ) {
    return {
      ok: false,
      reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
      reason: "full_address_rendered_in_cold_sms",
      template: selected_template_with_source,
      template_id: selected_template?.template_id || selected_template?.id || null,
      rendered_preview: normalized_rendered.slice(0, 200),
      rendered_message_body: null,
      missing_variables: rendered.missing_variables,
      property_address_full: variable_payload.property_address_full,
      property_street_address: variable_payload.property_street_address,
      variable_payload_preview: variable_payload,
      selected_template_preview: {
        id: selected_template?.id || null,
        template_id: selected_template?.template_id || null,
        podio_template_id: selected_template?.podio_template_id || null,
        template_name: selected_template?.template_name || null,
        use_case: selected_template?.use_case || null,
        language: selected_template?.language || null,
        stage_code: selected_template?.stage_code || null,
        stage_label: selected_template?.stage_label || null,
      },
      template_rotation,
    };
  }

  const agent_name_raw = clean(variable_payload.agent_name_raw);
  const agent_first_name = clean(variable_payload.agent_first_name);
  const has_full_agent_name = /\s+/.test(agent_name_raw);
  const rendered_lower = lower(normalized_rendered);
  if (
    isColdOutboundS1OwnershipCheck(selector) &&
    has_full_agent_name &&
    rendered_lower.includes(lower(agent_name_raw))
  ) {
    return {
      ok: false,
      reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
      reason: "agent_full_name_rendered",
      template: selected_template_with_source,
      template_id: selected_template?.template_id || selected_template?.id || null,
      rendered_preview: normalized_rendered.slice(0, 200),
      rendered_message_body: null,
      missing_variables: rendered.missing_variables,
      agent_name_raw,
      agent_first_name,
      variable_payload_preview: variable_payload,
      selected_template_preview: {
        id: selected_template?.id || null,
        template_id: selected_template?.template_id || null,
        podio_template_id: selected_template?.podio_template_id || null,
        template_name: selected_template?.template_name || null,
        use_case: selected_template?.use_case || null,
        language: selected_template?.language || null,
        stage_code: selected_template?.stage_code || null,
        stage_label: selected_template?.stage_label || null,
      },
      template_rotation,
    };
  }

  return {
    ok: true,
    reason_code: "OK",
    template: selected_template_with_source,
    template_use_case: selector.use_case,
    rendered_message_body: normalized_rendered,
    missing_variables: rendered.missing_variables,
    variable_payload_preview: variable_payload,
    selected_template_preview: {
      id: selected_template?.id || null,
      template_id: selected_template?.template_id || null,
      podio_template_id: selected_template?.podio_template_id || null,
      template_name: selected_template?.template_name || null,
      use_case: selected_template?.use_case || null,
      language: selected_template?.language || null,
      stage_code: selected_template?.stage_code || null,
      stage_label: selected_template?.stage_label || null,
    },
    template_rotation,
    stage_code: selected_template?.stage_code || selector.stage_code,
    stage_label: selected_template?.stage_label || selector.stage_label,
    language: selected_template?.language || selector.preferred_language,
  };
}

export async function createSendQueueItem(candidate = {}, options = {}, deps = {}) {
  const effective_phone_id = candidate.best_phone_id || candidate.phone_id;
  const idempotency_key = buildIdempotencyKey({
    master_owner_id: candidate.master_owner_id,
    property_id: candidate.property_id,
    phone_id: effective_phone_id,
    template_use_case: options.template_use_case,
    touch_number: candidate.touch_number,
    campaign_session_id: candidate.campaign_session_id,
  });

  const queue_key = buildQueueKeyFromIdempotencyKey(idempotency_key);
  const scheduled_for = options.scheduled_for || new Date().toISOString();
  const queue_status = new Date(scheduled_for).getTime() > Date.now() ? "scheduled" : "queued";

  const payload = {
    queue_key,
    queue_id: queue_key,
    queue_status,
    scheduled_for,
    scheduled_for_utc: scheduled_for,
    scheduled_for_local: scheduled_for,
    message_body: options.rendered_message_body,
    message_text: options.rendered_message_body,
    to_phone_number: candidate.canonical_e164,
    from_phone_number: options.selected_textgrid_number,
    textgrid_number_id: options.selected_textgrid_number_id,
    master_owner_id: candidate.master_owner_id,
    property_id: candidate.property_id,
    market_id: candidate.market_id,
    template_id: options.template_id,
    touch_number: candidate.touch_number,
    timezone: candidate.timezone,
    contact_window: candidate.contact_window || null,
    use_case_template: options.template_use_case,
    metadata: {
      idempotency_key,
      campaign_session_id: candidate.campaign_session_id,
      template_use_case: options.template_use_case,
      selected_textgrid_number_id: options.selected_textgrid_number_id,
      selected_textgrid_number: options.selected_textgrid_number,
      selected_textgrid_market: options.selected_textgrid_market,
      seller_market: candidate.market,
      seller_state: candidate.state,
      routing_tier: options.routing_tier,
      selection_reason: options.selection_reason,
      routing_allowed: options.routing_allowed,
      routing_block_reason: options.routing_block_reason,
      candidate_snapshot: {
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
        best_phone_id: candidate.best_phone_id,
        phone_id: candidate.phone_id,
        canonical_phone_masked: maskPhone(candidate.canonical_e164),
        seller_market: candidate.market,
        seller_state: candidate.state,
        touch_number: candidate.touch_number,
      },
      template: {
        id: options.template_id,
        source: options.template_source || "supabase",
        use_case: options.template_use_case,
      },
      template_rotation_enabled: Boolean(options.template_rotation_enabled),
      template_rotation_seed: clean(options.template_rotation_seed) || null,
      template_rotation_pool_size: asPositiveInteger(options.template_rotation_pool_size, 0),
      template_rotation_candidate_ids: Array.isArray(options.template_rotation_candidate_ids)
        ? options.template_rotation_candidate_ids
        : [],
      template_rotation_candidate_languages: Array.isArray(options.template_rotation_candidate_languages)
        ? options.template_rotation_candidate_languages
        : [],
      template_rotation_selected_index: Number.isFinite(Number(options.template_rotation_selected_index))
        ? Number(options.template_rotation_selected_index)
        : null,
      template_rotation_requested_language: clean(options.template_rotation_requested_language) || null,
      selected_template_language: clean(options.selected_template_language || options.template_language) || null,
      rotation_language_mismatch_detected: Boolean(options.rotation_language_mismatch_detected),
      template_rotation_strategy: clean(options.template_rotation_strategy) || null,
      template_rotation_best_score: Number.isFinite(Number(options.template_rotation_best_score))
        ? Number(options.template_rotation_best_score)
        : null,
      template_rotation_min_score: Number.isFinite(Number(options.template_rotation_min_score))
        ? Number(options.template_rotation_min_score)
        : null,
      rotation_strategy: clean(options.template_rotation_strategy) || null,
      rotation_best_score: Number.isFinite(Number(options.template_rotation_best_score))
        ? Number(options.template_rotation_best_score)
        : null,
      rotation_min_score: Number.isFinite(Number(options.template_rotation_min_score))
        ? Number(options.template_rotation_min_score)
        : null,
      selected_template_id: clean(options.selected_template_id || options.template_id) || null,
      selected_template_source: clean(options.selected_template_source || options.template_source || "supabase") || "supabase",
      selected_template_language: clean(options.selected_template_language || options.template_language) || null,
      selected_template_use_case: clean(options.selected_template_use_case || options.template_use_case) || null,
      selected_template_stage_code: clean(options.selected_template_stage_code || options.template_stage_code) || null,
      template_fetch_limit: Number.isFinite(Number(options.template_fetch_limit)) ? Number(options.template_fetch_limit) : null,
      template_fetch_language_filter_applied: Boolean(options.template_fetch_language_filter_applied),
      template_fetch_use_case_filter_applied: options.template_fetch_use_case_filter_applied !== false,
      template_fetch_stage_filter_applied: Boolean(options.template_fetch_stage_filter_applied),
      template_fetch_fallback_used: Boolean(options.template_fetch_fallback_used),
      raw_template_count_before_language_filter: Number(options.raw_template_count_before_language_filter || 0),
      template_count_after_language_filter: Number(options.template_count_after_language_filter || 0),
    },
  };

  if (options.dry_run) {
    return {
      ok: true,
      dry_run: true,
      queued: false,
      queue_key,
      queue_id: queue_key,
      idempotency_key,
      payload,
    };
  }

  if (typeof deps.createSendQueueItem === "function") {
    return deps.createSendQueueItem(payload, { idempotency_key, queue_key });
  }

  const inserted = await insertSupabaseSendQueueRow(payload, deps);
  if (!inserted?.ok && inserted?.reason === "duplicate_blocked") {
    return {
      ok: false,
      reason_code: REASON_CODES.DUPLICATE_QUEUE_ITEM,
      reason: "duplicate_queue_item",
      duplicate: true,
      queue_key,
      idempotency_key,
      inserted,
    };
  }

  return {
    ok: true,
    dry_run: false,
    queued: true,
    queue_key,
    queue_id: inserted?.queue_id || queue_key,
    queue_row_id: inserted?.queue_row_id || null,
    idempotency_key,
    inserted,
  };
}

export function buildFeederDiagnostics(summary = {}) {
  const diagnostics = {
    ok: summary.ok !== false,
    dry_run: Boolean(summary.dry_run),
    source: summary.source || null,
    candidate_source: summary.candidate_source || summary.source || null,
    requested_limit: Number(summary.requested_limit || 0),
    effective_candidate_fetch_limit: Number(summary.effective_candidate_fetch_limit || 0),
    fetched_candidate_count: Number(summary.fetched_candidate_count || 0),
    scanned_count: Number(summary.scanned_count || 0),
    eligible_count: Number(summary.eligible_count || 0),
    queued_count: Number(summary.queued_count || 0),
    skipped_count: Number(summary.skipped_count || 0),
    routing_block_count: Number(summary.routing_block_count || 0),
    suppression_block_count: Number(summary.suppression_block_count || 0),
    contact_window_block_count: Number(summary.contact_window_block_count || 0),
    pending_prior_touch_block_count: Number(summary.pending_prior_touch_block_count || 0),
    duplicate_queue_block_count: Number(summary.duplicate_queue_block_count || 0),
    template_block_count: Number(summary.template_block_count || 0),
    no_template_count: Number(summary.no_template_count || 0),
    template_render_failed_count: Number(summary.template_render_failed_count || 0),
    schedule_spread_enabled: Boolean(summary.schedule_spread_enabled),
    first_scheduled_for: summary.first_scheduled_for || null,
    last_scheduled_for: summary.last_scheduled_for || null,
    error: clean(summary.error) || null,
    candidate_source_error: clean(summary.candidate_source_error) || null,
    available_hint: Array.isArray(summary.available_hint) ? summary.available_hint : undefined,
    selected_textgrid_market_counts: summary.selected_textgrid_market_counts || {},
    routing_tier_counts: summary.routing_tier_counts || {},
    sample_created_queue_items: Array.isArray(summary.sample_created_queue_items)
      ? summary.sample_created_queue_items.slice(0, 10)
      : [],
    sample_skips: Array.isArray(summary.sample_skips) ? summary.sample_skips.slice(0, 20) : [],
  };

  return diagnostics;
}

export async function runSupabaseCandidateFeeder(input = {}, deps = {}) {
  const now = input.now || new Date().toISOString();
  const limit = Math.max(1, Math.min(asPositiveInteger(input.limit, 25), 500));
  const scan_limit = Math.max(limit, Math.min(asPositiveInteger(input.scan_limit ?? input.candidate_fetch_limit, 500), 5000));

  const options = {
    dry_run: asBoolean(input.dry_run, false),
    limit,
    scan_limit,
    candidate_source: clean(input.candidate_source) || null,
    market: clean(input.market) || null,
    state: clean(input.state) || null,
    routing_safe_only: asBoolean(input.routing_safe_only, true),
    allow_phone_fallback: asBoolean(input.allow_phone_fallback, false),
    within_contact_window_now: asBoolean(input.within_contact_window_now, true),
    template_use_case: clean(input.template_use_case) || "ownership_check",
    touch_number: asPositiveInteger(input.touch_number, 1),
    campaign_session_id: clean(input.campaign_session_id) || `session-${now.slice(0, 10)}`,
    debug_templates: asBoolean(input.debug_templates, false),
    schedule_spread: asBoolean(input.schedule_spread, false),
    schedule_start_local: clean(input.schedule_start_local) || "09:00",
    schedule_end_local: clean(input.schedule_end_local) || "20:00",
    schedule_interval_seconds_min: asPositiveInteger(input.schedule_interval_seconds_min, 45),
    schedule_interval_seconds_max: asPositiveInteger(input.schedule_interval_seconds_max, 180),
    now,
  };

  const source = await getSupabaseFeederCandidates(options, deps);
  if (source?.ok === false) {
    return buildFeederDiagnostics({
      ok: false,
      dry_run: options.dry_run,
      source: source.source || DEFAULT_CANDIDATE_SOURCE,
      candidate_source: source.source || DEFAULT_CANDIDATE_SOURCE,
      requested_limit: options.limit,
      effective_candidate_fetch_limit: source.effective_fetch_limit || options.scan_limit,
      fetched_candidate_count: 0,
      scanned_count: 0,
      eligible_count: 0,
      queued_count: 0,
      skipped_count: 0,
      routing_block_count: 0,
      suppression_block_count: 0,
      contact_window_block_count: 0,
      pending_prior_touch_block_count: 0,
      duplicate_queue_block_count: 0,
      template_block_count: 0,
      no_template_count: 0,
      template_render_failed_count: 0,
      selected_textgrid_market_counts: {},
      routing_tier_counts: {},
      sample_created_queue_items: [],
      sample_skips: [],
      error: "CANDIDATE_SOURCE_UNAVAILABLE",
      candidate_source_error: source.candidate_source_error || "candidate source unavailable",
      available_hint: Array.isArray(source.available_hint) ? source.available_hint : [...CANDIDATE_SOURCE_AVAILABLE_HINT],
    });
  }

  const use_spread = options.schedule_spread && !options.within_contact_window_now;
  const spread_scheduler = use_spread
    ? createSpreadScheduler({
        now_iso: options.now,
        schedule_start_local: options.schedule_start_local,
        schedule_end_local: options.schedule_end_local,
        schedule_interval_seconds_min: options.schedule_interval_seconds_min,
        schedule_interval_seconds_max: options.schedule_interval_seconds_max,
      })
    : null;

  const summary = {
    ok: true,
    dry_run: options.dry_run,
    source: source.source,
    candidate_source: source.source,
    requested_limit: options.limit,
    effective_candidate_fetch_limit: source.effective_fetch_limit || options.scan_limit,
    fetched_candidate_count: source.scanned_count,
    scanned_count: source.scanned_count,
    eligible_count: 0,
    queued_count: 0,
    skipped_count: 0,
    routing_block_count: 0,
    suppression_block_count: 0,
    contact_window_block_count: 0,
    pending_prior_touch_block_count: 0,
    duplicate_queue_block_count: 0,
    template_block_count: 0,
    no_template_count: 0,
    template_render_failed_count: 0,
    schedule_spread_enabled: use_spread,
    first_scheduled_for: null,
    last_scheduled_for: null,
    selected_textgrid_market_counts: {},
    routing_tier_counts: {},
    sample_created_queue_items: [],
    sample_skips: [],
  };

  for (const candidate of source.rows) {
    if (summary.queued_count >= options.limit) {
      summary.skipped_count += 1;
      summary.sample_skips.push({
        reason_code: REASON_CODES.CAMPAIGN_LIMIT_REACHED,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
      });
      continue;
    }

    candidate.touch_number = options.touch_number;
    candidate.template_use_case = options.template_use_case;
    candidate.campaign_session_id = options.campaign_session_id;

    const _preview = options.dry_run
      ? { candidate_preview: buildCandidateNormalizedPreview(candidate.raw, candidate) }
      : {};

    const eligibility = await evaluateCandidateEligibility(candidate, options, deps);
    if (!eligibility.ok) {
      summary.skipped_count += 1;
      const counter = mapReasonToDiagnosticCounter(eligibility.reason_code);
      if (counter) summary[counter] += 1;
      summary.sample_skips.push({
        reason_code: eligibility.reason_code,
        reason: eligibility.reason,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
        ..._preview,
      });
      continue;
    }

    summary.eligible_count += 1;

    const routing = await chooseTextgridNumber(candidate, options, deps);
    if (!routing.ok) {
      summary.skipped_count += 1;
      summary.routing_block_count += 1;
      summary.sample_skips.push({
        reason_code: routing.reason_code,
        reason: routing.routing_block_reason,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
        routing_allowed: false,
        routing_tier: routing.routing_tier,
        selection_reason: routing.selection_reason,
        routing_rule_name: routing.routing_rule_name,
        selected_textgrid_market: routing.selected_textgrid_market,
        selected_textgrid_number: routing.selected_textgrid_number,
        seller_market: routing.seller_market || candidate.market,
        seller_state: routing.seller_state || candidate.state,
        routing_block_reason: routing.routing_block_reason,
        ..._preview,
      });
      continue;
    }

    const rendered = await renderOutboundTemplate(candidate, options, deps);
    if (!rendered.ok) {
      summary.skipped_count += 1;
      summary.template_block_count += 1;
      if (rendered.reason_code === REASON_CODES.NO_TEMPLATE) {
        summary.no_template_count += 1;
      }
      if (rendered.reason_code === REASON_CODES.TEMPLATE_RENDER_FAILED) {
        summary.template_render_failed_count += 1;
      }
      summary.sample_skips.push({
        reason_code: rendered.reason_code,
        reason: rendered.reason,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
        ...(options.dry_run && options.debug_templates
          ? {
              template_lookup_use_case: candidate.template_lookup_use_case || candidate.template_use_case || null,
              template_id: rendered.template?.template_id || rendered.template?.id || null,
              podio_template_id: rendered.template?.podio_template_id || null,
              template_source: "sms_templates",
              template_name: rendered.template?.template_name || null,
              stage_code: rendered.template?.stage_code || candidate.stage_code || null,
              stage_label: rendered.template?.stage_label || candidate.stage_label || null,
              touch_number: candidate.touch_number,
              language: rendered.template?.language || candidate.best_language || candidate.language || "English",
              render_error_message: rendered.render_error_message || rendered.reason || null,
              missing_variables: rendered.missing_variables || [],
              variable_payload_preview: rendered.variable_payload_preview || {},
              selected_template_preview: rendered.selected_template_preview || null,
              eligible_template_count: Number(rendered.template_rotation?.eligible_template_count || 0),
              rotation_pool_size: Number(rendered.template_rotation?.rotation_pool_size || 0),
              rotation_strategy: clean(rendered.template_rotation?.rotation_strategy) || null,
              rotation_best_score: Number.isFinite(Number(rendered.template_rotation?.rotation_best_score))
                ? Number(rendered.template_rotation?.rotation_best_score)
                : null,
              rotation_min_score: Number.isFinite(Number(rendered.template_rotation?.rotation_min_score))
                ? Number(rendered.template_rotation?.rotation_min_score)
                : null,
              rotation_candidate_template_ids: rendered.template_rotation?.rotation_candidate_template_ids || [],
              rotation_candidate_languages: rendered.template_rotation?.rotation_candidate_languages || [],
              requested_language: rendered.template_rotation?.requested_language || null,
              selected_template_language: rendered.template_rotation?.selected_template_language || null,
              rotation_language_mismatch_detected: Boolean(rendered.template_rotation?.rotation_language_mismatch_detected),
              selected_template_id:
                rendered.template_rotation?.selected_template_id || rendered.template?.template_id || rendered.template?.id || null,
              rotation_seed: rendered.template_rotation?.seed || null,
              excluded_recent_template_ids: rendered.template_rotation?.excluded_recent_template_ids || [],
              template_fetch_limit: rendered.template_rotation?.template_fetch_limit ?? null,
              template_fetch_language_filter_applied: Boolean(rendered.template_rotation?.template_fetch_language_filter_applied),
              template_fetch_use_case_filter_applied: rendered.template_rotation?.template_fetch_use_case_filter_applied !== false,
              template_fetch_stage_filter_applied: Boolean(rendered.template_rotation?.template_fetch_stage_filter_applied),
              template_fetch_fallback_used: Boolean(rendered.template_rotation?.template_fetch_fallback_used),
              raw_template_count_before_language_filter: Number(rendered.template_rotation?.raw_template_count_before_language_filter || 0),
              template_count_after_language_filter: Number(rendered.template_rotation?.template_count_after_language_filter || 0),
            }
          : {}),
        ..._preview,
      });
      continue;
    }

    const scheduled_for = spread_scheduler
      ? spread_scheduler.nextScheduledFor(candidate)
      : eligibility.scheduled_for;

    const queue_result = await createSendQueueItem(
      candidate,
      {
        ...options,
        scheduled_for,
        rendered_message_body: rendered.rendered_message_body,
        template_id: rendered.template?.template_id || rendered.template?.item_id || null,
        template_source: rendered.template?.source || "supabase",
        template_use_case: rendered.template_use_case,
        template_name: rendered.template?.template_name || null,
        template_stage_code: rendered.stage_code || rendered.template?.stage_code || null,
        template_language: rendered.language || rendered.template?.language || null,
        template_rotation_enabled: Boolean(rendered.template_rotation?.enabled),
        template_rotation_seed: rendered.template_rotation?.seed || null,
        template_rotation_pool_size: rendered.template_rotation?.rotation_pool_size || 0,
        template_rotation_candidate_ids: rendered.template_rotation?.rotation_candidate_template_ids || [],
        template_rotation_candidate_languages: rendered.template_rotation?.rotation_candidate_languages || [],
        template_rotation_selected_index: rendered.template_rotation?.selected_index ?? null,
        template_rotation_requested_language: rendered.template_rotation?.requested_language || null,
        selected_template_language: rendered.template_rotation?.selected_template_language || rendered.language || rendered.template?.language || null,
        rotation_language_mismatch_detected: Boolean(rendered.template_rotation?.rotation_language_mismatch_detected),
        template_rotation_strategy: rendered.template_rotation?.rotation_strategy || null,
        template_rotation_best_score: rendered.template_rotation?.rotation_best_score ?? null,
        template_rotation_min_score: rendered.template_rotation?.rotation_min_score ?? null,
        selected_template_id: rendered.template_rotation?.selected_template_id || rendered.template?.template_id || rendered.template?.id || null,
        selected_template_source: rendered.template?.source || "sms_templates",
        selected_template_language: rendered.language || rendered.template?.language || null,
        template_fetch_limit: rendered.template_rotation?.template_fetch_limit ?? null,
        template_fetch_language_filter_applied: Boolean(rendered.template_rotation?.template_fetch_language_filter_applied),
        template_fetch_use_case_filter_applied: rendered.template_rotation?.template_fetch_use_case_filter_applied !== false,
        template_fetch_stage_filter_applied: Boolean(rendered.template_rotation?.template_fetch_stage_filter_applied),
        template_fetch_fallback_used: Boolean(rendered.template_rotation?.template_fetch_fallback_used),
        raw_template_count_before_language_filter: Number(rendered.template_rotation?.raw_template_count_before_language_filter || 0),
        template_count_after_language_filter: Number(rendered.template_rotation?.template_count_after_language_filter || 0),
        selected_template_use_case: rendered.template_use_case,
        selected_template_stage_code: rendered.stage_code || rendered.template?.stage_code || null,
        selected_textgrid_number_id: routing.selected.id,
        selected_textgrid_number: routing.selected.phone_number,
        selected_textgrid_market: routing.selected.market,
        routing_tier: routing.routing_tier,
        selection_reason: routing.selection_reason,
        routing_allowed: routing.routing_allowed,
        routing_block_reason: routing.routing_block_reason,
      },
      deps
    );

    if (!queue_result.ok) {
      summary.skipped_count += 1;
      const reason_code = queue_result.reason_code || REASON_CODES.DUPLICATE_QUEUE_ITEM;
      const counter = mapReasonToDiagnosticCounter(reason_code);
      if (counter) summary[counter] += 1;
      summary.sample_skips.push({
        reason_code,
        reason: queue_result.reason || "queue_create_failed",
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
        ..._preview,
      });
      continue;
    }

    summary.queued_count += 1;
    if (!summary.first_scheduled_for) summary.first_scheduled_for = scheduled_for;
    summary.last_scheduled_for = scheduled_for;
    summary.selected_textgrid_market_counts[routing.selected.market || "unknown"] =
      Number(summary.selected_textgrid_market_counts[routing.selected.market || "unknown"] || 0) + 1;
    summary.routing_tier_counts[routing.routing_tier || "unknown"] =
      Number(summary.routing_tier_counts[routing.routing_tier || "unknown"] || 0) + 1;

    summary.sample_created_queue_items.push({
      queue_row_id: queue_result.queue_row_id || null,
      queue_key: queue_result.queue_key,
      scheduled_for: scheduled_for || null,
      master_owner_id: candidate.master_owner_id,
      property_id: candidate.property_id,
      phone_masked: maskPhone(candidate.canonical_e164),
      selected_textgrid_number_id: routing.selected.id,
      selected_textgrid_number: routing.selected.phone_number,
      selected_textgrid_market: routing.selected.market,
      seller_market: candidate.market,
      seller_state: candidate.state,
      routing_tier: routing.routing_tier,
      selection_reason: routing.selection_reason,
      routing_rule_name: routing.routing_rule_name,
      routing_allowed: true,
      routing_block_reason: null,
      template_use_case: rendered.template_use_case,
      template_id: rendered.template?.template_id || rendered.template?.id || null,
      template_name: rendered.template?.template_name || null,
      stage_code: rendered.stage_code || rendered.template?.stage_code || null,
      language: rendered.language || rendered.template?.language || null,
      template_rotation_enabled: Boolean(rendered.template_rotation?.enabled),
      template_rotation_pool_size: Number(rendered.template_rotation?.rotation_pool_size || 0),
      template_rotation_selected_index: Number(rendered.template_rotation?.selected_index ?? -1),
      template_rotation_requested_language: clean(rendered.template_rotation?.requested_language) || null,
      selected_template_language: clean(rendered.template_rotation?.selected_template_language || rendered.language || rendered.template?.language) || null,
      rotation_language_mismatch_detected: Boolean(rendered.template_rotation?.rotation_language_mismatch_detected),
      rotation_candidate_languages: rendered.template_rotation?.rotation_candidate_languages || [],
      template_rotation_strategy: clean(rendered.template_rotation?.rotation_strategy) || null,
      template_rotation_best_score: Number.isFinite(Number(rendered.template_rotation?.rotation_best_score))
        ? Number(rendered.template_rotation?.rotation_best_score)
        : null,
      template_rotation_min_score: Number.isFinite(Number(rendered.template_rotation?.rotation_min_score))
        ? Number(rendered.template_rotation?.rotation_min_score)
        : null,
      template_fetch_limit: rendered.template_rotation?.template_fetch_limit ?? null,
      template_fetch_language_filter_applied: Boolean(rendered.template_rotation?.template_fetch_language_filter_applied),
      template_fetch_use_case_filter_applied: rendered.template_rotation?.template_fetch_use_case_filter_applied !== false,
      template_fetch_stage_filter_applied: Boolean(rendered.template_rotation?.template_fetch_stage_filter_applied),
      template_fetch_fallback_used: Boolean(rendered.template_rotation?.template_fetch_fallback_used),
      raw_template_count_before_language_filter: Number(rendered.template_rotation?.raw_template_count_before_language_filter || 0),
      template_count_after_language_filter: Number(rendered.template_rotation?.template_count_after_language_filter || 0),
      rendered_message_preview: clean(rendered.rendered_message_body).slice(0, 160),
      dry_run: options.dry_run,
    });
  }

  const diagnostics = buildFeederDiagnostics(summary);
  logger.info("outbound.supabase_feeder_completed", {
    source: diagnostics.source,
    dry_run: diagnostics.dry_run,
    scanned_count: diagnostics.scanned_count,
    eligible_count: diagnostics.eligible_count,
    queued_count: diagnostics.queued_count,
    skipped_count: diagnostics.skipped_count,
  });

  return diagnostics;
}

export { REASON_CODES };
