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
  const phone_id =
    pick(row.phone_id, row.best_phone_id, row.phone_item_id) || null;
  const primary_prospect_id = pick(row.primary_prospect_id) || null;
  const canonical_prospect_id = pick(row.canonical_prospect_id) || null;

  const canonical_e164 =
    normalizePhone(
      pick(
        row.best_phone_e164,
        row.canonical_e164,
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

  return {
    raw: row,
    master_owner_id,
    property_id,
    property_export_id,
    phone_id,
    primary_prospect_id,
    canonical_prospect_id,
    canonical_e164,
    market,
    state,
    state_code: state,
    owner_display_name,
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
    normalized_master_owner_id: candidate.master_owner_id,
    normalized_property_id: candidate.property_id,
    normalized_phone_id: candidate.phone_id,
    normalized_phone_e164: candidate.canonical_e164 || null,
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
  const owner_display_name = clean(
    pick(candidate.owner_display_name, candidate.raw?.display_name, candidate.raw?.owner_display_name)
  );
  if (!owner_display_name || isLikelyCorporateName(owner_display_name)) return "";
  const parts = owner_display_name.split(/\s+/).filter(Boolean);
  return clean(parts[0]);
}

function buildTemplateVariablePayload(candidate = {}) {
  const owner_display_name = clean(
    pick(candidate.raw?.display_name, candidate.owner_display_name, candidate.raw?.owner_display_name)
  );
  const language = clean(pick(candidate.best_language, candidate.language, "English")) || "English";
  const payload = {
    seller_first_name: deriveSellerFirstName(candidate),
    owner_display_name,
    owner_name: owner_display_name || candidate.seller_name || "",
    property_address: clean(candidate.property_address_full),
    property_city: clean(candidate.property_city),
    property_state: clean(candidate.property_state || candidate.state),
    property_zip: clean(candidate.property_zip),
    offer_price: formatCurrency(candidate.cash_offer),
    cash_offer: candidate.cash_offer,
    agent_name: clean(pick(candidate.agent_persona, candidate.raw?.agent_name, "Alex")) || "Alex",
    language,
    market: clean(candidate.market),
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
  const effective_fetch_limit = Math.min(Math.max(requestedLimit * 5, 10), 100);
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
  if (!candidate.phone_id) {
    return { ok: false, reason_code: REASON_CODES.NO_PHONE, reason: "missing_phone_id" };
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

  let templates = [];
  if (typeof deps.fetchSmsTemplates === "function") {
    templates = await deps.fetchSmsTemplates(selector, candidate, options);
  } else {
    const supabase = getSupabase(deps);
    const { data, error } = await supabase
      .from("sms_templates")
      .select("*")
      .eq("is_active", true)
      .eq("use_case", selector.use_case)
      .limit(200);

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

    templates = Array.isArray(data) ? data : [];

    if (!templates.length) {
      const fallback_any_use_case = await supabase
        .from("sms_templates")
        .select("*")
        .eq("is_active", true)
        .limit(200);

      templates = Array.isArray(fallback_any_use_case?.data) ? fallback_any_use_case.data : [];
    }
  }

  if (!templates.length) {
    return {
      ok: false,
      reason_code: REASON_CODES.NO_TEMPLATE,
      reason: "no_template_found",
      template: null,
      rendered_message_body: null,
      missing_variables: [],
      variable_payload_preview: buildTemplateVariablePayload(candidate),
      selected_template_preview: null,
    };
  }

  const selected_template = [...templates].sort((left, right) => sortTemplateCandidates(left, right, selector))[0] || null;
  const selected_template_with_source = selected_template
    ? { ...selected_template, source: "sms_templates" }
    : null;
  const source_body = clean(pick(selected_template?.template_body, selected_template?.english_translation));

  if (!source_body) {
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
    };
  }

  const variable_payload = buildTemplateVariablePayload(candidate);
  const rendered = applyTemplatePlaceholders(source_body, variable_payload);
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
    stage_code: selected_template?.stage_code || selector.stage_code,
    stage_label: selected_template?.stage_label || selector.stage_label,
    language: selected_template?.language || selector.preferred_language,
  };
}

export async function createSendQueueItem(candidate = {}, options = {}, deps = {}) {
  const idempotency_key = buildIdempotencyKey({
    master_owner_id: candidate.master_owner_id,
    property_id: candidate.property_id,
    phone_id: candidate.phone_id,
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
  const scan_limit = Math.max(limit, Math.min(asPositiveInteger(input.scan_limit, 500), 5000));

  const options = {
    dry_run: asBoolean(input.dry_run, false),
    limit,
    scan_limit,
    candidate_source: clean(input.candidate_source) || null,
    market: clean(input.market) || null,
    state: clean(input.state) || null,
    routing_safe_only: asBoolean(input.routing_safe_only, true),
    within_contact_window_now: asBoolean(input.within_contact_window_now, true),
    template_use_case: clean(input.template_use_case) || "ownership_check",
    touch_number: asPositiveInteger(input.touch_number, 1),
    campaign_session_id: clean(input.campaign_session_id) || `session-${now.slice(0, 10)}`,
    debug_templates: asBoolean(input.debug_templates, false),
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
            }
          : {}),
        ..._preview,
      });
      continue;
    }

    const queue_result = await createSendQueueItem(
      candidate,
      {
        ...options,
        scheduled_for: eligibility.scheduled_for,
        rendered_message_body: rendered.rendered_message_body,
        template_id: rendered.template?.template_id || rendered.template?.item_id || null,
        template_source: rendered.template?.source || "supabase",
        template_use_case: rendered.template_use_case,
        template_name: rendered.template?.template_name || null,
        template_stage_code: rendered.stage_code || rendered.template?.stage_code || null,
        template_language: rendered.language || rendered.template?.language || null,
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
    summary.selected_textgrid_market_counts[routing.selected.market || "unknown"] =
      Number(summary.selected_textgrid_market_counts[routing.selected.market || "unknown"] || 0) + 1;
    summary.routing_tier_counts[routing.routing_tier || "unknown"] =
      Number(summary.routing_tier_counts[routing.routing_tier || "unknown"] || 0) + 1;

    summary.sample_created_queue_items.push({
      queue_row_id: queue_result.queue_row_id || null,
      queue_key: queue_result.queue_key,
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
