import APP_IDS from "@/lib/config/app-ids.js";
import {
  MASTER_OWNER_FIELDS,
  findMasterOwnerItems,
  findMasterOwnerItemsByView,
  findSmsEligibleMasterOwnerItems,
  findMasterOwnerBySellerId,
  getMasterOwnerItem,
  getMasterOwnerView,
  listMasterOwnerViews,
} from "@/lib/podio/apps/master-owners.js";
import { findPropertyItems } from "@/lib/podio/apps/properties.js";
import { PHONE_FIELDS } from "@/lib/podio/apps/phone-numbers.js";
import { TEXTGRID_NUMBER_FIELDS } from "@/lib/podio/apps/textgrid-numbers.js";
import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";
import { loadRecentTemplates } from "@/lib/domain/context/load-recent-templates.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { renderTemplate } from "@/lib/domain/templates/render-template.js";
import { validateActivePhone } from "@/lib/domain/compliance/validate-active-phone.js";
import {
  deriveOutreachSuppressionSignals,
  shouldSuppressOutreach,
} from "@/lib/domain/compliance/should-suppress-outreach.js";
import { LIFECYCLE_STAGES, STAGES } from "@/lib/config/stages.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import {
  canonicalStageForUseCase,
  followUpUseCaseForStage,
  inferCanonicalUseCaseFromOutboundText,
  normalizeSellerFlowUseCase,
  preferredAgentTypeForSellerFlow,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import {
  chooseTextgridNumber,
  loadUsableTextgridNumbers,
} from "@/lib/domain/routing/choose-textgrid-number.js";
import { resolveMarketSendingProfile } from "@/lib/config/market-sending-zones.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import {
  resolveQueueSchedule,
  resolveSchedulingContactWindow,
} from "@/lib/domain/queue/queue-schedule.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import {
  fetchAllItems,
  getFieldValues,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getAppReferenceIds,
  getItem,
  getNumberValue,
  getPodioRetryAfterSeconds,
  getPhoneValue,
  isPodioRateLimitError,
  getTextValue,
  normalizeLanguage,
} from "@/lib/providers/podio.js";
import { parseMessageEventMetadata } from "@/lib/domain/events/message-event-metadata.js";
import { child, info, warn } from "@/lib/logging/logger.js";

const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_SCAN_LIMIT = 150;
const SAFE_TEST_LIMIT = 3;
const SAFE_TEST_SCAN_LIMIT = 10;
const MAX_HISTORY_ITEMS = 100;

const logger = child({
  module: "domain.master_owners.run_outbound_feeder",
});

const TERMINAL_CONTACT_STATUSES = new Set([
  "under contract",
  "dead",
  "in escrow",
]);

const FOLLOW_UP_CONTACT_STATUSES = new Set([
  "contacted",
  "engaged",
  "offer sent",
  "negotiating",
]);

const FOLLOW_UP_CONTACT_STATUS_2 = new Set([
  "sent",
  "received",
  "follow-up scheduled",
]);

const QUEUE_DUPLICATE_PENDING_STATUSES = new Set([
  "queued",
  "sending",
]);

const QUEUE_DUPLICATE_RECENT_STATUSES = new Set([
  "queued",
  "sending",
  "sent",
]);

// ── First-touch guardrail constants ──────────────────────────────────────────
// These use_cases must NEVER appear in a Stage-1 cold outbound to a lead whose
// Master Owner contact_status is blank (i.e. no CRM-confirmed engagement yet).
const FORBIDDEN_FIRST_TOUCH_USE_CASES = new Set([
  "asking_price",
  "asking_price_follow_up",
  "price_works_confirm_basics",
  "price_works_confirm_basics_follow_up",
  "price_high_condition_probe",
  "price_high_condition_probe_follow_up",
  "creative_probe",
  "creative_followup",
  "offer_reveal_cash",
  "offer_reveal_cash_follow_up",
  "offer_reveal_lease_option",
  "offer_reveal_subject_to",
  "offer_reveal_novation",
  "mf_offer_reveal",
  "close_handoff",
  "asks_contract",
  "contract_sent",
  "justify_price",
  "narrow_range",
  "ask_timeline",
  "ask_condition_clarifier",
  "reengagement",
]);

const FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES = new Set([
  "Contract",
  "Title",
  "Closing",
  "Disposition",
  "Post-Close",
]);

// Variant groups that are allowed for first-touch Stage-1 cold ownership outbounds.
// Follow-up variant groups are intentionally excluded: a cold lead has never been
// contacted before so no follow-up framing is appropriate as a first message.
const FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS = new Set([
  "Stage 1 — Ownership Confirmation",
  "Stage 1 — Ownership Check",
  "Stage 1 Ownership Check",
  "Stage 1 Ownership Confirmation",
]);

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function toViewSelector(source_view_id = null, source_view_name = null) {
  if (source_view_id !== null && source_view_id !== undefined && source_view_id !== "") {
    return String(source_view_id).trim();
  }

  const by_name = clean(source_view_name);
  return by_name || null;
}

function nowIso() {
  return new Date().toISOString();
}

function toPositiveInteger(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function isTimeoutError(error) {
  const message =
    error?.message ||
    error?.cause?.message ||
    "";

  return (
    /timeout of \d+ms exceeded/i.test(message) ||
    /ETIMEDOUT/i.test(message) ||
    /The server took too long to respond/i.test(message)
  );
}

function serializeFeederError(error) {
  const root = error?.cause || error;

  return {
    name: root?.name || error?.name || "Error",
    message: root?.message || error?.message || "Unknown error",
    timeout: isTimeoutError(error),
    stage: error?.feeder_stage || null,
    duration_ms: error?.feeder_duration_ms ?? null,
    meta: error?.feeder_meta || null,
    podio_status: root?.status ?? null,
    podio_path: root?.path ?? null,
  };
}

function annotateFeederError(error, stage, duration_ms, meta = {}) {
  if (!error || typeof error !== "object") return error;

  if (!error.feeder_stage) error.feeder_stage = stage;
  if (error.feeder_duration_ms === undefined) {
    error.feeder_duration_ms = duration_ms;
  }
  if (!error.feeder_meta) error.feeder_meta = meta;

  return error;
}

async function timedStage(log, event, meta = {}, fn) {
  const started_at = Date.now();
  log.info(`${event}.started`, meta);

  try {
    const result = await fn();
    log.info(`${event}.completed`, {
      ...meta,
      duration_ms: Date.now() - started_at,
    });
    return result;
  } catch (error) {
    const duration_ms = Date.now() - started_at;
    const annotated = annotateFeederError(error, event, duration_ms, meta);

    log.warn(`${event}.failed`, {
      ...meta,
      duration_ms,
      error: serializeFeederError(annotated),
    });

    throw annotated;
  }
}

function createRunState({
  dry_run = false,
  test_mode = false,
  page_size = 50,
  source = null,
} = {}) {
  return {
    dry_run,
    test_mode,
    page_size,
    source,
    item_cache: new Map(),
    owners_by_id: new Map(),
    owner_history_by_id: new Map(),
    textgrid_number_pool: null,
  };
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function newestIso(values = []) {
  let winner = null;
  let winnerTs = null;

  for (const value of values) {
    const ts = toTimestamp(value);
    if (ts === null) continue;
    if (winnerTs === null || ts > winnerTs) {
      winner = value;
      winnerTs = ts;
    }
  }

  return winner;
}

function summarizeDate(value) {
  return value || null;
}

function summarizeSource(source = null, overrides = {}) {
  if (!source) {
    return {
      type:
        overrides.type ||
        (overrides.requested_view_id || overrides.requested_view_name ? "view" : "recent_items"),
      app_id: APP_IDS.master_owners,
      view_id: null,
      view_name: null,
      requested_view_id: overrides.requested_view_id ?? null,
      requested_view_name: overrides.requested_view_name ?? null,
    };
  }

  return {
    type: source.type || "recent_items",
    app_id: APP_IDS.master_owners,
    view_id: source.view_id ?? null,
    view_name: source.view_name ?? null,
    requested_view_id: source.requested_view_id ?? null,
    requested_view_name: source.requested_view_name ?? null,
  };
}

function countReasons(results = []) {
  const counts = new Map();

  for (const result of results) {
    const reason = result?.reason || "unknown";
    counts.set(reason, (counts.get(reason) || 0) + 1);
  }

  return [...counts.entries()]
    .map(([reason, count]) => ({ reason, count }))
    .sort((left, right) => right.count - left.count);
}

function summarizeMasterOwner(owner_item) {
  return {
    item_id: owner_item?.item_id ?? null,
    seller_id: getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, ""),
    owner_name:
      getTextValue(owner_item, MASTER_OWNER_FIELDS.owner_full_name, "") ||
      owner_item?.title ||
      "",
    contact_status: getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status, null),
    contact_status_2: getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status_2, null),
    sms_eligible: getCategoryValue(owner_item, MASTER_OWNER_FIELDS.sms_eligible, null),
  };
}

function stripHtml(value) {
  return clean(value).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function parseSellerIdLocation(value) {
  const raw = stripHtml(value);
  if (!raw) {
    return {
      property_address: "",
      property_city: "",
      property_state: "",
    };
  }

  const location_segment = raw
    .split("~")
    .map((segment) => clean(segment))
    .find((segment) => segment.split("|").filter(Boolean).length >= 4);

  if (!location_segment) {
    return {
      property_address: "",
      property_city: "",
      property_state: "",
    };
  }

  const [property_address = "", property_city = "", property_state = ""] =
    location_segment
      .split("|")
      .map((segment) => clean(segment));

  return {
    property_address,
    property_city,
    property_state,
  };
}

// Expand common street-type abbreviations to their canonical long form.
// Applied case-insensitively on word boundaries so partial words (e.g. "Drive"
// already spelled out, "State") are not affected.
// Use (?!\w) instead of \b after the optional dot so patterns like "Ave." at end-of-string
// are consumed correctly (a trailing dot is non-word, so \b would not match after it).
const STREET_TYPE_EXPANSIONS = [
  [/\bSt\.?(?!\w)/gi, "Street"],
  [/\bAve\.?(?!\w)/gi, "Avenue"],
  [/\bBlvd\.?(?!\w)/gi, "Boulevard"],
  [/\bDr\.?(?!\w)/gi, "Drive"],
  [/\bRd\.?(?!\w)/gi, "Road"],
  [/\bLn\.?(?!\w)/gi, "Lane"],
  [/\bCt\.?(?!\w)/gi, "Court"],
  [/\bCir\.?(?!\w)/gi, "Circle"],
  [/\bPkwy\.?(?!\w)/gi, "Parkway"],
  [/\bPl\.?(?!\w)/gi, "Place"],
  [/\bTrl\.?(?!\w)/gi, "Trail"],
  [/\bHwy\.?(?!\w)/gi, "Highway"],
];

function normalizeStreetAddress(address) {
  let result = String(address ?? "")
    .trim()
    .replace(/\s+/g, " ");
  for (const [pattern, replacement] of STREET_TYPE_EXPANSIONS) {
    result = result.replace(pattern, replacement);
  }
  return result.trim().replace(/\s+/g, " ");
}

// Returns the cleaned raw address and a de-duped array of lower-cased variants
// (original + abbreviation-expanded) to use when post-filtering Podio results.
function addressLookupVariants(raw_address) {
  const cleaned = String(raw_address ?? "")
    .trim()
    .replace(/\s+/g, " ");
  const expanded = normalizeStreetAddress(cleaned);
  const variants = [lower(cleaned)];
  if (lower(expanded) !== lower(cleaned)) variants.push(lower(expanded));
  return { cleaned, expanded, variants };
}

// Builds a minimal synthetic property-like object from a parsed seller_id when no
// real Podio property item could be resolved. Returned only when the seller_id yields
// a usable street address; null otherwise.
//
// The object is shaped like a Podio item (fields array) so that getTextValue,
// getCategoryValue, and deriveContextSummary all read it without modification.
// item_id is intentionally null — buildSendQueueItem already handles null property_id.
function buildSyntheticPropertyFromSellerId(owner_item) {
  const seller_id_raw =
    getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "") || null;
  const location = parseSellerIdLocation(seller_id_raw ?? "");
  const property_address = clean(location.property_address);
  if (!property_address) return null;

  const property_city = clean(location.property_city);
  const property_state = clean(location.property_state);

  return {
    item_id: null,
    title: property_address,
    synthetic: true,
    source: "seller_id_fallback",
    // Internal metadata for logging — not consumed by Podio helpers.
    _seller_id: seller_id_raw,
    _synthetic_property_address: property_address,
    _synthetic_property_city: property_city,
    _synthetic_property_state: property_state,
    // Podio-item-like fields array so getTextValue / deriveContextSummary
    // resolve the three core address placeholders correctly.
    fields: [
      {
        external_id: "property-address",
        values: [{ value: property_address }],
      },
      ...(property_city
        ? [{ external_id: "city", values: [{ value: property_city }] }]
        : []),
      ...(property_state
        ? [{ external_id: "state", values: [{ value: property_state }] }]
        : []),
    ],
  };
}

function maskOwnerName(value) {
  const text = stripHtml(value);
  if (!text) return null;

  const parts = text.split(/\s+/).filter(Boolean);
  if (!parts.length) return null;

  return parts
    .slice(0, 2)
    .map((part) => `${part[0] || ""}***`)
    .join(" ");
}

function classifyCloseness(reason, full_result = null) {
  if (full_result?.ok && !full_result?.skipped) return 100;
  if (reason === "template_not_found" || reason === "textgrid_number_not_found") return 90;
  if (reason === "recent_contact_within_suppression_window") return 80;
  if (reason === "duplicate_pending_queue_item") return 78;
  if (reason === "duplicate_within_suppression_window") return 76;
  if (reason === "no_usable_phone") return 60;
  if (reason === "next_follow_up_not_due") return 40;
  if (reason === "owner_has_contract_or_closing") return 30;
  if (reason === "terminal_contact_status" || reason === "contact_status_dnc") return 20;
  if (reason === "sms_not_eligible") return 10;
  return 50;
}

function buildOwnerErrorResult(owner_item, error, evaluation_phase = "unknown") {
  return {
    ok: false,
    skipped: true,
    reason: isTimeoutError(error)
      ? "owner_evaluation_timeout"
      : "owner_evaluation_failed",
    evaluation_phase,
    owner: summarizeMasterOwner(owner_item),
    diagnostics: serializeFeederError(error),
  };
}

async function safeGetItem(
  item_id,
  {
    runtime = null,
    log = logger,
    call_name = "master_owner_feeder.podio_get_item",
    meta = {},
  } = {}
) {
  if (!item_id) return null;

  const cache_key = String(item_id);
  if (runtime?.item_cache?.has(cache_key)) {
    return runtime.item_cache.get(cache_key);
  }

  try {
    const item = await timedStage(
      log,
      call_name,
      {
        item_id,
        ...meta,
      },
      () => getItem(item_id)
    );

    if (runtime?.item_cache) {
      runtime.item_cache.set(cache_key, item || null);
    }

    return item;
  } catch (error) {
    if (isTimeoutError(error)) {
      throw error;
    }

    if (runtime?.item_cache) {
      runtime.item_cache.set(cache_key, null);
    }

    return null;
  }
}

function buildEmptyHistory() {
  return {
    queue_items: [],
    outbound_events: [],
  };
}

function mapFollowUpCadenceToDays(value) {
  const raw = lower(value);

  if (raw === "passive") return 14;
  if (raw === "aggressive") return 3;
  return 7;
}

function mapPriorityTierBonus(value) {
  const raw = clean(value).toUpperCase();

  if (raw === "TIER_1") return 300;
  if (raw === "TIER_2") return 200;
  if (raw === "TIER_3") return 100;
  return 0;
}

function deriveOwnerStageHint(owner_item) {
  const contact_status = lower(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status, null)
  );
  const contact_status_2 = lower(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status_2, null)
  );
  const has_offer = Boolean(
    getFirstAppReferenceId(owner_item, MASTER_OWNER_FIELDS.offer, null)
  );

  if (has_offer || ["offer sent", "negotiating"].includes(contact_status)) {
    return "Offer";
  }

  if (
    FOLLOW_UP_CONTACT_STATUSES.has(contact_status) ||
    FOLLOW_UP_CONTACT_STATUS_2.has(contact_status_2)
  ) {
    return "Follow-Up";
  }

  return "Ownership";
}

function deriveOwnerEmotion(owner_item) {
  const urgency = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.urgency_score, 0) || 0
  );
  const financial_pressure = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.financial_pressure_score, 0) || 0
  );
  const tax_delinquent = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_tax_delinquent_count, 0) || 0
  );
  const lien_count = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_lien_count, 0) || 0
  );

  if (
    urgency >= 80 ||
    financial_pressure >= 80 ||
    tax_delinquent > 0 ||
    lien_count > 0
  ) {
    return "motivated";
  }

  return "curious";
}

function deriveTemplatePrimaryCategory(property_item, owner_item, fallback = "Residential") {
  const property_class = getCategoryValue(property_item, "property-class", null);
  if (property_class) return property_class;

  const majority = clean(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.property_type_majority, null)
  ).toUpperCase();

  if (majority === "VACANT LAND") return "Vacant";
  return fallback || "Residential";
}

function deriveTemplateSecondaryCategory(property_item, owner_item, fallback = null) {
  return fallback;
}

function deriveSequencePosition(stage, owner_touch_count) {
  const touch_number = Math.max(1, Number(owner_touch_count || 0) + 1);

  if (stage === "Offer") {
    if (touch_number <= 1) return "V1";
    if (touch_number === 2) return "V2";
    return "V3";
  }

  if (touch_number <= 1) return "1st Touch";
  if (touch_number === 2) return "2nd Touch";
  if (touch_number === 3) return "3rd Touch";
  if (touch_number === 4) return "4th Touch";
  return "Final";
}

/**
 * A lead is first-touch when the CRM shows no real seller engagement.
 * Definition: contact_status AND contact_status_2 are both blank.
 *
 * Prior bad queue rows (e.g. sent with wrong-stage templates) are NOT treated as
 * genuine stage progression — only explicit CRM status changes prove engagement.
 * This prevents polluted history from auto-upgrading a cold lead into a later stage.
 */
function detectFirstTouch({ owner_item }) {
  const contact_status = lower(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status, null)
  );
  const contact_status_2 = lower(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status_2, null)
  );

  // Explicitly tracked engagement → not first-touch
  if (contact_status && FOLLOW_UP_CONTACT_STATUSES.has(contact_status)) return false;
  if (contact_status_2 && FOLLOW_UP_CONTACT_STATUS_2.has(contact_status_2)) return false;
  if (contact_status && TERMINAL_CONTACT_STATUSES.has(contact_status)) return false;

  // Blank CRM status = first-touch regardless of any prior outbound history
  return !contact_status && !contact_status_2;
}

function deriveQueueMessageType(stage, lifecycle_stage = null) {
  if (
    [STAGES.OFFER, STAGES.CONTRACT, STAGES.FOLLOW_UP].includes(stage) ||
    [
      LIFECYCLE_STAGES.TITLE,
      LIFECYCLE_STAGES.CLOSING,
      LIFECYCLE_STAGES.DISPOSITION,
      LIFECYCLE_STAGES.POST_CLOSE,
    ].includes(lifecycle_stage)
  ) {
    return "Follow-Up";
  }
  return "Cold Outbound";
}

function derivePriorityScore(owner_item, { overdue_bonus = 0 } = {}) {
  const master_priority = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.master_owner_priority_score, 0) || 0
  );
  const contactability = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.contactability_score, 0) || 0
  );
  const financial_pressure = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.financial_pressure_score, 0) || 0
  );
  const urgency = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.urgency_score, 0) || 0
  );
  const tax_delinquent = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_tax_delinquent_count, 0) || 0
  );
  const lien_count = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_lien_count, 0) || 0
  );
  const property_count = Number(
    getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_property_count, 0) || 0
  );
  const priority_tier_bonus = mapPriorityTierBonus(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.priority_tier, null)
  );

  return (
    master_priority * 10 +
    priority_tier_bonus +
    contactability * 4 +
    financial_pressure * 6 +
    urgency * 6 +
    tax_delinquent * 18 +
    lien_count * 14 +
    Math.min(property_count, 50) * 2 +
    overdue_bonus
  );
}

function deriveSendPriority(priority_score, owner_item) {
  const priority_tier = clean(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.priority_tier, null)
  ).toUpperCase();

  if (priority_tier === "TIER_1" || priority_score >= 1400) return "_ Urgent";
  if (priority_tier === "TIER_2" || priority_score >= 850) return "_ Normal";
  return "_ Low";
}

async function loadCachedTextgridNumberPool({ runtime = null, log = logger } = {}) {
  if (!runtime) {
    return loadUsableTextgridNumbers();
  }

  if (Array.isArray(runtime.textgrid_number_pool)) {
    return runtime.textgrid_number_pool;
  }

  const candidates = await timedStage(
    log,
    "master_owner_feeder.textgrid_number_pool_fetch",
    {},
    () => loadUsableTextgridNumbers()
  );

  runtime.textgrid_number_pool = Array.isArray(candidates) ? candidates : [];
  return runtime.textgrid_number_pool;
}

function summarizePhone(phone_item, slot = null) {
  const signals = deriveOutreachSuppressionSignals({
    phone_item,
  });

  return {
    slot,
    item_id: phone_item?.item_id ?? null,
    phone_hidden: getTextValue(phone_item, PHONE_FIELDS.phone_hidden, ""),
    canonical_e164: getTextValue(phone_item, PHONE_FIELDS.canonical_e164, ""),
    activity_status: getCategoryValue(phone_item, PHONE_FIELDS.phone_activity_status, null),
    do_not_call: getCategoryValue(phone_item, PHONE_FIELDS.do_not_call, null),
    dnc_source: signals.dnc_source || null,
    opt_out_date: signals.opt_out_date || null,
    pre_contact_phone_flag: signals.pre_contact_phone_flag,
    true_post_contact_suppression: signals.phone_post_contact_suppression,
    skip_reason: null,
    linked_master_owner_id: getFirstAppReferenceId(phone_item, PHONE_FIELDS.linked_master_owner, null),
    // Properties are NOT linked to Phones in this data model. primary_property_id
    // is intentionally omitted — property resolution goes through owner relations.
  };
}

function summarizeProperty(property_item, owner_item = null) {
  const seller_id_location = parseSellerIdLocation(
    getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "")
  );

  return {
    item_id: property_item?.item_id ?? null,
    title: property_item?.title || null,
    property_address:
      getTextValue(property_item, "property-address", "") ||
      property_item?.title ||
      seller_id_location.property_address ||
      "",
    property_class: getCategoryValue(property_item, "property-class", null),
    property_type: getCategoryValue(property_item, "property-type", null),
    market_id: getFirstAppReferenceId(property_item, "market-2", null),
  };
}

async function selectBestPhone(owner_item, { runtime = null, log = logger } = {}) {
  const owner_id = owner_item?.item_id ?? null;
  const slots = [
    { field: MASTER_OWNER_FIELDS.best_phone_1, label: "best-phone-1" },
    { field: MASTER_OWNER_FIELDS.best_phone_2, label: "best-phone-2" },
    { field: MASTER_OWNER_FIELDS.best_phone_3, label: "best-phone-3" },
  ];

  const rejected = [];
  const phone_records = [];

  for (const slot of slots) {
    const phone_item_id = getFirstAppReferenceId(owner_item, slot.field, null);

    if (!phone_item_id) {
      rejected.push({
        slot: slot.label,
        reason: "missing_phone_reference",
      });
      continue;
    }

    const phone_item = await safeGetItem(phone_item_id, {
      runtime,
      log,
      call_name: "master_owner_feeder.podio_get_phone_item",
      meta: {
        owner_id,
        slot: slot.label,
      },
    });

    if (!phone_item?.item_id) {
      rejected.push({
        slot: slot.label,
        phone_item_id,
        reason: "phone_item_not_found",
      });
      continue;
    }

    const linked_master_owner_id = getFirstAppReferenceId(
      phone_item,
      PHONE_FIELDS.linked_master_owner,
      null
    );

    if (
      linked_master_owner_id &&
      owner_id &&
      String(linked_master_owner_id) !== String(owner_id)
    ) {
      rejected.push({
        slot: slot.label,
        phone_item_id,
        reason: "phone_linked_to_different_master_owner",
        linked_master_owner_id,
      });
      continue;
    }

    const phone_validation = validateActivePhone(phone_item);
    if (!phone_validation.ok) {
      rejected.push({
        slot: slot.label,
        phone_item_id,
        reason: phone_validation.reason,
        activity_status: phone_validation.activity_status,
      });
      continue;
    }

    const suppression = shouldSuppressOutreach({
      phone_item,
      brain_item: null,
      classification: null,
    });

    if (suppression.suppress) {
      rejected.push({
        slot: slot.label,
        phone_item_id,
        reason: suppression.reason,
        details: suppression.details,
      });
      continue;
    }

    const normalized_phone = normalizePhone(
      getPhoneValue(phone_item, PHONE_FIELDS.phone, "") ||
        getTextValue(phone_item, PHONE_FIELDS.canonical_e164, "") ||
        getTextValue(phone_item, PHONE_FIELDS.phone_hidden, "")
    );

    if (!normalized_phone) {
      rejected.push({
        slot: slot.label,
        phone_item_id,
        reason: "phone_number_not_normalizable",
      });
      continue;
    }

    const record = {
      slot: slot.label,
      phone_item_id,
      phone_item,
      normalized_phone,
      prospect_id: getFirstAppReferenceId(phone_item, PHONE_FIELDS.linked_contact, null),
      // Properties are NOT linked to Phones — primary_property_id is not set here.
      // Property resolution uses owner relations, not phone relations.
      market_id: getFirstAppReferenceId(phone_item, PHONE_FIELDS.market, null),
      summary: summarizePhone(phone_item, slot.label),
    };

    phone_records.push(record);

    return {
      selected: record,
      phone_records,
      rejected,
    };
  }

  return {
    selected: null,
    phone_records,
    rejected,
  };
}

async function selectBestProperty(
  selected_phone_record,
  _phone_records = [], // kept for signature compatibility, phones do not link to properties
  { owner_item = null, runtime = null, log = logger } = {}
) {
  const master_owner_id = owner_item?.item_id ?? null;

  // ── Step 1: Master Owner → related Properties (direct Podio relation)
  // Properties are linked to Master Owners, NOT to Phones. Phone records carry
  // no meaningful property reference and must not be used as a resolution path.
  const related_property_ids = uniq(collectRelatedItemIdsByApp(owner_item, APP_IDS.properties));

  log.info("master_owner_feeder.property_resolution_step1_refs", {
    master_owner_id,
    phone_item_id: selected_phone_record?.phone_item_id ?? null,
    refs_count: related_property_ids.length,
    related_property_ids,
    has_refs: Array.isArray(owner_item?.refs) ? owner_item.refs.length : null,
  });

  if (related_property_ids.length > 0) {
    // Fetch all related property items first.
    const related_property_items = [];
    for (const property_id of related_property_ids) {
      const property_item = await safeGetItem(property_id, {
        runtime,
        log,
        call_name: "master_owner_feeder.podio_get_property_item",
        meta: {
          master_owner_id,
          phone_item_id: selected_phone_record?.phone_item_id ?? null,
          resolution_path: "owner_property_refs",
        },
      });
      if (property_item?.item_id) {
        related_property_items.push(property_item);
      }
    }

    if (related_property_items.length === 1) {
      // Unambiguous: exactly one related property.
      log.info("master_owner_feeder.property_resolved_from_owner_relation", {
        master_owner_id,
        property_item_id: related_property_items[0].item_id,
        resolution_path: "owner_property_refs_single",
      });
      return related_property_items[0];
    }

    if (related_property_items.length > 1) {
      // Multiple related properties — attempt disambiguation via seller_id address.
      const seller_id_raw =
        getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "") || null;
      const seller_id_location = parseSellerIdLocation(seller_id_raw ?? "");
      const raw_seller_address = clean(seller_id_location.property_address);

      if (raw_seller_address) {
        const { variants: address_variants } = addressLookupVariants(raw_seller_address);
        const lookup_city = lower(seller_id_location.property_city);
        const lookup_state = lower(seller_id_location.property_state);

        const disambiguated = related_property_items.filter((candidate) => {
          const candidate_address = lower(
            getTextValue(candidate, "property-address", "") || candidate?.title || ""
          );
          if (!address_variants.includes(candidate_address)) return false;
          if (lookup_city) {
            const candidate_city = lower(getTextValue(candidate, "city", ""));
            if (candidate_city && candidate_city !== lookup_city) return false;
          }
          if (lookup_state) {
            const candidate_state = lower(getTextValue(candidate, "state", ""));
            if (candidate_state && candidate_state !== lookup_state) return false;
          }
          return true;
        });

        if (disambiguated.length === 1) {
          log.info("master_owner_feeder.property_resolved_from_owner_relation", {
            master_owner_id,
            property_item_id: disambiguated[0].item_id,
            resolution_path: "owner_property_refs_address_disambiguated",
            candidate_count: related_property_items.length,
          });
          return disambiguated[0];
        }
      }

      // Cannot resolve unambiguously from owner relations.
      log.warn("master_owner_feeder.property_owner_relation_ambiguous", {
        master_owner_id,
        phone_item_id: selected_phone_record?.phone_item_id ?? null,
        candidate_count: related_property_items.length,
        candidate_item_ids: related_property_items.map((c) => c.item_id),
        seller_id: getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "") || null,
        resolution_path: "owner_property_refs",
      });
      return null;
    }
  }

  if (master_owner_id) {
    try {
      const response = await timedStage(
        log,
        "master_owner_feeder.property_lookup_by_master_owner",
        {
          master_owner_id,
          phone_item_id: selected_phone_record?.phone_item_id ?? null,
        },
        () => findPropertyItems({ "linked-master-owner": master_owner_id }, 1, 0)
      );

      const matched_property = response?.items?.[0] ?? response?.[0] ?? null;
      if (matched_property?.item_id) {
        return matched_property;
      }
    } catch (error) {
      if (isTimeoutError(error)) {
        throw error;
      }
      // Log non-timeout errors so silent failures are visible in the logs.
      log.warn("master_owner_feeder.property_lookup_by_master_owner_failed", {
        master_owner_id,
        phone_item_id: selected_phone_record?.phone_item_id ?? null,
        error: serializeFeederError(error),
      });
    }
  }

  // Last-resort: load the brain item linked from master_owner.linked_conversations
  // and pull property IDs from the brain's `properties` relation field.
  const brain_item_id = getFirstAppReferenceId(
    owner_item,
    MASTER_OWNER_FIELDS.linked_conversations,
    null
  );
  if (brain_item_id) {
    const brain_item = await safeGetItem(brain_item_id, {
      runtime,
      log,
      call_name: "master_owner_feeder.podio_get_brain_item_for_property",
      meta: {
        master_owner_id,
        brain_item_id,
        resolution_path: "brain_properties",
      },
    });

    if (brain_item?.item_id) {
      const brain_property_ids = getAppReferenceIds(brain_item, "properties");
      for (const property_id of brain_property_ids) {
        const property_item = await safeGetItem(property_id, {
          runtime,
          log,
          call_name: "master_owner_feeder.podio_get_property_item",
          meta: {
            master_owner_id,
            brain_item_id,
            resolution_path: "brain_properties",
          },
        });
        if (property_item?.item_id) {
          return property_item;
        }
      }
    }
  }

  // Seller-id address lookup: parse address from seller_id and search the Properties
  // app for an exact match. The parsed text is a LOOKUP SEED ONLY — we must find and
  // return a real property item. Only accept when exactly one candidate survives the
  // post-filter (address + optional city + optional state). Return null otherwise.
  const seller_id_raw =
    getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "") || null;
  const seller_id_location = parseSellerIdLocation(seller_id_raw ?? "");
  const raw_seller_address = clean(seller_id_location.property_address);
  const lookup_city = lower(seller_id_location.property_city);
  const lookup_state = lower(seller_id_location.property_state);

  if (raw_seller_address) {
    const { cleaned: lookup_address, variants: address_variants } =
      addressLookupVariants(raw_seller_address);

    log.info("master_owner_feeder.property_lookup_by_seller_id_started", {
      master_owner_id,
      phone_item_id: selected_phone_record?.phone_item_id ?? null,
      brain_item_id: brain_item_id ?? null,
      seller_id: seller_id_raw,
      parsed_property_address: raw_seller_address || null,
      parsed_property_city: lookup_city || null,
      parsed_property_state: lookup_state || null,
      address_variants,
    });

    try {
      const addr_response = await timedStage(
        log,
        "master_owner_feeder.property_lookup_by_address",
        {
          master_owner_id,
          lookup_address,
          lookup_city: lookup_city || null,
          lookup_state: lookup_state || null,
          address_variants,
        },
        () => findPropertyItems({ "property-address": lookup_address }, 5, 0)
      );

      const addr_candidates = Array.isArray(addr_response?.items)
        ? addr_response.items
        : Array.isArray(addr_response)
          ? addr_response
          : [];

      // Post-filter: require the candidate's address to match one of our lookup
      // variants (exact + abbreviation-expanded), and optionally city + state,
      // to avoid Podio's partial-text filter returning unrelated properties.
      const exact_matches = addr_candidates.filter((candidate) => {
        const candidate_address = lower(
          getTextValue(candidate, "property-address", "") || candidate?.title || ""
        );
        if (!address_variants.includes(candidate_address)) return false;
        if (lookup_city) {
          const candidate_city = lower(getTextValue(candidate, "city", ""));
          if (candidate_city && candidate_city !== lookup_city) return false;
        }
        if (lookup_state) {
          const candidate_state = lower(getTextValue(candidate, "state", ""));
          if (candidate_state && candidate_state !== lookup_state) return false;
        }
        return Boolean(candidate?.item_id);
      });

      if (exact_matches.length === 1) {
        log.info("master_owner_feeder.property_lookup_by_seller_id_matched", {
          master_owner_id,
          phone_item_id: selected_phone_record?.phone_item_id ?? null,
          brain_item_id: brain_item_id ?? null,
          seller_id: seller_id_raw,
          parsed_property_address: raw_seller_address || null,
          parsed_property_city: lookup_city || null,
          parsed_property_state: lookup_state || null,
          address_variants,
          property_item_id: exact_matches[0].item_id,
          resolution_path: "seller_id_address",
        });
        return exact_matches[0];
      }

      if (exact_matches.length > 1) {
        log.warn("master_owner_feeder.property_lookup_by_seller_id_multiple_matches", {
          master_owner_id,
          phone_item_id: selected_phone_record?.phone_item_id ?? null,
          brain_item_id: brain_item_id ?? null,
          seller_id: seller_id_raw,
          parsed_property_address: raw_seller_address || null,
          parsed_property_city: lookup_city || null,
          parsed_property_state: lookup_state || null,
          address_variants,
          candidate_count: exact_matches.length,
          candidate_item_ids: exact_matches.map((c) => c.item_id),
          resolution_path: "seller_id_address",
        });
      } else {
        log.warn("master_owner_feeder.property_lookup_by_seller_id_no_match", {
          master_owner_id,
          phone_item_id: selected_phone_record?.phone_item_id ?? null,
          brain_item_id: brain_item_id ?? null,
          seller_id: seller_id_raw,
          parsed_property_address: raw_seller_address || null,
          parsed_property_city: lookup_city || null,
          parsed_property_state: lookup_state || null,
          address_variants,
          candidate_count: addr_candidates.length,
          candidate_item_ids: addr_candidates.map((c) => c.item_id),
          resolution_path: "seller_id_address",
        });
      }
    } catch (error) {
      if (isTimeoutError(error)) {
        throw error;
      }
      log.warn("master_owner_feeder.property_lookup_by_address_failed", {
        master_owner_id,
        phone_item_id: selected_phone_record?.phone_item_id ?? null,
        seller_id: seller_id_raw,
        lookup_address,
        error: serializeFeederError(error),
      });
    }
  }

  log.warn("master_owner_feeder.property_resolution_failed", {
    master_owner_id,
    phone_item_id: selected_phone_record?.phone_item_id ?? null,
    brain_item_id: brain_item_id ?? null,
    seller_id: seller_id_raw ?? null,
    parsed_property_address: raw_seller_address || null,
    parsed_property_city: lookup_city || null,
    parsed_property_state: lookup_state || null,
  });

  return null;
}

function collectRelatedItemIdsByApp(root, target_app_id, depth = 0, seen = new Set()) {
  if (!root || depth > 4) return [];

  if (Array.isArray(root)) {
    return uniq(
      root.flatMap((entry) => collectRelatedItemIdsByApp(entry, target_app_id, depth + 1, seen))
    );
  }

  if (typeof root !== "object") return [];

  const matches = [];
  const candidate_item_id = Number(root?.item_id || 0) || null;
  const candidate_app_id =
    Number(root?.app?.app_id || root?.app_id || root?.appId || 0) || null;

  // Only guard against cycles when the object has a stable identity (item_id or
  // app_id).  Wrapper objects like Podio's { type: "item", data: {...} } have no
  // top-level id, so their key would collide and silently skip sibling entries in
  // the same refs array.  For id-less objects, depth capping (> 4) is sufficient.
  if (candidate_item_id || candidate_app_id) {
    const object_key = `${depth}:${candidate_item_id || ""}:${candidate_app_id || ""}`;
    if (seen.has(object_key)) return [];
    seen.add(object_key);
  }

  if (candidate_item_id && candidate_app_id === Number(target_app_id)) {
    matches.push(candidate_item_id);
  }

  const nested = [
    root.refs,
    root.references,
    root.related_items,
    root.linked_items,
    root.items,
    root.item,
    root.value,
    // Podio GET item returns refs as [{type:"item", data:{item_id,app:{app_id}}}]
    // — walk `data` so the real API response format is handled in addition to the
    // flat {item_id, app_id} format used in tests.
    root.data,
  ];

  return uniq([
    ...matches,
    ...nested.flatMap((entry) =>
      collectRelatedItemIdsByApp(entry, target_app_id, depth + 1, seen)
    ),
  ]);
}

function getHistoryTimestampFromQueueItem(queue_item) {
  return (
    getDateValue(queue_item, "sent-at", null) ||
    getDateValue(queue_item, "delivered-at", null) ||
    getDateValue(queue_item, "scheduled-for-utc", null) ||
    getDateValue(queue_item, "scheduled-for-local", null) ||
    null
  );
}

async function preloadOwnerHistories(master_owner_ids, { runtime = null, log = logger } = {}) {
  const owner_ids = uniq(master_owner_ids)
    .map((value) => Number(value))
    .filter(Boolean);

  if (!owner_ids.length || !runtime) return;

  const unresolved_owner_ids = owner_ids.filter(
    (owner_id) => !runtime.owner_history_by_id.has(String(owner_id))
  );

  if (!unresolved_owner_ids.length) return;

  const page_size = runtime.test_mode ? 25 : MAX_HISTORY_ITEMS;

  const [queue_items, outbound_events] = await Promise.all([
    timedStage(
      log,
      "master_owner_feeder.batch_queue_duplicate_fetch",
      {
        owner_count: unresolved_owner_ids.length,
        page_size,
      },
      () =>
        fetchAllItems(
          APP_IDS.send_queue,
          {
            "master-owner": unresolved_owner_ids,
            "queue-status": ["Queued", "Sending", "Sent"],
          },
          {
            page_size,
          }
        )
    ),
    timedStage(
      log,
      "master_owner_feeder.batch_message_event_duplicate_fetch",
      {
        owner_count: unresolved_owner_ids.length,
        page_size,
      },
      () =>
        fetchAllItems(
          APP_IDS.message_events,
          {
            "master-owner": unresolved_owner_ids,
            direction: "Outbound",
          },
          {
            page_size,
          }
        )
    ),
  ]);

  const history_by_owner_id = new Map(
    unresolved_owner_ids.map((owner_id) => [String(owner_id), buildEmptyHistory()])
  );

  for (const queue_item of queue_items) {
    const owner_id = getFirstAppReferenceId(queue_item, "master-owner", null);
    if (!owner_id) continue;

    const key = String(owner_id);
    if (!history_by_owner_id.has(key)) {
      history_by_owner_id.set(key, buildEmptyHistory());
    }

    history_by_owner_id.get(key).queue_items.push(queue_item);
  }

  for (const event_item of outbound_events) {
    const owner_id = getFirstAppReferenceId(event_item, "master-owner", null);
    if (!owner_id) continue;

    const key = String(owner_id);
    if (!history_by_owner_id.has(key)) {
      history_by_owner_id.set(key, buildEmptyHistory());
    }

    history_by_owner_id.get(key).outbound_events.push(event_item);
  }

  for (const owner_id of unresolved_owner_ids) {
    runtime.owner_history_by_id.set(
      String(owner_id),
      history_by_owner_id.get(String(owner_id)) || buildEmptyHistory()
    );
  }
}

async function loadOwnerHistory(master_owner_id, { runtime = null, log = logger } = {}) {
  const cache_key = String(master_owner_id || "");
  if (runtime?.owner_history_by_id?.has(cache_key)) {
    return runtime.owner_history_by_id.get(cache_key);
  }

  const page_size = runtime?.test_mode ? 25 : MAX_HISTORY_ITEMS;

  const [queue_items, outbound_events] = await Promise.all([
    timedStage(
      log,
      "master_owner_feeder.queue_duplicate_fetch",
      {
        master_owner_id,
        page_size,
      },
      () =>
        fetchAllItems(
          APP_IDS.send_queue,
          {
            "master-owner": master_owner_id,
            "queue-status": ["Queued", "Sending", "Sent"],
          },
          {
            page_size,
          }
        )
    ),
    timedStage(
      log,
      "master_owner_feeder.message_event_duplicate_fetch",
      {
        master_owner_id,
        page_size,
      },
      () =>
        fetchAllItems(
          APP_IDS.message_events,
          {
            "master-owner": master_owner_id,
            direction: "Outbound",
          },
          {
            page_size,
          }
        )
    ),
  ]);

  const history = {
    queue_items,
    outbound_events,
  };

  if (runtime?.owner_history_by_id) {
    runtime.owner_history_by_id.set(cache_key, history);
  }

  return history;
}

function deriveOwnerTouchCount(history) {
  const max_queue_touch = history.queue_items.reduce((max, item) => {
    const touch_number = Number(getNumberValue(item, "touch-number", 0) || 0);
    return Math.max(max, touch_number);
  }, 0);

  return Math.max(max_queue_touch, history.outbound_events.length);
}

// Checks for an active (Queued/Sending) duplicate for the given phone.
// When touch_number is provided, only same-touch rows are considered duplicates.
// This prevents an active touch-1 row from blocking a legitimate touch-2 queue
// while still blocking two cron runs from creating the same touch for the same phone.
function findPendingDuplicate(history, phone_item_id, touch_number = null) {
  return (
    history.queue_items.find((item) => {
      const status = lower(getCategoryValue(item, "queue-status", null));
      const candidate_phone_item_id = getFirstAppReferenceId(item, "phone-number", null);

      if (!QUEUE_DUPLICATE_PENDING_STATUSES.has(status)) return false;
      if (String(candidate_phone_item_id || "") !== String(phone_item_id || "")) return false;

      // If a touch_number is supplied, only consider it a duplicate when the existing
      // row shares the same touch sequence number.
      if (touch_number !== null) {
        const candidate_touch = Number(getNumberValue(item, "touch-number", 0) || 0);
        return candidate_touch === touch_number;
      }

      return true;
    }) || null
  );
}

// Returns a pending (Queued/Sending) queue row for the same phone whose
// touch-number is LOWER than current_touch_number.  A prior-touch row still in
// the queue means the sequence hasn't been processed yet — creating touch N+1
// while touch N is still pending results in out-of-order delivery.
export function findPendingPriorTouch(history, phone_item_id, current_touch_number) {
  if (!current_touch_number || current_touch_number <= 1) return null;
  return (
    history.queue_items.find((item) => {
      const status = lower(getCategoryValue(item, "queue-status", null));
      const candidate_phone_item_id = getFirstAppReferenceId(item, "phone-number", null);
      if (!QUEUE_DUPLICATE_PENDING_STATUSES.has(status)) return false;
      if (String(candidate_phone_item_id || "") !== String(phone_item_id || "")) return false;
      const candidate_touch = Number(getNumberValue(item, "touch-number", 0) || 0);
      return candidate_touch > 0 && candidate_touch < current_touch_number;
    }) || null
  );
}

function findRecentDuplicate(history, phone_item_id, cutoff_ts) {
  const recent_queue_item =
    history.queue_items.find((item) => {
      const status = lower(getCategoryValue(item, "queue-status", null));
      const candidate_phone_item_id = getFirstAppReferenceId(item, "phone-number", null);
      const candidate_ts = toTimestamp(getHistoryTimestampFromQueueItem(item));

      return (
        QUEUE_DUPLICATE_RECENT_STATUSES.has(status) &&
        String(candidate_phone_item_id || "") === String(phone_item_id || "") &&
        candidate_ts !== null &&
        candidate_ts >= cutoff_ts
      );
    }) || null;

  if (recent_queue_item) {
    return {
      type: "queue_item",
      item: recent_queue_item,
      timestamp: getHistoryTimestampFromQueueItem(recent_queue_item),
    };
  }

  const recent_event =
    history.outbound_events.find((item) => {
      const candidate_phone_item_id = getFirstAppReferenceId(item, "phone-number", null);
      const candidate_ts = toTimestamp(getDateValue(item, "timestamp", null));

      return (
        String(candidate_phone_item_id || "") === String(phone_item_id || "") &&
        candidate_ts !== null &&
        candidate_ts >= cutoff_ts
      );
    }) || null;

  if (!recent_event) return null;

  return {
    type: "message_event",
    item: recent_event,
    timestamp: getDateValue(recent_event, "timestamp", null),
  };
}

function sortHistoryEventsDesc(events = []) {
  return [...(events || [])].sort((left, right) => {
    const left_ts = toTimestamp(getDateValue(left, "timestamp", null));
    const right_ts = toTimestamp(getDateValue(right, "timestamp", null));
    return right_ts - left_ts;
  });
}

function resolveLatestOutboundSellerFlow(history = null) {
  const latest_event = sortHistoryEventsDesc(history?.outbound_events || [])[0] || null;
  if (!latest_event) return null;

  const metadata = parseMessageEventMetadata(latest_event);
  const message_body = getTextValue(latest_event, "message", "");
  const selected_use_case = clean(
    metadata.selected_use_case ||
      metadata.template_use_case ||
      inferCanonicalUseCaseFromOutboundText(message_body)
  );
  const next_expected_stage = clean(
    metadata.next_expected_stage || canonicalStageForUseCase(selected_use_case)
  );

  return {
    event_item: latest_event,
    metadata,
    selected_use_case: selected_use_case || null,
    template_use_case: clean(metadata.template_use_case) || null,
    selected_variant_group: clean(metadata.selected_variant_group) || null,
    selected_tone: clean(metadata.selected_tone) || null,
    next_expected_stage: next_expected_stage || null,
  };
}

export function deriveNoReplyFollowUpPlan({
  history = null,
  default_category = "Residential",
  default_tone = "Warm",
} = {}) {
  const latest = resolveLatestOutboundSellerFlow(history);
  const base_use_case =
    clean(latest?.next_expected_stage) ||
    normalizeSellerFlowUseCase(
      latest?.selected_use_case,
      latest?.selected_variant_group
    ) ||
    null;
  const follow_up_use_case = followUpUseCaseForStage(base_use_case);

  if (!base_use_case || !follow_up_use_case) return null;

  const tone = latest?.selected_tone || default_tone;

  const stage_follow_up = (variant_group, category = default_category, paired_with_agent_type = null) => ({
    base_use_case,
    template_lookup_use_case: follow_up_use_case,
    variant_group,
    tone,
    category,
    secondary_category: "Follow-Up",
    paired_with_agent_type:
      paired_with_agent_type ||
      preferredAgentTypeForSellerFlow({
        tone,
        template_use_case: base_use_case,
      }),
    fallback_agent_type: "Fallback / Market-Local",
    next_expected_stage:
      latest?.next_expected_stage || canonicalStageForUseCase(base_use_case),
  });

  switch (base_use_case) {
    case "ownership_check":
      return stage_follow_up("Stage 1 Follow-Up");
    case "consider_selling":
      return stage_follow_up("Stage 2 Follow-Up");
    case "asking_price":
      return stage_follow_up("Stage 3 Follow-Up");
    case "price_works_confirm_basics":
      return stage_follow_up("Stage 4A — Confirm Basics Follow-Up");
    case "price_high_condition_probe":
      return stage_follow_up("Stage 4B — Condition Probe Follow-Up");
    case "offer_reveal_cash":
      return stage_follow_up("Stage 5 — Offer No Response");
    case "mf_confirm_units":
      return stage_follow_up(
        "Multifamily Underwrite — Units Follow-Up",
        "Landlord / Multifamily",
        "Specialist-Landlord / Market-Local"
      );
    case "mf_occupancy":
      return stage_follow_up(
        "Multifamily Underwrite — Occupancy Follow-Up",
        "Landlord / Multifamily",
        "Specialist-Landlord / Market-Local"
      );
    case "mf_rents":
      return stage_follow_up(
        "Multifamily Underwrite — Rents Follow-Up",
        "Landlord / Multifamily",
        "Specialist-Landlord / Market-Local"
      );
    case "mf_expenses":
      return stage_follow_up(
        "Multifamily Underwrite — Expenses Follow-Up",
        "Landlord / Multifamily",
        "Specialist-Landlord / Market-Local"
      );
    default:
      return null;
  }
}

function extractLatestOwnerContactTimestamp(owner_item, history) {
  const owner_dates = [
    getDateValue(owner_item, MASTER_OWNER_FIELDS.last_contacted_at, null),
    getDateValue(owner_item, MASTER_OWNER_FIELDS.last_outbound, null),
    getDateValue(owner_item, MASTER_OWNER_FIELDS.last_inbound, null),
  ];

  const history_dates = [
    ...history.queue_items.map((item) => getHistoryTimestampFromQueueItem(item)),
    ...history.outbound_events.map((item) => getDateValue(item, "timestamp", null)),
  ];

  return newestIso([...owner_dates, ...history_dates]);
}

function buildOwnerContext({
  owner_item,
  phone_item,
  property_item = null,
  market_item = null,
  brain_item = null,
  agent_item = null,
  sms_agent_id = null,
  owner_touch_count = 0,
}) {
  const phone_item_id = phone_item?.item_id ?? null;
  const master_owner_id = owner_item?.item_id ?? null;
  const property_id = property_item?.item_id ?? null;
  const market_id =
    market_item?.item_id ??
    getFirstAppReferenceId(property_item, "market-2", null) ??
    getFirstAppReferenceId(phone_item, PHONE_FIELDS.market, null) ??
    null;
  const prospect_id = getFirstAppReferenceId(phone_item, PHONE_FIELDS.linked_contact, null);

  const recent_templates = loadRecentTemplates({
    brain_item,
    limit: 10,
  });

  const owner_market_name =
    getTextValue(market_item, "title", "") ||
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.markets, null) ||
    null;
  const base_summary = deriveContextSummary({
    phone_item,
    brain_item,
    master_owner_item: owner_item,
    property_item,
    agent_item,
    market_item,
    touch_count: owner_touch_count,
  });

  return {
    found: true,
    ids: {
      brain_item_id: brain_item?.item_id ?? null,
      phone_item_id,
      master_owner_id,
      prospect_id,
      property_id,
      market_id,
      assigned_agent_id: sms_agent_id || agent_item?.item_id || null,
    },
    items: {
      brain_item,
      phone_item,
      master_owner_item: owner_item,
      prospect_item: null,
      property_item,
      market_item,
      agent_item,
    },
    summary: {
      ...base_summary,
      conversation_stage:
        getCategoryValue(brain_item, "conversation-stage", null) ||
        deriveOwnerStageHint(owner_item),
      brain_ai_route: getCategoryValue(brain_item, "ai-route", null),
      language_preference: normalizeLanguage(
        getCategoryValue(owner_item, MASTER_OWNER_FIELDS.language_primary, null) ||
          getCategoryValue(brain_item, "language-preference", "English") ||
          "English"
      ),
      seller_profile: getCategoryValue(brain_item, "seller-profile", null),
      status_ai_managed: getCategoryValue(brain_item, "status-ai-managed", null),
      follow_up_trigger_state: getCategoryValue(brain_item, "follow-up-trigger-state", null),
      motivation_score: getNumberValue(brain_item, "seller-motivation-score", null),
      total_messages_sent: owner_touch_count,
      last_inbound_message: getTextValue(brain_item, "last-inbound-message", ""),
      last_outbound_message: getTextValue(brain_item, "last-outbound-message", ""),
      contact_window: getCategoryValue(owner_item, MASTER_OWNER_FIELDS.best_contact_window, null),
      market_name: owner_market_name,
      market_state: getTextValue(market_item, "state", ""),
      market_timezone:
        getTextValue(market_item, "timezone", "") ||
        getCategoryValue(owner_item, MASTER_OWNER_FIELDS.timezone, "Central"),
      market_area_code: getTextValue(market_item, "area-code", ""),
      timezone: getCategoryValue(owner_item, MASTER_OWNER_FIELDS.timezone, "Central"),
    },
    recent: {
      touch_count: owner_touch_count,
      last_template_id: recent_templates.last_template_id,
      recently_used_template_ids: recent_templates.recent_template_ids,
    },
  };
}

function buildSyntheticClassification({ owner_item, phone_item, stage_hint, language }) {
  return {
    message: "",
    language,
    objection: null,
    emotion: deriveOwnerEmotion(owner_item),
    stage_hint,
    compliance_flag: null,
    positive_signals: [],
    confidence: 1,
    motivation_score:
      getNumberValue(owner_item, MASTER_OWNER_FIELDS.urgency_score, null) ??
      getNumberValue(owner_item, MASTER_OWNER_FIELDS.financial_pressure_score, null) ??
      null,
    source: "master_owner_feeder",
    notes: "owner_batch_outbound",
    phone_activity_status: getCategoryValue(
      phone_item,
      PHONE_FIELDS.phone_activity_status,
      "Unknown"
    ),
  };
}

function isPositiveCategory(value) {
  const raw = lower(value);
  return [
    "yes",
    "true",
    "active",
    "enabled",
    "available",
    "on",
    "_ active",
    "_ warming up",
  ].includes(raw);
}

function isNegativeCategory(value) {
  const raw = lower(value);
  return [
    "no",
    "false",
    "inactive",
    "disabled",
    "retired",
    "blocked",
    "off",
    "_ paused",
    "_ flagged",
    "⚫ retired",
  ].includes(raw);
}

function isExplicitNumberPaused(textgrid_item, now_ts) {
  if (isPositiveCategory(getCategoryValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.hard_pause, null))) {
    return true;
  }

  const pause_until = getDateValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.pause_until, null);
  const pause_until_ts = toTimestamp(pause_until);

  if (pause_until_ts === null) return false;
  return pause_until_ts > now_ts;
}

function isExplicitOutboundNumberUsable(textgrid_item, now_ts) {
  if (!textgrid_item?.item_id) return false;

  const outbound_phone =
    getPhoneValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.title, "") ||
    getTextValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.title, "");

  if (!normalizePhone(outbound_phone)) return false;

  const status = getCategoryValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.status, null);
  if (status && isNegativeCategory(status)) return false;
  if (isExplicitNumberPaused(textgrid_item, now_ts)) return false;

  const daily_limit = Number(
    getNumberValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.daily_send_cap, 0) || 0
  );
  const sent_today = Number(
    getNumberValue(textgrid_item, TEXTGRID_NUMBER_FIELDS.sent_today, 0) || 0
  );

  if (daily_limit > 0 && sent_today >= daily_limit) {
    return false;
  }

  return true;
}

async function resolveOutboundNumber({
  owner_item,
  context,
  classification,
  route,
  rotation_key,
  now_ts,
  runtime = null,
  log = logger,
}) {
  const explicit_outbound_number_id = getFirstAppReferenceId(
    owner_item,
    MASTER_OWNER_FIELDS.outbound_number,
    null
  );
  const market_resolution = resolveMarketSendingProfile(context?.summary?.market_name || null);

  if (explicit_outbound_number_id) {
    const explicit_item = await safeGetItem(explicit_outbound_number_id, {
      runtime,
      log,
      call_name: "master_owner_feeder.podio_get_outbound_number",
      meta: {
        owner_id: owner_item?.item_id ?? null,
      },
    });

    if (isExplicitOutboundNumberUsable(explicit_item, now_ts)) {
      const explicit_phone =
        getPhoneValue(explicit_item, TEXTGRID_NUMBER_FIELDS.title, "") ||
        getTextValue(explicit_item, TEXTGRID_NUMBER_FIELDS.title, "");

      return {
        textgrid_number_item_id: explicit_outbound_number_id,
        source: "master_owner.outbound-number",
        diagnostics: {
          raw_seller_market: context?.summary?.market_name || null,
          normalized_seller_market: market_resolution.normalized_raw_market || null,
          resolved_sending_zone: market_resolution.sending_zone || null,
          allowed_phone_markets: market_resolution.allowed_phone_markets || [],
          selected_phone_number: normalizePhone(explicit_phone) || null,
          selected_phone_market: getCategoryValue(explicit_item, TEXTGRID_NUMBER_FIELDS.market, null),
          selection_reason: "explicit_outbound_number_override",
          fallback_reason: null,
        },
      };
    }

    warn("master_owner_feeder.outbound_number_fallback", {
      master_owner_id: owner_item?.item_id ?? null,
      explicit_outbound_number_id,
      reason: "explicit_outbound_number_unusable",
    });
  }

  const candidate_records = await loadCachedTextgridNumberPool({
    runtime,
    log,
  });

  const selected = await chooseTextgridNumber({
    context,
    classification,
    route,
    preferred_language: context?.summary?.language_preference || "English",
    rotation_key,
    candidate_records,
  });

  return {
    textgrid_number_item_id:
      selected?.item_id ||
      selected?.textgrid_number_item_id ||
      selected?.id ||
      null,
    source:
      selected?.item_id ||
      selected?.textgrid_number_item_id ||
      selected?.id
        ? "chooseTextgridNumber"
        : null,
    diagnostics: selected?.selection_diagnostics || {
      raw_seller_market: context?.summary?.market_name || null,
      normalized_seller_market: market_resolution.normalized_raw_market || null,
      resolved_sending_zone: market_resolution.sending_zone || null,
      allowed_phone_markets: market_resolution.allowed_phone_markets || [],
      selected_phone_number: null,
      selected_phone_market: null,
      selection_reason: selected?.selection_reason || market_resolution.reason || "textgrid_number_not_found",
      fallback_reason: selected?.fallback_reason || null,
    },
  };
}

async function evaluateOwner({
  owner_item,
  dry_run = false,
  create_brain_if_missing = false,
  now = nowIso(),
  runtime = null,
  evaluation_depth = "full",
}) {
  const master_owner_id = owner_item?.item_id ?? null;
  const log = child({
    module: "domain.master_owners.run_outbound_feeder.owner",
    master_owner_id,
    seller_id: getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, ""),
  });

  const owner_summary = summarizeMasterOwner(owner_item);
  const sms_eligible = getCategoryValue(owner_item, MASTER_OWNER_FIELDS.sms_eligible, null);
  const contact_status = getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status, null);
  const contact_status_2 = getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status_2, null);
  const next_follow_up_at = getDateValue(owner_item, MASTER_OWNER_FIELDS.next_follow_up_at, null);
  const now_ts = toTimestamp(now) ?? Date.now();
  const next_follow_up_ts = toTimestamp(next_follow_up_at);
  const cadence_days = mapFollowUpCadenceToDays(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.follow_up_cadence, null)
  );
  const suppression_window_ms = cadence_days * 24 * 60 * 60 * 1000;
  const has_contract = Boolean(getFirstAppReferenceId(owner_item, MASTER_OWNER_FIELDS.contract, null));
  const has_closing = Boolean(getFirstAppReferenceId(owner_item, MASTER_OWNER_FIELDS.closing, null));

  if (lower(sms_eligible) !== "yes") {
    return {
      ok: false,
      skipped: true,
      reason: "sms_not_eligible",
      owner: owner_summary,
    };
  }

  if (TERMINAL_CONTACT_STATUSES.has(lower(contact_status))) {
    return {
      ok: false,
      skipped: true,
      reason: "terminal_contact_status",
      owner: owner_summary,
      contact_status,
    };
  }

  if (lower(contact_status_2) === "dnc") {
    return {
      ok: false,
      skipped: true,
      reason: "contact_status_dnc",
      owner: owner_summary,
      contact_status_2,
    };
  }

  if (has_contract || has_closing) {
    return {
      ok: false,
      skipped: true,
      reason: "owner_has_contract_or_closing",
      owner: owner_summary,
      has_contract,
      has_closing,
    };
  }

  if (next_follow_up_ts !== null && next_follow_up_ts > now_ts) {
    return {
      ok: false,
      skipped: true,
      reason: "next_follow_up_not_due",
      owner: owner_summary,
      next_follow_up_at,
    };
  }

  const phone_selection = await timedStage(
    log,
    "master_owner_feeder.phone_resolution",
    {
      evaluation_depth,
    },
    () => selectBestPhone(owner_item, { runtime, log })
  );
  const selected_phone_record = phone_selection.selected;

  if (!selected_phone_record) {
    return {
      ok: false,
      skipped: true,
      reason: "no_usable_phone",
      owner: owner_summary,
      phone_attempts: phone_selection.rejected,
    };
  }

  const history = await timedStage(
    log,
    "master_owner_feeder.history_resolution",
    {
      evaluation_depth,
    },
    () => loadOwnerHistory(master_owner_id, { runtime, log })
  );
  const owner_touch_count = deriveOwnerTouchCount(history);
  const touch_number = owner_touch_count + 1;
  const latest_contact_at = timedStage(
    log,
    "master_owner_feeder.latest_contact_resolution",
    {
      evaluation_depth,
      phone_item_id: selected_phone_record.phone_item_id,
    },
    () =>
      Promise.resolve(
        extractLatestOwnerContactTimestamp(owner_item, history)
      )
  );
  const resolved_latest_contact_at = await latest_contact_at;
  const latest_contact_ts = toTimestamp(resolved_latest_contact_at);
  const explicit_follow_up_due = next_follow_up_ts !== null && next_follow_up_ts <= now_ts;

  // Detect first-touch here so suppression logic below can reference it.
  // A lead whose contact_status and contact_status_2 are both blank has no
  // CRM-confirmed engagement. Stale history from prior bad queue rows must not
  // permanently suppress them — only pending same-phone rows do.
  const is_first_touch = detectFirstTouch({ owner_item });

  // For engaged/follow-up leads, suppress if contacted recently.
  // For first-touch blank-status leads, skip this entirely — their stale outbound
  // history is unreliable signal and must not block queue creation.
  if (
    !is_first_touch &&
    !explicit_follow_up_due &&
    latest_contact_ts !== null &&
    latest_contact_ts >= now_ts - suppression_window_ms
  ) {
    return {
      ok: false,
      skipped: true,
      reason: "recent_contact_within_suppression_window",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      latest_contact_at: resolved_latest_contact_at,
      suppression_window_days: cadence_days,
    };
  }

  const pending_duplicate = await timedStage(
    log,
    "master_owner_feeder.pending_queue_duplicate_check",
    {
      evaluation_depth,
      phone_item_id: selected_phone_record.phone_item_id,
    },
    () =>
      Promise.resolve(
        findPendingDuplicate(history, selected_phone_record.phone_item_id, touch_number)
      )
  );
  if (pending_duplicate) {
    log.info("master_owner_feeder.duplicate_active_queue_blocked", {
      master_owner_id,
      phone_item_id: selected_phone_record.phone_item_id,
      touch_number,
      duplicate_queue_item_id: pending_duplicate.item_id,
      duplicate_queue_status: getCategoryValue(pending_duplicate, "queue-status", null),
    });
    return {
      ok: false,
      skipped: true,
      reason: "duplicate_pending_queue_item",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      duplicate_queue_item_id: pending_duplicate.item_id,
      duplicate_queue_status: getCategoryValue(pending_duplicate, "queue-status", null),
    };
  }

  // Block touch N+1 while touch N is still pending in the queue.  A lower-touch
  // row in Queued/Sending status means the sequence hasn't been delivered yet.
  // findPendingDuplicate only catches same-touch duplicates; this catches cross-touch
  // advancement (touch 2 created while touch 1 is still Queued).
  const pending_prior_touch = timedStage(
    log,
    "master_owner_feeder.pending_prior_touch_check",
    {
      evaluation_depth,
      phone_item_id: selected_phone_record.phone_item_id,
    },
    () =>
      Promise.resolve(
        findPendingPriorTouch(history, selected_phone_record.phone_item_id, touch_number)
      )
  );
  const resolved_pending_prior_touch = await pending_prior_touch;
  if (resolved_pending_prior_touch) {
    log.info("master_owner_feeder.pending_prior_touch_blocks_advancement", {
      master_owner_id,
      phone_item_id: selected_phone_record.phone_item_id,
      requested_touch_number: touch_number,
      blocking_touch_number: Number(
        getNumberValue(resolved_pending_prior_touch, "touch-number", 0) || 0
      ),
      blocking_queue_item_id: resolved_pending_prior_touch.item_id,
      blocking_queue_status: getCategoryValue(
        resolved_pending_prior_touch,
        "queue-status",
        null
      ),
    });
    return {
      ok: false,
      skipped: true,
      reason: "pending_prior_touch_blocks_advancement",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      blocking_queue_item_id: resolved_pending_prior_touch.item_id,
      blocking_touch_number: Number(
        getNumberValue(resolved_pending_prior_touch, "touch-number", 0) || 0
      ),
    };
  }

  // Universal same-day touch advancement guard.  A queue row or message event
  // for this phone created within the last 24 hours means a touch was recently
  // queued — do not advance to the next touch in the same calendar day.
  // Applies regardless of is_first_touch status.
  // Exempt: explicit_follow_up_due (user scheduled a deliberate same-day follow-up).
  if (!explicit_follow_up_due) {
    const same_day_cutoff_ts = now_ts - 24 * 60 * 60 * 1000;
    const same_day_block = await timedStage(
      log,
      "master_owner_feeder.same_day_touch_guard",
      {
        evaluation_depth,
        phone_item_id: selected_phone_record.phone_item_id,
      },
      () =>
        Promise.resolve(
          findRecentDuplicate(
            history,
            selected_phone_record.phone_item_id,
            same_day_cutoff_ts
          )
        )
    );
    if (same_day_block) {
      log.info("master_owner_feeder.same_day_touch_advancement_blocked", {
        master_owner_id,
        phone_item_id: selected_phone_record.phone_item_id,
        touch_number,
        is_first_touch,
        duplicate_source: same_day_block.type,
        duplicate_item_id: same_day_block.item?.item_id ?? null,
        duplicate_timestamp: same_day_block.timestamp || null,
      });
      return {
        ok: false,
        skipped: true,
        reason: "same_day_touch_advancement_blocked",
        owner: owner_summary,
        phone: selected_phone_record.summary,
        duplicate_source: same_day_block.type,
        duplicate_item_id: same_day_block.item?.item_id ?? null,
        duplicate_timestamp: same_day_block.timestamp || null,
      };
    }
  }

  // For first-touch leads, skip sent-history suppression for the same reason as above:
  // sent queue rows or message events from prior bad sends are not proof of engagement.
  // The pending_duplicate guard above already prevents double-queuing same-phone items.
  if (!is_first_touch) {
    const recent_duplicate = await timedStage(
      log,
      "master_owner_feeder.message_event_duplicate_check",
      {
        evaluation_depth,
        phone_item_id: selected_phone_record.phone_item_id,
      },
      () =>
        Promise.resolve(
          findRecentDuplicate(
            history,
            selected_phone_record.phone_item_id,
            now_ts - suppression_window_ms
          )
        )
    );

    if (recent_duplicate) {
      return {
        ok: false,
        skipped: true,
        reason: "duplicate_within_suppression_window",
        owner: owner_summary,
        phone: selected_phone_record.summary,
        duplicate_source: recent_duplicate.type,
        duplicate_item_id: recent_duplicate.item?.item_id ?? null,
        duplicate_timestamp: recent_duplicate.timestamp || null,
        suppression_window_days: cadence_days,
      };
    }
  }

  const overdue_bonus = explicit_follow_up_due && next_follow_up_ts !== null
    ? Math.min(200, Math.floor((now_ts - next_follow_up_ts) / (60 * 60 * 1000)) * 4)
    : 0;
  const priority_score = derivePriorityScore(owner_item, { overdue_bonus });
  const send_priority = deriveSendPriority(priority_score, owner_item);

  if (evaluation_depth !== "full") {
    return {
      ok: true,
      skipped: false,
      dry_run: true,
      reason: "candidate_ready_for_deep_evaluation",
      owner: owner_summary,
      plan: {
        master_owner_id,
        seller_id: owner_summary.seller_id,
        owner_name: owner_summary.owner_name,
        phone_item_id: selected_phone_record.phone_item_id,
        phone: selected_phone_record.summary,
        touch_number,
        send_priority,
        priority_score,
        suppression_window_days: cadence_days,
        next_follow_up_at: summarizeDate(next_follow_up_at),
        latest_contact_at: summarizeDate(resolved_latest_contact_at),
      },
    };
  }

  let property_item = await timedStage(
    log,
    "master_owner_feeder.property_selection",
    {
      evaluation_depth,
    },
    () =>
      selectBestProperty(selected_phone_record, phone_selection.phone_records, {
        owner_item,
        runtime,
        log,
      })
  );

  // Synthetic property fallback: for first-touch cold leads only, if no real
  // property item was resolved but seller_id contains a parseable address, build
  // a lightweight synthetic property so Stage-1 templates can render in dry_run.
  // This is intentionally gated on is_first_touch — follow-ups and later stages
  // must always link to a real Podio property item.
  if (!property_item?.item_id && is_first_touch) {
    const synthetic = buildSyntheticPropertyFromSellerId(owner_item);
    if (synthetic) {
      property_item = synthetic;
      log.info("master_owner_feeder.synthetic_property_fallback_used", {
        master_owner_id,
        seller_id: synthetic._seller_id ?? null,
        parsed_property_address: synthetic._synthetic_property_address ?? null,
        parsed_property_city: synthetic._synthetic_property_city ?? null,
        parsed_property_state: synthetic._synthetic_property_state ?? null,
        dry_run,
      });
    } else {
      log.warn("master_owner_feeder.synthetic_property_fallback_unusable", {
        master_owner_id,
        seller_id:
          getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "") || null,
        parsed_property_address: null,
      });
    }
  }

  // Part 1 — Real property gate for live queue rows.
  // dry_run diagnostics may continue with a synthetic property (useful for
  // understanding which first-touch leads lack a property record). Live queue
  // creation is blocked — a synthetic item has no item_id and therefore cannot
  // populate the Properties relation on the queue row.
  //
  // Override: set ALLOW_SYNTHETIC_FIRST_TOUCH_QUEUE=true to bypass this gate
  // while the property backfill is in progress.
  if (
    property_item?.synthetic &&
    !dry_run &&
    process.env.ALLOW_SYNTHETIC_FIRST_TOUCH_QUEUE !== "true"
  ) {
    log.info("master_owner_feeder.live_queue_blocked_missing_real_property", {
      master_owner_id,
      phone_item_id: selected_phone_record.phone_item_id,
      seller_id_raw: property_item._seller_id ?? null,
      synthetic_property_address: property_item._synthetic_property_address ?? null,
      synthetic_property_city: property_item._synthetic_property_city ?? null,
      synthetic_property_state: property_item._synthetic_property_state ?? null,
      attempted_paths: [
        "owner_property_refs",
        "find_by_master_owner",
        "brain_property_refs",
        "seller_id_address_lookup",
      ],
    });
    return {
      ok: false,
      skipped: true,
      reason: "real_property_required_for_live_queue",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      first_touch: true,
      diagnostics: {
        seller_id_raw: property_item._seller_id ?? null,
        synthetic_property_address: property_item._synthetic_property_address ?? null,
        synthetic_property_city: property_item._synthetic_property_city ?? null,
        master_owner_id,
        phone_item_id: selected_phone_record.phone_item_id,
      },
    };
  }

  // Structured resolution log so dashboards can distinguish real vs synthetic.
  if (property_item?.item_id) {
    log.info("master_owner_feeder.property_resolution_real_matched", {
      master_owner_id,
      property_item_id: property_item.item_id,
      phone_item_id: selected_phone_record.phone_item_id,
    });
  } else if (property_item?.synthetic) {
    // Only reaches here in dry_run or when ALLOW_SYNTHETIC_FIRST_TOUCH_QUEUE=true.
    log.info("master_owner_feeder.property_resolution_synthetic_only", {
      master_owner_id,
      phone_item_id: selected_phone_record.phone_item_id,
      synthetic_property_address: property_item._synthetic_property_address ?? null,
      dry_run,
    });
  }

  const sms_agent_id = getFirstAppReferenceId(owner_item, MASTER_OWNER_FIELDS.sms_agent, null);
  const assigned_agent_id = getFirstAppReferenceId(owner_item, MASTER_OWNER_FIELDS.assigned_agent, null);
  const resolved_agent_id = sms_agent_id || assigned_agent_id || null;
  const agent_item = await timedStage(
    log,
    "master_owner_feeder.agent_resolution",
    {
      evaluation_depth,
      agent_item_id: resolved_agent_id,
    },
    () =>
      resolved_agent_id
        ? safeGetItem(resolved_agent_id, {
            runtime,
            log,
            call_name: "master_owner_feeder.podio_get_agent_item",
            meta: {
              master_owner_id,
            },
          })
        : Promise.resolve(null)
  );

  // Part 2 — Populate Market from the resolved real property item.
  // market_item is used by buildOwnerContext to populate context.summary.market_name,
  // market_timezone, etc., and by buildSendQueueItem to write the Market relation
  // on the queue row.  We derive market_id from the property's market-2 field first,
  // then fall back to the phone record's market association.
  const raw_market_id =
    getFirstAppReferenceId(property_item, "market-2", null) ||
    getFirstAppReferenceId(property_item, "market", null) ||
    selected_phone_record.market_id ||
    null;

  const market_item = raw_market_id
    ? await safeGetItem(raw_market_id, {
        runtime,
        log,
        call_name: "master_owner_feeder.podio_get_market_item",
        meta: {
          master_owner_id,
          market_id: raw_market_id,
          property_item_id: property_item?.item_id ?? null,
        },
      })
    : null;

  // Declare market_id so it can be referenced later in the plan object.
  const market_id = market_item?.item_id ?? null;

  if (market_id) {
    log.info("master_owner_feeder.queue_market_hydrated", {
      master_owner_id,
      property_item_id: property_item?.item_id ?? null,
      market_id,
      market_name: getTextValue(market_item, "title", "") || null,
      source: getFirstAppReferenceId(property_item, "market-2", null)
        ? "property_market_2"
        : getFirstAppReferenceId(property_item, "market", null)
          ? "property_market"
          : "phone_record_market",
    });
  }

  // Part 3 — Load brain_item so recently_used_template_ids is populated.
  // safeGetItem uses the runtime cache, so if selectBestProperty already fetched
  // the brain item (for property resolution) this is a free cache hit.
  const owner_brain_item_id = getFirstAppReferenceId(
    owner_item,
    MASTER_OWNER_FIELDS.linked_conversations,
    null
  );
  const brain_item = owner_brain_item_id
    ? await safeGetItem(owner_brain_item_id, {
        runtime,
        log,
        call_name: "master_owner_feeder.podio_get_brain_item",
        meta: {
          master_owner_id,
          brain_item_id: owner_brain_item_id,
        },
      })
    : null;

  const context = buildOwnerContext({
    owner_item,
    phone_item: selected_phone_record.phone_item,
    property_item,
    market_item,
    brain_item,
    agent_item,
    sms_agent_id: resolved_agent_id,
    owner_touch_count,
  });

  const stage_hint = deriveOwnerStageHint(owner_item);
  const language = normalizeLanguage(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.language_primary, null) ||
      context.summary.language_preference ||
      "English"
  );

  const classification = buildSyntheticClassification({
    owner_item,
    phone_item: selected_phone_record.phone_item,
    stage_hint,
    language,
  });

  const route = resolveRoute({
    classification,
    brain_item: null,
    phone_item: selected_phone_record.phone_item,
    message: "",
  });

  const primary_category = deriveTemplatePrimaryCategory(
    property_item,
    owner_item,
    route?.primary_category || "Residential"
  );
  const follow_up_plan = explicit_follow_up_due
    ? deriveNoReplyFollowUpPlan({
        history,
        default_category: primary_category,
        default_tone: route?.tone || "Warm",
      })
    : null;
  const secondary_category = deriveTemplateSecondaryCategory(
    property_item,
    owner_item,
    route?.secondary_category || null
  );
  const sequence_position = deriveSequencePosition(route?.stage || stage_hint, owner_touch_count);

  // ── FIRST-TOUCH TEMPLATE GATING ─────────────────────────────────────────
  // is_first_touch was computed above (before suppression checks).
  // Suppress follow_up_plan for first-touch: any prior outbound history without
  // confirmed CRM engagement is contamination, not real stage progression.
  const effective_follow_up_plan = is_first_touch ? null : follow_up_plan;

  if (is_first_touch) {
    // Require at minimum a real property item OR a synthetic property built from
    // seller_id. The synthetic fallback was already attempted above before context
    // was built, so property_item?.synthetic being true here means seller_id
    // provided enough address context to continue.
    if (!property_item?.item_id && !property_item?.synthetic && !context.ids?.property_id) {
      const seller_id_location = parseSellerIdLocation(
        getTextValue(owner_item, MASTER_OWNER_FIELDS.seller_id, "")
      );
      warn("master_owner_feeder.first_touch_missing_property", {
        master_owner_id,
        detected_first_touch: true,
        contact_status: owner_summary.contact_status,
        contact_status_2: owner_summary.contact_status_2,
        phone_slot: selected_phone_record.slot,
        phone_item_id: selected_phone_record.phone_item_id,
        brain_item_id: getFirstAppReferenceId(
          owner_item,
          MASTER_OWNER_FIELDS.linked_conversations,
          null
        ),
        seller_id_has_address: Boolean(seller_id_location.property_address),
        seller_id_address: seller_id_location.property_address || null,
        seller_id_city: seller_id_location.property_city || null,
      });
      return {
        ok: false,
        skipped: true,
        reason: "missing_property_relation_for_first_touch",
        owner: owner_summary,
        phone: selected_phone_record.summary,
        first_touch: true,
      };
    }

    // If the routing engine selected a later-stage use_case or forbidden lifecycle,
    // log it but do NOT return early. Template lookup is already hard-clamped to
    // "ownership_check" / "Stage 1 — Ownership Confirmation" below, so polluted
    // route output is harmless at this point. The final post-template guard will
    // catch any case where loadTemplate somehow returns a wrong-stage template.
    const routed_use_case = route?.use_case || "ownership_check";
    const routed_lifecycle = route?.lifecycle_stage || null;
    if (
      FORBIDDEN_FIRST_TOUCH_USE_CASES.has(routed_use_case) ||
      (routed_lifecycle && FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES.has(routed_lifecycle))
    ) {
      warn("master_owner_feeder.first_touch_forbidden_route_clamped", {
        master_owner_id,
        detected_first_touch: true,
        routed_use_case,
        routed_lifecycle,
        property_id: property_item?.item_id ?? null,
        contact_status: owner_summary.contact_status,
        contact_status_2: owner_summary.contact_status_2,
        action: "clamped_to_ownership_check",
      });
    }
  }
  // ────────────────────────────────────────────────────────────────────────
  const message_variant_seed = clean(
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.message_variant_seed, null)
  );
  const resolved_timezone =
    getCategoryValue(owner_item, MASTER_OWNER_FIELDS.timezone, null) ||
    context.summary.timezone ||
    "Central";
  const resolved_contact_window =
    resolveSchedulingContactWindow({
      contact_window:
        getCategoryValue(owner_item, MASTER_OWNER_FIELDS.best_contact_window, null) ||
        context.summary.contact_window ||
        null,
      timezone_label: resolved_timezone,
      is_first_contact: touch_number <= 1,
    });
  // Part 3 — Improved rotation key for broader template spread.
  // master_owner_id is the primary seed (always unique per owner).
  // seller_id and synthetic address prevent first-touch owners with no real
  // property from all landing on the same "no-property" bucket.
  // primary_category and language ensure cross-bucket variation.
  const rotation_key = [
    master_owner_id || "no-owner",
    owner_summary.seller_id ||
      selected_phone_record.phone_item_id ||
      "no-seed",
    property_item?.item_id ||
      property_item?._synthetic_property_address ||
      "no-property",
    primary_category || "no-category",
    language || "no-language",
    route?.use_case || "no-use-case",
    message_variant_seed || "",
  ].join(":");

  const outbound_number = await timedStage(
    log,
    "master_owner_feeder.outbound_number_selection",
    {
      evaluation_depth,
    },
    () =>
      resolveOutboundNumber({
        owner_item,
        context,
        classification,
        route,
        rotation_key,
        now_ts,
        runtime,
        log,
      })
  );

  if (!outbound_number.textgrid_number_item_id) {
    return {
      ok: false,
      skipped: true,
      reason: "textgrid_number_not_found",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      outbound_number_source: outbound_number.source,
      outbound_number_diagnostics: outbound_number.diagnostics || null,
    };
  }

  const selected_template = await timedStage(
    log,
    "master_owner_feeder.template_selection",
    {
      evaluation_depth,
    },
    () =>
      loadTemplate({
        category: effective_follow_up_plan?.category || primary_category,
        secondary_category: effective_follow_up_plan?.secondary_category ?? secondary_category,
        // First-touch always clamped to Stage 1 ownership regardless of route output.
        use_case: is_first_touch
          ? "ownership_check"
          : (effective_follow_up_plan?.template_lookup_use_case || route?.use_case || "ownership_check"),
        variant_group: is_first_touch
          ? "Stage 1 — Ownership Confirmation"
          : (effective_follow_up_plan?.variant_group || route?.variant_group || "Stage 1 — Ownership Confirmation"),
        tone: is_first_touch
          ? "Warm"
          : (effective_follow_up_plan?.tone || route?.tone || "Warm"),
        gender_variant: "Neutral",
        language,
        sequence_position: is_first_touch ? "1st Touch" : sequence_position,
        paired_with_agent_type: is_first_touch
          ? "Warm Professional"
          : (effective_follow_up_plan?.paired_with_agent_type ||
              route?.template_filters?.paired_with_agent_type ||
              route?.persona ||
              "Warm Professional"),
        recently_used_template_ids: context?.recent?.recently_used_template_ids || [],
        rotation_key,
        fallback_agent_type:
          effective_follow_up_plan?.fallback_agent_type ||
          route?.template_filters?.fallback_agent_type ||
          "Warm Professional",
        context,
        // For first-touch cold outreach, constrain the scoring pool to Stage-1
        // ownership-check variant groups only.  This prevents follow-up, Stage 4+,
        // and other later-stage templates from being selected even if they happen
        // to share the same use_case and score higher due to send stats.
        allowed_variant_groups: is_first_touch ? FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS : undefined,
      })
  );

  if (!selected_template?.item_id) {
    return {
      ok: false,
      skipped: true,
      reason: "template_not_found",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      property: summarizeProperty(property_item, owner_item),
      outbound_number_source: outbound_number.source,
      outbound_number_diagnostics: outbound_number.diagnostics || null,
    };
  }

  // Part 3 — Log the selected template so template rotation can be monitored.
  log.info("master_owner_feeder.template_rotation_selected", {
    master_owner_id,
    template_id: selected_template.item_id,
    use_case: selected_template.use_case || null,
    variant_group: selected_template.variant_group || null,
    rotation_key,
    recently_used_count: context?.recent?.recently_used_template_ids?.length || 0,
    brain_item_id: owner_brain_item_id ?? null,
  });

  // ── FINAL FIRST-TOUCH TEMPLATE GUARD ──────────────────────────────────────
  // Safety net: allowed_variant_groups in loadTemplate should prevent later-stage
  // templates from reaching here.  If one slips through anyway (e.g. a template
  // with a forbidden use_case but no variant_group), reject it and treat the
  // result as "no valid first-touch template available" rather than as an error.
  if (is_first_touch) {
    const tmpl_use_case = selected_template.use_case || null;
    const tmpl_variant = selected_template.variant_group || null;
    const use_case_forbidden =
      tmpl_use_case && FORBIDDEN_FIRST_TOUCH_USE_CASES.has(tmpl_use_case);
    const variant_not_allowed =
      tmpl_variant && !FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(tmpl_variant);

    if (use_case_forbidden || variant_not_allowed) {
      warn("master_owner_feeder.first_touch_template_guard_rejected", {
        master_owner_id,
        template_id: selected_template.item_id,
        tmpl_use_case,
        tmpl_variant,
        use_case_forbidden,
        variant_not_allowed,
        note: "allowed_variant_groups filter should have excluded this — check template data in Podio",
      });
      return {
        ok: false,
        skipped: true,
        reason: "no_valid_first_touch_template",
        owner: owner_summary,
        phone: selected_phone_record.summary,
        property: summarizeProperty(property_item, owner_item),
        first_touch: true,
        template_id: selected_template.item_id,
        tmpl_use_case,
        tmpl_variant,
      };
    }
  }
  // ──────────────────────────────────────────────────────────────────────────

  const render_result = renderTemplate({
    template_text: selected_template.text,
    context,
    overrides: {
      language,
      conversation_stage: route?.stage || stage_hint,
      lifecycle_stage: route?.lifecycle_stage || null,
      ai_route: route?.brain_ai_route || context.summary?.brain_ai_route || null,
    },
    use_case: selected_template?.use_case || effective_follow_up_plan?.template_lookup_use_case,
    variant_group: selected_template?.variant_group || effective_follow_up_plan?.variant_group,
  });

  if (
    render_result?.invalid_placeholders?.length ||
    render_result?.missing_required_placeholders?.length
  ) {
    if (is_first_touch) {
      warn("master_owner_feeder.first_touch_placeholder_validation_failed", {
        master_owner_id,
        detected_first_touch: true,
        property_id: property_item?.item_id ?? null,
        template_id: selected_template.item_id,
        use_case: selected_template.use_case ?? null,
        variant_group: selected_template.variant_group ?? null,
        invalid_placeholders: render_result?.invalid_placeholders || [],
        missing_required_placeholders: render_result?.missing_required_placeholders || [],
      });
    }
    return {
      ok: false,
      skipped: true,
      reason: "template_placeholder_validation_failed",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      property: summarizeProperty(property_item, owner_item),
      first_touch: is_first_touch,
      template_id: selected_template.item_id,
      invalid_placeholders: render_result?.invalid_placeholders || [],
      missing_required_placeholders:
        render_result?.missing_required_placeholders || [],
    };
  }

  const rendered_message_text = clean(render_result?.rendered_text || "");

  if (!rendered_message_text) {
    return {
      ok: false,
      skipped: true,
      reason: "rendered_message_empty",
      owner: owner_summary,
      phone: selected_phone_record.summary,
      property: summarizeProperty(property_item, owner_item),
      template_id: selected_template.item_id,
    };
  }

  const schedule = resolveQueueSchedule({
    now,
    timezone_label: resolved_timezone,
    contact_window: resolved_contact_window,
    distribution_key: rotation_key,
  });

  const plan = {
    master_owner_id,
    seller_id: owner_summary.seller_id,
    owner_name: owner_summary.owner_name,
    phone_item_id: selected_phone_record.phone_item_id,
    phone: selected_phone_record.summary,
    property: summarizeProperty(property_item, owner_item),
    brain_item_id: null,
    assigned_agent_id: sms_agent_id || assigned_agent_id || null,
    market_id,
    textgrid_number_item_id: outbound_number.textgrid_number_item_id,
    outbound_number_source: outbound_number.source,
    outbound_number_diagnostics: outbound_number.diagnostics || null,
    template_id: selected_template.item_id,
    template_title: selected_template.title || null,
    rendered_message_text,
    rendered_character_count: rendered_message_text.length,
    deferred_template_resolution: false,
    deferred_brain_suppression: true,
    schedule,
    route: {
      stage: route?.stage || stage_hint,
      lifecycle_stage: route?.lifecycle_stage || null,
      use_case: effective_follow_up_plan?.base_use_case || route?.use_case || "ownership_check",
      tone: effective_follow_up_plan?.tone || route?.tone || "Warm",
      variant_group:
        effective_follow_up_plan?.variant_group ||
        route?.variant_group ||
        "Stage 1 — Ownership Confirmation",
      language,
      persona:
        effective_follow_up_plan?.paired_with_agent_type ||
        route?.template_filters?.paired_with_agent_type ||
        route?.persona ||
        "Warm Professional",
      category: effective_follow_up_plan?.category || primary_category,
      secondary_category: effective_follow_up_plan?.secondary_category ?? secondary_category,
      sequence_position,
      next_expected_stage:
        effective_follow_up_plan?.next_expected_stage ||
        canonicalStageForUseCase(route?.use_case || "ownership_check"),
    },
    touch_number,
    send_priority,
    priority_score,
    suppression_window_days: cadence_days,
    next_follow_up_at: summarizeDate(next_follow_up_at),
    latest_contact_at: summarizeDate(resolved_latest_contact_at),
  };

  if (dry_run) {
    return {
      ok: true,
      skipped: false,
      dry_run: true,
      reason: "would_queue_master_owner_touch",
      owner: owner_summary,
      plan,
    };
  }

  // Part 4 — Deterministic idempotency key for queue rows.
  // If two cron runs attempt to create the same first/follow-up touch for the same
  // owner+phone, the pending duplicate check (findPendingDuplicate) blocks the second
  // run. The idempotency key is written to the queue-id field as a cross-run signal —
  // it makes duplicates identifiable even after the queue is processed.
  const idempotency_queue_id = [
    "mo",
    master_owner_id,
    selected_phone_record.phone_item_id,
    touch_number,
  ].join(":");

  const queue_result = await buildSendQueueItem({
    context,
    rendered_message_text,
    template_id: selected_template.item_id,
    template_item: selected_template,
    defer_message_resolution: false,
    textgrid_number_item_id: outbound_number.textgrid_number_item_id,
    scheduled_for_local: { start: schedule.scheduled_for_local },
    scheduled_for_utc: { start: schedule.scheduled_for_utc },
    timezone: resolved_timezone,
    contact_window: resolved_contact_window,
    send_priority,
    message_type:
      effective_follow_up_plan
        ? "Follow-Up"
        : deriveQueueMessageType(
            route?.stage || stage_hint,
            route?.lifecycle_stage || null
          ),
    max_retries: 3,
    queue_status: "Queued",
    dnc_check: "✅ Cleared",
    delivery_confirmed: "⏳ Pending",
    touch_number,
    queue_id: idempotency_queue_id,
    property_type: primary_category || null,
    secondary_category: secondary_category || null,
    use_case_template: selected_template?.use_case || null,
  });

  return {
    ok: true,
    skipped: false,
    dry_run: false,
    reason: "master_owner_touch_queued",
    owner: owner_summary,
    plan,
    queue_item_id: queue_result?.queue_item_id || null,
    queue_result,
  };
}

async function resolveMasterOwnerSource({
  source_view_id = null,
  source_view_name = null,
  log = logger,
} = {}) {
  const selector = toViewSelector(source_view_id, source_view_name);
  if (!selector) return null;

  const requested_view_id =
    source_view_id !== null && source_view_id !== undefined && source_view_id !== ""
      ? String(source_view_id).trim()
      : null;
  const requested_view_name = clean(source_view_name) || null;

  const resolved = await timedStage(
    log,
    "master_owner_feeder.resolve_source_view",
    {
      selector,
      requested_view_id,
      requested_view_name,
    },
    async () => {
      try {
        if (requested_view_name && !requested_view_id) {
          const views = await listMasterOwnerViews();
          const list = Array.isArray(views)
            ? views
            : Array.isArray(views?.views)
              ? views.views
              : [];
          const matched = list.find(
            (view) =>
              lower(view?.name || view?.title || "") === lower(requested_view_name)
          );
          if (matched) return matched;
        }

        return getMasterOwnerView(selector);
      } catch (error) {
        const status = error?.status ?? error?.cause?.status ?? null;
        if (status === 404) {
          throw Object.assign(new Error("Master Owner source view not found"), {
            code: "master_owner_view_not_found",
            cause: error,
            source_view_id: requested_view_id,
            source_view_name: requested_view_name,
          });
        }

        throw error;
      }
    }
  );

  const view_id = Number(
    resolved?.view_id ?? resolved?.id ?? resolved?.viewId ?? 0
  ) || null;
  const view_name = clean(resolved?.name || resolved?.title || requested_view_name || "");

  if (!view_id) {
    throw Object.assign(new Error("Master Owner source view not found"), {
      code: "master_owner_view_not_found",
      source_view_id: requested_view_id,
      source_view_name: requested_view_name,
    });
  }

  return {
    type: "view",
    view_id,
    view_name: view_name || null,
    requested_view_id,
    requested_view_name,
    raw: resolved,
  };
}

async function loadSeedOwner({
  master_owner_id = null,
  seller_id = null,
  runtime = null,
  log = logger,
} = {}) {
  if (master_owner_id) {
    return safeGetItem(master_owner_id, {
      runtime,
      log,
      call_name: "master_owner_feeder.seed_owner_get_item",
      meta: {
        master_owner_id,
      },
    });
  }

  if (seller_id) {
    return timedStage(
      log,
      "master_owner_feeder.seed_owner_lookup_by_seller_id",
      {
        seller_id,
      },
      () => findMasterOwnerBySellerId(seller_id)
    );
  }

  return null;
}

async function loadMasterOwnerBatch({
  fetch_limit,
  offset,
  test_mode = false,
  source = null,
  log = logger,
}) {
  if (source?.type === "view" && source?.view_id) {
    const batch = await timedStage(
      log,
      "master_owner_feeder.initial_master_owner_fetch",
      {
        offset,
        fetch_limit,
        sorted: false,
        filtered: true,
        source_type: "view",
        source_view_id: source.view_id,
        source_view_name: source.view_name,
      },
      () =>
        findMasterOwnerItemsByView(source.view_id, {
          limit: fetch_limit,
          offset,
        })
    );

    const items = Array.isArray(batch?.items) ? batch.items : [];

    return {
      items,
      raw_count: items.length,
      fallback_used: false,
      source_type: "view",
    };
  }

  if (test_mode) {
    const unfiltered_limit = Math.max(fetch_limit * 2, 20);
    const batch = await timedStage(
      log,
      "master_owner_feeder.initial_master_owner_fetch",
      {
        offset,
        fetch_limit,
        sorted: false,
        filtered: false,
        unfiltered_limit,
      },
      () =>
        findMasterOwnerItems(
          {},
          {
            limit: unfiltered_limit,
            offset,
          }
        )
    );

    const owners = Array.isArray(batch?.items) ? batch.items : [];
    return {
      items: owners
        .filter(
          (owner_item) =>
            lower(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.sms_eligible, null)) ===
            "yes"
        )
        .slice(0, fetch_limit),
      raw_count: owners.length,
      fallback_used: false,
      source_type: "recent_items",
    };
  }

  try {
    const batch = await timedStage(
      log,
      "master_owner_feeder.initial_master_owner_fetch",
      {
        offset,
        fetch_limit,
        sorted: true,
        filtered: true,
      },
      () =>
        findSmsEligibleMasterOwnerItems({
          limit: fetch_limit,
          offset,
          sort_by: MASTER_OWNER_FIELDS.master_owner_priority_score,
          sort_desc: true,
        })
    );

    return {
      items: Array.isArray(batch?.items) ? batch.items : [],
      raw_count: Array.isArray(batch?.items) ? batch.items.length : 0,
      fallback_used: false,
      source_type: "recent_items",
    };
  } catch (error) {
    const podio_status = error?.status ?? error?.cause?.status ?? null;

    if (!isTimeoutError(error) && podio_status !== 503) {
      throw error;
    }

    const unfiltered_limit = Math.max(fetch_limit * 2, 20);
    const batch = await timedStage(
      log,
      "master_owner_feeder.initial_master_owner_fetch_fallback",
      {
        offset,
        fetch_limit,
        sorted: false,
        filtered: false,
        unfiltered_limit,
      },
      () =>
        findMasterOwnerItems(
          {},
          {
            limit: unfiltered_limit,
            offset,
          }
        )
    );

    const owners = Array.isArray(batch?.items) ? batch.items : [];

    return {
      items: owners
        .filter(
          (owner_item) =>
            lower(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.sms_eligible, null)) ===
            "yes"
        )
        .slice(0, fetch_limit),
      raw_count: owners.length,
      fallback_used: true,
      source_type: "recent_items",
    };
  }
}

async function loadRawMasterOwnerBatch({
  fetch_limit,
  offset,
  log = logger,
}) {
  return timedStage(
    log,
    "master_owner_feeder.raw_master_owner_fetch",
    {
      offset,
      fetch_limit,
    },
    () =>
      findMasterOwnerItems(
        {},
        {
          limit: fetch_limit,
          offset,
        }
      )
  );
}

export async function diagnoseMasterOwnerOutboundFeeder({
  raw_scan_limit = 100,
  closest_limit = 10,
  passing_limit = 3,
  now = nowIso(),
} = {}) {
  const effective_raw_scan_limit = toPositiveInteger(raw_scan_limit, 100);
  const effective_closest_limit = toPositiveInteger(closest_limit, 10);
  const effective_passing_limit = toPositiveInteger(passing_limit, 3);
  const page_size = Math.min(20, effective_raw_scan_limit);
  const runtime = createRunState({
    dry_run: true,
    test_mode: true,
    page_size,
  });

  const exclusion_counts = new Map();
  const closest_candidates = [];
  const passing_candidates = [];
  let raw_scanned_count = 0;
  let offset = 0;

  while (raw_scanned_count < effective_raw_scan_limit) {
    const fetch_limit = Math.min(page_size, effective_raw_scan_limit - raw_scanned_count);
    const batch = await loadRawMasterOwnerBatch({
      fetch_limit,
      offset,
      log: logger,
    });

    const raw_items = Array.isArray(batch?.items) ? batch.items : [];
    if (!raw_items.length) break;

    const sms_eligible_owner_ids = [];

    for (const owner_item of raw_items) {
      runtime.owners_by_id.set(String(owner_item?.item_id || ""), owner_item);

      if (
        lower(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.sms_eligible, null)) === "yes"
      ) {
        sms_eligible_owner_ids.push(owner_item.item_id);
      }
    }

    if (sms_eligible_owner_ids.length) {
      await preloadOwnerHistories(sms_eligible_owner_ids, {
        runtime,
        log: logger,
      });
    }

    for (const owner_item of raw_items) {
      if (raw_scanned_count >= effective_raw_scan_limit) break;
      raw_scanned_count += 1;

      const owner_summary = summarizeMasterOwner(owner_item);
      let result = null;

      if (lower(owner_summary.sms_eligible) !== "yes") {
        result = {
          ok: false,
          skipped: true,
          reason: "sms_not_eligible",
          owner: owner_summary,
        };
      } else {
        try {
          const light_result = await evaluateOwner({
            owner_item,
            dry_run: true,
            create_brain_if_missing: false,
            now,
            runtime,
            evaluation_depth: "light",
          });

          if (light_result?.ok && !light_result?.skipped) {
            result = await evaluateOwner({
              owner_item,
              dry_run: true,
              create_brain_if_missing: false,
              now,
              runtime,
              evaluation_depth: "full",
            });
          } else {
            result = light_result;
          }
        } catch (error) {
          result = buildOwnerErrorResult(owner_item, error, "diagnostics");
        }
      }

      const reason = result?.reason || "unknown";
      exclusion_counts.set(reason, (exclusion_counts.get(reason) || 0) + 1);

      closest_candidates.push({
        master_owner_id: owner_summary.item_id,
        owner_name_masked: maskOwnerName(owner_summary.owner_name),
        seller_id: stripHtml(owner_summary.seller_id),
        reason,
        closeness_score: classifyCloseness(reason, result),
        priority_score: Number(result?.plan?.priority_score || 0),
      });

      if (result?.ok && !result?.skipped && passing_candidates.length < effective_passing_limit) {
        passing_candidates.push({
          master_owner_id: owner_summary.item_id,
          owner_name_masked: maskOwnerName(owner_summary.owner_name),
          seller_id: stripHtml(owner_summary.seller_id),
        });
      }
    }

    offset += raw_items.length;
  }

  const top_exclusion_reasons = [...exclusion_counts.entries()]
    .map(([reason, count]) => ({ reason, count }))
    .sort((left, right) => right.count - left.count)
    .slice(0, 20);

  const nearest_candidates = [...closest_candidates]
    .sort((left, right) => {
      if (right.closeness_score !== left.closeness_score) {
        return right.closeness_score - left.closeness_score;
      }

      if (right.priority_score !== left.priority_score) {
        return right.priority_score - left.priority_score;
      }

      return Number(right.master_owner_id || 0) - Number(left.master_owner_id || 0);
    })
    .slice(0, effective_closest_limit);

  return {
    ok: true,
    raw_scan_limit: effective_raw_scan_limit,
    raw_scanned_count,
    top_exclusion_reasons,
    nearest_candidates,
    passing_candidates,
  };
}

export async function auditMasterOwnerSmsEligibleValues({
  raw_scan_limit = 1000,
} = {}) {
  const effective_raw_scan_limit = toPositiveInteger(raw_scan_limit, 1000);
  const page_size = Math.min(20, effective_raw_scan_limit);
  const field_options = [
    { id: 1, text: "Yes" },
    { id: 2, text: "No" },
  ];

  const counts = {
    yes: 0,
    no: 0,
    blank_or_null: 0,
    unexpected: 0,
  };

  const raw_value_counts = new Map();
  const unexpected_labels = new Map();
  const option_id_counts = new Map();
  const sample_item_ids_by_bucket = {
    yes: [],
    no: [],
    blank_or_null: [],
    unexpected: [],
  };

  let raw_scanned_count = 0;
  let offset = 0;

  while (raw_scanned_count < effective_raw_scan_limit) {
    const fetch_limit = Math.min(page_size, effective_raw_scan_limit - raw_scanned_count);
    const batch = await loadRawMasterOwnerBatch({
      fetch_limit,
      offset,
      log: logger,
    });

    const raw_items = Array.isArray(batch?.items) ? batch.items : [];
    if (!raw_items.length) break;

    for (const owner_item of raw_items) {
      if (raw_scanned_count >= effective_raw_scan_limit) break;
      raw_scanned_count += 1;

      const values = getFieldValues(owner_item, MASTER_OWNER_FIELDS.sms_eligible);
      const first = values[0] ?? null;
      const raw_value = first?.value ?? first ?? null;
      const raw_label = clean(
        raw_value?.text ??
          (typeof raw_value === "string" ? raw_value : null)
      );
      const option_id =
        raw_value?.id ??
        raw_value?.option_id ??
        first?.id ??
        first?.option_id ??
        null;

      const raw_key = raw_label || "(blank)";
      raw_value_counts.set(raw_key, (raw_value_counts.get(raw_key) || 0) + 1);

      if (option_id !== null && option_id !== undefined && option_id !== "") {
        const option_key = String(option_id);
        const current = option_id_counts.get(option_key) || {
          option_id,
          text: raw_label || null,
          count: 0,
        };
        current.count += 1;
        if (!current.text && raw_label) current.text = raw_label;
        option_id_counts.set(option_key, current);
      }

      const item_id = owner_item?.item_id ?? null;
      const normalized = lower(raw_label);

      if (!raw_label) {
        counts.blank_or_null += 1;
        if (sample_item_ids_by_bucket.blank_or_null.length < 10) {
          sample_item_ids_by_bucket.blank_or_null.push(item_id);
        }
        continue;
      }

      if (normalized === "yes") {
        counts.yes += 1;
        if (sample_item_ids_by_bucket.yes.length < 10) {
          sample_item_ids_by_bucket.yes.push(item_id);
        }
        continue;
      }

      if (normalized === "no") {
        counts.no += 1;
        if (sample_item_ids_by_bucket.no.length < 10) {
          sample_item_ids_by_bucket.no.push(item_id);
        }
        continue;
      }

      counts.unexpected += 1;
      if (sample_item_ids_by_bucket.unexpected.length < 10) {
        sample_item_ids_by_bucket.unexpected.push(item_id);
      }

      const unexpected = unexpected_labels.get(raw_label) || {
        label: raw_label,
        count: 0,
        option_ids: new Set(),
      };
      unexpected.count += 1;
      if (option_id !== null && option_id !== undefined && option_id !== "") {
        unexpected.option_ids.add(option_id);
      }
      unexpected_labels.set(raw_label, unexpected);
    }

    offset += raw_items.length;
  }

  const toPercent = (count) =>
    raw_scanned_count > 0 ? Number(((count / raw_scanned_count) * 100).toFixed(2)) : 0;

  return {
    ok: true,
    raw_scan_limit: effective_raw_scan_limit,
    raw_scanned_count,
    field: {
      external_id: MASTER_OWNER_FIELDS.sms_eligible,
      type: "category",
      multiple: false,
      schema_options: field_options,
    },
    value_counts: {
      yes: counts.yes,
      no: counts.no,
      blank_or_null: counts.blank_or_null,
      unexpected: counts.unexpected,
    },
    value_percentages: {
      yes: toPercent(counts.yes),
      no: toPercent(counts.no),
      blank_or_null: toPercent(counts.blank_or_null),
      unexpected: toPercent(counts.unexpected),
    },
    raw_value_breakdown: [...raw_value_counts.entries()]
      .map(([value, count]) => ({ value, count, percent: toPercent(count) }))
      .sort((left, right) => right.count - left.count),
    observed_option_ids: [...option_id_counts.values()]
      .sort((left, right) => right.count - left.count)
      .map((entry) => ({
        option_id: entry.option_id,
        text: entry.text,
        count: entry.count,
        percent: toPercent(entry.count),
      })),
    unexpected_labels: [...unexpected_labels.values()]
      .map((entry) => ({
        label: entry.label,
        count: entry.count,
        percent: toPercent(entry.count),
        option_ids: [...entry.option_ids.values()].sort((left, right) => Number(left) - Number(right)),
      }))
      .sort((left, right) => right.count - left.count),
    sample_item_ids_by_bucket,
  };
}

export async function runMasterOwnerOutboundFeeder({
  limit = DEFAULT_BATCH_SIZE,
  scan_limit = DEFAULT_SCAN_LIMIT,
  dry_run = false,
  seller_id = null,
  master_owner_id = null,
  source_view_id = null,
  source_view_name = null,
  now = nowIso(),
  create_brain_if_missing = !dry_run,
  test_mode = false,
} = {}) {
  const effective_limit = test_mode
    ? Math.min(toPositiveInteger(limit, DEFAULT_BATCH_SIZE), SAFE_TEST_LIMIT)
    : toPositiveInteger(limit, DEFAULT_BATCH_SIZE);
  const effective_scan_limit = test_mode
    ? Math.min(toPositiveInteger(scan_limit, DEFAULT_SCAN_LIMIT), SAFE_TEST_SCAN_LIMIT)
    : toPositiveInteger(scan_limit, DEFAULT_SCAN_LIMIT);
  const page_size = test_mode
    ? Math.min(Math.max(effective_limit * 2, effective_scan_limit), SAFE_TEST_SCAN_LIMIT)
    : Math.min(Math.max(effective_limit * 4, 50), 100);
  const raw_scan_cap = test_mode
    ? 100
    : Math.max(effective_scan_limit * 10, 200);

  function ownerIdFromResult(result) {
    return result?.plan?.master_owner_id ?? result?.owner?.item_id ?? null;
  }

  let source = null;

  try {
    source = await resolveMasterOwnerSource({
      source_view_id,
      source_view_name,
      log: logger,
    });
    const runtime = createRunState({
      dry_run,
      test_mode,
      page_size,
      source,
    });

    info("master_owner_feeder.run_started", {
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      requested_limit: limit,
      requested_scan_limit: scan_limit,
      dry_run,
      test_mode,
      page_size,
      raw_scan_cap,
      seller_id: seller_id || null,
      master_owner_id: master_owner_id || null,
      source: summarizeSource(source),
      create_brain_if_missing,
    });

    const seed_owner = await loadSeedOwner({
      master_owner_id,
      seller_id,
      runtime,
      log: logger,
    });
    const results = [];
    const actionable = [];

    if ((master_owner_id || seller_id) && !seed_owner?.item_id) {
      return {
        ok: false,
        dry_run,
        test_mode,
        run_started_at: now,
        reason: "master_owner_not_found",
        seller_id: seller_id || null,
        master_owner_id: master_owner_id || null,
        scanned_count: 0,
        queued_count: 0,
        skipped_count: 0,
        limit: effective_limit,
        scan_limit: effective_scan_limit,
        results: [],
      };
    }

    if (seed_owner?.item_id) {
      runtime.owners_by_id.set(String(seed_owner.item_id), seed_owner);
      await preloadOwnerHistories([seed_owner.item_id], { runtime, log: logger });

      try {
        const result = await evaluateOwner({
          owner_item: seed_owner,
          dry_run,
          create_brain_if_missing,
          now,
          runtime,
          evaluation_depth: "full",
        });

        results.push(result);
        if (result?.ok && !result?.skipped) actionable.push(result);
      } catch (error) {
        results.push(buildOwnerErrorResult(seed_owner, error, "full"));
      }
    } else {
      let scanned_count = 0;
      let offset = 0;
      let raw_scanned_count = 0;

      while (
        scanned_count < effective_scan_limit &&
        raw_scanned_count < raw_scan_cap
      ) {
        const remaining = effective_scan_limit - scanned_count;
        const fetch_limit = Math.min(page_size, remaining);

        const batch = await loadMasterOwnerBatch({
          fetch_limit,
          offset,
          test_mode,
          source,
          log: logger,
        });

        const owners = Array.isArray(batch?.items) ? batch.items : [];
        const raw_count = Math.max(batch?.raw_count || 0, owners.length);

        if (!owners.length && raw_count <= 0) break;
        raw_scanned_count += raw_count;

        for (const owner_item of owners) {
          runtime.owners_by_id.set(String(owner_item.item_id), owner_item);
        }

        if (owners.length) {
          await preloadOwnerHistories(
            owners.map((owner_item) => owner_item?.item_id),
            {
              runtime,
              log: logger,
            }
          );
        }

        for (const owner_item of owners) {
          scanned_count += 1;

          try {
            const result = await evaluateOwner({
              owner_item,
              dry_run: true,
              create_brain_if_missing: false,
              now,
              runtime,
              evaluation_depth: "light",
            });

            results.push(result);

            if (result?.ok && !result?.skipped) {
              actionable.push(result);
            }
          } catch (error) {
            results.push(buildOwnerErrorResult(owner_item, error, "light"));
          }

          if (scanned_count >= effective_scan_limit) {
            break;
          }
        }

        offset += raw_count;
      }

      runtime.raw_scanned_count = raw_scanned_count;
    }

    const ranked_candidates = [...actionable].sort(
      (left, right) =>
        Number(right?.plan?.priority_score || 0) -
        Number(left?.plan?.priority_score || 0)
    );
    // Evaluate all ranked candidates so dry-run diagnostics accurately reflect
    // what live mode would do.  Previously the dry-run window was capped at
    // effective_limit * 2, which caused the majority of eligible candidates to
    // appear as outside_deep_evaluation_budget even when they would have been
    // queueable.  The loop already stops at successful_count >= effective_limit,
    // so the only practical difference is that we now see real skip reasons
    // instead of the misleading outside_deep_evaluation_budget catch-all.
    const priority_window = ranked_candidates;
    const deep_results_by_owner_id = new Map();
    const final_selected_owner_ids = new Set();
    let successful_count = 0;

    for (const candidate of priority_window) {
      if (successful_count >= effective_limit) break;

      const candidate_owner_id = String(ownerIdFromResult(candidate) || "");
      const owner_item =
        runtime.owners_by_id.get(candidate_owner_id) ||
        (await safeGetItem(candidate_owner_id, {
          runtime,
          log: logger,
          call_name: "master_owner_feeder.rehydrate_master_owner",
          meta: {
            candidate_owner_id,
          },
        }));

      if (!owner_item?.item_id) {
        deep_results_by_owner_id.set(candidate_owner_id, {
          ok: false,
          skipped: true,
          reason: "master_owner_not_found",
          owner: candidate?.owner || null,
          plan: candidate?.plan || null,
        });
        continue;
      }

      runtime.owners_by_id.set(candidate_owner_id, owner_item);

      let executed = null;

      try {
        executed = await evaluateOwner({
          owner_item,
          dry_run,
          create_brain_if_missing,
          now,
          runtime,
          evaluation_depth: "full",
        });
      } catch (error) {
        executed = buildOwnerErrorResult(
          owner_item,
          error,
          dry_run ? "dry_run_full" : "full"
        );
      }

      deep_results_by_owner_id.set(candidate_owner_id, executed);

      if (executed?.ok && !executed?.skipped) {
        final_selected_owner_ids.add(candidate_owner_id);
        successful_count += 1;
      }
    }

    const ranked_owner_ids = new Set(
      ranked_candidates
        .map((result) => String(ownerIdFromResult(result) || ""))
        .filter(Boolean)
    );

    const final_results = results.map((result) => {
      const owner_id = String(ownerIdFromResult(result) || "");
      if (result?.skipped) return result;

      if (deep_results_by_owner_id.has(owner_id)) {
        const resolved = deep_results_by_owner_id.get(owner_id);

        if (resolved?.ok && !resolved?.skipped && !final_selected_owner_ids.has(owner_id)) {
          return {
            ...resolved,
            ok: false,
            skipped: true,
            reason: "outside_batch_limit",
          };
        }

        return resolved;
      }

      if (ranked_owner_ids.has(owner_id)) {
        return {
          ...result,
          ok: false,
          skipped: true,
          reason: "outside_deep_evaluation_budget",
        };
      }

      return {
        ...result,
        ok: false,
        skipped: true,
        reason: "outside_batch_limit",
      };
    });

    const queued_results = final_results.filter((result) => result?.ok && !result?.skipped);
    const skipped_results = final_results.filter((result) => result?.skipped);
    const skip_reason_counts = countReasons(skipped_results);
    const eligible_owner_count = actionable.length;
    const queued_owner_ids = queued_results
      .map((result) => result?.plan?.master_owner_id ?? result?.owner?.item_id ?? null)
      .filter(Boolean);

    const summary = {
      ok: true,
      dry_run,
      test_mode,
      run_started_at: now,
      source: summarizeSource(source),
      reason:
        final_results.length > 0
          ? null
          : test_mode
            ? "no_sms_eligible_master_owners_found_within_raw_scan_cap"
            : "no_eligible_master_owners_found",
      scanned_count: final_results.length,
      raw_scanned_count: runtime.raw_scanned_count || final_results.length,
      raw_items_pulled: runtime.raw_scanned_count || final_results.length,
      eligible_owner_count,
      queued_count: queued_results.length,
      queued_owner_ids,
      deferred_resolution: {
        template: true,
        brain_suppression: true,
      },
      skipped_count: skipped_results.length,
      skip_reason_counts,
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      page_size,
      raw_scan_cap,
      results: final_results,
    };

    info("master_owner_feeder.run_completed", {
      dry_run,
      test_mode,
      source: summarizeSource(source),
      scanned_count: summary.scanned_count,
      raw_items_pulled: summary.raw_items_pulled,
      eligible_owner_count: summary.eligible_owner_count,
      queued_count: summary.queued_count,
      skipped_count: summary.skipped_count,
      top_skip_reasons: skip_reason_counts.slice(0, 10),
      page_size,
    });

    return summary;
  } catch (error) {
    const diagnostics = serializeFeederError(error);

    warn("master_owner_feeder.run_failed", {
      dry_run,
      test_mode,
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      error: diagnostics,
    });

    return {
      ok: false,
      dry_run,
      test_mode,
      run_started_at: now,
      source: summarizeSource(source, {
        requested_view_id: clean(source_view_id) || null,
        requested_view_name: clean(source_view_name) || null,
      }),
      reason:
        (diagnostics.stage === "master_owner_feeder.resolve_source_view" &&
        diagnostics.podio_status === 404)
          ? "master_owner_view_not_found"
          : isPodioRateLimitError(error)
            ? "master_owner_feeder_rate_limited"
          : diagnostics.timeout
            ? "master_owner_feeder_timeout"
            : "master_owner_feeder_failed",
      retry_after_seconds: isPodioRateLimitError(error)
        ? getPodioRetryAfterSeconds(error, null)
        : null,
      diagnostics,
      scanned_count: 0,
      queued_count: 0,
      skipped_count: 0,
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      page_size,
      results: [],
    };
  }
}

export default runMasterOwnerOutboundFeeder;

// ── Exported for testing ──────────────────────────────────────────────────────
export {
  detectFirstTouch,
  parseSellerIdLocation,
  normalizeStreetAddress,
  addressLookupVariants,
  buildSyntheticPropertyFromSellerId,
  findPendingDuplicate,
  deriveOwnerTouchCount,
  selectBestProperty,
  FORBIDDEN_FIRST_TOUCH_USE_CASES,
  FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES,
  FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
};
