// ─── derive-context-summary.js ───────────────────────────────────────────
import {
  getCategoryValue,
  getDateValue,
  getFieldValues,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";
import { resolvePreferredContactLanguage } from "@/lib/domain/context/resolve-preferred-language.js";
import {
  normalizeUsPhoneToE164,
  sanitizeSmsTextValue,
} from "@/lib/sms/sanitize.js";

function clean(value) {
  return String(value ?? "").trim();
}

function titleCaseIfShouting(value = "") {
  const raw = clean(value);
  if (!raw) return "";

  const letters = raw.replace(/[^A-Za-z]+/g, "");
  if (!letters) return raw;

  const upper_ratio =
    letters.split("").filter((char) => char === char.toUpperCase()).length / letters.length;

  if (upper_ratio < 0.85) return raw;

  return raw
    .toLowerCase()
    .split(/(\s+|-|\/)/)
    .map((token) => {
      if (!token || /^\s+$/.test(token) || token === "-" || token === "/") return token;
      if (/^(n|s|e|w|ne|nw|se|sw)$/i.test(token)) return token.toUpperCase();
      return token.replace(/\b([a-z])([a-z']*)/g, (_match, first, rest) => {
        return `${first.toUpperCase()}${rest}`;
      });
    })
    .join("");
}

// Extracts just the street-address component from a Podio location field.
// Podio location fields expose geocoded sub-fields (street_address, city, state,
// postal_code) both directly on the value object and nested under value.value.
// The pre-formatted string (first.value.formatted or first.formatted) arrives in
// an unpredictable city-first order ("Jurupa Valley 92509 CA 7454 Mission Blvd"),
// so we always prefer the structured street_address sub-field and never fall back
// to formatted.  Callers that need the full address should use formatPropertyAddress
// in build-send-queue-item.js instead.
// When the field is stored as a plain text type (e.g. in tests or legacy imports),
// fall back to first.value directly.
function extractStreetAddress(property_item) {
  const values = getFieldValues(property_item, "property-address");
  const first = values[0];
  if (!first) return "";
  // Prefer structured location sub-field
  if (first.street_address) return first.street_address;
  if (first.value?.street_address) return first.value.street_address;
  // Fall back to plain text value (text-type field, not location-type)
  if (typeof first.value === "string") return first.value;
  return "";
}

function extractPostalCode(property_item) {
  const values = getFieldValues(property_item, "property-address");
  const first = values[0];
  if (!first) return "";
  return (
    first.postal_code ||
    first.zip ||
    first.value?.postal_code ||
    first.value?.zip ||
    ""
  );
}

function firstNonNull(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }
  return null;
}

export function deriveContextSummary({
  phone_item = null,
  brain_item = null,
  master_owner_item = null,
  owner_item = null,
  prospect_item = null,
  property_item = null,
  agent_item = null,
  market_item = null,
  touch_count = 0,
} = {}) {
  const trace = {
    owner_id:
      master_owner_item?.item_id ??
      owner_item?.item_id ??
      prospect_item?.item_id ??
      null,
    phone_item_id: phone_item?.item_id ?? null,
    brain_item_id: brain_item?.item_id ?? null,
    property_item_id: property_item?.item_id ?? null,
  };

  console.log("➡️ entering derive-context-summary", trace);

  const owner_name =
    firstNonNull(
      getTextValue(master_owner_item, "owner-full-name", ""),
      getTextValue(master_owner_item, "title", ""),
      getTextValue(owner_item, "owner-full-name", ""),
      getTextValue(prospect_item, "owner-full-name", ""),
      getTextValue(prospect_item, "name-of-contact", ""),
      getTextValue(prospect_item, "title", "")
    ) || "";
  const seller_first_name =
    firstNonNull(
      getTextValue(phone_item, "phone-first-name", ""),
      clean(owner_name).split(" ")[0]
    ) || "";
  const raw_property_address =
    firstNonNull(
      extractStreetAddress(property_item),
      getTextValue(property_item, "title", "")
    ) || "";
  const raw_property_city = getTextValue(property_item, "city", "") || "";
  const raw_phone_hidden = getTextValue(phone_item, "phone-hidden", "") || "";
  const raw_canonical_e164 = getTextValue(phone_item, "canonical-e164", "") || "";
  const resolved_phone_hidden = sanitizeSmsTextValue(raw_phone_hidden);
  const resolved_canonical_e164 =
    normalizeUsPhoneToE164(raw_canonical_e164) ||
    normalizeUsPhoneToE164(raw_phone_hidden);

  try {
    const summary = {
      phone_item_id: phone_item?.item_id ?? null,
      brain_item_id: brain_item?.item_id ?? null,
      master_owner_item_id: master_owner_item?.item_id ?? null,
      owner_item_id: owner_item?.item_id ?? null,
      prospect_item_id: prospect_item?.item_id ?? null,
      property_item_id: property_item?.item_id ?? null,
      agent_item_id: agent_item?.item_id ?? null,
      market_item_id: market_item?.item_id ?? null,

      phone_hidden: resolved_phone_hidden,
      canonical_e164: resolved_canonical_e164,
      phone_activity_status: getCategoryValue(phone_item, "phone-activity-status", "Unknown"),
      phone_usage_2_months: getCategoryValue(phone_item, "phone-usage-2-months", null),
      phone_usage_12_months: getCategoryValue(phone_item, "phone-usage-12-months", null),
      engagement_tier: getCategoryValue(phone_item, "engagement-tier", null),
      do_not_call: getCategoryValue(phone_item, "do-not-call", "FALSE"),
      dnc_source: getCategoryValue(phone_item, "dnc-source", null),

      conversation_stage: getCategoryValue(
        brain_item,
        "conversation-stage",
        "Ownership Confirmation"
      ),
      brain_ai_route: getCategoryValue(brain_item, "ai-route", "Unknown"),
      lifecycle_stage_number: getNumberValue(brain_item, "number", 1),
      current_seller_state: getCategoryValue(brain_item, "current-seller-state", "Unknown"),
      follow_up_step: getCategoryValue(brain_item, "follow-up-step", "None"),
      next_follow_up_due_at: getDateValue(brain_item, "next-follow-up-due-at", null),
      last_detected_intent: getCategoryValue(brain_item, "last-detected-intent", "Unknown"),
      language_preference: resolvePreferredContactLanguage({
        master_owner_item,
        owner_item,
        prospect_item,
        brain_item,
      }),
      seller_profile: getCategoryValue(brain_item, "seller-profile", null),
      status_ai_managed: getCategoryValue(brain_item, "status-ai-managed", null),
      deal_priority_tag: getCategoryValue(brain_item, "deal-prioirty-tag", null),
      follow_up_trigger_state: getCategoryValue(brain_item, "follow-up-trigger-state", null),
      motivation_score: getNumberValue(brain_item, "seller-motivation-score", null),
      risk_flags_ai: getCategoryValue(brain_item, "risk-flags-ai", null),
      seller_emotional_tone: getCategoryValue(brain_item, "category", "Unknown"),
      response_style_mode: getCategoryValue(brain_item, "category-2", "Unknown"),
      primary_objection_type: getCategoryValue(brain_item, "category-3", "Unknown"),
      seller_ask_price: getNumberValue(brain_item, "seller-asking-price", null),
      cash_offer_target: getNumberValue(brain_item, "cash-offer-target", null),
      creative_branch_eligibility: getCategoryValue(brain_item, "category-4", "Unknown"),
      deal_strategy_branch: getCategoryValue(brain_item, "category-5", "Unknown"),
      total_messages_sent: touch_count,
      last_message_summary_ai: getTextValue(brain_item, "transcript", ""),
      full_conversation_summary_ai: getTextValue(brain_item, "title", ""),
      ai_recommended_next_move: getTextValue(brain_item, "ais-recommended-next-move", ""),
      ai_next_message: getTextValue(brain_item, "ai-next-message", ""),
      last_inbound_message: getTextValue(brain_item, "last-inbound-message", ""),
      last_outbound_message: getTextValue(brain_item, "last-outbound-message", ""),

      owner_name: sanitizeSmsTextValue(owner_name),
      seller_first_name: sanitizeSmsTextValue(seller_first_name),
      contact_window: getCategoryValue(master_owner_item, "best-contact-window", null),

      property_address: sanitizeSmsTextValue(titleCaseIfShouting(raw_property_address)),
      property_city: sanitizeSmsTextValue(titleCaseIfShouting(raw_property_city)),
      property_state: sanitizeSmsTextValue(getTextValue(property_item, "state", "") || ""),
      property_zip: sanitizeSmsTextValue(extractPostalCode(property_item)),

      agent_name:
        firstNonNull(
          getTextValue(agent_item, "title", ""),
          getTextValue(agent_item, "agent-name", "")
        ) || "",
      agent_first_name:
        firstNonNull(
          getTextValue(agent_item, "first-name", ""),
          clean(getTextValue(agent_item, "title", "")).split(" ")[0],
          clean(getTextValue(agent_item, "agent-name", "")).split(" ")[0]
        ) || "",

      market_name: sanitizeSmsTextValue(getTextValue(market_item, "title", "")),
      market_state: sanitizeSmsTextValue(getTextValue(market_item, "state", "")),
      market_timezone: sanitizeSmsTextValue(getTextValue(market_item, "timezone", "")),
      market_area_code: sanitizeSmsTextValue(getTextValue(market_item, "area-code", "")),
    };

    summary.agent_name = sanitizeSmsTextValue(summary.agent_name);
    summary.agent_first_name = sanitizeSmsTextValue(summary.agent_first_name);

    console.log("⬅️ exiting derive-context-summary", {
      owner_id: trace.owner_id,
      conversation_stage: summary.conversation_stage,
      language_preference: summary.language_preference,
      property_item_id: summary.property_item_id,
    });

    return summary;
  } catch (error) {
    console.error("💥 derive-context-summary failed", {
      ...trace,
      message: error?.message ?? null,
      podio_status:
        error?.status ??
        error?.response?.status ??
        error?.cause?.status ??
        null,
    });
    throw error;
  }
}

export default deriveContextSummary;
