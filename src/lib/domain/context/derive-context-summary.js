// ─── derive-context-summary.js ───────────────────────────────────────────
import {
  getCategoryValue,
  getFieldValues,
  getNumberValue,
  getTextValue,
  normalizeLanguage,
} from "@/lib/providers/podio.js";

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
function extractStreetAddress(property_item) {
  const values = getFieldValues(property_item, "property-address");
  const first = values[0];
  if (!first) return "";
  return first.street_address || first.value?.street_address || "";
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

  return {
    phone_item_id: phone_item?.item_id ?? null,
    brain_item_id: brain_item?.item_id ?? null,
    master_owner_item_id: master_owner_item?.item_id ?? null,
    owner_item_id: owner_item?.item_id ?? null,
    prospect_item_id: prospect_item?.item_id ?? null,
    property_item_id: property_item?.item_id ?? null,
    agent_item_id: agent_item?.item_id ?? null,
    market_item_id: market_item?.item_id ?? null,

    phone_hidden: getTextValue(phone_item, "phone-hidden", ""),
    canonical_e164: getTextValue(phone_item, "canonical-e164", ""),
    phone_activity_status: getCategoryValue(phone_item, "phone-activity-status", "Unknown"),
    phone_usage_2_months: getCategoryValue(phone_item, "phone-usage-2-months", null),
    phone_usage_12_months: getCategoryValue(phone_item, "phone-usage-12-months", null),
    engagement_tier: getCategoryValue(phone_item, "engagement-tier", null),
    do_not_call: getCategoryValue(phone_item, "do-not-call", "FALSE"),
    dnc_source: getCategoryValue(phone_item, "dnc-source", null),

    conversation_stage: getCategoryValue(brain_item, "conversation-stage", "Ownership"),
    brain_ai_route: getCategoryValue(brain_item, "ai-route", "Soft"),
    language_preference: normalizeLanguage(
      getCategoryValue(brain_item, "language-preference", "English")
    ),
    seller_profile: getCategoryValue(brain_item, "seller-profile", null),
    status_ai_managed: getCategoryValue(brain_item, "status-ai-managed", null),
    follow_up_trigger_state: getCategoryValue(brain_item, "follow-up-trigger-state", null),
    motivation_score: getNumberValue(brain_item, "seller-motivation-score", null),
    total_messages_sent: touch_count,
    last_inbound_message: getTextValue(brain_item, "last-inbound-message", ""),
    last_outbound_message: getTextValue(brain_item, "last-outbound-message", ""),

    owner_name,
    seller_first_name,
    contact_window: getCategoryValue(master_owner_item, "best-contact-window", null),

    property_address: titleCaseIfShouting(raw_property_address),
    property_city: titleCaseIfShouting(raw_property_city),
    property_state: getTextValue(property_item, "state", "") || "",

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

    market_name: getTextValue(market_item, "title", ""),
    market_state: getTextValue(market_item, "state", ""),
    market_timezone: getTextValue(market_item, "timezone", ""),
    market_area_code: getTextValue(market_item, "area-code", ""),
  };
}

export default deriveContextSummary;
