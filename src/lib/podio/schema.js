import APP_IDS from "@/lib/config/app-ids.js";
import PODIO_ATTACHED_BASE_SCHEMA from "@/lib/podio/schema-attached.generated.js";
import PODIO_ATTACHED_SCHEMA_SUPPLEMENT from "@/lib/podio/schema-attached-supplement.generated.js";
import { toPodioDateTimeString } from "@/lib/utils/dates.js";

export const PODIO_ATTACHED_SCHEMA = Object.freeze({
  ...PODIO_ATTACHED_BASE_SCHEMA,
  ...PODIO_ATTACHED_SCHEMA_SUPPLEMENT,
});

const MESSAGE_EVENT_SOURCE_APP_COMPAT_VALUES = new Set([
  "buyer disposition",
  "buyer thread",
  "conversation brain",
  "contracts",
  "external api",
  "internal verification",
  "manual",
  "runtime lock",
  "send queue",
  "system alert",
  "workflow automation",
]);

const MESSAGE_EVENT_PROCESSED_BY_COMPAT_VALUES = new Set([
  "manual sender",
  "gpt 4o ai",
  "mistral 7b ai",
  "autoresponder",
  "drip campaign",
  "scheduled campaign",
  "queue runner",
  "send now api",
  "system",
  "verification harness",
  "buyer blast",
  "buyer blast dry run",
  "buyer response webhook",
  "contract document archive",
]);

const MESSAGE_EVENT_DIRECTION_COMPAT_VALUES = new Set([
  "inbound",
  "outbound",
]);

const MESSAGE_EVENT_EVENT_TYPE_COMPAT_VALUES = new Set([
  "seller inbound sms",
  "seller outbound sms",
  "delivery update",
  "send failure",
  "stage transition",
  "ai classification",
  "manual note",
]);

const MESSAGE_EVENT_AI_ROUTE_COMPAT_VALUES = new Set([
  "ownership check",
  "identity",
  "offer",
  "objection handling",
  "wrong number",
  "follow up",
  "re engagement",
  "dispo buyer",
  "spanish route",
  "legal opt out",
  "ownership confirmation",
  "offer interest",
  "price discovery",
  "condition discovery",
  "offer positioning",
  "negotiation",
  "contract push",
  "dead lead handling",
  "dnc",
  "unknown",
]);

const MESSAGE_EVENT_DELIVERY_STATUS_COMPAT_VALUES = new Set([
  "pending",
  "sent",
  "delivered",
  "failed",
  "received",
]);

const MESSAGE_EVENT_PROVIDER_DELIVERY_STATUS_COMPAT_VALUES = new Set([
  "queued",
  "sending",
  "sent",
  "delivered",
  "failed",
  "undelivered",
  "unknown",
]);

const MESSAGE_EVENT_FAILURE_BUCKET_COMPAT_VALUES = new Set([
  "carrier rejection",
  "undelivered",
  "rate limited",
  "invalid number",
  "opt out dnc",
  "timeout",
  "system error",
  "other",
  "hard bounce",
  "soft bounce",
]);

const YES_NO_CATEGORY_COMPAT_VALUES = new Set(["yes", "no"]);

const SEND_QUEUE_CONTACT_WINDOW_COMPAT_VALUES = new Set([
  "8AM-9PM CT",
  "8AM-9PM ET",
  "8AM-9PM MT",
  "8AM-9PM PT",
  "8AM-9PM Local",
  "9AM-8PM CT",
  "9AM-11AM ET",
  "12PM-1PM ET",
  "5PM-9PM PT",
  "9AM-11AM PT",
  "11AM-1PM PT",
  "8AM-10AM ET",
  "9AM-8PM PT",
  "11AM-1PM ET",
  "5PM-8PM PT",
  "9AM-8PM ET",
  "7AM-9AM ET",
  "5PM-8PM ET",
  "12PM-1PM PT",
  "8AM-10AM PT",
  "10AM-12PM PT",
  "5PM-9PM ET",
  "6PM-9PM PT",
  "7AM-9AM PT",
  "6AM-8AM PT",
  "10AM-12PM ET",
  "12PM-1PM Local",
  "6PM-9PM MT",
  "9AM-8PM Local",
  "8AM-10AM CT",
  "8AM-10AM Local",
  "7AM-9AM CT",
  "6AM-8AM ET",
  "6PM-9PM ET",
  "9AM-8PM MT",
  "5PM-9PM Local",
  "12PM-1PM CT",
  "12PM-1PM MT",
  "10AM-12PM CT",
  "11AM-1PM MT",
  "5PM-8PM CT",
  "10AM-12PM MT",
  "11AM-1PM CT",
  "12PM-2PM ET",
  "6PM-9PM Local",
  "12PM-2PM CT",
  "12PM-2PM PT",
  "3PM-6PM PT",
  "6AM-8AM CT",
  "3PM-6PM ET",
  "11AM-1PM Local",
  "3PM-6PM CT",
  "9AM-11AM Local",
  "12PM-2PM Local",
  "9AM-11AM CT",
  "3PM-6PM MT",
  "3PM-6PM Local",
  "9AM-11AM MT",
  "12PM-2PM MT",
  "5PM-8PM MT",
  "10AM-12PM Local",
  "5PM-9PM CT",
  "7AM-9AM Local",
  "7AM-9AM MT",
  "8AM-10AM MT",
  "6PM-9PM CT",
  "6AM-8AM MT",
  "5PM-9PM MT",
  "6AM-8AM Local",
  "5PM-8PM Local",
].map((value) => normalizeCategoryText(value)));

const SEND_QUEUE_CURRENT_STAGE_COMPAT_VALUES = new Set([
  "cold outbound",
  "ownership confirmation",
  "offer interest confirmation",
  "seller price discovery",
  "condition timeline discovery",
  "offer positioning",
  "negotiation",
  "verbal acceptance lock",
  "contract out",
  "signed closing",
  "closed dead outcome",
]);

const BRAIN_CONVERSATION_STAGE_COMPAT_VALUES = new Set([
  "ownership confirmation",
  "offer interest confirmation",
  "seller price discovery",
  "condition timeline discovery",
  "offer positioning",
  "negotiation",
  "verbal acceptance lock",
  "contract out",
  "signed closing",
  "closed dead outcome",
]);

const BRAIN_AI_ROUTE_COMPAT_VALUES = new Set([
  "ownership confirmation",
  "offer interest",
  "price discovery",
  "condition discovery",
  "offer positioning",
  "negotiation",
  "objection handling",
  "re engagement",
  "contract push",
  "dead lead handling",
  "wrong number",
  "dnc",
  "unknown",
]);

const BRAIN_CURRENT_SELLER_STATE_COMPAT_VALUES = new Set([
  "unconfirmed owner",
  "confirmed owner",
  "no longer owner",
  "open to offer",
  "maybe open",
  "not interested",
  "wants offer first",
  "price given",
  "no price given",
  "condition unknown",
  "condition known",
  "near range",
  "above range",
  "negotiating",
  "ready for contract",
  "signed",
  "closed",
  "dead",
  "dnc",
  "wrong number",
  "unknown",
]);

const BRAIN_FOLLOW_UP_STEP_COMPAT_VALUES = new Set([
  "a",
  "b",
  "c",
  "d",
  "final",
  "none",
]);

const BRAIN_LAST_INTENT_COMPAT_VALUES = new Set([
  "ownership confirmed",
  "ownership denied",
  "open to offer",
  "not interested",
  "wants offer",
  "asking price given",
  "wants higher price",
  "condition mentioned",
  "timeline mentioned",
  "negotiation",
  "contract ready",
  "wrong number",
  "dnc",
  "unknown",
]);

const BRAIN_STATUS_AI_MANAGED_COMPAT_VALUES = new Set([
  "active negotiation",
  "warm lead",
  "hot opportunity",
  "waiting on seller",
  "ai follow up running",
  "cold no response",
  "under contract",
  "closed",
  "dnc",
  "wrong number",
  "paused",
  "manual review",
]);

const BRAIN_FOLLOW_UP_TRIGGER_STATE_COMPAT_VALUES = new Set([
  "ai running",
  "waiting",
  "paused",
  "manual override",
  "completed",
  "expired",
]);

const BRAIN_EMOTIONAL_TONE_COMPAT_VALUES = new Set([
  "calm",
  "anxious",
  "motivated",
  "resistant",
  "grieving",
  "confused",
  "angry",
  "excited",
  "indifferent",
  "unknown",
]);

const BRAIN_RESPONSE_STYLE_COMPAT_VALUES = new Set([
  "empathetic",
  "direct",
  "formal",
  "casual",
  "spiritual",
  "urgent",
  "humorous",
  "unknown",
]);

const BRAIN_PRIMARY_OBJECTION_COMPAT_VALUES = new Set([
  "price too low",
  "not ready to sell",
  "has agent",
  "inherited dispute",
  "market comparing",
  "wants retail",
  "probate pending",
  "no objection",
  "unknown",
]);

const BRAIN_CREATIVE_ELIGIBILITY_COMPAT_VALUES = new Set([
  "yes",
  "no",
  "maybe",
  "unknown",
]);

const BRAIN_DEAL_STRATEGY_BRANCH_COMPAT_VALUES = new Set([
  "cash",
  "seller finance",
  "subject to",
  "novation",
  "lease option",
  "hybrid",
  "nurture",
  "dnc",
  "wrong number",
  "unknown",
]);

function isValidSendQueueContactWindow(value) {
  return /^(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*-\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s+(?:CT|ET|MT|PT|AT|HT|Local)$/i.test(
    String(value ?? "").trim()
  );
}

function normalizeCategoryText(value) {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function toFiniteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toAppReferenceIds(value) {
  if (value === null || value === undefined || value === "") return value;

  const list = Array.isArray(value) ? value : [value];

  return list
    .map((entry) => {
      if (typeof entry === "number") return entry;
      if (typeof entry === "string" && entry.trim() !== "") {
        const parsed = Number(entry);
        return Number.isFinite(parsed) ? parsed : null;
      }
      if (typeof entry?.item_id === "number") return entry.item_id;
      if (typeof entry?.value?.item_id === "number") return entry.value.item_id;
      if (typeof entry?.value === "number") return entry.value;
      return null;
    })
    .filter(Boolean);
}

export function hasAttachedSchema(app_id) {
  return Boolean(PODIO_ATTACHED_SCHEMA[String(app_id)]);
}

export function getAttachedAppSchema(app_id) {
  return PODIO_ATTACHED_SCHEMA[String(app_id)] || null;
}

export function getAttachedFieldSchema(app_id, external_id) {
  return getAttachedAppSchema(app_id)?.fields?.[external_id] || null;
}

export function getCategoryOptionId(app_id, external_id, value) {
  const field = getAttachedFieldSchema(app_id, external_id);
  if (!field || field.type !== "category") return null;

  const numeric = toFiniteNumber(value);
  if (numeric !== null) {
    return field.options.some((option) => option.id === numeric) ? numeric : null;
  }

  const normalized = normalizeCategoryText(value);
  if (!normalized) return null;

  return (
    field.options.find((option) => normalizeCategoryText(option.text) === normalized)?.id ||
    null
  );
}

function shouldAllowRawCategoryCompatibility(app_id, external_id, value) {
  const normalized_app_id = Number(app_id);
  const normalized_external_id = cleanExternalId(external_id);
  const normalized = normalizeCategoryText(value);

  if (normalized_app_id === APP_IDS.message_events) {
    if (normalized_external_id === "source-app") {
      return MESSAGE_EVENT_SOURCE_APP_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "processed-by") {
      return MESSAGE_EVENT_PROCESSED_BY_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "direction") {
      return MESSAGE_EVENT_DIRECTION_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "category") {
      return MESSAGE_EVENT_EVENT_TYPE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "ai-route") {
      return MESSAGE_EVENT_AI_ROUTE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "status-3") {
      return MESSAGE_EVENT_DELIVERY_STATUS_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "delivery-status") {
      return MESSAGE_EVENT_PROVIDER_DELIVERY_STATUS_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "failure-bucket") {
      return MESSAGE_EVENT_FAILURE_BUCKET_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "is-final-failure") {
      return YES_NO_CATEGORY_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "is-opt-out") {
      return YES_NO_CATEGORY_COMPAT_VALUES.has(normalized);
    }
  }

  if (normalized_app_id === APP_IDS.message_events && normalized_external_id === "source-app") {
    return MESSAGE_EVENT_SOURCE_APP_COMPAT_VALUES.has(normalized);
  }

  if (normalized_app_id === APP_IDS.send_queue && normalized_external_id === "contact-window") {
    return (
      SEND_QUEUE_CONTACT_WINDOW_COMPAT_VALUES.has(normalized) ||
      isValidSendQueueContactWindow(value)
    );
  }

  if (normalized_app_id === APP_IDS.send_queue && normalized_external_id === "current-stage") {
    return SEND_QUEUE_CURRENT_STAGE_COMPAT_VALUES.has(normalized);
  }

  if (normalized_app_id === APP_IDS.ai_conversation_brain) {
    if (normalized_external_id === "conversation-stage") {
      return BRAIN_CONVERSATION_STAGE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "ai-route") {
      return BRAIN_AI_ROUTE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "current-seller-state") {
      return BRAIN_CURRENT_SELLER_STATE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "follow-up-step") {
      return BRAIN_FOLLOW_UP_STEP_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "last-detected-intent") {
      return BRAIN_LAST_INTENT_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "status-ai-managed") {
      return BRAIN_STATUS_AI_MANAGED_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "follow-up-trigger-state") {
      return BRAIN_FOLLOW_UP_TRIGGER_STATE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "category") {
      return BRAIN_EMOTIONAL_TONE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "category-2") {
      return BRAIN_RESPONSE_STYLE_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "category-3") {
      return BRAIN_PRIMARY_OBJECTION_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "category-4") {
      return BRAIN_CREATIVE_ELIGIBILITY_COMPAT_VALUES.has(normalized);
    }
    if (normalized_external_id === "category-5") {
      return BRAIN_DEAL_STRATEGY_BRANCH_COMPAT_VALUES.has(normalized);
    }
  }

  return false;
}

export function shouldAllowRawCategoryCompatibilityValue(app_id, external_id, value) {
  return shouldAllowRawCategoryCompatibility(app_id, external_id, value);
}

function cleanExternalId(value) {
  return String(value ?? "").trim();
}

function normalizeCategoryValue(app_id, external_id, value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;

  const field = getAttachedFieldSchema(app_id, external_id);
  if (!field) return value;

  const rawValues = Array.isArray(value) ? value : [value];
  const option_ids = rawValues
    .map((entry) => getCategoryOptionId(app_id, external_id, entry))
    .filter((entry) => entry !== null);

  if (!option_ids.length) {
    if (rawValues.every((entry) => shouldAllowRawCategoryCompatibility(app_id, external_id, entry))) {
      return field.multiple ? rawValues : rawValues[0];
    }

    throw new Error(
      `[Podio] Invalid category value "${value}" for ${getAttachedAppSchema(app_id)?.app_name}::${external_id}`
    );
  }

  return field.multiple ? option_ids : option_ids[0];
}

function normalizeDateValue(value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;
  if (typeof value === "string") {
    return { start: toPodioDateTimeString(value) || value };
  }
  if (value instanceof Date) {
    return { start: toPodioDateTimeString(value) || value };
  }
  if (typeof value === "object") {
    if (value.start || value.end || value.start_date || value.end_date) {
      return {
        ...value,
        ...(value.start
          ? { start: toPodioDateTimeString(value.start) || value.start }
          : {}),
        ...(value.end
          ? { end: toPodioDateTimeString(value.end) || value.end }
          : {}),
        ...(value.start_date
          ? {
              start_date:
                toPodioDateTimeString(value.start_date) || value.start_date,
            }
          : {}),
        ...(value.end_date
          ? { end_date: toPodioDateTimeString(value.end_date) || value.end_date }
          : {}),
      };
    }
  }
  return value;
}

function normalizeMoneyValue(app_id, external_id, value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;

  if (typeof value === "object" && value !== null && "value" in value) {
    return {
      value: toFiniteNumber(value.value) ?? value.value,
      currency:
        value.currency ||
        getAttachedFieldSchema(app_id, external_id)?.allowed_currencies?.[0] ||
        "USD",
    };
  }

  const numeric = toFiniteNumber(value);
  if (numeric === null) {
    throw new Error(
      `[Podio] Invalid money value "${value}" for ${getAttachedAppSchema(app_id)?.app_name}::${external_id}`
    );
  }

  return {
    value: numeric,
    currency:
      getAttachedFieldSchema(app_id, external_id)?.allowed_currencies?.[0] || "USD",
  };
}

function normalizeNumberValue(value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;
  return toFiniteNumber(value) ?? value;
}

function normalizeAppValue(value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;
  return toAppReferenceIds(value);
}

export function normalizePodioFieldValue(app_id, external_id, value) {
  const field = getAttachedFieldSchema(app_id, external_id);
  if (!field) return value;

  switch (field.type) {
    case "category":
      return normalizeCategoryValue(app_id, external_id, value);
    case "date":
      return normalizeDateValue(value);
    case "money":
      return normalizeMoneyValue(app_id, external_id, value);
    case "number":
    case "progress":
      return normalizeNumberValue(value);
    case "app":
    case "contact":
    case "member":
      return normalizeAppValue(value);
    default:
      return value;
  }
}

export function normalizePodioFieldMap(app_id, fields = {}) {
  if (!fields || typeof fields !== "object") return fields;

  const appSchema = getAttachedAppSchema(app_id);
  if (!appSchema) return fields;

  const normalized = {};

  for (const [external_id, rawValue] of Object.entries(fields)) {
    if (!appSchema.fields[external_id]) {
      throw new Error(`[Podio] Unknown field for ${appSchema.app_name}: ${external_id}`);
    }

    normalized[external_id] = normalizePodioFieldValue(app_id, external_id, rawValue);
  }

  return normalized;
}

/**
 * Like normalizePodioFieldMap but post-processes for the Podio filter API:
 * category values must always be arrays of option IDs.
 */
export function normalizePodioFilterMap(app_id, filters = {}) {
  const normalized = normalizePodioFieldMap(app_id, filters);
  if (!normalized || typeof normalized !== "object") return normalized;

  const appSchema = getAttachedAppSchema(app_id);
  if (!appSchema) return normalized;

  const result = {};
  for (const [external_id, value] of Object.entries(normalized)) {
    const field = appSchema.fields[external_id];
    if (
      field?.type === "category" &&
      value !== null &&
      value !== undefined &&
      !Array.isArray(value)
    ) {
      result[external_id] = [value];
    } else {
      result[external_id] = value;
    }
  }
  return result;
}

export default {
  PODIO_ATTACHED_SCHEMA,
  hasAttachedSchema,
  getAttachedAppSchema,
  getAttachedFieldSchema,
  getCategoryOptionId,
  normalizePodioFieldValue,
  normalizePodioFieldMap,
  normalizePodioFilterMap,
};
