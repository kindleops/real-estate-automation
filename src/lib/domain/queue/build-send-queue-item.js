// ─── build-send-queue-item.js ────────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  createItem,
  getCategoryValue,
  getFieldValues,
  getFirstAppReferenceId,
  getTextValue,
  updateItem,
  PodioError,
} from "@/lib/providers/podio.js";

import { getCategoryOptionId, getAttachedFieldSchema } from "@/lib/podio/schema.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import { resolveTemplateFieldReference } from "@/lib/domain/templates/template-reference.js";
import { warn } from "@/lib/logging/logger.js";

// ══════════════════════════════════════════════════════════════════════════
// REAL SEND QUEUE FIELD IDS
// ══════════════════════════════════════════════════════════════════════════

const QUEUE_FIELDS = {
  queue_id_2: "queue-id-2",
  queue_sequence: "queue-sequence",

  scheduled_for_local: "scheduled-for-local",
  scheduled_for_utc: "scheduled-for-utc",
  timezone: "timezone",
  contact_window: "contact-window",
  send_priority: "send-priority",
  retry_count: "retry-count",
  max_retries: "max-retries",

  queue_status: "queue-status",
  sent_at: "sent-at",
  delivered_at: "delivered-at",
  failed_reason: "failed-reason",
  delivery_confirmed: "delivery-confirmed",

  master_owner: "master-owner",
  prospects: "prospects",
  properties: "properties",
  phone_number: "phone-number",
  market: "market",
  sms_agent: "sms-agent",
  textgrid_number: "textgrid-number",
  template: "template",

  touch_number: "touch-number",
  dnc_check: "dnc-check",

  message_type: "message-type",
  message_text: "message-text",
  personalization_tags_used: "personalization-tags-used",
  character_count: "character-count",

  property_address: "property-address",
  property_type: "property-type",
  owner_type: "owner-type",
  category: "category",
  use_case_template: "use-case-template",
};

function nowIso() {
  return new Date().toISOString();
}

function countCharacters(value) {
  return String(value || "").length;
}

function clean(value) {
  return String(value ?? "").trim();
}

// Strips HTML tags and converts common block-level elements to spaces so that
// rich-text template content is reduced to plain SMS text before persistence.
// <br>, </p>, </div>, </li> are treated as line separators → space.
// Named/numeric HTML entities are decoded to their literal characters.
function stripHtml(value) {
  return String(value ?? "")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<\/(p|div|li|tr|h[1-6])>/gi, " ")
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

// Fix malformed punctuation spacing that can arise from template rendering:
//   "Hi Jose ,"  → "Hi Jose,"
//   "this is Ricky ."  → "this is Ricky."
//   "sold - or - kept"  → handled correctly (em/en dash normalization)
// Applied as a final pass before the text is stored in Podio.
export function normalizeTextForSms(value) {
  return String(value ?? "")
    // Remove whitespace immediately before terminal punctuation
    .replace(/\s+([,\.!?;:])/g, "$1")
    // Normalize multiple spaces inside a sentence to a single space
    .replace(/\s{2,}/g, " ")
    // Normalize em-dash / en-dash surrounded by extra spaces: "word — word" stays,
    // "word  —  word" (extra spaces) collapses to "word — word"
    .replace(/\s{2,}([\u2013\u2014])\s{2,}/g, " $1 ")
    .replace(/\s{2,}([\u2013\u2014])/g, " $1")
    .replace(/([\u2013\u2014])\s{2,}/g, "$1 ")
    .trim();
}

// Flatten a rendered SMS for persistence in the Send Queue message-text field.
// 1. Strip any HTML markup the template renderer may have preserved.
// 2. Replace all CRLF/CR/LF sequences with a single space.
// 3. Collapse runs of whitespace and trim.
// 4. Fix punctuation spacing ("Hi Jose ," → "Hi Jose,").
export function normalizeForQueueText(value) {
  return normalizeTextForSms(
    stripHtml(String(value ?? ""))
      .replace(/\r\n|\r|\n/g, " ")
      .replace(/\s{2,}/g, " ")
      .trim()
  );
}

// Accepted contact-window time-range format: "HH:MM AM - HH:MM PM TZ"
// Mirrors the pattern in schema.js isValidSendQueueContactWindow.
// Values matching this pattern are passed through to Podio's compat bypass
// even when the local schema supplement has no matching option ID.
const CONTACT_WINDOW_PATTERN =
  /^(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*-\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s+(?:CT|ET|MT|PT|AT|HT|Local)$/i;

// Maps raw MasterOwners "owner-type" category values (e.g. "LLC/CORP | ABSENTEE")
// to the Send Queue "category" option labels (Corporate, Individual, etc.).
function mapOwnerTypeToQueueCategory(raw) {
  if (!raw) return null;
  const upper = String(raw).toUpperCase();
  if (upper.includes("LLC") || upper.includes("CORP")) return "Corporate";
  if (upper.includes("TRUST") || upper.includes("ESTATE")) return "Trust / Estate";
  if (upper.includes("BANK") || upper.includes("INSTITUTION") || upper.includes("LENDER")) return "Bank / Lender";
  if (upper.includes("GOV")) return "Government";
  if (upper.includes("INDIVIDUAL")) return "Individual";
  return null;
}

// Builds a clean "Street, City, State ZIP" address from the structured sub-fields
// of a Podio location field.  Podio location values expose the geocoded components
// at the same level as .value, so we prefer those over the pre-formatted string
// which can arrive in city-first order ("Tulsa 74127 OK 5139 W 11th St").
function formatPropertyAddress(property_item) {
  if (!property_item) return "";
  const values = getFieldValues(property_item, "property-address");
  const first = values[0];
  if (!first) return "";

  const street = first.street_address || first.value?.street_address || "";
  const city = first.city || first.value?.city || "";
  const state = first.state || first.value?.state || "";
  const zip = first.postal_code || first.zip || first.value?.postal_code || first.value?.zip || "";

  if (street && (city || state)) {
    const region = [city, state, zip].filter(Boolean).join(" ");
    return region ? `${street}, ${region}` : street;
  }

  // Fall back to whatever text value is stored
  return (
    first.formatted ||
    first.value?.formatted ||
    (typeof first.value === "string" ? first.value : "") ||
    ""
  );
}

function unique(list) {
  return [...new Set((list || []).filter(Boolean))];
}

function asArrayAppRef(value) {
  if (!value) return undefined;
  return [value];
}

function toItemId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function normalizePriority(value) {
  const raw = String(value || "").trim().toLowerCase();

  if (["_ urgent", "urgent", "high"].includes(raw)) return "_ Urgent";
  if (["_ low", "low"].includes(raw)) return "_ Low";
  return "_ Normal";
}

function normalizeMessageType(value) {
  const raw = String(value || "").trim().toLowerCase();

  if (raw === "follow-up" || raw === "follow up") return "Follow-Up";
  if (raw === "re-engagement" || raw === "reengagement") return "Re-Engagement";
  if (raw === "opt-out confirm" || raw === "opt out confirm") return "Opt-Out Confirm";

  return "Cold Outbound";
}

function normalizeDeliveryConfirmed(value = "⏳ Pending") {
  const raw = String(value || "").trim().toLowerCase();

  if (raw.includes("confirmed")) return "✅ Confirmed";
  if (raw.includes("failed")) return "❌ Failed";
  return "⏳ Pending";
}

function normalizeQueueStatus(value = "Queued") {
  const raw = String(value || "").trim().toLowerCase();

  if (raw === "processing") return "Sending";
  if (raw === "sending") return "Sending";
  if (raw === "sent") return "Sent";
  // "Delivered" is a distinct terminal state — do not collapse it to "Sent".
  // Requires the "Delivered" option to be added to the Send Queue::queue-status
  // Podio field and the supplement updated with the correct option id.
  if (raw === "delivered") return "Delivered";
  if (raw === "failed") return "Failed";
  if (raw === "blocked") return "Blocked";
  return "Queued";
}

function normalizeTimezone(value) {
  const raw = String(value || "").trim().toLowerCase();

  if (raw.includes("central") || raw === "ct" || raw === "cst" || raw === "cdt") return "Central";
  if (raw.includes("eastern") || raw === "et" || raw === "est" || raw === "edt") return "Eastern";
  if (raw.includes("mountain") || raw === "mt" || raw === "mst" || raw === "mdt") return "Mountain";
  if (raw.includes("pacific") || raw === "pt" || raw === "pst" || raw === "pdt") return "Pacific";
  if (raw.includes("hawaii")) return "Hawaii";
  if (raw.includes("alaska")) return "Alaska";

  return "Central";
}

function normalizeContactWindow(value, fallback = "8AM-9PM Local") {
  const raw = String(value || "").trim();
  return raw || fallback;
}

// Checks whether the resolved contact window has a matching category option in
// the Send Queue app schema.  The attached schema may be stale — it only has a
// subset of the real Podio options.  If no option ID exists the field must be
// omitted from the creation payload: the compat bypass in normalizeCategoryValue
// returns the raw string, which Podio rejects with 400 because category fields
// require integer option IDs, not text.
//
// Returns { field_value, category_option_id, omitted, reason }
// - field_value        the raw contact window string to include, or undefined if omitted
// - category_option_id the resolved integer option id, or null
// - omitted            true when the field should be excluded from the payload
// - reason             diagnostic string:
//                        'empty'                              value was blank
//                        'stale_empty_schema_options'         schema has options: [] — supplement needs refresh
//                        'no_matching_category_option_in_schema' options exist but none match the value

// Normalise a category label the same way getCategoryOptionId does in schema.js,
// so that _matchCategoryOption is consistent with the live lookup.
function normalizeCategoryLabel(value) {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

// Matches raw_value against an in-memory options list.  Used by tests so they
// can verify matching logic without requiring a live Podio schema.
// Returns the matched option id (integer) or null.
export function _matchCategoryOption(options, raw_value) {
  if (!Array.isArray(options) || !options.length) return null;
  const cleaned = String(raw_value ?? "").trim();
  if (!cleaned) return null;

  const numeric = Number(cleaned);
  if (Number.isFinite(numeric) && numeric > 0) {
    return options.some((o) => o.id === numeric) ? numeric : null;
  }

  const normalized = normalizeCategoryLabel(cleaned);
  if (!normalized) return null;

  return options.find((o) => normalizeCategoryLabel(o.text) === normalized)?.id ?? null;
}

// Generic version for any Send Queue category field — used for property-type,
// category, and use-case-template.  Same semantics as resolveContactWindowField:
// if the schema has no option matching the value the field is omitted to prevent
// a Podio 400 error.
function resolveQueueCategoryField(external_id, value) {
  const raw = clean(value);
  if (!raw) {
    return { field_value: undefined, category_option_id: null, omitted: true, reason: "empty" };
  }

  const field_schema = getAttachedFieldSchema(APP_IDS.send_queue, external_id);
  const available_labels = field_schema?.options?.map((o) => o.text) ?? [];
  const option_id = getCategoryOptionId(APP_IDS.send_queue, external_id, raw);

  if (option_id !== null) {
    return { field_value: raw, category_option_id: option_id, omitted: false, reason: null };
  }

  // Distinguish "schema has no options at all" (stale supplement) from
  // "options exist but none match" (label mismatch).
  const reason =
    available_labels.length === 0
      ? "stale_empty_schema_options"
      : "no_matching_category_option_in_schema";

  warn("queue.category_field_resolve_miss", {
    field: external_id,
    source_raw_value: raw,
    available_option_count: available_labels.length,
    available_option_labels: available_labels.slice(0, 15),
    matched_option_id: null,
    omitted: true,
    reason,
  });

  return {
    field_value: undefined,
    category_option_id: null,
    omitted: true,
    reason,
  };
}

function resolveContactWindowField(contact_window) {
  const raw = clean(contact_window);
  if (!raw) {
    return { field_value: undefined, category_option_id: null, omitted: true, reason: "empty" };
  }

  const option_id = getCategoryOptionId(
    APP_IDS.send_queue,
    QUEUE_FIELDS.contact_window,
    raw
  );

  if (option_id !== null) {
    // A valid schema option was found — include the text and let normalizePodioFieldMap
    // convert it to the correct option ID.
    return { field_value: raw, category_option_id: option_id, omitted: false, reason: null };
  }

  // Allow through values that match the canonical time-range format even when the
  // local schema supplement has no matching option ID.  schema.js normalizePodioFieldMap
  // has a compat bypass (shouldAllowRawCategoryCompatibility) that accepts these strings
  // as raw text — Podio stores them and rounds-trips them back as the real option text.
  if (CONTACT_WINDOW_PATTERN.test(raw)) {
    return { field_value: raw, category_option_id: null, omitted: false, reason: null };
  }

  // No option ID found and value doesn't match the canonical format.  Omit to
  // prevent a Podio 400 error.  The queue runner allows sending when contact-window
  // is absent.
  return {
    field_value: undefined,
    category_option_id: null,
    omitted: true,
    reason: "no_matching_category_option_in_schema",
  };
}

function derivePersonalizationTagsUsed({
  message_text,
  owner_name,
  property_address,
  agent_name,
  market_name,
}) {
  const body = String(message_text || "");
  const tags = [];

  if (owner_name && body.includes(owner_name)) tags.push("{{owner_name}}");
  if (property_address && body.includes(property_address)) tags.push("{{property_address}}");
  if (agent_name && body.includes(agent_name)) tags.push("{{agent_name}}");
  if (market_name && body.includes(market_name)) tags.push("{{market}}");

  return unique(tags);
}

function requireNonEmptyString(value, label) {
  const out = String(value || "").trim();
  if (!out) throw new Error(`buildSendQueueItem: missing ${label}`);
  return out;
}

function requireItemId(value, label) {
  if (!value) throw new Error(`buildSendQueueItem: missing ${label}`);
  return value;
}

function normalizeDateField(value) {
  if (!value) return null;

  if (typeof value === "string") {
    return { start: value };
  }

  if (value instanceof Date) {
    return { start: value.toISOString() };
  }

  if (typeof value === "object" && value.start) {
    return value;
  }

  return null;
}

function shouldRetryQueueCreateWithoutTemplate(error) {
  if (!(error instanceof PodioError)) return false;

  const message = clean(error?.message).toLowerCase();
  return (
    error.status === 400 &&
    (
      message.includes("template") ||
      message.includes("referenced") ||
      message.includes("item") ||
      message.includes("value")
    )
  );
}

async function createQueueItemWithTemplateCandidates({
  create_item,
  fields,
  template_candidates = [],
}) {
  if (!template_candidates.length) {
    return {
      created: await create_item(APP_IDS.send_queue, fields),
      selected_candidate: null,
      template_attach_rejected: false,
      template_attach_warning: null,
    };
  }

  for (const candidate of template_candidates) {
    const attempt_fields = {
      ...fields,
      [QUEUE_FIELDS.template]: candidate.field_value,
    };

    try {
      return {
        created: await create_item(APP_IDS.send_queue, attempt_fields),
        selected_candidate: candidate,
        template_attach_rejected: false,
        template_attach_warning: null,
      };
    } catch (error) {
      if (!shouldRetryQueueCreateWithoutTemplate(error)) {
        throw error;
      }
    }
  }

  const fallback_fields = { ...fields };
  delete fallback_fields[QUEUE_FIELDS.template];

  return {
    created: await create_item(APP_IDS.send_queue, fallback_fields),
    selected_candidate: null,
    template_attach_rejected: true,
    template_attach_warning:
      "Template relation was skipped because Send Queue.template rejected the selected template reference.",
  };
}

export async function buildSendQueueItem({
  context,
  rendered_message_text,
  template_id = null,
  template_item = null,
  defer_message_resolution = false,
  textgrid_number_item_id,
  scheduled_for_local,
  scheduled_for_utc = null,
  timezone = null,
  contact_window = null,
  send_priority = "_ Normal",
  message_type = "Cold Outbound",
  max_retries = 3,
  queue_status = "Queued",
  dnc_check = "✅ Cleared",
  delivery_confirmed = "⏳ Pending",
  touch_number = null,
  queue_id = null,
  failed_reason = null,
  sent_at = null,
  delivered_at = null,
  property_type = null,
  secondary_category = null,
  use_case_template = null,
  personalization_tags_used: explicit_personalization_tags_used = null,
  create_item = createItem,
  update_item = updateItem,
}) {
  if (!context?.found) {
    throw new Error("buildSendQueueItem: context not found");
  }

  const message_text = normalizeForQueueText(rendered_message_text);
  if (!defer_message_resolution && !message_text) {
    throw new Error("buildSendQueueItem: missing rendered_message_text");
  }

  const phone_item = context.items?.phone_item || null;
  const master_owner_item = context.items?.master_owner_item || null;
  const property_item = context.items?.property_item || null;
  const brain_item = context.items?.brain_item || null;
  const agent_item = context.items?.agent_item || null;
  const market_item = context.items?.market_item || null;

  const phone_item_id = requireItemId(
    context.ids?.phone_item_id || phone_item?.item_id,
    "context.ids.phone_item_id"
  );

  const master_owner_id =
    toItemId(context.ids?.master_owner_id) ||
    toItemId(master_owner_item?.item_id) ||
    getFirstAppReferenceId(phone_item, "linked-master-owner", null) ||
    getFirstAppReferenceId(brain_item, "master-owner", null) ||
    null;
  const prospect_id =
    toItemId(context.ids?.prospect_id) ||
    getFirstAppReferenceId(phone_item, "linked-contact", null) ||
    getFirstAppReferenceId(brain_item, "prospect", null) ||
    null;
  const property_id =
    toItemId(context.ids?.property_id) ||
    toItemId(property_item?.item_id) ||
    getFirstAppReferenceId(phone_item, "primary-property", null) ||
    getFirstAppReferenceId(brain_item, "properties", null) ||
    null;
  const market_id =
    toItemId(context.ids?.market_id) ||
    toItemId(market_item?.item_id) ||
    getFirstAppReferenceId(property_item, "market-2", null) ||
    getFirstAppReferenceId(property_item, "market", null) ||
    null;

  const assigned_agent_id =
    context.ids?.assigned_agent_id ||
    getFirstAppReferenceId(master_owner_item, "sms-agent", null) ||
    null;

  requireItemId(textgrid_number_item_id, "textgrid_number_item_id");

  const phone_activity_status = String(
    getCategoryValue(phone_item, "phone-activity-status", "Unknown") || "Unknown"
  )
    .trim()
    .toLowerCase();

  if (!phone_activity_status.startsWith("active")) {
    throw new Error(`buildSendQueueItem: phone not active (${phone_activity_status})`);
  }

  const phone_hidden = getTextValue(phone_item, "phone-hidden", "");
  const canonical_e164 = getTextValue(phone_item, "canonical-e164", "");
  const normalized_target = normalizePhone(canonical_e164 || phone_hidden);

  if (!normalized_target) {
    throw new Error("buildSendQueueItem: target phone is missing or invalid");
  }

  const owner_name =
    context.summary?.owner_name ||
    getTextValue(master_owner_item, "owner-full-name", "") ||
    "";

  // Build a properly-ordered "Street, City, State ZIP" address from the Podio
  // location field's structured sub-components.  Falls back to the context
  // summary (which also reads getTextValue so may have the ugly geocoded format)
  // only when the location field has no structured data.
  const property_address =
    formatPropertyAddress(property_item) ||
    context.summary?.property_address ||
    getTextValue(property_item, "title", "") ||
    "";

  const agent_name =
    context.summary?.agent_name ||
    getTextValue(agent_item, "title", "") ||
    getTextValue(agent_item, "agent-name", "") ||
    "";

  const market_name =
    context.summary?.market_name ||
    getTextValue(market_item, "title", "") ||
    "";

  const personalization_tags_used = Array.isArray(explicit_personalization_tags_used)
    ? unique(explicit_personalization_tags_used.map((tag) => clean(tag)).filter(Boolean))
    : derivePersonalizationTagsUsed({
        message_text,
        owner_name,
        property_address,
        agent_name,
        market_name,
      });

  // Warn when multiple tags are detected but the Podio field is still
  // single-select (multiple:false).  normalizeCategoryValue will silently
  // persist only the first tag.  Requires the field to be changed to
  // multi-select in Podio — until then, only the first tag is stored.
  if (personalization_tags_used.length > 1) {
    const tags_field_schema = getAttachedFieldSchema(
      APP_IDS.send_queue,
      QUEUE_FIELDS.personalization_tags_used
    );
    if (tags_field_schema && !tags_field_schema.multiple) {
      warn("queue.personalization_tags_field_single_select", {
        field: QUEUE_FIELDS.personalization_tags_used,
        detected_tags: personalization_tags_used,
        tags_will_persist: personalization_tags_used[0] || null,
        tags_lost: personalization_tags_used.slice(1),
        note: "Change Send Queue::personalization-tags-used to multi-select in Podio to persist all tags.",
      });
    }
  }

  const next_touch_number =
    touch_number ??
    ((context.recent?.touch_count || context.summary?.total_messages_sent || 0) + 1);

  const scheduled_local_value =
    normalizeDateField(scheduled_for_local) || { start: nowIso() };

  const scheduled_utc_value =
    normalizeDateField(scheduled_for_utc) || scheduled_local_value;

  const resolved_timezone = normalizeTimezone(
    timezone ||
      context.summary?.market_timezone ||
      context.summary?.timezone ||
      "Central"
  );

  const resolved_contact_window = normalizeContactWindow(
    contact_window ||
      context.summary?.contact_window ||
      "8AM-9PM Local"
  );

  // Validate the contact window against the Send Queue category field schema.
  // Omit the field if no matching option ID exists to prevent Podio 400 errors.
  const contact_window_field = resolveContactWindowField(resolved_contact_window);

  if (contact_window_field.omitted) {
    warn("queue.contact_window_category_write_omitted", {
      source_contact_window: resolved_contact_window,
      target_field: QUEUE_FIELDS.contact_window,
      field_type: "category",
      category_option_id: null,
      omitted: true,
      reason: contact_window_field.reason,
    });
  }

  // Prefer the property item's own "property-type" category (Single Family,
  // Multi-Family, etc.) over the caller-supplied value which may be sourced
  // from the broader "property-class" field (Residential, Vacant, …).
  const direct_property_type = getCategoryValue(property_item, "property-type", null);
  const resolved_property_type = direct_property_type || property_type;
  const property_type_field = resolveQueueCategoryField(QUEUE_FIELDS.property_type, resolved_property_type);

  // Persist the queue row's explicit owner-type from the linked Property item.
  // Properties stores the normalized owner bucket on owner-type-2, while
  // Master Owners stores the raw absentee/owner-occ hybrid type on owner-type.
  const property_owner_type =
    getCategoryValue(property_item, "owner-type-2", null) ||
    getCategoryValue(property_item, "owner-type", null);
  // Resolve owner entity category from the master owner item when the caller
  // does not supply a secondary_category.
  const owner_type_raw = getCategoryValue(master_owner_item, "owner-type", null);
  const resolved_owner_type =
    property_owner_type || mapOwnerTypeToQueueCategory(owner_type_raw);
  const resolved_owner_category = secondary_category || mapOwnerTypeToQueueCategory(owner_type_raw);
  const owner_type_field = resolveQueueCategoryField(QUEUE_FIELDS.owner_type, resolved_owner_type);
  const category_field = resolveQueueCategoryField(QUEUE_FIELDS.category, resolved_owner_category);
  const use_case_template_field = resolveQueueCategoryField(QUEUE_FIELDS.use_case_template, use_case_template);

  for (const [field_name, source_value, resolved] of [
    [QUEUE_FIELDS.property_type, property_type, property_type_field],
    [QUEUE_FIELDS.owner_type, resolved_owner_type, owner_type_field],
    [QUEUE_FIELDS.category, secondary_category, category_field],
    [QUEUE_FIELDS.use_case_template, use_case_template, use_case_template_field],
  ]) {
    if (resolved.omitted && resolved.reason !== "empty") {
      warn("queue.category_field_write_omitted", {
        field: field_name,
        source_value: source_value ?? null,
        category_option_id: null,
        omitted: true,
        reason: resolved.reason,
      });
    }
  }

  // Specific structured warning when owner-type cannot be resolved.
  // The Send Queue field external id is "owner-type".  If the Podio schema
  // does not have a matching option for the resolved value, the field is omitted
  // and ops will see a blank Owner Type on the queue row.
  if (owner_type_field.omitted && (property_owner_type || owner_type_raw)) {
    warn("queue.owner_type_write_failed", {
      field: QUEUE_FIELDS.owner_type,
      expected_external_id: "owner-type",
      property_owner_type: property_owner_type ?? null,
      master_owner_raw: owner_type_raw ?? null,
      resolved_value: resolved_owner_type ?? null,
      omit_reason: owner_type_field.reason,
      note: "Ensure Send Queue::owner-type Podio field exists and its options match the values returned by mapOwnerTypeToQueueCategory.",
    });
  }

  const template_reference = resolveTemplateFieldReference({
    host_app_id: APP_IDS.send_queue,
    host_field_external_id: QUEUE_FIELDS.template,
    template_id,
    template_item,
  });
  const template_field_value = template_reference.field_value;
  const template_candidates = template_reference.attachment_candidates || [];
  const missing_relation_warnings = [];

  if (!master_owner_id && (master_owner_item?.item_id || context.ids?.master_owner_id)) {
    missing_relation_warnings.push("master_owner_relation_unresolved");
  }
  if (!property_id && (property_item?.item_id || brain_item?.item_id || master_owner_id)) {
    missing_relation_warnings.push("property_relation_unresolved");
  }
  if (template_reference.selected_template_id && !template_field_value) {
    missing_relation_warnings.push("template_relation_unresolved");
  }

  if (missing_relation_warnings.length) {
    warn("queue.build_relation_payload_incomplete", {
      phone_item_id,
      master_owner_id,
      property_id,
      market_id,
      selected_template_id: template_reference.selected_template_id ?? null,
      selected_template_item_id: template_reference.selected_template_item_id ?? null,
      selected_template_source: template_reference.selected_template_source ?? null,
      selected_template_app_id: template_reference.selected_template_app_id ?? null,
      attempted_template_relation_id: template_reference.attached_template_id ?? null,
      template_attachment_strategy: template_reference.attachment_strategy ?? null,
      template_attachment_reason: template_reference.attachment_reason ?? null,
      warnings: missing_relation_warnings,
    });
  }

  const fields = {
    [QUEUE_FIELDS.queue_id_2]: queue_id || undefined,

    [QUEUE_FIELDS.scheduled_for_local]: scheduled_local_value,
    [QUEUE_FIELDS.scheduled_for_utc]: scheduled_utc_value,
    [QUEUE_FIELDS.timezone]: resolved_timezone,
    // Only write contact-window when a valid Podio category option ID exists.
    // If the schema doesn't recognise the value (e.g. stale options list), the
    // field is omitted here and a warning is logged above.  The queue runner
    // handles a null contact-window by allowing sending (no_contact_window).
    ...(contact_window_field.omitted
      ? {}
      : { [QUEUE_FIELDS.contact_window]: contact_window_field.field_value }),
    [QUEUE_FIELDS.send_priority]: normalizePriority(send_priority),
    [QUEUE_FIELDS.retry_count]: 0,
    [QUEUE_FIELDS.max_retries]: Number(max_retries) || 3,

    [QUEUE_FIELDS.queue_status]: normalizeQueueStatus(queue_status),
    [QUEUE_FIELDS.sent_at]: normalizeDateField(sent_at) || undefined,
    [QUEUE_FIELDS.delivered_at]: normalizeDateField(delivered_at) || undefined,
    [QUEUE_FIELDS.failed_reason]: failed_reason || undefined,
    [QUEUE_FIELDS.delivery_confirmed]: normalizeDeliveryConfirmed(delivery_confirmed),

    [QUEUE_FIELDS.phone_number]: asArrayAppRef(phone_item_id),
    [QUEUE_FIELDS.textgrid_number]: asArrayAppRef(textgrid_number_item_id),
    [QUEUE_FIELDS.message_type]: normalizeMessageType(message_type),
    [QUEUE_FIELDS.touch_number]: next_touch_number,
    [QUEUE_FIELDS.dnc_check]: dnc_check,

    ...(master_owner_id ? { [QUEUE_FIELDS.master_owner]: asArrayAppRef(master_owner_id) } : {}),
    ...(prospect_id ? { [QUEUE_FIELDS.prospects]: asArrayAppRef(prospect_id) } : {}),
    ...(property_id ? { [QUEUE_FIELDS.properties]: asArrayAppRef(property_id) } : {}),
    ...(market_id ? { [QUEUE_FIELDS.market]: asArrayAppRef(market_id) } : {}),
    ...(assigned_agent_id ? { [QUEUE_FIELDS.sms_agent]: asArrayAppRef(assigned_agent_id) } : {}),
    ...(message_text ? { [QUEUE_FIELDS.message_text]: message_text } : {}),
    ...(message_text ? { [QUEUE_FIELDS.character_count]: countCharacters(message_text) } : {}),
    ...(personalization_tags_used.length
      ? { [QUEUE_FIELDS.personalization_tags_used]: personalization_tags_used }
      : {}),
    // New enrichment fields — omitted when schema has no matching option ID or value is absent.
    ...(property_id && property_address
      ? { [QUEUE_FIELDS.property_address]: property_address }
      : {}),
    ...(property_type_field.omitted ? {} : { [QUEUE_FIELDS.property_type]: property_type_field.field_value }),
    ...(owner_type_field.omitted ? {} : { [QUEUE_FIELDS.owner_type]: owner_type_field.field_value }),
    ...(category_field.omitted ? {} : { [QUEUE_FIELDS.category]: category_field.field_value }),
    ...(use_case_template_field.omitted ? {} : { [QUEUE_FIELDS.use_case_template]: use_case_template_field.field_value }),
  };

  Object.keys(fields).forEach((key) => {
    if (fields[key] === undefined || fields[key] === null) {
      delete fields[key];
    }
  });

  let created = null;
  let template_attach_warning = null;
  let template_attach_rejected = false;
  let selected_template_candidate = null;

  ({
    created,
    selected_candidate: selected_template_candidate,
    template_attach_rejected,
    template_attach_warning,
  } = await createQueueItemWithTemplateCandidates({
    create_item,
    fields,
    template_candidates,
  }));

  const resolved_queue_id = queue_id || null;
  const queue_sequence_value = created?.item_id ? Number(created.item_id) : null;

  if (created?.item_id && queue_sequence_value) {
    await update_item(created.item_id, {
      [QUEUE_FIELDS.queue_sequence]: queue_sequence_value,
    });
  }

  const template_app_field_written =
    Boolean(selected_template_candidate?.attached_template_id) &&
    !template_attach_rejected;
  const template_relation_id = template_app_field_written
    ? selected_template_candidate?.attached_template_id ?? null
    : null;

  return {
    ok: true,
    queue_item_id: created?.item_id || null,
    queue_id: resolved_queue_id,
    queue_sequence: queue_sequence_value,
    phone_item_id,
    textgrid_number_item_id,
    template_id,
    selected_template_id: template_reference.selected_template_id ?? null,
    selected_template_item_id: template_reference.selected_template_item_id ?? null,
    selected_template_source: template_reference.selected_template_source ?? null,
    selected_template_title: template_reference.selected_template_title ?? null,
    selected_template_use_case: template_reference.selected_template_use_case ?? null,
    selected_template_variant_group:
      template_reference.selected_template_variant_group ?? null,
    selected_template_language: template_reference.selected_template_language ?? null,
    selected_template_tone: template_reference.selected_template_tone ?? null,
    selected_template_selection_diagnostics:
      template_reference.selected_template_selection_diagnostics ?? null,
    selected_template_resolution_source:
      template_reference.selected_template_resolution_source ?? null,
    selected_template_fallback_reason:
      template_reference.selected_template_fallback_reason ?? null,
    template_relation_id,
    attempted_template_relation_id: template_reference.attached_template_id ?? null,
    template_app_field_written,
    template_attachment_strategy: template_attach_rejected
      ? "podio_rejected_template_reference"
      : selected_template_candidate?.attachment_strategy ??
        template_reference.attachment_strategy ??
        null,
    template_attachment_reason: template_attach_rejected
      ? "template_attach_rejected_by_podio"
      : template_reference.attachment_reason ?? null,
    template_target_app_ids: template_reference.target_app_ids || [],
    template_relation_candidates: template_candidates.map(
      (candidate) => candidate.attached_template_id
    ),
    template_attached: template_app_field_written,
    message_text: message_text || null,
    deferred_message_resolution: Boolean(defer_message_resolution && !message_text),
    normalized_target,
    touch_number: next_touch_number,
    queue_status: normalizeQueueStatus(queue_status),
    contact_window_written: !contact_window_field.omitted,
    contact_window_omit_reason: contact_window_field.omitted
      ? contact_window_field.reason
      : null,
    property_address_written: Boolean(property_id && property_address),
    property_type_written: !property_type_field.omitted,
    owner_type_written: !owner_type_field.omitted,
    category_written: !category_field.omitted,
    use_case_template_written: !use_case_template_field.omitted,
    warnings: [
      ...missing_relation_warnings,
      ...(defer_message_resolution && !message_text
        ? ["Message text will be resolved during queue processing."]
        : []),
      ...(template_attach_warning ? [template_attach_warning] : []),
      ...(!template_field_value && template_reference.selected_template_id
        ? [
            `Template relation skipped: ${
              template_reference.attachment_reason || "template_relation_unresolved"
            }.`,
          ]
        : []),
      ...(contact_window_field.omitted && contact_window_field.reason !== "empty"
        ? [
            `contact-window field omitted: no matching category option for "${resolved_contact_window}" in Send Queue schema.`,
          ]
        : []),
    ],
    raw: created,
  };
}

export { resolveContactWindowField, resolveQueueCategoryField };
export default buildSendQueueItem;
