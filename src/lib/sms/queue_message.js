// ─── queue_message.js ─────────────────────────────────────────────────────
// Build and create Send Queue rows truthfully.
// Never claim an attached Podio template unless actually valid.
// Dedupe before create.

import crypto from "node:crypto";
import APP_IDS from "@/lib/config/app-ids.js";
import { createItem, getFirstMatchingItem } from "@/lib/providers/podio.js";
import { countSegments } from "@/lib/sms/personalize_template.js";

// ══════════════════════════════════════════════════════════════════════════
// QUEUE FIELD EXTERNAL IDS
// ══════════════════════════════════════════════════════════════════════════

export const QUEUE_FIELDS = Object.freeze({
  queue_id: "queue-id-2",
  scheduled_local: "scheduled-for-local",
  scheduled_utc: "scheduled-for-utc",
  timezone: "timezone",
  contact_window: "contact-window",
  queue_status: "queue-status",
  message_text: "message-text",
  character_count: "character-count",
  touch_number: "touch-number",
  message_type: "message-type",
  template: "template-2",
  sms_agent: "sms-agent",
  master_owner: "master-owner",
  prospects: "prospects",
  properties: "properties",
  phone_number: "phone-number",
  market: "market",
  textgrid_number: "textgrid-number",
  use_case: "use-case-template",
  property_address: "property-address",
  property_type: "property-type",
  owner_type: "owner-type",
  max_retries: "max-retries",
  retry_count: "retry-count",
  personalization_tags: "personalization-tags-used",
  current_stage: "current-stage",
  send_priority: "send-priority",
  dnc_check: "dnc-check",
  delivery_confirmed: "delivery-confirmed",
});

// ══════════════════════════════════════════════════════════════════════════
// MESSAGE TYPE MAPPING
// ══════════════════════════════════════════════════════════════════════════

export const MESSAGE_TYPES = Object.freeze({
  COLD_OUTBOUND: "Cold Outbound",
  FOLLOW_UP: "Follow-Up",
  RE_ENGAGEMENT: "Re-Engagement",
  OPT_OUT_CONFIRM: "Opt-Out Confirm",
});

function resolveMessageType(context = {}) {
  if (context.is_opt_out_confirm) return MESSAGE_TYPES.OPT_OUT_CONFIRM;
  if (context.is_reengagement) return MESSAGE_TYPES.RE_ENGAGEMENT;
  if (context.is_follow_up) return MESSAGE_TYPES.FOLLOW_UP;
  if (context.is_first_touch) return MESSAGE_TYPES.COLD_OUTBOUND;
  return MESSAGE_TYPES.FOLLOW_UP;
}

// ══════════════════════════════════════════════════════════════════════════
// TEMPLATE REF VALIDATION
// ══════════════════════════════════════════════════════════════════════════

/**
 * Only populate template app-ref if the item actually belongs to the
 * Podio app referenced by the Send Queue's template field.
 */
function resolveTemplateRef(resolution_result) {
  if (!resolution_result?.attachable_template_ref) return {};

  const ref = resolution_result.attachable_template_ref;
  if (ref.app_id === APP_IDS.templates && ref.item_id) {
    return { [QUEUE_FIELDS.template]: [ref.item_id] };
  }

  // Don't fake it
  return {};
}

// ══════════════════════════════════════════════════════════════════════════
// APP-REF HELPER
// ══════════════════════════════════════════════════════════════════════════

function appRef(value) {
  const id = Number(value);
  return Number.isFinite(id) && id > 0 ? [id] : undefined;
}

// ══════════════════════════════════════════════════════════════════════════
// DEDUPE
// ══════════════════════════════════════════════════════════════════════════

export function buildDedupeFingerprint({
  master_owner_id,
  phone_e164,
  use_case,
  stage_code,
  language,
  agent_style_fit,
  rendered_text,
} = {}) {
  const parts = [
    String(master_owner_id ?? ""),
    String(phone_e164 ?? ""),
    String(use_case ?? ""),
    String(stage_code ?? ""),
    String(language ?? ""),
    String(agent_style_fit ?? ""),
    String(rendered_text ?? ""),
  ].join("|");
  return crypto.createHash("sha256").update(parts, "utf8").digest("hex");
}

// ══════════════════════════════════════════════════════════════════════════
// INJECTABLE DEPS (for testing)
// ══════════════════════════════════════════════════════════════════════════

const defaultDeps = {
  createItem,
  getFirstMatchingItem,
};

let runtimeDeps = { ...defaultDeps };

export function __setQueueMessageTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetQueueMessageTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

// ══════════════════════════════════════════════════════════════════════════
// BUILD QUEUE ROW
// ══════════════════════════════════════════════════════════════════════════

/**
 * Build a Send Queue fields object from resolution + personalization + scheduling.
 *
 * @param {object} params
 * @param {string} params.rendered_text - Final rendered message text
 * @param {object} params.schedule - Output from computeScheduledSend
 * @param {object} params.resolution - Output from resolveTemplate
 * @param {object} params.links - Podio item IDs for relationships
 * @param {object} [params.context] - Additional context
 * @returns {object} Fields for createItem
 */
export function buildQueueFields({
  rendered_text,
  schedule,
  resolution,
  links = {},
  context = {},
} = {}) {
  const fields = {};

  // Text
  fields[QUEUE_FIELDS.message_text] = rendered_text;
  fields[QUEUE_FIELDS.character_count] = String(rendered_text ?? "").length;

  // Schedule
  if (schedule?.scheduled_local) {
    fields[QUEUE_FIELDS.scheduled_local] = { start: schedule.scheduled_local };
  }
  if (schedule?.scheduled_utc) {
    fields[QUEUE_FIELDS.scheduled_utc] = { start: schedule.scheduled_utc };
  }
  if (schedule?.timezone) {
    fields[QUEUE_FIELDS.timezone] = schedule.timezone;
  }

  // Status
  fields[QUEUE_FIELDS.queue_status] = "Queued";

  // Touch / type
  if (context.touch_number != null) {
    fields[QUEUE_FIELDS.touch_number] = context.touch_number;
  }
  fields[QUEUE_FIELDS.message_type] = resolveMessageType(context);

  // Use case
  if (resolution?.use_case) {
    fields[QUEUE_FIELDS.use_case] = resolution.use_case;
  }

  // Stage
  if (resolution?.stage_code) {
    fields[QUEUE_FIELDS.current_stage] = resolution.stage_code;
  }

  // Retries
  fields[QUEUE_FIELDS.max_retries] = context.max_retries ?? 3;
  fields[QUEUE_FIELDS.retry_count] = 0;

  // Personalization tags
  if (context.placeholders_used?.length) {
    fields[QUEUE_FIELDS.personalization_tags] = context.placeholders_used;
  }

  // Contact window
  if (context.contact_window) {
    fields[QUEUE_FIELDS.contact_window] = context.contact_window;
  }

  // App references — linked records
  const ref = (val) => appRef(val);
  if (ref(links.master_owner_id)) fields[QUEUE_FIELDS.master_owner] = ref(links.master_owner_id);
  if (ref(links.prospect_id)) fields[QUEUE_FIELDS.prospects] = ref(links.prospect_id);
  if (ref(links.property_id)) fields[QUEUE_FIELDS.properties] = ref(links.property_id);
  if (ref(links.phone_id)) fields[QUEUE_FIELDS.phone_number] = ref(links.phone_id);
  if (ref(links.market_id)) fields[QUEUE_FIELDS.market] = ref(links.market_id);
  if (ref(links.agent_id)) fields[QUEUE_FIELDS.sms_agent] = ref(links.agent_id);
  if (ref(links.textgrid_number_id)) fields[QUEUE_FIELDS.textgrid_number] = ref(links.textgrid_number_id);

  // Template ref — only if valid
  Object.assign(fields, resolveTemplateRef(resolution));

  // Property metadata
  if (context.property_address) {
    fields[QUEUE_FIELDS.property_address] = context.property_address;
  }
  if (context.property_type) {
    fields[QUEUE_FIELDS.property_type] = context.property_type;
  }
  if (context.owner_type) {
    fields[QUEUE_FIELDS.owner_type] = context.owner_type;
  }

  // Priority / compliance fields
  if (context.send_priority) {
    fields[QUEUE_FIELDS.send_priority] = context.send_priority;
  }
  if (context.dnc_check) {
    fields[QUEUE_FIELDS.dnc_check] = context.dnc_check;
  }
  if (context.delivery_confirmed) {
    fields[QUEUE_FIELDS.delivery_confirmed] = context.delivery_confirmed;
  }

  // Queue ID (unique per row)
  fields[QUEUE_FIELDS.queue_id] = buildDedupeFingerprint({
    master_owner_id: links.master_owner_id,
    phone_e164: context.phone_e164,
    use_case: resolution?.use_case,
    stage_code: resolution?.stage_code,
    language: resolution?.language,
    agent_style_fit: resolution?.agent_style_fit,
    rendered_text,
  }).slice(0, 16);

  return fields;
}

// ══════════════════════════════════════════════════════════════════════════
// CREATE WITH DEDUPE
// ══════════════════════════════════════════════════════════════════════════

/**
 * Create a Send Queue row, but only if no duplicate exists within the dedupe horizon.
 *
 * @param {object} params - Same as buildQueueFields
 * @returns {{ ok: boolean, item_id?: number, reason?: string, fields?: object }}
 */
export async function queueMessage(params = {}) {
  const fields = buildQueueFields(params);
  const queue_id = fields[QUEUE_FIELDS.queue_id];

  // Dedupe check: look for an existing row with the same queue ID
  try {
    const existing = await runtimeDeps.getFirstMatchingItem(
      APP_IDS.send_queue,
      { [QUEUE_FIELDS.queue_id]: queue_id },
      { sort_desc: true }
    );

    if (existing?.item_id) {
      const status = String(existing.fields?.find?.((f) => f.external_id === "queue-status")?.values?.[0]?.value?.text ?? "").toLowerCase();
      if (status === "queued" || status === "sending" || status === "sent") {
        return {
          ok: false,
          reason: "duplicate_blocked",
          existing_item_id: existing.item_id,
          existing_status: status,
          queue_id,
        };
      }
    }
  } catch {
    // Dedupe lookup failed — proceed cautiously with creation
  }

  const created = await runtimeDeps.createItem(APP_IDS.send_queue, fields);

  return {
    ok: true,
    item_id: created?.item_id || null,
    queue_id,
    fields,
  };
}

export default { queueMessage, buildQueueFields, buildDedupeFingerprint, QUEUE_FIELDS, MESSAGE_TYPES };
