// ─── queue_message.js ─────────────────────────────────────────────────────
// Build and create Send Queue rows truthfully.
// Never claim an attached Podio template unless actually valid.
// Dedupe before create.

import crypto from "node:crypto";
import APP_IDS from "@/lib/config/app-ids.js";
import { createItem, getFirstMatchingItem } from "@/lib/providers/podio.js";
import { countSegments } from "@/lib/sms/personalize_template.js";
import { info, warn } from "@/lib/logging/logger.js";
import { hasSupabaseConfig } from "@/lib/supabase/client.js";
import { insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";
import {
  normalizeUsPhoneToE164,
  prepareRenderedSmsForQueue,
  sanitizeSmsTextValue,
} from "@/lib/sms/sanitize.js";

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
// EMPTY STRING GUARD
// ══════════════════════════════════════════════════════════════════════════

/**
 * Omit a field entirely if the value is empty-string, null, or undefined.
 * Podio rejects empty strings for category fields, and empty values are
 * indistinguishable from "not set" in the UI.
 */
function omitEmpty(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "string" && value.trim() === "") return undefined;
  return value;
}

function mapSendPriorityToNumber(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (["_ urgent", "urgent", "high", "10"].includes(raw)) return 10;
  if (["_ low", "low", "1"].includes(raw)) return 1;
  return 5;
}

function shouldUseSupabaseQueueWrite() {
  return (
    hasSupabaseConfig() &&
    runtimeDeps.createItem === defaultDeps.createItem &&
    runtimeDeps.getFirstMatchingItem === defaultDeps.getFirstMatchingItem
  );
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
  const safe_rendered_text = sanitizeSmsTextValue(rendered_text);
  const fields = {};

  // Text
  fields[QUEUE_FIELDS.message_text] = safe_rendered_text;
  fields[QUEUE_FIELDS.character_count] = safe_rendered_text.length;

  // Schedule
  if (schedule?.scheduled_local) {
    fields[QUEUE_FIELDS.scheduled_local] = { start: schedule.scheduled_local };
  }
  if (schedule?.scheduled_utc) {
    fields[QUEUE_FIELDS.scheduled_utc] = { start: schedule.scheduled_utc };
  }
  if (omitEmpty(schedule?.timezone) !== undefined) {
    fields[QUEUE_FIELDS.timezone] = schedule.timezone;
  }

  // Status
  fields[QUEUE_FIELDS.queue_status] = "Queued";

  // Touch / type
  if (context.touch_number != null) {
    fields[QUEUE_FIELDS.touch_number] = context.touch_number;
  }
  fields[QUEUE_FIELDS.message_type] = resolveMessageType(context);

  // Use case — omit empty strings so Podio doesn't throw
  if (omitEmpty(resolution?.use_case) !== undefined) {
    fields[QUEUE_FIELDS.use_case] = resolution.use_case;
  }

  // Stage
  if (omitEmpty(resolution?.stage_code) !== undefined) {
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
  if (omitEmpty(context.contact_window) !== undefined) {
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

  // Property metadata — property-address is a Podio location field
  if (omitEmpty(context.property_address) !== undefined) {
    const addr = context.property_address;
    fields[QUEUE_FIELDS.property_address] =
      typeof addr === "object" && addr !== null ? addr : { value: addr };
  }
  if (omitEmpty(context.property_type) !== undefined) {
    fields[QUEUE_FIELDS.property_type] = context.property_type;
  }
  if (omitEmpty(context.owner_type) !== undefined) {
    fields[QUEUE_FIELDS.owner_type] = context.owner_type;
  }

  // Priority / compliance fields — guard against empty strings
  if (omitEmpty(context.send_priority) !== undefined) {
    fields[QUEUE_FIELDS.send_priority] = context.send_priority;
  }
  if (omitEmpty(context.dnc_check) !== undefined) {
    fields[QUEUE_FIELDS.dnc_check] = context.dnc_check;
  }
  if (omitEmpty(context.delivery_confirmed) !== undefined) {
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
    rendered_text: safe_rendered_text,
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
  const rendered_sms = prepareRenderedSmsForQueue({
    rendered_message_text: params?.rendered_text,
    template_id:
      params?.resolution?.template_id ||
      params?.resolution?.attachable_template_ref?.item_id ||
      null,
    template_source: params?.resolution?.source || null,
  });

  if (!rendered_sms.ok) {
    warn("queue_message.render_contains_html", {
      reason: rendered_sms.reason,
      diagnostics: rendered_sms.diagnostics,
    });

    return {
      ok: false,
      reason: rendered_sms.reason,
      diagnostics: rendered_sms.diagnostics,
      storage: null,
    };
  }

  const safe_params = {
    ...params,
    rendered_text: rendered_sms.text,
  };
  const fields = buildQueueFields(safe_params);
  const queue_id = fields[QUEUE_FIELDS.queue_id];
  const resolved_to_phone_number = normalizeUsPhoneToE164(
    safe_params?.context?.phone_e164 ||
      safe_params?.context?.canonical_e164 ||
      safe_params?.context?.phone_hidden
  );

  info("queue_message.building", {
    queue_id,
    has_message_text: Boolean(fields[QUEUE_FIELDS.message_text]),
    has_master_owner: Boolean(fields[QUEUE_FIELDS.master_owner]),
    has_prospects: Boolean(fields[QUEUE_FIELDS.prospects]),
    has_properties: Boolean(fields[QUEUE_FIELDS.properties]),
    has_phone: Boolean(fields[QUEUE_FIELDS.phone_number]),
    has_textgrid: Boolean(fields[QUEUE_FIELDS.textgrid_number]),
    has_template: Boolean(fields[QUEUE_FIELDS.template]),
    queue_status: fields[QUEUE_FIELDS.queue_status],
    message_type: fields[QUEUE_FIELDS.message_type],
    use_case: fields[QUEUE_FIELDS.use_case] || null,
    field_count: Object.keys(fields).length,
  });

  if (shouldUseSupabaseQueueWrite()) {
    const now = new Date().toISOString();
    const queue_result = await insertSupabaseSendQueueRow({
      queue_key: queue_id,
      queue_id,
      queue_status: "queued",
      scheduled_for:
        safe_params?.schedule?.scheduled_utc ||
        safe_params?.schedule?.scheduled_local ||
        now,
      scheduled_for_utc:
        safe_params?.schedule?.scheduled_utc ||
        safe_params?.schedule?.scheduled_local ||
        now,
      scheduled_for_local:
        safe_params?.schedule?.scheduled_local ||
        safe_params?.schedule?.scheduled_utc ||
        now,
      timezone:
        safe_params?.schedule?.timezone ||
        safe_params?.context?.timezone ||
        "America/Chicago",
      contact_window: safe_params?.context?.contact_window || null,
      send_priority: mapSendPriorityToNumber(safe_params?.context?.send_priority),
      is_locked: false,
      retry_count: 0,
      max_retries: safe_params?.context?.max_retries ?? 3,
      message_body: safe_params?.rendered_text || "",
      message_text: safe_params?.rendered_text || "",
      to_phone_number: resolved_to_phone_number || null,
      from_phone_number: safe_params?.context?.from_phone_number || null,
      property_address: safe_params?.context?.property_address || null,
      property_type: safe_params?.context?.property_type || null,
      owner_type: safe_params?.context?.owner_type || null,
      master_owner_id: safe_params?.links?.master_owner_id || null,
      prospect_id: safe_params?.links?.prospect_id || null,
      property_id: safe_params?.links?.property_id || null,
      market_id: safe_params?.links?.market_id || null,
      sms_agent_id: safe_params?.links?.agent_id || null,
      textgrid_number_id: safe_params?.links?.textgrid_number_id || null,
      template_id:
        safe_params?.resolution?.template_id ||
        safe_params?.resolution?.attachable_template_ref?.item_id ||
        null,
      touch_number: safe_params?.context?.touch_number ?? null,
      dnc_check: safe_params?.context?.dnc_check || null,
      current_stage: safe_params?.resolution?.stage_code || null,
      message_type: fields[QUEUE_FIELDS.message_type] || null,
      use_case_template: safe_params?.resolution?.use_case || null,
      personalization_tags_used: safe_params?.context?.placeholders_used || null,
      character_count: String(safe_params?.rendered_text || "").length,
      metadata: {
        source: "queue_message",
        schedule: safe_params?.schedule || null,
        resolution_source: safe_params?.resolution?.source || null,
        queue_fields: fields,
        queue_context: safe_params?.context || null,
        canonical_e164:
          normalizeUsPhoneToE164(safe_params?.context?.canonical_e164 || "") || null,
        phone_hidden: sanitizeSmsTextValue(safe_params?.context?.phone_hidden || ""),
        raw_destination_phone:
          sanitizeSmsTextValue(
            safe_params?.context?.phone_hidden ||
              safe_params?.context?.canonical_e164 ||
              safe_params?.context?.phone_e164 ||
              ""
          ) || null,
        resolved_to_phone_number: resolved_to_phone_number || null,
      },
      cash_offer_snapshot_id: safe_params?.cash_offer_snapshot_id || null,
    });

    info("queue_message.created", {
      queue_id,
      item_id: queue_result?.item_id || null,
      storage: "supabase",
      reason: queue_result?.reason || null,
    });

    return {
      ok: queue_result?.ok !== false,
      item_id: queue_result?.item_id || null,
      queue_item_id: queue_result?.queue_item_id || null,
      queue_id: queue_result?.queue_id || queue_id,
      queue_key: queue_result?.queue_key || queue_id,
      reason: queue_result?.reason || null,
      storage: "supabase",
      fields,
      raw: queue_result?.raw || null,
    };
  }

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
        info("queue_message.duplicate_blocked", {
          queue_id,
          existing_item_id: existing.item_id,
          existing_status: status,
        });
        return {
          ok: false,
          reason: "duplicate_blocked",
          existing_item_id: existing.item_id,
          existing_status: status,
          queue_id,
        };
      }
    }
  } catch (err) {
    warn("queue_message.dedupe_lookup_failed", {
      queue_id,
      error: err?.message || "unknown",
    });
    // Dedupe lookup failed — proceed cautiously with creation
  }

  let created;
  try {
    created = await runtimeDeps.createItem(APP_IDS.send_queue, fields);
  } catch (err) {
    warn("queue_message.create_failed", {
      queue_id,
      error: err?.message || "unknown",
      field_keys: Object.keys(fields),
    });
    throw err;
  }

  info("queue_message.created", {
    queue_id,
    item_id: created?.item_id || null,
  });

  return {
    ok: true,
    item_id: created?.item_id || null,
    queue_id,
    fields,
  };
}

export default { queueMessage, buildQueueFields, buildDedupeFingerprint, QUEUE_FIELDS, MESSAGE_TYPES };
