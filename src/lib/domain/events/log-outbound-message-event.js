// ─── log-outbound-message-event.js ───────────────────────────────────────
import { createMessageEvent, getCategoryValue } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import {
  buildQueueMessageEventMetadata,
  buildQueueSendTriggerName,
  serializeMessageEventMetadata,
} from "@/lib/domain/events/message-event-metadata.js";
import { warn } from "@/lib/logging/logger.js";

const EVENT_FIELDS = {
  message_id: "message-id",
  provider_message_sid: "text-2",
  timestamp: "timestamp",
  direction: "direction",
  event_type: "category",
  message_variant: "message-variant",
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
  sms_agent: "sms-agent",
  conversation: "conversation",
  market: "market",
  ai_route: "ai-route",
  processed_by: "processed-by",
  source_app: "source-app",
  trigger_name: "trigger-name",
  message: "message",
  template: "template",
  property_address: "property-address",
  character_count: "character-count",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  latency_ms: "latency-ms",
  ai_output: "ai-output",
};

function clean(value) {
  return String(value ?? "").trim();
}

// Returns current time as "YYYY-MM-DD HH:MM:SS" in America/Chicago so that
// Podio date fields display Central time to ops.
function nowCentral() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const get = (type) => parts.find((p) => p.type === type)?.value ?? "00";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

function mapDeliveryStatusForEvent(send_result) {
  if (!send_result?.ok) return "Failed";
  return "Sent";
}

function asArrayAppRef(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? [parsed] : undefined;
}

export function buildOutboundMessageEventFields({
  brain_item = null,
  conversation_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  market_id = null,
  phone_item_id = null,
  outbound_number_item_id = null,
  sms_agent_id = null,
  property_address = null,
  message_body = "",
  provider_message_id = null,
  queue_item_id = null,
  client_reference_id = null,
  template_id = null,
  message_variant = null,
  latency_ms = null,
  selected_use_case = null,
  template_use_case = null,
  next_expected_stage = null,
  selected_variant_group = null,
  selected_tone = null,
  send_result = null,
  processed_by = "Queue Runner",
  source_app = "Send Queue",
  trigger_name = "queue-send",
} = {}) {
  const ai_route = getCategoryValue(brain_item, "ai-route", null);
  const resolved_message_id = provider_message_id || client_reference_id || null;
  const conversation_relation = asArrayAppRef(conversation_item_id || brain_item?.item_id);
  const missing_relation_warnings = [];

  if (phone_item_id && !asArrayAppRef(phone_item_id)) {
    missing_relation_warnings.push("phone_relation_invalid");
  }
  if (property_id && !asArrayAppRef(property_id)) {
    missing_relation_warnings.push("property_relation_invalid");
  }
  if (template_id && !asArrayAppRef(template_id)) {
    missing_relation_warnings.push("template_relation_invalid");
  }
  if ((conversation_item_id || brain_item?.item_id) && !asArrayAppRef(conversation_item_id || brain_item?.item_id)) {
    missing_relation_warnings.push("conversation_relation_invalid");
  }

  if (missing_relation_warnings.length) {
    warn("events.outbound_relation_payload_incomplete", {
      master_owner_id,
      prospect_id,
      property_id,
      market_id,
      phone_item_id,
      outbound_number_item_id,
      conversation_item_id: conversation_item_id || brain_item?.item_id || null,
      template_id,
      warnings: missing_relation_warnings,
    });
  }

  return {
    [EVENT_FIELDS.message_id]: resolved_message_id,
    [EVENT_FIELDS.provider_message_sid]: provider_message_id || null,
    [EVENT_FIELDS.direction]: "Outbound",
    [EVENT_FIELDS.event_type]: "Seller Outbound SMS",
    [EVENT_FIELDS.timestamp]: { start: nowCentral() },
    [EVENT_FIELDS.message]: String(message_body || ""),
    [EVENT_FIELDS.character_count]: String(message_body || "").length,
    [EVENT_FIELDS.delivery_status]: mapDeliveryStatusForEvent(send_result),
    [EVENT_FIELDS.raw_carrier_status]:
      send_result?.status || send_result?.error_status || "sent",
    [EVENT_FIELDS.processed_by]: processed_by,
    [EVENT_FIELDS.source_app]: source_app,
    [EVENT_FIELDS.trigger_name]:
      queue_item_id ? buildQueueSendTriggerName(queue_item_id) : trigger_name,
    [EVENT_FIELDS.ai_output]: serializeMessageEventMetadata(
      buildQueueMessageEventMetadata({
        queue_item_id,
        client_reference_id,
        provider_message_id,
        event_kind: "outbound_send",
        message_variant,
        master_owner_id,
        prospect_id,
        property_id,
        market_id,
        phone_item_id,
        outbound_number_item_id,
        conversation_item_id: conversation_item_id || brain_item?.item_id || null,
        template_id,
        selected_use_case: clean(selected_use_case) || null,
        template_use_case: clean(template_use_case) || null,
        next_expected_stage: clean(next_expected_stage) || null,
        selected_variant_group: clean(selected_variant_group) || null,
        selected_tone: clean(selected_tone) || null,
      })
    ),
    ...(message_variant !== null && message_variant !== undefined
      ? { [EVENT_FIELDS.message_variant]: Number(message_variant) || undefined }
      : {}),
    ...(asArrayAppRef(master_owner_id)
      ? { [EVENT_FIELDS.master_owner]: asArrayAppRef(master_owner_id) }
      : {}),
    ...(asArrayAppRef(prospect_id)
      ? { [EVENT_FIELDS.prospect]: asArrayAppRef(prospect_id) }
      : {}),
    ...(asArrayAppRef(property_id)
      ? { [EVENT_FIELDS.property]: asArrayAppRef(property_id) }
      : {}),
    ...(asArrayAppRef(market_id)
      ? { [EVENT_FIELDS.market]: asArrayAppRef(market_id) }
      : {}),
    ...(asArrayAppRef(phone_item_id)
      ? { [EVENT_FIELDS.phone_number]: asArrayAppRef(phone_item_id) }
      : {}),
    ...(asArrayAppRef(outbound_number_item_id)
      ? { [EVENT_FIELDS.textgrid_number]: asArrayAppRef(outbound_number_item_id) }
      : {}),
    ...(conversation_relation
      ? { [EVENT_FIELDS.conversation]: conversation_relation }
      : {}),
    ...(asArrayAppRef(template_id)
      ? { [EVENT_FIELDS.template]: asArrayAppRef(template_id) }
      : {}),
    ...(asArrayAppRef(sms_agent_id)
      ? { [EVENT_FIELDS.sms_agent]: asArrayAppRef(sms_agent_id) }
      : {}),
    ...(clean(property_address)
      ? { [EVENT_FIELDS.property_address]: clean(property_address) }
      : {}),
    ...(latency_ms !== null && latency_ms !== undefined
      ? { [EVENT_FIELDS.latency_ms]: Number(latency_ms) || 0 }
      : {}),
    ...(ai_route ? { [EVENT_FIELDS.ai_route]: ai_route } : {}),
  };
}

export async function logOutboundMessageEvent(payload = {}) {
  const fields = buildOutboundMessageEventFields(payload);

  const created = await createMessageEvent(fields);

  await linkMessageEventToBrain({
    brain_item: payload.brain_item || null,
    brain_id: payload.conversation_item_id || payload.brain_item?.item_id || null,
    message_event_id: created?.item_id ?? null,
  });

  return created;
}

export default logOutboundMessageEvent;
