import { createMessageEvent, getCategoryValue } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import {
  buildQueueMessageEventMetadata,
  buildQueueSendTriggerName,
} from "@/lib/domain/events/message-event-metadata.js";
import {
  buildBaseSellerMessageEventFields,
  buildOutboundMessageEventKey,
} from "@/lib/domain/events/seller-message-event.js";
import { warn } from "@/lib/logging/logger.js";

const defaultDeps = {
  createMessageEvent,
  linkMessageEventToBrain,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function asValidId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
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

export function __setLogOutboundMessageEventTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetLogOutboundMessageEventTestDeps() {
  runtimeDeps = { ...defaultDeps };
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
  sent_at = null,
  prior_message_id = null,
  response_to_message_id = null,
} = {}) {
  const resolved_provider_message_id =
    clean(provider_message_id) || clean(send_result?.message_id) || null;
  const resolved_conversation_item_id =
    asValidId(conversation_item_id) ||
    asValidId(brain_item?.item_id) ||
    null;
  const ai_route = getCategoryValue(brain_item, "ai-route", null);
  const stage_before = getCategoryValue(brain_item, "conversation-stage", null);
  const relationship_ids = {
    master_owner_id,
    prospect_id,
    property_id,
    market_id,
    phone_item_id,
    textgrid_number_item_id: outbound_number_item_id,
    sms_agent_id,
    conversation_item_id: resolved_conversation_item_id,
    template_id,
  };
  const invalid_relationships = Object.entries({
    master_owner_id,
    prospect_id,
    property_id,
    market_id,
    phone_item_id,
    outbound_number_item_id,
    sms_agent_id,
    conversation_item_id: resolved_conversation_item_id,
    template_id,
  })
    .filter(([, value]) => value !== null && value !== undefined && !asValidId(value))
    .map(([key]) => key);

  if (invalid_relationships.length) {
    warn("events.outbound_relation_payload_incomplete", {
      queue_item_id,
      invalid_relationships,
    });
  }

  return buildBaseSellerMessageEventFields({
    message_event_key: buildOutboundMessageEventKey({
      queue_item_id,
      client_reference_id,
      provider_message_id: resolved_provider_message_id,
    }),
    provider_message_id: resolved_provider_message_id,
    timestamp: sent_at || nowCentral(),
    direction: "Outbound",
    event_type: "Seller Outbound SMS",
    message_body,
    delivery_status: send_result?.ok === false ? "Failed" : "Sent",
    provider_delivery_status: send_result?.status || "sent",
    raw_carrier_status:
      send_result?.status || send_result?.error_status || "sent",
    message_variant,
    latency_ms,
    property_address,
    ai_route,
    processed_by,
    source_app,
    trigger_name:
      queue_item_id ? buildQueueSendTriggerName(queue_item_id) : trigger_name,
    prior_message_id,
    response_to_message_id,
    stage_before,
    stage_after: clean(next_expected_stage) || null,
    relationship_ids,
    metadata: buildQueueMessageEventMetadata({
      queue_item_id,
      client_reference_id,
      provider_message_id: resolved_provider_message_id,
      event_kind: "outbound_send",
      message_event_key: buildOutboundMessageEventKey({
        queue_item_id,
        client_reference_id,
        provider_message_id: resolved_provider_message_id,
      }),
      message_variant,
      master_owner_id: asValidId(master_owner_id),
      prospect_id: asValidId(prospect_id),
      property_id: asValidId(property_id),
      market_id: asValidId(market_id),
      phone_item_id: asValidId(phone_item_id),
      outbound_number_item_id: asValidId(outbound_number_item_id),
      sms_agent_id: asValidId(sms_agent_id),
      conversation_item_id: resolved_conversation_item_id,
      template_id: asValidId(template_id),
      selected_use_case: clean(selected_use_case) || null,
      template_use_case: clean(template_use_case) || null,
      next_expected_stage: clean(next_expected_stage) || null,
      selected_variant_group: clean(selected_variant_group) || null,
      selected_tone: clean(selected_tone) || null,
    }),
  });
}

export async function logOutboundMessageEvent(payload = {}) {
  const fields = buildOutboundMessageEventFields(payload);
  const created = await runtimeDeps.createMessageEvent(fields);

  await runtimeDeps.linkMessageEventToBrain({
    brain_item: payload.brain_item || null,
    brain_id:
      payload.conversation_item_id ||
      payload.brain_item?.item_id ||
      null,
    message_event_id: created?.item_id ?? null,
  });

  return created;
}

export default logOutboundMessageEvent;
