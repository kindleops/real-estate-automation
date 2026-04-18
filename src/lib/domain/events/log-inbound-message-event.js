import { createMessageEvent, updateMessageEvent, getCategoryValue } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import { buildQueueMessageEventMetadata } from "@/lib/domain/events/message-event-metadata.js";
import {
  buildBaseSellerMessageEventFields,
  buildInboundMessageEventKey,
  extractOptOutDetails,
} from "@/lib/domain/events/seller-message-event.js";

const defaultDeps = {
  createMessageEvent,
  updateMessageEvent,
  getCategoryValue,
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

export function __setLogInboundMessageEventTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetLogInboundMessageEventTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export function buildInboundMessageEventFields({
  brain_item = null,
  conversation_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  market_id = null,
  phone_item_id = null,
  inbound_number_item_id = null,
  sms_agent_id = null,
  property_address = null,
  message_body = "",
  provider_message_id = null,
  raw_carrier_status = "received",
  received_at = null,
  processed_by = "Manual Sender",
  source_app = "External API",
  trigger_name = "textgrid-inbound",
  inbound_from = null,
  inbound_to = null,
  prior_message_id = null,
  response_to_message_id = null,
  stage_before = null,
  stage_after = null,
  metadata = {},
  opt_out_keyword = null,
  is_opt_out = null,
} = {}) {
  const resolved_conversation_item_id =
    asValidId(conversation_item_id) ||
    asValidId(brain_item?.item_id) ||
    null;
  const ai_route = runtimeDeps.getCategoryValue(brain_item, "ai-route", null);
  const detected_opt_out = extractOptOutDetails(message_body);
  const explicit_opt_out = Boolean(is_opt_out);

  return buildBaseSellerMessageEventFields({
    message_event_key: buildInboundMessageEventKey({
      provider_message_id,
      from: inbound_from,
      to: inbound_to,
      message_body,
      received_at,
    }),
    provider_message_id,
    timestamp: received_at || nowCentral(),
    direction: "Inbound",
    event_type: "Seller Inbound SMS",
    message_body,
    delivery_status: "Received",
    raw_carrier_status,
    property_address,
    ai_route,
    processed_by,
    source_app,
    trigger_name,
    prior_message_id,
    response_to_message_id,
    stage_before,
    stage_after,
    relationship_ids: {
      master_owner_id,
      prospect_id,
      property_id,
      market_id,
      phone_item_id,
      textgrid_number_item_id: inbound_number_item_id,
      sms_agent_id,
      conversation_item_id: resolved_conversation_item_id,
    },
    metadata: buildQueueMessageEventMetadata({
      event_kind: "inbound_message",
      provider_message_id,
      message_event_key: buildInboundMessageEventKey({
        provider_message_id,
        from: inbound_from,
        to: inbound_to,
        message_body,
        received_at,
      }),
      inbound_from: clean(inbound_from) || null,
      inbound_to: clean(inbound_to) || null,
      master_owner_id: asValidId(master_owner_id),
      prospect_id: asValidId(prospect_id),
      property_id: asValidId(property_id),
      market_id: asValidId(market_id),
      phone_item_id: asValidId(phone_item_id),
      inbound_number_item_id: asValidId(inbound_number_item_id),
      sms_agent_id: asValidId(sms_agent_id),
      conversation_item_id: resolved_conversation_item_id,
      ...(metadata && typeof metadata === "object" ? metadata : {}),
    }),
    opt_out: explicit_opt_out || detected_opt_out["is-opt-out"] === "Yes",
    opt_out_keyword:
      clean(opt_out_keyword) ||
      clean(detected_opt_out["opt-out-keyword"]) ||
      null,
  });
}

export async function logInboundMessageEvent({
  record_item_id = null,
  ...payload
} = {}) {
  const fields = buildInboundMessageEventFields(payload);

  let created;
  if (record_item_id) {
    await runtimeDeps.updateMessageEvent(record_item_id, fields);
    created = { item_id: record_item_id };
  } else {
    created = await runtimeDeps.createMessageEvent(fields);
  }

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

export default logInboundMessageEvent;
