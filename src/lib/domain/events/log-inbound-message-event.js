// ─── log-inbound-message-event.js ────────────────────────────────────────
import { createMessageEvent, updateMessageEvent, getCategoryValue } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";

const EVENT_FIELDS = {
  message_id: "message-id",
  provider_message_sid: "text-2",
  timestamp: "timestamp",
  direction: "direction",
  event_type: "category",
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
  conversation: "conversation",
  ai_route: "ai-route",
  processed_by: "processed-by",
  source_app: "source-app",
  trigger_name: "trigger-name",
  message: "message",
  character_count: "character-count",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  ai_output: "ai-output",
  processing_metadata: "processing-metadata",
};

function nowIso() {
  return new Date().toISOString();
}

function asArrayAppRef(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? [parsed] : undefined;
}

const defaultDeps = {
  createMessageEvent,
  updateMessageEvent,
  getCategoryValue,
  linkMessageEventToBrain,
};

let runtimeDeps = { ...defaultDeps };

export function __setLogInboundMessageEventTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetLogInboundMessageEventTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function logInboundMessageEvent({
  record_item_id = null,
  brain_item = null,
  conversation_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  inbound_number_item_id = null,
  message_body = "",
  provider_message_id = null,
  raw_carrier_status = "received",
  received_at = null,
  processed_by = "Manual Sender",
  source_app = "External API",
  trigger_name = "textgrid-inbound",
  processing_metadata = null,
} = {}) {
  const ai_route = runtimeDeps.getCategoryValue(brain_item, "ai-route", null);
  const normalized_message = String(message_body || "");

  const fields = {
    [EVENT_FIELDS.provider_message_sid]: provider_message_id || null,
    [EVENT_FIELDS.direction]: "Inbound",
    [EVENT_FIELDS.event_type]: "Seller Inbound SMS",
    [EVENT_FIELDS.timestamp]: { start: received_at || nowIso() },
    [EVENT_FIELDS.message]: normalized_message,
    [EVENT_FIELDS.character_count]: normalized_message.length,
    [EVENT_FIELDS.delivery_status]: "Received",
    [EVENT_FIELDS.raw_carrier_status]: raw_carrier_status,
    [EVENT_FIELDS.processed_by]: processed_by,
    [EVENT_FIELDS.source_app]: source_app,
    [EVENT_FIELDS.trigger_name]: trigger_name,
    [EVENT_FIELDS.ai_output]: "",
    ...(asArrayAppRef(master_owner_id)
      ? { [EVENT_FIELDS.master_owner]: asArrayAppRef(master_owner_id) }
      : {}),
    ...(asArrayAppRef(prospect_id)
      ? { [EVENT_FIELDS.prospect]: asArrayAppRef(prospect_id) }
      : {}),
    ...(asArrayAppRef(property_id)
      ? { [EVENT_FIELDS.property]: asArrayAppRef(property_id) }
      : {}),
    ...(asArrayAppRef(phone_item_id)
      ? { [EVENT_FIELDS.phone_number]: asArrayAppRef(phone_item_id) }
      : {}),
    ...(asArrayAppRef(inbound_number_item_id)
      ? { [EVENT_FIELDS.textgrid_number]: asArrayAppRef(inbound_number_item_id) }
      : {}),
    ...(asArrayAppRef(conversation_item_id || brain_item?.item_id)
      ? {
          [EVENT_FIELDS.conversation]: asArrayAppRef(
            conversation_item_id || brain_item?.item_id
          ),
        }
      : {}),
    ...(ai_route ? { [EVENT_FIELDS.ai_route]: ai_route } : {}),
  };

  if (processing_metadata) {
    fields[EVENT_FIELDS.processing_metadata] =
      typeof processing_metadata === "string"
        ? processing_metadata
        : JSON.stringify(processing_metadata);
  }

  let created;
  if (record_item_id) {
    // Update the existing idempotency record with actual event data.
    // Preserve message-id (idempotency key) so dedup lookup still works.
    await runtimeDeps.updateMessageEvent(record_item_id, fields);
    created = { item_id: record_item_id };
  } else {
    // Fallback: create a new record (no idempotency record to enrich).
    fields[EVENT_FIELDS.message_id] = provider_message_id || null;
    created = await runtimeDeps.createMessageEvent(fields);
  }

  await runtimeDeps.linkMessageEventToBrain({
    brain_item,
    brain_id: conversation_item_id || brain_item?.item_id || null,
    message_event_id: created?.item_id ?? null,
  });

  return created;
}

export default logInboundMessageEvent;
