// ─── log-inbound-message-event.js ────────────────────────────────────────
import { createMessageEvent, getCategoryValue } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";

const EVENT_FIELDS = {
  message_id: "message-id",
  timestamp: "timestamp",
  direction: "direction",
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
};

function nowIso() {
  return new Date().toISOString();
}

export async function logInboundMessageEvent({
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
  processed_by = "Inbound Webhook",
  source_app = "TextGrid",
  trigger_name = "textgrid-inbound",
} = {}) {
  const ai_route = getCategoryValue(brain_item, "ai-route", null);
  const normalized_message = String(message_body || "");

  const fields = {
    [EVENT_FIELDS.message_id]: provider_message_id || null,
    [EVENT_FIELDS.direction]: "Inbound",
    [EVENT_FIELDS.timestamp]: { start: nowIso() },
    [EVENT_FIELDS.message]: normalized_message,
    [EVENT_FIELDS.character_count]: normalized_message.length,
    [EVENT_FIELDS.delivery_status]: "Received",
    [EVENT_FIELDS.raw_carrier_status]: raw_carrier_status,
    [EVENT_FIELDS.processed_by]: processed_by,
    [EVENT_FIELDS.source_app]: source_app,
    [EVENT_FIELDS.trigger_name]: trigger_name,
    ...(master_owner_id ? { [EVENT_FIELDS.master_owner]: master_owner_id } : {}),
    ...(prospect_id ? { [EVENT_FIELDS.prospect]: prospect_id } : {}),
    ...(property_id ? { [EVENT_FIELDS.property]: property_id } : {}),
    ...(phone_item_id ? { [EVENT_FIELDS.phone_number]: phone_item_id } : {}),
    ...(inbound_number_item_id ? { [EVENT_FIELDS.textgrid_number]: inbound_number_item_id } : {}),
    ...((conversation_item_id || brain_item?.item_id)
      ? { [EVENT_FIELDS.conversation]: conversation_item_id || brain_item?.item_id }
      : {}),
    ...(ai_route ? { [EVENT_FIELDS.ai_route]: ai_route } : {}),
  };

  const created = await createMessageEvent(fields);

  await linkMessageEventToBrain({
    brain_item,
    brain_id: conversation_item_id || brain_item?.item_id || null,
    message_event_id: created?.item_id ?? null,
  });

  return created;
}

export default logInboundMessageEvent;
