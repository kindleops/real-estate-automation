// ─── log-outbound-message-event.js ───────────────────────────────────────
import { createMessageEvent, getCategoryValue } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import {
  buildQueueMessageEventMetadata,
  buildQueueSendTriggerName,
  serializeMessageEventMetadata,
} from "@/lib/domain/events/message-event-metadata.js";

const EVENT_FIELDS = {
  message_id: "message-id",
  timestamp: "timestamp",
  direction: "direction",
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
  ai_route: "ai-route",
  processed_by: "processed-by",
  source_app: "source-app",
  trigger_name: "trigger-name",
  message: "message",
  template_selected: "template-selected",
  character_count: "character-count",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  ai_output: "ai-output",
};

function nowIso() {
  return new Date().toISOString();
}

function mapDeliveryStatusForEvent(send_result) {
  if (!send_result?.ok) return "Failed";
  return "Sent";
}

export async function logOutboundMessageEvent({
  brain_item = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  outbound_number_item_id = null,
  message_body = "",
  provider_message_id = null,
  queue_item_id = null,
  client_reference_id = null,
  template_id = null,
  send_result = null,
  processed_by = "Scheduled Campaign",
  source_app = "Send Queue",
  trigger_name = "queue-send",
} = {}) {
  const ai_route = getCategoryValue(brain_item, "ai-route", null);

  const fields = {
    [EVENT_FIELDS.message_id]: provider_message_id || null,
    [EVENT_FIELDS.direction]: "Outbound",
    [EVENT_FIELDS.timestamp]: { start: nowIso() },
    [EVENT_FIELDS.message]: String(message_body || ""),
    [EVENT_FIELDS.character_count]: String(message_body || "").length,
    [EVENT_FIELDS.delivery_status]: mapDeliveryStatusForEvent(send_result),
    [EVENT_FIELDS.raw_carrier_status]: send_result?.status || "sent",
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
      })
    ),
    ...(master_owner_id ? { [EVENT_FIELDS.master_owner]: master_owner_id } : {}),
    ...(prospect_id ? { [EVENT_FIELDS.prospect]: prospect_id } : {}),
    ...(property_id ? { [EVENT_FIELDS.property]: property_id } : {}),
    ...(phone_item_id ? { [EVENT_FIELDS.phone_number]: phone_item_id } : {}),
    ...(outbound_number_item_id ? { [EVENT_FIELDS.textgrid_number]: outbound_number_item_id } : {}),
    ...(template_id ? { [EVENT_FIELDS.template_selected]: template_id } : {}),
    ...(ai_route ? { [EVENT_FIELDS.ai_route]: ai_route } : {}),
  };

  const created = await createMessageEvent(fields);

  await linkMessageEventToBrain({
    brain_item,
    brain_id: brain_item?.item_id ?? null,
    message_event_id: created?.item_id ?? null,
  });

  return created;
}

export default logOutboundMessageEvent;
