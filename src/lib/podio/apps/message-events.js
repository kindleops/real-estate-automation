import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.message_events;

const EVENT_FIELDS = {
  message_id: "message-id",
  provider_message_sid: "text-2",
  direction: "direction",
  delivery_status: "status-3",
  provider_delivery_status: "delivery-status",
  raw_carrier_status: "status-2",
  failure_bucket: "failure-bucket",
};

export async function createMessageEvent(fields = {}) {
  return createItem(APP_ID, fields);
}

export async function getMessageEvent(item_id) {
  return getItem(item_id);
}

export async function updateMessageEvent(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export async function findMessageEvents(filters = {}, limit = 30, offset = 0, options = {}) {
  const res = await filterAppItems(APP_ID, filters, { limit, offset, ...(options || {}) });
  return res?.items ?? [];
}

export async function findMessageEventByMessageId(message_id) {
  if (!message_id) return null;
  return findByField(APP_ID, EVENT_FIELDS.message_id, message_id);
}

export async function findMessageEventByProviderMessageSid(provider_message_sid) {
  if (!provider_message_sid) return null;

  return (
    (await findByField(APP_ID, EVENT_FIELDS.provider_message_sid, provider_message_sid)) ||
    (await findMessageEventByMessageId(provider_message_sid)) ||
    null
  );
}

export async function findMessageEventsByMessageId(
  message_id,
  limit = 50,
  offset = 0,
  options = {}
) {
  if (!message_id) return [];
  return findMessageEvents(
    { [EVENT_FIELDS.message_id]: message_id },
    limit,
    offset,
    options
  );
}

export async function findMessageEventsByProviderMessageSid(
  provider_message_sid,
  limit = 50,
  offset = 0,
  options = {}
) {
  if (!provider_message_sid) return [];

  const [by_provider_sid, by_legacy_message_id] = await Promise.all([
    findMessageEvents(
      { [EVENT_FIELDS.provider_message_sid]: provider_message_sid },
      limit,
      offset,
      options
    ),
    findMessageEventsByMessageId(provider_message_sid, limit, offset, options),
  ]);

  const deduped = new Map();
  for (const item of [...(by_provider_sid || []), ...(by_legacy_message_id || [])]) {
    if (!item?.item_id) continue;
    deduped.set(Number(item.item_id), item);
  }

  return [...deduped.values()];
}

export async function findMessageEventsByTriggerName(
  trigger_name,
  limit = 50,
  offset = 0,
  options = {}
) {
  if (!trigger_name) return [];
  return findMessageEvents(
    { "trigger-name": trigger_name },
    limit,
    offset,
    options
  );
}

export default {
  APP_ID,
  EVENT_FIELDS,
  createMessageEvent,
  getMessageEvent,
  updateMessageEvent,
  findMessageEvents,
  findMessageEventByMessageId,
  findMessageEventByProviderMessageSid,
  findMessageEventsByMessageId,
  findMessageEventsByProviderMessageSid,
  findMessageEventsByTriggerName,
};
