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
  direction: "direction",
  delivery_status: "status-3",
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

export async function findMessageEvents(filters = {}, limit = 30, offset = 0) {
  return filterAppItems(APP_ID, filters, { limit, offset });
}

export async function findMessageEventByMessageId(message_id) {
  if (!message_id) return null;
  return findByField(APP_ID, EVENT_FIELDS.message_id, message_id);
}

export async function findMessageEventsByMessageId(message_id, limit = 50, offset = 0) {
  if (!message_id) return [];
  return findMessageEvents(
    { [EVENT_FIELDS.message_id]: message_id },
    limit,
    offset
  );
}

export async function findMessageEventsByTriggerName(trigger_name, limit = 50, offset = 0) {
  if (!trigger_name) return [];
  return findMessageEvents(
    { "trigger-name": trigger_name },
    limit,
    offset
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
  findMessageEventsByMessageId,
  findMessageEventsByTriggerName,
};
