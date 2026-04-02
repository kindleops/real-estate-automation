import {
  getMessageEvent,
  findMessageEventByMessageId,
  findMessageEvents,
} from "@/lib/podio/apps/message-events.js";

export async function findMessageEventsQuery({
  item_id = null,
  message_id = null,
  filters = null,
  limit = 25,
  offset = 0,
}) {
  if (item_id) {
    const item = await getMessageEvent(item_id);
    return item ? [item] : [];
  }

  if (message_id) {
    const item = await findMessageEventByMessageId(message_id);
    return item ? [item] : [];
  }

  if (filters) {
    const res = await findMessageEvents(filters, limit, offset);
    return res?.items ?? res ?? [];
  }

  return [];
}

export default findMessageEventsQuery;