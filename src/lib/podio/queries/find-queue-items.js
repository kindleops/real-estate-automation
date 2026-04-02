import {
  getSendQueueItem,
  findSendQueueItemByQueueId,
  findSendQueueItems,
  findDueSendQueueItems,
  findRetryableSendQueueItems,
} from "@/lib/podio/apps/send-queue.js";

export async function findQueueItems({
  item_id = null,
  queue_id = null,
  filters = null,
  due_only = false,
  retryable_only = false,
  limit = 25,
  offset = 0,
}) {
  if (item_id) {
    const item = await getSendQueueItem(item_id);
    return item ? [item] : [];
  }

  if (queue_id) {
    const item = await findSendQueueItemByQueueId(queue_id);
    return item ? [item] : [];
  }

  if (due_only) {
    const res = await findDueSendQueueItems(limit, offset);
    return res?.items ?? res ?? [];
  }

  if (retryable_only) {
    const res = await findRetryableSendQueueItems(limit, offset);
    return res?.items ?? res ?? [];
  }

  if (filters) {
    const res = await findSendQueueItems(filters, limit, offset);
    return res?.items ?? res ?? [];
  }

  return [];
}

export default findQueueItems;