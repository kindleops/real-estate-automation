import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
  getCategoryValue,
  getDateValue,
  getNumberValue,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.send_queue;

const QUEUE_FIELDS = {
  queue_id_2: "queue-id-2",
  queue_status: "queue-status",
  scheduled_for_local: "scheduled-for-local",
  scheduled_for_utc: "scheduled-for-utc",
  retry_count: "retry-count",
  max_retries: "max-retries",
};

const RETRYABLE_STATUSES = new Set(["Failed"]);
const RUNNABLE_STATUSES = new Set(["Queued"]);

function clean(value) {
  return String(value ?? "").trim();
}

function normalizeQueueStatus(value = "") {
  const raw = clean(value).toLowerCase();

  if (raw === "queued") return "Queued";
  if (raw === "sending") return "Sending";
  if (raw === "sent") return "Sent";
  if (raw === "delivered") return "Sent";
  if (raw === "failed") return "Failed";
  if (raw === "blocked") return "Blocked";
  if (raw === "cancelled") return "Cancelled";

  return clean(value);
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function resolveScheduledAt(queue_item) {
  return (
    getDateValue(queue_item, QUEUE_FIELDS.scheduled_for_utc, null) ||
    getDateValue(queue_item, QUEUE_FIELDS.scheduled_for_local, null) ||
    null
  );
}

function sortOldestScheduledFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_ts = toTimestamp(resolveScheduledAt(a)) ?? Number.MAX_SAFE_INTEGER;
    const b_ts = toTimestamp(resolveScheduledAt(b)) ?? Number.MAX_SAFE_INTEGER;
    return a_ts - b_ts;
  });
}

export function isRunnableQueueStatus(status) {
  return RUNNABLE_STATUSES.has(normalizeQueueStatus(status));
}

export function isRetryableQueueStatus(status) {
  return RETRYABLE_STATUSES.has(normalizeQueueStatus(status));
}

export function isDueQueueItem(queue_item, now = new Date().toISOString()) {
  const now_ts = toTimestamp(now) ?? Date.now();
  const scheduled_ts = toTimestamp(resolveScheduledAt(queue_item));

  if (scheduled_ts === null) return true;
  return scheduled_ts <= now_ts;
}

export function canRetryQueueItem(queue_item) {
  const status = normalizeQueueStatus(
    getCategoryValue(queue_item, QUEUE_FIELDS.queue_status, "")
  );

  if (!isRetryableQueueStatus(status)) return false;

  const retry_count = Number(
    getNumberValue(queue_item, QUEUE_FIELDS.retry_count, 0) || 0
  );

  const max_retries = Number(
    getNumberValue(queue_item, QUEUE_FIELDS.max_retries, 3) || 3
  );

  return retry_count < max_retries;
}

export async function createSendQueueItem(fields = {}) {
  return createItem(APP_ID, fields);
}

export async function getSendQueueItem(item_id) {
  return getItem(item_id);
}

export async function updateSendQueueItem(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export async function findSendQueueItems(filters = {}, limit = 30, offset = 0) {
  return filterAppItems(APP_ID, filters, { limit, offset });
}

export async function findSendQueueItemByQueueId(queue_id) {
  if (!queue_id) return null;
  return findByField(APP_ID, QUEUE_FIELDS.queue_id_2, queue_id);
}

export async function findAllSendQueueItemsByQueueId(queue_id) {
  if (!queue_id) return [];
  const response = await filterAppItems(
    APP_ID,
    { [QUEUE_FIELDS.queue_id_2]: queue_id },
    { limit: 10, offset: 0 }
  );
  const items = Array.isArray(response?.items) ? response.items : Array.isArray(response) ? response : [];
  return items;
}

export async function findQueuedSendQueueItems(limit = 25, offset = 0) {
  return filterAppItems(
    APP_ID,
    { [QUEUE_FIELDS.queue_status]: "Queued" },
    { limit, offset }
  );
}

export async function findFailedSendQueueItems(limit = 25, offset = 0) {
  return filterAppItems(
    APP_ID,
    { [QUEUE_FIELDS.queue_status]: "Failed" },
    { limit, offset }
  );
}

export async function findDueSendQueueItems({
  limit = 25,
  offset = 0,
  now = new Date().toISOString(),
} = {}) {
  const queued_items = await findQueuedSendQueueItems(Math.max(limit * 2, 50), offset);

  return sortOldestScheduledFirst(
    queued_items.filter((item) => isDueQueueItem(item, now))
  ).slice(0, limit);
}

export async function findRetryableSendQueueItems({
  limit = 25,
  offset = 0,
} = {}) {
  const failed_items = await findFailedSendQueueItems(Math.max(limit * 2, 50), offset);

  return sortOldestScheduledFirst(
    failed_items.filter((item) => canRetryQueueItem(item))
  ).slice(0, limit);
}

export default {
  APP_ID,
  QUEUE_FIELDS,
  RETRYABLE_STATUSES,
  RUNNABLE_STATUSES,
  isRunnableQueueStatus,
  isRetryableQueueStatus,
  isDueQueueItem,
  canRetryQueueItem,
  createSendQueueItem,
  getSendQueueItem,
  updateSendQueueItem,
  findSendQueueItems,
  findSendQueueItemByQueueId,
  findAllSendQueueItemsByQueueId,
  findQueuedSendQueueItems,
  findFailedSendQueueItems,
  findDueSendQueueItems,
  findRetryableSendQueueItems,
};
