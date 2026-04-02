export function buildQueueId(prefix = "queue") {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

export function buildEventId(prefix = "event") {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

export function buildRecordKey(prefix = "rec") {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

export default {
  buildQueueId,
  buildEventId,
  buildRecordKey,
};