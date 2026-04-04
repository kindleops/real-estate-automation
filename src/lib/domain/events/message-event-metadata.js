import { getTextValue } from "@/lib/providers/podio.js";

const AI_OUTPUT_FIELD = "ai-output";

function clean(value) {
  return String(value ?? "").trim();
}

export function serializeMessageEventMetadata(metadata = {}) {
  try {
    return JSON.stringify(metadata);
  } catch {
    return "";
  }
}

export function parseMessageEventMetadata(input = null) {
  const raw =
    typeof input === "string"
      ? input
      : getTextValue(input, AI_OUTPUT_FIELD, "");

  if (!clean(raw)) return {};

  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

export function buildQueueSendTriggerName(queue_item_id) {
  return `queue-send:${clean(queue_item_id)}`;
}

export function buildQueueSendFailedTriggerName(queue_item_id) {
  return `queue-send-failed:${clean(queue_item_id)}`;
}

export function buildQueueClientReferenceId(queue_item_id) {
  return `queue-${clean(queue_item_id)}`;
}

export function buildVerificationTextgridSendTriggerName(run_id = "") {
  return `verification-textgrid-send:${clean(run_id)}`;
}

export function buildVerificationTextgridClientReferenceId(run_id = "") {
  return `verify-textgrid-${clean(run_id)}`;
}

export function parseQueueItemIdFromTriggerName(trigger_name = "") {
  const match = clean(trigger_name).match(/^queue-send(?:-failed)?:([0-9]+)$/i);
  return match ? Number(match[1]) : null;
}

export function parseQueueItemIdFromClientReference(client_reference_id = "") {
  const match = clean(client_reference_id).match(/^queue-([0-9]+)$/i);
  return match ? Number(match[1]) : null;
}

export function getQueueItemIdFromMessageEvent(event_item = null) {
  const meta = parseMessageEventMetadata(event_item);
  const meta_queue_item_id = Number(meta?.queue_item_id || 0);

  if (Number.isFinite(meta_queue_item_id) && meta_queue_item_id > 0) {
    return meta_queue_item_id;
  }

  return parseQueueItemIdFromTriggerName(
    getTextValue(event_item, "trigger-name", "")
  );
}

export function isQueueSendEventTriggerName(trigger_name = "") {
  return /^queue-send(?::[0-9]+)?$/i.test(clean(trigger_name));
}

export function isQueueSendFailedTriggerName(trigger_name = "") {
  return /^queue-send-failed(?::[0-9]+)?$/i.test(clean(trigger_name));
}

export function isVerificationTextgridSendTriggerName(trigger_name = "") {
  return /^verification-textgrid-send(?::.+)?$/i.test(clean(trigger_name));
}

export function isQueueSendEventItem(event_item = null) {
  return isQueueSendEventTriggerName(getTextValue(event_item, "trigger-name", ""));
}

export function isQueueSendFailedEventItem(event_item = null) {
  return isQueueSendFailedTriggerName(getTextValue(event_item, "trigger-name", ""));
}

export function isVerificationTextgridSendEventItem(event_item = null) {
  return isVerificationTextgridSendTriggerName(
    getTextValue(event_item, "trigger-name", "")
  );
}

export function buildQueueMessageEventMetadata({
  queue_item_id = null,
  client_reference_id = null,
  provider_message_id = null,
  event_kind = "outbound_send",
  ...extra
} = {}) {
  return {
    version: 1,
    event_kind,
    queue_item_id: queue_item_id ? Number(queue_item_id) : null,
    client_reference_id: clean(client_reference_id) || null,
    provider_message_id: clean(provider_message_id) || null,
    ...extra,
  };
}
