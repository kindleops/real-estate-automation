// ─── validate-send-queue-item.js ─────────────────────────────────────────
import {
  getCategoryValue,
  getFirstAppReferenceId,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function isTerminalStatus(status) {
  return ["sent", "failed", "cancelled", "blocked"].includes(lower(status));
}

export function validateSendQueueItem(queue_item = null) {
  if (!queue_item?.item_id) {
    return {
      ok: false,
      reason: "missing_queue_item",
    };
  }

  const queue_status = getCategoryValue(queue_item, "queue-status", null);
  const phone_item_id = getFirstAppReferenceId(queue_item, "phone-number", null);
  const textgrid_number_item_id = getFirstAppReferenceId(queue_item, "textgrid-number", null);
  const message_text = getTextValue(queue_item, "message-text", "");
  const retry_count = Number(getNumberValue(queue_item, "retry-count", 0) || 0);
  const max_retries = Number(getNumberValue(queue_item, "max-retries", 3) || 3);

  if (queue_status && isTerminalStatus(queue_status)) {
    return {
      ok: false,
      reason: `terminal_status:${queue_status}`,
      queue_status,
      skipped: true,
    };
  }

  if (!phone_item_id) {
    return {
      ok: false,
      reason: "missing_phone_item",
      queue_status,
    };
  }

  if (!textgrid_number_item_id) {
    return {
      ok: false,
      reason: "missing_textgrid_number",
      queue_status,
    };
  }

  if (!clean(message_text)) {
    return {
      ok: false,
      reason: "empty_message_body",
      queue_status,
    };
  }

  if (retry_count >= max_retries) {
    return {
      ok: false,
      reason: "max_retries_exceeded",
      queue_status,
      retry_count,
      max_retries,
    };
  }

  return {
    ok: true,
    queue_status,
    phone_item_id,
    textgrid_number_item_id,
    message_text: clean(message_text),
    retry_count,
    max_retries,
  };
}

export default validateSendQueueItem;
