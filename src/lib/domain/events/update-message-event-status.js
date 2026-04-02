// ─── update-message-event-status.js ──────────────────────────────────────
import {
  getCategoryValue,
  getTextValue,
  updateItem,
} from "@/lib/providers/podio.js";
import { findMessageEventsByMessageId } from "@/lib/podio/apps/message-events.js";
import { isQueueSendEventItem } from "@/lib/domain/events/message-event-metadata.js";

const EVENT_FIELDS = {
  message_id: "message-id",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  failure_bucket: "failure-bucket",
  is_final_failure: "is-final-failure",
};

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function normalizeDeliveryStatus(value, fallback = "Sent") {
  const raw = lower(value);

  if (["queued", "pending", "accepted"].includes(raw)) return "Pending";
  if (raw === "sent") return "Sent";
  if (raw === "delivered") return "Delivered";
  if (raw === "failed") return "Failed";
  if (raw === "undelivered") return "Failed";
  if (raw === "received") return "Received";

  return fallback;
}

function normalizeFinalFailure(value) {
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }

  const raw = lower(value);
  if (["yes", "true", "1"].includes(raw)) return "Yes";
  if (["no", "false", "0"].includes(raw)) return "No";

  return undefined;
}

export async function findMessageEventByProviderMessageId(provider_message_id) {
  const message_id = clean(provider_message_id);
  if (!message_id) return null;

  const events = await findMessageEventsByMessageId(message_id, 50, 0);

  return (
    events.find((event_item) => isQueueSendEventItem(event_item)) ||
    events.find(
      (event_item) =>
        lower(getCategoryValue(event_item, "direction", "")) === "outbound" &&
        lower(getTextValue(event_item, "trigger-name", "")) !== "textgrid-delivery"
    ) ||
    events[0] ||
    null
  );
}

export async function updateMessageEventStatus({
  event_item_id = null,
  provider_message_id,
  delivery_status = null,
  raw_carrier_status = null,
  failure_bucket = null,
  is_final_failure = null,
} = {}) {
  const event_item = event_item_id
    ? { item_id: Number(event_item_id) || event_item_id }
    : await findMessageEventByProviderMessageId(provider_message_id);

  if (!event_item?.item_id) {
    return {
      ok: false,
      reason: "message_event_not_found",
      provider_message_id: clean(provider_message_id),
    };
  }

  const fields = {
    ...(delivery_status
      ? {
          [EVENT_FIELDS.delivery_status]: normalizeDeliveryStatus(delivery_status),
        }
      : {}),
    ...(raw_carrier_status
      ? {
          [EVENT_FIELDS.raw_carrier_status]: clean(raw_carrier_status),
        }
      : {}),
    ...(failure_bucket
      ? {
          [EVENT_FIELDS.failure_bucket]: clean(failure_bucket),
        }
      : {}),
  };

  const normalized_final_failure = normalizeFinalFailure(is_final_failure);
  if (normalized_final_failure) {
    fields[EVENT_FIELDS.is_final_failure] = normalized_final_failure;
  }

  if (!Object.keys(fields).length) {
    return {
      ok: false,
      reason: "no_fields_to_update",
      provider_message_id: clean(provider_message_id),
      event_item_id: event_item.item_id,
    };
  }

  await updateItem(event_item.item_id, fields);

  return {
    ok: true,
    provider_message_id: clean(provider_message_id),
    event_item_id: event_item.item_id,
    updated_fields: fields,
  };
}

export default updateMessageEventStatus;
