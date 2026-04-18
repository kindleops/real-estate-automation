import {
  getCategoryValue,
  updateItem,
} from "@/lib/providers/podio.js";
import {
  findMessageEventsByProviderMessageSid,
  getMessageEvent,
} from "@/lib/podio/apps/message-events.js";
import { isQueueSendEventItem } from "@/lib/domain/events/message-event-metadata.js";
import {
  SELLER_MESSAGE_EVENT_FIELDS,
  mergeMessageEventMetadata,
  normalizeSellerDeliveryStatus,
} from "@/lib/domain/events/seller-message-event.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
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

function normalizeProviderDeliveryStatus(value = null) {
  const raw = lower(value);

  if (!raw) return undefined;
  if (["queued", "pending", "accepted"].includes(raw)) return "Queued";
  if (["sending"].includes(raw)) return "Sending";
  if (["sent"].includes(raw)) return "Sent";
  if (["delivered", "delivery_confirmed", "confirmed"].includes(raw)) return "Delivered";
  if (["failed", "delivery_failed", "error"].includes(raw)) return "Failed";
  if (["undelivered"].includes(raw)) return "Undelivered";
  return "Unknown";
}

export async function findMessageEventByProviderMessageId(provider_message_id) {
  const message_id = clean(provider_message_id);
  if (!message_id) return null;

  const events = await findMessageEventsByProviderMessageSid(message_id, 50, 0);

  return (
    events.find((event_item) => isQueueSendEventItem(event_item)) ||
    events.find(
      (event_item) =>
        lower(getCategoryValue(event_item, "direction", "")) === "outbound"
    ) ||
    events[0] ||
    null
  );
}

export async function updateMessageEventStatus({
  event_item_id = null,
  provider_message_id,
  delivery_status = null,
  provider_delivery_status = null,
  raw_carrier_status = null,
  failure_bucket = null,
  is_final_failure = null,
  latency_ms = null,
  occurred_at = null,
  delivered_at = null,
  failed_at = null,
  failure_code = null,
  failure_reason = null,
} = {}) {
  const event_item = event_item_id
    ? await getMessageEvent(Number(event_item_id) || event_item_id)
    : await findMessageEventByProviderMessageId(provider_message_id);

  if (!event_item?.item_id) {
    return {
      ok: false,
      reason: "message_event_not_found",
      provider_message_id: clean(provider_message_id),
    };
  }

  const normalized_delivery_status =
    delivery_status ? normalizeSellerDeliveryStatus(delivery_status) : null;
  const normalized_provider_status =
    normalizeProviderDeliveryStatus(
      provider_delivery_status || raw_carrier_status || delivery_status
    );
  const fields = {
    ...(normalized_delivery_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.delivery_status]:
            normalized_delivery_status,
        }
      : {}),
    ...(normalized_provider_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.provider_delivery_status]:
            normalized_provider_status,
        }
      : {}),
    ...(raw_carrier_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.raw_carrier_status]:
            clean(raw_carrier_status),
        }
      : {}),
    ...(failure_bucket
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.failure_bucket]:
            clean(failure_bucket),
        }
      : {}),
    ...(latency_ms !== null && latency_ms !== undefined
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.latency_ms]:
            Number(latency_ms) || 0,
        }
      : {}),
  };

  const normalized_final_failure = normalizeFinalFailure(is_final_failure);
  if (normalized_final_failure) {
    fields[SELLER_MESSAGE_EVENT_FIELDS.is_final_failure] =
      normalized_final_failure;
  }

  const metadata = mergeMessageEventMetadata(event_item, {
    provider_message_id: clean(provider_message_id) || null,
    delivery_status: normalized_delivery_status || null,
    provider_delivery_status: normalized_provider_status || null,
    raw_carrier_status: clean(raw_carrier_status) || null,
    failure_bucket: clean(failure_bucket) || null,
    is_final_failure:
      normalized_final_failure === "Yes"
        ? true
        : normalized_final_failure === "No"
          ? false
          : null,
    occurred_at: clean(occurred_at) || null,
    delivered_at: clean(delivered_at) || clean(occurred_at) || null,
    failed_at: clean(failed_at) || clean(occurred_at) || null,
    failure_code: clean(failure_code) || null,
    failure_reason: clean(failure_reason) || null,
    latency_ms:
      latency_ms !== null && latency_ms !== undefined
        ? Number(latency_ms) || 0
        : null,
  });
  fields[SELLER_MESSAGE_EVENT_FIELDS.ai_output] = JSON.stringify(metadata);

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
