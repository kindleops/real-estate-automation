import { mapTextgridFailureBucket } from "@/lib/providers/textgrid.js";
import { updateMessageEventStatus } from "@/lib/domain/events/update-message-event-status.js";

const defaultDeps = {
  mapTextgridFailureBucket,
  updateMessageEventStatus,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function normalizeDeliveryStatus(value) {
  const raw = lower(value);

  if (["delivered", "delivery_confirmed", "confirmed"].includes(raw)) {
    return "Delivered";
  }

  if (["failed", "undelivered", "delivery_failed", "error"].includes(raw)) {
    return "Failed";
  }

  if (["received"].includes(raw)) {
    return "Received";
  }

  if (["sent"].includes(raw)) {
    return "Sent";
  }

  if (["queued", "accepted", "pending"].includes(raw)) {
    return "Pending";
  }

  return "Sent";
}

export function __setLogDeliveryEventTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetLogDeliveryEventTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function logDeliveryEvent({
  provider_message_id = null,
  delivery_status = null,
  raw_carrier_status = null,
  error_message = null,
  error_status = null,
  event_item_id = null,
  occurred_at = null,
} = {}) {
  const normalized_status = normalizeDeliveryStatus(
    delivery_status || raw_carrier_status
  );
  const is_failed = normalized_status === "Failed";
  const failure_bucket = is_failed
    ? runtimeDeps.mapTextgridFailureBucket({
        ok: false,
        error_message,
        error_status,
      }) || "Other"
    : null;

  const result = await runtimeDeps.updateMessageEventStatus({
    event_item_id,
    provider_message_id,
    delivery_status: normalized_status,
    provider_delivery_status: raw_carrier_status || delivery_status || normalized_status,
    raw_carrier_status: String(
      error_status || raw_carrier_status || normalized_status || ""
    ),
    failure_bucket,
    is_final_failure: is_failed,
    occurred_at,
    delivered_at: normalized_status === "Delivered" ? occurred_at : null,
    failed_at: normalized_status === "Failed" ? occurred_at : null,
    failure_code: error_status,
    failure_reason: error_message,
  });

  if (!result.ok) {
    return {
      ok: false,
      reason: result.reason || "message_event_not_found",
      provider_message_id: clean(provider_message_id),
    };
  }

  return {
    ok: true,
    updated: true,
    event_item_id: result.event_item_id,
    provider_message_id: clean(provider_message_id),
    delivery_status: normalized_status,
    failure_bucket,
  };
}

export default logDeliveryEvent;
