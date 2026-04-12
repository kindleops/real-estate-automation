// ─── log-delivery-event.js ───────────────────────────────────────────────
import { createMessageEvent } from "@/lib/providers/podio.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import { mapTextgridFailureBucket } from "@/lib/providers/textgrid.js";
import {
  buildQueueMessageEventMetadata,
  serializeMessageEventMetadata,
} from "@/lib/domain/events/message-event-metadata.js";

const EVENT_FIELDS = {
  message_id: "message-id",
  provider_message_sid: "text-2",
  timestamp: "timestamp",
  direction: "direction",
  event_type: "category",
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
  conversation: "conversation",
  processed_by: "processed-by",
  source_app: "source-app",
  trigger_name: "trigger-name",
  message: "message",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  failure_bucket: "failure-bucket",
  is_final_failure: "is-final-failure",
  ai_output: "ai-output",
};

function nowIso() {
  return new Date().toISOString();
}

function clean(value) {
  return String(value ?? "").trim();
}

function asArrayAppRef(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? [parsed] : undefined;
}

const defaultDeps = {
  createMessageEvent,
  linkMessageEventToBrain,
  mapTextgridFailureBucket,
};

let runtimeDeps = { ...defaultDeps };

export function __setLogDeliveryEventTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetLogDeliveryEventTestDeps() {
  runtimeDeps = { ...defaultDeps };
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

export async function logDeliveryEvent({
  provider_message_id = null,
  delivery_status = null,
  raw_carrier_status = null,
  error_message = null,
  error_status = null,
  queue_item_id = null,
  client_reference_id = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  textgrid_number_item_id = null,
  conversation_item_id = null,
  processed_by = "System",
  source_app = "External API",
  trigger_name = "textgrid-delivery",
} = {}) {
  const normalized_status = normalizeDeliveryStatus(delivery_status || raw_carrier_status);
  const is_failed = normalized_status === "Failed";

  const failure_bucket = is_failed
    ? runtimeDeps.mapTextgridFailureBucket({
        ok: false,
        error_message,
        error_status,
      }) || "Other"
    : null;

  const message =
    is_failed
      ? `Delivery failed: ${clean(error_message) || "Unknown error"}`
      : `Delivery update: ${normalized_status}`;

  const fields = {
    [EVENT_FIELDS.message_id]: provider_message_id || null,
    [EVENT_FIELDS.provider_message_sid]: provider_message_id || null,
    [EVENT_FIELDS.direction]: "Outbound",
    [EVENT_FIELDS.event_type]: is_failed ? "Send Failure" : "Delivery Update",
    [EVENT_FIELDS.timestamp]: { start: nowIso() },
    [EVENT_FIELDS.message]: message,
    [EVENT_FIELDS.delivery_status]: normalized_status,
    [EVENT_FIELDS.raw_carrier_status]: String(
      error_status || raw_carrier_status || normalized_status || ""
    ),
    [EVENT_FIELDS.processed_by]: processed_by,
    [EVENT_FIELDS.source_app]: source_app,
    [EVENT_FIELDS.trigger_name]: trigger_name,
    [EVENT_FIELDS.ai_output]: serializeMessageEventMetadata(
      buildQueueMessageEventMetadata({
        queue_item_id,
        client_reference_id,
        provider_message_id,
        event_kind: "delivery_update",
        conversation_item_id,
      })
    ),
    ...(asArrayAppRef(master_owner_id)
      ? { [EVENT_FIELDS.master_owner]: asArrayAppRef(master_owner_id) }
      : {}),
    ...(asArrayAppRef(prospect_id)
      ? { [EVENT_FIELDS.prospect]: asArrayAppRef(prospect_id) }
      : {}),
    ...(asArrayAppRef(property_id)
      ? { [EVENT_FIELDS.property]: asArrayAppRef(property_id) }
      : {}),
    ...(asArrayAppRef(phone_item_id)
      ? { [EVENT_FIELDS.phone_number]: asArrayAppRef(phone_item_id) }
      : {}),
    ...(asArrayAppRef(textgrid_number_item_id)
      ? { [EVENT_FIELDS.textgrid_number]: asArrayAppRef(textgrid_number_item_id) }
      : {}),
    ...(asArrayAppRef(conversation_item_id)
      ? { [EVENT_FIELDS.conversation]: asArrayAppRef(conversation_item_id) }
      : {}),
    ...(failure_bucket ? { [EVENT_FIELDS.failure_bucket]: failure_bucket } : {}),
    ...(is_failed ? { [EVENT_FIELDS.is_final_failure]: "Yes" } : {}),
  };

  const created = await runtimeDeps.createMessageEvent(fields);

  await runtimeDeps.linkMessageEventToBrain({
    brain_id: conversation_item_id || null,
    message_event_id: created?.item_id ?? null,
  });

  return created;
}

export default logDeliveryEvent;
