// ─── handle-textgrid-delivery.js ─────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  getItem,
  fetchAllItems,
  getFirstAppReferenceId,
  getCategoryValue,
  updateItem,
} from "@/lib/providers/podio.js";
import { getCategoryOptionId } from "@/lib/podio/schema.js";
import {
  findBestBrainMatch,
  findLatestBrainByMasterOwnerId,
  findLatestBrainByProspectId,
} from "@/lib/podio/apps/ai-conversation-brain.js";
import {
  PHONE_FIELDS,
  updatePhoneNumberItem,
} from "@/lib/podio/apps/phone-numbers.js";
import { findMessageEventsByMessageId as findMessageEventItemsByMessageId } from "@/lib/podio/apps/message-events.js";

import { mapTextgridFailureBucket } from "@/lib/providers/textgrid.js";
import {
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
// logDeliveryEvent intentionally not imported — delivery callbacks only update
// original outbound events, never create new Message Event items.
import {
  getQueueItemIdFromMessageEvent,
  isQueueSendEventItem,
  isVerificationTextgridSendEventItem,
  parseQueueItemIdFromClientReference,
} from "@/lib/domain/events/message-event-metadata.js";
import { updateMessageEventStatus } from "@/lib/domain/events/update-message-event-status.js";
import { updateBrainAfterDelivery } from "@/lib/domain/brain/update-brain-after-delivery.js";
import { info, warn } from "@/lib/logging/logger.js";
import { normalizeTextgridDeliveryPayload } from "@/lib/webhooks/textgrid-delivery-normalize.js";

const QUEUE_FIELDS = {
  queue_status: "queue-status",
  delivered_at: "delivered-at",
  failed_reason: "failed-reason",
  delivery_confirmed: "delivery-confirmed",
  master_owner: "master-owner",
  prospects: "prospects",
  properties: "properties",
  phone_number: "phone-number",
  textgrid_number: "textgrid-number",
};

const EVENT_FIELDS = {
  phone_number: "phone-number",
  textgrid_number: "textgrid-number",
  master_owner: "master-owner",
  prospect: "linked-seller",
  conversation: "conversation",
  processed_by: "processed-by",
  source_app: "source-app",
};

const defaultDeps = {
  getItem,
  fetchAllItems,
  getFirstAppReferenceId,
  getCategoryValue,
  updateItem,
  findBestBrainMatch,
  findLatestBrainByMasterOwnerId,
  findLatestBrainByProspectId,
  updatePhoneNumberItem,
  findMessageEventItemsByMessageId,
  mapTextgridFailureBucket,
  hashIdempotencyPayload,
  // logDeliveryEvent removed — delivery callbacks update existing events only
  updateMessageEventStatus,
  updateBrainAfterDelivery,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setTextgridDeliveryTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetTextgridDeliveryTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function nowIso() {
  return new Date().toISOString();
}

// Returns current time as "YYYY-MM-DD HH:MM:SS" in America/Chicago so that
// operational Podio date fields (Delivered At) display Central time to ops
// instead of the UTC hours that toISOString() would produce.
function nowPodioDateTimeCentral() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const get = (type) => parts.find((p) => p.type === type)?.value ?? "00";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function extractWebhookPayload(payload = {}) {
  const normalized = normalizeTextgridDeliveryPayload(payload?.raw || payload);
  const message_id = normalized.message_id || null;
  const from = normalized.from || null;
  const to = normalized.to || null;
  const status = lower(
    normalized.status ||
      payload.status ||
      payload.event_type ||
      payload.event ||
      ""
  );
  const error_message =
    normalized.error_message ||
    payload.error_message ||
    payload.error?.message ||
    "";
  const error_status =
    normalized.error_code ||
    payload.error_status ||
    payload.error?.status ||
    payload.status_code ||
    null;
  const client_reference_id =
    normalized.client_reference_id ||
    payload.client_reference_id ||
    payload.clientReferenceId ||
    payload.external_id ||
    payload.externalId ||
    payload.raw?.client_reference_id ||
    payload.raw?.clientReferenceId ||
    payload.raw?.external_id ||
    payload.raw?.externalId ||
    null;
  const delivered_at =
    normalized.delivered_at ||
    payload.delivered_at ||
    payload.timestamp ||
    payload.updated_at ||
    payload.raw?.delivered_at ||
    payload.raw?.timestamp ||
    payload.raw?.updated_at ||
    null;

  return {
    raw: payload,
    message_id,
    from,
    to,
    status,
    error_message: clean(error_message),
    error_status,
    client_reference_id: clean(client_reference_id) || null,
    delivered_at: clean(delivered_at) || null,
  };
}

function normalizeDeliveryState(status) {
  const raw = lower(status);

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

function mapFailureReasonToQueueCategory({ error_message, error_status }) {
  const bucket = runtimeDeps.mapTextgridFailureBucket({
    ok: false,
    error_message,
    error_status,
  });

  if (bucket === "DNC") return "Opt-Out";
  if (bucket === "Hard Bounce") return "Invalid Number";
  if (bucket === "Soft Bounce") return "Network Error";
  if (bucket === "Spam") return "Carrier Block";

  const msg = lower(error_message);
  if (msg.includes("daily") && msg.includes("limit")) return "Daily Limit Hit";

  return "Network Error";
}

async function findMessageEventsByProviderMessageId(message_id) {
  if (!message_id) return [];
  return runtimeDeps.findMessageEventItemsByMessageId(message_id, 50, 0);
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function uniqueQueueItemIds(values = []) {
  return [...new Set(values.map((value) => Number(value || 0)).filter((value) => value > 0))];
}

function buildDeliveryIdempotencyKey(extracted = {}) {
  const base = {
    provider: "textgrid",
    message_id: clean(extracted.message_id) || null,
    from: clean(extracted.from) || null,
    to: clean(extracted.to) || null,
    status: clean(extracted.status) || null,
    error_status: clean(extracted.error_status) || null,
    error_message: clean(extracted.error_message) || null,
    delivered_at: clean(extracted.delivered_at) || null,
    client_reference_id: clean(extracted.client_reference_id) || null,
  };

  return runtimeDeps.hashIdempotencyPayload(base);
}

async function findCandidateQueueItemsFromEvent(event_item) {
  const phone_item_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.phone_number,
    null
  );
  const textgrid_number_item_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.textgrid_number,
    null
  );

  if (!phone_item_id && !textgrid_number_item_id) return [];

  const all_queue_items = await runtimeDeps.fetchAllItems(
    APP_IDS.send_queue,
    {},
    { page_size: 200 }
  );

  return all_queue_items.filter((queue_item) => {
    const queue_status = clean(
      runtimeDeps.getCategoryValue(queue_item, QUEUE_FIELDS.queue_status, "")
    );

    if (["Cancelled", "Blocked"].includes(queue_status)) return false;

    const queue_phone_item_id = runtimeDeps.getFirstAppReferenceId(
      queue_item,
      QUEUE_FIELDS.phone_number,
      null
    );

    const queue_textgrid_item_id = runtimeDeps.getFirstAppReferenceId(
      queue_item,
      QUEUE_FIELDS.textgrid_number,
      null
    );

    const phone_match = phone_item_id && queue_phone_item_id === phone_item_id;
    const tg_match =
      textgrid_number_item_id &&
      queue_textgrid_item_id === textgrid_number_item_id;

    return phone_match || tg_match;
  });
}

async function findOutboundSendEventsByProviderMessageId(message_id) {
  const matched_events = await findMessageEventsByProviderMessageId(message_id);
  return sortNewestFirst(
    matched_events.filter(
      (event_item) =>
        isQueueSendEventItem(event_item) ||
        isVerificationTextgridSendEventItem(event_item)
    )
  );
}

async function loadQueueItemsByIds(queue_item_ids = []) {
  const unique_ids = uniqueQueueItemIds(queue_item_ids);
  const loaded = await Promise.all(
    unique_ids.map((queue_item_id) => runtimeDeps.getItem(queue_item_id))
  );
  return loaded.filter((item) => item?.item_id);
}

async function updateQueueCandidates(candidates, normalized_state, extracted) {
  const failed_reason =
    normalized_state === "Failed"
      ? mapFailureReasonToQueueCategory({
          error_message: extracted.error_message,
          error_status: extracted.error_status,
        })
      : null;

  // Determine if the Podio queue-status field has a "Delivered" option.
  // The supplement adds this option (id:7 placeholder) once the Podio field is
  // updated.  When the option is absent, fall back to "Sent" so the write
  // succeeds and we don't lose the confirmed delivery state.
  const delivered_option_id = getCategoryOptionId(
    APP_IDS.send_queue,
    QUEUE_FIELDS.queue_status,
    "Delivered"
  );
  const effective_delivered_status = delivered_option_id !== null ? "Delivered" : "Sent";

  const central_now = nowPodioDateTimeCentral();

  runtimeDeps.info("textgrid.delivery_queue_update_start", {
    normalized_state,
    candidate_count: candidates.length,
    effective_delivered_status,
    delivered_option_available: delivered_option_id !== null,
    central_timestamp: central_now,
  });

  if (normalized_state === "Delivered" && delivered_option_id === null) {
    runtimeDeps.warn("queue.delivery_status_option_missing", {
      desired_status: "Delivered",
      fallback_status: "Sent",
      note: "Add 'Delivered' option to Send Queue::queue-status in Podio, then run the schema refresh script.",
    });
  }

  const results = [];

  for (const queue_item of candidates) {
    const queue_item_id = queue_item.item_id;

    if (normalized_state === "Delivered") {
      await runtimeDeps.updateItem(queue_item_id, {
        // Delivered At — written in Central time so ops sees local hours.
        [QUEUE_FIELDS.delivered_at]: { start: central_now },
        [QUEUE_FIELDS.delivery_confirmed]: "✅ Confirmed",
        [QUEUE_FIELDS.queue_status]: effective_delivered_status,
      });

      runtimeDeps.info("queue.delivery_lifecycle_transition", {
        queue_item_id,
        transition: "delivered",
        delivery_confirmed: "✅ Confirmed",
        queue_status: effective_delivered_status,
        delivered_at_central: central_now,
      });

      results.push({
        ok: true,
        queue_item_id,
        updated_state: "delivered",
        queue_status: effective_delivered_status,
      });
      continue;
    }

    if (normalized_state === "Failed") {
      await runtimeDeps.updateItem(queue_item_id, {
        [QUEUE_FIELDS.delivery_confirmed]: "❌ Failed",
        [QUEUE_FIELDS.queue_status]: "Failed",
        [QUEUE_FIELDS.failed_reason]: failed_reason,
      });

      runtimeDeps.info("queue.delivery_lifecycle_transition", {
        queue_item_id,
        transition: "failed",
        delivery_confirmed: "❌ Failed",
        queue_status: "Failed",
        failed_reason,
        error_message: extracted.error_message || null,
        error_status: extracted.error_status || null,
      });

      results.push({
        ok: true,
        queue_item_id,
        updated_state: "failed",
        failed_reason,
      });
      continue;
    }

    await runtimeDeps.updateItem(queue_item_id, {
      [QUEUE_FIELDS.delivery_confirmed]: "⏳ Pending",
    });

    results.push({
      ok: true,
      queue_item_id,
      updated_state: "pending",
    });
  }

  return results;
}

async function resolveBrainForEvent(event_item) {
  const conversation_item_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.conversation,
    null
  );
  if (conversation_item_id) {
    return runtimeDeps.getItem(conversation_item_id);
  }

  const prospect_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.prospect,
    null
  );
  const phone_item_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.phone_number,
    null
  );
  const master_owner_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.master_owner,
    null
  );

  return (
    (runtimeDeps.findBestBrainMatch
      ? await runtimeDeps.findBestBrainMatch({
          phone_item_id,
          prospect_id,
          master_owner_id,
        })
      : null) ||
    (prospect_id ? await runtimeDeps.findLatestBrainByProspectId(prospect_id) : null) ||
    (master_owner_id ? await runtimeDeps.findLatestBrainByMasterOwnerId(master_owner_id) : null) ||
    null
  );
}

async function resolveBrainForRefs({
  conversation_item_id = null,
  phone_item_id = null,
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  if (conversation_item_id) {
    return runtimeDeps.getItem(conversation_item_id);
  }

  return (
    (runtimeDeps.findBestBrainMatch
      ? await runtimeDeps.findBestBrainMatch({
          phone_item_id,
          prospect_id,
          master_owner_id,
        })
      : null) ||
    (prospect_id ? await runtimeDeps.findLatestBrainByProspectId(prospect_id) : null) ||
    (master_owner_id ? await runtimeDeps.findLatestBrainByMasterOwnerId(master_owner_id) : null) ||
    null
  );
}

async function updatePhoneComplianceFromDelivery(event_item, failure_bucket) {
  if (!["DNC", "Hard Bounce"].includes(failure_bucket)) return null;

  const phone_item_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.phone_number,
    null
  );
  if (!phone_item_id) return null;

  const payload = {
    [PHONE_FIELDS.do_not_call]: "TRUE",
    [PHONE_FIELDS.dnc_source]: "Carrier Flag",
    [PHONE_FIELDS.opt_out_date]: { start: nowIso() },
    [PHONE_FIELDS.last_compliance_check]: { start: nowIso() },
  };

  await runtimeDeps.updatePhoneNumberItem(phone_item_id, payload);
  return {
    phone_item_id,
    payload,
    suppression_reason:
      failure_bucket === "Hard Bounce"
        ? "carrier_destination_unreachable"
        : "carrier_opt_out",
  };
}

function deriveFailureBucket(extracted, normalized_state) {
  if (normalized_state !== "Failed") return null;

  return (
    runtimeDeps.mapTextgridFailureBucket({
      ok: false,
      error_message: extracted.error_message,
      error_status: extracted.error_status,
    }) || "Other"
  );
}

export async function resolveTextgridDeliveryCorrelation(extracted = {}) {
  const queue_item_id_from_client_reference = parseQueueItemIdFromClientReference(
    extracted.client_reference_id
  );
  const linked_events = extracted.message_id
    ? await findOutboundSendEventsByProviderMessageId(extracted.message_id)
    : [];
  const exact_queue_item_ids = uniqueQueueItemIds([
    queue_item_id_from_client_reference,
    ...linked_events.map((event_item) => getQueueItemIdFromMessageEvent(event_item)),
  ]);

  if (exact_queue_item_ids.length > 1) {
    return {
      ok: false,
      reason: "ambiguous_queue_correlation",
      correlation_mode: "ambiguous",
      linked_events,
      exact_queue_item_ids,
      queue_items: [],
    };
  }

  if (exact_queue_item_ids.length === 1) {
    return {
      ok: true,
      reason: "exact_queue_correlation_resolved",
      correlation_mode: queue_item_id_from_client_reference
        ? "client_reference"
        : "provider_message_event",
      linked_events,
      exact_queue_item_ids,
      queue_items: await loadQueueItemsByIds(exact_queue_item_ids),
    };
  }

  if (!linked_events.length) {
    return {
      ok: true,
      reason: "message_event_not_found",
      correlation_mode: "none",
      linked_events,
      exact_queue_item_ids,
      queue_items: [],
    };
  }

  const legacy_candidates = [];
  for (const event_item of linked_events) {
    const candidates = await findCandidateQueueItemsFromEvent(event_item);
    legacy_candidates.push(...candidates);
  }

  const queue_items = sortNewestFirst(
    legacy_candidates.filter(
      (candidate, index, all) =>
        all.findIndex((entry) => Number(entry?.item_id || 0) === Number(candidate?.item_id || 0)) ===
        index
    )
  );

  if (queue_items.length > 1) {
    return {
      ok: false,
      reason: "ambiguous_legacy_queue_correlation",
      correlation_mode: "legacy_phone_match",
      linked_events,
      exact_queue_item_ids,
      queue_items,
    };
  }

  return {
    ok: true,
    reason: queue_items.length ? "legacy_queue_correlation_resolved" : "message_event_not_found",
    correlation_mode: queue_items.length ? "legacy_phone_match" : "none",
    linked_events,
    exact_queue_item_ids,
    queue_items,
  };
}

export async function handleTextgridDeliveryWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);
  const normalized_state = normalizeDeliveryState(extracted.status);
  const failure_bucket = deriveFailureBucket(extracted, normalized_state);
  const idempotency_key = buildDeliveryIdempotencyKey(extracted);
  const queue_item_id_from_client_reference = parseQueueItemIdFromClientReference(
    extracted.client_reference_id
  );

  runtimeDeps.info("textgrid.delivery_received", {
    message_id: extracted.message_id,
    from: extracted.from || null,
    to: extracted.to || null,
    status: extracted.status,
    normalized_state,
    error_code: extracted.error_status || null,
    error_message: extracted.error_message || null,
    client_reference_id: extracted.client_reference_id,
    delivered_at_raw: extracted.delivered_at || null,
  });

  if (!extracted.message_id && !queue_item_id_from_client_reference) {
    runtimeDeps.warn("textgrid.delivery_missing_message_id", {
      status: extracted.status,
    });

    return {
      ok: false,
      reason: "missing_message_id",
    };
  }

  // ── Lightweight duplicate guard ───────────────────────────────────────
  // Instead of writing idempotency records to the Message Events Podio app
  // (which pollutes the view with non-message items), we rely on the fact
  // that delivery status updates are idempotent — re-applying the same
  // status to an event or queue item is a no-op.  This avoids creating any
  // new Podio items for delivery callbacks.

  try {
    const correlation = await resolveTextgridDeliveryCorrelation(extracted);
    const linked_events = correlation.linked_events || [];
    const exact_queue_item_ids = correlation.exact_queue_item_ids || [];
    let correlation_mode = correlation.correlation_mode || "none";
    let queue_items = correlation.queue_items || [];
    let queue_results = [];

    if (!correlation.ok && correlation.reason === "ambiguous_queue_correlation") {
      runtimeDeps.warn("textgrid.delivery_ambiguous_queue_correlation", {
        message_id: extracted.message_id,
        client_reference_id: extracted.client_reference_id,
        queue_item_ids: exact_queue_item_ids,
      });

      return {
        ok: false,
        reason: correlation.reason,
        message_id: extracted.message_id,
        client_reference_id: extracted.client_reference_id,
        queue_item_ids: exact_queue_item_ids,
        matched_event_count: linked_events.length,
      };
    }

    if (!correlation.ok && correlation.reason === "ambiguous_legacy_queue_correlation") {
      runtimeDeps.warn("textgrid.delivery_ambiguous_legacy_queue_match", {
        message_id: extracted.message_id,
        queue_item_ids: queue_items.map((item) => item?.item_id || null).filter(Boolean),
      });

      return {
        ok: false,
        reason: correlation.reason,
        message_id: extracted.message_id,
        matched_event_count: linked_events.length,
        candidate_queue_item_ids: queue_items
          .map((item) => item?.item_id || null)
          .filter(Boolean),
      };
    }

    if (exact_queue_item_ids.length === 1 || queue_items.length === 1) {
      queue_results = await updateQueueCandidates(
        queue_items,
        normalized_state,
        extracted
      );
    }

    if (!linked_events.length && !queue_items.length) {
      runtimeDeps.warn("textgrid.delivery_event_not_found", {
        message_id: extracted.message_id,
        client_reference_id: extracted.client_reference_id,
        status: extracted.status,
      });

      return {
        ok: false,
        reason: "message_event_not_found",
        message_id: extracted.message_id,
        client_reference_id: extracted.client_reference_id,
      };
    }

    const primary_event = linked_events[0] || null;
    const primary_queue_item = queue_items[0] || null;
    const primary_master_owner_id =
      runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.master_owner, null) ||
      runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.master_owner, null);
    const primary_prospect_id =
      runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.prospect, null) ||
      runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.prospects, null);
    const primary_phone_item_id =
      runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.phone_number, null) ||
      runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.phone_number, null);
    const primary_conversation_item_id =
      runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.conversation, null) ||
      null;
    const primary_brain_item = await resolveBrainForRefs({
      conversation_item_id: primary_conversation_item_id,
      phone_item_id: primary_phone_item_id,
      prospect_id: primary_prospect_id,
      master_owner_id: primary_master_owner_id,
    });
    const primary_brain_id = primary_brain_item?.item_id || primary_conversation_item_id || null;

    // No longer create a separate delivery event — just update existing events.

    for (const event_item of linked_events) {
      await runtimeDeps.updateMessageEventStatus({
        event_item_id: event_item.item_id,
        provider_message_id: extracted.message_id,
        delivery_status: normalized_state,
        raw_carrier_status: extracted.error_status || extracted.status || normalized_state,
        failure_bucket,
        is_final_failure: normalized_state === "Failed",
      });
    }

    const results = [];

    for (const event_item of linked_events) {
      const brain_item = await resolveBrainForEvent(event_item);
      const brain_id = brain_item?.item_id || null;
      const phone_update = await updatePhoneComplianceFromDelivery(
        event_item,
        failure_bucket
      );

      await runtimeDeps.updateBrainAfterDelivery({
        brain_id,
        delivery_status: normalized_state,
        failure_bucket,
      });

      results.push({
        event_item_id: event_item.item_id,
        brain_id,
        phone_update,
      });
    }

    runtimeDeps.info("textgrid.delivery_processed", {
      message_id: extracted.message_id,
      status: extracted.status,
      normalized_state,
      matched_event_count: linked_events.length,
      queue_item_count: queue_items.length,
      correlation_mode,
    });

    const result = {
      ok: true,
      message_id: extracted.message_id,
      client_reference_id: extracted.client_reference_id,
      status: extracted.status,
      normalized_state,
      matched_event_count: linked_events.length,
      queue_item_count: queue_items.length,
      correlation_mode,
      queue_results,
      results,
      idempotency_key,
    };

    return result;
  } catch (error) {
    throw error;
  }
}

export const handleTextgridDelivery = handleTextgridDeliveryWebhook;

export default handleTextgridDeliveryWebhook;
