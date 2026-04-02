// ─── handle-textgrid-delivery.js ─────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  getItem,
  fetchAllItems,
  getFirstAppReferenceId,
  getCategoryValue,
  updateItem,
} from "@/lib/providers/podio.js";
import {
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
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { logDeliveryEvent } from "@/lib/domain/events/log-delivery-event.js";
import {
  getQueueItemIdFromMessageEvent,
  isQueueSendEventItem,
  isVerificationTextgridSendEventItem,
  parseQueueItemIdFromClientReference,
} from "@/lib/domain/events/message-event-metadata.js";
import { updateMessageEventStatus } from "@/lib/domain/events/update-message-event-status.js";
import { updateBrainAfterDelivery } from "@/lib/domain/brain/update-brain-after-delivery.js";
import { info, warn } from "@/lib/logging/logger.js";

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
};

const defaultDeps = {
  getItem,
  fetchAllItems,
  getFirstAppReferenceId,
  getCategoryValue,
  updateItem,
  findLatestBrainByMasterOwnerId,
  findLatestBrainByProspectId,
  updatePhoneNumberItem,
  findMessageEventItemsByMessageId,
  mapTextgridFailureBucket,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  logDeliveryEvent,
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

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function extractWebhookPayload(payload = {}) {
  const message_id =
    payload.id ||
    payload.message_id ||
    payload.messageId ||
    null;

  const from =
    payload.from ||
    payload.sender ||
    null;

  const to =
    payload.to ||
    payload.recipient ||
    null;

  const status = lower(
    payload.status ||
      payload.event_type ||
      payload.event ||
      ""
  );

  const error_message =
    payload.error_message ||
    payload.error?.message ||
    "";

  const error_status =
    payload.error_status ||
    payload.error?.status ||
    payload.status_code ||
    null;

  const client_reference_id =
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

  const results = [];

  for (const queue_item of candidates) {
    const queue_item_id = queue_item.item_id;

    if (normalized_state === "Delivered") {
      await runtimeDeps.updateItem(queue_item_id, {
        [QUEUE_FIELDS.delivered_at]: { start: nowIso() },
        [QUEUE_FIELDS.delivery_confirmed]: "✅ Confirmed",
        [QUEUE_FIELDS.queue_status]: "Sent",
      });

      results.push({
        ok: true,
        queue_item_id,
        updated_state: "delivered",
      });
      continue;
    }

    if (normalized_state === "Failed") {
      await runtimeDeps.updateItem(queue_item_id, {
        [QUEUE_FIELDS.delivery_confirmed]: "❌ Failed",
        [QUEUE_FIELDS.queue_status]: "Failed",
        [QUEUE_FIELDS.failed_reason]: failed_reason,
      });

      results.push({
        ok: true,
        queue_item_id,
        updated_state: "failed",
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
  const prospect_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.prospect,
    null
  );
  const master_owner_id = runtimeDeps.getFirstAppReferenceId(
    event_item,
    EVENT_FIELDS.master_owner,
    null
  );

  return (
    (prospect_id ? await runtimeDeps.findLatestBrainByProspectId(prospect_id) : null) ||
    (master_owner_id ? await runtimeDeps.findLatestBrainByMasterOwnerId(master_owner_id) : null) ||
    null
  );
}

async function updatePhoneComplianceFromDelivery(event_item, failure_bucket) {
  if (failure_bucket !== "DNC") return null;

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
  return { phone_item_id, payload };
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
    status: extracted.status,
    normalized_state,
    client_reference_id: extracted.client_reference_id,
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

  const idempotency = await runtimeDeps.beginIdempotentProcessing({
    scope: "textgrid_delivery",
    key: idempotency_key,
    summary: `Processed delivery callback ${idempotency_key}`,
    metadata: {
      provider_message_id: clean(extracted.message_id) || null,
      client_reference_id: extracted.client_reference_id || null,
      normalized_state,
    },
  });

  if (!idempotency.ok) {
    return {
      ok: false,
      reason: idempotency.reason,
      message_id: extracted.message_id,
      client_reference_id: extracted.client_reference_id,
      idempotency_key,
    };
  }

  if (idempotency.duplicate) {
    runtimeDeps.info("textgrid.delivery_duplicate_ignored", {
      message_id: extracted.message_id,
      client_reference_id: extracted.client_reference_id,
      reason: idempotency.reason,
      idempotency_key,
    });

    return {
      ok: true,
      duplicate: true,
      updated: false,
      reason: idempotency.reason,
      message_id: extracted.message_id,
      client_reference_id: extracted.client_reference_id,
      normalized_state,
      idempotency_key,
    };
  }

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

      const result = {
        ok: false,
        reason: correlation.reason,
        message_id: extracted.message_id,
        client_reference_id: extracted.client_reference_id,
        queue_item_ids: exact_queue_item_ids,
        matched_event_count: linked_events.length,
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_delivery",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          client_reference_id: extracted.client_reference_id || null,
          queue_item_ids: exact_queue_item_ids,
        },
      });

      return result;
    }

    if (!correlation.ok && correlation.reason === "ambiguous_legacy_queue_correlation") {
      runtimeDeps.warn("textgrid.delivery_ambiguous_legacy_queue_match", {
        message_id: extracted.message_id,
        queue_item_ids: queue_items.map((item) => item?.item_id || null).filter(Boolean),
      });

      const result = {
        ok: false,
        reason: correlation.reason,
        message_id: extracted.message_id,
        matched_event_count: linked_events.length,
        candidate_queue_item_ids: queue_items
          .map((item) => item?.item_id || null)
          .filter(Boolean),
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_delivery",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          matched_event_count: linked_events.length,
        },
      });

      return result;
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

      const result = {
        ok: false,
        reason: "message_event_not_found",
        message_id: extracted.message_id,
        client_reference_id: extracted.client_reference_id,
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_delivery",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          client_reference_id: extracted.client_reference_id || null,
        },
      });

      return result;
    }

    const primary_event = linked_events[0] || null;
    const primary_queue_item = queue_items[0] || null;

    await runtimeDeps.logDeliveryEvent({
      provider_message_id: extracted.message_id,
      delivery_status: normalized_state,
      raw_carrier_status: extracted.error_status || extracted.status || normalized_state,
      error_message: extracted.error_message,
      error_status: extracted.error_status,
      queue_item_id: primary_queue_item?.item_id || exact_queue_item_ids[0] || null,
      client_reference_id: extracted.client_reference_id,
      master_owner_id:
        runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.master_owner, null) ||
        runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.master_owner, null),
      prospect_id:
        runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.prospect, null) ||
        runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.prospects, null),
      property_id:
        runtimeDeps.getFirstAppReferenceId(primary_event, "property", null) ||
        runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.properties, null),
      phone_item_id:
        runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.phone_number, null) ||
        runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.phone_number, null),
      textgrid_number_item_id:
        runtimeDeps.getFirstAppReferenceId(primary_event, EVENT_FIELDS.textgrid_number, null) ||
        runtimeDeps.getFirstAppReferenceId(primary_queue_item, QUEUE_FIELDS.textgrid_number, null),
      trigger_name:
        primary_queue_item?.item_id
          ? `textgrid-delivery:${primary_queue_item.item_id}`
          : "textgrid-delivery",
    });

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

    await runtimeDeps.completeIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "textgrid_delivery",
      key: idempotency_key,
      summary: `Delivery callback completed ${idempotency_key}`,
      metadata: {
        provider_message_id: clean(extracted.message_id) || null,
        client_reference_id: extracted.client_reference_id || null,
        normalized_state,
        matched_event_count: linked_events.length,
        queue_item_ids: queue_items.map((item) => item?.item_id || null).filter(Boolean),
        correlation_mode,
      },
    });

    return result;
  } catch (error) {
    await runtimeDeps.failIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "textgrid_delivery",
      key: idempotency_key,
      error,
      metadata: {
        provider_message_id: clean(extracted.message_id) || null,
        client_reference_id: extracted.client_reference_id || null,
        normalized_state,
      },
    });

    throw error;
  }
}

export const handleTextgridDelivery = handleTextgridDeliveryWebhook;

export default handleTextgridDeliveryWebhook;
