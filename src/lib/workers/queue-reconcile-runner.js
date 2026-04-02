import APP_IDS from "@/lib/config/app-ids.js";

import {
  fetchAllItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
  updateItem,
} from "@/lib/providers/podio.js";
import { findMessageEventsByTriggerName } from "@/lib/podio/apps/message-events.js";
import {
  buildQueueSendFailedTriggerName,
  buildQueueSendTriggerName,
  isQueueSendEventItem,
  isQueueSendFailedEventItem,
} from "@/lib/domain/events/message-event-metadata.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";
import { getTextgridProviderCapabilities } from "@/lib/providers/textgrid.js";

import { info, warn } from "@/lib/logging/logger.js";

const DEFAULT_LIMIT = 50;
const DEFAULT_STALE_AFTER_MINUTES = 20;

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toTimestamp(value) {
  if (!value) return null;
  const timestamp = new Date(value).getTime();
  return Number.isNaN(timestamp) ? null : timestamp;
}

function getItemUpdatedAt(item) {
  return (
    item?.current_revision?.created_on ||
    item?.last_edit_on ||
    item?.created_on ||
    null
  );
}

function getEventOccurredAt(event_item) {
  return (
    getDateValue(event_item, "timestamp", null) ||
    event_item?.current_revision?.created_on ||
    event_item?.last_edit_on ||
    event_item?.created_on ||
    null
  );
}

function sortNewestFirst(items = [], getDate = getEventOccurredAt) {
  return [...items].sort((a, b) => {
    const a_ts = toTimestamp(getDate(a)) ?? Number(a?.item_id || 0);
    const b_ts = toTimestamp(getDate(b)) ?? Number(b?.item_id || 0);
    return b_ts - a_ts;
  });
}

function isSending(item) {
  return lower(getCategoryValue(item, "queue-status", "")) === "sending";
}

function isStale(item, stale_after_ms, now_ts) {
  const updated_at = getItemUpdatedAt(item);
  const updated_ts = toTimestamp(updated_at);

  if (updated_ts === null) {
    return false;
  }

  return now_ts - updated_ts >= stale_after_ms;
}

function buildDeliveryTriggerName(queue_item_id) {
  return `textgrid-delivery:${clean(queue_item_id)}`;
}

function mapFailureBucketToQueueReason(failure_bucket = "") {
  const normalized = lower(failure_bucket);

  if (normalized === "dnc") return "Opt-Out";
  if (normalized === "hard bounce") return "Invalid Number";
  if (normalized === "spam") return "Carrier Block";
  if (normalized === "soft bounce") return "Network Error";
  if (normalized === "other") return "Network Error";

  return null;
}

export async function findQueueEvidenceEvents(queue_item_id) {
  const [send_events, failed_events, delivery_events] = await Promise.all([
    findMessageEventsByTriggerName(buildQueueSendTriggerName(queue_item_id), 20, 0),
    findMessageEventsByTriggerName(buildQueueSendFailedTriggerName(queue_item_id), 20, 0),
    findMessageEventsByTriggerName(buildDeliveryTriggerName(queue_item_id), 20, 0),
  ]);

  return sortNewestFirst([
    ...(Array.isArray(send_events) ? send_events : []),
    ...(Array.isArray(failed_events) ? failed_events : []),
    ...(Array.isArray(delivery_events) ? delivery_events : []),
  ]);
}

export function classifyQueueEvidence(events = []) {
  const sorted_events = sortNewestFirst(events);

  for (const event_item of sorted_events) {
    const delivery_status = lower(getCategoryValue(event_item, "status-3", ""));

    if (delivery_status === "delivered") {
      return {
        kind: "delivered",
        event_item,
      };
    }

    if (delivery_status === "failed" || isQueueSendFailedEventItem(event_item)) {
      return {
        kind: "failed",
        event_item,
      };
    }
  }

  for (const event_item of sorted_events) {
    const delivery_status = lower(getCategoryValue(event_item, "status-3", ""));
    const provider_message_id = clean(getTextValue(event_item, "message-id", ""));

    if (
      isQueueSendEventItem(event_item) ||
      ["sent", "pending", "received"].includes(delivery_status) ||
      provider_message_id
    ) {
      return {
        kind: "accepted",
        event_item,
      };
    }
  }

  return {
    kind: "unknown",
    event_item: sorted_events[0] || null,
  };
}

export async function recoverQueueItemFromEvidence(
  queue_item_id,
  evidence,
  now,
  deps = {}
) {
  const update = deps.updateItem || updateItem;
  const provider_capabilities =
    deps.getTextgridProviderCapabilities?.() || getTextgridProviderCapabilities();
  const event_item = evidence?.event_item || null;
  const event_timestamp = getEventOccurredAt(event_item) || now;
  const failure_bucket = getCategoryValue(event_item, "failure-bucket", null);
  const provider_message_id = clean(getTextValue(event_item, "message-id", ""));

  if (evidence?.kind === "delivered") {
    await update(queue_item_id, {
      "queue-status": "Sent",
      "delivery-confirmed": "✅ Confirmed",
      "delivered-at": { start: event_timestamp },
    });

    return {
      ok: true,
      action: "recovered_delivered",
      provider_message_id: provider_message_id || null,
      evidence_event_item_id: event_item?.item_id || null,
      evidence_timestamp: event_timestamp,
    };
  }

  if (evidence?.kind === "failed") {
    await update(queue_item_id, {
      "queue-status": "Failed",
      "delivery-confirmed": "❌ Failed",
      "failed-reason":
        mapFailureBucketToQueueReason(failure_bucket) || "Network Error",
    });

    return {
      ok: true,
      action: "recovered_failed",
      provider_message_id: provider_message_id || null,
      evidence_event_item_id: event_item?.item_id || null,
      evidence_timestamp: event_timestamp,
      failure_bucket: clean(failure_bucket) || null,
    };
  }

  if (evidence?.kind === "accepted") {
    await update(queue_item_id, {
      "queue-status": "Sent",
      "delivery-confirmed": "⏳ Pending",
    });

    return {
      ok: true,
      action: "recovered_sent_pending_delivery",
      provider_message_id: provider_message_id || null,
      evidence_event_item_id: event_item?.item_id || null,
      evidence_timestamp: event_timestamp,
    };
  }

  await update(queue_item_id, {
    "queue-status": "Blocked",
    "delivery-confirmed": "⏳ Pending",
  });

  return {
    ok: true,
    action: "blocked_manual_review_provider_verification_incomplete",
    provider_message_id: provider_message_id || null,
    evidence_event_item_id: event_item?.item_id || null,
    evidence_timestamp: event_timestamp,
    provider_verification_available: Boolean(
      provider_capabilities?.message_status_lookup?.supported
    ),
    provider_verification_reason:
      provider_capabilities?.message_status_lookup?.reason ||
      "provider_verification_unavailable",
    manual_action_required: true,
  };
}

export async function runQueueReconcileRunner({
  limit = DEFAULT_LIMIT,
  stale_after_minutes = DEFAULT_STALE_AFTER_MINUTES,
  now = new Date().toISOString(),
  master_owner_id = null,
} = {}) {
  const provider_capabilities = getTextgridProviderCapabilities();
  const now_ts = toTimestamp(now) ?? Date.now();
  const stale_after_ms = Math.max(Number(stale_after_minutes) || 0, 1) * 60_000;
  const scoped_master_owner_id = Number(master_owner_id || 0) || null;

  return withRunLock({
    scope: scoped_master_owner_id
      ? `queue-reconcile:${scoped_master_owner_id}`
      : "queue-reconcile",
    lease_ms: 10 * 60_000,
    owner: "queue_reconcile_runner",
    metadata: {
      limit,
      stale_after_minutes,
      master_owner_id: scoped_master_owner_id,
    },
    onLocked: async (lock) => {
      await recordSystemAlert({
        subsystem: "reconcile",
        code: "runner_overlap",
        severity: "warning",
        retryable: true,
        summary: "Queue reconcile skipped because an active lease is already in progress.",
        dedupe_key: scoped_master_owner_id
          ? `reconcile:${scoped_master_owner_id}`
          : "reconcile",
        metadata: {
          now,
          limit,
          stale_after_minutes,
          master_owner_id: scoped_master_owner_id,
          lock,
        },
      });

      return {
        ok: true,
        skipped: true,
        reason: "queue_reconcile_lock_active",
        now,
        stale_after_minutes,
        scanned_count: 0,
        processed_count: 0,
        recovered_delivered_count: 0,
        recovered_failed_count: 0,
        recovered_sent_count: 0,
        manual_review_count: 0,
        skipped_count: 0,
        results: [],
        master_owner_id: scoped_master_owner_id,
        lock,
        provider_verification_available: Boolean(
          provider_capabilities?.message_status_lookup?.supported
        ),
        provider_verification_reason:
          provider_capabilities?.message_status_lookup?.reason ||
          "provider_verification_unavailable",
      };
    },
    fn: async () => {
      info("queue.reconcile_started", {
        limit,
        stale_after_minutes,
        now,
        master_owner_id: scoped_master_owner_id,
      });

      const sending_items = await fetchAllItems(
        APP_IDS.send_queue,
        {
          "queue-status": "Sending",
        },
        {
          page_size: Math.max(limit, 50),
        }
      );

      const stale_items = sending_items
        .filter((item) => isSending(item))
        .filter((item) =>
          scoped_master_owner_id
            ? Number(getFirstAppReferenceId(item, "master-owner", 0) || 0) ===
              scoped_master_owner_id
            : true
        )
        .filter((item) => isStale(item, stale_after_ms, now_ts))
        .slice(0, limit);

      let recovered_delivered_count = 0;
      let recovered_failed_count = 0;
      let recovered_sent_count = 0;
      let manual_review_count = 0;
      let skipped_count = 0;
      const results = [];

      for (const item of stale_items) {
        const queue_item_id = item?.item_id || null;

        try {
          const evidence_events = await findQueueEvidenceEvents(queue_item_id);
          const evidence = classifyQueueEvidence(evidence_events);
          const outcome = await recoverQueueItemFromEvidence(queue_item_id, evidence, now);

          if (outcome.action === "recovered_delivered") recovered_delivered_count += 1;
          if (outcome.action === "recovered_failed") recovered_failed_count += 1;
          if (outcome.action === "recovered_sent_pending_delivery") recovered_sent_count += 1;
          if (outcome.action === "blocked_manual_review_provider_verification_incomplete") {
            manual_review_count += 1;
          }

          results.push({
            queue_item_id,
            updated_at: getItemUpdatedAt(item),
            evidence_kind: evidence.kind,
            evidence_event_count: evidence_events.length,
            ...outcome,
          });
        } catch (error) {
          warn("queue.reconcile_item_failed", {
            queue_item_id,
            message: error?.message || "Unknown queue reconcile error",
          });

          skipped_count += 1;
          results.push({
            queue_item_id,
            ok: false,
            reason: error?.message || "queue_reconcile_update_failed",
            updated_at: getItemUpdatedAt(item),
          });
        }
      }

      const summary = {
        ok: skipped_count === 0,
        now,
        stale_after_minutes,
        scanned_count: sending_items.length,
        processed_count: stale_items.length,
        recovered_delivered_count,
        recovered_failed_count,
        recovered_sent_count,
        manual_review_count,
        skipped_count,
        results,
        master_owner_id: scoped_master_owner_id,
        provider_verification_available: Boolean(
          provider_capabilities?.message_status_lookup?.supported
        ),
        provider_verification_reason:
          provider_capabilities?.message_status_lookup?.reason ||
          "provider_verification_unavailable",
      };

      if (manual_review_count > 0 || skipped_count > 0) {
        await recordSystemAlert({
          subsystem: "reconcile",
          code: "manual_review_required",
          severity: manual_review_count > 0 ? "high" : "warning",
          retryable: true,
          summary: `Queue reconcile left ${manual_review_count} item(s) in manual review and skipped ${skipped_count} item(s).`,
          dedupe_key: scoped_master_owner_id
            ? `reconcile:${scoped_master_owner_id}`
            : "reconcile",
          affected_ids: results
            .filter((result) => result?.manual_action_required || result?.ok === false)
            .map((result) => result?.queue_item_id),
          metadata: {
            manual_review_count,
            skipped_count,
            processed_count: summary.processed_count,
            provider_verification_available: summary.provider_verification_available,
            provider_verification_reason: summary.provider_verification_reason,
            master_owner_id: scoped_master_owner_id,
          },
        });
      } else {
        await resolveSystemAlert({
          subsystem: "reconcile",
          code: "manual_review_required",
          dedupe_key: scoped_master_owner_id
            ? `reconcile:${scoped_master_owner_id}`
            : "reconcile",
          resolution_message: "Queue reconcile completed without manual review leftovers.",
        });
      }

      info("queue.reconcile_completed", summary);

      return summary;
    },
  });
}

export default runQueueReconcileRunner;
