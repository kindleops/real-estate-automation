// ─── retry-send-queue.js ─────────────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  fetchAllItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getNumberValue,
  updateItem,
} from "@/lib/providers/podio.js";

import { info, warn } from "@/lib/logging/logger.js";

const DEFAULT_RETRY_LIMIT = 50;

const RETRY_POLICIES = {
  "network error": {
    retryable: true,
    schedule_minutes: [15, 60, 240],
  },
  "daily limit hit": {
    retryable: true,
    schedule_minutes: [12 * 60, 24 * 60, 24 * 60],
  },
  "carrier block": {
    retryable: false,
    terminal_reason: "carrier_block_manual_review",
  },
  "invalid number": {
    retryable: false,
    terminal_reason: "invalid_number_terminal",
  },
  "opt-out": {
    retryable: false,
    terminal_reason: "opt_out_terminal",
  },
};

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

function addMinutes(iso, minutes) {
  const base_ts = toTimestamp(iso) ?? Date.now();
  return new Date(base_ts + Math.max(Number(minutes) || 0, 0) * 60_000).toISOString();
}

function isFailedStatus(value) {
  return lower(value) === "failed";
}

export function getRetryPolicy(failed_reason = "") {
  const normalized_reason = lower(failed_reason);
  return (
    RETRY_POLICIES[normalized_reason] || {
      retryable: false,
      terminal_reason: "unsupported_failure_reason",
    }
  );
}

export function getRetryBackoffMinutes({
  failed_reason = "",
  retry_count = 0,
} = {}) {
  const policy = getRetryPolicy(failed_reason);
  if (!policy.retryable) return null;

  const schedule = Array.isArray(policy.schedule_minutes)
    ? policy.schedule_minutes
    : [15];
  const index = Math.max(0, Math.min(schedule.length - 1, Number(retry_count) || 0));

  return Number(schedule[index] || schedule[schedule.length - 1] || 15);
}

function getScheduledRetryAt(item) {
  return (
    getDateValue(item, "scheduled-for-utc", null) ||
    getDateValue(item, "scheduled-for-local", null) ||
    null
  );
}

function sortRetryCandidates(items = []) {
  return [...items].sort((a, b) => {
    const a_scheduled = toTimestamp(getScheduledRetryAt(a));
    const b_scheduled = toTimestamp(getScheduledRetryAt(b));

    if (a_scheduled === null && b_scheduled === null) {
      return Number(a?.item_id || 0) - Number(b?.item_id || 0);
    }

    if (a_scheduled === null) return -1;
    if (b_scheduled === null) return 1;

    return a_scheduled - b_scheduled;
  });
}

export function buildRetryDecision(item, { now = new Date().toISOString() } = {}) {
  const queue_item_id = item?.item_id || null;
  const queue_status = getCategoryValue(item, "queue-status", null);
  const failed_reason = getCategoryValue(item, "failed-reason", null);
  const retry_count = Number(getNumberValue(item, "retry-count", 0) || 0);
  const max_retries = Number(getNumberValue(item, "max-retries", 3) || 3);
  const scheduled_retry_at = getScheduledRetryAt(item);
  const scheduled_retry_ts = toTimestamp(scheduled_retry_at);
  const now_ts = toTimestamp(now) ?? Date.now();
  const policy = getRetryPolicy(failed_reason);

  if (!isFailedStatus(queue_status)) {
    return {
      ok: false,
      queue_item_id,
      action: "skip_not_failed",
      reason: "queue_status_not_failed",
      queue_status,
      failed_reason,
    };
  }

  if (!policy.retryable) {
    return {
      ok: true,
      queue_item_id,
      action: "terminal_non_retryable",
      reason: policy.terminal_reason,
      failed_reason,
      retry_count,
      max_retries,
      update: {
        "queue-status": "Blocked",
        "delivery-confirmed": "❌ Failed",
      },
    };
  }

  if (retry_count >= max_retries) {
    return {
      ok: true,
      queue_item_id,
      action: "terminal_max_retries_exhausted",
      reason: "max_retries_exhausted",
      failed_reason,
      retry_count,
      max_retries,
      update: {
        "queue-status": "Blocked",
        "delivery-confirmed": "❌ Failed",
      },
    };
  }

  if (scheduled_retry_ts !== null && scheduled_retry_ts > now_ts) {
    return {
      ok: false,
      queue_item_id,
      action: "skip_backoff_active",
      reason: "retry_backoff_not_due",
      failed_reason,
      retry_count,
      max_retries,
      scheduled_retry_at,
    };
  }

  if (scheduled_retry_ts !== null && scheduled_retry_ts <= now_ts) {
    return {
      ok: true,
      queue_item_id,
      action: "requeue_now",
      reason: "retry_due",
      failed_reason,
      retry_count,
      max_retries,
      scheduled_retry_at,
      update: {
        "queue-status": "Queued",
        "delivery-confirmed": "⏳ Pending",
        "scheduled-for-utc": { start: now },
        "scheduled-for-local": { start: now },
      },
    };
  }

  const backoff_minutes = getRetryBackoffMinutes({
    failed_reason,
    retry_count,
  });
  const next_retry_at = addMinutes(now, backoff_minutes);

  return {
    ok: true,
    queue_item_id,
    action: "schedule_retry",
    reason: "retry_scheduled",
    failed_reason,
    retry_count,
    max_retries,
    backoff_minutes,
    next_retry_at,
    update: {
      "queue-status": "Failed",
      "delivery-confirmed": "❌ Failed",
      "scheduled-for-utc": { start: next_retry_at },
      "scheduled-for-local": { start: next_retry_at },
    },
  };
}

export async function retrySendQueue({
  limit = DEFAULT_RETRY_LIMIT,
  now = new Date().toISOString(),
  master_owner_id = null,
} = {}) {
  const scoped_master_owner_id = Number(master_owner_id || 0) || null;

  info("queue.retry_started", {
    limit,
    now,
    master_owner_id: scoped_master_owner_id,
  });

  const failed_items = await fetchAllItems(
    APP_IDS.send_queue,
    {
      "queue-status": "Failed",
    },
    {
      page_size: Math.max(limit * 4, 50),
    }
  );

  const ordered_items = sortRetryCandidates(
    failed_items.filter((item) => {
      if (!isFailedStatus(getCategoryValue(item, "queue-status", null))) return false;
      if (!scoped_master_owner_id) return true;

      return (
        Number(getFirstAppReferenceId(item, "master-owner", 0) || 0) ===
        scoped_master_owner_id
      );
    })
  );

  let retried_count = 0;
  let scheduled_count = 0;
  let terminal_count = 0;
  let skipped_count = 0;
  let processed_count = 0;
  const results = [];

  for (const item of ordered_items) {
    if (processed_count >= limit) break;

    const queue_item_id = item?.item_id || null;
    const decision = buildRetryDecision(item, { now });

    if (!decision.ok) {
      skipped_count += 1;
      results.push({
        queue_item_id,
        ok: false,
        action: decision.action,
        reason: decision.reason,
        scheduled_retry_at: decision.scheduled_retry_at || null,
      });
      continue;
    }

    try {
      await updateItem(queue_item_id, decision.update);
      processed_count += 1;

      if (decision.action === "requeue_now") retried_count += 1;
      if (decision.action === "schedule_retry") scheduled_count += 1;
      if (decision.action.startsWith("terminal_")) terminal_count += 1;

      results.push({
        queue_item_id,
        ok: true,
        action: decision.action,
        reason: decision.reason,
        failed_reason: decision.failed_reason,
        retry_count: decision.retry_count,
        max_retries: decision.max_retries,
        backoff_minutes: decision.backoff_minutes || null,
        next_retry_at: decision.next_retry_at || null,
        scheduled_retry_at: decision.scheduled_retry_at || null,
      });
    } catch (err) {
      warn("queue.retry_item_failed", {
        queue_item_id,
        action: decision.action,
        message: err?.message || "Unknown retry error",
      });

      skipped_count += 1;
      results.push({
        queue_item_id,
        ok: false,
        action: decision.action,
        reason: err?.message || "retry_update_failed",
      });
    }
  }

  const summary = {
    ok: true,
    retried_count,
    scheduled_count,
    terminal_count,
    skipped_count,
    processed_count,
    scanned_count: ordered_items.length,
    master_owner_id: scoped_master_owner_id,
    results,
  };

  info("queue.retry_completed", summary);

  return summary;
}

export default retrySendQueue;
