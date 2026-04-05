import APP_IDS from "@/lib/config/app-ids.js";

import {
  fetchAllItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  PodioError,
} from "@/lib/providers/podio.js";

import { processSendQueueItem } from "@/lib/domain/queue/process-send-queue.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";
import { info, warn } from "@/lib/logging/logger.js";

const DEFAULT_BATCH_SIZE = 50;

function nowIso() {
  return new Date().toISOString();
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function isRevisionLimitExceededError(error) {
  if (!(error instanceof PodioError)) return false;
  return lower(error?.message).includes(
    "this item has exceeded the maximum number of revisions"
  );
}

async function recordRevisionLimitAlert(
  record_alert,
  queue_item_id,
  {
    run_started_at,
    master_owner_id = null,
    error = null,
  } = {}
) {
  try {
    await record_alert({
      subsystem: "queue",
      code: "revision_limit_exceeded",
      severity: "warning",
      retryable: false,
      summary: `Queue item ${queue_item_id} hit the Podio revision limit and was skipped for manual review.`,
      dedupe_key: `queue-revision-limit:${queue_item_id}`,
      affected_ids: [queue_item_id],
      metadata: {
        queue_item_id,
        failure_bucket: "revision_limit_exceeded",
        run_started_at,
        master_owner_id,
        podio_error_message: error?.message || null,
        recovery: "manual_review_required",
      },
    });
  } catch (alert_error) {
    warn("queue.run_revision_limit_alert_failed", {
      queue_item_id,
      failure_bucket: "revision_limit_exceeded",
      message: alert_error?.message || "Unknown alert recording error",
    });
  }
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function isDue(queue_item, now_ts) {
  const scheduled_utc =
    getDateValue(queue_item, "scheduled-for-utc", null) ||
    getDateValue(queue_item, "scheduled-for-local", null);

  if (!scheduled_utc) return true;

  const scheduled_ts = toTimestamp(scheduled_utc);
  if (scheduled_ts === null) return false;

  return scheduled_ts <= now_ts;
}

function sortByScheduledAtAsc(items) {
  return [...items].sort((a, b) => {
    const a_date =
      getDateValue(a, "scheduled-for-utc", null) ||
      getDateValue(a, "scheduled-for-local", null) ||
      "9999-12-31T23:59:59.999Z";

    const b_date =
      getDateValue(b, "scheduled-for-utc", null) ||
      getDateValue(b, "scheduled-for-local", null) ||
      "9999-12-31T23:59:59.999Z";

    const a_ts = toTimestamp(a_date) ?? Number.MAX_SAFE_INTEGER;
    const b_ts = toTimestamp(b_date) ?? Number.MAX_SAFE_INTEGER;

    return a_ts - b_ts;
  });
}

function isRunnableStatus(status) {
  const normalized = lower(status);
  return normalized === "queued";
}

function mapTimezoneToIana(value) {
  const raw = lower(value);

  if (raw === "eastern" || raw === "et" || raw === "est" || raw === "edt") {
    return "America/New_York";
  }

  if (raw === "central" || raw === "ct" || raw === "cst" || raw === "cdt") {
    return "America/Chicago";
  }

  if (raw === "mountain" || raw === "mt" || raw === "mst" || raw === "mdt") {
    return "America/Denver";
  }

  if (raw === "pacific" || raw === "pt" || raw === "pst" || raw === "pdt") {
    return "America/Los_Angeles";
  }

  if (raw === "alaska") {
    return "America/Anchorage";
  }

  if (raw === "hawaii") {
    return "Pacific/Honolulu";
  }

  return "America/Chicago";
}

function getLocalParts(date, timezone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });

  const parts = formatter.formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type)?.value || "00";

  const hour = Number(get("hour"));
  const minute = Number(get("minute"));

  return {
    hour,
    minute,
    minutes_since_midnight: hour * 60 + minute,
  };
}

function parseTimeToken(token) {
  const raw = clean(token).toUpperCase();
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)$/);

  if (!match) return null;

  let hour = Number(match[1]);
  const minute = Number(match[2] || "0");
  const meridiem = match[3];

  if (hour === 12) hour = 0;
  if (meridiem === "PM") hour += 12;

  return hour * 60 + minute;
}

function parseContactWindow(window_value) {
  const raw = clean(window_value);
  if (!raw) return null;

  const normalized = raw.toUpperCase();

  const range_match = normalized.match(
    /(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*-\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))/
  );

  if (!range_match) return null;

  const start = parseTimeToken(range_match[1]);
  const end = parseTimeToken(range_match[2]);

  if (start === null || end === null) return null;

  return { start, end };
}

function isWithinContactWindow(queue_item, now_date) {
  const timezone_label = getCategoryValue(queue_item, "timezone", "Central");
  const contact_window = getCategoryValue(queue_item, "contact-window", null);

  if (!contact_window) {
    return {
      allowed: true,
      timezone_label,
      contact_window: null,
      reason: "no_contact_window",
    };
  }

  const parsed_window = parseContactWindow(contact_window);
  if (!parsed_window) {
    return {
      allowed: true,
      timezone_label,
      contact_window,
      reason: "unparseable_contact_window",
    };
  }

  const timezone = mapTimezoneToIana(timezone_label);
  const local = getLocalParts(now_date, timezone);

  const { start, end } = parsed_window;
  const current = local.minutes_since_midnight;

  const allowed =
    end >= start
      ? current >= start && current <= end
      : current >= start || current <= end;

  return {
    allowed,
    timezone_label,
    timezone,
    contact_window,
    local_minutes_since_midnight: current,
    reason: allowed ? "inside_window" : "outside_window",
  };
}

export async function runSendQueue({
  limit = DEFAULT_BATCH_SIZE,
  now = null,
  dry_run = false,
  master_owner_id = null,
} = {}, deps = {}) {
  const fetch_all_items = deps.fetchAllItems || fetchAllItems;
  const process_send_queue_item =
    deps.processSendQueueItem || processSendQueueItem;
  const record_system_alert = deps.recordSystemAlert || recordSystemAlert;
  const resolve_system_alert = deps.resolveSystemAlert || resolveSystemAlert;
  const with_run_lock = deps.withRunLock || withRunLock;
  const info_log = deps.info || info;
  const warn_log = deps.warn || warn;
  const run_started_at = now || nowIso();
  const now_ts = toTimestamp(run_started_at) ?? Date.now();
  const now_date = new Date(run_started_at);
  const scoped_master_owner_id = Number(master_owner_id || 0) || null;

  async function executeRun() {
    info_log("queue.run_started", {
      limit,
      run_started_at,
      dry_run,
      master_owner_id: scoped_master_owner_id,
    });

    info_log("queue.run_fetch_started", {
      send_queue_app_id: APP_IDS.send_queue,
      filter: { "queue-status": "Queued" },
      page_size: Math.max(limit, 50),
      sort_by: "scheduled-for-utc",
      run_started_at,
    });

    const queued_items = await fetch_all_items(
      APP_IDS.send_queue,
      {
        "queue-status": "Queued",
      },
      {
        page_size: Math.max(limit, 50),
        sort_by: "scheduled-for-utc",
        sort_desc: false,
      }
    );

    let future_rows_count = 0;
    let outside_window_count = 0;
    const filter_diagnostics = [];

    const all_due_runnable = queued_items.filter((item) => {
      const item_id = item?.item_id;
      const status = getCategoryValue(item, "queue-status", null);

      if (!isRunnableStatus(status)) {
        if (filter_diagnostics.length < 10) {
          filter_diagnostics.push({ item_id, reason: "bad_status", status });
        }
        return false;
      }

      if (
        scoped_master_owner_id &&
        Number(getFirstAppReferenceId(item, "master-owner", 0) || 0) !==
          scoped_master_owner_id
      ) {
        if (filter_diagnostics.length < 10) {
          filter_diagnostics.push({ item_id, reason: "scoped_mismatch" });
        }
        return false;
      }

      if (!isDue(item, now_ts)) {
        future_rows_count += 1;
        const scheduled =
          getDateValue(item, "scheduled-for-utc", null) ||
          getDateValue(item, "scheduled-for-local", null);
        if (filter_diagnostics.length < 10) {
          filter_diagnostics.push({ item_id, reason: "not_due_yet", scheduled });
        }
        return false;
      }

      const contact_window_check = isWithinContactWindow(item, now_date);
      if (!contact_window_check.allowed) {
        outside_window_count += 1;
        if (filter_diagnostics.length < 10) {
          filter_diagnostics.push({
            item_id,
            reason: "outside_contact_window",
            timezone: contact_window_check.timezone_label,
            contact_window: contact_window_check.contact_window,
            local_minutes_since_midnight: contact_window_check.local_minutes_since_midnight,
          });
        }
        return false;
      }

      return true;
    });

    const runnable_items = sortByScheduledAtAsc(all_due_runnable).slice(0, limit);

    info_log("queue.run_candidates_loaded", {
      now_utc: run_started_at,
      total_rows_loaded: queued_items.length,
      queued_rows_loaded: queued_items.length,
      due_rows: all_due_runnable.length,
      future_rows: future_rows_count,
      outside_window_rows: outside_window_count,
      runnable_count: runnable_items.length,
      master_owner_id: scoped_master_owner_id,
      first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
      first_10_filter_excluded: filter_diagnostics,
    });

    if (dry_run) {
      const summary = {
        ok: true,
        dry_run: true,
        run_started_at,
        processed_count: runnable_items.length,
        sent_count: 0,
        failed_count: 0,
        skipped_count: 0,
        master_owner_id: scoped_master_owner_id,
        results: runnable_items.map((item) => ({
          queue_item_id: item?.item_id || null,
          ok: true,
          dry_run: true,
          action: "would_process",
        })),
      };

      info_log("queue.run_completed", {
        now_utc: run_started_at,
        run_started_at,
        total_rows_loaded: queued_items.length,
        queued_rows_loaded: queued_items.length,
        due_rows: all_due_runnable.length,
        future_rows: future_rows_count,
        outside_window_rows: outside_window_count,
        runnable_count: runnable_items.length,
        processed_count: summary.processed_count,
        sent_rows: 0,
        sent_count: 0,
        blocked_rows: 0,
        failed_count: 0,
        skipped_rows: 0,
        skipped_count: 0,
        dry_run: true,
        master_owner_id: scoped_master_owner_id,
        first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
        first_10_skipped_item_ids_with_reason: [],
      });

      return summary;
    }

    const results = [];
    let sent_count = 0;
    let failed_count = 0;
    let skipped_count = 0;
    let blocked_count = 0;
    const skipped_details = [];

    for (const item of runnable_items) {
      const queue_item_id = item?.item_id;

      try {
        const result = await process_send_queue_item(queue_item_id);

        results.push({
          queue_item_id,
          ...result,
        });

        if (result?.skipped) {
          skipped_count += 1;
          if (skipped_details.length < 10) {
            skipped_details.push({ queue_item_id, reason: result.reason || "skipped" });
          }
        } else if (result?.ok) {
          sent_count += 1;
        } else {
          failed_count += 1;
          blocked_count += 1;
          warn_log("queue.run_item_not_dispatched", {
            queue_item_id,
            reason: result.reason || "unknown",
            details: result.details || null,
          });
          if (skipped_details.length < 10) {
            skipped_details.push({ queue_item_id, reason: result.reason || "unknown" });
          }
        }
      } catch (err) {
        if (isRevisionLimitExceededError(err)) {
          warn_log("queue.run_item_skipped_revision_limit", {
            queue_item_id,
            failure_bucket: "revision_limit_exceeded",
            message: err?.message || "Unknown queue processing error",
          });

          skipped_count += 1;
          if (skipped_details.length < 10) {
            skipped_details.push({ queue_item_id, reason: "revision_limit_exceeded" });
          }
          results.push({
            queue_item_id,
            ok: true,
            skipped: true,
            reason: "queue_item_revision_limit_exceeded",
            failure_bucket: "revision_limit_exceeded",
            manual_review_required: true,
          });

          await recordRevisionLimitAlert(record_system_alert, queue_item_id, {
            run_started_at,
            master_owner_id: scoped_master_owner_id,
            error: err,
          });
          continue;
        }

        warn_log("queue.run_item_crashed", {
          queue_item_id,
          message: err?.message || "Unknown queue processing error",
        });

        failed_count += 1;
        if (skipped_details.length < 10) {
          skipped_details.push({ queue_item_id, reason: `crash:${err?.message || "unknown"}` });
        }
        results.push({
          queue_item_id,
          ok: false,
          reason: err?.message || "queue_processing_crash",
        });
      }
    }

    const summary = {
      ok: failed_count === 0,
      dry_run: false,
      run_started_at,
      processed_count: runnable_items.length,
      sent_count,
      failed_count,
      skipped_count,
      master_owner_id: scoped_master_owner_id,
      results,
    };

    if (failed_count > 0) {
      try {
        await record_system_alert({
          subsystem: "queue",
          code: "runner_failed_items",
          severity: "warning",
          retryable: true,
          summary: `Queue runner completed with ${failed_count} failed item(s).`,
          dedupe_key: scoped_master_owner_id
            ? `queue-run:${scoped_master_owner_id}`
            : "queue-run",
          affected_ids: results
            .filter((result) => result?.ok === false)
            .map((result) => result?.queue_item_id),
          metadata: {
            run_started_at,
            processed_count: summary.processed_count,
            failed_count,
            sent_count,
            skipped_count,
            master_owner_id: scoped_master_owner_id,
          },
        });
      } catch (alert_err) {
        warn_log("queue.run_system_alert_write_failed", {
          run_started_at,
          operation: "record_failed_items_alert",
          failure_bucket: isRevisionLimitExceededError(alert_err)
            ? "revision_limit_exceeded"
            : "write_error",
          message: alert_err?.message || null,
        });
      }
    } else {
      try {
        await resolve_system_alert({
          subsystem: "queue",
          code: "runner_failed_items",
          dedupe_key: scoped_master_owner_id
            ? `queue-run:${scoped_master_owner_id}`
            : "queue-run",
          resolution_message: "Queue runner completed without failed items.",
        });
      } catch (alert_err) {
        warn_log("queue.run_system_alert_write_failed", {
          run_started_at,
          operation: "resolve_failed_items_alert",
          failure_bucket: isRevisionLimitExceededError(alert_err)
            ? "revision_limit_exceeded"
            : "write_error",
          message: alert_err?.message || null,
        });
      }
    }

    info_log("queue.run_completed", {
      now_utc: run_started_at,
      run_started_at,
      total_rows_loaded: queued_items.length,
      queued_rows_loaded: queued_items.length,
      due_rows: all_due_runnable.length,
      future_rows: future_rows_count,
      outside_window_rows: outside_window_count,
      runnable_count: runnable_items.length,
      processed_count: summary.processed_count,
      sent_rows: sent_count,
      sent_count,
      blocked_rows: blocked_count,
      failed_count,
      skipped_rows: skipped_count,
      skipped_count,
      master_owner_id: scoped_master_owner_id,
      first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
      first_10_skipped_item_ids_with_reason: skipped_details,
    });

    return summary;
  }

  return with_run_lock({
    scope: scoped_master_owner_id
      ? `queue-run:${scoped_master_owner_id}`
      : "queue-run",
    enabled: !dry_run,
    lease_ms: 10 * 60_000,
    owner: "queue_runner",
    metadata: {
      limit,
      master_owner_id: scoped_master_owner_id,
    },
    onLocked: async (lock) => {
      warn_log("queue.run_skipped_lock_active", {
        reason: "queue_runner_lock_active",
        lock_scope: lock?.scope || null,
        lock_record_item_id: lock?.record_item_id || null,
        lock_lease_token: lock?.meta?.lease_token || null,
        lock_expires_at: lock?.meta?.expires_at || null,
        lock_owner: lock?.meta?.owner || null,
        lock_acquired_at: lock?.meta?.acquired_at || null,
        lock_acquisition_count: lock?.meta?.acquisition_count ?? null,
        run_started_at,
        recovery_hint: "If this persists beyond the expires_at time, the lock is stuck. Call forceReleaseStaleLock({ scope }) to recover.",
      });

      try {
        await record_system_alert({
          subsystem: "queue",
          code: "runner_overlap",
          severity: "warning",
          retryable: true,
          summary: "Queue runner skipped because an active lease is already in progress.",
          dedupe_key: scoped_master_owner_id
            ? `queue-run:${scoped_master_owner_id}`
            : "queue-run",
          metadata: {
            run_started_at,
            limit,
            master_owner_id: scoped_master_owner_id,
            lock,
          },
        });
      } catch (alert_err) {
        warn_log("queue.run_overlap_alert_write_failed", {
          run_started_at,
          failure_bucket: isRevisionLimitExceededError(alert_err)
            ? "revision_limit_exceeded"
            : "write_error",
          message: alert_err?.message || null,
        });
      }

      return {
        ok: true,
        dry_run: false,
        skipped: true,
        reason: "queue_runner_lock_active",
        run_started_at,
        limit,
        master_owner_id: scoped_master_owner_id,
        lock,
      };
    },
    fn: executeRun,
  });
}

export default runSendQueue;
