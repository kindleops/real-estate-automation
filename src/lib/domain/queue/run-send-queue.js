import APP_IDS from "@/lib/config/app-ids.js";

import {
  buildPodioCooldownSkipResult,
  filterAppItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getNumberValue,
  isPodioRateLimitError,
  PodioError,
} from "@/lib/providers/podio.js";

import {
  failQueueItem,
  processSendQueueItem,
} from "@/lib/domain/queue/process-send-queue.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";
import { info, warn } from "@/lib/logging/logger.js";

const DEFAULT_BATCH_SIZE = 50;
const QUEUE_RUN_SCAN_CAP_MULTIPLIER = 4;
const QUEUE_RUN_PAGE_SIZE_CAP = 100;
const QUEUE_RUN_SCAN_CAP_MAX = 250;
const BLOCKED_REASONS = Object.freeze(
  new Set([
    "message_resolution_failed",
    "junk_message_body",
    "phone_not_active",
    "destination_number_invalid",
    "outbound_number_item_missing",
    "outbound_number_phone_invalid",
    "phone_post_contact_suppression",
    "classification_stop_texting",
  ])
);

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

function extractItems(response) {
  if (Array.isArray(response)) return response;
  return Array.isArray(response?.items) ? response.items : [];
}

function summarizeProblemReason(value) {
  const normalized = clean(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "unknown";
}

function isBlockedDisposition(result = {}) {
  const queue_status = lower(result?.queue_status);
  if (queue_status === "blocked") return true;

  const reason = summarizeProblemReason(result?.reason);
  if (BLOCKED_REASONS.has(reason)) return true;
  if (reason.startsWith("outbound_number_inactive")) return true;
  if (reason.startsWith("outbound_number_paused_until")) return true;
  if (reason.startsWith("outbound_number_hard_paused")) return true;

  return false;
}

function classifyQueueRunItemResult(result = {}) {
  if (result?.skipped) {
    return {
      disposition: "skipped",
      reason: summarizeProblemReason(result.reason || "skipped"),
    };
  }

  if (result?.sent) {
    return {
      disposition: "sent",
      reason: summarizeProblemReason(result.reason || "sent"),
    };
  }

  if (isBlockedDisposition(result)) {
    return {
      disposition: "blocked",
      reason: summarizeProblemReason(result.reason || "blocked"),
    };
  }

  if (result?.ok) {
    return {
      disposition: "sent",
      reason: summarizeProblemReason(result.reason || "ok"),
    };
  }

  return {
    disposition: "failed",
    reason: summarizeProblemReason(result.reason || "failed"),
  };
}

function wasQueueItemClaimed(result = {}, classification = null) {
  if (typeof result?.claimed === "boolean") {
    return result.claimed;
  }

  const resolved_classification =
    classification || classifyQueueRunItemResult(result);

  if (resolved_classification.disposition === "skipped") {
    return false;
  }

  if (result?.sent) {
    return true;
  }

  if (clean(result?.provider_message_id) || clean(result?.message_id)) {
    return true;
  }

  return false;
}

async function loadQueuedItemsWindow({
  limit,
  filter_app_items,
}) {
  const page_size = Math.min(
    Math.max(Number(limit || 0) * 2, DEFAULT_BATCH_SIZE),
    QUEUE_RUN_PAGE_SIZE_CAP
  );
  const scan_cap = Math.min(
    Math.max(Number(limit || 0) * QUEUE_RUN_SCAN_CAP_MULTIPLIER, page_size),
    QUEUE_RUN_SCAN_CAP_MAX
  );
  const items = [];
  let offset = 0;
  let page_count = 0;

  while (items.length < scan_cap) {
    const response = await filter_app_items(
      APP_IDS.send_queue,
      {
        "queue-status": "Queued",
      },
      {
        limit: Math.min(page_size, scan_cap - items.length),
        offset,
        sort_by: "scheduled-for-utc",
        sort_desc: false,
        cache_ttl_ms: 15_000,
      }
    );
    const page_items = extractItems(response);
    page_count += 1;

    if (!page_items.length) break;

    items.push(...page_items);

    if (page_items.length < page_size) break;
    offset += page_items.length;
  }

  return {
    items,
    page_size,
    page_count,
    scan_cap,
  };
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
  const fetch_all_items = deps.fetchAllItems || null;
  const filter_app_items = deps.filterAppItems || filterAppItems;
  const process_send_queue_item =
    deps.processSendQueueItem || processSendQueueItem;
  const fail_queue_item = deps.failQueueItem || failQueueItem;
  const record_system_alert = deps.recordSystemAlert || recordSystemAlert;
  const resolve_system_alert = deps.resolveSystemAlert || resolveSystemAlert;
  const with_run_lock = deps.withRunLock || withRunLock;
  const build_cooldown_skip_result =
    deps.buildPodioCooldownSkipResult || buildPodioCooldownSkipResult;
  const info_log = deps.info || info;
  const warn_log = deps.warn || warn;
  const run_started_at = now || nowIso();
  const batch_started_ms = Date.now();
  const now_ts = toTimestamp(run_started_at) ?? Date.now();
  const now_date = new Date(run_started_at);
  const scoped_master_owner_id = Number(master_owner_id || 0) || null;

  const cooldown_skip = await build_cooldown_skip_result({
    dry_run,
    run_started_at,
    limit,
    master_owner_id: scoped_master_owner_id,
    total_rows_loaded: 0,
    queued_rows_loaded: 0,
    due_rows: 0,
    future_rows: 0,
    outside_window_rows: 0,
    attempted_count: 0,
    claimed_count: 0,
    started_count: 0,
    processed_count: 0,
    sent_count: 0,
    failed_count: 0,
    blocked_count: 0,
    skipped_count: 0,
    duplicate_locked_count: 0,
    first_failure_queue_item_id: null,
    first_failure_reason: null,
    batch_duration_ms: 0,
    results: [],
  });

  if (cooldown_skip?.podio_cooldown?.active) {
    warn_log("queue.run_skipped_podio_cooldown", {
      run_started_at,
      limit,
      dry_run,
      master_owner_id: scoped_master_owner_id,
      retry_after_seconds: cooldown_skip.retry_after_seconds,
      retry_after_at: cooldown_skip.retry_after_at,
      podio_status: cooldown_skip.podio_cooldown?.status ?? null,
      podio_path: cooldown_skip.podio_cooldown?.path ?? null,
      podio_operation: cooldown_skip.podio_cooldown?.operation ?? null,
      rate_limit_remaining:
        cooldown_skip.podio_cooldown?.rate_limit_remaining ?? null,
      rate_limit_limit:
        cooldown_skip.podio_cooldown?.rate_limit_limit ?? null,
    });

    return cooldown_skip;
  }

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
      page_size: fetch_all_items
        ? Math.max(limit, 50)
        : Math.min(Math.max(limit * 2, DEFAULT_BATCH_SIZE), QUEUE_RUN_PAGE_SIZE_CAP),
      sort_by: "scheduled-for-utc",
      run_started_at,
      loader: fetch_all_items ? "fetch_all_items" : "paged_filter_window",
    });

    const queue_window = fetch_all_items
      ? {
          items: await fetch_all_items(
            APP_IDS.send_queue,
            {
              "queue-status": "Queued",
            },
            {
              page_size: Math.max(limit, 50),
              sort_by: "scheduled-for-utc",
              sort_desc: false,
            }
          ),
          page_size: Math.max(limit, 50),
          page_count: null,
          scan_cap: null,
        }
      : await loadQueuedItemsWindow({
          limit,
          filter_app_items,
        });

    const queued_items = queue_window.items;

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

    const sorted_candidates = sortByScheduledAtAsc(all_due_runnable).slice(0, limit);

    // Within-batch duplicate guard: if the same (master_owner_id, phone_item_id,
    // touch_number) appears more than once in the batch, only the earliest-
    // scheduled item is processed.  Duplicates are skipped with a warning so
    // the operator can investigate why multiple queue rows exist for the same
    // send target and touch sequence.
    const seen_send_keys = new Set();
    const batch_duplicate_details = [];

    const runnable_items = sorted_candidates.filter((item) => {
      const owner_id = getFirstAppReferenceId(item, "master-owner", null);
      const phone_id = getFirstAppReferenceId(item, "phone-number", null);
      const touch_num = getNumberValue(item, "touch-number", null);

      if (owner_id && phone_id && touch_num !== null) {
        const dedup_key = `${owner_id}:${phone_id}:${touch_num}`;
        if (seen_send_keys.has(dedup_key)) {
          batch_duplicate_details.push({
            item_id: item?.item_id,
            owner_id,
            phone_id,
            touch_num,
            reason: "duplicate_owner_phone_touch_in_batch",
          });
          return false;
        }
        seen_send_keys.add(dedup_key);
      }
      return true;
    });

    if (batch_duplicate_details.length > 0) {
      warn_log("queue.run_batch_duplicates_suppressed", {
        duplicate_count: batch_duplicate_details.length,
        first_5_duplicates: batch_duplicate_details.slice(0, 5),
        run_started_at,
      });
    }

    info_log("queue.run_candidates_loaded", {
      now_utc: run_started_at,
      total_rows_loaded: queued_items.length,
      queued_rows_loaded: queued_items.length,
      due_rows: all_due_runnable.length,
      future_rows: future_rows_count,
      outside_window_rows: outside_window_count,
      runnable_count: runnable_items.length,
      queue_pages_loaded: queue_window.page_count,
      queue_scan_cap: queue_window.scan_cap,
      master_owner_id: scoped_master_owner_id,
      first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
      first_10_filter_excluded: filter_diagnostics,
    });

    if (dry_run) {
      const summary = {
        ok: true,
        dry_run: true,
        run_started_at,
        total_rows_loaded: queued_items.length,
        queued_rows_loaded: queued_items.length,
        due_rows: all_due_runnable.length,
        future_rows: future_rows_count,
        outside_window_rows: outside_window_count,
        first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
        first_10_excluded: filter_diagnostics,
        attempted_count: runnable_items.length,
        claimed_count: 0,
        started_count: runnable_items.length,
        processed_count: runnable_items.length,
        sent_count: 0,
        failed_count: 0,
        blocked_count: 0,
        skipped_count: 0,
        duplicate_locked_count: 0,
        first_failing_queue_item_id: null,
        first_failing_reason: null,
        first_failure_queue_item_id: null,
        first_failure_reason: null,
        batch_duration_ms: Date.now() - batch_started_ms,
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
        attempted_count: summary.attempted_count,
        claimed_count: summary.claimed_count,
        started_count: summary.started_count,
        processed_count: summary.processed_count,
        sent_rows: 0,
        sent_count: 0,
        blocked_rows: 0,
        blocked_count: 0,
        failed_count: 0,
        skipped_rows: 0,
        skipped_count: 0,
        duplicate_locked_count: 0,
        batch_duration_ms: summary.batch_duration_ms,
        dry_run: true,
        master_owner_id: scoped_master_owner_id,
        first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
        first_10_skipped_item_ids_with_reason: [],
      });

      return summary;
    }

    const results = [];
    let attempted_count = 0;
    let claimed_count = 0;
    let started_count = 0;
    let processed_count = 0;
    let sent_count = 0;
    let failed_count = 0;
    let skipped_count = 0;
    let blocked_count = 0;
    let duplicate_locked_count = 0;
    const skipped_details = [];
    let first_failing_queue_item_id = null;
    let first_failing_reason = null;

    for (const item of runnable_items) {
      const queue_item_id = item?.item_id;
      attempted_count += 1;
      started_count += 1;

      info_log("queue.run_item_started", {
        queue_item_id,
        run_started_at,
        master_owner_id: scoped_master_owner_id,
      });

      try {
        const result = await process_send_queue_item(queue_item_id);
        processed_count += 1;
        const classification = classifyQueueRunItemResult(result);
        if (wasQueueItemClaimed(result, classification)) {
          claimed_count += 1;
        }

        results.push({
          queue_item_id,
          ...result,
        });

        if (classification.disposition === "skipped") {
          skipped_count += 1;
          if (result?.claim_conflict || classification.reason === "queue_item_claim_conflict") {
            duplicate_locked_count += 1;
          }
          if (skipped_details.length < 10) {
            skipped_details.push({
              queue_item_id,
              reason: classification.reason,
            });
          }
        } else if (classification.disposition === "sent") {
          sent_count += 1;
        } else {
          if (classification.disposition === "blocked") {
            blocked_count += 1;
          } else {
            failed_count += 1;
          }

          if (!first_failing_queue_item_id) {
            first_failing_queue_item_id = queue_item_id || null;
            first_failing_reason = classification.reason;
          }

          warn_log("queue.run_item_failed_soft", {
            queue_item_id,
            reason: classification.reason,
            queue_status: result?.queue_status || null,
            failed_reason: result?.failed_reason || null,
            details: result.details || null,
          });
          if (skipped_details.length < 10) {
            skipped_details.push({
              queue_item_id,
              reason: classification.reason,
            });
          }
        }

        info_log("queue.run_item_completed", {
          queue_item_id,
          disposition: classification.disposition,
          reason: classification.reason,
          queue_status: result?.queue_status || null,
          failed_reason: result?.failed_reason || null,
          sent: Boolean(result?.sent),
          skipped: Boolean(result?.skipped),
        });
      } catch (err) {
        if (isPodioRateLimitError(err)) {
          warn_log("queue.run_rate_limit_abort", {
            queue_item_id,
            message: err?.message || "Podio cooldown active",
          });
          throw err;
        }

        if (isRevisionLimitExceededError(err)) {
          warn_log("queue.run_item_skipped_revision_limit", {
            queue_item_id,
            failure_bucket: "revision_limit_exceeded",
            message: err?.message || "Unknown queue processing error",
          });

          processed_count += 1;
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

        const machine_reason = "queue_processing_exception";
        const error_message = err?.message || "Unknown queue processing error";
        let crash_mark_applied = false;

        try {
          await fail_queue_item(queue_item_id, {
            queue_status: "Failed",
            failed_reason: "Network Error",
            _phase: "queue_run_item_crashed",
          });
          crash_mark_applied = true;
        } catch (mark_error) {
          warn_log("queue.run_item_crash_mark_failed", {
            queue_item_id,
            reason: machine_reason,
            message: mark_error?.message || "Unknown queue crash write failure",
          });
        }

        warn_log("queue.run_item_crashed", {
          queue_item_id,
          reason: machine_reason,
          message: error_message,
          crash_mark_applied,
        });

        failed_count += 1;
        processed_count += 1;
        if (!first_failing_queue_item_id) {
          first_failing_queue_item_id = queue_item_id || null;
          first_failing_reason = machine_reason;
        }
        if (skipped_details.length < 10) {
          skipped_details.push({ queue_item_id, reason: machine_reason });
        }
        results.push({
          queue_item_id,
          ok: false,
          sent: false,
          queue_status: "Failed",
          failed_reason: "Network Error",
          reason: machine_reason,
          error_message,
          crash_mark_applied,
        });
      }
    }

    const partial_failure = failed_count > 0 || blocked_count > 0;
    const summary = {
      ok: true,
      dry_run: false,
      partial: partial_failure,
      run_started_at,
      total_rows_loaded: queued_items.length,
      queued_rows_loaded: queued_items.length,
      due_rows: all_due_runnable.length,
      future_rows: future_rows_count,
      outside_window_rows: outside_window_count,
      first_10_candidate_item_ids: runnable_items.slice(0, 10).map((i) => i?.item_id),
      first_10_excluded: filter_diagnostics,
      attempted_count,
      claimed_count,
      started_count,
      processed_count,
      sent_count,
      failed_count,
      blocked_count,
      skipped_count,
      duplicate_locked_count,
      first_failing_queue_item_id,
      first_failing_reason,
      first_failure_queue_item_id: first_failing_queue_item_id,
      first_failure_reason: first_failing_reason,
      batch_duration_ms: Date.now() - batch_started_ms,
      master_owner_id: scoped_master_owner_id,
      results,
    };

    if (partial_failure) {
      try {
        await record_system_alert({
          subsystem: "queue",
          code: "runner_failed_items",
          severity: "warning",
          retryable: true,
          summary: `Queue runner completed with ${failed_count} failed item(s) and ${blocked_count} blocked item(s).`,
          dedupe_key: scoped_master_owner_id
            ? `queue-run:${scoped_master_owner_id}`
            : "queue-run",
          affected_ids: results
            .filter(
              (result) =>
                result?.ok === false ||
                lower(result?.queue_status) === "blocked"
            )
            .map((result) => result?.queue_item_id),
          metadata: {
            run_started_at,
            attempted_count,
            claimed_count,
            started_count: summary.started_count,
            processed_count: summary.processed_count,
            failed_count,
            blocked_count,
            sent_count,
            skipped_count,
            duplicate_locked_count,
            first_failing_queue_item_id,
            first_failing_reason,
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
      attempted_count: summary.attempted_count,
      claimed_count: summary.claimed_count,
      started_count: summary.started_count,
      processed_count: summary.processed_count,
      sent_rows: sent_count,
      sent_count,
      blocked_rows: blocked_count,
      blocked_count,
      failed_count,
      skipped_rows: skipped_count,
      skipped_count,
      duplicate_locked_count,
      first_failing_queue_item_id,
      first_failing_reason,
      first_failure_queue_item_id: first_failing_queue_item_id,
      first_failure_reason: first_failing_reason,
      batch_duration_ms: summary.batch_duration_ms,
      partial: partial_failure,
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
