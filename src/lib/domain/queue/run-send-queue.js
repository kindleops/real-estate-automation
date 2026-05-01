import crypto from "node:crypto";

import {
  failQueueItem,
  processSendQueueItem,
} from "@/lib/domain/queue/process-send-queue.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";
import { isRevisionLimitExceeded } from "@/lib/providers/podio.js";
import { info, warn } from "@/lib/logging/logger.js";
import { isManualInboxSend } from "@/lib/domain/queue/is-manual-inbox-send.js";
import { hasSupabaseConfig } from "@/lib/supabase/client.js";
import {
  claimSendQueueRow,
  evaluateContactWindow,
  finalizeClaimedSendQueueRows,
  loadRunnableSendQueueRows,
  normalizeQueueRowId,
  normalizeSendQueueRow,
  pauseInvalidQueueRow,
  pauseMaxRetriesQueueRow,
  pauseNameMissingQueueRow,
  resolveQueueSellerFirstName,
} from "@/lib/supabase/sms-engine.js";
import { captureSystemEvent } from "@/lib/analytics/posthog-server.js";
import { sendCriticalAlert } from "@/lib/alerts/discord.js";
import { getSystemFlag, buildDisabledResponse, SystemControlDisabledError } from "@/lib/system-control.js";

const DEFAULT_BATCH_SIZE = 50;
const QUEUE_RUN_LOCK_SCOPE = "queue-run";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function asPositiveInteger(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeRows(data) {
  return Array.isArray(data) ? data : [];
}

function getPlainValue(raw) {
  if (raw === null || raw === undefined) return null;
  if (typeof raw !== "object") return raw;
  if ("value" in raw) {
    const value = raw.value;
    if (value && typeof value === "object" && "text" in value) return value.text;
    if (value && typeof value === "object" && "item_id" in value) return value.item_id;
    return value;
  }
  if ("start" in raw) return raw.start;
  return raw;
}

function getRecordFieldValues(record = null, key = "") {
  if (!record || typeof record !== "object") return [];

  if (Object.prototype.hasOwnProperty.call(record, key)) {
    const direct = record[key];
    return Array.isArray(direct) ? direct : [direct];
  }

  const fields = Array.isArray(record.fields) ? record.fields : [];
  const matched = fields.find((field) => clean(field?.external_id) === clean(key));
  if (!matched) return [];
  return Array.isArray(matched.values) ? matched.values : [];
}

function getCategoryLike(record = null, key = "", fallback = null) {
  const values = getRecordFieldValues(record, key);
  if (!values.length) return fallback;
  const first = values[0];
  if (first?.value?.text) return clean(first.value.text) || fallback;
  return clean(getPlainValue(first)) || fallback;
}

function getDateLike(record = null, key = "", fallback = null) {
  const values = getRecordFieldValues(record, key);
  if (!values.length) return fallback;
  return clean(values[0]?.start ?? getPlainValue(values[0])) || fallback;
}

function getNumberLike(record = null, key = "", fallback = null) {
  const values = getRecordFieldValues(record, key);
  if (!values.length) return fallback;
  const parsed = Number(getPlainValue(values[0]));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getAppRefLike(record = null, key = "", fallback = null) {
  const values = getRecordFieldValues(record, key);
  if (!values.length) return fallback;
  return asPositiveInteger(getPlainValue(values[0]), fallback);
}

function toTimestamp(value) {
  if (!value) return null;
  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? null : parsed;
}

function getQueueRowId(row = null) {
  return normalizeQueueRowId(
    row?.queue_row_id ?? row?.id ?? row?.queue_item_id ?? row?.item_id,
    null
  );
}

function getQueueStatus(row = null) {
  return clean(row?.queue_status || getCategoryLike(row, "queue-status", ""));
}

function getScheduledAt(row = null) {
  return (
    clean(row?.scheduled_for) ||
    clean(row?.scheduled_for_utc) ||
    clean(row?.scheduled_for_local) ||
    clean(getDateLike(row, "scheduled-for-utc", null)) ||
    clean(getDateLike(row, "scheduled-for-local", null)) ||
    null
  );
}

function getMasterOwnerId(row = null) {
  return asPositiveInteger(
    row?.master_owner_id ??
      row?.master_owner_item_id ??
      getAppRefLike(row, "master-owner", null),
    null
  );
}

function getPhoneItemId(row = null) {
  return asPositiveInteger(
    row?.phone_item_id ??
      row?.phone_number_item_id ??
      getAppRefLike(row, "phone-number", null),
    null
  );
}

function getTouchNumber(row = null) {
  return asPositiveInteger(
    row?.touch_number ?? row?.touch ?? getNumberLike(row, "touch-number", null),
    null
  );
}

function dedupeRowsByOwnerPhoneTouch(rows = []) {
  const seen = new Set();
  const deduped = [];
  const duplicates = [];

  for (const row of rows) {
    const owner_id = getMasterOwnerId(row);
    const phone_id = getPhoneItemId(row);
    const touch_number = getTouchNumber(row);

    if (!owner_id || !phone_id || !touch_number) {
      deduped.push(row);
      continue;
    }

    const key = `${owner_id}:${phone_id}:${touch_number}`;
    if (seen.has(key)) {
      duplicates.push(row);
      continue;
    }

    seen.add(key);
    deduped.push(row);
  }

  return { deduped, duplicates };
}

function isRunnableStatus(row = null) {
  return lower(getQueueStatus(row)) === "queued";
}

function isDue(row = null, now = nowIso()) {
  const scheduled_at = getScheduledAt(row);
  if (!scheduled_at) return true;

  const scheduled_ts = toTimestamp(scheduled_at);
  const now_ts = toTimestamp(now);

  if (scheduled_ts === null || now_ts === null) return false;
  return scheduled_ts <= now_ts;
}

function sortByScheduledAt(rows = []) {
  return [...rows].sort((left, right) => {
    const left_ts = toTimestamp(getScheduledAt(left)) ?? Number.MAX_SAFE_INTEGER;
    const right_ts = toTimestamp(getScheduledAt(right)) ?? Number.MAX_SAFE_INTEGER;
    return left_ts - right_ts;
  });
}

function isLegacyQueueRow(row = null) {
  return Boolean(row?.item_id) && !row?.id;
}

export async function loadQueuedItems(limit = 50, deps = {}) {
  if (typeof deps.loadQueuedItems === "function") {
    return normalizeRows(await deps.loadQueuedItems(limit));
  }

  if (typeof deps.fetchAllItems === "function") {
    return normalizeRows(await deps.fetchAllItems());
  }

  const result = await loadRunnableSendQueueRows(limit, deps);
  console.log("SUPABASE QUEUE LOADED", { count: result.rows.length });
  return result.rows;
}

async function buildLegacyCandidateSummary(limit = 50, now = nowIso(), deps = {}) {
  const loaded_rows = sortByScheduledAt(normalizeRows(await deps.fetchAllItems()));
  const queued_rows = loaded_rows.filter((row) => isRunnableStatus(row));
  const due_rows = [];
  const future_rows = [];

  for (const row of queued_rows) {
    if (isDue(row, now)) {
      due_rows.push(row);
    } else {
      future_rows.push(row);
    }
  }

  const runnable_rows = due_rows.slice(0, limit);

  return {
    rows: runnable_rows,
    raw_rows: loaded_rows,
    total_rows_loaded: loaded_rows.length,
    queued_rows_loaded: queued_rows.length,
    due_rows: due_rows.length,
    future_rows: future_rows.length,
    outside_window_rows: 0,
    preclaim_outside_window_excluded_count: 0,
    preclaim_retry_pending_excluded_count: 0,
    preclaim_paused_name_missing_count: 0,
    preclaim_paused_invalid_count: 0,
    preclaim_paused_max_retries_count: 0,
    preclaim_scanned_count: loaded_rows.length,
    eligible_claim_count: runnable_rows.length,
    first_10_candidate_item_ids: runnable_rows
      .map((row) => getQueueRowId(row))
      .filter(Boolean)
      .slice(0, 10),
    first_10_excluded: future_rows.slice(0, 10).map((row) => ({
      item_id: getQueueRowId(row),
      reason: "not_due_yet",
    })),
    skipped: [],
  };
}

async function buildSupabaseCandidateSummary(limit = 50, now = nowIso(), deps = {}) {
  const load_runnable_send_queue_rows = deps.loadRunnableSendQueueRows || loadRunnableSendQueueRows;
  const loaded = await load_runnable_send_queue_rows(limit, {
    ...deps,
    now,
  });

  const rows = normalizeRows(loaded.rows);
  const raw_rows = normalizeRows(loaded.raw_rows);
  const skipped = normalizeRows(loaded.skipped);

  return {
    rows,
    raw_rows,
    total_rows_loaded: raw_rows.length,
    queued_rows_loaded: raw_rows.length,
    due_rows: rows.length,
    future_rows: skipped.filter((entry) =>
      ["scheduled_for_in_future", "next_retry_pending"].includes(entry?.reason)
    ).length,
    outside_window_rows: Number(loaded.preclaim_outside_window_excluded_count || 0),
    preclaim_outside_window_excluded_count: Number(
      loaded.preclaim_outside_window_excluded_count || 0
    ),
    preclaim_retry_pending_excluded_count: Number(
      loaded.preclaim_retry_pending_excluded_count || 0
    ),
    preclaim_paused_name_missing_count: Number(
      loaded.preclaim_paused_name_missing_count || 0
    ),
    preclaim_paused_invalid_count: Number(
      loaded.preclaim_paused_invalid_count || 0
    ),
    preclaim_paused_max_retries_count: Number(
      loaded.preclaim_paused_max_retries_count || 0
    ),
    preclaim_scanned_count: Number(loaded.preclaim_scanned_count || raw_rows.length),
    eligible_claim_count: Number(loaded.eligible_claim_count || rows.length),
    first_10_candidate_item_ids: rows
      .map((row) => getQueueRowId(row))
      .filter(Boolean)
      .slice(0, 10),
    first_10_excluded: skipped.slice(0, 10).map((entry) => ({
      item_id: getQueueRowId(entry?.row || entry),
      reason: entry?.reason || "excluded",
    })),
    skipped,
  };
}

async function buildQueueCandidates(limit = 50, now = nowIso(), deps = {}) {
  if (typeof deps.fetchAllItems === "function") {
    return buildLegacyCandidateSummary(limit, now, deps);
  }

  return buildSupabaseCandidateSummary(limit, now, deps);
}

function classifyRunResult(result = {}) {
  if (result?.skipped && result?.reason === "queue_item_claim_conflict") {
    return { disposition: "duplicate_locked", reason: "queue_item_claim_conflict" };
  }

  if (result?.reason === "outside_contact_window") {
    return { disposition: "blocked", reason: "outside_contact_window" };
  }

  if (result?.skipped) {
    return {
      disposition: "skipped",
      reason: clean(result?.reason) || "skipped",
    };
  }

  if (lower(result?.queue_status) === "blocked") {
    return {
      disposition: "blocked",
      reason: clean(result?.reason) || "blocked",
    };
  }

  if (
    result?.sent ||
    clean(result?.provider_message_id) ||
    clean(result?.message_id) ||
    clean(result?.sid)
  ) {
    return {
      disposition: "sent",
      reason: clean(result?.reason) || "sent",
    };
  }

  if (result?.ok === false) {
    return {
      disposition: "failed",
      reason: clean(result?.reason) || "failed",
    };
  }

  return {
    disposition: "sent",
    reason: clean(result?.reason) || "ok",
  };
}

function buildSkippedSummary(queue_item_id, reason) {
  return {
    queue_item_id,
    queue_row_id: queue_item_id,
    reason,
  };
}

function metadataValue(row = null, key = "") {
  const metadata = row && typeof row.metadata === "object" && !Array.isArray(row.metadata)
    ? row.metadata
    : {};
  return metadata[key];
}

function hasCandidateSnapshot(row = null) {
  const snapshot = metadataValue(row, "candidate_snapshot");
  return snapshot && typeof snapshot === "object" && !Array.isArray(snapshot);
}

function hasQueueSellerFirstName(row = null) {
  return Boolean(clean(resolveQueueSellerFirstName(row)));
}

function invalidQueueRowReason(row = null) {
  const normalized = normalizeSendQueueRow(row);
  const manual_inbox_send = isManualInboxSend(normalized);
  const selected_template_id = clean(
    normalized.template_id ||
      metadataValue(normalized, "selected_template_id") ||
      metadataValue(normalized, "template_id")
  );
  if (!manual_inbox_send && !selected_template_id) return "missing_selected_template_id";
  if (!manual_inbox_send && !hasCandidateSnapshot(normalized)) return "missing_candidate_snapshot";
  if (!clean(normalized.message_body || normalized.message_text)) return "missing_message_body";
  if (!clean(normalized.to_phone_number)) return "missing_to_phone_number";
  if (!clean(normalized.from_phone_number)) return "missing_from_phone_number";
  if (!manual_inbox_send && !hasQueueSellerFirstName(normalized)) return "missing_seller_first_name";
  return null;
}

function finalQueueStatusForResult(result = {}) {
  return clean(result.final_queue_status || result.queue_status) || null;
}

function withFinalQueueStatus(result = {}, fallback = null) {
  return {
    ...result,
    final_queue_status: finalQueueStatusForResult(result) || fallback || null,
  };
}

export async function runSendQueue(
  {
    limit = DEFAULT_BATCH_SIZE,
    dry_run = false,
    now = nowIso(),
  } = {},
  deps = {}
) {
  const get_system_flag = deps.getSystemFlag || getSystemFlag;
  // ── System control gate ────────────────────────────────────────────────
  if (!dry_run) {
    const queue_runner_enabled = await get_system_flag("queue_runner_enabled");
    if (!queue_runner_enabled) {
      info("queue_runner.blocked", { flag: "queue_runner_enabled" });
      return { ok: false, status: 423, ...buildDisabledResponse("queue_runner_enabled", "runSendQueue"), skipped: true, reason: "system_control_disabled", sent_count: 0, results: [] };
    }
    const outbound_sms_enabled = await get_system_flag("outbound_sms_enabled");
    if (!outbound_sms_enabled) {
      info("queue_runner.blocked", { flag: "outbound_sms_enabled" });
      return { ok: false, status: 423, ...buildDisabledResponse("outbound_sms_enabled", "runSendQueue"), skipped: true, reason: "system_control_disabled", sent_count: 0, results: [] };
    }
  }

  const with_run_lock = deps.withRunLock || withRunLock;
  const build_podio_cooldown_skip_result =
    typeof deps.buildPodioCooldownSkipResult === "function"
      ? deps.buildPodioCooldownSkipResult
      : null;
  const process_send_queue_item =
    deps.processSendQueueItem || processSendQueueItem;
  const fail_queue_item = deps.failQueueItem || failQueueItem;
  const claim_send_queue_row = deps.claimSendQueueRow || claimSendQueueRow;
  const pause_invalid_queue_row = deps.pauseInvalidQueueRow || pauseInvalidQueueRow;
  const pause_max_retries_queue_row = deps.pauseMaxRetriesQueueRow || pauseMaxRetriesQueueRow;
  const pause_name_missing_queue_row =
    deps.pauseNameMissingQueueRow || pauseNameMissingQueueRow;
  const finalize_claimed_send_queue_rows =
    deps.finalizeClaimedSendQueueRows || finalizeClaimedSendQueueRows;
  const record_system_alert = deps.recordSystemAlert || (async () => ({}));
  const resolve_system_alert = deps.resolveSystemAlert || (async () => ({}));
  const log_info = deps.info || info;
  const log_warn = deps.warn || warn;
  const run_started_at = now;
  const processing_run_id = clean(deps.processing_run_id) || crypto.randomUUID();

  if (!dry_run && build_podio_cooldown_skip_result) {
    const cooldown_skip = await build_podio_cooldown_skip_result({
      attempted_count: 0,
      claimed_count: 0,
      started_count: 0,
      processed_count: 0,
      sent_count: 0,
      failed_count: 0,
      blocked_count: 0,
      skipped_count: 0,
      duplicate_locked_count: 0,
      total_rows_loaded: 0,
      queued_rows_loaded: 0,
      due_rows: 0,
      future_rows: 0,
      run_started_at,
      results: [],
    });

    if (cooldown_skip?.skipped && cooldown_skip?.reason === "podio_rate_limit_cooldown_active") {
      log_warn("queue.run_skipped_podio_cooldown", {
        reason: cooldown_skip.reason,
        retry_after_seconds: cooldown_skip.retry_after_seconds || null,
        retry_after_at: cooldown_skip.retry_after_at || null,
      });
      return cooldown_skip;
    }
  }

  return with_run_lock({
    scope: QUEUE_RUN_LOCK_SCOPE,
    enabled: !dry_run,
    owner: "queue_runner",
    metadata: {
      limit,
      dry_run,
      run_started_at,
      processing_run_id,
    },
    onLocked: async (lock) => {
      const lock_meta = lock?.meta || {};
      log_warn("queue.run_skipped_lock_active", {
        reason: "queue_runner_lock_active",
        lock_scope: lock?.scope || QUEUE_RUN_LOCK_SCOPE,
        lock_record_item_id: lock?.record_item_id || null,
        lock_lease_token: lock_meta?.lease_token || lock?.lease_token || null,
        lock_expires_at: lock_meta?.expires_at || null,
        lock_owner: lock_meta?.owner || null,
        lock_acquired_at: lock_meta?.acquired_at || null,
        lock_acquisition_count: lock_meta?.acquisition_count || null,
        recovery_hint: "If stale, run forceReleaseStaleLock for queue-run and retry.",
      });

      try {
        await record_system_alert({
          code: "queue_run_lock_active",
          affected_ids: [lock?.record_item_id || null].filter(Boolean),
          metadata: {
            scope: lock?.scope || QUEUE_RUN_LOCK_SCOPE,
            reason: "queue_runner_lock_active",
          },
        });
        await resolve_system_alert({ code: "queue_run_started" }).catch(() => {});
      } catch (_) {
        // Non-fatal for lock-skipped response.
      }

      return {
        ok: true,
        skipped: true,
        reason: "queue_runner_lock_active",
        dry_run,
        lock,
        attempted_count: 0,
        claimed_count: 0,
        started_count: 0,
        processed_count: 0,
        sent_count: 0,
        failed_count: 0,
        blocked_count: 0,
        skipped_count: 0,
        duplicate_locked_count: 0,
        total_rows_loaded: 0,
        queued_rows_loaded: 0,
        due_rows: 0,
        future_rows: 0,
        batch_duration_ms: 0,
        run_started_at,
        results: [],
      };
    },
    fn: async () => {
      log_info("queue.run_started", {
        limit,
        dry_run,
        now_utc: now,
      });

      log_info("queue.run_fetch_started", {
        limit,
        dry_run,
      });

      console.log("QUEUE START");
      console.log("ENV CHECK", {
        supabase_configured: hasSupabaseConfig(),
        textgrid_account_sid_present: Boolean(clean(process.env.TEXTGRID_ACCOUNT_SID)),
        textgrid_auth_token_present: Boolean(clean(process.env.TEXTGRID_AUTH_TOKEN)),
      });

      const started_at_ms = Date.now();
      const candidate_summary = await buildQueueCandidates(limit, now, {
        ...deps,
        dry_run,
      });
      const dedupe_result = dedupeRowsByOwnerPhoneTouch(
        candidate_summary.rows
      );
      let rows = dedupe_result.deduped;
      const duplicates = dedupe_result.duplicates;
      let preclaim_paused_name_missing_count = Number(
        candidate_summary.preclaim_paused_name_missing_count || 0
      );
      let preclaim_paused_invalid_count = Number(
        candidate_summary.preclaim_paused_invalid_count || 0
      );
      let preclaim_paused_max_retries_count = Number(
        candidate_summary.preclaim_paused_max_retries_count || 0
      );
      let preclaim_outside_window_excluded_count = Number(
        candidate_summary.preclaim_outside_window_excluded_count || 0
      );
      let preclaim_retry_pending_excluded_count = Number(
        candidate_summary.preclaim_retry_pending_excluded_count || 0
      );

      console.log("ROWS LOADED", {
        total_rows_loaded: candidate_summary.total_rows_loaded,
        runnable_count: rows.length,
        preclaim_scanned_count: candidate_summary.preclaim_scanned_count,
        preclaim_outside_window_excluded_count:
          preclaim_outside_window_excluded_count,
        preclaim_retry_pending_excluded_count:
          preclaim_retry_pending_excluded_count,
        preclaim_paused_name_missing_count,
        preclaim_paused_invalid_count,
        preclaim_paused_max_retries_count,
      });
      console.log("RAW ROWS", candidate_summary.raw_rows);

      log_info("queue.run_candidates_loaded", {
        total_rows_loaded: candidate_summary.total_rows_loaded,
        queued_rows_loaded: candidate_summary.queued_rows_loaded,
        due_rows: candidate_summary.due_rows,
        future_rows: candidate_summary.future_rows,
        runnable_count: rows.length,
        preclaim_outside_window_excluded_count:
          preclaim_outside_window_excluded_count,
        preclaim_retry_pending_excluded_count:
          preclaim_retry_pending_excluded_count,
        preclaim_paused_name_missing_count,
        preclaim_paused_invalid_count,
        preclaim_paused_max_retries_count,
        preclaim_scanned_count: candidate_summary.preclaim_scanned_count,
        eligible_claim_count: candidate_summary.eligible_claim_count,
        now_utc: now,
        first_10_candidate_item_ids: candidate_summary.first_10_candidate_item_ids,
        first_10_filter_excluded: candidate_summary.first_10_excluded,
      });

      if (duplicates.length > 0) {
        log_warn("queue.run_batch_duplicates_suppressed", {
          duplicate_count: duplicates.length,
          suppressed_queue_item_ids: duplicates
            .map((row) => getQueueRowId(row))
            .filter(Boolean)
            .slice(0, 20),
        });
      }

      const results = [];
      const skipped_reasons = [];
      const claimed_rows = [];
      let invalid_queue_row_count = 0;
      let finalize_safety_net_count = 0;
      let stuck_recycled_count = 0;
      let attempted_count = 0;
      let claimed_count = 0;
      let started_count = 0;
      let processed_count = 0;
      let sent_count = 0;
      let failed_count = 0;
      let blocked_count = 0;
      let skipped_count = 0;
      let duplicate_locked_count = 0;
      let partial = false;
      let first_failing_queue_item_id = null;
      let first_failing_reason = null;
      let first_failure_queue_item_id = null;
      let first_failure_reason = null;

      if (!dry_run) {
        const pausePreclaimTerminalRow = async (row, reason) => {
          const queue_item_id = getQueueRowId(row);
          if (!queue_item_id) return false;

          const is_name_missing = reason === "missing_seller_first_name";
          const is_max_retries = reason === "max_retries_reached";
          const queue_status = is_name_missing
            ? "paused_name_missing"
            : is_max_retries
            ? "paused_max_retries"
            : "paused_invalid_queue_row";
          const pause_fn = is_name_missing
            ? pause_name_missing_queue_row
            : is_max_retries
            ? pause_max_retries_queue_row
            : pause_invalid_queue_row;

          try {
            const paused = await pause_fn(row, reason, {
              ...deps,
              now,
            });
            if (!is_name_missing && !is_max_retries) {
              invalid_queue_row_count += 1;
            }
            skipped_count += 1;
            blocked_count += 1;
            skipped_reasons.push(buildSkippedSummary(queue_item_id, reason));
            results.push(withFinalQueueStatus({
              ok: false,
              skipped: true,
              reason,
              queue_status,
              queue_item_id,
              queue_row_id: queue_item_id,
            }, paused?.queue_status || queue_status));
            return true;
          } catch (error) {
            partial = true;
            failed_count += 1;
            log_warn("queue.preclaim_terminal_pause_failed", {
              queue_item_id,
              reason,
              message: clean(error?.message) || "preclaim_terminal_pause_failed",
            });
            return false;
          }
        };

        const preflight_skipped = normalizeRows(candidate_summary.skipped);
        for (const skipped of preflight_skipped) {
          const skipped_row = skipped?.row ? normalizeSendQueueRow(skipped.row) : null;
          const queue_item_id = getQueueRowId(skipped_row || skipped);
          const reason = clean(skipped?.reason) || "excluded";
          if (!skipped_row || !queue_item_id) continue;

          const already_paused_status = lower(skipped?.queue_status);
          if (
            [
              "paused_name_missing",
              "paused_invalid_queue_row",
              "paused_max_retries",
            ].includes(already_paused_status)
          ) {
            if (already_paused_status === "paused_invalid_queue_row") {
              invalid_queue_row_count += 1;
            }
            skipped_count += 1;
            blocked_count += 1;
            skipped_reasons.push(buildSkippedSummary(queue_item_id, reason));
            results.push(withFinalQueueStatus({
              ok: false,
              skipped: true,
              reason,
              queue_status: already_paused_status,
              queue_item_id,
              queue_row_id: queue_item_id,
            }, already_paused_status));
            continue;
          }

          if (reason === "max_retries_reached") {
            await pausePreclaimTerminalRow(skipped_row, reason);
          } else if (reason === "missing_seller_first_name") {
            await pausePreclaimTerminalRow(skipped_row, reason);
          } else if (
            [
              "missing_message_body",
              "missing_to_phone_number",
              "missing_from_phone_number",
              "missing_selected_template_id",
              "missing_candidate_snapshot",
              "missing_queue_row_id",
            ].includes(reason)
          ) {
            await pausePreclaimTerminalRow(skipped_row, reason);
          }
        }

        const runnable_rows = [];
        for (const row of rows) {
          if (isLegacyQueueRow(row)) {
            runnable_rows.push(row);
            continue;
          }

          const normalized_row = normalizeSendQueueRow(row);
          const next_retry_ts = toTimestamp(normalized_row.next_retry_at);
          const now_ts = toTimestamp(now) ?? Date.now();
          if (next_retry_ts !== null && next_retry_ts > now_ts) {
            preclaim_retry_pending_excluded_count += 1;
            continue;
          }

          const contact_window = (deps.evaluateContactWindow || evaluateContactWindow)(normalized_row, {
            ...deps,
            now,
          });
          if (contact_window && contact_window.allowed === false) {
            preclaim_outside_window_excluded_count += 1;
            continue;
          }

          if (Number(normalized_row.retry_count || 0) >= Number(normalized_row.max_retries || 3)) {
            preclaim_paused_max_retries_count += 1;
            await pausePreclaimTerminalRow(normalized_row, "max_retries_reached");
            continue;
          }

          const invalid_reason = invalidQueueRowReason(normalized_row);
          if (!invalid_reason) {
            runnable_rows.push(normalized_row);
            continue;
          }

          if (invalid_reason === "missing_seller_first_name") {
            preclaim_paused_name_missing_count += 1;
          } else {
            preclaim_paused_invalid_count += 1;
          }
          await pausePreclaimTerminalRow(normalized_row, invalid_reason);
        }
        rows = runnable_rows;
      }

      for (const duplicate of duplicates) {
        const queue_item_id = getQueueRowId(duplicate);
        skipped_count += 1;
        skipped_reasons.push(
          buildSkippedSummary(queue_item_id, "queue_batch_duplicate_suppressed")
        );
        results.push({
          ok: true,
          skipped: true,
          reason: "queue_batch_duplicate_suppressed",
          queue_item_id,
          queue_row_id: queue_item_id,
        });
      }

      for (const row of rows) {
        const queue_item_id = getQueueRowId(row);
        const legacy_mode = isLegacyQueueRow(row);
        let queue_row = row;
        let lock_token = clean(deps.claimedLockToken) || null;

        started_count += 1;
        attempted_count += 1;

        console.log("PROCESSING ROW", queue_item_id);

        if (dry_run) {
          processed_count += 1;
          skipped_count += 1;
          skipped_reasons.push(buildSkippedSummary(queue_item_id, "dry_run"));
          results.push({
            ok: true,
            skipped: true,
            dry_run: true,
            queue_item_id,
            queue_row_id: queue_item_id,
            reason: "dry_run",
          });
          continue;
        }

        if (!legacy_mode) {
          const claim_result = await claim_send_queue_row(queue_row, {
            ...deps,
            now,
            processing_run_id,
            run_started_at,
          });

          if (!claim_result?.claimed) {
            duplicate_locked_count += 1;
            skipped_count += 1;
            skipped_reasons.push(
              buildSkippedSummary(queue_item_id, claim_result?.reason || "queue_item_claim_conflict")
            );
          results.push({
            ok: true,
            skipped: true,
            reason: claim_result?.reason || "queue_item_claim_conflict",
            queue_item_id,
            queue_row_id: queue_item_id,
          });
            continue;
          }

          claimed_count += 1;
          queue_row = claim_result.row || queue_row;
          lock_token = claim_result.lock_token || lock_token;
          claimed_rows.push({
            row: queue_row,
            lock_token,
            queue_row_id: getQueueRowId(queue_row),
            reason: "row_process_finally_safety_net",
          });
        }

        try {
          const result = await process_send_queue_item(
            legacy_mode ? queue_item_id : queue_row,
            {
              ...deps,
              now,
              claimedLockToken: lock_token,
            }
          );
          processed_count += 1;
          const classification = classifyRunResult(result);

          if (classification.disposition === "sent") {
            if (legacy_mode) claimed_count += 1;
            sent_count += 1;
          }

          if (classification.disposition === "failed") {
            partial = true;
            failed_count += 1;
            skipped_reasons.push(
              buildSkippedSummary(queue_item_id, classification.reason)
            );
            log_warn("queue.run_item_failed_soft", {
              queue_item_id,
              reason: classification.reason,
            });

            if (!first_failing_queue_item_id) {
              first_failing_queue_item_id = queue_item_id;
              first_failing_reason = classification.reason;
            }
            if (!first_failure_queue_item_id) {
              first_failure_queue_item_id = queue_item_id;
              first_failure_reason = classification.reason;
            }
          }

          if (classification.disposition === "blocked") {
            blocked_count += 1;
            skipped_reasons.push(
              buildSkippedSummary(queue_item_id, classification.reason)
            );
          }

          if (classification.disposition === "skipped") {
            skipped_count += 1;
            skipped_reasons.push(
              buildSkippedSummary(queue_item_id, classification.reason)
            );
          }

          if (classification.disposition === "duplicate_locked") {
            duplicate_locked_count += 1;
            skipped_count += 1;
            skipped_reasons.push(
              buildSkippedSummary(queue_item_id, classification.reason)
            );
          }

          results.push({
            ...result,
            final_queue_status:
              finalQueueStatusForResult(result) ||
              (classification.disposition === "sent"
                ? "sent"
                : classification.disposition === "failed"
                ? "failed"
                : classification.disposition === "blocked"
                ? lower(result?.queue_status) || "blocked"
                : null),
            queue_item_id:
              result?.queue_row_id ??
              result?.queue_item_id ??
              queue_item_id,
            queue_row_id:
              result?.queue_row_id ??
              result?.queue_item_id ??
              queue_item_id,
          });
        } catch (error) {
          if (isRevisionLimitExceeded(error)) {
            processed_count += 1;
            skipped_count += 1;
            skipped_reasons.push(
              buildSkippedSummary(queue_item_id, "queue_item_revision_limit_exceeded")
            );

            log_warn("queue.run_item_skipped_revision_limit", {
              queue_item_id,
              failure_bucket: "revision_limit_exceeded",
              manual_review_required: true,
              message: clean(error?.message) || null,
            });

            try {
              await record_system_alert({
                code: "revision_limit_exceeded",
                affected_ids: [queue_item_id],
                metadata: {
                  failure_bucket: "revision_limit_exceeded",
                  recovery: "manual_review_required",
                },
              });
              await resolve_system_alert({
                code: "revision_limit_exceeded",
                affected_ids: [queue_item_id],
              }).catch(() => {});
            } catch (_) {
              // Non-fatal alerting failure.
            }

            results.push({
              queue_item_id,
              ok: true,
              skipped: true,
              reason: "queue_item_revision_limit_exceeded",
              failure_bucket: "revision_limit_exceeded",
              manual_review_required: true,
            });
            continue;
          }

          partial = true;
          processed_count += 1;
          failed_count += 1;

          if (!first_failing_queue_item_id) {
            first_failing_queue_item_id = queue_item_id;
            first_failing_reason = "queue_processing_exception";
          }
          if (!first_failure_queue_item_id) {
            first_failure_queue_item_id = queue_item_id;
            first_failure_reason = "queue_processing_exception";
          }

          let fail_result = null;
          try {
            const fail_payload = {
              queue_status: legacy_mode ? "Failed" : "failed",
              failed_reason: "Network Error",
              retry_count: Number(queue_row?.retry_count || 0) + 1,
            };

            if (legacy_mode && deps.failQueueItem) {
              fail_result = await fail_queue_item(queue_item_id, fail_payload);
            } else {
              fail_result = await fail_queue_item(queue_row, fail_payload, {
                ...deps,
                now,
                lock_token: lock_token || queue_row?.lock_token || null,
              });
            }
          } catch (update_error) {
            log_warn("queue.run_crash_fail_update_failed", {
              queue_item_id,
              message: update_error?.message || "Unknown queue fail update error",
            });
          }

          log_warn("queue.run_item_crashed", {
            queue_item_id,
            reason: "queue_processing_exception",
            message: error?.message || "Unknown queue processing error",
          });

          skipped_reasons.push(
            buildSkippedSummary(queue_item_id, "queue_processing_exception")
          );

          results.push({
            ok: false,
            sent: false,
            queue_status: fail_result?.queue_status || "failed",
            final_queue_status: fail_result?.queue_status || "failed",
            reason: "queue_processing_exception",
            failed_reason: clean(error?.message) || "queue_processing_exception",
            queue_item_id,
            queue_row_id: queue_item_id,
          });
        } finally {
          // Claimed rows are swept after the batch. Keeping this hook explicit
          // makes every per-row path pass through a finalization checkpoint.
        }
      }

      if (!dry_run && claimed_rows.length > 0) {
        try {
          const safety = await finalize_claimed_send_queue_rows(claimed_rows, {
            ...deps,
            now,
            processing_run_id,
            run_started_at,
          });
          finalize_safety_net_count = Number(
            safety?.finalized_count ?? safety?.finalize_safety_net_count ?? 0
          );
          stuck_recycled_count = Number(
            safety?.stuck_recycled_count ?? finalize_safety_net_count
          );

          if (Array.isArray(safety?.finalized) && safety.finalized.length > 0) {
            const finalized_by_id = new Map(
              safety.finalized.map((row) => [String(getQueueRowId(row)), row])
            );
            for (const result of results) {
              const result_id = String(result?.queue_row_id ?? result?.queue_item_id ?? "");
              const finalized = finalized_by_id.get(result_id);
              if (finalized) {
                result.final_queue_status = finalized.queue_status || result.final_queue_status || null;
                result.finalize_safety_net = true;
              }
            }
          }

          if (safety?.errors?.length) {
            partial = true;
            log_warn("queue.finalize_safety_net_errors", {
              processing_run_id,
              error_count: safety.errors.length,
              errors: safety.errors.slice(0, 10),
            });
          }
        } catch (error) {
          partial = true;
          log_warn("queue.finalize_safety_net_failed", {
            processing_run_id,
            claimed_count: claimed_rows.length,
            message: clean(error?.message) || "finalize_safety_net_failed",
          });
        }
      }

      const batch_duration_ms = Date.now() - started_at_ms;
      const summary = {
        ok: true,
        partial,
        dry_run,
        skipped: undefined,
        reason: null,
        attempted_count,
        claimed_count,
        started_count,
        processed_count,
        sent_count,
        failed_count,
        blocked_count,
        skipped_count,
        duplicate_locked_count,
        invalid_queue_row_count,
        finalize_safety_net_count,
        stuck_recycled_count,
        preclaim_outside_window_excluded_count:
          preclaim_outside_window_excluded_count,
        preclaim_retry_pending_excluded_count:
          preclaim_retry_pending_excluded_count,
        preclaim_paused_name_missing_count,
        preclaim_paused_invalid_count,
        preclaim_paused_max_retries_count,
        preclaim_scanned_count: candidate_summary.preclaim_scanned_count,
        eligible_claim_count: rows.length,
        first_failing_queue_item_id,
        first_failing_reason,
        first_failure_queue_item_id,
        first_failure_reason,
        batch_duration_ms,
        total_rows_loaded: candidate_summary.total_rows_loaded,
        queued_rows_loaded: candidate_summary.queued_rows_loaded,
        due_rows: candidate_summary.due_rows,
        future_rows: candidate_summary.future_rows,
        outside_window_rows: blocked_count || candidate_summary.outside_window_rows,
        first_10_candidate_item_ids: candidate_summary.first_10_candidate_item_ids,
        first_10_excluded: candidate_summary.first_10_excluded,
        run_started_at,
        processing_run_id,
        results,
      };

      log_info("queue.run_completed", {
        ...summary,
        sent_rows: sent_count,
        blocked_rows: blocked_count,
        now_utc: now,
        first_10_skipped_item_ids_with_reason: skipped_reasons.slice(0, 10),
      });

      captureSystemEvent("queue_run_completed", {
        ok: summary.ok,
        partial: summary.partial,
        dry_run: summary.dry_run,
        attempted_count: summary.attempted_count,
        claimed_count: summary.claimed_count,
        sent_count: summary.sent_count,
        failed_count: summary.failed_count,
        blocked_count: summary.blocked_count,
        skipped_count: summary.skipped_count,
        duplicate_locked_count: summary.duplicate_locked_count,
        invalid_queue_row_count: summary.invalid_queue_row_count,
        finalize_safety_net_count: summary.finalize_safety_net_count,
        stuck_recycled_count: summary.stuck_recycled_count,
        preclaim_outside_window_excluded_count:
          summary.preclaim_outside_window_excluded_count,
        preclaim_retry_pending_excluded_count:
          summary.preclaim_retry_pending_excluded_count,
        preclaim_paused_name_missing_count:
          summary.preclaim_paused_name_missing_count,
        preclaim_paused_invalid_count:
          summary.preclaim_paused_invalid_count,
        preclaim_paused_max_retries_count:
          summary.preclaim_paused_max_retries_count,
        preclaim_scanned_count: summary.preclaim_scanned_count,
        eligible_claim_count: summary.eligible_claim_count,
        batch_duration_ms: summary.batch_duration_ms,
        total_rows_loaded: summary.total_rows_loaded,
        due_rows: summary.due_rows,
      });

      if (summary.failed_count > 0) {
        sendCriticalAlert({
          title: "Queue Run Failures",
          description: `${summary.failed_count} SMS send${summary.failed_count === 1 ? "" : "s"} failed in queue run`,
          color: 0xe74c3c,
          fields: [
            { name: "Failed", value: String(summary.failed_count), inline: true },
            { name: "Sent", value: String(summary.sent_count), inline: true },
            { name: "Duration (ms)", value: String(summary.batch_duration_ms), inline: true },
            { name: "First Failure Reason", value: summary.first_failure_reason || "unknown", inline: false },
          ],
          timestamp: new Date().toISOString(),
          footer: { text: "queue_run_completed" },
        });
      }

      return summary;
    },
  });
}

export default runSendQueue;
