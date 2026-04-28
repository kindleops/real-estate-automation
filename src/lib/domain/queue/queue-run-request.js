import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

function asNumber(value, fallback = null) {
  if (value === null || value === undefined || clean(value) === "") {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function statusForResult(result) {
  if (result?.status != null) return result.status;
  return result?.ok === false ? 500 : 200;
}

export async function handleQueueRunRequest(request, method, deps = {}) {
  console.log("QUEUE EXECUTION STARTED");

  const require_cron_auth =
    deps.requireCronAuth ||
    (await import("@/lib/security/cron-auth.js")).requireCronAuth;
  const run_send_queue =
    deps.runSendQueue ||
    (await import("@/lib/domain/queue/run-send-queue.js")).runSendQueue;
  const build_podio_cooldown_skip_result =
    deps.buildPodioCooldownSkipResult || null;
  const route_logger = deps.logger;
  const json_response =
    deps.jsonResponse ||
    ((body, init) =>
      Response.json(body, {
        status: init?.status,
      }));

  route_logger?.info?.("queue_run.route_enter", { method });

  try {
    const auth = require_cron_auth(request, route_logger);
    if (!auth.authorized) return auth.response;

    const body =
      method === "POST"
        ? await request.json().catch(() => ({}))
        : {};
    const search_params = new URL(request.url).searchParams;

    const limit = Math.max(
      1,
      Math.min(
        50,
        asNumber(method === "POST" ? body?.limit : search_params.get("limit"), 50)
      )
    );
    const dry_run = asBoolean(
      method === "POST" ? body?.dry_run : search_params.get("dry_run"),
      false
    );

    route_logger?.info?.("queue_run.requested", {
      method,
      limit,
      dry_run,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    route_logger?.info?.("queue_run.before_run_send_queue", {
      limit,
      dry_run,
      dry_run_reason: dry_run ? "requested" : "disabled",
      rollout_mode: process.env.ROLLOUT_MODE || null,
      forced_dry_run: false,
      master_owner_id: null,
      scope_reason: "supabase_queue_runner",
    });

    await notifyDiscordOps({
      event_type: "queue_run_started",
      severity: "info",
      domain: "queue",
      title: "Queue Run Started",
      summary: `Queue run started (limit=${limit}, dry_run=${dry_run})`,
      fields: [
        { name: "Limit", value: String(limit), inline: true },
        { name: "Dry Run", value: String(Boolean(dry_run)), inline: true },
      ],
      dedupe_key: `queue_run_started:${dry_run ? "dry" : "live"}`,
      throttle_window_seconds: 60,
    });

    const result = await run_send_queue({
      limit,
      dry_run,
    });

    if (result?.skipped) {
      route_logger?.warn?.("queue_run.early_return", {
        reason: result.reason || "unknown",
        skipped: true,
        lock_expires_at: result.lock?.meta?.expires_at || null,
        lock_owner: result.lock?.meta?.owner || null,
        lock_acquired_at: result.lock?.meta?.acquired_at || null,
        run_started_at: result.run_started_at || null,
      });
    }

    route_logger?.info?.("queue_run.after_run_send_queue", {
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      partial: result?.partial || false,
      dry_run: result?.dry_run ?? null,
      reason: result?.reason || null,
      attempted_count: result?.attempted_count ?? null,
      claimed_count: result?.claimed_count ?? null,
      started_count: result?.started_count ?? null,
      processed_count: result?.processed_count ?? null,
      sent_count: result?.sent_count ?? null,
      failed_count: result?.failed_count ?? null,
      blocked_count: result?.blocked_count ?? null,
      skipped_count: result?.skipped_count ?? null,
      duplicate_locked_count: result?.duplicate_locked_count ?? null,
      first_failing_queue_item_id:
        result?.first_failing_queue_item_id ?? null,
      first_failing_reason: result?.first_failing_reason ?? null,
      first_failure_queue_item_id:
        result?.first_failure_queue_item_id ?? null,
      first_failure_reason: result?.first_failure_reason ?? null,
      batch_duration_ms: result?.batch_duration_ms ?? null,
      due_rows: result?.due_rows ?? null,
      future_rows: result?.future_rows ?? null,
      total_rows_loaded: result?.total_rows_loaded ?? null,
    });

    route_logger?.info?.("queue_run.summary", {
      attempted_count: result?.attempted_count ?? 0,
      claimed_count: result?.claimed_count ?? 0,
      processed_count: result?.processed_count ?? 0,
      sent_count: result?.sent_count ?? 0,
      failed_count: result?.failed_count ?? 0,
      blocked_count: result?.blocked_count ?? 0,
      skipped_count: result?.skipped_count ?? 0,
      invalid_queue_row_count: result?.invalid_queue_row_count ?? 0,
      preclaim_paused_name_missing_count:
        result?.preclaim_paused_name_missing_count ?? 0,
      preclaim_outside_window_excluded_count:
        result?.preclaim_outside_window_excluded_count ?? 0,
      preclaim_retry_pending_excluded_count:
        result?.preclaim_retry_pending_excluded_count ?? 0,
      eligible_claim_count: result?.eligible_claim_count ?? 0,
      first_failure_reason:
        result?.first_failure_reason ??
        result?.first_failing_reason ??
        null,
    });

    await notifyDiscordOps({
      event_type: result?.ok === false ? "queue_run_failed" : "queue_run_completed",
      severity: result?.ok === false || (result?.failed_count || 0) > 0 ? "error" : "success",
      domain: "queue",
      title: result?.ok === false ? "Queue Run Failed" : "Queue Run Completed",
      summary: result?.ok === false
        ? clean(result?.reason) || "queue_run_failed"
        : `Queue run completed: sent=${result?.sent_count || 0}, failed=${result?.failed_count || 0}, blocked=${result?.blocked_count || 0}`,
      fields: [
        { name: "Sent", value: String(result?.sent_count || 0), inline: true },
        { name: "Failed", value: String(result?.failed_count || 0), inline: true },
        { name: "Blocked", value: String(result?.blocked_count || 0), inline: true },
      ],
      metadata: { result },
      should_alert_critical: result?.ok === false || (result?.failed_count || 0) > 0,
    });

    return json_response(
      {
        ok: result?.ok !== false,
        route: "internal/queue/run",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = {
      name: error?.name || "Error",
      message: error?.message || "queue_run_failed",
      status: Number(error?.status || 0) || null,
      path: clean(error?.path) || null,
      method: clean(error?.method) || null,
      operation: clean(error?.operation) || null,
      retry_after_seconds:
        error?.retry_after_seconds === undefined ||
        error?.retry_after_seconds === null
          ? null
          : Number(error.retry_after_seconds),
      rate_limit_remaining:
        error?.rate_limit_remaining === undefined ||
        error?.rate_limit_remaining === null
          ? null
          : Number(error.rate_limit_remaining),
      stack: error?.stack || null,
    };

    route_logger?.error?.("queue_run.failed", {
      method,
      error: diagnostics,
    });

    if (
      build_podio_cooldown_skip_result &&
      diagnostics.name === "PodioError" &&
      diagnostics.status === 420
    ) {
      const result = await build_podio_cooldown_skip_result({
        results: [],
        processed_count: 0,
        sent_count: 0,
        failed_count: 0,
        skipped_count: 0,
      });

      return json_response(
        {
          ok: true,
          route: "internal/queue/run",
          result,
        },
        { status: 200 }
      );
    }

    await notifyDiscordOps({
      event_type: "queue_run_failed",
      severity: "critical",
      domain: "queue",
      title: "Queue Run Request Failed",
      summary: diagnostics.message,
      metadata: diagnostics,
      should_alert_critical: true,
    });

    return json_response(
      {
        ok: false,
        error: "queue_run_failed",
        message: diagnostics.message,
      },
      { status: 500 }
    );
  }
}

export default handleQueueRunRequest;
