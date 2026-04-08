import {
  capQueueBatch,
  getRolloutControls,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";
import {
  buildPodioCooldownSkipResult,
  isPodioRateLimitError,
  serializePodioError,
} from "@/lib/providers/podio.js";

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes"].includes(normalized)) return true;
    if (["false", "0", "no"].includes(normalized)) return false;
  }
  return fallback;
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

export async function handleQueueRunRequest(request, method, deps = {}) {
  const require_cron_auth =
    deps.requireCronAuth ||
    (await import("@/lib/security/cron-auth.js")).requireCronAuth;
  const run_send_queue =
    deps.runSendQueue ||
    (await import("@/lib/domain/queue/run-send-queue.js")).runSendQueue;
  const build_podio_cooldown_skip_result =
    deps.buildPodioCooldownSkipResult || buildPodioCooldownSkipResult;
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

    const { searchParams } = new URL(request.url);
    const body =
      method === "POST"
        ? await request.json().catch(() => ({}))
        : null;

    const rollout = getRolloutControls();
    const master_owner_scope = resolveScopedId({
      requested_id: asNumber(
        method === "POST" ? body?.master_owner_id : searchParams.get("master_owner_id"),
        null
      ),
      safe_id: rollout.single_master_owner_id,
      resource: "master_owner",
    });
    if (!master_owner_scope.ok) {
      return json_response(
        {
          ok: false,
          error: master_owner_scope.reason,
        },
        { status: 400 }
      );
    }

    const limit = capQueueBatch(
      asNumber(method === "POST" ? body?.limit : searchParams.get("limit"), 50),
      50
    );
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(
        method === "POST" ? body?.dry_run : searchParams.get("dry_run"),
        false
      ),
    });

    route_logger?.info?.("queue_run.requested", {
      method,
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    route_logger?.info?.("queue_run.before_run_send_queue", {
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      dry_run_reason: dry_run_resolution.reason,
      rollout_mode: dry_run_resolution.mode,
      forced_dry_run: dry_run_resolution.forced,
      master_owner_id: master_owner_scope.effective_id,
      scope_reason: master_owner_scope.reason,
    });

    const result = await run_send_queue({
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
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

    return json_response(
      {
        ok: result?.ok !== false,
        route: "internal/queue/run",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    route_logger?.error?.("queue_run.failed", {
      method,
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await build_podio_cooldown_skip_result({
        dry_run: false,
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
        run_started_at: new Date().toISOString(),
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
