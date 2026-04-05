import {
  getRolloutControls,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";

const FORCE_DUE_MAX_ROWS = 25;

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

export async function handleQueueForceDueRequest(request, method, deps = {}) {
  const require_cron_auth =
    deps.requireCronAuth ||
    (await import("@/lib/security/cron-auth.js")).requireCronAuth;
  const force_due_queued_items =
    deps.forceDueQueuedItems ||
    (await import("@/lib/domain/queue/force-due-queued-items.js"))
      .forceDueQueuedItems;
  const route_logger = deps.logger;
  const json_response =
    deps.jsonResponse ||
    ((body, init) => Response.json(body, { status: init?.status }));

  route_logger?.info?.("queue_force_due.route_enter", { method });

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
        method === "POST"
          ? body?.master_owner_id
          : searchParams.get("master_owner_id"),
        null
      ),
      safe_id: rollout.single_master_owner_id,
      resource: "master_owner",
    });

    if (!master_owner_scope.ok) {
      return json_response(
        { ok: false, error: master_owner_scope.reason },
        { status: 400 }
      );
    }

    const limit = Math.min(
      asNumber(
        method === "POST" ? body?.limit : searchParams.get("limit"),
        FORCE_DUE_MAX_ROWS
      ) ?? FORCE_DUE_MAX_ROWS,
      FORCE_DUE_MAX_ROWS
    );

    // Default dry_run=true for this recovery tool — must be explicitly opt-out
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(
        method === "POST" ? body?.dry_run : searchParams.get("dry_run"),
        true
      ),
    });

    const older_than_minutes = asNumber(
      method === "POST"
        ? body?.older_than_minutes
        : searchParams.get("older_than_minutes"),
      null
    );

    route_logger?.info?.("queue_force_due.requested", {
      method,
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
      older_than_minutes,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await force_due_queued_items({
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
      older_than_minutes,
    });

    route_logger?.info?.("queue_force_due.completed", {
      ok: result?.ok !== false,
      dry_run: result?.dry_run ?? null,
      total_rows_loaded: result?.total_rows_loaded ?? null,
      eligible_rows: result?.eligible_rows ?? null,
      rescheduled_count: result?.rescheduled_count ?? null,
      skipped_count: result?.skipped_count ?? null,
      master_owner_id: result?.master_owner_id ?? null,
    });

    return json_response(
      {
        ok: result?.ok !== false,
        route: "internal/queue/force-due",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    route_logger?.error?.("queue_force_due.failed", { error });

    return json_response(
      { ok: false, error: "queue_force_due_failed" },
      { status: 500 }
    );
  }
}
