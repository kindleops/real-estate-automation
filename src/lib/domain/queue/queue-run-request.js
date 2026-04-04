import {
  capQueueBatch,
  getRolloutControls,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";

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
  const route_logger = deps.logger;
  const json_response =
    deps.jsonResponse ||
    ((body, init) =>
      Response.json(body, {
        status: init?.status,
      }));

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

    const result = await run_send_queue({
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
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
    route_logger?.error?.("queue_run.failed", { error });

    return json_response(
      {
        ok: false,
        error: "queue_run_failed",
      },
      { status: 500 }
    );
  }
}