import { NextResponse } from "next/server";

import { capQueueBatch, getRolloutControls, resolveMutationDryRun, resolveScopedId } from "@/lib/config/rollout-controls.js";
import { child } from "@/lib/logging/logger.js";
import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.queue.run",
});

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

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

export async function GET(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);

    const rollout = getRolloutControls();
    const master_owner_scope = resolveScopedId({
      requested_id: asNumber(searchParams.get("master_owner_id"), null),
      safe_id: rollout.single_master_owner_id,
      resource: "master_owner",
    });
    if (!master_owner_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: master_owner_scope.reason,
        },
        { status: 400 }
      );
    }

    const limit = capQueueBatch(asNumber(searchParams.get("limit"), 50), 50);
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(searchParams.get("dry_run"), false),
    });

    logger.info("queue_run.requested", {
      method: "GET",
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await runSendQueue({
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/run",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("queue_run.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "queue_run_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));

    const rollout = getRolloutControls();
    const master_owner_scope = resolveScopedId({
      requested_id: asNumber(body?.master_owner_id, null),
      safe_id: rollout.single_master_owner_id,
      resource: "master_owner",
    });
    if (!master_owner_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: master_owner_scope.reason,
        },
        { status: 400 }
      );
    }

    const limit = capQueueBatch(asNumber(body?.limit, 50), 50);
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(body?.dry_run, false),
    });

    logger.info("queue_run.requested", {
      method: "POST",
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await runSendQueue({
      limit,
      dry_run: dry_run_resolution.effective_dry_run,
      master_owner_id: master_owner_scope.effective_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/run",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("queue_run.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "queue_run_failed",
      },
      { status: 500 }
    );
  }
}
