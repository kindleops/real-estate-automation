import { NextResponse } from "next/server";

import { capReconcileBatch, getRolloutControls, resolveScopedId } from "@/lib/config/rollout-controls.js";
import { child } from "@/lib/logging/logger.js";
import {
  buildPodioCooldownSkipResult,
  isPodioRateLimitError,
  serializePodioError,
} from "@/lib/providers/podio.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import { runQueueReconcileRunner } from "@/lib/workers/queue-reconcile-runner.js";
import { reconcileSupabaseDeliveryStatuses } from "@/lib/domain/events/normalize-delivery-status.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.queue.reconcile",
});

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
    const limit = capReconcileBatch(asNumber(searchParams.get("limit"), 50), 50);
    const stale_after_minutes = asNumber(searchParams.get("stale_after_minutes"), 20);

    logger.info("queue_reconcile.requested", {
      method: "GET",
      limit,
      stale_after_minutes,
      master_owner_id: master_owner_scope.effective_id,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await runQueueReconcileRunner({
      limit,
      stale_after_minutes,
      master_owner_id: master_owner_scope.effective_id,
    });

    const supabase_delivery_result = await reconcileSupabaseDeliveryStatuses({
      limit,
    }).catch((err) => {
      logger.warn("queue_reconcile.supabase_delivery_reconcile_failed", { error: err?.message });
      return { ok: false, total_normalized: 0 };
    });

    logger.info("queue_reconcile.completed", {
      method: "GET",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      scanned_count: result?.scanned_count ?? 0,
      processed_count: result?.processed_count ?? 0,
      manual_review_count: result?.manual_review_count ?? 0,
      skipped_count: result?.skipped_count ?? 0,
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/reconcile",
        result,
        supabase_delivery_reconcile: supabase_delivery_result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("queue_reconcile.failed", {
      method: "GET",
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await buildPodioCooldownSkipResult({
        scanned_count: 0,
        processed_count: 0,
        recovered_delivered_count: 0,
        recovered_failed_count: 0,
        recovered_sent_count: 0,
        manual_review_count: 0,
        skipped_count: 0,
        results: [],
      });

      return NextResponse.json(
        {
          ok: true,
          route: "internal/queue/reconcile",
          result,
        },
        { status: 200 }
      );
    }

    return NextResponse.json(
      {
        ok: false,
        error: "queue_reconcile_failed",
        message: diagnostics.message,
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
    const limit = capReconcileBatch(asNumber(body?.limit, 50), 50);
    const stale_after_minutes = asNumber(body?.stale_after_minutes, 20);

    logger.info("queue_reconcile.requested", {
      method: "POST",
      limit,
      stale_after_minutes,
      master_owner_id: master_owner_scope.effective_id,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await runQueueReconcileRunner({
      limit,
      stale_after_minutes,
      master_owner_id: master_owner_scope.effective_id,
    });

    const supabase_delivery_result = await reconcileSupabaseDeliveryStatuses({
      limit,
    }).catch((err) => {
      logger.warn("queue_reconcile.supabase_delivery_reconcile_failed", { error: err?.message });
      return { ok: false, total_normalized: 0 };
    });

    logger.info("queue_reconcile.completed", {
      method: "POST",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      scanned_count: result?.scanned_count ?? 0,
      processed_count: result?.processed_count ?? 0,
      manual_review_count: result?.manual_review_count ?? 0,
      skipped_count: result?.skipped_count ?? 0,
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/reconcile",
        result,
        supabase_delivery_reconcile: supabase_delivery_result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("queue_reconcile.failed", {
      method: "POST",
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await buildPodioCooldownSkipResult({
        scanned_count: 0,
        processed_count: 0,
        recovered_delivered_count: 0,
        recovered_failed_count: 0,
        recovered_sent_count: 0,
        manual_review_count: 0,
        skipped_count: 0,
        results: [],
      });

      return NextResponse.json(
        {
          ok: true,
          route: "internal/queue/reconcile",
          result,
        },
        { status: 200 }
      );
    }

    return NextResponse.json(
      {
        ok: false,
        error: "queue_reconcile_failed",
        message: diagnostics.message,
      },
      { status: 500 }
    );
  }
}
