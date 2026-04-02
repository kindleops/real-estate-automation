import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { getPodioRetryAfterSeconds, isPodioRateLimitError } from "@/lib/providers/podio.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import {
  capFeederBatch,
  capFeederScanLimit,
  getRolloutControls,
  resolveFeederViewScope,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { runMasterOwnerOutboundFeeder } from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.outbound.feed_master_owners",
});

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
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function statusForResult(result) {
  if (result?.reason === "master_owner_feeder_rate_limited") return 429;
  return result?.ok === false ? 400 : 200;
}

function buildRateLimitResponse(error) {
  const retry_after_seconds = getPodioRetryAfterSeconds(error, null);
  const retry_after_at =
    Number.isFinite(retry_after_seconds) && retry_after_seconds > 0
      ? new Date(Date.now() + retry_after_seconds * 1000).toISOString()
      : null;

  return NextResponse.json(
    {
      ok: false,
      error: "master_owner_feeder_rate_limited",
      retry_after_seconds,
      retry_after_at,
    },
    { status: 429 }
  );
}

async function executeRun({
  limit = 25,
  scan_limit = 150,
  dry_run = false,
  seller_id = null,
  master_owner_id = null,
  source_view_id = null,
  source_view_name = null,
  test_mode = false,
}) {
  return runMasterOwnerOutboundFeeder({
    limit,
    scan_limit,
    dry_run,
    seller_id: clean(seller_id) || null,
    master_owner_id: asNumber(master_owner_id, null),
    source_view_id: clean(source_view_id) || null,
    source_view_name: clean(source_view_name) || null,
    test_mode,
  });
}

async function runFeederWithRollout({
  limit = 25,
  scan_limit = 150,
  dry_run = false,
  seller_id = null,
  master_owner_id = null,
  source_view_id = null,
  source_view_name = null,
  test_mode = false,
} = {}) {
  const rollout = getRolloutControls();
  const dry_run_resolution = resolveMutationDryRun({
    requested_dry_run: dry_run,
  });
  const safe_owner_scope = resolveScopedId({
    requested_id: master_owner_id,
    safe_id: rollout.single_master_owner_id,
    resource: "master_owner",
  });
  const view_scope = resolveFeederViewScope({
    requested_view_id: source_view_id,
    requested_view_name: source_view_name,
  });

  if (!safe_owner_scope.ok) {
    return {
      ok: false,
      reason: safe_owner_scope.reason,
      dry_run: dry_run_resolution.effective_dry_run,
      rollout_reason: dry_run_resolution.reason,
    };
  }

  if (!view_scope.ok) {
    return {
      ok: false,
      reason: view_scope.reason,
      dry_run: dry_run_resolution.effective_dry_run,
      rollout_reason: dry_run_resolution.reason,
    };
  }

  const effective_limit = capFeederBatch(limit, 25);
  const effective_scan_limit = capFeederScanLimit(scan_limit, 150);
  const effective_master_owner_id = safe_owner_scope.effective_id || null;
  const effective_dry_run = dry_run_resolution.effective_dry_run;
  const lock_scope = effective_master_owner_id
    ? `feeder:${effective_master_owner_id}`
    : view_scope.source_view_id
      ? `feeder:view:${view_scope.source_view_id}`
      : view_scope.source_view_name
        ? `feeder:view:${view_scope.source_view_name}`
        : "feeder";

  const execute = async () =>
    executeRun({
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      dry_run: effective_dry_run,
      seller_id,
      master_owner_id: effective_master_owner_id,
      source_view_id: view_scope.source_view_id,
      source_view_name: view_scope.source_view_name,
      test_mode,
    }).then((result) => ({
      ...result,
      rollout: {
        requested_dry_run: Boolean(dry_run),
        effective_dry_run,
        rollout_reason: dry_run_resolution.reason,
        requested_limit: limit,
        effective_limit,
        requested_scan_limit: scan_limit,
        effective_scan_limit,
        requested_master_owner_id: master_owner_id || null,
        effective_master_owner_id,
        effective_source_view_id: view_scope.source_view_id,
        effective_source_view_name: view_scope.source_view_name,
        resolved_source_view_id: result?.source?.view_id ?? view_scope.source_view_id,
        resolved_source_view_name: result?.source?.view_name ?? view_scope.source_view_name,
      },
    }));

  if (effective_dry_run) {
    return execute();
  }

  return withRunLock({
    scope: lock_scope,
    lease_ms: 20 * 60_000,
    owner: "feeder_route",
    metadata: {
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      master_owner_id: effective_master_owner_id,
      source_view_id: view_scope.source_view_id,
      source_view_name: view_scope.source_view_name,
    },
    onLocked: async (lock) => {
      await recordSystemAlert({
        subsystem: "feeder",
        code: "runner_overlap",
        severity: "warning",
        retryable: true,
        summary: "Master-owner feeder skipped because an active lease is already in progress.",
        dedupe_key: lock_scope,
        metadata: {
          limit: effective_limit,
          scan_limit: effective_scan_limit,
          master_owner_id: effective_master_owner_id,
          source_view_id: view_scope.source_view_id,
          source_view_name: view_scope.source_view_name,
          lock,
        },
      });

      return {
        ok: true,
        skipped: true,
        reason: "master_owner_feeder_lock_active",
        dry_run: false,
        rollout: {
          requested_dry_run: Boolean(dry_run),
          effective_dry_run: false,
          rollout_reason: dry_run_resolution.reason,
        },
        lock,
      };
    },
    fn: async () => {
      const result = await execute();

      if (result?.ok === false) {
        await recordSystemAlert({
          subsystem: "feeder",
          code: "runner_failed",
          severity: "high",
          retryable: true,
          summary: `Master-owner feeder failed: ${clean(result?.reason) || "unknown_error"}`,
          dedupe_key: lock_scope,
          affected_ids: result?.queued_owner_ids || [],
          metadata: result?.rollout || {},
        });
      } else {
        await resolveSystemAlert({
          subsystem: "feeder",
          code: "runner_failed",
          dedupe_key: lock_scope,
          resolution_message: "Master-owner feeder completed without fatal failure.",
        });
      }

      return result;
    },
  });
}

export async function GET(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);

    const limit = asNumber(searchParams.get("limit"), 25);
    const scan_limit = asNumber(searchParams.get("scan_limit"), 150);
    const dry_run = asBoolean(searchParams.get("dry_run"), false);
    const test_mode = asBoolean(searchParams.get("test_mode"), false);
    const seller_id = clean(searchParams.get("seller_id"));
    const master_owner_id = searchParams.get("master_owner_id");
    const source_view_id = searchParams.get("source_view_id");
    const source_view_name = clean(searchParams.get("source_view_name"));

    logger.info("master_owner_feeder.requested", {
      method: "GET",
      limit,
      scan_limit,
      dry_run,
      test_mode,
      seller_id: seller_id || null,
      master_owner_id: asNumber(master_owner_id, null),
      source_view_id: clean(source_view_id) || null,
      source_view_name: source_view_name || null,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await runFeederWithRollout({
      limit,
      scan_limit,
      dry_run,
      test_mode,
      seller_id,
      master_owner_id,
      source_view_id,
      source_view_name,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("master_owner_feeder.failed", { error });

    if (isPodioRateLimitError(error)) {
      return buildRateLimitResponse(error);
    }

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_failed",
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

    const limit = asNumber(body?.limit, 25);
    const scan_limit = asNumber(body?.scan_limit, 150);
    const dry_run = asBoolean(body?.dry_run, false);
    const test_mode = asBoolean(body?.test_mode, false);
    const seller_id = clean(body?.seller_id);
    const master_owner_id = body?.master_owner_id;
    const source_view_id = body?.source_view_id;
    const source_view_name = clean(body?.source_view_name);

    logger.info("master_owner_feeder.requested", {
      method: "POST",
      limit,
      scan_limit,
      dry_run,
      test_mode,
      seller_id: seller_id || null,
      master_owner_id: asNumber(master_owner_id, null),
      source_view_id: clean(source_view_id) || null,
      source_view_name: source_view_name || null,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await runFeederWithRollout({
      limit,
      scan_limit,
      dry_run,
      test_mode,
      seller_id,
      master_owner_id,
      source_view_id,
      source_view_name,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("master_owner_feeder.failed", { error });

    if (isPodioRateLimitError(error)) {
      return buildRateLimitResponse(error);
    }

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_failed",
      },
      { status: 500 }
    );
  }
}
