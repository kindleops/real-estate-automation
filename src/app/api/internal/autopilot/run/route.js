import { NextResponse } from "next/server";

import {
  buildPodioCooldownSkipResult,
  isPodioRateLimitError,
  serializePodioError,
} from "@/lib/providers/podio.js";
import {
  capAutopilotScan,
  getRolloutControls,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { child } from "@/lib/logging/logger.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import { runDealsAutopilot } from "@/lib/domain/autopilot/run-deals-autopilot.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.autopilot.run",
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
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

async function runAutopilotWithRollout({
  scan_limit = null,
  dry_run = false,
  contract_item_id = null,
} = {}) {
  const rollout = getRolloutControls();
  const contract_scope = resolveScopedId({
    requested_id: contract_item_id,
    safe_id: rollout.single_contract_id,
    resource: "contract",
  });

  if (!contract_scope.ok) {
    return {
      ok: false,
      reason: contract_scope.reason,
      dry_run: true,
    };
  }

  const dry_run_resolution = resolveMutationDryRun({
    requested_dry_run: dry_run,
  });
  const effective_scan_limit = capAutopilotScan(
    scan_limit ?? rollout.autopilot_max_scan,
    rollout.autopilot_max_scan
  );
  const effective_contract_item_id = contract_scope.effective_id || null;
  const effective_dry_run = dry_run_resolution.effective_dry_run;
  const lock_scope = effective_contract_item_id
    ? `autopilot:${effective_contract_item_id}`
    : "autopilot";

  const cooldown_skip = await buildPodioCooldownSkipResult({
    dry_run: effective_dry_run,
    contract_item_id: effective_contract_item_id,
    scan_limit: effective_scan_limit,
    rollout: {
      requested_dry_run: Boolean(dry_run),
      effective_dry_run,
      rollout_reason: dry_run_resolution.reason,
      requested_scan_limit: scan_limit,
      effective_scan_limit,
      requested_contract_item_id: contract_item_id,
      effective_contract_item_id,
    },
  });

  if (cooldown_skip?.podio_cooldown?.active) {
    logger.warn("autopilot_run.skipped_podio_cooldown", {
      scan_limit: effective_scan_limit,
      dry_run: effective_dry_run,
      contract_item_id: effective_contract_item_id,
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

  const execute = async () =>
    runDealsAutopilot({
      scan_limit: effective_scan_limit,
      dry_run: effective_dry_run,
      contract_item_id: effective_contract_item_id,
    }).then((result) => ({
      ...result,
      rollout: {
        requested_dry_run: Boolean(dry_run),
        effective_dry_run,
        rollout_reason: dry_run_resolution.reason,
        requested_scan_limit: scan_limit,
        effective_scan_limit,
        requested_contract_item_id: contract_item_id,
        effective_contract_item_id,
      },
    }));

  if (effective_dry_run) {
    return execute();
  }

  return withRunLock({
    scope: lock_scope,
    lease_ms: 20 * 60_000,
    owner: "autopilot_route",
    metadata: {
      scan_limit: effective_scan_limit,
      contract_item_id: effective_contract_item_id,
    },
    onLocked: async (lock) => {
      await recordSystemAlert({
        subsystem: "autopilot",
        code: "runner_overlap",
        severity: "warning",
        retryable: true,
        summary: "Deals autopilot skipped because an active lease is already in progress.",
        dedupe_key: lock_scope,
        metadata: {
          scan_limit: effective_scan_limit,
          contract_item_id: effective_contract_item_id,
          lock,
        },
      });

      return {
        ok: true,
        skipped: true,
        reason: "autopilot_lock_active",
        dry_run: false,
        contract_item_id: effective_contract_item_id,
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
          subsystem: "autopilot",
          code: "runner_failed",
          severity: "high",
          retryable: true,
          summary: `Deals autopilot failed: ${clean(result?.summary?.error_count) || clean(result?.reason) || "unknown_error"}`,
          dedupe_key: lock_scope,
          metadata: result?.rollout || {},
        });
      } else {
        await resolveSystemAlert({
          subsystem: "autopilot",
          code: "runner_failed",
          dedupe_key: lock_scope,
          resolution_message: "Deals autopilot completed without fatal run failure.",
        });
      }

      return result;
    },
  });
}

export async function GET(request) {
  let request_meta = {
    scan_limit: null,
    dry_run: false,
    contract_item_id: null,
  };

  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const scan_limit = asNumber(searchParams.get("scan_limit"), null);
    const dry_run = asBoolean(searchParams.get("dry_run"), false);
    const contract_item_id = asNumber(searchParams.get("contract_id"), null);
    request_meta = {
      scan_limit,
      dry_run,
      contract_item_id,
    };

    const result = await runAutopilotWithRollout({
      scan_limit,
      dry_run,
      contract_item_id,
    });

    logger.info("autopilot_run.completed", {
      method: "GET",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      scan_limit: result?.scan_limit ?? scan_limit,
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json({
      ok: result?.ok !== false,
      route: "internal/autopilot/run",
      result,
    });
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("autopilot_run.failed", {
      method: "GET",
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await buildPodioCooldownSkipResult({
        ...request_meta,
      });

      return NextResponse.json({
        ok: true,
        route: "internal/autopilot/run",
        result,
      });
    }

    return NextResponse.json(
      {
        ok: false,
        error: "autopilot_run_failed",
        message: diagnostics.message,
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  let request_meta = {
    scan_limit: null,
    dry_run: false,
    contract_item_id: null,
  };

  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const scan_limit = asNumber(body?.scan_limit, null);
    const dry_run = asBoolean(body?.dry_run, false);
    const contract_item_id = asNumber(body?.contract_id, null);
    request_meta = {
      scan_limit,
      dry_run,
      contract_item_id,
    };

    const result = await runAutopilotWithRollout({
      scan_limit,
      dry_run,
      contract_item_id,
    });

    logger.info("autopilot_run.completed", {
      method: "POST",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      scan_limit: result?.scan_limit ?? scan_limit,
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json({
      ok: result?.ok !== false,
      route: "internal/autopilot/run",
      result,
    });
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("autopilot_run.failed", {
      method: "POST",
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await buildPodioCooldownSkipResult({
        ...request_meta,
      });

      return NextResponse.json({
        ok: true,
        route: "internal/autopilot/run",
        result,
      });
    }

    return NextResponse.json(
      {
        ok: false,
        error: "autopilot_run_failed",
        message: diagnostics.message,
      },
      { status: 500 }
    );
  }
}
