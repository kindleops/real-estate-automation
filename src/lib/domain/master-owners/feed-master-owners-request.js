import { child } from "@/lib/logging/logger.js";
import {
  DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
  capFeederBatch,
  capFeederScanLimit,
  getRolloutControls,
  resolveFeederViewScope,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";

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
  const normalized = clean(value);
  if (!normalized) return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function asPositiveNumber(value, fallback) {
  const n = asNumber(value, null);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function buildSourceViewLogMeta({
  requested_source_view_id = null,
  requested_source_view_name = null,
  resolved_source_view_id = null,
  resolved_source_view_name = null,
  safe_scope_passed = false,
  safe_scope_reason = null,
  defaulted = false,
} = {}) {
  return {
    requested_source_view_id: clean(requested_source_view_id) || null,
    requested_source_view_name: clean(requested_source_view_name) || null,
    resolved_source_view_id: clean(resolved_source_view_id) || null,
    resolved_source_view_name: clean(resolved_source_view_name) || null,
    safe_scope_passed: Boolean(safe_scope_passed),
    safe_scope_reason: clean(safe_scope_reason) || null,
    defaulted: Boolean(defaulted),
  };
}

export function normalizeFeederRequest(input = {}) {
  return {
    limit: asPositiveNumber(input?.limit, 25),
    scan_limit: asPositiveNumber(input?.scan_limit, 150),
    dry_run: asBoolean(input?.dry_run, false),
    seller_id: clean(input?.seller_id) || null,
    master_owner_id: input?.master_owner_id ?? null,
    source_view_id: clean(input?.source_view_id) || null,
    source_view_name: clean(input?.source_view_name) || null,
    test_mode: asBoolean(input?.test_mode, false),
  };
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
  const { runMasterOwnerOutboundFeeder } = await import(
    "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js"
  );

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

async function defaultWithRunLock(args) {
  const { withRunLock } = await import("@/lib/domain/runs/run-locks.js");
  return withRunLock(args);
}

async function defaultRecordSystemAlert(args) {
  const { recordSystemAlert } = await import("@/lib/domain/alerts/system-alerts.js");
  return recordSystemAlert(args);
}

async function defaultResolveSystemAlert(args) {
  const { resolveSystemAlert } = await import("@/lib/domain/alerts/system-alerts.js");
  return resolveSystemAlert(args);
}

export async function runFeederWithRollout(input = {}, deps = {}) {
  const {
    limit,
    scan_limit,
    dry_run,
    seller_id,
    master_owner_id,
    source_view_id,
    source_view_name,
    test_mode,
  } = normalizeFeederRequest(input);
  const {
    getRolloutControlsImpl = getRolloutControls,
    resolveMutationDryRunImpl = resolveMutationDryRun,
    resolveScopedIdImpl = resolveScopedId,
    resolveFeederViewScopeImpl = resolveFeederViewScope,
    capFeederBatchImpl = capFeederBatch,
    capFeederScanLimitImpl = capFeederScanLimit,
    executeRunImpl = executeRun,
    withRunLockImpl = defaultWithRunLock,
    recordSystemAlertImpl = defaultRecordSystemAlert,
    resolveSystemAlertImpl = defaultResolveSystemAlert,
    logger: route_logger = logger,
  } = deps;
  const rollout = getRolloutControlsImpl();
  const dry_run_resolution = resolveMutationDryRunImpl({
    requested_dry_run: dry_run,
  });
  const safe_owner_scope = resolveScopedIdImpl({
    requested_id: master_owner_id,
    safe_id: rollout.single_master_owner_id,
    resource: "master_owner",
  });
  const view_scope = resolveFeederViewScopeImpl({
    requested_view_id: source_view_id,
    requested_view_name: source_view_name,
  });
  const scope_log_meta = buildSourceViewLogMeta({
    requested_source_view_id: source_view_id,
    requested_source_view_name: source_view_name,
    resolved_source_view_id: view_scope.source_view_id,
    resolved_source_view_name:
      view_scope.source_view_name ||
      (view_scope.defaulted ? DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME : null),
    safe_scope_passed: view_scope.safe_scope_passed,
    safe_scope_reason: view_scope.reason,
    defaulted: view_scope.defaulted,
  });

  route_logger.info("master_owner_feeder.source_view_scope_evaluated", scope_log_meta);

  if (!safe_owner_scope.ok) {
    return {
      ok: false,
      reason: safe_owner_scope.reason,
      dry_run: dry_run_resolution.effective_dry_run,
      rollout_reason: dry_run_resolution.reason,
    };
  }

  if (!view_scope.ok) {
    route_logger.warn("master_owner_feeder.source_view_scope_blocked", scope_log_meta);

    return {
      ok: false,
      reason: view_scope.reason,
      dry_run: dry_run_resolution.effective_dry_run,
      rollout_reason: dry_run_resolution.reason,
    };
  }

  const effective_limit = capFeederBatchImpl(limit, 25);
  const effective_scan_limit = capFeederScanLimitImpl(scan_limit, 150);
  const effective_master_owner_id = safe_owner_scope.effective_id || null;
  const effective_dry_run = dry_run_resolution.effective_dry_run;
  const lock_scope = effective_master_owner_id
    ? `feeder:${effective_master_owner_id}`
    : view_scope.source_view_id
      ? `feeder:view:${view_scope.source_view_id}`
      : view_scope.source_view_name
        ? `feeder:view:${view_scope.source_view_name}`
        : "feeder";

  const execute = async () => {
    const result = await executeRunImpl({
      limit: effective_limit,
      scan_limit: effective_scan_limit,
      dry_run: effective_dry_run,
      seller_id,
      master_owner_id: effective_master_owner_id,
      source_view_id: view_scope.source_view_id,
      source_view_name: view_scope.source_view_name,
      test_mode,
    });

    const resolved_result = {
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
    };

    route_logger.info(
      "master_owner_feeder.source_view_resolved",
      buildSourceViewLogMeta({
        requested_source_view_id: source_view_id,
        requested_source_view_name: source_view_name,
        resolved_source_view_id: resolved_result?.rollout?.resolved_source_view_id ?? null,
        resolved_source_view_name:
          resolved_result?.rollout?.resolved_source_view_name ?? null,
        safe_scope_passed: true,
        safe_scope_reason: view_scope.reason,
        defaulted: view_scope.defaulted,
      })
    );

    route_logger.info("master_owner_feeder.completed", {
      effective_source_view_name:
        resolved_result?.rollout?.effective_source_view_name ?? null,
      resolved_source_view_name:
        resolved_result?.rollout?.resolved_source_view_name ?? null,
      resolved_source_view_id:
        resolved_result?.rollout?.resolved_source_view_id ?? null,
      scanned_count: resolved_result?.scanned_count ?? 0,
      eligible_owner_count: resolved_result?.eligible_owner_count ?? 0,
      queued_count: resolved_result?.queued_count ?? 0,
      skip_reason_counts: resolved_result?.skip_reason_counts ?? [],
      template_resolution_diagnostics:
        resolved_result?.template_resolution_diagnostics ?? null,
    });

    return resolved_result;
  };

  if (effective_dry_run) {
    return execute();
  }

  return withRunLockImpl({
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
      await recordSystemAlertImpl({
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
        await recordSystemAlertImpl({
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
        await resolveSystemAlertImpl({
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
