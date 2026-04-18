import APP_IDS from "@/lib/config/app-ids.js";
import { child } from "@/lib/logging/logger.js";
import {
  buildPodioBackpressureSkipResult,
  buildPodioCooldownSkipResult,
  filterAppItems,
  getDateValue,
} from "@/lib/providers/podio.js";
import {
  DEFAULT_FEEDER_BATCH_SIZE,
  DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
  DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
  DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
  DEFAULT_FEEDER_BUFFER_MIN_QUEUED,
  DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
  DEFAULT_FEEDER_SCAN_LIMIT,
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

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function extractItems(response) {
  if (Array.isArray(response)) return response;
  return Array.isArray(response?.items) ? response.items : [];
}

function getResponseCount(response) {
  if (Array.isArray(response)) return response.length;
  return Number(
    response?.filtered ??
      response?.total ??
      response?.count ??
      extractItems(response).length
  ) || 0;
}

function getQueueScheduledAt(item) {
  return (
    getDateValue(item, "scheduled-for-utc", null) ||
    getDateValue(item, "scheduled-for-local", null) ||
    null
  );
}

function resolveDesiredBufferTarget({
  available_inventory_count = 0,
  critical_low_threshold = DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
  replenish_target = DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
  healthy_target = DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
  ideal_target = DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
} = {}) {
  if (available_inventory_count >= ideal_target) return ideal_target;
  if (available_inventory_count < critical_low_threshold) return ideal_target;
  if (available_inventory_count < replenish_target) return healthy_target;
  return ideal_target;
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

function buildRolloutSourceViewMeta({
  requested_source_view_id = null,
  requested_source_view_name = null,
  effective_source_view_id = null,
  effective_source_view_name = null,
  resolved_source_view_id = null,
  resolved_source_view_name = null,
  source_view_fallback_occurred = false,
  source_view_fallback_reason = null,
} = {}) {
  const keepIfPresent = (value) => {
    const normalized = clean(value);
    return normalized ? value : null;
  };

  return {
    requested_source_view_id: keepIfPresent(requested_source_view_id),
    requested_source_view_name: clean(requested_source_view_name) || null,
    effective_source_view_id: keepIfPresent(effective_source_view_id),
    effective_source_view_name: clean(effective_source_view_name) || null,
    resolved_source_view_id: keepIfPresent(resolved_source_view_id),
    resolved_source_view_name: clean(resolved_source_view_name) || null,
    source_view_fallback_occurred: Boolean(source_view_fallback_occurred),
    source_view_fallback_reason: clean(source_view_fallback_reason) || null,
  };
}

export function normalizeFeederRequest(input = {}) {
  const defaults = getRolloutControls();
  const limit_was_provided = Boolean(clean(input?.limit));
  const scan_limit_was_provided = Boolean(clean(input?.scan_limit));

  return {
    limit: asPositiveNumber(
      input?.limit,
      defaults.feeder_default_batch || DEFAULT_FEEDER_BATCH_SIZE
    ),
    scan_limit: asPositiveNumber(
      input?.scan_limit,
      defaults.feeder_default_scan_limit || DEFAULT_FEEDER_SCAN_LIMIT
    ),
    limit_was_provided,
    scan_limit_was_provided,
    dry_run: asBoolean(input?.dry_run, false),
    seller_id: clean(input?.seller_id) || null,
    master_owner_id: input?.master_owner_id ?? null,
    source_view_id: clean(input?.source_view_id) || null,
    source_view_name: clean(input?.source_view_name) || null,
    test_mode: asBoolean(input?.test_mode, false),
  };
}

async function executeRun({
  limit = DEFAULT_FEEDER_BATCH_SIZE,
  scan_limit = DEFAULT_FEEDER_SCAN_LIMIT,
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

async function defaultInspectQueueBuffer({
  now = new Date().toISOString(),
  buffer_target = DEFAULT_FEEDER_BUFFER_MIN_QUEUED,
  critical_low_threshold = DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
  replenish_target = DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
  healthy_target = DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
  ideal_target = DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
  snapshot_limit = null,
} = {}) {
  const normalized_critical_low = Math.max(
    0,
    Number(critical_low_threshold) || DEFAULT_FEEDER_BUFFER_CRITICAL_LOW
  );
  const normalized_replenish_target = Math.max(
    normalized_critical_low,
    Number(buffer_target) ||
      Number(replenish_target) ||
      DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET
  );
  const normalized_healthy_target = Math.max(
    normalized_replenish_target,
    Number(healthy_target) || DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET
  );
  const normalized_ideal_target = Math.max(
    normalized_healthy_target,
    Number(ideal_target) || DEFAULT_FEEDER_BUFFER_IDEAL_TARGET
  );
  const effective_snapshot_limit = Math.max(
    Number(snapshot_limit) || 0,
    normalized_healthy_target,
    DEFAULT_FEEDER_BATCH_SIZE,
    250
  );
  const bounded_snapshot_limit = Math.min(effective_snapshot_limit, 500);
  const [queued_response, sending_response, failed_response] = await Promise.all([
    filterAppItems(
      APP_IDS.send_queue,
      { "queue-status": "Queued" },
      {
        limit: bounded_snapshot_limit,
        offset: 0,
        sort_by: "scheduled-for-utc",
        sort_desc: false,
        cache_ttl_ms: 15_000,
      }
    ),
    filterAppItems(
      APP_IDS.send_queue,
      { "queue-status": "Sending" },
      {
        limit: 100,
        offset: 0,
        cache_ttl_ms: 15_000,
      }
    ),
    filterAppItems(
      APP_IDS.send_queue,
      { "queue-status": "Failed" },
      {
        limit: 50,
        offset: 0,
        cache_ttl_ms: 15_000,
      }
    ),
  ]);
  const items = extractItems(queued_response);
  const now_ts = toTimestamp(now) ?? Date.now();

  let queued_future_count = 0;
  let queued_due_now_count = 0;

  for (const item of items) {
    const scheduled_ts = toTimestamp(getQueueScheduledAt(item));
    if (scheduled_ts !== null && scheduled_ts > now_ts) {
      queued_future_count += 1;
      continue;
    }
    queued_due_now_count += 1;
  }

  const queued_inventory_count = getResponseCount(queued_response);
  const sending_count = getResponseCount(sending_response);
  const failed_recent_count = extractItems(failed_response).length;
  const available_inventory_count = queued_inventory_count + sending_count;
  const desired_buffer_target = resolveDesiredBufferTarget({
    available_inventory_count,
    critical_low_threshold: normalized_critical_low,
    replenish_target: normalized_replenish_target,
    healthy_target: normalized_healthy_target,
    ideal_target: normalized_ideal_target,
  });
  const critical_low_threshold_breached =
    available_inventory_count < normalized_critical_low;
  const replenish_threshold_met =
    available_inventory_count >= normalized_replenish_target;
  const healthy_buffer_threshold_met =
    available_inventory_count >= normalized_healthy_target;
  const ideal_buffer_threshold_met =
    available_inventory_count >= normalized_ideal_target;

  return {
    queued_inventory_count,
    available_inventory_count,
    future_inventory_count: queued_future_count,
    due_inventory_count: queued_due_now_count,
    queued_future_count,
    queued_due_now_count,
    sending_count,
    failed_recent_count,
    critical_low_threshold: normalized_critical_low,
    replenish_target: normalized_replenish_target,
    healthy_target: normalized_healthy_target,
    ideal_target: normalized_ideal_target,
    desired_buffer_target,
    critical_low_threshold_breached,
    replenish_threshold_met,
    healthy_buffer_threshold_met,
    ideal_buffer_threshold_met,
    buffer_target: desired_buffer_target,
    buffer_deficit: Math.max(desired_buffer_target - available_inventory_count, 0),
    buffer_satisfied:
      desired_buffer_target > 0 && available_inventory_count >= desired_buffer_target,
    snapshot_limit: bounded_snapshot_limit,
  };
}

export async function runFeederWithRollout(input = {}, deps = {}) {
  const {
    limit,
    scan_limit,
    limit_was_provided,
    scan_limit_was_provided,
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
    inspectQueueBufferImpl = defaultInspectQueueBuffer,
    buildPodioCooldownSkipResultImpl = buildPodioCooldownSkipResult,
    buildPodioBackpressureSkipResultImpl = buildPodioBackpressureSkipResult,
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

  let effective_limit = capFeederBatchImpl(
    limit,
    rollout.feeder_default_batch || DEFAULT_FEEDER_BATCH_SIZE
  );
  let effective_scan_limit = capFeederScanLimitImpl(
    scan_limit,
    rollout.feeder_default_scan_limit || DEFAULT_FEEDER_SCAN_LIMIT
  );
  const effective_master_owner_id = safe_owner_scope.effective_id || null;
  const effective_dry_run = dry_run_resolution.effective_dry_run;
  const rollout_source_view_meta = buildRolloutSourceViewMeta({
    requested_source_view_id: source_view_id,
    requested_source_view_name: source_view_name,
    effective_source_view_id: view_scope.source_view_id,
    effective_source_view_name: view_scope.source_view_name,
    resolved_source_view_id: view_scope.source_view_id,
    resolved_source_view_name: view_scope.source_view_name,
    source_view_fallback_occurred: false,
    source_view_fallback_reason: null,
  });
  const lock_scope = effective_master_owner_id
    ? `feeder:${effective_master_owner_id}`
    : view_scope.source_view_id
      ? `feeder:view:${view_scope.source_view_id}`
      : view_scope.source_view_name
        ? `feeder:view:${view_scope.source_view_name}`
        : "feeder";

  const cooldown_skip = await buildPodioCooldownSkipResultImpl({
    dry_run: effective_dry_run,
    scanned_count: 0,
    raw_items_pulled: 0,
    eligible_owner_count: 0,
    queued_count: 0,
    skipped_count: 0,
    skip_reason_counts: [],
    template_resolution_diagnostics: null,
    results: [],
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
      ...rollout_source_view_meta,
    },
  });

  if (cooldown_skip?.podio_cooldown?.active) {
    route_logger.warn("master_owner_feeder.skipped_podio_cooldown", {
      ...scope_log_meta,
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

  const backpressure_skip = await buildPodioBackpressureSkipResultImpl(
    {
      dry_run: effective_dry_run,
      scanned_count: 0,
      raw_items_pulled: 0,
      eligible_owner_count: 0,
      queued_count: 0,
      skipped_count: 0,
      skip_reason_counts: [],
      template_resolution_diagnostics: null,
      results: [],
    },
    {
      min_remaining: 150,
      max_age_ms: 10 * 60_000,
    }
  );

  if (backpressure_skip?.podio_backpressure?.active) {
    route_logger.warn("master_owner_feeder.skipped_podio_backpressure", {
      ...scope_log_meta,
      reason: backpressure_skip.reason,
      min_remaining:
        backpressure_skip.podio_backpressure?.min_remaining ?? null,
      rate_limit_remaining:
        backpressure_skip.podio_backpressure?.observation?.rate_limit_remaining ??
        null,
      rate_limit_limit:
        backpressure_skip.podio_backpressure?.observation?.rate_limit_limit ??
        null,
      podio_path:
        backpressure_skip.podio_backpressure?.observation?.path ?? null,
      podio_operation:
        backpressure_skip.podio_backpressure?.observation?.operation ?? null,
      observed_at:
        backpressure_skip.podio_backpressure?.observation?.observed_at ?? null,
    });

    return backpressure_skip;
  }

  let queue_inventory = {
    queued_inventory_count: null,
    available_inventory_count: null,
    future_inventory_count: null,
    due_inventory_count: null,
    queued_future_count: null,
    queued_due_now_count: null,
    sending_count: null,
    failed_recent_count: null,
    critical_low_threshold:
      rollout.feeder_buffer_critical_low || DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
    replenish_target:
      rollout.feeder_buffer_replenish_target || DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
    healthy_target:
      rollout.feeder_buffer_healthy_target || DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
    ideal_target:
      rollout.feeder_buffer_ideal_target || DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
    desired_buffer_target:
      rollout.feeder_buffer_ideal_target || DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
    critical_low_threshold_breached: false,
    replenish_threshold_met: false,
    healthy_buffer_threshold_met: false,
    ideal_buffer_threshold_met: false,
    buffer_target:
      rollout.feeder_buffer_ideal_target || DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
    buffer_deficit: null,
    buffer_satisfied: false,
    snapshot_limit: null,
  };

  const should_manage_queue_buffer =
    !effective_dry_run &&
    !test_mode &&
    !clean(seller_id) &&
    !effective_master_owner_id;

  if (should_manage_queue_buffer) {
    queue_inventory = await inspectQueueBufferImpl({
      now: new Date().toISOString(),
      buffer_target:
        rollout.feeder_buffer_replenish_target || DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
      critical_low_threshold:
        rollout.feeder_buffer_critical_low || DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
      replenish_target:
        rollout.feeder_buffer_replenish_target || DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
      healthy_target:
        rollout.feeder_buffer_healthy_target || DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
      ideal_target:
        rollout.feeder_buffer_ideal_target || DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
      snapshot_limit: Math.max(
        rollout.feeder_buffer_healthy_target || DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
        effective_limit
      ),
    });

    route_logger.info("master_owner_feeder.queue_buffer_evaluated", {
      queued_inventory_count: queue_inventory.queued_inventory_count,
      available_inventory_count: queue_inventory.available_inventory_count,
      future_inventory_count: queue_inventory.future_inventory_count,
      due_inventory_count: queue_inventory.due_inventory_count,
      queued_future_count: queue_inventory.queued_future_count,
      queued_due_now_count: queue_inventory.queued_due_now_count,
      sending_count: queue_inventory.sending_count,
      failed_recent_count: queue_inventory.failed_recent_count,
      critical_low_threshold: queue_inventory.critical_low_threshold,
      replenish_target: queue_inventory.replenish_target,
      healthy_target: queue_inventory.healthy_target,
      ideal_target: queue_inventory.ideal_target,
      desired_buffer_target: queue_inventory.desired_buffer_target,
      critical_low_threshold_breached:
        queue_inventory.critical_low_threshold_breached,
      replenish_threshold_met: queue_inventory.replenish_threshold_met,
      healthy_buffer_threshold_met:
        queue_inventory.healthy_buffer_threshold_met,
      ideal_buffer_threshold_met: queue_inventory.ideal_buffer_threshold_met,
      buffer_target: queue_inventory.buffer_target,
      buffer_deficit: queue_inventory.buffer_deficit,
      buffer_satisfied: queue_inventory.buffer_satisfied,
      snapshot_limit: queue_inventory.snapshot_limit,
      limit_was_provided,
      scan_limit_was_provided,
    });

    if (queue_inventory.buffer_satisfied) {
      route_logger.info("master_owner_feeder.skipped_queue_buffer_satisfied", {
        queued_inventory_count: queue_inventory.queued_inventory_count,
        available_inventory_count: queue_inventory.available_inventory_count,
        future_inventory_count: queue_inventory.future_inventory_count,
        due_inventory_count: queue_inventory.due_inventory_count,
        sending_count: queue_inventory.sending_count,
        desired_buffer_target: queue_inventory.desired_buffer_target,
        buffer_target: queue_inventory.buffer_target,
        buffer_deficit: queue_inventory.buffer_deficit,
      });

      return {
        ok: true,
        skipped: true,
        reason: "feeder_queue_buffer_satisfied",
        dry_run: false,
        scanned_count: 0,
        raw_items_pulled: 0,
        eligible_owner_count: 0,
        queued_count: 0,
        skipped_count: 0,
        skip_reason_counts: [],
        template_resolution_diagnostics: null,
        queue_inventory,
        results: [],
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
          ...rollout_source_view_meta,
          queue_inventory,
        },
      };
    }

    if (!limit_was_provided && queue_inventory.buffer_deficit > effective_limit) {
      effective_limit = capFeederBatchImpl(
        queue_inventory.buffer_deficit,
        rollout.feeder_default_batch || DEFAULT_FEEDER_BATCH_SIZE
      );
    }

    if (!scan_limit_was_provided) {
      effective_scan_limit = capFeederScanLimitImpl(
        Math.max(
          effective_scan_limit,
          effective_limit * 10,
          Number(queue_inventory.desired_buffer_target || 0) * 2,
          Number(queue_inventory.buffer_target || 0)
        ),
        rollout.feeder_default_scan_limit || DEFAULT_FEEDER_SCAN_LIMIT
      );
    }
  }

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

    if (result?.reason === "master_owner_feeder_rate_limited") {
      return buildPodioCooldownSkipResultImpl({
        dry_run: effective_dry_run,
        scanned_count: result?.scanned_count ?? 0,
        raw_items_pulled: result?.raw_items_pulled ?? 0,
        eligible_owner_count: result?.eligible_owner_count ?? 0,
        queued_count: result?.queued_count ?? 0,
        skipped_count: result?.skipped_count ?? 0,
        skip_reason_counts: result?.skip_reason_counts ?? [],
        template_resolution_diagnostics:
          result?.template_resolution_diagnostics ?? null,
        results: result?.results ?? [],
        source: result?.source ?? null,
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
          ...buildRolloutSourceViewMeta({
            requested_source_view_id: source_view_id,
            requested_source_view_name: source_view_name,
            effective_source_view_id: view_scope.source_view_id,
            effective_source_view_name: view_scope.source_view_name,
            resolved_source_view_id:
              result?.source?.resolved_view_id ??
              result?.source?.view_id ??
              view_scope.source_view_id,
            resolved_source_view_name:
              result?.source?.resolved_view_name ??
              result?.source?.view_name ??
              view_scope.source_view_name,
            source_view_fallback_occurred:
              result?.source?.fallback_occurred ?? false,
            source_view_fallback_reason:
              result?.source?.fallback_reason ?? null,
          }),
        },
      });
    }

    const resolved_result = {
      ...result,
      queue_inventory,
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
        ...buildRolloutSourceViewMeta({
          requested_source_view_id: source_view_id,
          requested_source_view_name: source_view_name,
          effective_source_view_id: view_scope.source_view_id,
          effective_source_view_name: view_scope.source_view_name,
          resolved_source_view_id:
            result?.source?.resolved_view_id ??
            result?.source?.view_id ??
            view_scope.source_view_id,
          resolved_source_view_name:
            result?.source?.resolved_view_name ??
            result?.source?.view_name ??
            view_scope.source_view_name,
          source_view_fallback_occurred:
            result?.source?.fallback_occurred ?? false,
          source_view_fallback_reason:
            result?.source?.fallback_reason ?? null,
        }),
        queue_inventory,
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
      requested_source_view_name:
        resolved_result?.rollout?.requested_source_view_name ?? null,
      requested_source_view_id:
        resolved_result?.rollout?.requested_source_view_id ?? null,
      effective_source_view_name:
        resolved_result?.rollout?.effective_source_view_name ?? null,
      resolved_source_view_name:
        resolved_result?.rollout?.resolved_source_view_name ?? null,
      resolved_source_view_id:
        resolved_result?.rollout?.resolved_source_view_id ?? null,
      source_view_fallback_occurred:
        resolved_result?.rollout?.source_view_fallback_occurred ?? false,
      source_view_fallback_reason:
        resolved_result?.rollout?.source_view_fallback_reason ?? null,
      scanned_count: resolved_result?.scanned_count ?? 0,
      eligible_owner_count: resolved_result?.eligible_owner_count ?? 0,
      queued_count: resolved_result?.queued_count ?? 0,
      skip_reason_counts: resolved_result?.skip_reason_counts ?? [],
      template_resolution_diagnostics:
        resolved_result?.template_resolution_diagnostics ?? null,
      queued_inventory_count:
        resolved_result?.queue_inventory?.queued_inventory_count ?? null,
      available_inventory_count:
        resolved_result?.queue_inventory?.available_inventory_count ?? null,
      queued_due_now_count:
        resolved_result?.queue_inventory?.queued_due_now_count ?? null,
      queued_future_count:
        resolved_result?.queue_inventory?.queued_future_count ?? null,
      future_inventory_count:
        resolved_result?.queue_inventory?.future_inventory_count ?? null,
      sending_count:
        resolved_result?.queue_inventory?.sending_count ?? null,
      failed_recent_count:
        resolved_result?.queue_inventory?.failed_recent_count ?? null,
      healthy_buffer_threshold_met:
        resolved_result?.queue_inventory?.healthy_buffer_threshold_met ?? null,
      ideal_buffer_threshold_met:
        resolved_result?.queue_inventory?.ideal_buffer_threshold_met ?? null,
      buffer_target:
        resolved_result?.queue_inventory?.buffer_target ?? null,
      desired_buffer_target:
        resolved_result?.queue_inventory?.desired_buffer_target ?? null,
      buffer_deficit:
        resolved_result?.queue_inventory?.buffer_deficit ?? null,
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
