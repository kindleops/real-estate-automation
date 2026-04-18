import ENV from "@/lib/config/env.js";

export const DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME = "SMS / TIER #1 / ALL";
export const DEFAULT_FEEDER_BATCH_SIZE = 500;
export const DEFAULT_FEEDER_SCAN_LIMIT = 5000;
export const DEFAULT_FEEDER_BUFFER_CRITICAL_LOW = 250;
export const DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET = 750;
export const DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET = 1500;
export const DEFAULT_FEEDER_BUFFER_IDEAL_TARGET = 2000;
export const DEFAULT_FEEDER_BUFFER_MIN_QUEUED = DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET;
export const DEFAULT_FEEDER_EVALUATION_LOCK_HOURS = 4;

const FEEDER_SOURCE_VIEW_SAFE_NAME_PATTERNS = Object.freeze([
  /^SMS \/ TIER #1 \/ ALL$/i,
  /^SMS \/ TIER #1 \/ FILE #\d+$/i,
]);

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function normalizeMode(value = "") {
  const normalized = clean(value).toLowerCase();
  return normalized === "live" ? "live" : "beta";
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function clampLimit(requested, cap, fallback) {
  const normalized_fallback = normalizePositiveInteger(fallback, 1);
  const normalized_cap = normalizePositiveInteger(cap, normalized_fallback);
  const normalized_requested = normalizePositiveInteger(requested, normalized_fallback);

  return Math.min(normalized_requested, normalized_cap);
}

export function getRolloutControls() {
  const feeder_buffer_critical_low = normalizePositiveInteger(
    ENV.ROLLOUT_FEEDER_BUFFER_CRITICAL_LOW,
    DEFAULT_FEEDER_BUFFER_CRITICAL_LOW
  );
  const feeder_buffer_replenish_target = normalizePositiveInteger(
    ENV.ROLLOUT_FEEDER_BUFFER_REPLENISH_TARGET,
    normalizePositiveInteger(
      ENV.ROLLOUT_FEEDER_BUFFER_MIN_QUEUED,
      DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET
    )
  );
  const feeder_buffer_healthy_target = Math.max(
    feeder_buffer_replenish_target,
    normalizePositiveInteger(
      ENV.ROLLOUT_FEEDER_BUFFER_HEALTHY_TARGET,
      DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET
    )
  );
  const feeder_buffer_ideal_target = Math.max(
    feeder_buffer_healthy_target,
    normalizePositiveInteger(
      ENV.ROLLOUT_FEEDER_BUFFER_IDEAL_TARGET,
      DEFAULT_FEEDER_BUFFER_IDEAL_TARGET
    )
  );

  return {
    mode: normalizeMode(ENV.ROLLOUT_MODE),
    feeder_default_batch: normalizePositiveInteger(
      ENV.ROLLOUT_FEEDER_DEFAULT_BATCH,
      DEFAULT_FEEDER_BATCH_SIZE
    ),
    feeder_default_scan_limit: normalizePositiveInteger(
      ENV.ROLLOUT_FEEDER_DEFAULT_SCAN_LIMIT,
      DEFAULT_FEEDER_SCAN_LIMIT
    ),
    feeder_buffer_min_queued: feeder_buffer_replenish_target,
    feeder_buffer_critical_low,
    feeder_buffer_replenish_target,
    feeder_buffer_healthy_target,
    feeder_buffer_ideal_target,
    feeder_max_batch: normalizePositiveInteger(ENV.ROLLOUT_FEEDER_MAX_BATCH, 1000),
    feeder_evaluation_lock_hours: normalizePositiveInteger(
      ENV.ROLLOUT_FEEDER_EVALUATION_LOCK_HOURS,
      DEFAULT_FEEDER_EVALUATION_LOCK_HOURS
    ),
    queue_max_batch: normalizePositiveInteger(ENV.ROLLOUT_QUEUE_MAX_BATCH, 50),
    retry_max_batch: normalizePositiveInteger(ENV.ROLLOUT_RETRY_MAX_BATCH, 50),
    reconcile_max_batch: normalizePositiveInteger(ENV.ROLLOUT_RECONCILE_MAX_BATCH, 50),
    autopilot_max_scan: normalizePositiveInteger(ENV.ROLLOUT_AUTOPILOT_MAX_SCAN, 25),
    buyer_blast_max_recipients: normalizePositiveInteger(
      ENV.ROLLOUT_BUYER_BLAST_MAX_RECIPIENTS,
      5
    ),
    feeder_view_only_id: clean(ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID) || null,
    feeder_view_only_name: clean(ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME) || null,
    single_master_owner_id:
      normalizePositiveInteger(ENV.ROLLOUT_SINGLE_MASTER_OWNER_ID, null) || null,
    single_contract_id:
      normalizePositiveInteger(ENV.ROLLOUT_SINGLE_CONTRACT_ID, null) || null,
    single_buyer_match_id:
      normalizePositiveInteger(ENV.ROLLOUT_SINGLE_BUYER_MATCH_ID, null) || null,
  };
}

export function isLiveRolloutMode() {
  return getRolloutControls().mode === "live";
}

export function resolveMutationDryRun({
  requested_dry_run = false,
  live_required = true,
} = {}) {
  const controls = getRolloutControls();
  const requested = Boolean(requested_dry_run);
  const forced = Boolean(live_required) && controls.mode !== "live";

  return {
    requested,
    effective_dry_run: forced ? true : requested,
    forced,
    mode: controls.mode,
    reason: forced ? "rollout_beta_mode_forced_dry_run" : requested ? "requested_dry_run" : "live_mode",
  };
}

export function resolveScopedId({
  requested_id = null,
  safe_id = null,
  resource = "resource",
  allow_auto_fill = true,
} = {}) {
  const requested = normalizePositiveInteger(requested_id, null);
  const allowed = normalizePositiveInteger(safe_id, null);

  if (!allowed) {
    return {
      ok: true,
      enforced: false,
      requested_id: requested,
      effective_id: requested,
      reason: "no_safe_scope_configured",
      resource,
    };
  }

  if (!requested && allow_auto_fill) {
    return {
      ok: true,
      enforced: true,
      requested_id: null,
      effective_id: allowed,
      reason: "safe_scope_auto_applied",
      resource,
    };
  }

  if (requested && requested !== allowed) {
    return {
      ok: false,
      enforced: true,
      requested_id: requested,
      effective_id: allowed,
      reason: `${resource}_outside_safe_scope`,
      resource,
    };
  }

  return {
    ok: true,
    enforced: true,
    requested_id: requested,
    effective_id: allowed,
    reason: requested ? "safe_scope_confirmed" : "safe_scope_required",
    resource,
  };
}

export function resolveFeederViewScope({
  requested_view_id = null,
  requested_view_name = null,
} = {}) {
  const controls = getRolloutControls();
  const normalized_requested_id = clean(requested_view_id) || null;
  const normalized_requested_name = clean(requested_view_name) || null;
  const enforced_view_id = clean(controls.feeder_view_only_id) || null;
  const enforced_view_name = clean(controls.feeder_view_only_name) || null;
  const enforced = Boolean(enforced_view_id || enforced_view_name);

  if (!normalized_requested_id && !normalized_requested_name) {
    return {
      ok: true,
      enforced,
      safe_scope_passed: true,
      source_view_id: null,
      source_view_name: DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
      requested_view_id: null,
      requested_view_name: null,
      defaulted: true,
      reason: "feeder_view_default_applied",
    };
  }

  const resolved_view_name = normalized_requested_name || null;
  const matches_safe_pattern = FEEDER_SOURCE_VIEW_SAFE_NAME_PATTERNS.some((pattern) =>
    pattern.test(resolved_view_name)
  );
  const matches_configured_safe_name =
    Boolean(enforced_view_name) &&
    lower(resolved_view_name) === lower(enforced_view_name);

  if (normalized_requested_id && !normalized_requested_name) {
    if (enforced_view_id && normalized_requested_id === enforced_view_id) {
      return {
        ok: true,
        enforced,
        safe_scope_passed: true,
        source_view_id: enforced_view_id,
        source_view_name: enforced_view_name || null,
        requested_view_id: normalized_requested_id,
        requested_view_name: null,
        defaulted: false,
        reason: "feeder_view_safe_scope_applied",
      };
    }

    return {
      ok: false,
      enforced,
      safe_scope_passed: false,
      source_view_id: null,
      source_view_name: null,
      requested_view_id: normalized_requested_id,
      requested_view_name: null,
      defaulted: false,
      reason: "feeder_view_outside_safe_scope",
    };
  }

  if (!matches_safe_pattern && !matches_configured_safe_name) {
    return {
      ok: false,
      enforced,
      safe_scope_passed: false,
      source_view_id: null,
      source_view_name: null,
      requested_view_id: normalized_requested_id,
      requested_view_name: normalized_requested_name,
      defaulted: false,
      reason: "feeder_view_outside_safe_scope",
    };
  }

  return {
    ok: true,
    enforced,
    safe_scope_passed: true,
    source_view_id: matches_configured_safe_name ? enforced_view_id : null,
    source_view_name: resolved_view_name,
    requested_view_id: normalized_requested_id,
    requested_view_name: normalized_requested_name,
    defaulted: false,
    reason: "feeder_view_safe_scope_applied",
  };
}

export function capFeederBatch(
  limit,
  fallback = getRolloutControls().feeder_default_batch
) {
  return clampLimit(limit, getRolloutControls().feeder_max_batch, fallback);
}

export function capFeederScanLimit(
  scan_limit,
  fallback = getRolloutControls().feeder_default_scan_limit
) {
  const controls = getRolloutControls();
  // Cap at feeder_max_batch * 10 to stay within Vercel function timeout.
  // Buffer targets are aspirational — scanning thousands of owners in a single
  // 300s invocation is not feasible.  The cron runs every 8 minutes so the
  // buffer fills incrementally across invocations.
  const scan_cap = Math.max(
    controls.feeder_default_scan_limit,
    controls.feeder_max_batch * 10
  );
  return clampLimit(scan_limit, scan_cap, fallback);
}

export function capQueueBatch(limit, fallback = 50) {
  return clampLimit(limit, getRolloutControls().queue_max_batch, fallback);
}

export function capRetryBatch(limit, fallback = 50) {
  return clampLimit(limit, getRolloutControls().retry_max_batch, fallback);
}

export function capReconcileBatch(limit, fallback = 50) {
  return clampLimit(limit, getRolloutControls().reconcile_max_batch, fallback);
}

export function capAutopilotScan(scan_limit, fallback = 25) {
  return clampLimit(scan_limit, getRolloutControls().autopilot_max_scan, fallback);
}

export function capBuyerBlastRecipients(max_buyers, fallback = 5) {
  return clampLimit(
    max_buyers,
    getRolloutControls().buyer_blast_max_recipients,
    fallback
  );
}

export default {
  DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
  DEFAULT_FEEDER_BATCH_SIZE,
  DEFAULT_FEEDER_SCAN_LIMIT,
  DEFAULT_FEEDER_BUFFER_MIN_QUEUED,
  DEFAULT_FEEDER_EVALUATION_LOCK_HOURS,
  getRolloutControls,
  isLiveRolloutMode,
  resolveMutationDryRun,
  resolveScopedId,
  resolveFeederViewScope,
  capFeederBatch,
  capFeederScanLimit,
  capQueueBatch,
  capRetryBatch,
  capReconcileBatch,
  capAutopilotScan,
  capBuyerBlastRecipients,
};
