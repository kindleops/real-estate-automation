import ENV from "@/lib/config/env.js";

function clean(value) {
  return String(value ?? "").trim();
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
  return {
    mode: normalizeMode(ENV.ROLLOUT_MODE),
    feeder_max_batch: normalizePositiveInteger(ENV.ROLLOUT_FEEDER_MAX_BATCH, 25),
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
  dry_run = false,
} = {}) {
  const controls = getRolloutControls();
  const enforced_view_id = controls.feeder_view_only_id;
  const enforced_view_name = controls.feeder_view_only_name;

  if (dry_run || (!enforced_view_id && !enforced_view_name)) {
    return {
      ok: true,
      enforced: false,
      source_view_id: clean(requested_view_id) || null,
      source_view_name: clean(requested_view_name) || null,
      reason: dry_run ? "dry_run_view_scope_bypassed" : "no_view_scope_configured",
    };
  }

  const normalized_requested_id = clean(requested_view_id) || null;
  const normalized_requested_name = clean(requested_view_name) || null;

  if (enforced_view_id && normalized_requested_id && normalized_requested_id !== enforced_view_id) {
    return {
      ok: false,
      enforced: true,
      source_view_id: enforced_view_id,
      source_view_name: enforced_view_name,
      reason: "feeder_view_outside_safe_scope",
    };
  }

  if (
    !enforced_view_id &&
    enforced_view_name &&
    normalized_requested_name &&
    normalized_requested_name.toLowerCase() !== enforced_view_name.toLowerCase()
  ) {
    return {
      ok: false,
      enforced: true,
      source_view_id: enforced_view_id,
      source_view_name: enforced_view_name,
      reason: "feeder_view_outside_safe_scope",
    };
  }

  return {
    ok: true,
    enforced: true,
    source_view_id: enforced_view_id || normalized_requested_id,
    source_view_name: enforced_view_name || normalized_requested_name,
    reason: "feeder_view_safe_scope_applied",
  };
}

export function capFeederBatch(limit, fallback = 25) {
  return clampLimit(limit, getRolloutControls().feeder_max_batch, fallback);
}

export function capFeederScanLimit(scan_limit, fallback = 150) {
  const controls = getRolloutControls();
  const scan_cap = Math.max(controls.feeder_max_batch * 6, controls.feeder_max_batch);
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
