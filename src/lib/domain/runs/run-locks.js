import crypto from "node:crypto";

import {
  buildRuntimeStateRecordId,
  createRuntimeStateIfAbsent,
  readRuntimeState,
  writeRuntimeState,
} from "@/lib/domain/runtime/runtime-state-store.js";
import { warn } from "@/lib/logging/logger.js";

const RUN_LOCK_LOGGER_KEY = "domain.runs.run_locks";
const RUN_LOCK_NAMESPACE = "run-locks";

const defaultDeps = {
  createRuntimeStateIfAbsent,
  readRuntimeState,
  writeRuntimeState,
  warn,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function buildRunLockRecordId(scope = "") {
  return buildRuntimeStateRecordId(RUN_LOCK_NAMESPACE, clean(scope));
}

async function findRunLockState(scope) {
  return runtimeDeps.readRuntimeState({
    namespace: RUN_LOCK_NAMESPACE,
    key: clean(scope),
  });
}

function isLeaseActive(meta = {}, at = Date.now()) {
  if (clean(meta?.status).toLowerCase() !== "locked") return false;
  const expires_at_ts = toTimestamp(meta?.expires_at);
  return expires_at_ts !== null && expires_at_ts > at;
}

function buildLockPayload({
  scope,
  lease_token,
  owner = null,
  lease_ms,
  metadata = {},
  existing_meta = {},
  state = "locked",
  reason = null,
  outcome = null,
  error = null,
} = {}) {
  const timestamp = nowIso();
  const expires_at = new Date(Date.now() + Math.max(Number(lease_ms) || 0, 1)).toISOString();

  return {
    version: 1,
    scope: clean(scope),
    status: clean(state) || "locked",
    lease_token: clean(lease_token) || null,
    owner: clean(owner) || null,
    lease_ms: Math.max(Number(lease_ms) || 0, 1),
    started_at: existing_meta?.started_at || timestamp,
    acquired_at: existing_meta?.acquired_at || timestamp,
    last_heartbeat_at: timestamp,
    expires_at,
    released_at:
      state === "released"
        ? timestamp
        : existing_meta?.released_at || null,
    reason: clean(reason) || null,
    outcome: clean(outcome) || null,
    last_error:
      clean(error?.message || error) ||
      existing_meta?.last_error ||
      null,
    acquisition_count: Number(existing_meta?.acquisition_count || 0) + 1,
    metadata: metadata && typeof metadata === "object" ? metadata : {},
  };
}

export function __setRunLockTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetRunLockTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function acquireRunLock({
  scope,
  lease_ms = 10 * 60_000,
  owner = null,
  metadata = {},
} = {}) {
  const normalized_scope = clean(scope);
  if (!normalized_scope) {
    return {
      ok: false,
      acquired: false,
      reason: "missing_run_lock_scope",
    };
  }

  const record_item_id = buildRunLockRecordId(normalized_scope);
  const existing_meta = await findRunLockState(normalized_scope);

  if (isLeaseActive(existing_meta)) {
    return {
      ok: true,
      acquired: false,
      reason: "run_lock_active",
      record_item_id,
      scope: normalized_scope,
      meta: existing_meta,
    };
  }

  const lease_token = crypto.randomUUID();
  const next_meta = buildLockPayload({
    scope: normalized_scope,
    lease_token,
    owner,
    lease_ms,
    metadata,
    existing_meta,
    state: "locked",
    reason:
      existing_meta?.status === "locked"
        ? "stale_lock_reclaimed"
        : "lock_acquired",
  });

  if (!existing_meta) {
    const created = await runtimeDeps.createRuntimeStateIfAbsent({
      namespace: RUN_LOCK_NAMESPACE,
      key: normalized_scope,
      state: next_meta,
    });

    if (created?.created) {
      return {
        ok: true,
        acquired: true,
        reason: "lock_acquired",
        scope: normalized_scope,
        record_item_id,
        lease_token,
        meta: next_meta,
      };
    }

    const live_state = created?.state || (await findRunLockState(normalized_scope));
    if (isLeaseActive(live_state)) {
      return {
        ok: true,
        acquired: false,
        reason: "run_lock_active",
        record_item_id,
        scope: normalized_scope,
        meta: live_state,
      };
    }
  }

  await runtimeDeps.writeRuntimeState({
    namespace: RUN_LOCK_NAMESPACE,
    key: normalized_scope,
    state: next_meta,
  });

  const confirmed_meta = await findRunLockState(normalized_scope);
  if (clean(confirmed_meta?.lease_token) !== lease_token) {
    runtimeDeps.warn("run_lock.acquire_race_lost", {
      module: RUN_LOCK_LOGGER_KEY,
      scope: normalized_scope,
      record_item_id,
      existing_lease_token: clean(confirmed_meta?.lease_token) || null,
    });

    return {
      ok: true,
      acquired: false,
      reason: "run_lock_race_lost",
      scope: normalized_scope,
      record_item_id,
      meta: confirmed_meta,
    };
  }

  return {
    ok: true,
    acquired: true,
    reason: next_meta.reason,
    scope: normalized_scope,
    record_item_id,
    lease_token,
    meta: next_meta,
  };
}

export async function releaseRunLock({
  scope,
  record_item_id = null,
  lease_token = null,
  outcome = "completed",
  metadata = {},
  error = null,
} = {}) {
  const normalized_scope =
    clean(scope) ||
    clean(record_item_id).replace(/^run-locks:/, "");

  if (!normalized_scope) {
    return {
      ok: false,
      released: false,
      reason: "missing_run_lock_record_item_id",
    };
  }

  const existing_meta = (await findRunLockState(normalized_scope)) || {};
  const next_meta = {
    ...existing_meta,
    version: 1,
    scope: normalized_scope,
    status: "released",
    lease_token: clean(lease_token) || clean(existing_meta?.lease_token) || null,
    outcome: clean(outcome) || null,
    released_at: nowIso(),
    last_error: clean(error?.message || error) || null,
    metadata: metadata && typeof metadata === "object" ? metadata : {},
  };

  await runtimeDeps.writeRuntimeState({
    namespace: RUN_LOCK_NAMESPACE,
    key: normalized_scope,
    state: next_meta,
  });

  return {
    ok: true,
    released: true,
    reason: "run_lock_released",
    record_item_id: buildRunLockRecordId(normalized_scope),
    scope: normalized_scope,
    outcome: clean(outcome) || null,
  };
}

export async function withRunLock({
  scope,
  enabled = true,
  lease_ms = 10 * 60_000,
  owner = null,
  metadata = {},
  onLocked = null,
  fn,
} = {}) {
  if (typeof fn !== "function") {
    throw new Error("withRunLock requires fn");
  }

  if (!enabled) {
    return fn({
      lock: null,
      refresh: async () => ({ ok: true, skipped: true, reason: "run_lock_disabled" }),
    });
  }

  const lock = await acquireRunLock({
    scope,
    lease_ms,
    owner,
    metadata,
  });

  if (!lock.ok || !lock.acquired) {
    if (typeof onLocked === "function") {
      return onLocked(lock);
    }

    return {
      ok: true,
      skipped: true,
      reason: lock?.reason || "run_lock_not_acquired",
      lock,
    };
  }

  try {
    const result = await fn({
      lock,
      refresh: async () => ({
        ok: true,
        skipped: true,
        reason: "run_lock_refresh_not_implemented",
      }),
    });

    await releaseRunLock({
      scope,
      record_item_id: lock.record_item_id,
      lease_token: lock.lease_token,
      outcome: result?.ok === false ? "completed_with_errors" : "completed",
      metadata: {
        result_reason: clean(result?.reason) || null,
        processed_count: Number(result?.processed_count || 0) || 0,
      },
    });

    return result;
  } catch (error) {
    await releaseRunLock({
      scope,
      record_item_id: lock.record_item_id,
      lease_token: lock.lease_token,
      outcome: "failed",
      metadata,
      error,
    });
    throw error;
  }
}

export async function forceReleaseStaleLock({
  scope,
  reason = "force_released_stale",
} = {}, _deps = {}) {
  const read_state = _deps.readRuntimeState || runtimeDeps.readRuntimeState;
  const write_state = _deps.writeRuntimeState || runtimeDeps.writeRuntimeState;

  const normalized_scope = clean(scope);
  if (!normalized_scope) {
    return {
      ok: false,
      released: false,
      reason: "missing_run_lock_scope",
    };
  }

  const record = await read_state({
    namespace: RUN_LOCK_NAMESPACE,
    key: normalized_scope,
  });
  if (!record) {
    return {
      ok: true,
      released: false,
      reason: "no_lock_record_found",
      scope: normalized_scope,
    };
  }

  const was_active = isLeaseActive(record);
  const forced_meta = {
    ...record,
    status: "released",
    released_at: nowIso(),
    outcome: reason,
    last_error: `Force-released: ${reason}`,
  };

  await write_state({
    namespace: RUN_LOCK_NAMESPACE,
    key: normalized_scope,
    state: forced_meta,
  });

  return {
    ok: true,
    released: true,
    reason,
    scope: normalized_scope,
    record_item_id: buildRunLockRecordId(normalized_scope),
    was_active,
    previous_expires_at: record?.expires_at || null,
    previous_owner: record?.owner || null,
    previous_acquired_at: record?.acquired_at || null,
  };
}

export default {
  __setRunLockTestDeps,
  __resetRunLockTestDeps,
  acquireRunLock,
  releaseRunLock,
  withRunLock,
  forceReleaseStaleLock,
};
