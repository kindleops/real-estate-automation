import crypto from "node:crypto";

import APP_IDS from "@/lib/config/app-ids.js";
import { createMessageEvent, updateMessageEvent } from "@/lib/podio/apps/message-events.js";
import {
  getFirstMatchingItem,
  getTextValue,
  isRevisionLimitExceeded,
} from "@/lib/providers/podio.js";

const RUN_LOCK_LOGGER_KEY = "domain.runs.run_locks";

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

function parseJson(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function buildRunLockMessageId(scope = "") {
  return `run-lock:${clean(scope)}`;
}

function buildRunLockTriggerName(scope = "") {
  return `run-lock:${clean(scope)}`;
}

function parseRunLockMeta(item = null) {
  return parseJson(getTextValue(item, "ai-output", ""));
}

async function findRunLockRecord(scope) {
  return getFirstMatchingItem(
    APP_IDS.message_events,
    {
      "message-id": buildRunLockMessageId(scope),
    },
    {
      sort_desc: true,
    }
  );
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

  const record = await findRunLockRecord(normalized_scope);
  const existing_meta = parseRunLockMeta(record);

  if (record?.item_id && isLeaseActive(existing_meta)) {
    return {
      ok: true,
      acquired: false,
      reason: "run_lock_active",
      record_item_id: record.item_id,
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
      record?.item_id && existing_meta?.status === "locked"
        ? "stale_lock_reclaimed"
        : "lock_acquired",
  });

  const fields = {
    "message-id": buildRunLockMessageId(normalized_scope),
    "timestamp": { start: next_meta.last_heartbeat_at },
    "trigger-name": buildRunLockTriggerName(normalized_scope),
    "source-app": "Runtime Lock",
    "message": `Run lock ${normalized_scope} ${next_meta.reason}`,
    "ai-output": JSON.stringify(next_meta),
  };

  if (record?.item_id) {
    try {
      await updateMessageEvent(record.item_id, fields);
    } catch (error) {
      if (!isRevisionLimitExceeded(error)) throw error;
      // Existing lock record has hit the Podio revision limit; create a fresh one.
      // The stale record will be ignored on future reads since the new one will
      // sort higher (sort_desc: true on message-id lookup).
      console.warn(
        JSON.stringify({
          level: "WARN",
          event: "run_lock.acquire_revision_limit_fresh_record",
          meta: {
            module: RUN_LOCK_LOGGER_KEY,
            scope: normalized_scope,
            old_record_item_id: record.item_id,
            failure_bucket: "revision_limit_exceeded",
            message: error?.message || null,
          },
        })
      );
      const fresh = await createMessageEvent(fields);
      return {
        ok: true,
        acquired: true,
        reason: "lock_acquired_fresh_record",
        scope: normalized_scope,
        record_item_id: fresh?.item_id || null,
        lease_token,
        meta: next_meta,
      };
    }
    return {
      ok: true,
      acquired: true,
      reason: next_meta.reason,
      scope: normalized_scope,
      record_item_id: record.item_id,
      lease_token,
      meta: next_meta,
    };
  }

  const created = await createMessageEvent(fields);

  return {
    ok: true,
    acquired: true,
    reason: "lock_acquired",
    scope: normalized_scope,
    record_item_id: created?.item_id || null,
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
  if (!record_item_id) {
    return {
      ok: false,
      released: false,
      reason: "missing_run_lock_record_item_id",
    };
  }

  const next_meta = {
    version: 1,
    scope: clean(scope),
    status: "released",
    lease_token: clean(lease_token) || null,
    outcome: clean(outcome) || null,
    released_at: nowIso(),
    last_error: clean(error?.message || error) || null,
    metadata: metadata && typeof metadata === "object" ? metadata : {},
  };

  const release_fields = {
    "timestamp": { start: next_meta.released_at },
    "trigger-name": buildRunLockTriggerName(scope),
    "source-app": "Runtime Lock",
    "message": `Run lock ${clean(scope)} released (${clean(outcome) || "completed"})`,
    "ai-output": JSON.stringify(next_meta),
  };

  try {
    await updateMessageEvent(record_item_id, release_fields);
  } catch (error) {
    if (!isRevisionLimitExceeded(error)) throw error;
    // The lock record has hit the Podio revision limit. The run completed
    // successfully; we cannot update the record but we do not need to.
    // Log and return gracefully so the caller is not affected.
    console.warn(
      JSON.stringify({
        level: "WARN",
        event: "run_lock.release_revision_limit",
        meta: {
          module: RUN_LOCK_LOGGER_KEY,
          scope: clean(scope),
          record_item_id,
          outcome: clean(outcome) || null,
          failure_bucket: "revision_limit_exceeded",
          message: error?.message || null,
        },
      })
    );
    return {
      ok: true,
      released: true,
      reason: "run_lock_released_revision_limit",
      record_item_id,
      scope: clean(scope),
      outcome: clean(outcome) || null,
    };
  }

  return {
    ok: true,
    released: true,
    reason: "run_lock_released",
    record_item_id,
    scope: clean(scope),
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

export default {
  acquireRunLock,
  releaseRunLock,
  withRunLock,
};
