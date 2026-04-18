import crypto from "node:crypto";

import {
  buildRuntimeStateRecordId,
  createRuntimeStateIfAbsent,
  parseRuntimeStateRecordId,
  readRuntimeState,
  writeRuntimeState,
} from "@/lib/domain/runtime/runtime-state-store.js";

const IDEMPOTENCY_NAMESPACE = "idempotency";

const defaultDeps = {
  createRuntimeStateIfAbsent,
  readRuntimeState,
  writeRuntimeState,
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
  const timestamp = new Date(value).getTime();
  return Number.isNaN(timestamp) ? null : timestamp;
}

function buildLedgerStateKey(scope, key) {
  return `${clean(scope)}:${clean(key)}`;
}

function buildLedgerRecordId(scope, key) {
  return buildRuntimeStateRecordId(
    IDEMPOTENCY_NAMESPACE,
    buildLedgerStateKey(scope, key)
  );
}

function parseLedgerRecordId(record_item_id = "") {
  const parsed = parseRuntimeStateRecordId(record_item_id);
  const composite_key = clean(parsed?.key);
  const separator_index = composite_key.indexOf(":");

  if (separator_index === -1) {
    return {
      scope: null,
      key: composite_key || null,
    };
  }

  return {
    scope: composite_key.slice(0, separator_index) || null,
    key: composite_key.slice(separator_index + 1) || null,
  };
}

async function findLedgerRecord(scope, key) {
  return runtimeDeps.readRuntimeState({
    namespace: IDEMPOTENCY_NAMESPACE,
    key: buildLedgerStateKey(scope, key),
  });
}

function isProcessingLeaseStale(meta = {}, lease_ms) {
  const started_at_ts = toTimestamp(meta.started_at);
  if (started_at_ts === null) return true;
  return Date.now() - started_at_ts > lease_ms;
}

export function __setIdempotencyLedgerTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetIdempotencyLedgerTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export function hashIdempotencyPayload(value) {
  return crypto
    .createHash("sha256")
    .update(typeof value === "string" ? value : JSON.stringify(value), "utf8")
    .digest("hex");
}

export async function beginIdempotentProcessing({
  scope,
  key,
  summary = "",
  metadata = {},
  lease_ms = 10 * 60_000,
} = {}) {
  const normalized_scope = clean(scope);
  const normalized_key = clean(key);

  if (!normalized_scope || !normalized_key) {
    return {
      ok: false,
      duplicate: false,
      reason: "missing_idempotency_scope_or_key",
      record_item_id: null,
      key: normalized_key || null,
    };
  }

  const record_item_id = buildLedgerRecordId(normalized_scope, normalized_key);
  const existing = await findLedgerRecord(normalized_scope, normalized_key);
  const started_at = nowIso();
  const claim_token = crypto.randomUUID();
  const next_meta = {
    ...(existing || {}),
    ...metadata,
    scope: normalized_scope,
    key: normalized_key,
    summary: clean(summary) || null,
    status: "processing",
    started_at,
    completed_at: null,
    failed_at: null,
    last_error: null,
    attempts: Number(existing?.attempts || 0) + 1,
    claim_token,
  };

  if (existing?.status === "completed") {
    return {
      ok: true,
      duplicate: true,
      reason: "duplicate_event_ignored",
      record_item_id,
      key: normalized_key,
      scope: normalized_scope,
      meta: existing,
    };
  }

  if (
    existing?.status === "processing" &&
    !isProcessingLeaseStale(existing, lease_ms)
  ) {
    return {
      ok: true,
      duplicate: true,
      reason: "event_already_processing",
      record_item_id,
      key: normalized_key,
      scope: normalized_scope,
      meta: existing,
    };
  }

  if (!existing) {
    const created = await runtimeDeps.createRuntimeStateIfAbsent({
      namespace: IDEMPOTENCY_NAMESPACE,
      key: buildLedgerStateKey(normalized_scope, normalized_key),
      state: next_meta,
    });

    if (created?.created) {
      return {
        ok: true,
        duplicate: false,
        reason: "event_claimed",
        record_item_id,
        key: normalized_key,
        scope: normalized_scope,
      };
    }

    const live_state = created?.state || (await findLedgerRecord(normalized_scope, normalized_key));
    if (live_state?.status === "completed") {
      return {
        ok: true,
        duplicate: true,
        reason: "duplicate_event_ignored",
        record_item_id,
        key: normalized_key,
        scope: normalized_scope,
        meta: live_state,
      };
    }

    if (
      live_state?.status === "processing" &&
      !isProcessingLeaseStale(live_state, lease_ms)
    ) {
      return {
        ok: true,
        duplicate: true,
        reason: "event_already_processing",
        record_item_id,
        key: normalized_key,
        scope: normalized_scope,
        meta: live_state,
      };
    }
  }

  await runtimeDeps.writeRuntimeState({
    namespace: IDEMPOTENCY_NAMESPACE,
    key: buildLedgerStateKey(normalized_scope, normalized_key),
    state: next_meta,
  });

  const confirmed_meta = await findLedgerRecord(normalized_scope, normalized_key);
  if (clean(confirmed_meta?.claim_token) !== claim_token) {
    return {
      ok: true,
      duplicate: true,
      reason: "event_already_processing",
      record_item_id,
      key: normalized_key,
      scope: normalized_scope,
      meta: confirmed_meta,
    };
  }

  return {
    ok: true,
    duplicate: false,
    reason: existing ? "stale_or_failed_event_reclaimed" : "event_claimed",
    record_item_id,
    key: normalized_key,
    scope: normalized_scope,
    meta: next_meta,
  };
}

export async function completeIdempotentProcessing({
  record_item_id = null,
  scope = null,
  key = null,
  summary = "",
  metadata = {},
  skip_content_fields = false,
} = {}) {
  if (!record_item_id) {
    return {
      ok: false,
      reason: "missing_record_item_id",
    };
  }

  const parsed_record = parseLedgerRecordId(record_item_id);
  const resolved_scope = clean(scope) || clean(parsed_record.scope);
  const resolved_key = clean(key) || clean(parsed_record.key);
  const completed_at = nowIso();
  const existing = await findLedgerRecord(resolved_scope, resolved_key);
  const processing_meta = {
    ...(existing || {}),
    ...metadata,
    scope: resolved_scope,
    key: resolved_key,
    summary: clean(summary) || null,
    status: "completed",
    completed_at,
    skip_content_fields: Boolean(skip_content_fields),
  };
  delete processing_meta.claim_token;

  await runtimeDeps.writeRuntimeState({
    namespace: IDEMPOTENCY_NAMESPACE,
    key: buildLedgerStateKey(resolved_scope, resolved_key),
    state: processing_meta,
  });

  return {
    ok: true,
    reason: "idempotency_record_completed",
    record_item_id,
  };
}

export async function failIdempotentProcessing({
  record_item_id = null,
  scope = null,
  key = null,
  error = null,
  metadata = {},
  skip_content_fields = false,
} = {}) {
  if (!record_item_id) {
    return {
      ok: false,
      reason: "missing_record_item_id",
    };
  }

  const parsed_record = parseLedgerRecordId(record_item_id);
  const resolved_scope = clean(scope) || clean(parsed_record.scope);
  const resolved_key = clean(key) || clean(parsed_record.key);
  const failed_at = nowIso();
  const error_message =
    clean(error?.message) ||
    clean(error) ||
    "unknown_error";
  const existing = await findLedgerRecord(resolved_scope, resolved_key);
  const processing_meta = {
    ...(existing || {}),
    ...metadata,
    scope: resolved_scope,
    key: resolved_key,
    status: "failed",
    failed_at,
    last_error: error_message,
    skip_content_fields: Boolean(skip_content_fields),
  };
  delete processing_meta.claim_token;

  await runtimeDeps.writeRuntimeState({
    namespace: IDEMPOTENCY_NAMESPACE,
    key: buildLedgerStateKey(resolved_scope, resolved_key),
    state: processing_meta,
  });

  return {
    ok: true,
    reason: "idempotency_record_failed",
    record_item_id,
    error_message,
  };
}

export default {
  __setIdempotencyLedgerTestDeps,
  __resetIdempotencyLedgerTestDeps,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
};
