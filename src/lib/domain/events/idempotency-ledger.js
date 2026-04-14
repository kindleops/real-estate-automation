import crypto from "node:crypto";

import { createMessageEvent, updateMessageEvent } from "@/lib/podio/apps/message-events.js";
import { getFirstMatchingItem, getTextValue } from "@/lib/providers/podio.js";
import APP_IDS from "@/lib/config/app-ids.js";

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

function parseJson(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function buildRecordMessageId(scope, key) {
  return `idempotency:${clean(scope)}:${clean(key)}`;
}

function buildTriggerName(scope) {
  return `idempotency:${clean(scope)}`;
}

function buildRecordMessage(scope, key, summary = "") {
  return clean(summary) || `Processed ${clean(scope)} webhook event ${clean(key)}`;
}

function parseLedgerMeta(item = null) {
  return parseJson(getTextValue(item, "ai-output", ""));
}

async function findLedgerRecord(scope, key) {
  return getFirstMatchingItem(
    APP_IDS.message_events,
    {
      "message-id": buildRecordMessageId(scope, key),
    },
    {
      sort_desc: true,
    }
  );
}

function isProcessingLeaseStale(meta = {}, lease_ms) {
  const started_at_ts = toTimestamp(meta.started_at);
  if (started_at_ts === null) return true;
  return Date.now() - started_at_ts > lease_ms;
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

  const existing = await findLedgerRecord(normalized_scope, normalized_key);
  const started_at = nowIso();

  if (existing?.item_id) {
    const existing_meta = parseLedgerMeta(existing);

    if (existing_meta.status === "completed") {
      return {
        ok: true,
        duplicate: true,
        reason: "duplicate_event_ignored",
        record_item_id: existing.item_id,
        key: normalized_key,
        scope: normalized_scope,
        meta: existing_meta,
      };
    }

    if (
      existing_meta.status === "processing" &&
      !isProcessingLeaseStale(existing_meta, lease_ms)
    ) {
      return {
        ok: true,
        duplicate: true,
        reason: "event_already_processing",
        record_item_id: existing.item_id,
        key: normalized_key,
        scope: normalized_scope,
        meta: existing_meta,
      };
    }

    const next_meta = {
      ...existing_meta,
      ...metadata,
      scope: normalized_scope,
      key: normalized_key,
      status: "processing",
      started_at,
      completed_at: null,
      last_error: null,
      attempts: Number(existing_meta.attempts || 0) + 1,
    };

    await updateMessageEvent(existing.item_id, {
      "timestamp": { start: started_at },
      "trigger-name": buildTriggerName(normalized_scope),
      "source-app": "External API",
      "message": buildRecordMessage(normalized_scope, normalized_key, summary),
      "ai-output": JSON.stringify(next_meta),
    });

    return {
      ok: true,
      duplicate: false,
      reason: "stale_or_failed_event_reclaimed",
      record_item_id: existing.item_id,
      key: normalized_key,
      scope: normalized_scope,
      meta: next_meta,
    };
  }

  const created = await createMessageEvent({
    "message-id": buildRecordMessageId(normalized_scope, normalized_key),
    "timestamp": { start: started_at },
    "trigger-name": buildTriggerName(normalized_scope),
    "source-app": "External API",
    "message": buildRecordMessage(normalized_scope, normalized_key, summary),
    "ai-output": JSON.stringify({
      ...metadata,
      scope: normalized_scope,
      key: normalized_key,
      status: "processing",
      started_at,
      completed_at: null,
      last_error: null,
      attempts: 1,
    }),
  });

  return {
    ok: true,
    duplicate: false,
    reason: "event_claimed",
    record_item_id: created?.item_id || null,
    key: normalized_key,
    scope: normalized_scope,
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

  const completed_at = nowIso();
  const processing_meta = JSON.stringify({
    ...metadata,
    scope: clean(scope),
    key: clean(key),
    status: "completed",
    completed_at,
  });

  const fields = {
    "timestamp": { start: completed_at },
    "trigger-name": buildTriggerName(scope),
    "source-app": "External API",
  };

  if (!skip_content_fields) {
    fields["message"] = buildRecordMessage(scope, key, summary);
    fields["ai-output"] = processing_meta;
  }

  await updateMessageEvent(record_item_id, fields);

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

  const failed_at = nowIso();
  const error_message =
    clean(error?.message) ||
    clean(error) ||
    "unknown_error";

  const processing_meta = JSON.stringify({
    ...metadata,
    scope: clean(scope),
    key: clean(key),
    status: "failed",
    failed_at,
    last_error: error_message,
  });

  const fields = {
    "timestamp": { start: failed_at },
    "trigger-name": buildTriggerName(scope),
    "source-app": "External API",
  };

  if (!skip_content_fields) {
    fields["message"] = buildRecordMessage(scope, key, `Failed ${clean(scope)} event ${clean(key)}`);
    fields["ai-output"] = processing_meta;
  }

  await updateMessageEvent(record_item_id, fields);

  return {
    ok: true,
    reason: "idempotency_record_failed",
    record_item_id,
    error_message,
  };
}

export default {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
};
