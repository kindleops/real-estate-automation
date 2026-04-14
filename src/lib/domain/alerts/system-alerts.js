import crypto from "node:crypto";

import APP_IDS from "@/lib/config/app-ids.js";
import { deliverSystemAlert } from "@/lib/domain/alerts/alert-delivery.js";
import { child } from "@/lib/logging/logger.js";
import {
  createMessageEvent,
  getMessageEvent,
  updateMessageEvent,
  findMessageEvents,
} from "@/lib/podio/apps/message-events.js";
import { getFirstMatchingItem, getTextValue, isRevisionLimitExceeded } from "@/lib/providers/podio.js";

const logger = child({
  module: "domain.alerts.system_alerts",
});

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

function uniq(values = []) {
  return [...new Set(values.map((value) => clean(value)).filter(Boolean))];
}

function severityRank(severity = "") {
  const normalized = clean(severity).toLowerCase();
  if (normalized === "critical") return 4;
  if (normalized === "high") return 3;
  if (normalized === "warning") return 2;
  if (normalized === "info") return 1;
  return 0;
}

function normalizeSeverity(left = "", right = "") {
  return severityRank(left) >= severityRank(right) ? clean(left) || "warning" : clean(right) || "warning";
}

function buildAlertSignature({
  subsystem = "",
  code = "",
  dedupe_key = null,
} = {}) {
  const explicit = clean(dedupe_key);
  if (explicit) return explicit;

  return crypto
    .createHash("sha256")
    .update(`${clean(subsystem)}:${clean(code)}`, "utf8")
    .digest("hex")
    .slice(0, 16);
}

function buildAlertMessageId({ subsystem = "", signature = "" } = {}) {
  return `system-alert:${clean(subsystem)}:${clean(signature)}`;
}

function buildAlertTriggerName(subsystem = "") {
  return `system-alert:${clean(subsystem)}`;
}

function parseAlertMeta(item = null) {
  return parseJson(getTextValue(item, "ai-output", ""));
}

function appendOperatorHistory(existing = [], entry = null, limit = 20) {
  const list = Array.isArray(existing) ? existing.filter(Boolean) : [];
  if (entry && typeof entry === "object") {
    list.push(entry);
  }

  return list.slice(-limit);
}

export function getEffectiveAlertOperatorState(alert_meta = {}, at = nowIso()) {
  if (clean(alert_meta?.status).toLowerCase() === "resolved") {
    return "resolved";
  }

  const operator_state = clean(alert_meta?.operator_state).toLowerCase();
  if (operator_state === "silenced") {
    const silenced_until_ts = toTimestamp(alert_meta?.silenced_until);
    const at_ts = toTimestamp(at) ?? Date.now();
    if (silenced_until_ts !== null && silenced_until_ts > at_ts) {
      return "silenced";
    }
  }

  if (operator_state === "acknowledged") {
    return "acknowledged";
  }

  return "open";
}

function buildOperatorStateForRecord(existing_meta = {}, at = nowIso()) {
  if (clean(existing_meta?.status).toLowerCase() === "resolved") {
    return {
      operator_state: "open",
      acknowledged_at: null,
      acknowledged_by: null,
      acknowledged_note: null,
      silenced_at: null,
      silenced_by: null,
      silenced_reason: null,
      silenced_until: null,
    };
  }

  const operator_state = getEffectiveAlertOperatorState(existing_meta, at);
  if (operator_state === "acknowledged") {
    return {
      operator_state,
      acknowledged_at: clean(existing_meta?.acknowledged_at) || null,
      acknowledged_by: clean(existing_meta?.acknowledged_by) || null,
      acknowledged_note: clean(existing_meta?.acknowledged_note) || null,
      silenced_at: null,
      silenced_by: null,
      silenced_reason: null,
      silenced_until: null,
    };
  }

  if (operator_state === "silenced") {
    return {
      operator_state,
      acknowledged_at: clean(existing_meta?.acknowledged_at) || null,
      acknowledged_by: clean(existing_meta?.acknowledged_by) || null,
      acknowledged_note: clean(existing_meta?.acknowledged_note) || null,
      silenced_at: clean(existing_meta?.silenced_at) || null,
      silenced_by: clean(existing_meta?.silenced_by) || null,
      silenced_reason: clean(existing_meta?.silenced_reason) || null,
      silenced_until: clean(existing_meta?.silenced_until) || null,
    };
  }

  return {
    operator_state: "open",
    acknowledged_at: clean(existing_meta?.acknowledged_at) || null,
    acknowledged_by: clean(existing_meta?.acknowledged_by) || null,
    acknowledged_note: clean(existing_meta?.acknowledged_note) || null,
    silenced_at: null,
    silenced_by: null,
    silenced_reason: null,
    silenced_until: null,
  };
}

export function applyAlertOperatorActionMeta(
  existing_meta = {},
  {
    action = "",
    actor = "",
    note = "",
    silenced_until = null,
    timestamp = nowIso(),
  } = {}
) {
  const normalized_action = clean(action).toLowerCase();
  const next_meta = {
    ...existing_meta,
    operator_history: appendOperatorHistory(existing_meta?.operator_history),
  };
  const history_entry = {
    action: normalized_action,
    at: clean(timestamp) || nowIso(),
    by: clean(actor) || null,
    note: clean(note) || null,
    silenced_until: clean(silenced_until) || null,
  };

  if (normalized_action === "acknowledge") {
    return {
      ...next_meta,
      operator_state: "acknowledged",
      acknowledged_at: history_entry.at,
      acknowledged_by: history_entry.by,
      acknowledged_note: history_entry.note,
      operator_history: appendOperatorHistory(next_meta.operator_history, history_entry),
    };
  }

  if (normalized_action === "silence") {
    return {
      ...next_meta,
      operator_state: "silenced",
      silenced_at: history_entry.at,
      silenced_by: history_entry.by,
      silenced_reason: history_entry.note,
      silenced_until: history_entry.silenced_until,
      operator_history: appendOperatorHistory(next_meta.operator_history, history_entry),
    };
  }

  if (normalized_action === "unsilence") {
    return {
      ...next_meta,
      operator_state: "open",
      silenced_at: null,
      silenced_by: null,
      silenced_reason: null,
      silenced_until: null,
      operator_history: appendOperatorHistory(next_meta.operator_history, history_entry),
    };
  }

  return next_meta;
}

function buildAlertFields({
  subsystem = "",
  signature = "",
  summary = "",
  timestamp = nowIso(),
  meta = {},
} = {}) {
  return {
    "message-id": buildAlertMessageId({ subsystem, signature }),
    "timestamp": { start: timestamp },
    "trigger-name": buildAlertTriggerName(subsystem),
    "source-app": "System Alert",
    "message": clean(summary),
    "ai-output": JSON.stringify(meta),
  };
}

async function findSystemAlertRecord({ subsystem = "", signature = "" } = {}) {
  return getFirstMatchingItem(
    APP_IDS.message_events,
    {
      "message-id": buildAlertMessageId({ subsystem, signature }),
    },
    {
      sort_desc: true,
    }
  );
}

async function getSystemAlertRecordByReference({
  alert_item_id = null,
  subsystem = "",
  code = "",
  dedupe_key = null,
  signature = "",
} = {}) {
  if (alert_item_id) {
    const item = await getMessageEvent(alert_item_id).catch(() => null);
    if (item?.item_id) return item;
  }

  const resolved_signature =
    clean(signature) ||
    buildAlertSignature({
      subsystem,
      code,
      dedupe_key,
    });

  if (!clean(subsystem) || !clean(resolved_signature)) return null;
  return findSystemAlertRecord({
    subsystem,
    signature: resolved_signature,
  });
}

export async function recordSystemAlert({
  subsystem,
  code,
  summary = "",
  severity = "warning",
  retryable = false,
  affected_ids = [],
  metadata = {},
  dedupe_key = null,
} = {}) {
  const normalized_subsystem = clean(subsystem);
  const normalized_code = clean(code);

  if (!normalized_subsystem || !normalized_code) {
    return {
      ok: false,
      reason: "missing_alert_subsystem_or_code",
    };
  }

  const signature = buildAlertSignature({
    subsystem: normalized_subsystem,
    code: normalized_code,
    dedupe_key,
  });
  const existing = await findSystemAlertRecord({
    subsystem: normalized_subsystem,
    signature,
  });
  const existing_meta = parseAlertMeta(existing);
  const last_seen_at = nowIso();
  const next_meta = {
    version: 1,
    subsystem: normalized_subsystem,
    code: normalized_code,
    signature,
    severity: normalizeSeverity(clean(severity) || "warning", existing_meta?.severity || ""),
    retryable: Boolean(retryable),
    summary: clean(summary) || clean(existing_meta?.summary) || `${normalized_subsystem} ${normalized_code}`,
    first_seen_at: existing_meta?.first_seen_at || last_seen_at,
    last_seen_at,
    occurrence_count: Number(existing_meta?.occurrence_count || 0) + 1,
    affected_ids: uniq([
      ...(Array.isArray(existing_meta?.affected_ids) ? existing_meta.affected_ids : []),
      ...(Array.isArray(affected_ids) ? affected_ids : []),
    ]),
    status: "open",
    metadata: {
      ...(existing_meta?.metadata && typeof existing_meta.metadata === "object"
        ? existing_meta.metadata
        : {}),
      ...(metadata && typeof metadata === "object" ? metadata : {}),
    },
    deliveries:
      existing_meta?.deliveries && typeof existing_meta.deliveries === "object"
        ? existing_meta.deliveries
        : {},
    operator_history: Array.isArray(existing_meta?.operator_history)
      ? existing_meta.operator_history
      : [],
    ...buildOperatorStateForRecord(existing_meta, last_seen_at),
  };
  const fields = buildAlertFields({
    subsystem: normalized_subsystem,
    signature,
    summary: next_meta.summary,
    timestamp: last_seen_at,
    meta: next_meta,
  });

  try {
    let alert_item_id = null;
    let created = false;
    let updated = false;

    if (existing?.item_id) {
      try {
        await updateMessageEvent(existing.item_id, fields);
        alert_item_id = existing.item_id;
        updated = true;
      } catch (updateError) {
        if (isRevisionLimitExceeded(updateError)) {
          logger.warn("system_alert.revision_limit_rotation", {
            subsystem: normalized_subsystem,
            code: normalized_code,
            old_item_id: existing.item_id,
          });
          const rotated = await createMessageEvent(fields);
          alert_item_id = rotated?.item_id || null;
          created = true;
        } else {
          throw updateError;
        }
      }
    } else {
      const created_record = await createMessageEvent(fields);
      alert_item_id = created_record?.item_id || null;
      created = true;
    }

    let delivery_attempts = [];
    try {
      const delivered = await deliverSystemAlert({
        alert_meta: next_meta,
        previous_meta: existing_meta,
      });

      if (delivered?.deliveries && typeof delivered.deliveries === "object") {
        next_meta.deliveries = delivered.deliveries;
        if (alert_item_id) {
          await updateMessageEvent(
            alert_item_id,
            buildAlertFields({
              subsystem: normalized_subsystem,
              signature,
              summary: next_meta.summary,
              timestamp: last_seen_at,
              meta: next_meta,
            })
          );
        }
      }

      delivery_attempts = Array.isArray(delivered?.attempts) ? delivered.attempts : [];
    } catch (error) {
      logger.warn("system_alert.delivery_failed", {
        subsystem: normalized_subsystem,
        code: normalized_code,
        alert_item_id,
        message: error?.message || "unknown_error",
      });
    }

    return {
      ok: true,
      created,
      updated,
      alert_item_id,
      meta: next_meta,
      delivery_attempts,
    };
  } catch (error) {
    logger.warn("system_alert.record_failed", {
      subsystem: normalized_subsystem,
      code: normalized_code,
      message: error?.message || "unknown_error",
    });

    return {
      ok: false,
      reason: clean(error?.message) || "system_alert_record_failed",
    };
  }
}

export async function resolveSystemAlert({
  subsystem,
  code,
  dedupe_key = null,
  resolution_message = "",
  metadata = {},
} = {}) {
  const normalized_subsystem = clean(subsystem);
  const normalized_code = clean(code);
  if (!normalized_subsystem || !normalized_code) {
    return {
      ok: false,
      reason: "missing_alert_subsystem_or_code",
    };
  }

  const signature = buildAlertSignature({
    subsystem: normalized_subsystem,
    code: normalized_code,
    dedupe_key,
  });
  const existing = await findSystemAlertRecord({
    subsystem: normalized_subsystem,
    signature,
  });
  if (!existing?.item_id) {
    return {
      ok: true,
      resolved: false,
      reason: "alert_not_found",
    };
  }

  const existing_meta = parseAlertMeta(existing);
  const resolved_at = nowIso();

  const resolved_fields = {
    ...buildAlertFields({
      subsystem: normalized_subsystem,
      signature,
      summary:
        clean(resolution_message) || clean(existing_meta?.summary) || `${normalized_subsystem} resolved`,
      timestamp: resolved_at,
      meta: {
        ...existing_meta,
        status: "resolved",
        operator_state: "resolved",
        resolved_at,
        resolution_message: clean(resolution_message) || null,
        metadata: {
          ...(existing_meta?.metadata && typeof existing_meta.metadata === "object"
            ? existing_meta.metadata
            : {}),
          ...(metadata && typeof metadata === "object" ? metadata : {}),
        },
      },
    }),
  };

  let alert_item_id = existing.item_id;

  try {
    await updateMessageEvent(existing.item_id, resolved_fields);
  } catch (error) {
    if (isRevisionLimitExceeded(error)) {
      logger.warn("system_alert.resolve_revision_limit_rotation", {
        subsystem: normalized_subsystem,
        code: normalized_code,
        old_item_id: existing.item_id,
      });
      try {
        const rotated = await createMessageEvent(resolved_fields);
        alert_item_id = rotated?.item_id || existing.item_id;
      } catch (createError) {
        logger.warn("system_alert.resolve_rotation_failed", {
          subsystem: normalized_subsystem,
          code: normalized_code,
          message: createError?.message || "unknown_error",
        });
        return {
          ok: false,
          reason: clean(createError?.message) || "resolve_rotation_failed",
        };
      }
    } else {
      logger.warn("system_alert.resolve_failed", {
        subsystem: normalized_subsystem,
        code: normalized_code,
        alert_item_id: existing.item_id,
        failure_bucket: "write_error",
        message: error?.message || "unknown_error",
      });
      return {
        ok: false,
        reason: clean(error?.message) || "resolve_failed",
      };
    }
  }

  return {
    ok: true,
    resolved: true,
    alert_item_id,
  };
}

async function updateSystemAlertOperatorState({
  action = "",
  alert_item_id = null,
  subsystem = "",
  code = "",
  dedupe_key = null,
  signature = "",
  actor = "",
  note = "",
  silenced_until = null,
} = {}) {
  const existing = await getSystemAlertRecordByReference({
    alert_item_id,
    subsystem,
    code,
    dedupe_key,
    signature,
  });

  if (!existing?.item_id) {
    return {
      ok: false,
      reason: "alert_not_found",
    };
  }

  const existing_meta = parseAlertMeta(existing);
  const next_meta = applyAlertOperatorActionMeta(existing_meta, {
    action,
    actor,
    note,
    silenced_until,
    timestamp: nowIso(),
  });

  await updateMessageEvent(
    existing.item_id,
    buildAlertFields({
      subsystem: clean(next_meta?.subsystem || subsystem),
      signature: clean(next_meta?.signature || signature),
      summary: clean(next_meta?.summary) || clean(getTextValue(existing, "message", "")) || "System alert",
      timestamp: nowIso(),
      meta: next_meta,
    })
  );

  return {
    ok: true,
    updated: true,
    alert_item_id: existing.item_id,
    meta: next_meta,
  };
}

export async function acknowledgeSystemAlert(options = {}) {
  return updateSystemAlertOperatorState({
    ...options,
    action: "acknowledge",
  });
}

export async function silenceSystemAlert(options = {}) {
  return updateSystemAlertOperatorState({
    ...options,
    action: "silence",
  });
}

export async function unsilenceSystemAlert(options = {}) {
  return updateSystemAlertOperatorState({
    ...options,
    action: "unsilence",
  });
}

export async function listSystemAlerts({
  limit = 100,
  offset = 0,
} = {}) {
  const items = await findMessageEvents({ "source-app": "System Alert" }, limit, offset);
  const list = Array.isArray(items?.items) ? items.items : Array.isArray(items) ? items : [];

  return list
    .map((item) => ({
      item_id: item?.item_id || null,
      ...parseAlertMeta(item),
    }))
    .filter((item) => clean(item?.subsystem));
}

export default {
  applyAlertOperatorActionMeta,
  getEffectiveAlertOperatorState,
  recordSystemAlert,
  resolveSystemAlert,
  acknowledgeSystemAlert,
  silenceSystemAlert,
  unsilenceSystemAlert,
  listSystemAlerts,
};
