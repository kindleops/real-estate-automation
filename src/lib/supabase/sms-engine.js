import crypto from "node:crypto";

import {
  mapTextgridFailureBucket,
  normalizePhone,
} from "@/lib/providers/textgrid.js";
import { hasSupabaseConfig, supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { captureRouteException, addSentryBreadcrumb } from "@/lib/monitoring/sentry.js";
import { captureSystemEvent } from "@/lib/analytics/posthog-server.js";
import { sendCriticalAlert } from "@/lib/alerts/discord.js";

const SEND_QUEUE_TABLE = "send_queue";
const MESSAGE_EVENTS_TABLE = "message_events";
const TEXTGRID_NUMBERS_TABLE = "textgrid_numbers";
const WEBHOOK_LOG_TABLE = "webhook_log";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function addMinutesIso(value, minutes = 5) {
  const base = new Date(value || nowIso());
  base.setMinutes(base.getMinutes() + Number(minutes || 0));
  return base.toISOString();
}

function asNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asNullableNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function normalizeQueueRowId(value, fallback = null) {
  if (value === null || value === undefined) return fallback;

  if (typeof value === "number") {
    return Number.isFinite(value) && value > 0 ? Math.trunc(value) : fallback;
  }

  const normalized = clean(value);
  if (!normalized) return fallback;

  if (/^\d+$/.test(normalized)) {
    const parsed = Number(normalized);
    return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
  }

  return normalized;
}

function ensureObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function pickFirst(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && clean(value) !== "") {
      return value;
    }
  }
  return null;
}

function getQueueRowDestinationCandidates(row = null) {
  const safe_row = ensureObject(row);
  const metadata = ensureObject(safe_row.metadata);
  const queue_context = ensureObject(metadata.queue_context);

  return [
    ["to_phone_number", safe_row.to_phone_number],
    ["metadata.resolved_to_phone_number", metadata.resolved_to_phone_number],
    ["metadata.canonical_e164", metadata.canonical_e164],
    ["metadata.phone_hidden", metadata.phone_hidden],
    ["metadata.raw_phone_number", metadata.raw_phone_number],
    ["metadata.normalized_target", metadata.normalized_target],
    ["metadata.queue_context.phone_e164", queue_context.phone_e164],
    ["metadata.queue_context.canonical_e164", queue_context.canonical_e164],
    ["metadata.queue_context.phone_hidden", queue_context.phone_hidden],
  ];
}

function toTimestamp(value) {
  if (!value) return null;
  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? null : parsed;
}

function normalizeQueueStatusValue(value) {
  const raw = lower(value);
  if (!raw) return "";
  if (raw === "delivered") return "sent";
  return raw;
}

function getSupabase(deps = {}) {
  if (!deps.supabase && !deps.supabaseClient && !hasSupabaseConfig()) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  return deps.supabase || deps.supabaseClient || defaultSupabase;
}

export function normalizeSendQueueRow(row) {
  const safe_row = ensureObject(row);
  const row_id = normalizeQueueRowId(
    safe_row.id ??
      safe_row.queue_row_id ??
      safe_row.queue_item_id ??
      safe_row.item_id,
    null
  );
  const body = clean(safe_row.message_body || safe_row.message_text || "");

  return {
    id: row_id,
    queue_row_id: row_id,
    queue_item_id: row_id,
    item_id: row_id,
    queue_key: safe_row.queue_key || safe_row.queue_id,
    queue_id: safe_row.queue_id || safe_row.queue_key,
    queue_status: String(safe_row.queue_status || "").toLowerCase(),
    scheduled_for:
      safe_row.scheduled_for ||
      safe_row.scheduled_for_utc ||
      safe_row.scheduled_for_local ||
      safe_row.created_at ||
      null,
    send_priority: Number(safe_row.send_priority ?? 5),
    is_locked: Boolean(safe_row.is_locked),
    locked_at: safe_row.locked_at || null,
    lock_token: safe_row.lock_token || null,
    retry_count: Number(safe_row.retry_count ?? 0),
    max_retries: Number(safe_row.max_retries ?? 3),
    next_retry_at: safe_row.next_retry_at || null,
    message_body: body,
    message_text: safe_row.message_text || safe_row.message_body || "",
    to_phone_number: safe_row.to_phone_number || null,
    from_phone_number: safe_row.from_phone_number || null,
    provider_message_id: safe_row.provider_message_id || null,
    master_owner_id: safe_row.master_owner_id || null,
    prospect_id: safe_row.prospect_id || null,
    property_id: safe_row.property_id || null,
    market_id: safe_row.market_id || null,
    sms_agent_id: safe_row.sms_agent_id || null,
    textgrid_number_id: safe_row.textgrid_number_id || null,
    template_id: safe_row.template_id || null,
    property_address: safe_row.property_address || null,
    property_type: safe_row.property_type || null,
    owner_type: safe_row.owner_type || null,
    timezone: safe_row.timezone || "America/Chicago",
    contact_window: safe_row.contact_window || null,
    touch_number: safe_row.touch_number || null,
    dnc_check: safe_row.dnc_check || null,
    current_stage: safe_row.current_stage || null,
    message_type: safe_row.message_type || null,
    use_case_template: safe_row.use_case_template || null,
    personalization_tags_used: safe_row.personalization_tags_used || null,
    character_count: Number(safe_row.character_count ?? body.length),
    metadata: ensureObject(safe_row.metadata),
    scheduled_for_local: safe_row.scheduled_for_local || null,
    scheduled_for_utc: safe_row.scheduled_for_utc || null,
    created_at: safe_row.created_at || null,
    updated_at: safe_row.updated_at || null,
    sent_at: safe_row.sent_at || null,
    delivered_at: safe_row.delivered_at || null,
    failed_reason: safe_row.failed_reason || null,
    guard_status: safe_row.guard_status || null,
    guard_reason: safe_row.guard_reason || null,
    paused_reason: safe_row.paused_reason || null,
    delivery_confirmed: safe_row.delivery_confirmed || null,
    // Offer record sync tracking (added 2026-04-22)
    cash_offer_snapshot_id:    safe_row.cash_offer_snapshot_id    || null,
    offer_podio_item_id:       safe_row.offer_podio_item_id       || null,
    offer_record_sync_status:  safe_row.offer_record_sync_status  || null,
    offer_record_sync_error:   safe_row.offer_record_sync_error   || null,
    offer_record_synced_at:    safe_row.offer_record_synced_at    || null,
  };
}

export function resolveQueueDestinationPhone(row = null) {
  for (const [source, candidate] of getQueueRowDestinationCandidates(row)) {
    const normalized = normalizePhone(candidate);
    if (normalized) {
      return {
        phone: normalized,
        source,
        raw: clean(candidate) || null,
      };
    }
  }

  return {
    phone: "",
    source: null,
    raw: null,
  };
}

export function shouldRunSendQueueRow(row, now = nowIso()) {
  const normalized = normalizeSendQueueRow(row);
  const destination = resolveQueueDestinationPhone(normalized);
  const now_ts = toTimestamp(now) ?? Date.now();
  const scheduled_ts = toTimestamp(normalized.scheduled_for);
  const next_retry_ts = toTimestamp(normalized.next_retry_at);
  const queue_status_value = normalizeQueueStatusValue(normalized.queue_status);

  if (queue_status_value !== "queued") {
    return {
      ok: false,
      reason: "queue_status_not_queued",
      row: normalized,
    };
  }

  if (normalized.is_locked || clean(normalized.lock_token)) {
    return {
      ok: false,
      reason: "row_locked",
      row: normalized,
    };
  }

  if (scheduled_ts !== null && scheduled_ts > now_ts) {
    return {
      ok: false,
      reason: "scheduled_for_in_future",
      row: normalized,
    };
  }

  if (next_retry_ts !== null && next_retry_ts > now_ts) {
    return {
      ok: false,
      reason: "next_retry_pending",
      row: normalized,
    };
  }

  if (normalized.retry_count >= normalized.max_retries) {
    return {
      ok: false,
      reason: "max_retries_reached",
      row: normalized,
    };
  }

  if (!clean(normalized.message_body)) {
    return {
      ok: false,
      reason: "missing_message_body",
      row: normalized,
    };
  }

  if (!clean(destination.phone)) {
    return {
      ok: false,
      reason: "missing_to_phone_number",
      row: normalized,
    };
  }

  return {
    ok: true,
    reason: "runnable",
    row: normalized,
  };
}

function getQueueSortValues(row) {
  const normalized = normalizeSendQueueRow(row);
  return {
    send_priority_value: asNumber(normalized.send_priority, 5),
    scheduled_ts: toTimestamp(normalized.scheduled_for) ?? Number.MIN_SAFE_INTEGER,
  };
}

export function sortQueuedRows(rows = []) {
  return [...rows].sort((left, right) => {
    const left_values = getQueueSortValues(left);
    const right_values = getQueueSortValues(right);

    if (left_values.send_priority_value !== right_values.send_priority_value) {
      return right_values.send_priority_value - left_values.send_priority_value;
    }

    return left_values.scheduled_ts - right_values.scheduled_ts;
  });
}

function resolvePreclaimScanLimit(limit = 50, deps = {}) {
  const requested_limit = Math.max(1, Math.trunc(asNumber(limit, 50)));
  const requested_scan_cap = Math.trunc(
    asNumber(
      deps.preclaim_scan_cap ??
        deps.preclaimScanCap ??
        deps.scan_cap ??
        deps.scanLimit,
      0
    )
  );

  if (requested_scan_cap > 0) {
    return Math.max(requested_limit, Math.min(requested_scan_cap, 5000));
  }

  return Math.min(Math.max(requested_limit * 20, 250), 1000);
}

export async function loadRunnableSendQueueRows(limit = 50, deps = {}) {
  const supabase = getSupabase(deps);
  const now = deps.now || nowIso();
  const requested_limit = Math.max(1, Math.trunc(asNumber(limit, 50)));
  const preclaim_scan_limit = resolvePreclaimScanLimit(requested_limit, deps);
  const evaluate_contact_window = deps.evaluateContactWindow || evaluateContactWindow;

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .select("*")
    .eq("queue_status", "queued")
    .or(`scheduled_for.is.null,scheduled_for.lte.${now}`)
    .not("is_locked", "is", "true")
    .order("send_priority", { ascending: false, nullsFirst: false })
    .order("scheduled_for", { ascending: true, nullsFirst: true })
    .limit(preclaim_scan_limit);

  if (error) throw error;

  const raw_rows = Array.isArray(data) ? data : [];
  const runnable = [];
  const skipped = [];
  let preclaim_scanned_count = 0;
  let preclaim_outside_window_excluded_count = 0;
  let preclaim_retry_pending_excluded_count = 0;

  for (const row of sortQueuedRows(raw_rows)) {
    preclaim_scanned_count += 1;
    const decision = shouldRunSendQueueRow(row, now);
    if (!decision.ok) {
      if (decision.reason === "next_retry_pending") {
        preclaim_retry_pending_excluded_count += 1;
      }
      skipped.push({
        id: decision.row?.id || null,
        reason: decision.reason,
        row: decision.row,
      });
      continue;
    }

    const contact_window = evaluate_contact_window(decision.row, { ...deps, now });
    if (contact_window && contact_window.allowed === false) {
      preclaim_outside_window_excluded_count += 1;
      skipped.push({
        id: decision.row?.id || null,
        reason: contact_window.reason || "outside_contact_window",
        row: decision.row,
        contact_window,
      });
      continue;
    }

    runnable.push(decision.row);
    if (runnable.length >= requested_limit) break;
  }

  return {
    rows: runnable.slice(0, requested_limit),
    raw_rows,
    skipped,
    now,
    preclaim_outside_window_excluded_count,
    preclaim_retry_pending_excluded_count,
    preclaim_scanned_count,
    eligible_claim_count: Math.min(runnable.length, requested_limit),
    preclaim_scan_limit,
  };
}

export async function claimSendQueueRow(row, deps = {}) {
  const normalized = normalizeSendQueueRow(row);
  if (!normalized.id) {
    return {
      ok: false,
      claimed: false,
      reason: "missing_queue_row_id",
      row: normalized,
    };
  }
  const claimed_at = deps.now || nowIso();
  const lock_token = crypto.randomUUID();
  const metadata = ensureObject(normalized.metadata);
  const processing_run_id = clean(deps.processing_run_id || deps.run_id || metadata.processing_run_id || lock_token);
  const run_started_at = clean(deps.run_started_at || metadata.run_started_at || claimed_at);
  const payload = {
    queue_status: "sending",
    is_locked: true,
    locked_at: claimed_at,
    lock_token,
    metadata: {
      ...metadata,
      processing_run_id,
      run_started_at,
      claimed_at: metadata.claimed_at || claimed_at,
      claimed_by: metadata.claimed_by || "queue_runner",
    },
    updated_at: claimed_at,
  };

  if (typeof deps.claimSendQueueRow === "function") {
    return deps.claimSendQueueRow(normalized, payload);
  }

  const supabase = getSupabase(deps);

  const query = supabase
    .from(SEND_QUEUE_TABLE)
    .update(payload)
    .eq("id", normalized.id)
    .in("queue_status", ["queued", "Queued"])
    .is("lock_token", null)
    .select()
    .maybeSingle();

  const { data, error } = await query;
  if (error) throw error;
  if (!data) {
    return {
      ok: false,
      claimed: false,
      reason: "queue_item_claim_conflict",
      row: normalized,
    };
  }

  return {
    ok: true,
    claimed: true,
    reason: "claimed",
    row: normalizeSendQueueRow(data),
    lock_token,
    claimed_at,
  };
}

export async function updateSendQueueRowWithLock(row_id, lock_token, payload, deps = {}) {
  const normalized_row_id = normalizeQueueRowId(row_id, null);

  if (!normalized_row_id) {
    throw new Error("missing_queue_row_id");
  }

  if (typeof deps.updateSendQueueRowWithLock === "function") {
    return deps.updateSendQueueRowWithLock(normalized_row_id, lock_token, payload);
  }

  const supabase = getSupabase(deps);

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .update(payload)
    .eq("id", normalized_row_id)
    .eq("lock_token", lock_token)
    .select()
    .maybeSingle();

  if (error) throw error;

  return data ? normalizeSendQueueRow(data) : null;
}

function buildTimeFormatter(timezone = "America/Chicago") {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "numeric",
    minute: "2-digit",
    hour12: false,
  });
}

function buildDateParts(date, timezone = "America/Chicago") {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "numeric",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const hour = Number(parts.find((part) => part.type === "hour")?.value || 0);
  const minute = Number(parts.find((part) => part.type === "minute")?.value || 0);

  return {
    hour,
    minute,
    minutes_of_day: hour * 60 + minute,
  };
}

function parseWindowTime(raw = "", previous_period = null) {
  const normalized = clean(raw).toUpperCase();
  const match = normalized.match(/^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?$/);
  if (!match) return null;

  let hour = Number(match[1]);
  const minute = Number(match[2] || 0);
  const period = match[3] || previous_period;

  if (hour < 1 || hour > 12 || minute < 0 || minute > 59) {
    return null;
  }

  if (!period) return null;

  if (period === "AM") {
    if (hour === 12) hour = 0;
  } else if (period === "PM") {
    if (hour !== 12) hour += 12;
  } else {
    return null;
  }

  return {
    minutes_of_day: hour * 60 + minute,
    period,
  };
}

function parseContactWindow(window_text = "") {
  const normalized = clean(window_text)
    .replace(/\bLOCAL\b/gi, "")
    .replace(/\bCT\b/gi, "")
    .replace(/\bCST\b/gi, "")
    .replace(/\bCDT\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();

  if (!normalized) {
    return {
      valid: false,
      reason: "missing_contact_window",
    };
  }

  const parts = normalized
    .split(/\s*-\s*|\s+to\s+/i)
    .map((part) => clean(part))
    .filter(Boolean);

  if (parts.length !== 2) {
    return {
      valid: false,
      reason: "invalid_contact_window_format",
    };
  }

  const start = parseWindowTime(parts[0], null);
  const end = parseWindowTime(parts[1], start?.period || null);

  if (!start || !end) {
    return {
      valid: false,
      reason: "invalid_contact_window_time",
    };
  }

  return {
    valid: true,
    start_minutes: start.minutes_of_day,
    end_minutes: end.minutes_of_day,
  };
}

export function evaluateContactWindow(row, deps = {}) {
  const normalized = normalizeSendQueueRow(row);
  const timezone_raw = clean(normalized.timezone) || "America/Chicago";
  const current_time = deps.now ? new Date(deps.now) : new Date();
  let resolved_timezone = timezone_raw;

  // Resolve abbreviated labels (Eastern, Central, etc.) to IANA names.
  const TIMEZONE_MAP = {
    eastern: "America/New_York",
    et: "America/New_York",
    est: "America/New_York",
    edt: "America/New_York",
    central: "America/Chicago",
    ct: "America/Chicago",
    cst: "America/Chicago",
    cdt: "America/Chicago",
    mountain: "America/Denver",
    mt: "America/Denver",
    mst: "America/Denver",
    mdt: "America/Denver",
    pacific: "America/Los_Angeles",
    pt: "America/Los_Angeles",
    pst: "America/Los_Angeles",
    pdt: "America/Los_Angeles",
  };
  const tz_lower = timezone_raw.toLowerCase();
  if (TIMEZONE_MAP[tz_lower]) {
    resolved_timezone = TIMEZONE_MAP[tz_lower];
  }

  try {
    buildTimeFormatter(resolved_timezone).format(current_time);
  } catch {
    resolved_timezone = "America/Chicago";
  }

  // Hard local-time window: 08:00 ≤ local < 21:00 (8 AM – 9 PM).
  const LOCAL_SEND_START = 8 * 60;   // 480
  const LOCAL_SEND_END   = 21 * 60;  // 1260 (exclusive)

  const local_parts = buildDateParts(current_time, resolved_timezone);
  const current_minutes = local_parts.minutes_of_day;

  if (current_minutes < LOCAL_SEND_START || current_minutes >= LOCAL_SEND_END) {
    return {
      allowed: false,
      reason: "outside_local_send_window",
      timezone: resolved_timezone,
      valid_window: true,
      current_minutes,
      start_minutes: LOCAL_SEND_START,
      end_minutes: LOCAL_SEND_END,
    };
  }

  // If the row has a finer-grained contact_window, also honour that.
  if (!clean(normalized.contact_window)) {
    return {
      allowed: true,
      reason: "inside_local_send_window",
      timezone: resolved_timezone,
      valid_window: true,
      current_minutes,
      start_minutes: LOCAL_SEND_START,
      end_minutes: LOCAL_SEND_END,
    };
  }

  const parsed_window = parseContactWindow(normalized.contact_window);
  if (!parsed_window.valid) {
    return {
      allowed: true,
      reason: "inside_local_send_window_contact_window_unparseable",
      timezone: resolved_timezone,
      valid_window: false,
      current_minutes,
    };
  }

  const start_minutes = parsed_window.start_minutes;
  const end_minutes = parsed_window.end_minutes;

  let within_window = false;
  if (start_minutes <= end_minutes) {
    within_window = current_minutes >= start_minutes && current_minutes <= end_minutes;
  } else {
    within_window = current_minutes >= start_minutes || current_minutes <= end_minutes;
  }

  return {
    allowed: within_window,
    reason: within_window ? "inside_contact_window" : "outside_contact_window",
    timezone: resolved_timezone,
    valid_window: true,
    current_minutes,
    start_minutes,
    end_minutes,
  };
}

export async function selectAvailableTextgridNumber(row, deps = {}) {
  const normalized = normalizeSendQueueRow(row);

  if (clean(normalized.from_phone_number)) {
    return {
      ok: true,
      selected: {
        id: normalized.textgrid_number_id || null,
        phone_number: normalizePhone(normalized.from_phone_number),
        metadata: {},
      },
      from_phone_number: normalizePhone(normalized.from_phone_number),
      reason: "queue_row_from_phone_number_present",
    };
  }

  if (typeof deps.selectAvailableTextgridNumber === "function") {
    return deps.selectAvailableTextgridNumber(normalized);
  }

  const supabase = getSupabase(deps);

  const { data, error } = await supabase
    .from(TEXTGRID_NUMBERS_TABLE)
    .select("*")
    .order("messages_sent_today", { ascending: true, nullsFirst: true })
    .order("last_used_at", { ascending: true, nullsFirst: true })
    .limit(50);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const active_rows = rows.filter((candidate) => {
    const status = lower(candidate?.status);
    const daily_limit = asNullableNumber(candidate?.daily_limit, null);
    const sent_today = asNumber(candidate?.messages_sent_today, 0);

    if (status && status !== "active") return false;
    if (daily_limit !== null && sent_today >= daily_limit) return false;
    return Boolean(normalizePhone(candidate?.phone_number));
  });

  const preferred = active_rows.find(
    (candidate) =>
      String(candidate?.id || "") === String(normalized.textgrid_number_id || "")
  );
  const selected = preferred || active_rows[0] || null;

  if (!selected) {
    return {
      ok: false,
      reason: "no_available_textgrid_numbers",
      selected: null,
      from_phone_number: null,
    };
  }

  return {
    ok: true,
    reason: preferred ? "preferred_textgrid_number_selected" : "rotation_textgrid_number_selected",
    selected,
    from_phone_number: normalizePhone(selected.phone_number),
  };
}

export async function reserveFromPhoneNumber(row, lock_token, selection, deps = {}) {
  const normalized = normalizeSendQueueRow(row);
  const from_phone_number = normalizePhone(selection?.from_phone_number);
  const textgrid_number_id = selection?.selected?.id || null;
  const now = deps.now || nowIso();

  if (!from_phone_number) {
    throw new Error("missing_from_phone_number");
  }

  const updated = await updateSendQueueRowWithLock(
    normalized.id,
    lock_token,
    {
      from_phone_number,
      textgrid_number_id,
      updated_at: now,
    },
    deps
  );

  return updated || {
    ...normalized,
    from_phone_number,
    textgrid_number_id,
  };
}

export async function incrementTextgridNumberUsage(selection, deps = {}) {
  const selected = selection?.selected || null;

  if (!selected?.id) return null;

  if (typeof deps.incrementTextgridNumberUsage === "function") {
    return deps.incrementTextgridNumberUsage(selected);
  }

  const supabase = getSupabase(deps);

  const next_sent_today = asNumber(selected.messages_sent_today, 0) + 1;
  const payload = {
    messages_sent_today: next_sent_today,
    last_used_at: deps.now || nowIso(),
    updated_at: deps.now || nowIso(),
  };

  const { data, error } = await supabase
    .from(TEXTGRID_NUMBERS_TABLE)
    .update(payload)
    .eq("id", selected.id)
    .select()
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

function buildSuccessMessageEvent(row, send_result, options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const event_timestamp = options.now || nowIso();
  const queue_key = clean(normalized.queue_key) || clean(normalized.queue_id) || String(normalized.id);
  const sid = clean(
    options.provider_message_sid ||
      send_result?.sid ||
      send_result?.provider_message_id ||
      send_result?.message_id
  );

  return {
    message_event_key: `outbound_${queue_key}`,
    provider_message_sid: sid,
    direction: "outbound",
    event_type: "outbound_send",
    message_body: normalized.message_body,
    to_phone_number: normalized.to_phone_number,
    from_phone_number: normalized.from_phone_number,
    queue_id: normalized.id,
    sent_at: event_timestamp,
    event_timestamp,
    created_at: event_timestamp,
    provider_delivery_status: clean(send_result?.status) || null,
    delivery_status: "sent",
    character_count: normalized.character_count || normalized.message_body.length,
    latency_ms: options.latency_ms ?? null,
    master_owner_id: normalized.master_owner_id,
    prospect_id: normalized.prospect_id,
    property_id: normalized.property_id,
    market_id: normalized.market_id,
    sms_agent_id: normalized.sms_agent_id,
    textgrid_number_id: normalized.textgrid_number_id,
    template_id: normalized.template_id,
    property_address: normalized.property_address,
    stage_before: normalized.current_stage || null,
    stage_after: normalized.current_stage || null,
    metadata: {
      source: "supabase_send_queue",
      queue_key,
      send_result,
      queue_row: {
        id: normalized.id,
        queue_key: normalized.queue_key,
        queue_status: normalized.queue_status,
      },
    },
  };
}

function buildFailureMessageEvent(row, error, options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const event_timestamp = options.now || nowIso();
  const queue_key = clean(normalized.queue_key) || clean(normalized.queue_id) || String(normalized.id);
  const timestamp_key = event_timestamp.replace(/[^0-9]/g, "").slice(0, 14);
  const failure_result =
    ensureObject(options.send_result).error_message || ensureObject(options.send_result).error_status
      ? options.send_result
      : {
          ok: false,
          error_message: clean(error?.message),
          error_status: error?.status || null,
        };

  return {
    message_event_key: `failed_${queue_key}_${timestamp_key}`,
    direction: "outbound",
    event_type: "outbound_send_failed",
    message_body: normalized.message_body,
    to_phone_number: normalized.to_phone_number,
    from_phone_number: normalized.from_phone_number,
    queue_id: normalized.id,
    failed_at: event_timestamp,
    event_timestamp,
    error_message: clean(error?.message) || clean(options.send_result?.error_message) || "send_failed",
    failure_reason: clean(error?.message) || clean(options.send_result?.error_message) || "send_failed",
    failure_bucket: mapTextgridFailureBucket(failure_result) || null,
    is_final_failure:
      normalized.retry_count + 1 >= normalized.max_retries,
    master_owner_id: normalized.master_owner_id,
    prospect_id: normalized.prospect_id,
    property_id: normalized.property_id,
    market_id: normalized.market_id,
    sms_agent_id: normalized.sms_agent_id,
    textgrid_number_id: normalized.textgrid_number_id,
    template_id: normalized.template_id,
    property_address: normalized.property_address,
    metadata: {
      source: "supabase_send_queue",
      queue_key,
      error: {
        message: clean(error?.message) || null,
        status: error?.status || null,
      },
      send_result: options.send_result || null,
    },
  };
}

export async function writeOutboundSuccessMessageEvent(row, send_result, options = {}) {
  const payload = buildSuccessMessageEvent(row, send_result, options);
  const normalized = normalizeSendQueueRow(row);

  // Analytics fires regardless of whether the DB write is real or injected —
  // the SMS is already confirmed sent at this call site.
  captureSystemEvent("sms_send_succeeded", {
    queue_row_id: normalized.id,
    queue_key: normalized.queue_key,
    provider_message_id: normalized.provider_message_id || send_result?.sid || null,
    master_owner_id: normalized.master_owner_id || null,
    template_id: normalized.template_id || null,
    touch_number: normalized.touch_number ?? null,
    character_count: normalized.character_count ?? 0,
    campaign_id: normalized.metadata?.campaign_id ?? null,
  });

  captureSystemEvent("message_event_created", {
    queue_row_id: normalized.id,
    queue_key: normalized.queue_key,
    provider_message_id: normalized.provider_message_id || send_result?.sid || null,
    master_owner_id: normalized.master_owner_id || null,
    template_id: normalized.template_id || null,
    campaign_id: normalized.metadata?.campaign_id ?? null,
    direction: "outbound",
    event_type: "outbound_sms",
  });

  if (typeof options.writeOutboundSuccessMessageEvent === "function") {
    return options.writeOutboundSuccessMessageEvent(payload);
  }

  const supabase = getSupabase(options);

  let data, error;
  try {
    ({ data, error } = await supabase
      .from(MESSAGE_EVENTS_TABLE)
      .upsert(payload, {
        onConflict: "message_event_key",
        ignoreDuplicates: false,
      })
      .select()
      .maybeSingle());
  } catch (db_error) {
    captureRouteException(db_error, {
      route: "sms-engine/writeOutboundSuccessMessageEvent",
      subsystem: "sms_engine",
      context: {
        queue_row_id: normalized.id,
        queue_key: normalized.queue_key,
        master_owner_id: normalized.master_owner_id,
      },
    });
    throw db_error;
  }

  if (error) {
    captureRouteException(error, {
      route: "sms-engine/writeOutboundSuccessMessageEvent",
      subsystem: "sms_engine",
      context: {
        queue_row_id: normalized.id,
        queue_key: normalized.queue_key,
        master_owner_id: normalized.master_owner_id,
      },
    });
    throw error;
  }

  addSentryBreadcrumb("sms_send", "sms_send_succeeded", {
    queue_row_id: normalized.id,
    queue_key: normalized.queue_key,
    provider_message_id: normalized.provider_message_id,
    master_owner_id: normalized.master_owner_id,
  });

  return data || payload;
}

export async function writeOutboundFailureMessageEvent(row, error, options = {}) {
  const payload = buildFailureMessageEvent(row, error, options);

  // Capture the original SMS send error to Sentry. Fires regardless of whether
  // the DB write is real or injected via options — telemetry is a side-effect
  // that applies in all cases.
  const normalized_for_sentry = normalizeSendQueueRow(row);
  captureRouteException(error, {
    route: "sms-engine/writeOutboundFailureMessageEvent",
    subsystem: "sms_engine",
    context: {
      queue_row_id: normalized_for_sentry.id,
      queue_key: normalized_for_sentry.queue_key,
      master_owner_id: normalized_for_sentry.master_owner_id,
    },
  });
  addSentryBreadcrumb("sms_send", "sms_send_failed", {
    queue_row_id: normalized_for_sentry.id,
    queue_key: normalized_for_sentry.queue_key,
    master_owner_id: normalized_for_sentry.master_owner_id,
    error_message: error?.message || String(error),
  });

  captureSystemEvent("sms_send_failed", {
    queue_row_id: normalized_for_sentry.id,
    queue_key: normalized_for_sentry.queue_key,
    master_owner_id: normalized_for_sentry.master_owner_id || null,
    template_id: normalized_for_sentry.template_id || null,
    touch_number: normalized_for_sentry.touch_number ?? null,
    campaign_id: normalized_for_sentry.metadata?.campaign_id ?? null,
    error_message: error?.message || String(error),
  });

  sendCriticalAlert({
    title: "SMS Send Failed",
    description: `Failed to send SMS for queue row ${normalized_for_sentry.id ?? "unknown"}`,
    color: 0xe74c3c,
    fields: [
      { name: "Queue Row ID", value: String(normalized_for_sentry.id ?? "?"), inline: true },
      { name: "Master Owner ID", value: String(normalized_for_sentry.master_owner_id ?? "?"), inline: true },
      { name: "Touch", value: String(normalized_for_sentry.touch_number ?? "?"), inline: true },
      { name: "Error", value: (error?.message || String(error)).slice(0, 256), inline: false },
    ],
    timestamp: new Date().toISOString(),
    footer: { text: "sms_engine/writeOutboundFailureMessageEvent" },
  });

  if (typeof options.writeOutboundFailureMessageEvent === "function") {
    return options.writeOutboundFailureMessageEvent(payload);
  }

  const supabase = getSupabase(options);

  const { data: insert_data, error: insert_error } = await supabase
    .from(MESSAGE_EVENTS_TABLE)
    .insert(payload)
    .select()
    .maybeSingle();

  if (insert_error) {
    captureRouteException(insert_error, {
      route: "sms-engine/writeOutboundFailureMessageEvent/db_write",
      subsystem: "sms_engine",
      context: {
        queue_row_id: normalized_for_sentry.id,
        queue_key: normalized_for_sentry.queue_key,
        master_owner_id: normalized_for_sentry.master_owner_id,
      },
    });
    throw insert_error;
  }
  return insert_data || payload;
}

export async function finalizeSendQueueSuccess(row, lock_token, send_result, options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const provider_message_id = clean(
    send_result?.sid || send_result?.provider_message_id || send_result?.message_id
  );

  if (!provider_message_id) {
    throw new Error("SEND FAILED - NO SID");
  }

  const now = options.now || nowIso();
  const payload = {
    queue_status: "sent",
    sent_at: now,
    delivery_confirmed: "pending",
    provider_message_id,
    is_locked: false,
    locked_at: null,
    lock_token: null,
    character_count: normalized.message_body.length,
    updated_at: now,
    failed_reason: null,
    from_phone_number: normalized.from_phone_number,
    textgrid_number_id: normalized.textgrid_number_id,
  };

  const updated_row = await updateSendQueueRowWithLock(
    normalized.id,
    lock_token,
    payload,
    options
  );

  if (!updated_row) {
    throw new Error("queue_row_lock_mismatch_after_send");
  }

  return updated_row;
}

export async function finalizeSendQueueFailure(row, lock_token, error, options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const now = options.now || nowIso();
  const next_retry_count = normalized.retry_count + 1;
  const is_final_failure = next_retry_count >= normalized.max_retries;
  const error_message = clean(error?.message) || "send_failed";

  const payload = {
    queue_status: is_final_failure ? "failed" : "queued",
    failed_reason: error_message,
    retry_count: next_retry_count,
    next_retry_at: is_final_failure ? null : addMinutesIso(now, 5),
    is_locked: false,
    locked_at: null,
    lock_token: null,
    updated_at: now,
    metadata: {
      ...normalized.metadata,
      provider_error: {
        message: error_message,
        status: error?.status || null,
        retryable: !is_final_failure,
        final_queue_status: is_final_failure ? "failed" : "queued",
        recorded_at: now,
      },
      final_queue_status: is_final_failure ? "failed" : "queued",
      finalized_at: now,
    },
  };

  const updated_row = await updateSendQueueRowWithLock(
    normalized.id,
    lock_token,
    payload,
    options
  );

  return updated_row || {
    ...normalized,
    ...payload,
  };
}

export async function releaseSkippedQueueRow(row, lock_token, reason, options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const now = options.now || nowIso();
  const skip_reason = clean(reason) || "skipped";

  const payload = {
    queue_status: "queued",
    is_locked: false,
    locked_at: null,
    lock_token: null,
    updated_at: now,
    metadata: {
      ...normalized.metadata,
      skip_reason,
      final_queue_status: "queued",
      finalized_at: now,
    },
  };

  const updated_row = await updateSendQueueRowWithLock(
    normalized.id,
    lock_token,
    payload,
    options
  );

  return updated_row || {
    ...normalized,
    ...payload,
    skip_reason,
  };
}

export async function pauseInvalidQueueRow(row, reason = "invalid_queue_row", options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const now = options.now || nowIso();
  const queue_row_id = normalizeQueueRowId(normalized.id, null);
  const skip_reason = clean(reason) || "invalid_queue_row";

  if (!queue_row_id) {
    throw new Error("missing_queue_row_id");
  }

  const payload = {
    queue_status: "paused_invalid_queue_row",
    guard_status: "blocked",
    guard_reason: skip_reason,
    paused_reason: skip_reason,
    is_locked: false,
    locked_at: null,
    lock_token: null,
    updated_at: now,
    metadata: {
      ...normalized.metadata,
      skip_reason,
      invalid_queue_row: true,
      final_queue_status: "paused_invalid_queue_row",
      finalized_at: now,
    },
  };

  if (typeof options.pauseInvalidQueueRow === "function") {
    return options.pauseInvalidQueueRow(normalized, payload);
  }

  if (typeof options.updateQueueRow === "function") {
    await options.updateQueueRow(queue_row_id, payload);
    return {
      ...normalized,
      ...payload,
    };
  }

  const supabase = getSupabase(options);
  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .update(payload)
    .eq("id", queue_row_id)
    .select()
    .maybeSingle();

  if (error) throw error;

  return data ? normalizeSendQueueRow(data) : {
    ...normalized,
    ...payload,
  };
}

export async function pauseMaxRetriesQueueRow(row, reason = "max_retries_reached", options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const now = options.now || nowIso();
  const queue_row_id = normalizeQueueRowId(normalized.id, null);
  const skip_reason = clean(reason) || "max_retries_reached";

  if (!queue_row_id) {
    throw new Error("missing_queue_row_id");
  }

  const payload = {
    queue_status: "paused_max_retries",
    guard_status: "blocked",
    guard_reason: skip_reason,
    paused_reason: skip_reason,
    is_locked: false,
    locked_at: null,
    lock_token: null,
    updated_at: now,
    metadata: {
      ...normalized.metadata,
      skip_reason,
      final_queue_status: "paused_max_retries",
      finalized_at: now,
    },
  };

  if (typeof options.pauseMaxRetriesQueueRow === "function") {
    return options.pauseMaxRetriesQueueRow(normalized, payload);
  }

  if (typeof options.updateQueueRow === "function") {
    await options.updateQueueRow(queue_row_id, payload);
    return {
      ...normalized,
      ...payload,
    };
  }

  const supabase = getSupabase(options);
  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .update(payload)
    .eq("id", queue_row_id)
    .select()
    .maybeSingle();

  if (error) throw error;

  return data ? normalizeSendQueueRow(data) : {
    ...normalized,
    ...payload,
  };
}

export async function loadClaimedQueueRow(row, options = {}) {
  const normalized = normalizeSendQueueRow(row);
  const queue_row_id = normalizeQueueRowId(normalized.id, null);

  if (!queue_row_id) return null;

  if (typeof options.loadQueueRowById === "function") {
    const loaded = await options.loadQueueRowById(queue_row_id);
    return loaded ? normalizeSendQueueRow(loaded) : null;
  }

  const supabase = getSupabase(options);
  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .select("*")
    .eq("id", queue_row_id)
    .maybeSingle();

  if (error) throw error;
  return data ? normalizeSendQueueRow(data) : null;
}

export async function recycleClaimedSendingRow(row, lock_token, reason = "finalize_safety_net", options = {}) {
  const latest = normalizeSendQueueRow(row);
  const now = options.now || nowIso();
  const queue_row_id = normalizeQueueRowId(latest.id, null);
  const resolved_lock_token = clean(lock_token || latest.lock_token);

  if (!queue_row_id) {
    return null;
  }

  if (lower(latest.queue_status) !== "sending") {
    return null;
  }

  const next_retry_count = latest.retry_count + 1;
  const final_queue_status = next_retry_count >= latest.max_retries ? "failed" : "queued";
  const finalization_reason = clean(reason) || "finalize_safety_net";
  const payload = {
    queue_status: final_queue_status,
    failed_reason: final_queue_status === "failed" ? finalization_reason : latest.failed_reason || null,
    retry_count: next_retry_count,
    next_retry_at: final_queue_status === "failed" ? null : addMinutesIso(now, 5),
    is_locked: false,
    locked_at: null,
    lock_token: null,
    updated_at: now,
    metadata: {
      ...latest.metadata,
      finalize_safety_net: true,
      skip_reason: latest.metadata?.skip_reason || finalization_reason,
      finalization_error: finalization_reason,
      final_queue_status,
      finalized_at: now,
    },
  };

  if (typeof options.recycleClaimedSendingRow === "function") {
    return options.recycleClaimedSendingRow(latest, resolved_lock_token, payload);
  }

  if (resolved_lock_token) {
    const updated = await updateSendQueueRowWithLock(
      queue_row_id,
      resolved_lock_token,
      payload,
      options
    );
    return updated || null;
  }

  if (typeof options.updateQueueRow === "function") {
    await options.updateQueueRow(queue_row_id, payload);
    return {
      ...latest,
      ...payload,
    };
  }

  const supabase = getSupabase(options);
  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .update(payload)
    .eq("id", queue_row_id)
    .eq("queue_status", "sending")
    .select()
    .maybeSingle();

  if (error) throw error;
  return data ? normalizeSendQueueRow(data) : null;
}

export async function finalizeClaimedSendQueueRows(claimed_rows = [], options = {}) {
  const rows = Array.isArray(claimed_rows) ? claimed_rows : [];
  const finalized = [];
  const errors = [];

  if (typeof options.finalizeClaimedSendQueueRows === "function") {
    return options.finalizeClaimedSendQueueRows(rows, options);
  }

  for (const claimed of rows) {
    const row = claimed?.row || claimed;
    const lock_token = clean(claimed?.lock_token || row?.lock_token);
    const queue_row_id = normalizeQueueRowId(row?.id ?? row?.queue_row_id, null);
    if (!queue_row_id) continue;

    try {
      const latest = await loadClaimedQueueRow(row, options);
      if (!latest || lower(latest.queue_status) !== "sending") {
        continue;
      }
      if (lock_token && clean(latest.lock_token) && clean(latest.lock_token) !== lock_token) {
        continue;
      }

      const recycled = await recycleClaimedSendingRow(
        latest,
        lock_token || latest.lock_token,
        claimed?.reason || "finalize_safety_net",
        options
      );

      if (recycled) {
        finalized.push(recycled);
      }
    } catch (error) {
      errors.push({
        queue_row_id,
        error: clean(error?.message) || "finalize_safety_net_failed",
      });
    }
  }

  return {
    ok: errors.length === 0,
    finalized_count: finalized.length,
    stuck_recycled_count: finalized.length,
    finalized,
    errors,
  };
}

function buildWebhookLogCandidates({
  event_type,
  direction = null,
  provider_message_sid = null,
  payload = {},
  headers = {},
  received_at = nowIso(),
  source = "textgrid",
} = {}) {
  const raw_payload = ensureObject(payload);
  const raw_headers = ensureObject(headers);

  return [
    {
      provider: source,
      event_type,
      direction,
      provider_message_sid,
      payload: raw_payload,
      headers: raw_headers,
      created_at: received_at,
    },
    {
      source,
      event_type,
      direction,
      provider_message_sid,
      raw_payload,
      headers: raw_headers,
      created_at: received_at,
    },
    {
      event_type,
      provider_message_sid,
      payload: raw_payload,
      created_at: received_at,
    },
    {
      raw_payload,
      created_at: received_at,
    },
  ];
}

export async function writeWebhookLog(options = {}) {
  const candidates = buildWebhookLogCandidates(options);

  if (typeof options.writeWebhookLog === "function") {
    return options.writeWebhookLog(candidates[0]);
  }

  const supabase = getSupabase(options);

  let last_error = null;
  for (const payload of candidates) {
    const { data, error } = await supabase
      .from(WEBHOOK_LOG_TABLE)
      .insert(payload)
      .select()
      .maybeSingle();

    if (!error) return data || payload;
    last_error = error;
  }

  throw last_error || new Error("webhook_log_write_failed");
}

export async function logInboundMessageEvent(payload, options = {}) {
  const now = options.now || nowIso();
  const message_sid = clean(
    payload?.message_id || payload?.provider_message_sid || payload?.sid
  );
  const from_phone_number = normalizePhone(payload?.from || payload?.from_phone_number);
  const to_phone_number = normalizePhone(payload?.to || payload?.to_phone_number);

  // Extract body from all possible field names. The normalizer outputs .message
  // and .message_body; raw/un-normalized payloads may use Body, MessageBody, etc.
  const body_candidates = [
    ["message",      payload?.message],
    ["message_body", payload?.message_body],
    ["body",         payload?.body],
    ["Body",         payload?.Body],
    ["MessageBody",  payload?.MessageBody],
    ["Message",      payload?.Message],
    ["Text",         payload?.Text],
    ["text",         payload?.text],
    ["payload.Body", payload?.payload?.Body],
    ["payload.body", payload?.payload?.body],
    ["data.Body",    payload?.data?.Body],
    ["data.body",    payload?.data?.body],
  ];

  let message_body = null;
  let body_source = payload?.body_source || null;

  if (body_source && payload?.message) {
    message_body = clean(payload.message);
  } else {
    for (const [key, val] of body_candidates) {
      const s = clean(val);
      if (s) {
        message_body = s;
        body_source = key;
        break;
      }
    }
  }

  const body_missing = !message_body;
  const raw_body_keys = payload?.raw_body_keys || Object.keys(payload || {});

  if (body_missing) {
    console.warn("INBOUND BODY MISSING", JSON.stringify({
      message_sid,
      from_phone_number,
      available_payload_keys: raw_body_keys,
    }));
  }

  const event = {
    message_event_key: `inbound_${message_sid || crypto.randomUUID()}`,
    provider_message_sid: message_sid || null,
    direction: "inbound",
    event_type: "inbound_sms",
    message_body: message_body || null,
    to_phone_number,
    from_phone_number,
    received_at: pickFirst(payload?.received_at, now),
    event_timestamp: pickFirst(payload?.received_at, now),
    created_at: now,
    character_count: message_body ? message_body.length : 0,
    metadata: {
      source: "textgrid_inbound_webhook",
      raw_body_keys,
      body_source,
      ...(body_missing ? {
        body_missing: true,
        available_payload_keys: raw_body_keys,
      } : {}),
      payload,
    },
  };

  // Analytics fires regardless of DI injection path — the inbound message
  // payload is already validated at this point.
  captureSystemEvent("inbound_sms_logged", {
    provider_message_sid: message_sid || null,
    character_count: event.character_count,
  });

  if (typeof options.logInboundMessageEvent === "function") {
    return options.logInboundMessageEvent(event);
  }

  const supabase = getSupabase(options);

  const { data, error } = await supabase
    .from(MESSAGE_EVENTS_TABLE)
    .upsert(event, {
      onConflict: "message_event_key",
      ignoreDuplicates: false,
    })
    .select()
    .maybeSingle();

  if (error) throw error;

  return data || event;
}

export async function syncDeliveryEvent(payload, options = {}) {
  const now = options.now || nowIso();
  const provider_message_sid = clean(payload?.message_id || payload?.provider_message_sid || payload?.sid);
  const provider_status = lower(payload?.status || payload?.provider_delivery_status);
  const raw_carrier_status = clean(payload?.error_status || payload?.status || "");
  const delivery_status =
    provider_status === "delivered"
      ? "delivered"
      : ["failed", "undelivered", "error"].includes(provider_status)
        ? "failed"
        : provider_status || "sent";

  const message_events_payload = {
    provider_delivery_status: provider_status || null,
    raw_carrier_status: raw_carrier_status || null,
    delivery_status,
  };

  if (provider_status === "delivered") {
    message_events_payload.delivered_at = pickFirst(payload?.delivered_at, now);
  } else if (["failed", "undelivered", "error"].includes(provider_status)) {
    message_events_payload.failed_at = now;
    message_events_payload.error_message =
      clean(payload?.error_message) || "delivery_failed";
    message_events_payload.failure_reason =
      clean(payload?.error_message) || "delivery_failed";
    message_events_payload.failure_bucket =
      mapTextgridFailureBucket({
        ok: false,
        error_message: payload?.error_message,
        error_status: payload?.error_status,
      }) || null;
  }

  if (typeof options.syncDeliveryEvent === "function") {
    // Analytics fires before DI early return — delivery_status is already computed.
    captureSystemEvent("sms_delivery_updated", {
      provider_message_sid: provider_message_sid || null,
      delivery_status,
      provider_delivery_status: provider_status || null,
      error_status: clean(payload?.error_status) || null,
      error_message: clean(payload?.error_message) || null,
    });
    return options.syncDeliveryEvent(provider_message_sid, message_events_payload);
  }

  const supabase = getSupabase(options);

  const { data: message_events_data, error: message_events_error } = await supabase
    .from(MESSAGE_EVENTS_TABLE)
    .update(message_events_payload)
    .eq("provider_message_sid", provider_message_sid)
    .select();

  if (message_events_error) throw message_events_error;

  const queue_payload = {
    updated_at: now,
  };

  if (provider_status === "delivered") {
    queue_payload.delivered_at = pickFirst(payload?.delivered_at, now);
    queue_payload.delivery_confirmed = "confirmed";
  } else if (["failed", "undelivered", "error"].includes(provider_status)) {
    queue_payload.delivery_confirmed = "failed";
    queue_payload.failed_reason =
      clean(payload?.error_message) || "delivery_failed";
  }

  const { data: send_queue_data, error: send_queue_error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .update(queue_payload)
    .eq("provider_message_id", provider_message_sid)
    .select();

  if (send_queue_error) throw send_queue_error;

  captureSystemEvent("sms_delivery_updated", {
    provider_message_sid: provider_message_sid || null,
    delivery_status,
    provider_delivery_status: provider_status || null,
    error_status: clean(payload?.error_status) || null,
    error_message: clean(payload?.error_message) || null,
    message_events_updated: Array.isArray(message_events_data) ? message_events_data.length : 0,
  });

  return {
    provider_message_sid,
    provider_status,
    message_events_count: Array.isArray(message_events_data) ? message_events_data.length : 0,
    send_queue_count: Array.isArray(send_queue_data) ? send_queue_data.length : 0,
  };
}

/**
 * Build the canonical dedupe key for a send_queue row.
 * The unique partial index on send_queue prevents active rows with the same key.
 */
export function buildSendQueueDedupeKey({
  master_owner_id,
  property_id,
  to_phone_number,
  template_use_case,
  touch_number,
  campaign_session_id,
} = {}) {
  const parts = [
    clean(master_owner_id) || "no_owner",
    clean(property_id) || "no_property",
    clean(to_phone_number) || "no_phone",
    clean(template_use_case) || "no_use_case",
    String(touch_number ?? "0"),
    clean(campaign_session_id) || "no_session",
  ];
  return parts.join(":");
}

export async function insertSupabaseSendQueueRow(payload, deps = {}) {
  const now = deps.now || nowIso();
  const row = normalizeSendQueueRow({
    ...payload,
    queue_status: payload.queue_status || "queued",
    scheduled_for: payload.scheduled_for || payload.scheduled_for_utc || now,
    scheduled_for_utc: payload.scheduled_for_utc || payload.scheduled_for || now,
    scheduled_for_local: payload.scheduled_for_local || payload.scheduled_for || now,
    created_at: payload.created_at || now,
    updated_at: payload.updated_at || now,
  });

  const insert_payload = {
    queue_key: clean(row.queue_key) || crypto.randomUUID(),
    queue_id: clean(row.queue_id) || clean(row.queue_key) || crypto.randomUUID(),
    queue_status: normalizeQueueStatusValue(row.queue_status) || "queued",
    scheduled_for: row.scheduled_for || now,
    send_priority: asNumber(row.send_priority, 5),
    is_locked: false,
    locked_at: null,
    lock_token: null,
    retry_count: asNumber(row.retry_count, 0),
    max_retries: asNumber(row.max_retries, 3),
    next_retry_at: row.next_retry_at || null,
    message_body: row.message_body,
    message_text: row.message_text || row.message_body,
    to_phone_number: resolveQueueDestinationPhone(row).phone || null,
    from_phone_number: normalizePhone(row.from_phone_number) || null,
    metadata: row.metadata,
    created_at: row.created_at || now,
    updated_at: row.updated_at || now,
    property_address: row.property_address || null,
    queue_sequence: row.touch_number || null,
    property_type: row.property_type || null,
    owner_type: row.owner_type || null,
    scheduled_for_local: row.scheduled_for_local || row.scheduled_for || now,
    scheduled_for_utc: row.scheduled_for_utc || row.scheduled_for || now,
    timezone: row.timezone || "America/Chicago",
    contact_window: row.contact_window || null,
    sent_at: row.sent_at || null,
    delivered_at: row.delivered_at || null,
    failed_reason: row.failed_reason || null,
    delivery_confirmed: row.delivery_confirmed || null,
    master_owner_id: row.master_owner_id || null,
    prospect_id: row.prospect_id || null,
    property_id: row.property_id || null,
    market_id: row.market_id || null,
    sms_agent_id: row.sms_agent_id || null,
    textgrid_number_id: row.textgrid_number_id || null,
    template_id: row.template_id || null,
    touch_number: row.touch_number || null,
    dnc_check: row.dnc_check || null,
    current_stage: row.current_stage || null,
    message_type: row.message_type || null,
    use_case_template: row.use_case_template || null,
    personalization_tags_used: row.personalization_tags_used || null,
    character_count: row.character_count || row.message_body.length,
    provider_message_id: row.provider_message_id || null,
    // Hardening columns (added 2026-04-28)
    dedupe_key: clean(payload.dedupe_key || row.metadata?.idempotency_key || row.queue_key) || null,
    seller_first_name: clean(payload.seller_first_name || row.metadata?.seller_first_name || row.metadata?.queue_context?.seller_first_name) || null,
    seller_display_name: clean(payload.seller_display_name || row.metadata?.seller_display_name) || null,
  };

  if (typeof deps.insertSupabaseSendQueueRow === "function") {
    return deps.insertSupabaseSendQueueRow(insert_payload);
  }

  const supabase = getSupabase(deps);

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .insert(insert_payload)
    .select()
    .maybeSingle();

  if (!error) {
    return {
      ok: true,
      item_id: data?.id || null,
      queue_row_id: data?.id || null,
      queue_item_id: data?.id || null,
      queue_id: data?.queue_id || insert_payload.queue_id,
      queue_key: data?.queue_key || insert_payload.queue_key,
      raw: data || insert_payload,
    };
  }

  if (error.code === "23505") {
    const { data: existing, error: existing_error } = await supabase
      .from(SEND_QUEUE_TABLE)
      .select("*")
      .eq("queue_key", insert_payload.queue_key)
      .maybeSingle();

    if (existing_error) throw existing_error;

    return {
      ok: false,
      reason: "duplicate_blocked",
      item_id: existing?.id || null,
      queue_row_id: existing?.id || null,
      queue_item_id: existing?.id || null,
      queue_id: existing?.queue_id || insert_payload.queue_id,
      queue_key: existing?.queue_key || insert_payload.queue_key,
      raw: existing || insert_payload,
    };
  }

  throw error;
}
