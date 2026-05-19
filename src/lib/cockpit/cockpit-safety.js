/**
 * cockpit-safety.js
 *
 * Centralized safety gate checks for all cockpit endpoints.
 * Every function returns { ok: true } or { ok: false, reason: string, diagnostics?: object }.
 * Callers must check ok before proceeding with any mutation.
 */

import { getSystemFlags } from "@/lib/system-control.js";
import { normalizePhone } from "@/lib/utils/phones.js";
import { supabase as defaultSupabase } from "@/lib/supabase/client.js";

function clean(value) { return String(value ?? "").trim(); }
function lower(value) { return clean(value).toLowerCase(); }

const CANONICAL_E164_RE = /^\+1\d{10}$/;

// ── Phone / thread_key ────────────────────────────────────────────────────

export function checkCanonicalThreadKey(thread_key) {
  const key = clean(thread_key);
  if (!key) return { ok: false, reason: "THREAD_CONTEXT_UNRESOLVED", diagnostics: { missing: "thread_key" } };
  if (!CANONICAL_E164_RE.test(key)) {
    return {
      ok: false,
      reason: "QUEUE_SAFETY_BLOCKED",
      diagnostics: { thread_key: key, issue: "non_canonical_thread_key", expected: "+1XXXXXXXXXX" },
    };
  }
  return { ok: true };
}

export function checkCanonicalPhone(phone, field = "phone") {
  const normalized = normalizePhone(clean(phone));
  if (!normalized) {
    return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", diagnostics: { field, raw: phone, issue: "invalid_phone" } };
  }
  return { ok: true, normalized };
}

// ── System control flags ──────────────────────────────────────────────────

export async function checkSmsEnabled() {
  const flags = await getSystemFlags(["outbound_sms_enabled"]);
  if (!flags.outbound_sms_enabled) {
    return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", flag_key: "outbound_sms_enabled" };
  }
  return { ok: true };
}

export async function checkAutoReplyEnabled() {
  const flags = await getSystemFlags(["auto_reply_enabled"]);
  if (!flags.auto_reply_enabled) {
    return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", flag_key: "auto_reply_enabled" };
  }
  return { ok: true };
}

export async function checkFollowupEnabled() {
  const flags = await getSystemFlags(["followup_enabled"]);
  if (!flags.followup_enabled) {
    return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", flag_key: "followup_enabled" };
  }
  return { ok: true };
}

export async function checkQueueRunnerEnabled() {
  const flags = await getSystemFlags(["queue_runner_enabled"]);
  if (!flags.queue_runner_enabled) {
    return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", flag_key: "queue_runner_enabled" };
  }
  return { ok: true };
}

// ── Queue row validation ──────────────────────────────────────────────────

const TERMINAL_STATUSES = new Set([
  "sent", "failed", "blocked", "paused_name_missing",
  "paused_invalid_queue_row", "paused_duplicate", "paused_global_lock",
  "paused_max_retries", "cancelled",
]);

export function isTerminalStatus(status) {
  return TERMINAL_STATUSES.has(lower(status));
}

export function checkQueueRowExists(row, queue_item_id) {
  if (!row) {
    return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", diagnostics: { queue_item_id, issue: "row_not_found" } };
  }
  return { ok: true };
}

export function checkNotPausedReview(row) {
  if (lower(row?.queue_status) === "paused_review") {
    return {
      ok: false,
      reason: "QUEUE_SAFETY_BLOCKED",
      diagnostics: { queue_status: row.queue_status, issue: "paused_review_not_runnable" },
    };
  }
  return { ok: true };
}

export function checkNotTerminal(row) {
  if (isTerminalStatus(row?.queue_status)) {
    return {
      ok: false,
      reason: "QUEUE_SAFETY_BLOCKED",
      diagnostics: { queue_status: row.queue_status, issue: "terminal_status_not_modifiable" },
    };
  }
  return { ok: true };
}

export function checkCancel(row) {
  // Cancel is allowed from any non-final state except already cancelled/sent
  const final = new Set(["sent", "cancelled"]);
  if (final.has(lower(row?.queue_status))) {
    return {
      ok: false,
      reason: "QUEUE_SAFETY_BLOCKED",
      diagnostics: { queue_status: row.queue_status, issue: "already_final" },
    };
  }
  return { ok: true };
}

export function checkCanonicalQueueThreadKey(row) {
  const thread_key = clean(row?.thread_key);
  if (thread_key && !CANONICAL_E164_RE.test(thread_key)) {
    return {
      ok: false,
      reason: "QUEUE_SAFETY_BLOCKED",
      diagnostics: { thread_key, issue: "noncanonical_thread_key" },
    };
  }
  return { ok: true };
}

// ── Seller identity / template ────────────────────────────────────────────

export function checkSellerIdentity(row) {
  const body = clean(row?.message_body || row?.message_text);
  // Detect blank greeting: "Hey ," "Hi ," "Hello ,"
  if (/\b(?:hey|hi|hello)\s*,\s/i.test(body) && !/\bhey\s+\w{2,}/i.test(body)) {
    return {
      ok: false,
      reason: "SELLER_IDENTITY_UNRESOLVED",
      diagnostics: { issue: "blank_greeting_detected", body_preview: body.slice(0, 80) },
    };
  }
  return { ok: true };
}

export function checkMessageBody(message_body) {
  if (!clean(message_body)) {
    return { ok: false, reason: "TEMPLATE_UNRESOLVED", diagnostics: { issue: "empty_message_body" } };
  }
  return { ok: true };
}

// ── Thread classification ─────────────────────────────────────────────────

export async function checkNoNegativeClassification(thread_key, supabase = defaultSupabase) {
  if (!thread_key) return { ok: true };

  try {
    const { data } = await supabase
      .from("message_events")
      .select("is_wrong_number, is_not_interested, is_dnc, direction, created_at")
      .eq("thread_key", thread_key)
      .eq("direction", "inbound")
      .order("created_at", { ascending: false })
      .limit(10);

    if (!data?.length) return { ok: true };

    for (const row of data) {
      if (row.is_wrong_number === true) {
        return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", diagnostics: { issue: "wrong_number", thread_key } };
      }
      if (row.is_not_interested === true) {
        return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", diagnostics: { issue: "not_interested", thread_key } };
      }
      if (row.is_dnc === true) {
        return { ok: false, reason: "QUEUE_SAFETY_BLOCKED", diagnostics: { issue: "is_dnc", thread_key } };
      }
    }
  } catch {
    // Non-fatal — fail open to avoid blocking valid sends on DB hiccup
  }

  return { ok: true };
}

// ── Queue row loader ──────────────────────────────────────────────────────

export async function loadQueueRow(queue_item_id, supabase = defaultSupabase) {
  const id = clean(queue_item_id);
  if (!id) return null;
  const { data } = await supabase
    .from("send_queue")
    .select("id, queue_key, queue_status, thread_key, to_phone_number, from_phone_number, message_body, message_text, scheduled_for, retry_count, max_retries, is_locked, master_owner_id, property_id, metadata, created_at, updated_at")
    .eq("id", id)
    .maybeSingle();
  return data || null;
}

// ── Composite safety check for queue mutations ────────────────────────────

export async function runQueueMutationSafety(queue_item_id, { allow_paused_review = false, allow_terminal = false, check_sms = true } = {}, supabase = defaultSupabase) {
  const row = await loadQueueRow(queue_item_id, supabase);
  const exists = checkQueueRowExists(row, queue_item_id);
  if (!exists.ok) return { ok: false, ...exists };

  if (!allow_paused_review) {
    const pr = checkNotPausedReview(row);
    if (!pr.ok) return { ok: false, row, ...pr };
  }
  if (!allow_terminal) {
    const term = checkNotTerminal(row);
    if (!term.ok) return { ok: false, row, ...term };
  }

  const threadCheck = checkCanonicalQueueThreadKey(row);
  if (!threadCheck.ok) return { ok: false, row, ...threadCheck };

  if (check_sms) {
    const smsCheck = await checkSmsEnabled();
    if (!smsCheck.ok) return { ok: false, row, ...smsCheck };
  }

  return { ok: true, row };
}
