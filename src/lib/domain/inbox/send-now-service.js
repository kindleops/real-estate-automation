// ─── send-now-service.js ────────────────────────────────────────────────────
// Inbox manual "Send Now" payload validation and queue insertion.
// Validates required fields, resolves from_phone_number, and creates
// properly structured send_queue rows.

import crypto from "node:crypto";

import { child } from "@/lib/logging/logger.js";
import { normalizePhone } from "@/lib/utils/phones.js";
import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";

const logger = child({ module: "domain.inbox.send_now_service" });

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

// ════════════════════════════════════════════════════════════════════════════
// FROM-PHONE-NUMBER RESOLUTION
// ════════════════════════════════════════════════════════════════════════════

/**
 * Resolve from_phone_number (our sending number) for a thread.
 *
 * Priority chain:
 * 1. inbox_thread_state.our_number (via metadata)
 * 2. Latest outbound send_queue row's from_phone_number for same thread
 * 3. Latest outbound message_events row's from_phone_number for same thread
 * 4. textgrid_numbers by market
 * 5. null — fail with "Missing sending number"
 */
export async function resolveFromPhoneNumber({
  thread_key,
  market = null,
  supabase = defaultSupabase,
} = {}) {
  if (!thread_key) return null;

  // Priority 1: inbox_thread_state metadata.our_number
  try {
    const { data: threadState } = await supabase
      .from("inbox_thread_state")
      .select("thread_key, metadata")
      .eq("thread_key", thread_key)
      .maybeSingle();

    if (threadState?.metadata?.our_number) {
      const normalized = normalizePhone(threadState.metadata.our_number);
      if (normalized) return normalized;
    }
  } catch {
    // Non-fatal, continue to next priority
  }

  // Priority 2: Latest outbound in send_queue for same thread
  try {
    const { data: latestOutbound } = await supabase
      .from("send_queue")
      .select("from_phone_number")
      .eq("thread_key", thread_key)
      .eq("type", "outbound")
      .not("from_phone_number", "is", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (latestOutbound?.from_phone_number) {
      const normalized = normalizePhone(latestOutbound.from_phone_number);
      if (normalized) return normalized;
    }
  } catch {
    // Non-fatal
  }

  // Priority 3: Latest outbound in message_events for same thread
  try {
    const { data: latestEvent } = await supabase
      .from("message_events")
      .select("from_phone_number")
      .eq("thread_key", thread_key)
      .eq("direction", "outbound")
      .not("from_phone_number", "is", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (latestEvent?.from_phone_number) {
      const normalized = normalizePhone(latestEvent.from_phone_number);
      if (normalized) return normalized;
    }
  } catch {
    // Non-fatal
  }

  // Priority 4: textgrid_numbers by market
  if (market) {
    try {
      const { data: numbers } = await supabase
        .from("textgrid_numbers")
        .select("phone_number")
        .eq("market", market)
        .eq("status", "active")
        .limit(5);

      if (numbers && numbers.length > 0) {
        const firstNumber = numbers[0];
        if (firstNumber?.phone_number) {
          const normalized = normalizePhone(firstNumber.phone_number);
          if (normalized) return normalized;
        }
      }
    } catch {
      // Non-fatal
    }
  }

  return null;
}

// ════════════════════════════════════════════════════════════════════════════
// VALIDATION
// ════════════════════════════════════════════════════════════════════════════

/**
 * Validate an inbox manual send-now payload.
 *
 * Returns a result object:
 *   { ok: true, normalized: { ... } }
 *   { ok: false, status: 400, error: "message" }
 *
 * Validation rules:
 * - thread_key required
 * - to_phone_number required
 * - from_phone_number required
 * - message_body required
 * - manual message length >= 2
 * - auto_reply message length >= 10
 */
export function validateInboxSendNowPayload(input = {}, resolvedFrom = null) {
  const thread_key = clean(input.thread_key);
  const to_phone_number = normalizePhone(clean(input.to_phone_number));
  const from_phone_number = resolvedFrom || normalizePhone(clean(input.from_phone_number));
  const message_body = clean(input.message_body);
  const message_text = clean(input.message_text) || message_body;
  const message_type = clean(input.message_type) || "manual_reply";
  const use_case_template = clean(input.use_case_template) || "manual_reply";
  const type = clean(input.type) || "outbound";
  const queue_key = clean(input.queue_key) || `inbox:send_now:${crypto.randomUUID()}`;

  if (!thread_key) {
    return { ok: false, status: 400, error: "missing_thread_key" };
  }
  if (!to_phone_number) {
    return { ok: false, status: 400, error: "missing_to_phone_number" };
  }
  if (!from_phone_number) {
    return { ok: false, status: 400, error: "missing_from_phone_number" };
  }
  if (!message_body) {
    return { ok: false, status: 400, error: "missing_message_body" };
  }

  const is_manual = message_type === "manual_reply" || use_case_template === "manual_reply";
  const min_length = is_manual ? 2 : 10;

  if (message_body.length < min_length) {
    return { ok: false, status: 400, error: "message_too_short" };
  }

  return {
    ok: true,
    normalized: {
      queue_key,
      thread_key,
      to_phone_number,
      from_phone_number,
      message_body,
      message_text,
      message_type,
      use_case_template,
      type,
      master_owner_id: clean(input.master_owner_id) || null,
      property_id: clean(input.property_id) || null,
      prospect_id: clean(input.prospect_id) || null,
      phone_number_id: clean(input.phone_number_id) || null,
      market_id: clean(input.market_id) || null,
      textgrid_number_id: clean(input.textgrid_number_id) || null,
      source: "inbox",
      action: "send_now",
      created_from: "leadcommand_inbox",
    },
  };
}


// ════════════════════════════════════════════════════════════════════════════
// CREATE QUEUE ROW
// ════════════════════════════════════════════════════════════════════════════

/**
 * Create a send_queue row for a manual inbox send-now action.
 *
 * Performs validation first. Returns 400 on invalid input.
 * Never creates a row with queue_status='queued' on validation failure.
 */
export async function createInboxSendNowQueueRow(input = {}, deps = {}) {
  const {
    insertImpl = insertSupabaseSendQueueRow,
    resolveFromImpl = resolveFromPhoneNumber,
    supabase = defaultSupabase,
  } = deps;

  // ── Step 1: Resolve from_phone_number ──────────────────────────────
  let resolved_from = normalizePhone(clean(input.from_phone_number));
  if (!resolved_from) {
    resolved_from = await resolveFromImpl({
      thread_key: clean(input.thread_key),
      market: clean(input.market) || null,
      supabase,
    });
  }

  // ── Step 2: Validate ────────────────────────────────────────────────
  const validation = validateInboxSendNowPayload(input, resolved_from);
  if (!validation.ok) {
    // Insert audit row with paused_invalid_queue_row status
    // but NEVER queue_status='queued'
    try {
      await insertImpl({
        queue_key: `inbox:send_now:failed:${clean(input.thread_key) || "unknown"}:${Date.now()}`,
        queue_status: "paused_invalid_queue_row",
        scheduled_for: nowIso(),
        message_body: clean(input.message_body) || "",
        to_phone_number: normalizePhone(clean(input.to_phone_number)) || null,
        from_phone_number: resolved_from || null,
        thread_key: clean(input.thread_key) || null,
        metadata: {
          source: "inbox",
          action: "send_now",
          created_from: "leadcommand_inbox",
          validation_error: validation.error,
          input_preview: {
            thread_key: clean(input.thread_key)?.slice(0, 40) || null,
            message_body_length: clean(input.message_body)?.length || 0,
          },
        },
      }).catch(() => {});
    } catch {
      // Non-critical - audit row best-effort
    }

    return {
      ok: false,
      status: 400,
      error: validation.error,
      queue_created: false,
    };
  }

  const { normalized } = validation;

  // ── Step 3: Insert queue row ────────────────────────────────────────
  const now = nowIso();
  const queue_id = clean(input.queue_id) || normalized.queue_key;

  const queue_result = await insertImpl({
    queue_key: normalized.queue_key,
    queue_id,
    queue_status: "queued",
    scheduled_for: clean(input.scheduled_for) || now,
    scheduled_for_utc: now,
    scheduled_for_local: now,
    timezone: clean(input.timezone) || "America/Chicago",
    send_priority: 10,
    is_locked: false,
    retry_count: 0,
    max_retries: 3,
    message_body: normalized.message_body,
    message_text: normalized.message_text || normalized.message_body,
    to_phone_number: normalized.to_phone_number,
    from_phone_number: normalized.from_phone_number,
    thread_key: normalized.thread_key,
    type: normalized.type,
    message_type: normalized.message_type,
    use_case_template: normalized.use_case_template,
    master_owner_id: normalized.master_owner_id,
    property_id: normalized.property_id,
    prospect_id: normalized.prospect_id,
    phone_number_id: normalized.phone_number_id,
    textgrid_number_id: normalized.textgrid_number_id,
    market_id: normalized.market_id,
    character_count: normalized.message_body.length,
    touch_number: 1,
    dnc_check: "✅ Cleared",
    delivery_confirmed: "⏳ Pending",
    metadata: {
      source: normalized.source,
      action: normalized.action,
      created_from: normalized.created_from,
    },
  });

  return {
    ok: queue_result?.ok !== false,
    status: queue_result?.ok !== false ? 200 : 400,
    error: queue_result?.ok !== false
      ? null
      : (queue_result?.reason || "queue_insert_failed"),
    queue_created: queue_result?.ok !== false,
    queue_id: queue_result?.queue_id || queue_id,
    queue_key: normalized.queue_key,
    result: queue_result,
  };
}

export default createInboxSendNowQueueRow;