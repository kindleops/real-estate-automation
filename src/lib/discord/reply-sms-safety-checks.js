/**
 * Discord SMS Reply Safety Checks
 *
 * Validates that a Discord reply is safe before queuing:
 * - Inbound event exists and direction is inbound
 * - Reply text is valid (non-empty, under SMS limit)
 * - Recipient not suppressed/opted-out
 * - From number is in our TextGrid inventory
 * - No duplicate reply for same inbound event
 */

import { normalizePhone } from "@/lib/utils/phones.js";
import { clean } from "@/lib/utils/strings.js";
import { nowIso } from "@/lib/utils/dates.js";

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

const MAX_REPLY_LENGTH = 160; // SMS safe length
const HASH_GENERATION_VERSION = "v1";

/**
 * Generate hash for deduplication (reply_text + inbound event ID)
 */
export function generateReplyHash(reply_text = "", message_event_id = "") {
  if (typeof globalThis.crypto?.subtle !== "undefined") {
    // Use SubtleCrypto if available (browser/Node 15+)
    try {
      const text = `${clean(reply_text)}:${clean(message_event_id)}:${HASH_GENERATION_VERSION}`;
      const encoder = new TextEncoder();
      const data = encoder.encode(text);
      // For now, just use a simple checksum in Node/serverless context
      // Can be upgraded to actual SHA256 if needed
      let hash = 0;
      for (let i = 0; i < data.length; i++) {
        hash = ((hash << 5) - hash) + data[i];
        hash = hash & 0xffffffff; // Convert to 32-bit integer
      }
      return `reply:${HASH_GENERATION_VERSION}:${Math.abs(hash).toString(16)}`;
    } catch {}
  }

  // Fallback: simple checksum
  const text = `${clean(reply_text)}:${clean(message_event_id)}:${HASH_GENERATION_VERSION}`;
  let hash = 0;
  for (let i = 0; i < text.length; i++) {
    hash = ((hash << 5) - hash) + text.charCodeAt(i);
    hash = hash & 0xffffffff;
  }
  return `reply:${HASH_GENERATION_VERSION}:${Math.abs(hash).toString(16)}`;
}

/**
 * Validate reply text (non-empty, valid length)
 */
export function validateReplyText(reply_text = "") {
  const trimmed = clean(reply_text);

  if (!trimmed) {
    return {
      valid: false,
      reason: "empty_reply_text",
      message: "Reply text is required",
    };
  }

  if (trimmed.length > 480) {
    // Allow up to ~3 SMS segments worth
    return {
      valid: false,
      reason: "reply_text_exceeds_max_length",
      message: `Reply exceeds 480 characters (was ${trimmed.length})`,
      max_length: 480,
      actual_length: trimmed.length,
    };
  }

  return {
    valid: true,
    reason: "reply_text_valid",
    text: trimmed,
    length: trimmed.length,
  };
}

/**
 * Check if inbound event exists and is inbound direction
 */
export async function validateInboundMessageEvent(message_event_id = "", supabase = null) {
  const event_id = clean(message_event_id);

  if (!event_id) {
    return {
      valid: false,
      reason: "missing_message_event_id",
      message: "message_event_id required",
    };
  }

  if (!supabase) {
    return {
      valid: false,
      reason: "missing_supabase",
      message: "Database connection required",
    };
  }

  try {
    const { data, error } = await supabase
      .from("message_events")
      .select("id, direction, from_phone_number, to_phone_number, master_owner_id, prospect_id, property_id, textgrid_number_id, conversation_brain_id, metadata, created_at")
      .eq("id", event_id)
      .maybeSingle();

    if (error) throw error;

    if (!data) {
      return {
        valid: false,
        reason: "message_event_not_found",
        message: `Message event ${event_id} not found`,
      };
    }

    if (data.direction !== "inbound") {
      return {
        valid: false,
        reason: "not_inbound_direction",
        message: `Cannot reply to ${data.direction} event`,
        event_direction: data.direction,
      };
    }

    // Extract phone numbers
    const from_phone = normalizePhone(data.from_phone_number || data.metadata?.from_phone);
    const to_phone = normalizePhone(data.to_phone_number || data.metadata?.to_phone);

    if (!from_phone || !to_phone) {
      return {
        valid: false,
        reason: "missing_phone_numbers_in_event",
        message: "Inbound event has missing phone numbers",
        from_phone,
        to_phone,
      };
    }

    return {
      valid: true,
      reason: "inbound_event_valid",
      event: {
        id: data.id,
        from_phone_number: from_phone,
        to_phone_number: to_phone,
        master_owner_id: data.master_owner_id,
        prospect_id: data.prospect_id,
        property_id: data.property_id,
        textgrid_number_id: data.textgrid_number_id,
        conversation_brain_id: data.conversation_brain_id,
        metadata: ensureObject(data.metadata),
        created_at: data.created_at,
      },
    };
  } catch (err) {
    return {
      valid: false,
      reason: "database_error",
      message: `Database error: ${err?.message}`,
      error: err?.message,
    };
  }
}

/**
 * Check if recipient is suppressed or opted out
 */
export async function validateRecipientNotSuppressed(
  to_phone_number = "",
  message_event_metadata = {},
  supabase = null
) {
  const phone = normalizePhone(to_phone_number);

  if (!phone) {
    return {
      valid: false,
      reason: "invalid_phone_number",
      message: "Invalid phone number",
    };
  }

  if (!supabase) {
    return {
      valid: false,
      reason: "missing_supabase",
      message: "Database connection required",
    };
  }

  const metadata = ensureObject(message_event_metadata);
  const event_type = metadata.event_type || "";

  // Cannot auto-reply to opt-out events (but allow if explicitly approved by user)
  if (event_type === "opt_out" || event_type === "inbound_opt_out") {
    return {
      valid: false,
      reason: "recipient_opted_out",
      message: `Cannot reply: Recipient opted out (handle as internal note only)`,
      is_opt_out: true,
    };
  }

  // Cannot reply to wrong number scenarios
  if (event_type === "wrong_number" || event_type === "inbound_wrong_number") {
    return {
      valid: false,
      reason: "wrong_number_scenario",
      message: `Cannot reply: Classified as wrong number (handle as internal note only)`,
      is_wrong_number: true,
    };
  }

  try {
    const { data: suppressed, error: supp_error } = await supabase
      .from("sms_suppression_list")
      .select("id, suppression_reason, suppressed_at")
      .eq("phone_number", phone)
      .eq("is_active", true)
      .maybeSingle();

    if (supp_error) throw supp_error;

    if (suppressed) {
      return {
        valid: false,
        reason: "recipient_suppressed",
        message: `Recipient suppressed (${suppressed.suppression_reason})`,
        suppression_reason: suppressed.suppression_reason,
        suppressed_at: suppressed.suppressed_at,
      };
    }

    return {
      valid: true,
      reason: "recipient_not_suppressed",
    };
  } catch (err) {
    // If suppression list unavailable, allow reply but log warning
    return {
      valid: true,
      reason: "suppression_check_skipped",
      warning: `Could not verify suppression list: ${err?.message}`,
      error: err?.message,
    };
  }
}

/**
 * Verify from_phone_number is in our TextGrid inventory
 */
export async function validateFromPhoneIsOurs(
  from_phone_number = "",
  supabase = null
) {
  const phone = normalizePhone(from_phone_number);

  if (!phone) {
    return {
      valid: false,
      reason: "invalid_phone_number",
      message: "Invalid from phone number",
    };
  }

  if (!supabase) {
    return {
      valid: false,
      reason: "missing_supabase",
      message: "Database connection required",
    };
  }

  try {
    const { data: textgrid_number, error } = await supabase
      .from("textgrid_numbers")
      .select("id, phone_number, status, metadata")
      .eq("phone_number", phone)
      .maybeSingle();

    if (error) throw error;

    if (!textgrid_number) {
      return {
        valid: false,
        reason: "phone_not_in_textgrid_inventory",
        message: `Phone ${phone} is not in our TextGrid inventory`,
      };
    }

    const status = clean(textgrid_number.status).toLowerCase();
    if (status !== "active") {
      return {
        valid: false,
        reason: "textgrid_number_not_active",
        message: `TextGrid number ${phone} is not active (status: ${status})`,
        status,
      };
    }

    return {
      valid: true,
      reason: "from_phone_valid_and_active",
      textgrid_number_id: textgrid_number.id,
      phone_number: phone,
    };
  } catch (err) {
    return {
      valid: false,
      reason: "database_error_checking_textgrid",
      message: `Error verifying TextGrid number: ${err?.message}`,
      error: err?.message,
    };
  }
}

/**
 * Check for duplicate reply (same inbound event + similar reply hash)
 */
export async function validateNoDuplicateReply(
  message_event_id = "",
  reply_text = "",
  supabase = null,
  lookbackMinutes = 10
) {
  const event_id = clean(message_event_id);
  const reply_hash = generateReplyHash(reply_text, message_event_id);

  if (!event_id) {
    return {
      valid: true,
      reason: "skip_check_no_event_id",
    };
  }

  if (!supabase) {
    return {
      valid: true,
      reason: "skip_check_no_supabase",
    };
  }

  try {
    const since_time = new Date(Date.now() - lookbackMinutes * 60000).toISOString();

    const { data: recent_replies, error } = await supabase
      .from("send_queue")
      .select("id, metadata, created_at, message_body")
      .eq("metadata->>inbound_message_event_id", event_id)
      .gt("created_at", since_time)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) throw error;

    const replies = recent_replies || [];

    // Check if exact same reply hash exists
    for (const row of replies) {
      const row_meta = ensureObject(row.metadata);
      const row_hash = row_meta.reply_hash || "";

      if (row_hash === reply_hash) {
        return {
          valid: false,
          reason: "duplicate_reply_detected",
          message: `Duplicate reply already queued (${lookbackMinutes}min lookback)`,
          duplicate_queue_id: row.id,
          duplicate_created_at: row.created_at,
          reply_hash,
        };
      }

      // Also check for exact text match (more strict)
      if (clean(row.message_body) === clean(reply_text)) {
        return {
          valid: false,
          reason: "exact_duplicate_reply_text",
          message: `Exact duplicate reply text found (${lookbackMinutes}min lookback)`,
          duplicate_queue_id: row.id,
          duplicate_created_at: row.created_at,
        };
      }
    }

    return {
      valid: true,
      reason: "no_duplicate_reply",
      recent_reply_count: replies.length,
    };
  } catch (err) {
    // If duplicate check fails, allow reply but log warning
    return {
      valid: true,
      reason: "duplicate_check_skipped",
      warning: `Could not verify duplicates: ${err?.message}`,
      error: err?.message,
    };
  }
}

/**
 * Comprehensive safety check runner
 *
 * Runs all safety checks and returns aggregated result
 */
export async function runReplySmsSafetyChecks(
  {
    message_event_id = "",
    reply_text = "",
    supabase = null,
    inbound_event_override = null,
  } = {},
  deps = {}
) {
  const supabase_client = supabase || deps.supabase;
  const checks = [];
  let inbound_event = inbound_event_override;

  // 1. Validate reply text
  const text_check = validateReplyText(reply_text);
  checks.push({
    name: "reply_text_validation",
    ...text_check,
  });

  if (!text_check.valid) {
    return {
      safe: false,
      reason: "reply_text_invalid",
      message: text_check.message,
      checks,
      details: text_check,
    };
  }

  // 2. Validate inbound message event exists
  if (!inbound_event) {
    const event_check = await validateInboundMessageEvent(message_event_id, supabase_client);
    checks.push({
      name: "inbound_event_validation",
      ...event_check,
    });

    if (!event_check.valid) {
      return {
        safe: false,
        reason: "inbound_event_invalid",
        message: event_check.message,
        checks,
        details: event_check,
      };
    }

    inbound_event = event_check.event;
  }

  // 3. Validate recipient not suppressed
  const suppression_check = await validateRecipientNotSuppressed(
    inbound_event.from_phone_number,
    inbound_event.metadata,
    supabase_client
  );
  checks.push({
    name: "suppression_check",
    ...suppression_check,
  });

  if (!suppression_check.valid) {
    return {
      safe: false,
      reason: "recipient_suppressed_or_opted_out",
      message: suppression_check.message,
      checks,
      details: suppression_check,
    };
  }

  // 4. Validate from phone is ours
  const from_phone_check = await validateFromPhoneIsOurs(
    inbound_event.to_phone_number,
    supabase_client
  );
  checks.push({
    name: "from_phone_validation",
    ...from_phone_check,
  });

  if (!from_phone_check.valid) {
    return {
      safe: false,
      reason: "from_phone_not_in_inventory",
      message: from_phone_check.message,
      checks,
      details: from_phone_check,
    };
  }

  // 5. Check for duplicate reply
  const dup_check = await validateNoDuplicateReply(
    message_event_id,
    reply_text,
    supabase_client,
    10 // 10 minute lookback
  );
  checks.push({
    name: "duplicate_reply_check",
    ...dup_check,
  });

  if (!dup_check.valid) {
    return {
      safe: false,
      reason: "duplicate_reply",
      message: dup_check.message,
      checks,
      details: dup_check,
    };
  }

  // All checks passed
  return {
    safe: true,
    reason: "all_checks_passed",
    message: "Reply is safe to queue",
    checks,
    verified_event: inbound_event,
    reply_hash: generateReplyHash(reply_text, message_event_id),
    textgrid_number_id: from_phone_check.textgrid_number_id,
  };
}

/**
 * Export dependency injection functions for testing
 */
export function __setReplySmsChecksDeps(overrides = {}) {
  // Placeholder for test injection
}

export function __resetReplySmsChecksDeps() {
  // Placeholder for test reset
}
