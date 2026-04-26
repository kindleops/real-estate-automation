/**
 * Discord SMS Reply Audit Logging
 *
 * Tracks all Discord-initiated SMS replies for compliance, debugging, and audit trails.
 * Records to discord_action_audit table with enriched context.
 */

import { nowIso } from "@/lib/utils/dates.js";
import { clean } from "@/lib/utils/strings.js";

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}
import { child } from "@/lib/logging/logger.js";

const logger = child({ module: "discord.reply_sms_audit" });

const AUDIT_TABLE = "discord_action_audit";

/**
 * Log a Discord SMS reply action
 *
 * @param {Object} payload - Audit record
 * @param {string} payload.action = "sms_reply"
 * @param {string} payload.discord_user_id - User who initiated reply
 * @param {string} payload.channel_id - Discord channel where action taken
 * @param {string} payload.message_id - Discord message that prompted action
 * @param {string} payload.message_event_id - Inbound message event ID
 * @param {string} payload.send_queue_id - Queued outbound send (if created)
 * @param {string} payload.status - "success" | "blocked" | "failed"
 * @param {string} payload.status_reason - Detailed reason (e.g., "duplicate_reply", "opt_out")
 * @param {string} payload.reply_text - The reply being sent (first 500 chars)
 * @param {Object} payload.metadata - Additional context
 * @param {string} payload.error - Error message if failed
 */
async function auditReplyAction(payload = {}, supabase = null) {
  const now = nowIso();
  const record = {
    action: clean(payload.action) || "sms_reply",
    action_type: clean(payload.action_type) || "sms_reply",
    discount_user_id: clean(payload.discord_user_id) || null,
    user_id: clean(payload.discord_user_id) || null,
    channel_id: clean(payload.channel_id) || null,
    message_id: clean(payload.message_id) || null,
    message_event_id: clean(payload.message_event_id) || null,
    send_queue_id: clean(payload.send_queue_id) || null,
    status: clean(payload.status) || "unknown",
    status_reason: clean(payload.status_reason) || null,
    error: clean(payload.error) || null,
    metadata: {
      reply_text_preview: clean(payload.reply_text || "").slice(0, 500),
      reply_length: (clean(payload.reply_text || "") || "").length,
      metadata: ensureObject(payload.metadata),
      req_body_keys: Object.keys(ensureObject(payload.req_body || {})),
      source: "discord_sms_reply",
      ...ensureObject(payload.metadata),
    },
    created_at: now,
  };

  if (!supabase) {
    logger.warn("no_supabase_for_audit", {
      action: record.action,
      status: record.status,
      reason: record.status_reason,
    });
    return record; // Audit is best-effort
  }

  try {
    const { data, error } = await supabase
      .from(AUDIT_TABLE)
      .insert(record)
      .select()
      .maybeSingle();

    if (error) {
      logger.warn("audit_insert_error", {
        error: error?.message,
        status: record.status,
        message_event_id: record.message_event_id,
      });
      // Don't throw — audit is best-effort, primary operation must succeed
      return record;
    }

    return data || record;
  } catch (err) {
    logger.warn("audit_exception", {
      error: err?.message,
      status: record.status,
      message_event_id: record.message_event_id,
    });
    return record; // Audit is best-effort
  }
}

/**
 * Log successful reply queue action
 */
export async function auditReplyQueued(
  {
    discord_user_id = "",
    channel_id = "",
    message_id = "",
    message_event_id = "",
    send_queue_id = "",
    reply_text = "",
    action_type = "send_suggested_sms_reply",
    metadata = {},
  } = {},
  supabase = null
) {
  return auditReplyAction(
    {
      action: "sms_reply",
      action_type,
      discord_user_id,
      channel_id,
      message_id,
      message_event_id,
      send_queue_id,
      status: "success",
      status_reason: "reply_queued",
      reply_text,
      metadata,
    },
    supabase
  );
}

/**
 * Log blocked/failed reply attempt
 */
export async function auditReplyBlocked(
  {
    discord_user_id = "",
    channel_id = "",
    message_id = "",
    message_event_id = "",
    reply_text = "",
    action_type = "send_suggested_sms_reply",
    block_reason = "unknown",
    details = {},
    error = null,
  } = {},
  supabase = null
) {
  return auditReplyAction(
    {
      action: "sms_reply",
      action_type,
      discord_user_id,
      channel_id,
      message_id,
      message_event_id,
      status: "blocked",
      status_reason: block_reason,
      reply_text,
      error: error?.message || clean(error),
      metadata: details,
    },
    supabase
  );
}

/**
 * Log action handlers (modal submit, button click)
 */
export async function auditActionHandler(
  {
    action_type = "",
    discord_user_id = "",
    channel_id = "",
    message_id = "",
    details = {},
    error = null,
  } = {},
  supabase = null
) {
  const status = error ? "failed" : "success";
  const reason = error ? `handler_error: ${error?.message}` : `${action_type}_initiated`;

  return auditReplyAction(
    {
      action: action_type,
      action_type,
      discord_user_id,
      channel_id,
      message_id,
      status,
      status_reason: reason,
      error: error?.message || null,
      metadata: details,
    },
    supabase
  );
}

/**
 * Log slash command invocation
 */
export async function auditSlashCommand(
  {
    discord_user_id = "",
    channel_id = "",
    interaction_id = "",
    message_event_id = "",
    reply_text = "",
    send_now = false,
    status = "success",
    error = null,
  } = {},
  supabase = null
) {
  return auditReplyAction(
    {
      action: "sms_reply_slash_command",
      action_type: "reply_sms_command",
      discord_user_id,
      channel_id,
      message_id: interaction_id,
      message_event_id,
      status,
      status_reason: error ? "command_failed" : "command_invoked",
      reply_text,
      error: error?.message || null,
      metadata: {
        send_now,
        interaction_id,
      },
    },
    supabase
  );
}
