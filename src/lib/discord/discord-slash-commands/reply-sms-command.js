/**
 * Slash Command: /reply-sms
 *
 * Usage:
 * /reply-sms message_event_id: <event_id> reply_text: <text> [send_now: true/false]
 */

import { child } from "@/lib/logging/logger.js";
import { getDefaultSupabaseClient } from "@/lib/supabase/default-client.js";
import {
  runReplySmsSafetyChecks,
  generateReplyHash,
} from "@/lib/discord/reply-sms-safety-checks.js";
import {
  auditSlashCommand,
  auditReplyQueued,
  auditReplyBlocked,
} from "@/lib/discord/reply-sms-audit.js";
import { clean } from "@/lib/utils/strings.js";
import { ephemeralMessage } from "@/lib/discord/discord-response-helpers.js";

const logger = child({ module: "discord.slash_commands.reply_sms" });

/**
 * Handle /reply-sms slash command
 * Options:
 * - message_event_id (required, string)
 * - reply_text (required, string)
 * - send_now (optional, boolean)
 */
export async function handleReplySmsCommand(context = {}, options = {}) {
  const { user_id, guild_id, channel_id } = context;
  const { message_event_id = "", reply_text = "", send_now = false } = options;

  logger.debug("reply_sms_command", {
    message_event_id: message_event_id.slice(0, 8),
    reply_length: reply_text.length,
    send_now,
  });

  // Validate inputs
  if (!message_event_id || !reply_text) {
    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id,
        reply_text,
        status: "failed",
        error: new Error("missing_required_args"),
      },
      getDefaultSupabaseClient()
    ).catch(() => {});

    return ephemeralMessage("❌ Required: message_event_id and reply_text");
  }

  const supabase = getDefaultSupabaseClient();

  // Run safety checks
  const safety_result = await runReplySmsSafetyChecks(
    {
      message_event_id,
      reply_text,
      supabase,
    }
  );

  if (!safety_result.safe) {
    logger.warn("safety_check_failed", {
      reason: safety_result.reason,
    });

    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id,
        reply_text,
        status: "failed",
        error: new Error(safety_result.reason),
      },
      supabase
    ).catch(() => {});

    await auditReplyBlocked(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id,
        reply_text,
        action_type: "slash_command",
        block_reason: safety_result.reason,
        details: safety_result.details,
      },
      supabase
    ).catch(() => {});

    return ephemeralMessage(`🚫 ${safety_result.message}`);
  }

  // Queue reply via endpoint
  try {
    const endpoint_response = await fetch("/api/internal/discord/reply-sms", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Internal-Secret": process.env.INTERNAL_API_SECRET || "",
      },
      body: JSON.stringify({
        message_event_id,
        reply_text,
        send_now,
        approved_by_discord_user_id: user_id,
        source_channel_id: channel_id,
        action_type: "slash_command_reply",
      }),
    });

    const endpoint_data = await endpoint_response.json();

    if (!endpoint_response.ok || !endpoint_data.ok) {
      throw new Error(endpoint_data.message || "endpoint_error");
    }

    logger.info("slash_command_success", {
      queue_id: endpoint_data.queue_id?.slice(0, 8),
    });

    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id,
        reply_text,
        send_now,
        status: "success",
      },
      supabase
    ).catch(() => {});

    const phone_preview = (endpoint_data.to_phone_number || "").slice(-4).padStart(4, "*");
    return ephemeralMessage(
      `✅ Reply queued to **${phone_preview}**\n` +
      `Queue ID: \`${endpoint_data.queue_id.slice(0, 8)}\`\n` +
      `Preview: "${endpoint_data.preview}..."`
    );
  } catch (err) {
    logger.error("slash_command_error", {
      error: err?.message,
    });

    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id,
        reply_text,
        status: "failed",
        error: err,
      },
      supabase
    ).catch(() => {});

    return ephemeralMessage(`❌ Error: ${err?.message}`);
  }
}

/**
 * Export DI functions for testing
 */
export function __setReplySmsCommandDeps(overrides = {}) {
  // Placeholder
}

export function __resetReplySmsCommandDeps() {
  // Placeholder
}
