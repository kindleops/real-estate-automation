/**
 * Slash Command: /reply-sms
 *
 * Supports:
 * - auto template approval by message_event_id
 * - explicit template_id approval
 * - manual fallback reply text
 */

import { child } from "@/lib/logging/logger.js";
import { getDefaultSupabaseClient } from "@/lib/supabase/default-client.js";
import {
  auditSlashCommand,
  auditReplyBlocked,
} from "@/lib/discord/reply-sms-audit.js";
import { clean } from "@/lib/utils/strings.js";
import { ephemeralMessage } from "@/lib/discord/discord-response-helpers.js";

const logger = child({ module: "discord.slash_commands.reply_sms" });

function normalizeReplyMode(value) {
  const mode = clean(value).toLowerCase();
  if (["auto_template", "template", "manual"].includes(mode)) return mode;
  return "auto_template";
}

/**
 * Handle /reply-sms slash command
 */
export async function handleReplySmsCommand(context = {}, options = {}) {
  const { user_id, channel_id } = context;
  const {
    message_event_id = "",
    reply_text = "",
    send_now = false,
    reply_mode = "auto_template",
    template_id = "",
  } = options;

  const normalized_mode = normalizeReplyMode(reply_mode);
  const trimmed_event_id = clean(message_event_id);

  logger.debug("reply_sms_command", {
    message_event_id: trimmed_event_id.slice(0, 8),
    reply_mode: normalized_mode,
    has_template_id: Boolean(clean(template_id)),
    has_manual_reply_text: Boolean(clean(reply_text)),
    send_now,
  });

  if (!trimmed_event_id) {
    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id,
        reply_text,
        send_now,
        status: "failed",
        error: new Error("missing_message_event_id"),
      },
      getDefaultSupabaseClient()
    ).catch(() => {});

    return ephemeralMessage("❌ Required: message_event_id");
  }

  const supabase = getDefaultSupabaseClient();

  try {
    const endpoint_response = await fetch("/api/internal/discord/reply-sms", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Internal-API-Secret": process.env.INTERNAL_API_SECRET || "",
      },
      body: JSON.stringify({
        message_event_id: trimmed_event_id,
        reply_mode: normalized_mode,
        template_id: clean(template_id) || undefined,
        reply_text: clean(reply_text) || undefined,
        send_now,
        approved_by_discord_user_id: user_id,
        source_channel_id: channel_id,
        action_type: "slash_command_reply",
      }),
    });

    const endpoint_data = await endpoint_response.json();

    if (!endpoint_response.ok || !endpoint_data.ok) {
      await auditReplyBlocked(
        {
          discord_user_id: user_id,
          channel_id,
          message_event_id: trimmed_event_id,
          reply_text,
          action_type: "reply_sms_command",
          block_reason: endpoint_data?.reason || "endpoint_error",
          details: endpoint_data?.details || {},
        },
        supabase
      ).catch(() => {});

      throw new Error(endpoint_data.message || endpoint_data.reason || "endpoint_error");
    }

    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id: trimmed_event_id,
        reply_text,
        send_now,
        status: "success",
      },
      supabase
    ).catch(() => {});

    const phone_preview = (endpoint_data.to_phone_number || "").slice(-4).padStart(4, "*");
    const summary_lines = [
      `✅ Reply queued to **${phone_preview}**`,
      `Queue ID: \`${String(endpoint_data.queue_id || "").slice(0, 8)}\``,
      `Mode: ${endpoint_data.reply_mode || normalized_mode}`,
    ];

    if (endpoint_data.selected_template_id) {
      summary_lines.push(`Template: ${endpoint_data.selected_template_id}`);
    }

    if (endpoint_data.rendered_message_preview) {
      summary_lines.push(`Preview: "${endpoint_data.rendered_message_preview}"`);
    }

    return ephemeralMessage(summary_lines.join("\n"));
  } catch (err) {
    logger.error("slash_command_error", {
      error: err?.message,
      reply_mode: normalized_mode,
    });

    await auditSlashCommand(
      {
        discord_user_id: user_id,
        channel_id,
        message_event_id: trimmed_event_id,
        reply_text,
        send_now,
        status: "failed",
        error: err,
      },
      supabase
    ).catch(() => {});

    return ephemeralMessage(`❌ Error: ${err?.message}`);
  }
}

export function __setReplySmsCommandDeps() {}
export function __resetReplySmsCommandDeps() {}
