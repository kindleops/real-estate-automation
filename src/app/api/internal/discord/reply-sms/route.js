/**
 * Discord SMS Reply Endpoint
 * POST /api/internal/discord/reply-sms
 *
 * Receives Discord reply request, validates safety, queues through send_queue
 */

import { NextResponse } from "next/server";
import { child } from "@/lib/logging/logger.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";
import { getDefaultSupabaseClient } from "@/lib/supabase/default-client.js";
import { requireInternalSecret } from "@/lib/security/internal-secret.js";
import {
  runReplySmsSafetyChecks,
  generateReplyHash,
} from "@/lib/discord/reply-sms-safety-checks.js";
import {
  auditReplyQueued,
  auditReplyBlocked,
} from "@/lib/discord/reply-sms-audit.js";
import { normalizePhone } from "@/lib/utils/phones.js";
import { clean } from "@/lib/utils/strings.js";
import { nowIso } from "@/lib/utils/dates.js";

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}
import { v4 as uuid } from "uuid";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";

const logger = child({ module: "api.internal.discord.reply_sms" });
const SEND_QUEUE_TABLE = "send_queue";

function ensureObject(value) {
  if (typeof value === "object" && value !== null) return value;
  return {};
}

/**
 * Insert queued reply into send_queue
 */
async function queueReplyWhenSmsSendQueue(
  {
    message_event_id = "",
    inbound_event = {},
    reply_text = "",
    reply_hash = "",
    action_type = "send_suggested_sms_reply",
    discord_user_id = "",
    channel_id = "",
    message_id = "",
    source_channel_id = "",
    source_message_id = "",
    mode = "queue",
    textgrid_number_id = null,
    send_now = false,
  } = {},
  supabase = null
) {
  if (!supabase) {
    throw new Error("missing_supabase");
  }

  const now = nowIso();
  const queue_key = uuid();
  const queue_id = uuid();

  // Phone swapping: reply goes FROM inbound.to_phone TO inbound.from_phone
  const to_phone = normalizePhone(inbound_event.from_phone_number);
  const from_phone = normalizePhone(inbound_event.to_phone_number);

  if (!to_phone || !from_phone) {
    throw new Error("missing_phone_numbers_in_inbound_event");
  }

  const message_type = action_type === "send_suggested_sms_reply"
    ? "Discord Suggested Reply"
    : "Discord Manual Reply";

  const use_case_template = "discord_sms_reply";

  // Determine touch number from conversation history
  let touch_number = 1;
  if (inbound_event.metadata?.touch_number) {
    touch_number = Number(inbound_event.metadata.touch_number) || 2;
  } else {
    // Count previous messages in conversation
    const { data: previous, error: prev_err } = await supabase
      .from("message_events")
      .select("id")
      .or(
        `and(master_owner_id.eq.${inbound_event.master_owner_id},property_id.eq.${inbound_event.property_id})`
      )
      .lt("created_at", inbound_event.created_at)
      .order("created_at", { ascending: false })
      .limit(100);

    if (!prev_err && Array.isArray(previous)) {
      touch_number = previous.length + 1;
    }
  }

  const payload = {
    queue_key,
    queue_id,
    queue_status: "queued",
    scheduled_for: now,
    scheduled_for_utc: now,
    scheduled_for_local: now,
    created_at: now,
    updated_at: now,
    send_priority: send_now ? 10 : 5, // Higher priority if send_now
    message_body: reply_text,
    message_text: reply_text,
    to_phone_number: to_phone,
    from_phone_number: from_phone,
    master_owner_id: inbound_event.master_owner_id || null,
    prospect_id: inbound_event.prospect_id || null,
    property_id: inbound_event.property_id || null,
    textgrid_number_id: textgrid_number_id || inbound_event.textgrid_number_id || null,
    message_type,
    use_case_template,
    character_count: (reply_text || "").length,
    touch_number,
    metadata: {
      discord_reply: true,
      source: "discord",
      source_channel_id,
      source_message_id,
      approved_by_discord_user_id: discord_user_id,
      inbound_message_event_id: message_event_id,
      action_type,
      reply_hash,
      conversation_brain_id: inbound_event.conversation_brain_id,
      stage_before: inbound_event.metadata?.current_stage || null,
      stage_after: inbound_event.metadata?.current_stage || null,
      ...ensureObject(inbound_event.metadata),
    },
  };

  // Insert into send_queue
  const { data: queue_row, error: queue_error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .insert(payload)
    .select()
    .maybeSingle();

  if (queue_error) {
    logger.error("queue_insert_failed", {
      error: queue_error?.message,
      to_phone,
      from_phone,
    });
    throw queue_error;
  }

  if (!queue_row) {
    throw new Error("queue_row_insert_returned_no_data");
  }

  logger.info("reply_queued", {
    queue_id: queue_row.id,
    message_event_id,
    to_phone: to_phone.slice(-4),
  });

  return {
    queue_row,
    queue_id: queue_row.id,
  };
}

/**
 * Append reply to AI conversation brain
 */
async function appendToBrain(
  {
    conversation_brain_id = "",
    reply_text = "",
    inbound_message_event_id = "",
    message_event_id = "",
  } = {},
  supabase = null
) {
  if (!supabase || !conversation_brain_id) {
    logger.debug("brain_sync_skipped", {
      reason: !supabase ? "no_supabase" : "no_brain_id",
    });
    return { ok: false, reason: "missing_prereq" };
  }

  try {
    // Link both inbound and outbound events to brain for context
    if (inbound_message_event_id) {
      await linkMessageEventToBrain({
        brain_id: conversation_brain_id,
        message_event_id: inbound_message_event_id,
      });
    }

    if (message_event_id) {
      await linkMessageEventToBrain({
        brain_id: conversation_brain_id,
        message_event_id,
      });
    }

    logger.info("brain_updated", {
      conversation_brain_id,
      inbound_event: !!inbound_message_event_id,
      outbound_event: !!message_event_id,
    });

    return {
      ok: true,
      reason: "brain_updated",
    };
  } catch (err) {
    logger.warn("brain_update_failed", {
      error: err?.message,
      conversation_brain_id,
    });
    return {
      ok: false,
      reason: "brain_update_error",
      error: err?.message,
    };
  }
}

/**
 * Notify Discord ops of reply action
 */
async function notifyOpsOfReply(
  {
    status = "queued",
    inbound_event = {},
    send_queue_id = "",
    reply_text = "",
    discord_user_id = "",
    reason = "",
  } = {}
) {
  try {
    const inbound_phone = inbound_event.from_phone_number || "unknown";
    const safe_phone = String(inbound_phone).slice(-4).padStart(4, "*");

    const fields = {
      From: `+1${safe_phone}`,
      Reply: reply_text.slice(0, 100),
      User: discord_user_id || "api_call",
    };

    if (reason) {
      fields["Block Reason"] = reason;
    }

    await notifyDiscordOps({
      event_type: status === "queued" ? "discord_sms_reply_queued" : "discord_sms_reply_blocked",
      severity: status === "queued" ? "info" : "warning",
      domain: "discord",
      title: status === "queued" ? "✅ SMS Reply Queued" : "🚫 SMS Reply Blocked",
      summary: `Discord user replied to inbound from +1${safe_phone}`,
      fields,
      metadata: {
        status,
        message_event_id: inbound_event.id,
        send_queue_id,
        discord_user_id,
      },
      should_alert_critical: false,
    }).catch((err) => {
      logger.warn("ops_notification_failed", { error: err?.message });
    });
  } catch (err) {
    logger.warn("ops_notify_exception", { error: err?.message });
  }
}

/**
 * POST /api/internal/discord/reply-sms
 */
export async function POST(request) {
  const start_ms = Date.now();

  try {
    // Verify internal API secret
    const auth_check = requireInternalSecret(request);
    if (!auth_check.authorized) {
      return NextResponse.json(auth_check, { status: 401 });
    }

    // Parse body
    const body = await request.json().catch(() => ({}));

    const {
      message_event_id = "",
      reply_text = "",
      mode = "queue",
      send_now = false,
      approved_by_discord_user_id = "",
      source_channel_id = "",
      source_message_id = "",
      action_type = "send_suggested_sms_reply",
    } = body;

    logger.debug("reply_sms_request", {
      message_event_id: message_event_id.slice(0, 8),
      reply_length: (reply_text || "").length,
      action_type,
    });

    const supabase = getDefaultSupabaseClient();

    // Comprehensive safety checks
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
        message_event_id: message_event_id.slice(0, 8),
      });

      // Audit blocked reply
      await auditReplyBlocked(
        {
          discord_user_id: approved_by_discord_user_id,
          channel_id: source_channel_id,
          message_id: source_message_id,
          message_event_id,
          reply_text,
          action_type,
          block_reason: safety_result.reason,
          details: safety_result.details,
        },
        supabase
      ).catch(() => {});

      // Notify ops of block
      await notifyOpsOfReply(
        {
          status: "blocked",
          inbound_event: safety_result.verified_event || {},
          discord_user_id: approved_by_discord_user_id,
          reply_text,
          reason: safety_result.message,
        }
      );

      return NextResponse.json(
        {
          ok: false,
          status: "blocked",
          reason: safety_result.reason,
          message: safety_result.message,
          details: safety_result.details,
        },
        { status: 400 }
      );
    }

    const verified_event = safety_result.verified_event;
    const reply_hash = safety_result.reply_hash;

    // Queue reply
    const queue_result = await queueReplyWhenSmsSendQueue(
      {
        message_event_id,
        inbound_event: verified_event,
        reply_text,
        reply_hash,
        action_type,
        discord_user_id: approved_by_discord_user_id,
        channel_id: source_channel_id,
        message_id: source_message_id,
        source_channel_id,
        source_message_id,
        mode,
        send_now,
        textgrid_number_id: safety_result.textgrid_number_id,
      },
      supabase
    );

    const queue_id = queue_result.queue_id;

    // Link to brain (best-effort)
    await appendToBrain(
      {
        conversation_brain_id: verified_event.conversation_brain_id,
        reply_text,
        inbound_message_event_id: message_event_id,
      },
      supabase
    ).catch(() => {});

    // Audit success
    await auditReplyQueued(
      {
        discord_user_id: approved_by_discord_user_id,
        channel_id: source_channel_id,
        message_id: source_message_id,
        message_event_id,
        send_queue_id: queue_id,
        reply_text,
        action_type,
      },
      supabase
    ).catch(() => {});

    // Notify ops of success
    await notifyOpsOfReply(
      {
        status: "queued",
        inbound_event: verified_event,
        send_queue_id: queue_id,
        reply_text,
        discord_user_id: approved_by_discord_user_id,
      }
    );

    const elapsed_ms = Date.now() - start_ms;
    logger.info("reply_sms_success", {
      queue_id: queue_id.slice(0, 8),
      to_phone: verified_event.from_phone_number.slice(-4),
      elapsed_ms,
    });

    return NextResponse.json(
      {
        ok: true,
        status: "queued",
        queue_id,
        message_event_id,
        to_phone_number: verified_event.from_phone_number,
        preview: reply_text.slice(0, 50),
        queued_at: nowIso(),
      },
      { status: 200 }
    );
  } catch (err) {
    logger.error("reply_sms_exception", {
      error: err?.message,
      stack: err?.stack,
    });

    captureRouteException(err, {
      route: "api.internal.discord.reply_sms",
      subsystem: "discord_sms_reply",
      context: {
        message_event_id: clean(body?.message_event_id).slice(0, 8),
      },
    });

    return NextResponse.json(
      {
        ok: false,
        status: "error",
        reason: "internal_error",
        message: err?.message || "Internal server error",
      },
      { status: 500 }
    );
  }
}
