/**
 * Discord SMS Reply Action Handlers
 * Handles button clicks and modal submissions for SMS reply feature
 */

import { child } from "@/lib/logging/logger.js";
import { getDefaultSupabaseClient } from "@/lib/supabase/default-client.js";
import {
  cancelInboundAutopilotQueue,
  expediteInboundAutopilotQueue,
} from "@/lib/discord/inbound-autopilot-queue.js";

// Button styles
const STYLE = {
  PRIMARY: 1,
  SECONDARY: 2,
  SUCCESS: 3,
  DANGER: 4,
};

function button({ label, custom_id, style = STYLE.PRIMARY, disabled = false }) {
  return {
    type: 2,
    style,
    label: String(label).slice(0, 80),
    custom_id: String(custom_id).slice(0, 100),
    disabled: Boolean(disabled),
  };
}

function actionRow(buttons) {
  return { type: 1, components: buttons.slice(0, 5) };
}

function textInput({ custom_id, label, style = 1, placeholder = "", value = "", min_length = 1, max_length = 4000, required = true }) {
  return {
    type: 4, // TEXT_INPUT
    custom_id: String(custom_id).slice(0, 100),
    label: String(label).slice(0, 45),
    style: Number(style),
    placeholder: String(placeholder).slice(0, 100),
    value: String(value).slice(0, max_length),
    min_length: Number(min_length),
    max_length: Number(max_length),
    required: Boolean(required),
  };
}
import { clean } from "@/lib/utils/strings.js";

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

const logger = child({ module: "discord.action_handlers.sms_reply" });

/**
 * Handle: open_sms_reply_modal
 * Shows Discord modal for editing/entering reply text
 */
export async function handleOpenSmsReplyModal({
  interaction = {},
  message_event_id = "",
  suggested_reply = "",
}) {
  if (!interaction?.id) {
    return {
      ok: false,
      error: "missing_interaction_id",
    };
  }

  const modal_title = clean(suggested_reply) ? "Edit Reply" : "Manual Reply";
  const placeholder_text = clean(suggested_reply)
    ? "Edit the suggested reply..."
    : "Enter your SMS reply...";

  const modal_payload = {
    custom_id: `sms_reply_manual_modal:${message_event_id}`,
    title: modal_title,
    components: [
      actionRow([
        textInput({
          custom_id: "reply_text_input",
          label: "Reply Text",
          style: 2, // PARAGRAPH
          placeholder: placeholder_text,
          value: clean(suggested_reply) || "",
          min_length: 1,
          max_length: 480,
          required: true,
        }),
      ]),
    ],
  };

  return {
    ok: true,
    type: 9, // MODAL
    data: modal_payload,
  };
}

/**
 * Handle: submit_sms_reply_modal
 * Modal submission → call reply endpoint
 * This is actually bridged through discord-action-router to the /api/internal/discord/reply-sms endpoint
 */
export async function handleSubmitSmsReplyModal({
  interaction = {},
  discord_user_id = "",
  channel_id = "",
  message_id = "",
}) {
  const modal_custom_id = interaction?.data?.custom_id || "";
  const message_event_id = modal_custom_id.split(":").slice(1).join(":");

  // Extract reply text from modal submission
  const components = interaction?.data?.components || [];
  let reply_text = "";

  for (const component_row of components) {
    const row_components = component_row.components || [];
    for (const component of row_components) {
      if (component.custom_id === "reply_text_input") {
        reply_text = clean(component.value);
        break;
      }
    }
  }

  if (!reply_text || !message_event_id) {
    return {
      ok: false,
      error: "missing_reply_text_or_event_id",
      ephemeral: true,
    };
  }

  logger.info("modal_submit_intercepted", {
    message_event_id: message_event_id.slice(0, 8),
    reply_length: reply_text.length,
  });

  // Bridge to endpoint
  return {
    ok: true,
    bridge_endpoint: "/api/internal/discord/reply-sms",
    bridge_payload: {
      message_event_id,
      reply_mode: "manual",
      reply_text,
      approved_by_discord_user_id: discord_user_id,
      source_channel_id: channel_id,
      source_message_id: message_id,
      action_type: "manual_inbound_sms_reply",
    },
    method: "POST",
  };
}

/**
 * Handle: send_suggested_sms_reply
 * Direct send of suggested reply (one-click)
 */
export async function handleSendSuggestedSmsReply({
  interaction = {},
  message_event_id = "",
  template_id = "",
  discord_user_id = "",
  channel_id = "",
  message_id = "",
}) {
  if (!message_event_id) {
    return {
      ok: false,
      error: "missing_message_event_id",
      ephemeral: true,
    };
  }

  logger.info("suggested_reply_handler", {
    message_event_id: message_event_id.slice(0, 8),
    reply_mode: template_id ? "template" : "auto_template",
  });

  // Bridge to endpoint (server selects/renders template by message_event_id)
  return {
    ok: true,
    bridge_endpoint: "/api/internal/discord/reply-sms",
    bridge_payload: {
      message_event_id,
      reply_mode: template_id ? "template" : "auto_template",
      template_id: clean(template_id) || undefined,
      approved_by_discord_user_id: discord_user_id,
      source_channel_id: channel_id,
      source_message_id: message_id,
      action_type: "approve_send_now",
    },
    method: "POST",
  };
}

/**
 * Handle: manual_sms_reply
 * Opens modal for manual reply (no suggestion)
 */
export async function handleManualSmsReply({
  interaction = {},
  message_event_id = "",
  discord_user_id = "",
  channel_id = "",
  message_id = "",
}) {
  if (!message_event_id) {
    return {
      ok: false,
      error: "missing_message_event_id",
      ephemeral: true,
    };
  }

  return handleOpenSmsReplyModal({
    interaction,
    message_event_id,
    suggested_reply: "",
  });
}

async function updateMessageEventMetadata(message_event_id = "", updater = () => ({}), supabase = null) {
  if (!message_event_id || !supabase) {
    return { ok: false, reason: "missing_event_or_supabase" };
  }

  const { data: event_row, error: load_error } = await supabase
    .from("message_events")
    .select("id, metadata")
    .eq("id", message_event_id)
    .maybeSingle();

  if (load_error) {
    return { ok: false, reason: load_error.message || "load_failed" };
  }

  const current_metadata = ensureObject(event_row?.metadata);
  const next_metadata = ensureObject(updater(current_metadata));

  const { error: update_error } = await supabase
    .from("message_events")
    .update({ metadata: next_metadata })
    .eq("id", message_event_id);

  if (update_error) {
    return { ok: false, reason: update_error.message || "update_failed" };
  }

  return { ok: true, metadata: next_metadata };
}

export async function handleNotInterestedSmsReply({
  message_event_id = "",
  discord_user_id = "",
}) {
  const supabase = getDefaultSupabaseClient();
  await cancelInboundAutopilotQueue({
    message_event_id,
    supabase,
    discord_user_id,
    review_status: "not_interested",
    cancellation_reason: "not_interested",
  }).catch(() => null);

  const result = await updateMessageEventMetadata(
    message_event_id,
    (metadata) => ({
      ...metadata,
      discord_review_status: "not_interested",
      contact_status: "not_interested",
      suppress_followup: true,
      reviewed_by_discord_user_id: discord_user_id,
      reviewed_at: new Date().toISOString(),
    }),
    supabase
  );

  return result.ok
    ? { ok: true, ephemeral: true, content: "Marked not interested. Follow-up suppressed." }
    : { ok: false, ephemeral: true, error: result.reason };
}

async function suppressInboundContact({ message_event_id = "", discord_user_id = "", suppression_reason = "opt_out" } = {}) {
  const supabase = getDefaultSupabaseClient();
  await cancelInboundAutopilotQueue({
    message_event_id,
    supabase,
    discord_user_id,
    review_status:
      suppression_reason === "wrong_number" ? "wrong_number_suppressed" : "opted_out",
    cancellation_reason: suppression_reason,
  }).catch(() => null);

  const { data: event_row } = await supabase
    .from("message_events")
    .select("id, from_phone_number, metadata")
    .eq("id", message_event_id)
    .maybeSingle();

  const from_phone_number = clean(event_row?.from_phone_number || event_row?.metadata?.from_phone_number || event_row?.metadata?.inbound_from);
  if (from_phone_number) {
    await supabase.from("sms_suppression_list").insert({
      phone_number: from_phone_number,
      suppression_reason,
      is_active: true,
      suppressed_by_discord_user_id: discord_user_id,
      suppressed_at: new Date().toISOString(),
    }).maybeSingle().catch(() => null);
  }

  return updateMessageEventMetadata(
    message_event_id,
    (metadata) => ({
      ...metadata,
      discord_review_status:
        suppression_reason === "wrong_number" ? "wrong_number_suppressed" : "opted_out",
      reviewed_by_discord_user_id: discord_user_id,
      reviewed_at: new Date().toISOString(),
      suppress_followup: true,
      contact_status: suppression_reason === "wrong_number" ? "wrong_number" : metadata.contact_status,
      suppression_reason,
    }),
    supabase
  );
}

export async function handleWrongNumberSmsReply({ message_event_id = "", discord_user_id = "" }) {
  const result = await suppressInboundContact({
    message_event_id,
    discord_user_id,
    suppression_reason: "wrong_number",
  });
  return result.ok
    ? { ok: true, ephemeral: true, content: "Wrong number recorded. Further outreach suppressed." }
    : { ok: false, ephemeral: true, error: result.reason };
}

export async function handleOptOutSmsReply({ message_event_id = "", discord_user_id = "" }) {
  const result = await suppressInboundContact({
    message_event_id,
    discord_user_id,
    suppression_reason: "opt_out",
  });
  return result.ok
    ? { ok: true, ephemeral: true, content: "Opt-out recorded. Further outreach suppressed." }
    : { ok: false, ephemeral: true, error: result.reason };
}

export async function handleOpenRecord({ message_event_id = "" }) {
  return {
    ok: true,
    ephemeral: true,
    content: `Record reference: ${clean(message_event_id) || "unknown"}`,
  };
}

export async function handleCancelAutopilotSmsReply({
  message_event_id = "",
  discord_user_id = "",
}) {
  const supabase = getDefaultSupabaseClient();
  const cancel_result = await cancelInboundAutopilotQueue({
    message_event_id,
    supabase,
    discord_user_id,
    review_status: "autopilot_cancelled",
    cancellation_reason: "discord_cancel_autopilot",
  }).catch((error) => ({ ok: false, error: error?.message || "cancel_failed" }));

  if (!cancel_result?.ok) {
    return { ok: false, ephemeral: true, error: cancel_result.error || "cancel_failed" };
  }

  const metadata_result = await updateMessageEventMetadata(
    message_event_id,
    (metadata) => ({
      ...metadata,
      discord_review_status: "autopilot_cancelled",
      cancelled_by_discord_user_id: discord_user_id,
      autopilot_cancelled_at: new Date().toISOString(),
    }),
    supabase
  );

  return metadata_result.ok
    ? { ok: true, ephemeral: true, content: cancel_result.cancelled ? "Autopilot cancelled." : "No pending autopilot reply to cancel." }
    : { ok: false, ephemeral: true, error: metadata_result.reason };
}

export async function handleApproveSendNowSmsReply({
  message_event_id = "",
  discord_user_id = "",
  channel_id = "",
  message_id = "",
}) {
  const supabase = getDefaultSupabaseClient();
  const expedite_result = await expediteInboundAutopilotQueue({
    message_event_id,
    supabase,
    discord_user_id,
    review_status: "approved_send_now",
  }).catch((error) => ({ ok: false, error: error?.message || "expedite_failed" }));

  if (!expedite_result?.ok) {
    return { ok: false, ephemeral: true, error: expedite_result.error || "expedite_failed" };
  }

  if (expedite_result?.expedited) {
    await updateMessageEventMetadata(
      message_event_id,
      (metadata) => ({
        ...metadata,
        discord_review_status: "approved_send_now",
        approved_by_discord_user_id: discord_user_id,
        approved_send_now_at: new Date().toISOString(),
      }),
      supabase
    ).catch(() => null);

    return {
      ok: true,
      ephemeral: true,
      content: "Queued autopilot reply released for immediate send.",
    };
  }

  return handleSendSuggestedSmsReply({
    message_event_id,
    discord_user_id,
    channel_id,
    message_id,
  });
}

/**
 * Handle: suppress_number
 * Suppress inbound number from further contact
 */
export async function handleSuppressNumber({
  interaction = {},
  message_event_id = "",
  from_phone_number = "",
  discord_user_id = "",
  channel_id = "",
}) {
  if (!from_phone_number) {
    return {
      ok: false,
      error: "missing_from_phone_number",
      ephemeral: true,
    };
  }

  const supabase = getDefaultSupabaseClient();

  try {
    const { error } = await supabase
      .from("sms_suppression_list")
      .insert({
        phone_number: from_phone_number,
        suppression_reason: "discord_suppress_action",
        is_active: true,
        suppressed_by_discord_user_id: discord_user_id,
        suppressed_at: new Date().toISOString(),
      })
      .maybeSingle();

    if (error && !error?.message?.includes("duplicate")) {
      throw error;
    }

    logger.info("number_suppressed", {
      phone: from_phone_number.slice(-4),
      user: discord_user_id,
    });

    return {
      ok: true,
      ephemeral: true,
      content: `✅ Suppressed ${from_phone_number.slice(-4)}`,
    };
  } catch (err) {
    logger.error("suppress_error", { error: err?.message });
    return {
      ok: false,
      error: err?.message,
      ephemeral: true,
    };
  }
}

/**
 * Handle: mark_hot_lead
 * Flag inbound as hot lead in supabase (best-effort)
 */
export async function handleMarkHotLead({
  interaction = {},
  message_event_id = "",
  discord_user_id = "",
  channel_id = "",
}) {
  if (!message_event_id) {
    return {
      ok: false,
      error: "missing_message_event_id",
      ephemeral: true,
    };
  }

  const supabase = getDefaultSupabaseClient();

  try {
    const { error } = await supabase
      .from("message_events")
      .update({
        metadata: {
          marked_hot_by_discord: true,
          marked_hot_by_user_id: discord_user_id,
          marked_hot_at: new Date().toISOString(),
        },
      })
      .eq("id", message_event_id);

    if (error) {
      logger.warn("mark_hot_error", { error: error?.message });
      // Don't throw — this is enhancement only
    } else {
      logger.info("marked_hot", {
        message_event_id: message_event_id.slice(0, 8),
        user: discord_user_id,
      });
    }

    return {
      ok: true,
      ephemeral: true,
      content: "🔥 Marked as hot lead",
    };
  } catch (err) {
    logger.warn("mark_hot_exception", { error: err?.message });
    return {
      ok: true, // Still return OK (enhancement-only)
      ephemeral: true,
      content: "🔥 Hot lead flag enqueued",
    };
  }
}

/**
 * Export DI functions for testing
 */
export function __setSmReplyHandlersDeps(overrides = {}) {
  // Placeholder
}

export function __resetSmsReplyHandlersDeps() {
  // Placeholder
}
