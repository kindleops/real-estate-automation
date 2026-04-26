/**
 * Discord SMS Reply Endpoint
 * POST /api/internal/discord/reply-sms
 *
 * Resolves reply content by reply_mode (auto_template | template | manual),
 * validates safety, then queues through send_queue.
 */

import { NextResponse } from "next/server";
import { randomUUID } from "crypto";

import { child } from "@/lib/logging/logger.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";
import supabaseClient from "@/lib/supabase/client.js";
import {
  runReplySmsSafetyChecks,
  validateInboundMessageEvent,
} from "@/lib/discord/reply-sms-safety-checks.js";
import {
  auditReplyQueued,
  auditReplyBlocked,
} from "@/lib/discord/reply-sms-audit.js";
import { normalizePhone } from "@/lib/utils/phones.js";
import { clean } from "@/lib/utils/strings.js";
import { nowIso } from "@/lib/utils/dates.js";
import { linkMessageEventToBrain } from "@/lib/domain/brain/link-message-event-to-brain.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";
import {
  cancelInboundAutopilotQueue,
  findInboundAutopilotQueue,
} from "@/lib/discord/inbound-autopilot-queue.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { mapNextAction, ACTIONS } from "@/lib/sms/flow_map.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import { prepareRenderedSmsForQueue } from "@/lib/sms/sanitize.js";

const logger = child({ module: "api.internal.discord.reply_sms" });
const SEND_QUEUE_TABLE = "send_queue";
const REPLY_MODES = new Set(["auto_template", "template", "manual"]);

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeReplyMode(value) {
  const mode = clean(value).toLowerCase();
  return REPLY_MODES.has(mode) ? mode : "auto_template";
}

function requireInternalSecret(request) {
  const expected = process.env.INTERNAL_API_SECRET || "";
  const provided =
    request.headers.get("x-internal-api-secret") ||
    request.headers.get("x-api-secret") ||
    "";

  if (!expected || provided !== expected) {
    return {
      ok: false,
      error: "invalid_internal_api_secret_token",
      status: 401,
    };
  }

  return { ok: true };
}

function resolveInboundSeedMessage(inbound_event = {}) {
  const metadata = ensureObject(inbound_event.metadata);
  return (
    clean(inbound_event.message_body) ||
    clean(inbound_event.message_text) ||
    clean(metadata.message_text) ||
    clean(metadata.inbound_message_text) ||
    clean(metadata.inbound_text) ||
    ""
  );
}

function buildTemplateRenderContext(inbound_event = {}) {
  const metadata = ensureObject(inbound_event.metadata);
  const summary = ensureObject(metadata.summary);

  return {
    seller_first_name:
      clean(summary.seller_first_name) ||
      clean(metadata.seller_first_name) ||
      clean(metadata.owner_first_name) ||
      "",
    agent_name:
      clean(summary.agent_name) ||
      clean(metadata.agent_name) ||
      clean(process.env.SMS_AGENT_NAME) ||
      "",
    property_address:
      clean(summary.property_address) ||
      clean(metadata.property_address) ||
      "",
    property_city:
      clean(summary.property_city) ||
      clean(metadata.property_city) ||
      "",
    city:
      clean(summary.property_city) ||
      clean(metadata.property_city) ||
      "",
    offer_price:
      summary.offer_price ?? metadata.offer_price ?? metadata.smart_cash_offer_display ?? null,
    repair_cost:
      summary.repair_cost ?? metadata.repair_cost ?? metadata.estimated_repair_cost ?? null,
    unit_count:
      summary.unit_count ?? metadata.unit_count ?? null,
  };
}

function previewText(value, max = 120) {
  return clean(value).slice(0, max);
}

async function fetchTemplateById(template_id = "", supabase = null) {
  const wanted = clean(template_id);
  if (!wanted || !supabase) return null;

  const select_fields = [
    "id",
    "template_id",
    "podio_template_id",
    "template_name",
    "template_body",
    "use_case",
    "stage_code",
    "language",
    "is_active",
    "metadata",
  ].join(",");

  const by_id = await supabase
    .from("sms_templates")
    .select(select_fields)
    .eq("id", wanted)
    .eq("is_active", true)
    .maybeSingle();

  if (!by_id?.error && by_id?.data) {
    return {
      ...by_id.data,
      source: "supabase_sms_templates",
      template_resolution_source: "supabase_sms_templates",
      item_id: by_id.data.template_id || by_id.data.id,
      text: clean(by_id.data.template_body),
      template_text: clean(by_id.data.template_body),
    };
  }

  const by_template_id = await supabase
    .from("sms_templates")
    .select(select_fields)
    .eq("template_id", wanted)
    .eq("is_active", true)
    .maybeSingle();

  if (!by_template_id?.error && by_template_id?.data) {
    return {
      ...by_template_id.data,
      source: "supabase_sms_templates",
      template_resolution_source: "supabase_sms_templates",
      item_id: by_template_id.data.template_id || by_template_id.data.id,
      text: clean(by_template_id.data.template_body),
      template_text: clean(by_template_id.data.template_body),
    };
  }

  return null;
}

function renderTemplateMessage(template = {}, inbound_event = {}) {
  const template_text =
    clean(template.template_text) ||
    clean(template.text) ||
    clean(template.template_body);

  if (!template_text) {
    return {
      ok: false,
      reason: "missing_rendered_template",
      message: "Selected template has empty body",
      details: {
        selected_template_id: template.id || template.item_id || null,
      },
    };
  }

  const rendered = personalizeTemplate(template_text, buildTemplateRenderContext(inbound_event));

  if (!rendered?.ok || !clean(rendered?.text)) {
    return {
      ok: false,
      reason: "missing_rendered_template",
      message: rendered?.reason || "Template render failed",
      details: {
        selected_template_id: template.id || template.item_id || null,
        missing_placeholders: rendered?.missing || [],
      },
    };
  }

  const template_source =
    clean(template.template_resolution_source) ||
    clean(template.selected_template_source) ||
    clean(template.source) ||
    "unknown";

  const prepared = prepareRenderedSmsForQueue({
    rendered_message_text: rendered.text,
    template_id: template.id || template.item_id || null,
    template_source,
  });

  if (!prepared?.ok || !clean(prepared?.text)) {
    return {
      ok: false,
      reason: "missing_rendered_template",
      message: prepared?.reason || "Rendered template invalid",
      details: prepared?.diagnostics || {},
    };
  }

  return {
    ok: true,
    rendered_text: clean(prepared.text),
    rendered_preview: previewText(prepared.text),
    template_source,
  };
}

async function resolveAutoTemplateReply({ inbound_event = {} } = {}) {
  const inbound_message = resolveInboundSeedMessage(inbound_event);

  const classification = await classify(inbound_message, null).catch((err) => {
    logger.warn("auto_template_classification_failed", { error: err?.message });
    return {
      message: inbound_message,
      language: clean(inbound_event?.metadata?.language) || "English",
      emotion: "calm",
      stage_hint: clean(inbound_event?.metadata?.current_stage) || "Ownership Confirmation",
      compliance_flag: null,
      positive_signals: [],
      confidence: 1,
      source: "discord_auto_template_fallback",
    };
  });

  const route = resolveRoute({
    classification,
    brain_item: null,
    phone_item: null,
    message: inbound_message,
  });

  const flow = mapNextAction({
    classify_result: classification,
    brain_state: {
      conversation_stage:
        clean(inbound_event?.metadata?.current_stage) || clean(route?.stage) || null,
      close_sub_stage: null,
    },
    property_context: {
      property_type: clean(inbound_event?.metadata?.property_type) || null,
      owner_type: clean(inbound_event?.metadata?.owner_type) || null,
      is_first_touch: false,
      touch_number: 2,
      is_multifamily: Boolean(route?.is_multifamily_like),
    },
  });

  if (flow?.action && flow.action !== ACTIONS.QUEUE_REPLY && flow.action !== ACTIONS.AI_FREEFORM) {
    return {
      ok: false,
      reason: "missing_rendered_template",
      message: `Reply flow action ${flow.action} is not queueable for template approval`,
      details: { flow_action: flow.action, flow_reason: flow.reason || null },
    };
  }

  const selected_use_case = clean(flow?.use_case) || clean(route?.use_case) || "ownership_check";
  const selected_stage_code =
    clean(flow?.stage_code) || clean(route?.stage) || clean(inbound_event?.metadata?.current_stage) || null;
  const selected_language =
    clean(classification?.language) || clean(route?.language) || clean(inbound_event?.metadata?.language) || "English";

  const template = await loadTemplate({
    template_selector: route?.template_selector || null,
    category: route?.primary_category || "Residential",
    secondary_category: route?.secondary_category || null,
    use_case: selected_use_case,
    variant_group: route?.variant_group || null,
    tone: route?.tone || null,
    language: selected_language,
    sequence_position: route?.sequence_position || null,
    paired_with_agent_type: route?.template_filters?.paired_with_agent_type || "Warm Professional",
    fallback_agent_type: route?.template_filters?.fallback_agent_type || "Warm Professional",
    touch_type: route?.template_selector?.touch_type || null,
    touch_number: 2,
    message_type: "Follow-Up",
    property_type_scope: route?.template_filters?.category || route?.primary_category || null,
    deal_strategy: route?.template_selector?.deal_strategy || null,
    context: { summary: buildTemplateRenderContext(inbound_event), route },
  });

  if (!template) {
    return {
      ok: false,
      reason: "missing_rendered_template",
      message: "No template selected by auto_template flow",
      details: {
        use_case: selected_use_case,
        stage_code: selected_stage_code,
        language: selected_language,
      },
    };
  }

  const rendered = renderTemplateMessage(template, inbound_event);
  if (!rendered.ok) return rendered;

  const template_source = rendered.template_source;
  if (!template_source.includes("supabase")) {
    return {
      ok: false,
      reason: "missing_rendered_template",
      message: "Auto-selected template did not come from Supabase sms_templates",
      details: {
        template_source,
        selected_template_id: template.id || template.item_id || null,
      },
    };
  }

  return {
    ok: true,
    reply_text: rendered.rendered_text,
    rendered_message_preview: rendered.rendered_preview,
    selected_template_id: template.id || template.item_id || null,
    selected_template_use_case: clean(template.use_case) || selected_use_case,
    stage_code: clean(template.stage_code) || selected_stage_code,
    language: clean(template.language) || selected_language,
    template_source,
  };
}

function buildManualResult(reply_text = "") {
  const trimmed = clean(reply_text);
  if (!trimmed) {
    return {
      ok: false,
      reason: "reply_text_invalid",
      message: "Manual reply_text is required",
      details: { mode: "manual" },
    };
  }

  return {
    ok: true,
    reply_text: trimmed,
    rendered_message_preview: previewText(trimmed),
    selected_template_id: null,
    selected_template_use_case: null,
    stage_code: null,
    language: null,
    template_source: "manual",
  };
}

export async function resolveReplyContentForMode(
  {
    message_event_id = "",
    reply_mode = "auto_template",
    reply_text = "",
    template_id = "",
    supabase = null,
  } = {},
  deps = {}
) {
  const {
    inbound_event_override = null,
    fetchTemplateByIdImpl = fetchTemplateById,
    resolveAutoTemplateReplyImpl = resolveAutoTemplateReply,
    renderTemplateMessageImpl = renderTemplateMessage,
  } = ensureObject(deps);

  let inbound_event = inbound_event_override;

  if (!inbound_event) {
    const inbound_check = await validateInboundMessageEvent(message_event_id, supabase);
    if (!inbound_check.valid) {
      return {
        ok: false,
        reason: "inbound_event_invalid",
        message: inbound_check.message,
        details: inbound_check,
      };
    }
    inbound_event = inbound_check.event;
  }

  const normalized_mode = normalizeReplyMode(reply_mode);
  const manual_fallback = buildManualResult(reply_text);

  if (normalized_mode === "manual") {
    if (!manual_fallback.ok) return manual_fallback;
    return { ok: true, reply_mode: "manual", inbound_event, ...manual_fallback };
  }

  if (normalized_mode === "template") {
    const selected_template = await fetchTemplateByIdImpl(template_id, supabase);

    if (!selected_template) {
      if (manual_fallback.ok) {
        return { ok: true, reply_mode: "manual", inbound_event, ...manual_fallback };
      }
      return {
        ok: false,
        reason: "invalid_template_id",
        message: "template_id was not found in active sms_templates",
        details: { template_id: clean(template_id) || null },
      };
    }

    const rendered = renderTemplateMessageImpl(selected_template, inbound_event);
    if (!rendered.ok) {
      if (manual_fallback.ok) {
        return { ok: true, reply_mode: "manual", inbound_event, ...manual_fallback };
      }
      return rendered;
    }

    const template_source = rendered.template_source;
    if (!template_source.includes("supabase")) {
      if (manual_fallback.ok) {
        return { ok: true, reply_mode: "manual", inbound_event, ...manual_fallback };
      }
      return {
        ok: false,
        reason: "invalid_template_id",
        message: "template_id resolved outside supabase sms_templates",
        details: { template_id: clean(template_id) || null, template_source },
      };
    }

    return {
      ok: true,
      reply_mode: "template",
      inbound_event,
      reply_text: rendered.rendered_text,
      rendered_message_preview: rendered.rendered_preview,
      selected_template_id: selected_template.id || selected_template.item_id || null,
      selected_template_use_case: clean(selected_template.use_case) || null,
      stage_code: clean(selected_template.stage_code) || null,
      language: clean(selected_template.language) || null,
      template_source,
    };
  }

  const auto_result = await resolveAutoTemplateReplyImpl({ inbound_event, supabase });
  if (auto_result.ok) {
    return { ok: true, reply_mode: "auto_template", inbound_event, ...auto_result };
  }

  if (manual_fallback.ok) {
    return { ok: true, reply_mode: "manual", inbound_event, ...manual_fallback };
  }

  return auto_result;
}

async function queueReplyWhenSmsSendQueue(
  {
    message_event_id = "",
    inbound_event = {},
    reply_text = "",
    reply_hash = "",
    action_type = "approve_template_sms_reply",
    reply_mode = "auto_template",
    selected_template_id = null,
    selected_template_use_case = null,
    selected_stage_code = null,
    selected_language = null,
    template_source = null,
    rendered_message_preview = null,
    discord_user_id = "",
    source_channel_id = "",
    source_message_id = "",
    send_now = false,
    textgrid_number_id = null,
  } = {},
  supabase = null
) {
  if (!supabase) {
    throw new Error("missing_supabase");
  }

  const now = nowIso();
  const queue_key = randomUUID();
  const queue_id = randomUUID();

  const to_phone = normalizePhone(inbound_event.from_phone_number);
  const from_phone = normalizePhone(inbound_event.to_phone_number);

  if (!to_phone || !from_phone) {
    throw new Error("missing_phone_numbers_in_inbound_event");
  }

  const message_type =
    reply_mode === "manual"
      ? "Discord Manual Reply"
      : reply_mode === "template"
        ? "Discord Template Reply"
        : "Discord Auto Template Reply";

  const use_case_template = clean(selected_template_use_case) || "discord_sms_reply";

  let touch_number = 1;
  if (inbound_event.metadata?.touch_number) {
    touch_number = Number(inbound_event.metadata.touch_number) || 2;
  } else {
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
    send_priority: send_now ? 10 : 5,
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
      reply_mode,
      reply_hash,
      selected_template_id,
      selected_template_use_case,
      stage_code: selected_stage_code,
      language: selected_language,
      rendered_message_preview,
      template_source,
      conversation_brain_id: inbound_event.conversation_brain_id,
      stage_before: inbound_event.metadata?.current_stage || null,
      stage_after: selected_stage_code || inbound_event.metadata?.current_stage || null,
      ...ensureObject(inbound_event.metadata),
    },
  };

  const { data: queue_row, error: queue_error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .insert(payload)
    .select()
    .maybeSingle();

  if (queue_error) throw queue_error;
  if (!queue_row) throw new Error("queue_row_insert_returned_no_data");

  return { queue_row, queue_id: queue_row.id };
}

async function appendToBrain(
  {
    conversation_brain_id = "",
    inbound_message_event_id = "",
    message_event_id = "",
  } = {}
) {
  if (!conversation_brain_id) return { ok: false, reason: "no_brain_id" };

  try {
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

    return { ok: true, reason: "brain_updated" };
  } catch {
    return { ok: false, reason: "brain_update_error" };
  }
}

async function notifyOpsOfReply(
  {
    status = "queued",
    inbound_event = {},
    send_queue_id = "",
    reply_text = "",
    discord_user_id = "",
    reason = "",
    reply_mode = "auto_template",
    selected_template_id = null,
  } = {}
) {
  try {
    const inbound_phone = inbound_event.from_phone_number || "unknown";
    const safe_phone = String(inbound_phone).slice(-4).padStart(4, "*");

    const fields = {
      From: `+1${safe_phone}`,
      Reply: previewText(reply_text, 100),
      "Reply Mode": reply_mode,
      User: discord_user_id || "api_call",
    };

    if (selected_template_id) fields["Template ID"] = String(selected_template_id);
    if (reason) fields["Block Reason"] = reason;

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
        reply_mode,
        selected_template_id,
      },
      should_alert_critical: false,
    }).catch(() => {});
  } catch {}
}

async function updateInboundDiscordReviewMetadata(
  {
    supabase = null,
    message_event_id = "",
    inbound_event = {},
    discord_review_status = "approved",
    approved_by_discord_user_id = "",
    outbound_queue_id = "",
    final_reply_text_preview = "",
    action_type = "approve_inbound_template_reply",
  } = {}
) {
  if (!supabase || !clean(message_event_id)) return;

  const base_metadata = ensureObject(inbound_event.metadata);
  const next_metadata = {
    ...base_metadata,
    discord_review_status,
    approved_by_discord_user_id: clean(approved_by_discord_user_id) || null,
    outbound_queue_id: clean(outbound_queue_id) || null,
    final_reply_text_preview: previewText(final_reply_text_preview, 160),
    reviewed_action_type: clean(action_type) || null,
    discord_reviewed_at: nowIso(),
  };

  await supabase
    .from("message_events")
    .update({ metadata: next_metadata })
    .eq("id", message_event_id);
}

function deriveDiscordReviewStatus(action_type = "", reply_mode = "auto_template") {
  const action = clean(action_type);
  if (action === "manual_inbound_sms_reply") return "manual_override_sent";
  if (action === "approve_send_now") return "approved_send_now";
  if (action === "approve_inbound_template_reply") return "approved_send_now";
  return reply_mode === "manual" ? "manual_override_sent" : "approved_send_now";
}

export async function POST(request) {
  const start_ms = Date.now();
  let body = {};

  try {
    const auth_check = requireInternalSecret(request);
    if (!auth_check.ok) {
      return NextResponse.json(
        { ok: false, error: auth_check.error || "invalid_internal_api_secret_token" },
        { status: auth_check.status || 401 }
      );
    }

    body = await request.json().catch(() => ({}));

    const {
      message_event_id = "",
      reply_text = "",
      reply_mode = "auto_template",
      template_id = "",
      send_now = false,
      approved_by_discord_user_id = "",
      source_channel_id = "",
      source_message_id = "",
      action_type = "approve_template_sms_reply",
    } = body;

    const supabase = supabaseClient;

    const resolved = await resolveReplyContentForMode({
      message_event_id,
      reply_mode,
      reply_text,
      template_id,
      supabase,
    });

    if (clean(action_type) === "manual_inbound_sms_reply") {
      await cancelInboundAutopilotQueue({
        message_event_id,
        supabase,
        discord_user_id: approved_by_discord_user_id,
        review_status: "manual_override_sent",
        cancellation_reason: "manual_override",
      }).catch(() => null);
    }

    if (clean(action_type) === "approve_send_now") {
      const pending_autopilot = await findInboundAutopilotQueue({
        message_event_id,
        supabase,
        includeStatuses: ["queued"],
      }).catch(() => null);

      if (pending_autopilot?.id) {
        return NextResponse.json(
          {
            ok: true,
            status: "already_pending",
            queue_id: pending_autopilot.id,
            message_event_id,
          },
          { status: 200 }
        );
      }
    }

    if (!resolved.ok) {
      await auditReplyBlocked(
        {
          discord_user_id: approved_by_discord_user_id,
          channel_id: source_channel_id,
          message_id: source_message_id,
          message_event_id,
          reply_text,
          action_type,
          block_reason: resolved.reason,
          details: resolved.details,
        },
        supabase
      ).catch(() => {});

      return NextResponse.json(
        {
          ok: false,
          status: "blocked",
          reason: resolved.reason,
          message: resolved.message,
          details: resolved.details || {},
        },
        { status: 400 }
      );
    }

    const safety_result = await runReplySmsSafetyChecks({
      message_event_id,
      reply_text: resolved.reply_text,
      supabase,
      inbound_event_override: resolved.inbound_event,
    });

    if (!safety_result.safe) {
      await auditReplyBlocked(
        {
          discord_user_id: approved_by_discord_user_id,
          channel_id: source_channel_id,
          message_id: source_message_id,
          message_event_id,
          reply_text: resolved.reply_text,
          action_type,
          block_reason: safety_result.reason,
          details: {
            ...ensureObject(safety_result.details),
            reply_mode: resolved.reply_mode,
            selected_template_id: resolved.selected_template_id,
            selected_template_use_case: resolved.selected_template_use_case,
            stage_code: resolved.stage_code,
            language: resolved.language,
            rendered_message_preview: resolved.rendered_message_preview,
            template_source: resolved.template_source,
          },
        },
        supabase
      ).catch(() => {});

      await notifyOpsOfReply({
        status: "blocked",
        inbound_event: safety_result.verified_event || resolved.inbound_event || {},
        discord_user_id: approved_by_discord_user_id,
        reply_text: resolved.reply_text,
        reason: safety_result.message,
        reply_mode: resolved.reply_mode,
        selected_template_id: resolved.selected_template_id,
      });

      return NextResponse.json(
        {
          ok: false,
          status: "blocked",
          reason: safety_result.reason,
          message: safety_result.message,
          details: safety_result.details,
          reply_mode: resolved.reply_mode,
        },
        { status: 400 }
      );
    }

    const queue_result = await queueReplyWhenSmsSendQueue(
      {
        message_event_id,
        inbound_event: safety_result.verified_event,
        reply_text: resolved.reply_text,
        reply_hash: safety_result.reply_hash,
        action_type,
        reply_mode: resolved.reply_mode,
        selected_template_id: resolved.selected_template_id,
        selected_template_use_case: resolved.selected_template_use_case,
        selected_stage_code: resolved.stage_code,
        selected_language: resolved.language,
        template_source: resolved.template_source,
        rendered_message_preview: resolved.rendered_message_preview,
        discord_user_id: approved_by_discord_user_id,
        source_channel_id,
        source_message_id,
        send_now,
        textgrid_number_id: safety_result.textgrid_number_id,
      },
      supabase
    );

    const queue_id = queue_result.queue_id;
    const verified_event = safety_result.verified_event;

    await updateInboundDiscordReviewMetadata({
      supabase,
      message_event_id,
      inbound_event: verified_event,
      discord_review_status: deriveDiscordReviewStatus(action_type, resolved.reply_mode),
      approved_by_discord_user_id,
      outbound_queue_id: queue_id,
      final_reply_text_preview: resolved.reply_text,
      action_type,
    }).catch(() => {});

    await appendToBrain({
      conversation_brain_id: verified_event.conversation_brain_id,
      inbound_message_event_id: message_event_id,
    }).catch(() => {});

    await auditReplyQueued(
      {
        discord_user_id: approved_by_discord_user_id,
        channel_id: source_channel_id,
        message_id: source_message_id,
        message_event_id,
        send_queue_id: queue_id,
        reply_text: resolved.reply_text,
        action_type,
        metadata: {
          reply_mode: resolved.reply_mode,
          selected_template_id: resolved.selected_template_id,
          selected_template_use_case: resolved.selected_template_use_case,
          stage_code: resolved.stage_code,
          language: resolved.language,
          rendered_message_preview: resolved.rendered_message_preview,
          template_source: resolved.template_source,
        },
      },
      supabase
    ).catch(() => {});

    await notifyOpsOfReply({
      status: "queued",
      inbound_event: verified_event,
      send_queue_id: queue_id,
      reply_text: resolved.reply_text,
      discord_user_id: approved_by_discord_user_id,
      reply_mode: resolved.reply_mode,
      selected_template_id: resolved.selected_template_id,
    });

    return NextResponse.json(
      {
        ok: true,
        status: "queued",
        queue_id,
        message_event_id,
        to_phone_number: verified_event.from_phone_number,
        preview: previewText(resolved.reply_text, 50),
        reply_mode: resolved.reply_mode,
        selected_template_id: resolved.selected_template_id,
        selected_template_use_case: resolved.selected_template_use_case,
        stage_code: resolved.stage_code,
        language: resolved.language,
        rendered_message_preview: resolved.rendered_message_preview,
        template_source: resolved.template_source,
        queued_at: nowIso(),
      },
      { status: 200 }
    );
  } catch (err) {
    logger.error("reply_sms_exception", { error: err?.message, stack: err?.stack });

    captureRouteException(err, {
      route: "api.internal.discord.reply_sms",
      subsystem: "discord_sms_reply",
      context: {
        message_event_id: clean(body?.message_event_id).slice(0, 8),
        reply_mode: normalizeReplyMode(body?.reply_mode),
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
