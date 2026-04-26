import { validateInboundMessageEvent } from "@/lib/discord/reply-sms-safety-checks.js";
import { clean } from "@/lib/utils/strings.js";
import { child } from "@/lib/logging/logger.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import { prepareRenderedSmsForQueue } from "@/lib/sms/sanitize.js";

const logger = child({ module: "discord.reply_sms_content_resolver" });
const REPLY_MODES = new Set(["auto_template", "template", "manual"]);

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeReplyMode(value) {
  const mode = clean(value).toLowerCase();
  return REPLY_MODES.has(mode) ? mode : "auto_template";
}

function previewText(value, max = 120) {
  return clean(value).slice(0, max);
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

  const [{ classify }, { resolveRoute }, { mapNextAction, ACTIONS }, { loadTemplate }] = await Promise.all([
    import("@/lib/domain/classification/classify.js"),
    import("@/lib/domain/routing/resolve-route.js"),
    import("@/lib/sms/flow_map.js"),
    import("@/lib/domain/templates/load-template.js"),
  ]);

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
