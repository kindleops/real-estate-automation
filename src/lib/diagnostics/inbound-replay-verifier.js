import {
  normalizeSellerInboundIntent,
} from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";
import {
  SELLER_FLOW_SAFETY_TIERS,
} from "@/lib/domain/seller-flow/seller-flow-safety-policy.js";

function clean(v) {
  return String(v ?? "").trim();
}

export function normalizeInboundReplayMode(raw_mode = null) {
  const mode = clean(raw_mode).toLowerCase();
  if (!mode) return "deterministic_replay";
  if (["verify", "verification", "deterministic", "deterministic_replay"].includes(mode)) {
    return "deterministic_replay";
  }
  if (mode === "classify_only") return "classify_only";
  if (mode === "full_replay") return "full_replay";
  return "deterministic_replay";
}

function summarizeClassification(classification = null) {
  if (!classification || typeof classification !== "object") return null;
  return {
    source: classification.source || null,
    objection: classification.objection || null,
    confidence: classification.confidence ?? null,
    language: classification.language || null,
    emotion: classification.emotion || null,
    compliance_flag: classification.compliance_flag || null,
  };
}

function resolveMatchedOutboundQueueRow(context = null) {
  const outbound_event =
    context?.recent?.recent_events?.find((event) =>
      String(event?.direction || "").toLowerCase() === "outbound"
    ) || null;

  const match =
    context?.fallback_match_data ||
    context?.recent?.outbound_pair_match ||
    context?.match ||
    null;

  if (!match) return null;

  const queue_row_id =
    match.matched_queue_id ||
    match.queue_row_id ||
    context?.fallback_match_id ||
    null;

  return {
    queue_row_id,
    queue_status: match.matched_queue_status || null,
    sent_at: match.matched_sent_at || null,
    from_number:
      match.from_phone_number ||
      outbound_event?.from_phone_number ||
      outbound_event?.outbound_from ||
      null,
    to_number:
      match.to_phone_number ||
      outbound_event?.to_phone_number ||
      outbound_event?.outbound_to ||
      null,
    template_id:
      match.template_id ||
      outbound_event?.template_id ||
      context?.ids?.template_id ||
      null,
    use_case_template:
      match.use_case_template ||
      outbound_event?.use_case_template ||
      outbound_event?.use_case ||
      null,
    property_id: context?.ids?.property_id || null,
    master_owner_id: context?.ids?.master_owner_id || null,
    source: match.matched_source || context?.fallback_match_source || outbound_event?.source || null,
    match_strategy: match.match_strategy || null,
    context_verified: Boolean(match.context_verified),
    reason: queue_row_id ? null : "exact_queue_row_unavailable_from_context",
  };
}

function isOptOutIntent(intent = null) {
  return clean(intent).toLowerCase() === "opt_out";
}

function normalizeTemplateMissingError(error = null) {
  const message = clean(error?.message || error);
  if (!message) return "template_not_found";
  if (message.includes("ENOENT")) return "template_not_found";
  if (message.includes("NO_STAGE_1_TEMPLATE_FOUND")) return "template_not_found";
  return "template_not_found";
}

async function resolveTemplatePreview({
  selected_use_case,
  language,
  context,
  mode,
  deps,
} = {}) {
  if (mode === "classify_only") {
    return {
      selected_template: null,
      rendered_reply_preview: "",
      preview_error: null,
    };
  }

  if (!selected_use_case) {
    return {
      selected_template: null,
      rendered_reply_preview: "",
      preview_error: null,
    };
  }

  try {
    const template = await deps.loadTemplate({
      use_case: selected_use_case,
      language: language || "English",
      context,
      require_podio_template: true,
    });

    if (!template) {
      return {
        selected_template: null,
        rendered_reply_preview: "",
        preview_error: "template_not_found",
      };
    }

    const template_text = clean(template?.text || template?.body);
    const rendered = deps.personalizeTemplate(template_text, {
      seller_first_name: context?.summary?.seller_first_name || null,
      agent_name: context?.summary?.agent_name || null,
      property_address: context?.summary?.property_address || null,
      property_city: context?.summary?.property_city || null,
    });

    return {
      selected_template: template?.item_id || template?.id || template?.template_id || null,
      rendered_reply_preview: rendered?.ok ? clean(rendered.text) : "",
      preview_error: rendered?.ok ? null : "template_not_found",
    };
  } catch (error) {
    return {
      selected_template: null,
      rendered_reply_preview: "",
      preview_error: normalizeTemplateMissingError(error),
    };
  }
}

export async function loadReplayPayload({ message_id, from, body, to, deps }) {
  if (message_id) {
    const { data: event, error } = await deps.supabase
      .from("message_events")
      .select("*")
      .eq("provider_message_id", message_id)
      .maybeSingle();

    if (error || !event) {
      return {
        ok: false,
        status: 404,
        error: "Message not found in DB",
        detail: error?.message,
      };
    }

    return {
      ok: true,
      payload: {
        message_id: event.provider_message_id,
        from: event.inbound_from,
        to: event.inbound_to,
        body: event.message_body,
      },
    };
  }

  return {
    ok: true,
    payload: {
      message_id: `diag-${Date.now()}`,
      from,
      to: to || "+14693131600",
      body,
    },
  };
}

export async function buildVerificationDiagnostics({
  body,
  from,
  to,
  current_stage,
  auto_reply_enabled = true,
  mode = "deterministic_replay",
  deps,
} = {}) {
  const normalized_from = clean(from);
  const normalized_to = clean(to);
  const normalized_body = clean(body);

  const primary_context = await deps.loadContext({
    inbound_from: normalized_from,
    create_brain_if_missing: false,
  });

  const context = await deps.loadContextWithFallback({
    inbound_from: normalized_from,
    inbound_to: normalized_to,
    create_brain_if_missing: false,
    primary_context,
  });

  const classification = await deps.classify(normalized_body, context?.items?.brain_item || null);
  const route = deps.resolveRoute({
    classification,
    brain_item: context?.items?.brain_item || null,
    phone_item: context?.items?.phone_item || null,
    message: normalized_body,
  });

  const effective_current_stage =
    clean(current_stage) ||
    clean(context?.summary?.conversation_stage) ||
    null;

  const plan = await deps.resolveSellerAutoReplyPlan({
    message_body: normalized_body,
    classification,
    route,
    conversation_context: context,
    current_stage: effective_current_stage,
    prior_use_case: route?.use_case || null,
    auto_reply_enabled,
    force_queue_reply: false,
    now: new Date().toISOString(),
  });

  const transition = deps.resolveDeterministicStageTransition({
    current_stage: effective_current_stage,
    inbound_intent: plan?.inbound_intent || normalizeSellerInboundIntent({ message_body: normalized_body }),
    should_queue_reply: Boolean(plan?.should_queue_reply),
    autopilot_enabled: auto_reply_enabled,
  });

  const detected_intent = plan?.inbound_intent || transition.inbound_intent;
  const is_opt_out = isOptOutIntent(detected_intent);

  const selected_use_case = is_opt_out
    ? null
    : plan?.selected_use_case || transition.template_use_case || null;
  const next_stage = transition.next_stage || plan?.next_stage || null;

  const should_queue_reply = is_opt_out
    ? false
    : (transition.should_queue_reply ?? Boolean(plan?.should_queue_reply));

  const suppression_reason = is_opt_out
    ? "opt_out_intent_no_marketing"
    : (transition.suppression_reason || plan?.suppression_reason || null);

  const preview = await resolveTemplatePreview({
    selected_use_case: should_queue_reply ? selected_use_case : null,
    language: plan?.selected_language || classification?.language || "English",
    context,
    mode,
    deps,
  });

  const matched = Boolean(context?.found);
  const matched_outbound_queue_row = resolveMatchedOutboundQueueRow(context) || (
    matched
      ? {
          queue_row_id: null,
          queue_status: null,
          sent_at: null,
          from_number: null,
          to_number: null,
          template_id: context?.ids?.template_id || null,
          use_case_template: null,
          property_id: context?.ids?.property_id || null,
          master_owner_id: context?.ids?.master_owner_id || null,
          match_strategy: context?.fallback_match_source || "context_only",
          context_verified: Boolean(context?.found),
          reason: "matched_context_without_exact_outbound_queue_row",
        }
      : null
  );

  return {
    matched,
    matched_outbound_queue_row,
    classification: summarizeClassification(classification),
    detected_intent,
    current_stage: plan?.current_stage || effective_current_stage,
    next_stage,
    selected_use_case,
    selected_template: preview.selected_template,
    rendered_reply_preview: preview.rendered_reply_preview,
    would_queue_reply: Boolean(should_queue_reply),
    suppression_reason,
    auto_send_eligible: false,
    brain_created: false,
    brain_id: context?.ids?.brain_item_id || context?.ids?.conversation_brain_id || null,
    discord_notification_count_expectation: 1,
    safety_tier:
      is_opt_out
        ? SELLER_FLOW_SAFETY_TIERS.SUPPRESS
        : transition.safety_tier || plan?.safety_tier || SELLER_FLOW_SAFETY_TIERS.REVIEW,
    policy_match: {
      next_stage: transition.next_stage || next_stage,
      template_use_case: transition.template_use_case,
      template_use_case_candidates: transition.template_use_case_candidates || null,
      safety_tier: transition.safety_tier,
      policy_source: transition.policy_source,
      deterministic_match: transition.deterministic_match,
    },
    routing_consistent:
      !transition.next_stage || !plan?.next_stage
        ? true
        : transition.next_stage === plan.next_stage,
    mode,
    verification_write_guard: "no_live_sms_no_queue_mutation",
    preview_error: preview.preview_error,
    pending_outbound_policy:
      is_opt_out
        ? "would_cancel_pending_supported_paths"
        : null,
  };
}
