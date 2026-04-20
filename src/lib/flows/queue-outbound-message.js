// ─── queue-outbound-message.js ───────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";
import { loadContext } from "@/lib/domain/context/load-context.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { chooseTextgridNumber } from "@/lib/domain/routing/choose-textgrid-number.js";
import {
  getCategoryValue,
  getFirstAppReferenceId,
  getNumberValue,
  getDateValue,
  getTextValue,
  normalizeUsPhone10,
} from "@/lib/providers/podio.js";
import { findQueueItems } from "@/lib/podio/queries/find-queue-items.js";
import { info, warn } from "@/lib/logging/logger.js";

// ── New SMS engine modules ───────────────────────────────────────────────
import { mapNextAction, ACTIONS } from "@/lib/sms/flow_map.js";
import { resolveTemplate } from "@/lib/sms/template_resolver.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import { computeScheduledSend } from "@/lib/sms/latency.js";
import { queueMessage as smsQueueMessage } from "@/lib/sms/queue_message.js";
import { normalizeAgentStyleFit } from "@/lib/sms/agent_style.js";
import { normalizeLanguage } from "@/lib/sms/language_aliases.js";
import { resolvePropertyTypeScope } from "@/lib/sms/property_scope.js";
import { resolveDealStrategy } from "@/lib/sms/deal_strategy.js";

// ══════════════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════════════

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeScheduledDate(value) {
  if (!value) return null;

  if (typeof value === "string") {
    return { start: value };
  }

  if (value instanceof Date) {
    return { start: value.toISOString() };
  }

  if (typeof value === "object" && value.start) {
    return value;
  }

  return null;
}

function deriveMessageType({
  explicit_message_type = null,
  use_case = null,
  stage = null,
  lifecycle_stage = null,
  compliance_flag = null,
}) {
  if (explicit_message_type) return explicit_message_type;
  if (compliance_flag === "stop_texting") return "Opt-Out Confirm";
  if (
    stage === "Follow-Up" ||
    stage === "Contract" ||
    ["Title", "Closing", "Disposition", "Post-Close"].includes(lifecycle_stage)
  ) {
    return "Follow-Up";
  }
  if (use_case === "reengagement") return "Re-Engagement";
  return "Cold Outbound";
}

function deriveQueueStatus(value = null) {
  const raw = clean(value).toLowerCase();

  if (raw === "processing") return "Sending";
  if (raw === "sending") return "Sending";
  if (raw === "blocked") return "Blocked";
  if (raw === "failed") return "Failed";
  if (raw === "sent") return "Sent";
  // "Delivered" is a distinct terminal state — do not collapse to "Sent".
  if (raw === "delivered") return "Delivered";

  return "Queued";
}

function deriveSendPriority({
  explicit_send_priority = null,
  classification = null,
  route = null,
}) {
  // Caller-supplied priority always wins — allows per-flow overrides.
  if (explicit_send_priority) return explicit_send_priority;

  const objection = classification?.objection || null;
  const use_case = route?.use_case || null;
  const stage = route?.stage || null;
  const lifecycle_stage = route?.lifecycle_stage || null;

  // ── Urgent ──────────────────────────────────────────────────────────────
  // Inbound-driven objections that need immediate human or AI response.
  if (objection === "financial_distress" || objection === "send_offer_first") {
    return "_ Urgent";
  }

  // Active offer/closing use-cases where delays cost deals.
  if (
    [
      "offer_reveal",
      "offer_reveal_cash",
      "offer_reveal_lease_option",
      "offer_reveal_subject_to",
      "offer_reveal_novation",
      "mf_offer_reveal",
    ].includes(use_case)
  ) {
    return "_ Urgent";
  }

  // Time-critical transactional touches (closing docs, title deadlines).
  if (
    ["clear_to_close", "day_before_close", "seller_docs_needed", "probate_doc_needed"].includes(
      use_case
    )
  ) {
    return "_ Urgent";
  }

  // ── Low ─────────────────────────────────────────────────────────────────
  if (lifecycle_stage === "Post-Close") {
    return "_ Low";
  }

  if (stage === "Follow-Up") {
    return "_ Low";
  }

  // ── Normal ───────────────────────────────────────────────────────────────
  // Standard first touches, Title/Closing lifecycle outbound, everything else.
  // NOTE: emotion === "motivated" was previously a blanket Urgent trigger but
  // produced Urgent priority for nearly all cold-outbound distressed-list sends
  // because many properties have tax delinquency or liens.  Motivation is now
  // handled through use_case routing (offer_reveal etc.) rather than a score
  // threshold, so routine sends stay Normal and don't crowd out true Urgents.
  return "_ Normal";
}

function deriveTimezone({
  explicit_timezone = null,
  context = null,
}) {
  return (
    explicit_timezone ||
    context?.summary?.market_timezone ||
    context?.summary?.timezone ||
    "Central"
  );
}

function deriveContactWindow({
  explicit_contact_window = null,
  context = null,
}) {
  return (
    explicit_contact_window ||
    context?.summary?.contact_window ||
    "8AM-9PM Local"
  );
}

function deriveRotationKey({
  explicit_rotation_key = null,
  context = null,
  use_case = null,
  stage = null,
}) {
  if (explicit_rotation_key) return explicit_rotation_key;

  return [
    context?.ids?.phone_item_id || "no-phone",
    context?.ids?.property_id || "no-property",
    use_case || "no-use-case",
    stage || "no-stage",
  ].join(":");
}

function deriveNextTouchNumber({
  explicit_touch_number = null,
  context = null,
}) {
  const parsed = Number(explicit_touch_number);
  if (Number.isFinite(parsed) && parsed > 0) return Math.floor(parsed);

  const historical_touch_count =
    context?.recent?.touch_count ||
    context?.summary?.total_messages_sent ||
    0;

  return Math.max(1, Number(historical_touch_count || 0) + 1);
}

const PENDING_QUEUE_STATUSES = new Set(["queued", "sending"]);

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

/**
 * Find a pending duplicate queue item.
 *
 * Matches on:
 *   1. Same phone_item_id + same touch_number (existing check — exact duplicate)
 *   2. Same phone_item_id + same use_case created within the last 24 hours
 *      (prevents re-queuing the same use-case touch after a cancellation race)
 *
 * @param {object[]} queue_items
 * @param {number|string|null} phone_item_id
 * @param {number} touch_number
 * @param {string|null} use_case  Normalized use_case label for secondary dedupe
 * @returns {object|null}
 */
export function findPendingQueueDuplicateItem(queue_items = [], phone_item_id, touch_number, use_case = null) {
  if (!phone_item_id) return null;

  const now_ts = Date.now();
  const window_24h_ts = now_ts - 24 * 60 * 60 * 1000;

  return (
    queue_items.find((item) => {
      const status = clean(getCategoryValue(item, "queue-status", "")).toLowerCase();
      if (!PENDING_QUEUE_STATUSES.has(status)) return false;

      const candidate_phone_id = getFirstAppReferenceId(item, "phone-number", null);
      if (String(candidate_phone_id || "") !== String(phone_item_id || "")) return false;

      // Primary guard: exact same touch number
      const candidate_touch = Number(getNumberValue(item, "touch-number", 0) || 0);
      if (candidate_touch === Number(touch_number || 0)) return true;

      // Secondary guard: same use_case within 24-hour window
      if (use_case) {
        const candidate_use_case = clean(getCategoryValue(item, "use-case-template", "") || "").toLowerCase();
        if (candidate_use_case && candidate_use_case === clean(use_case).toLowerCase()) {
          const scheduled_ts =
            toTimestamp(getDateValue(item, "scheduled-for-utc", null)) ||
            toTimestamp(getDateValue(item, "scheduled-for-local", null));
          if (scheduled_ts && scheduled_ts >= window_24h_ts) return true;
        }
      }

      return false;
    }) || null
  );
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN FLOW
// ══════════════════════════════════════════════════════════════════════════

export async function queueOutboundMessage({
  inbound_from,
  phone = null,
  seed_message = "",
  create_brain_if_missing = false,

  // Optional overrides
  category = null,
  secondary_category = null,
  use_case = null,
  template_lookup_use_case = undefined,
  template_lookup_secondary_category = undefined,
  template_selector = null,
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  language = null,
  sequence_position = null,
  touch_type = null,
  property_type_scope = null,
  deal_strategy = null,
  paired_with_agent_type = null,
  fallback_agent_type = null,
  lifecycle_stage = null,

  scheduled_for_local = null,
  scheduled_for_utc = null,
  timezone = null,
  contact_window = null,
  send_priority = null,
  message_type = null,
  queue_status = "Queued",
  max_retries = 3,
  dnc_check = "✅ Cleared",
  delivery_confirmed = "⏳ Pending",
  touch_number = null,
  queue_id = null,
  rotation_key = null,

  // Hard overrides
  template_id = null,
  template_item = null,
  message_text = null,
  rendered_message_text = null,
  template_render_overrides = {},
  textgrid_number_item_id = null,
  extra_queue_context = null,
  cash_offer_snapshot_id = null,
} = {}, deps = {}) {
  const {
    loadContextImpl = loadContext,
    classifyImpl = classify,
    resolveRouteImpl = resolveRoute,
    mapNextActionImpl = mapNextAction,
    resolveTemplateImpl = resolveTemplate,
    personalizeTemplateImpl = personalizeTemplate,
    computeScheduledSendImpl = computeScheduledSend,
    smsQueueMessageImpl = smsQueueMessage,
    chooseTextgridNumberImpl = chooseTextgridNumber,
    findQueueItemsImpl = findQueueItems,
  } = deps;
  const started_at = nowIso();
  const resolved_inbound_from = clean(inbound_from) || clean(phone);
  const normalized_inbound_from = normalizeUsPhone10(resolved_inbound_from);
  const message_override = clean(rendered_message_text) || clean(message_text);

  info("outbound.queue_message_started", {
    inbound_from: resolved_inbound_from,
    create_brain_if_missing,
    has_seed_message: Boolean(clean(seed_message)),
    has_template_override: Boolean(template_id || template_item),
    has_message_override: Boolean(message_override),
    has_textgrid_number_override: Boolean(textgrid_number_item_id),
  });

  if (!resolved_inbound_from) {
    warn("outbound.queue_message_skip", {
      reason: "missing_inbound_from",
      inbound_from: resolved_inbound_from,
      use_case: use_case || null,
    });
    return {
      ok: false,
      stage: "input",
      reason: "missing_inbound_from",
      inbound_from: resolved_inbound_from,
    };
  }

  if (!normalized_inbound_from) {
    warn("outbound.queue_message_skip", {
      reason: "invalid_inbound_from",
      inbound_from: resolved_inbound_from,
      use_case: use_case || null,
    });
    return {
      ok: false,
      stage: "input",
      reason: "invalid_inbound_from",
      inbound_from: resolved_inbound_from,
    };
  }

  const context = await loadContextImpl({
    inbound_from: normalized_inbound_from,
    create_brain_if_missing,
  });

  if (!context?.found) {
    warn("outbound.queue_message_context_not_found", {
      inbound_from: resolved_inbound_from,
      reason: context?.reason || "context_not_found",
    });

    return {
      ok: false,
      stage: "context",
      reason: context?.reason || "context_not_found",
      inbound_from: normalized_inbound_from,
      context,
    };
  }

  const brain_item = context?.items?.brain_item || null;
  const phone_item = context?.items?.phone_item || null;

  let classification;

  if (clean(seed_message)) {
    classification = await classifyImpl(clean(seed_message), brain_item);
  } else {
    classification = {
      message: "",
      language: language || context?.summary?.language_preference || "English",
      objection: null,
      emotion: "calm",
      stage_hint:
        context?.summary?.conversation_stage || "Ownership Confirmation",
      compliance_flag: null,
      positive_signals: [],
      confidence: 1,
      motivation_score: context?.summary?.motivation_score ?? 50,
      source: "system",
      notes: "outbound_initiation",
      phone_activity_status: context?.summary?.phone_activity_status || "Unknown",
    };
  }

  // ── Resolve route (kept for offer/underwriting/pipeline metadata) ──────
  const route = resolveRouteImpl({
    classification,
    brain_item,
    phone_item,
    message: clean(seed_message),
  });

  // ── New SMS engine: flow_map → template → personalize → schedule → queue ─
  const agent_item = context?.items?.agent_item || null;
  const agent_style_fit = normalizeAgentStyleFit({
    agent_style: paired_with_agent_type || null,
    agent_archetype: getTextValue(agent_item, "text", ""),
    agent_family: getCategoryValue(agent_item, "category", null),
  });

  const resolved_language =
    language ||
    normalizeLanguage(classification?.language) ||
    context?.summary?.language_preference ||
    "English";

  const resolved_touch_number = deriveNextTouchNumber({
    explicit_touch_number: touch_number,
    context,
  });

  const resolved_timezone = deriveTimezone({
    explicit_timezone: timezone,
    context,
  });

  const base_contact_window = deriveContactWindow({
    explicit_contact_window: contact_window,
    context,
  });

  const resolved_message_type = deriveMessageType({
    explicit_message_type: message_type,
    use_case: use_case || route?.use_case,
    stage: route?.stage,
    lifecycle_stage: lifecycle_stage || route?.lifecycle_stage,
    compliance_flag: classification?.compliance_flag,
  });

  const is_first_touch = resolved_message_type === "Cold Outbound" && resolved_touch_number <= 1;

  const property_context = {
    property_type: context?.summary?.property_type || null,
    owner_type: context?.summary?.owner_type || null,
    is_first_touch,
    touch_number: resolved_touch_number,
    is_multifamily: route?.is_multifamily_like || false,
  };

  // ── Flow map: determine next action from classification ──────────────
  const flow = mapNextActionImpl({
    classify_result: classification,
    brain_state: {
      conversation_stage: context?.summary?.conversation_stage || null,
      close_sub_stage: null,
    },
    property_context,
    agent_style_fit,
  });

  // Compliance / STOP — abort immediately (always honored, even pre-routed)
  if (flow.action === ACTIONS.STOP && !message_override) {
    info("outbound.queue_message_stopped", {
      inbound_from: resolved_inbound_from,
      reason: flow.reason,
      cancel_queued: flow.cancel_queued || false,
    });
    return {
      ok: false,
      stage: "flow_map",
      reason: flow.reason,
      action: flow.action,
      cancel_queued: flow.cancel_queued || false,
      inbound_from: normalized_inbound_from,
      context,
      classification,
      route,
    };
  }

  // When the caller provided an explicit use_case, they already performed
  // routing (e.g. routeSellerConversation).  Skip WAIT / ESCALATE gates —
  // only STOP (compliance) is honoured above.
  const caller_pre_routed = Boolean(use_case);

  if (flow.action === ACTIONS.WAIT && !message_override && !caller_pre_routed) {
    warn("outbound.flow_action_wait", {
      inbound_from: resolved_inbound_from,
      phone_item_id: context?.ids?.phone_item_id || null,
      brain_item_id: context?.ids?.brain_item_id || null,
      conversation_stage: context?.summary?.conversation_stage || null,
      reason: flow.reason,
      action: flow.action,
      use_case: use_case || null,
    });
    return {
      ok: false,
      stage: "flow_map",
      reason: flow.reason,
      action: flow.action,
      inbound_from: normalized_inbound_from,
      context,
      classification,
      route,
    };
  }

  if (flow.action === ACTIONS.ESCALATE && !message_override && !caller_pre_routed) {
    warn("outbound.flow_action_escalate", {
      inbound_from: resolved_inbound_from,
      phone_item_id: context?.ids?.phone_item_id || null,
      brain_item_id: context?.ids?.brain_item_id || null,
      conversation_stage: context?.summary?.conversation_stage || null,
      reason: flow.reason,
      action: flow.action,
      use_case: use_case || null,
    });
    return {
      ok: false,
      stage: "flow_map",
      reason: flow.reason,
      action: flow.action,
      human_review: true,
      inbound_from: normalized_inbound_from,
      context,
      classification,
      route,
    };
  }

  // ── Resolve use case (caller override → flow_map → route → default) ──
  const resolved_use_case =
    use_case ||
    flow.use_case ||
    route?.use_case ||
    "ownership_check";

  const resolved_stage_code =
    flow.stage_code ||
    route?.stage ||
    context?.summary?.conversation_stage ||
    null;

  const resolved_rotation_key = deriveRotationKey({
    explicit_rotation_key: rotation_key,
    context,
    use_case: resolved_use_case,
    stage: resolved_stage_code,
  });

  const resolved_send_priority = deriveSendPriority({
    explicit_send_priority: send_priority,
    classification,
    route,
  });

  // ── Template resolution via new SMS engine ───────────────────────────
  let final_message_text = "";
  let rendered_placeholders = [];
  let resolution = null;
  let selected_template_id = template_id || null;

  if (message_override) {
    // Caller provided explicit message — skip template resolution
    final_message_text = message_override;
  } else if (template_item) {
    // Caller provided a template object — personalize it via new engine
    const template_text = template_item?.text || "";
    selected_template_id = selected_template_id || template_item?.item_id || null;

    const personalization_context = {
      seller_first_name: context?.summary?.seller_first_name || "",
      agent_name: context?.summary?.agent_name || "",
      property_address: context?.summary?.property_address || "",
      property_city: context?.summary?.property_city || "",
      city: context?.summary?.property_city || "",
      ...(template_render_overrides || {}),
    };

    const render = personalizeTemplateImpl(template_text, personalization_context);
    if (!render.ok) {
      warn("outbound.queue_message_personalization_failed", {
        inbound_from: resolved_inbound_from,
        template_id: selected_template_id,
        missing: render.missing,
        reason: render.reason,
      });
      return {
        ok: false,
        stage: "render",
        reason: render.reason || "personalization_failed",
        missing_placeholders: render.missing || [],
        inbound_from: normalized_inbound_from,
        context,
        classification,
        route,
        template_id: selected_template_id,
      };
    }
    final_message_text = render.text;
    rendered_placeholders = render.placeholders_used || [];

    // Build a synthetic resolution for enrichment fields
    resolution = {
      resolved: true,
      template_text,
      use_case: template_item?.use_case || resolved_use_case,
      stage_code: resolved_stage_code,
      language: resolved_language,
      agent_style_fit,
      attachable_template_ref: template_item?.item_id
        ? { app_id: APP_IDS?.templates, item_id: template_item.item_id }
        : null,
      source: "caller_provided",
    };
  } else {
    // Full new pipeline: template_resolver → personalize_template
    const property_scope = resolvePropertyTypeScope({
      use_case: resolved_use_case,
      is_follow_up: resolved_use_case?.includes("follow") || false,
      ...property_context,
    });
    const deal_strat = resolveDealStrategy({
      ...property_context,
      objection: classification?.objection,
      stage_code: resolved_stage_code,
    });

    resolution = resolveTemplateImpl({
      use_case: resolved_use_case,
      stage_code: resolved_stage_code,
      language: resolved_language,
      agent_style_fit,
      property_type_scope: property_scope,
      deal_strategy: deal_strat,
      is_first_touch,
      is_follow_up: resolved_use_case?.includes("follow") || false,
      master_owner_id: context?.ids?.master_owner_id,
      phone_e164: normalized_inbound_from,
    });

    if (!resolution.resolved) {
      warn("outbound.queue_message_template_not_found", {
        inbound_from: resolved_inbound_from,
        phone_item_id: context?.ids?.phone_item_id || null,
        use_case: resolved_use_case,
        language: resolved_language,
        agent_style_fit,
        fallback_reason: resolution.fallback_reason,
      });

      return {
        ok: false,
        stage: "template",
        reason: resolution.fallback_reason || "template_not_found",
        inbound_from: normalized_inbound_from,
        context,
        classification,
        route,
      };
    }

    selected_template_id = resolution.template_id || null;

    const personalization_context = {
      seller_first_name: context?.summary?.seller_first_name || "",
      agent_name: context?.summary?.agent_name || "",
      property_address: context?.summary?.property_address || "",
      property_city: context?.summary?.property_city || "",
      city: context?.summary?.property_city || "",
      ...(template_render_overrides || {}),
    };

    const render = personalizeTemplateImpl(resolution.template_text, personalization_context);
    if (!render.ok) {
      warn("outbound.queue_message_personalization_failed", {
        inbound_from: resolved_inbound_from,
        template_id: selected_template_id,
        missing: render.missing,
        reason: render.reason,
      });
      return {
        ok: false,
        stage: "render",
        reason: render.reason || "personalization_failed",
        missing_placeholders: render.missing || [],
        inbound_from: normalized_inbound_from,
        context,
        classification,
        route,
        template_id: selected_template_id,
      };
    }
    final_message_text = render.text;
    rendered_placeholders = render.placeholders_used || [];
  }

  if (!final_message_text) {
    warn("outbound.queue_message_render_failed", {
      inbound_from: resolved_inbound_from,
      phone_item_id: context?.ids?.phone_item_id || null,
      template_id: selected_template_id,
    });

    return {
      ok: false,
      stage: "render",
      reason: "rendered_message_empty",
      inbound_from: normalized_inbound_from,
      context,
      classification,
      route,
      template_id: selected_template_id,
    };
  }

  // ── TextGrid number selection (unchanged) ────────────────────────────
  let resolved_textgrid_number_item_id = textgrid_number_item_id || null;

  if (!resolved_textgrid_number_item_id) {
    const chosen_number = await chooseTextgridNumberImpl({
      context,
      classification,
      route,
      preferred_language: resolved_language,
      rotation_key: resolved_rotation_key,
    });

    resolved_textgrid_number_item_id =
      chosen_number?.item_id ||
      chosen_number?.textgrid_number_item_id ||
      chosen_number?.id ||
      null;
  }

  if (!resolved_textgrid_number_item_id) {
    warn("outbound.queue_message_textgrid_number_not_found", {
      inbound_from: resolved_inbound_from,
      phone_item_id: context?.ids?.phone_item_id || null,
      market_id: context?.ids?.market_id || null,
      language: resolved_language,
    });

    return {
      ok: false,
      stage: "number_selection",
      reason: "textgrid_number_not_found",
      inbound_from: normalized_inbound_from,
      context,
      classification,
      route,
      template_id: selected_template_id,
    };
  }

  // ── Schedule via new latency engine ──────────────────────────────────
  let schedule;

  if (normalizeScheduledDate(scheduled_for_local) || normalizeScheduledDate(scheduled_for_utc)) {
    // Caller provided explicit schedule
    schedule = {
      scheduled_local:
        normalizeScheduledDate(scheduled_for_local)?.start ||
        normalizeScheduledDate(scheduled_for_utc)?.start ||
        null,
      scheduled_utc:
        normalizeScheduledDate(scheduled_for_utc)?.start || null,
      timezone: resolved_timezone,
      latency_seconds: 0,
      delay_source: "caller_override",
    };
  } else {
    schedule = computeScheduledSendImpl({
      now_utc: new Date(),
      timezone: resolved_timezone,
      assigned_agent: agent_item,
      message_kind: (resolved_use_case?.includes("follow") || false) ? "follow_up" : "reply",
      stage_code: resolved_stage_code,
      classify_result: classification,
      contact_window: base_contact_window,
      delay_profile: flow.delay_profile,
      seeded_key: [
        context?.ids?.master_owner_id,
        normalized_inbound_from,
        resolved_use_case,
        resolved_stage_code,
      ],
    });
  }

  // ── Duplicate guard (unchanged) ──────────────────────────────────────
  const queue_history = await findQueueItemsImpl({
    filters: {
      "phone-number": Number(context?.ids?.phone_item_id || 0) || undefined,
    },
    limit: 25,
  });

  const duplicate_queue_item = findPendingQueueDuplicateItem(
    queue_history,
    context?.ids?.phone_item_id || null,
    resolved_touch_number,
    resolved_use_case
  );

  if (duplicate_queue_item) {
    warn("outbound.queue_message_duplicate_suppressed", {
      inbound_from: resolved_inbound_from,
      phone_item_id: context?.ids?.phone_item_id || null,
      duplicate_queue_item_id: duplicate_queue_item?.item_id || null,
      duplicate_touch_number: resolved_touch_number,
      duplicate_queue_status: getCategoryValue(duplicate_queue_item, "queue-status", null),
    });

    return {
      ok: false,
      stage: "duplicate_guard",
      reason: "duplicate_pending_queue_item",
      inbound_from: normalized_inbound_from,
      duplicate_queue_item_id: duplicate_queue_item?.item_id || null,
      duplicate_touch_number: resolved_touch_number,
      duplicate_queue_status: getCategoryValue(duplicate_queue_item, "queue-status", null),
      context,
      classification,
      route,
    };
  }

  // ── Template selection audit log ─────────────────────────────────────
  info("outbound.template_selection_audit", {
    inbound_from: resolved_inbound_from,
    phone_item_id: context?.ids?.phone_item_id || null,
    master_owner_id: context?.ids?.master_owner_id || null,
    selected_template_id: selected_template_id || null,
    resolution_source: resolution?.source || null,
    resolution_path: resolution?.resolution_path || null,
    fallback_reason: resolution?.fallback_reason || null,
    resolved_use_case,
    resolved_language,
    agent_style_fit,
    message_override_used: Boolean(message_override),
    pipeline: "sms_engine_v2",
  });

  // ── Queue via new SMS engine ─────────────────────────────────────────
  const links = {
    master_owner_id: context?.ids?.master_owner_id || null,
    prospect_id: context?.ids?.prospect_id || null,
    property_id: context?.ids?.property_id || null,
    phone_id: context?.ids?.phone_item_id || null,
    market_id: context?.ids?.market_id || null,
    agent_id: context?.ids?.assigned_agent_id || null,
    textgrid_number_id: resolved_textgrid_number_item_id,
  };

  const queue_context = {
    touch_number: resolved_touch_number,
    is_first_touch,
    is_follow_up: resolved_use_case?.includes("follow") || false,
    is_reengagement: resolved_use_case === "reengagement",
    is_opt_out_confirm: classification?.compliance_flag === "stop_texting",
    phone_e164: normalized_inbound_from,
    contact_window: base_contact_window,
    placeholders_used: rendered_placeholders,
    property_address: context?.summary?.property_address || null,
    property_type: context?.summary?.property_type || null,
    owner_type: context?.summary?.owner_type || null,
    max_retries,
    send_priority: resolved_send_priority,
    dnc_check,
    delivery_confirmed,
    ...(extra_queue_context && typeof extra_queue_context === "object"
      ? extra_queue_context
      : {}),
  };

  const queue_result = await smsQueueMessageImpl({
    rendered_text: final_message_text,
    schedule,
    resolution: resolution || {
      use_case: resolved_use_case,
      stage_code: resolved_stage_code,
      language: resolved_language,
      agent_style_fit,
    },
    links,
    context: queue_context,
    cash_offer_snapshot_id: cash_offer_snapshot_id || null,
  });

  info("outbound.queue_message_completed", {
    inbound_from: resolved_inbound_from,
    queue_item_id: queue_result?.item_id || null,
    phone_item_id: context?.ids?.phone_item_id || null,
    template_id: selected_template_id,
    resolution_source: resolution?.source || null,
    fallback_reason: resolution?.fallback_reason || null,
    textgrid_number_item_id: resolved_textgrid_number_item_id,
    use_case: resolved_use_case,
    stage: resolved_stage_code,
    message_override_used: Boolean(message_override),
    pipeline: "sms_engine_v2",
  });

  return {
    ok: queue_result?.ok ?? true,
    stage: "queued",
    inbound_from: normalized_inbound_from,
    queue_item_id: queue_result?.item_id || null,
    template_id: selected_template_id,
    template_item: template_item || null,
    selected_template_source: resolution?.source || null,
    selected_template_resolution_source: resolution?.source || null,
    selected_template_fallback_reason: resolution?.fallback_reason || null,
    template_attached: Boolean(resolution?.attachable_template_ref?.item_id),
    message_override_used: Boolean(message_override),
    textgrid_number_item_id: resolved_textgrid_number_item_id,
    rendered_message_text: final_message_text,
    context,
    classification,
    route,
    queue_result,
    // New SMS engine enrichment
    flow_action: flow.action,
    flow_reason: flow.reason,
    resolution,
    schedule,
    pipeline: "sms_engine_v2",
  };
}

export default queueOutboundMessage;
