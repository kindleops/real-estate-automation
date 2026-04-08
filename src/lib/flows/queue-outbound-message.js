// ─── queue-outbound-message.js ───────────────────────────────────────────
import { loadContext } from "@/lib/domain/context/load-context.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { renderTemplate } from "@/lib/domain/templates/render-template.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import { normalizeSellerFlowUseCase } from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import {
  resolveQueueSchedule,
  resolveSchedulingContactWindow,
} from "@/lib/domain/queue/queue-schedule.js";
import { chooseTextgridNumber } from "@/lib/domain/routing/choose-textgrid-number.js";
import {
  getCategoryValue,
  getFirstAppReferenceId,
  getNumberValue,
  normalizeUsPhone10,
} from "@/lib/providers/podio.js";
import { findQueueItems } from "@/lib/podio/queries/find-queue-items.js";
import { info, warn } from "@/lib/logging/logger.js";

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
  if (raw === "delivered") return "Sent";

  return "Queued";
}

function deriveSendPriority({
  explicit_send_priority = null,
  classification = null,
  route = null,
}) {
  if (explicit_send_priority) return explicit_send_priority;

  const emotion = classification?.emotion || null;
  const objection = classification?.objection || null;
  const use_case = route?.use_case || null;

  if (
    objection === "financial_distress" ||
    objection === "send_offer_first" ||
    emotion === "motivated" ||
    [
      "offer_reveal",
      "offer_reveal_cash",
      "offer_reveal_lease_option",
      "offer_reveal_subject_to",
      "offer_reveal_novation",
      "mf_offer_reveal",
    ].includes(use_case) ||
    ["clear_to_close", "day_before_close", "seller_docs_needed", "probate_doc_needed"].includes(
      use_case
    )
  ) {
    return "_ Urgent";
  }

  if (["Title", "Closing"].includes(route?.lifecycle_stage || null)) {
    return "_ Normal";
  }

  if ((route?.lifecycle_stage || null) === "Post-Close") {
    return "_ Low";
  }

  if (route?.stage === "Follow-Up") {
    return "_ Low";
  }

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

export function findPendingQueueDuplicateItem(queue_items = [], phone_item_id, touch_number) {
  if (!phone_item_id) return null;

  return (
    queue_items.find((item) => {
      const status = clean(getCategoryValue(item, "queue-status", "")).toLowerCase();
      if (!PENDING_QUEUE_STATUSES.has(status)) return false;

      const candidate_phone_id = getFirstAppReferenceId(item, "phone-number", null);
      if (String(candidate_phone_id || "") !== String(phone_item_id || "")) return false;

      const candidate_touch = Number(getNumberValue(item, "touch-number", 0) || 0);
      return candidate_touch === Number(touch_number || 0);
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
  create_brain_if_missing = true,

  // Optional overrides
  category = null,
  secondary_category = null,
  use_case = null,
  template_lookup_use_case = undefined,
  template_lookup_secondary_category = undefined,
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  language = null,
  sequence_position = null,
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
} = {}, deps = {}) {
  const {
    loadContextImpl = loadContext,
    classifyImpl = classify,
    resolveRouteImpl = resolveRoute,
    loadTemplateImpl = loadTemplate,
    renderTemplateImpl = renderTemplate,
    buildSendQueueItemImpl = buildSendQueueItem,
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
    return {
      ok: false,
      stage: "input",
      reason: "missing_inbound_from",
      inbound_from: resolved_inbound_from,
    };
  }

  if (!normalized_inbound_from) {
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
      stage_hint: context?.summary?.conversation_stage || "Ownership",
      compliance_flag: null,
      positive_signals: [],
      confidence: 1,
      motivation_score: context?.summary?.motivation_score ?? 50,
      source: "system",
      notes: "outbound_initiation",
      phone_activity_status: context?.summary?.phone_activity_status || "Unknown",
    };
  }

  const route = resolveRouteImpl({
    classification,
    brain_item,
    phone_item,
    message: clean(seed_message),
  });

  const resolved_language =
    language ||
    route?.language ||
    classification?.language ||
    context?.summary?.language_preference ||
    "English";

  const resolved_use_case =
    use_case ||
    route?.use_case ||
    "ownership_check";

  const resolved_variant_group =
    variant_group ||
    route?.variant_group ||
    "Stage 1 — Ownership Confirmation";

  const resolved_tone =
    tone ||
    route?.tone ||
    "Warm";

  const resolved_sequence_position =
    sequence_position ||
    route?.sequence_position ||
    "1st Touch";

  const resolved_agent_type =
    paired_with_agent_type ||
    route?.template_filters?.paired_with_agent_type ||
    route?.persona ||
    "Warm Professional";

  const resolved_category =
    category ||
    route?.template_filters?.category ||
    route?.primary_category ||
    "Residential";

  const resolved_secondary_category =
    secondary_category ||
    route?.template_filters?.secondary_category ||
    route?.secondary_category ||
    null;
  const resolved_template_lookup_secondary_category =
    template_lookup_secondary_category !== undefined
      ? template_lookup_secondary_category
      : resolved_secondary_category;
  const resolved_template_lookup_use_case =
    template_lookup_use_case !== undefined
      ? template_lookup_use_case
      : resolved_use_case;

  const resolved_lifecycle_stage =
    lifecycle_stage ||
    route?.lifecycle_stage ||
    route?.template_filters?.lifecycle_stage ||
    route?.stage ||
    null;

  const resolved_fallback_agent_type =
    fallback_agent_type ||
    route?.template_filters?.fallback_agent_type ||
    "Warm Professional";
  const resolved_rotation_key = deriveRotationKey({
    explicit_rotation_key: rotation_key,
    context,
    use_case: resolved_use_case,
    stage: resolved_lifecycle_stage || route?.stage,
  });
  const resolved_message_type = deriveMessageType({
    explicit_message_type: message_type,
    use_case: resolved_use_case,
    stage: route?.stage,
    lifecycle_stage: resolved_lifecycle_stage,
    compliance_flag: classification?.compliance_flag,
  });
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
  const resolved_contact_window = resolveSchedulingContactWindow({
    contact_window: base_contact_window,
    timezone_label: resolved_timezone,
    is_first_contact:
      resolved_message_type === "Cold Outbound" && resolved_touch_number <= 1,
  });

  let selected_template = template_item || null;

  if (!selected_template && !template_id && !message_override) {
    selected_template = await loadTemplateImpl({
      category: resolved_category,
      secondary_category: resolved_template_lookup_secondary_category,
      use_case: resolved_template_lookup_use_case,
      variant_group: resolved_variant_group,
      tone: resolved_tone,
      gender_variant,
      language: resolved_language,
      sequence_position: resolved_sequence_position,
      paired_with_agent_type: resolved_agent_type,
      recently_used_template_ids:
        context?.recent?.recently_used_template_ids || [],
      rotation_key: resolved_rotation_key,
      fallback_agent_type: resolved_fallback_agent_type,
      context,
      template_render_overrides,
    });
  }

  if (!selected_template && !message_override) {
    warn("outbound.queue_message_template_not_found", {
      inbound_from: resolved_inbound_from,
      phone_item_id: context?.ids?.phone_item_id || null,
      use_case: resolved_use_case,
      template_lookup_use_case: resolved_template_lookup_use_case,
      language: resolved_language,
      category: resolved_category,
      secondary_category: resolved_template_lookup_secondary_category,
      sequence_position: resolved_sequence_position,
      paired_with_agent_type: resolved_agent_type,
    });

    return {
      ok: false,
      stage: "template",
      reason: "template_not_found",
      inbound_from: normalized_inbound_from,
      context,
      classification,
      route,
    };
  }

  const selected_template_id =
    template_id ||
    selected_template?.item_id ||
    null;

  let final_message_text = "";
  let rendered_placeholders = [];

  if (message_override) {
    final_message_text = message_override;
  } else if (selected_template) {
    const template_text = selected_template?.text || "";

    const render_result = renderTemplateImpl({
      template_text,
      context,
      overrides: {
        language: resolved_language,
        conversation_stage: route?.stage,
        lifecycle_stage: resolved_lifecycle_stage,
        ai_route: route?.brain_ai_route,
        ...(template_render_overrides || {}),
      },
      use_case: selected_template?.use_case || resolved_template_lookup_use_case,
      variant_group: selected_template?.variant_group || resolved_variant_group,
    });

    if (
      render_result?.invalid_placeholders?.length ||
      render_result?.missing_required_placeholders?.length
    ) {
      warn("outbound.queue_message_template_placeholder_validation_failed", {
        inbound_from: resolved_inbound_from,
        phone_item_id: context?.ids?.phone_item_id || null,
        template_id: selected_template_id,
        invalid_placeholders: render_result?.invalid_placeholders || [],
        missing_required_placeholders:
          render_result?.missing_required_placeholders || [],
      });

      return {
        ok: false,
        stage: "render",
        reason: "template_placeholder_validation_failed",
        inbound_from: normalized_inbound_from,
        context,
        classification,
        route,
        template_id: selected_template_id,
        invalid_placeholders: render_result?.invalid_placeholders || [],
        missing_required_placeholders:
          render_result?.missing_required_placeholders || [],
      };
    }

    final_message_text = clean(render_result?.rendered_text || "");
    rendered_placeholders = Array.isArray(render_result?.used_placeholders)
      ? render_result.used_placeholders
      : [];
  } else {
    final_message_text = message_override;
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

  const derived_schedule =
    normalizeScheduledDate(scheduled_for_local) || normalizeScheduledDate(scheduled_for_utc)
      ? {
          scheduled_for_local:
            normalizeScheduledDate(scheduled_for_local) ||
            normalizeScheduledDate(scheduled_for_utc),
          scheduled_for_utc: normalizeScheduledDate(scheduled_for_utc),
        }
      : resolveQueueSchedule({
          now: started_at,
          timezone_label: resolved_timezone,
          contact_window: resolved_contact_window,
          distribution_key: resolved_rotation_key,
      });

  const queue_history = await findQueueItemsImpl({
    filters: {
      "phone-number": Number(context?.ids?.phone_item_id || 0) || undefined,
    },
    limit: 25,
  });

  const duplicate_queue_item = findPendingQueueDuplicateItem(
    queue_history,
    context?.ids?.phone_item_id || null,
    resolved_touch_number
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

  const queue_result = await buildSendQueueItemImpl({
    context,
    rendered_message_text: final_message_text,
    template_id: selected_template_id,
    template_item: selected_template,
    textgrid_number_item_id: resolved_textgrid_number_item_id,
    scheduled_for_local:
      derived_schedule?.scheduled_for_local || { start: started_at },
    scheduled_for_utc:
      derived_schedule?.scheduled_for_utc || normalizeScheduledDate(scheduled_for_utc),
    timezone: resolved_timezone,
    contact_window: resolved_contact_window,
    send_priority: deriveSendPriority({
      explicit_send_priority: send_priority,
      classification,
      route,
    }),
    message_type: deriveMessageType({
      explicit_message_type: resolved_message_type,
    }),
    max_retries,
    queue_status: deriveQueueStatus(queue_status),
    dnc_check,
    delivery_confirmed,
    touch_number: resolved_touch_number,
    queue_id,
    // Enrichment fields — populate Send Queue category fields for observability.
    // Values are sourced from the selected template and resolved route parameters.
    // resolveQueueCategoryField will safely omit any field whose value has no
    // matching option in the Send Queue schema (e.g. stale supplement).
    property_type: selected_template?.category_primary ?? resolved_category ?? null,
    secondary_category: resolved_secondary_category,
    use_case_template:
      normalizeSellerFlowUseCase(selected_template?.use_case || resolved_use_case) || null,
    personalization_tags_used: rendered_placeholders,
  });

  info("outbound.queue_message_completed", {
    inbound_from: resolved_inbound_from,
    queue_item_id: queue_result?.queue_item_id || null,
    phone_item_id: context?.ids?.phone_item_id || null,
    template_id: selected_template_id,
    template_source: selected_template?.source || null,
    template_title: selected_template?.title || null,
    template_relation_id: queue_result?.template_relation_id ?? null,
    template_app_field_written: queue_result?.template_app_field_written ?? false,
    textgrid_number_item_id: resolved_textgrid_number_item_id,
    use_case: resolved_use_case,
    stage: route?.stage || null,
    lifecycle_stage: resolved_lifecycle_stage,
    template_lookup_use_case: resolved_template_lookup_use_case,
    template_lookup_secondary_category: resolved_template_lookup_secondary_category,
    message_override_used: Boolean(message_override),
  });

  return {
    ok: true,
    stage: "queued",
    inbound_from: normalized_inbound_from,
    queue_item_id: queue_result?.queue_item_id || null,
    template_id: selected_template_id,
    template_item: selected_template,
    selected_template_source: selected_template?.source || null,
    selected_template_title: selected_template?.title || null,
    template_relation_id: queue_result?.template_relation_id ?? null,
    template_app_field_written: queue_result?.template_app_field_written ?? false,
    message_override_used: Boolean(message_override),
    textgrid_number_item_id: resolved_textgrid_number_item_id,
    rendered_message_text: final_message_text,
    context,
    classification,
    route,
    queue_result,
  };
}

export default queueOutboundMessage;
