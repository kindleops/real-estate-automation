import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";
import { resolveLatencyAwareQueueSchedule } from "@/lib/domain/queue/queue-schedule.js";
import {
  brainStageForUseCase,
  SELLER_FLOW_STAGES,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { routeSellerConversation } from "@/lib/domain/seller-flow/route-seller-conversation.js";
import APP_IDS from "@/lib/config/app-ids.js";
import { info as _info, warn as _warn } from "@/lib/logging/logger.js";

// ── Injectable logger deps (for testing) ─────────────────────────────────

const defaultLogDeps = { info: _info, warn: _warn };
let logDeps = { ...defaultLogDeps };

export function __setSellerQueueLogDeps(overrides = {}) {
  logDeps = { ...logDeps, ...overrides };
}

export function __resetSellerQueueLogDeps() {
  logDeps = { ...defaultLogDeps };
}

const DEFAULT_LATENCY_BY_TIER = Object.freeze({
  hot: Object.freeze({ min_minutes: 3, max_minutes: 8 }),
  neutral: Object.freeze({ min_minutes: 12, max_minutes: 30 }),
  cold: Object.freeze({ min_minutes: 90, max_minutes: 240 }),
});

function clean(value) {
  return String(value ?? "").trim();
}

function normalizeResponseTier(value = null) {
  const raw = clean(value).toLowerCase();
  if (raw === "hot") return "hot";
  if (raw === "cold") return "cold";
  return "neutral";
}

function deriveSendPriority(response_tier = "neutral") {
  switch (normalizeResponseTier(response_tier)) {
    case "hot":
      return "_ Urgent";
    case "cold":
      return "_ Low";
    default:
      return "_ Normal";
  }
}

function deriveTimezoneLabel(context = null) {
  const market_timezone = clean(context?.summary?.market_timezone);

  if (/new[_/\s-]?york|eastern|\bet\b/i.test(market_timezone)) return "Eastern";
  if (/chicago|central|\bct\b/i.test(market_timezone)) return "Central";
  if (/denver|mountain|\bmt\b/i.test(market_timezone)) return "Mountain";
  if (/los[_/\s-]?angeles|pacific|\bpt\b/i.test(market_timezone)) return "Pacific";
  if (/anchorage|alaska/i.test(market_timezone)) return "Alaska";
  if (/honolulu|hawaii/i.test(market_timezone)) return "Hawaii";

  const contact_window = clean(context?.summary?.contact_window);
  const suffix_match = contact_window.match(/\b(ET|CT|MT|PT)\b/i);

  switch ((suffix_match?.[1] || "").toUpperCase()) {
    case "ET":
      return "Eastern";
    case "MT":
      return "Mountain";
    case "PT":
      return "Pacific";
    case "CT":
    default:
      return "Central";
  }
}

function derivePrimaryCategory(context = null) {
  const property_class = clean(
    getCategoryValue(context?.items?.property_item || null, "property-class", null)
  );
  if (property_class) return property_class;

  const majority = clean(
    getCategoryValue(context?.items?.master_owner_item || null, "property-type-majority", null)
  ).toUpperCase();

  if (majority === "VACANT LAND") return "Vacant";
  return "Residential";
}

function deriveRotationKey({ context = null, plan = null } = {}) {
  return [
    context?.ids?.master_owner_id || "no-owner",
    context?.ids?.phone_item_id || "no-phone",
    context?.ids?.property_id || "no-property",
    plan?.selected_use_case || "no-use-case",
    plan?.selected_variant_group || "no-variant-group",
    context?.recent?.touch_count || context?.summary?.total_messages_sent || 0,
  ].join(":");
}

function deriveAgentLatencyWindow(agent_item = null, response_tier = "neutral") {
  const tier = normalizeResponseTier(response_tier);
  const defaults = DEFAULT_LATENCY_BY_TIER[tier];

  const min_field =
    tier === "hot"
      ? "latency-hot-min"
      : tier === "cold"
        ? "latency-cold-min"
        : "latency-neutral-min";
  const max_field =
    tier === "hot"
      ? "latency-hot-max"
      : tier === "cold"
        ? "latency-cold-max"
        : "latency-neutral-max";

  const raw_min = getNumberValue(agent_item, min_field, defaults.min_minutes);
  const raw_max = getNumberValue(agent_item, max_field, defaults.max_minutes);
  const min_minutes = Math.max(0, Number(raw_min ?? defaults.min_minutes) || defaults.min_minutes);
  const max_minutes = Math.max(min_minutes, Number(raw_max ?? defaults.max_minutes) || defaults.max_minutes);

  return {
    response_tier: tier,
    min_minutes,
    max_minutes,
  };
}

function buildAlwaysOnContactWindow(timezone_label = "Central") {
  switch (clean(timezone_label)) {
    case "Eastern":
      return "12AM-11:59PM ET";
    case "Mountain":
      return "12AM-11:59PM MT";
    case "Pacific":
      return "12AM-11:59PM PT";
    case "Alaska":
      return "12AM-11:59PM AT";
    case "Hawaii":
      return "12AM-11:59PM HT";
    case "Central":
    default:
      return "12AM-11:59PM CT";
  }
}

export async function maybeQueueSellerStageReply({
  inbound_from = null,
  context = null,
  classification = null,
  message = "",
  previous_outbound_use_case = null,
  maybe_offer = null,
  existing_offer = null,
  explicit_use_case = null,
  explicit_template_lookup_use_case = null,
  explicit_variant_group = null,
  explicit_tone = null,
  force_queue_reply = null,
  extra_queue_context = null,
  extra_template_render_overrides = null,
  cash_offer_snapshot_id = null,
  now = new Date().toISOString(),
  queue_message = queueOutboundMessage,
  schedule_resolver = resolveLatencyAwareQueueSchedule,
} = {}) {
  const phone_id = context?.ids?.phone_item_id || null;
  const brain_id = context?.ids?.brain_item_id || null;
  const master_owner_id = context?.ids?.master_owner_id || null;

  const prospect_id = context?.ids?.prospect_id || null;
  const property_id = context?.ids?.property_id || null;
  const stage = context?.summary?.conversation_stage || null;

  logDeps.info("seller_queue.entry", {
    inbound_from,
    phone_id,
    brain_id,
    master_owner_id,
    prospect_id,
    property_id,
    stage,
    send_queue_app_id: APP_IDS.send_queue,
    has_context: Boolean(context?.found),
    has_classification: Boolean(classification),
    message_preview: String(message || "").slice(0, 80),
  });

  const base_plan = routeSellerConversation({
    context,
    classification,
    message,
    previous_outbound_use_case,
    maybe_offer,
    existing_offer,
  });

  const plan = explicit_use_case
    ? {
        ...base_plan,
        handled: true,
        should_queue_reply: force_queue_reply ?? true,
        selected_use_case: explicit_use_case,
        template_lookup_use_case:
          explicit_template_lookup_use_case ?? explicit_use_case,
        selected_variant_group:
          explicit_variant_group ?? base_plan?.selected_variant_group ?? null,
        selected_tone: explicit_tone ?? base_plan?.selected_tone ?? null,
      }
    : base_plan;

  if (!plan?.handled) {
    logDeps.info("seller_queue.skip", {
      inbound_from,
      phone_id,
      brain_id,
      prospect_id,
      property_id,
      stage,
      reason: "seller_flow_not_handled",
      plan_handled: false,
      plan_should_queue_reply: plan?.should_queue_reply ?? null,
      selected_use_case: plan?.selected_use_case || null,
      send_queue_app_id: APP_IDS.send_queue,
    });
    return {
      ok: true,
      queued: false,
      handled: false,
      reason: "seller_flow_not_handled",
      plan,
      brain_stage: null,
    };
  }

  if (!plan.should_queue_reply) {
    logDeps.info("seller_queue.skip", {
      inbound_from,
      phone_id,
      brain_id,
      prospect_id,
      property_id,
      stage,
      reason: "seller_flow_no_auto_reply_needed",
      plan_handled: true,
      plan_should_queue_reply: false,
      selected_use_case: plan.selected_use_case || null,
      detected_intent: plan.detected_intent || null,
      send_queue_app_id: APP_IDS.send_queue,
    });
    return {
      ok: true,
      queued: false,
      handled: true,
      reason: "seller_flow_no_auto_reply_needed",
      plan,
      brain_stage: brainStageForUseCase(plan.selected_use_case),
    };
  }

  const response_window = deriveAgentLatencyWindow(
    context?.items?.agent_item || null,
    plan.response_tier
  );
  const rotation_key = deriveRotationKey({ context, plan });
  const timezone_label = deriveTimezoneLabel(context);
  const contact_window = buildAlwaysOnContactWindow(timezone_label);
  const schedule = schedule_resolver({
    now,
    timezone_label,
    contact_window,
    distribution_key: rotation_key,
    delay_min_minutes: response_window.min_minutes,
    delay_max_minutes: response_window.max_minutes,
  });

  logDeps.info("seller_queue.before_create", {
    inbound_from,
    phone_id,
    brain_id,
    prospect_id,
    property_id,
    stage,
    use_case: plan.selected_use_case,
    template_lookup_use_case: plan.template_lookup_use_case || null,
    next_action: "queue_outbound_message",
    send_queue_app_id: APP_IDS.send_queue,
    queue_status: "Queued",
    scheduled_for_utc: schedule.scheduled_for_utc,
    scheduled_for_local: schedule.scheduled_for_local,
    timezone: schedule.timezone_label || timezone_label,
    rotation_key,
    response_tier: plan.response_tier || null,
    variant_group: plan.selected_variant_group || null,
    tone: plan.selected_tone || null,
  });

  let queued;
  try {
    queued = await queue_message({
      inbound_from,
      create_brain_if_missing: true,
      category: derivePrimaryCategory(context),
      secondary_category: null,
      template_lookup_secondary_category: null,
      use_case: plan.selected_use_case,
      template_lookup_use_case: plan.template_lookup_use_case,
      variant_group: plan.selected_variant_group,
      tone: plan.selected_tone,
      language: plan.detected_language,
      paired_with_agent_type: plan.paired_with_agent_type,
      scheduled_for_local: schedule.scheduled_for_local,
      scheduled_for_utc: schedule.scheduled_for_utc,
      timezone: schedule.timezone_label || timezone_label,
      contact_window: schedule.contact_window || contact_window,
      send_priority: deriveSendPriority(plan.response_tier),
      message_type: plan.selected_use_case === "reengagement" ? "Re-Engagement" : "Follow-Up",
      queue_status: "Queued",
      rotation_key,
      template_render_overrides: {
        offer_price: plan.offer_price_display,
        smart_cash_offer_display: plan.offer_price_display,
        ...(extra_template_render_overrides || {}),
      },
      extra_queue_context: extra_queue_context || undefined,
      cash_offer_snapshot_id: cash_offer_snapshot_id || undefined,
    });
  } catch (err) {
    logDeps.warn("seller_queue.create_failed", {
      inbound_from,
      phone_id,
      brain_id,
      prospect_id,
      property_id,
      stage,
      use_case: plan.selected_use_case,
      next_action: "queue_outbound_message",
      send_queue_app_id: APP_IDS.send_queue,
      queue_status: "Queued",
      scheduled_for_utc: schedule.scheduled_for_utc,
      rotation_key,
      error: err?.message || "unknown",
      error_description: err?.response?.data?.error_description || err?.error_description || null,
    });
    throw err;
  }

  // Log the resolved next action (QUEUE / WAIT / STOP / ESCALATE)
  logDeps.info("seller_queue.next_action", {
    inbound_from,
    phone_id,
    brain_id,
    prospect_id,
    property_id,
    stage,
    use_case: plan.selected_use_case,
    action: queued?.flow_action || (queued?.ok ? "queue_reply" : queued?.action || "unknown"),
    reason: queued?.flow_reason || queued?.reason || null,
    ok: Boolean(queued?.ok),
    send_queue_app_id: APP_IDS.send_queue,
  });

  if (queued?.ok) {
    logDeps.info("seller_queue.create_success", {
      inbound_from,
      phone_id,
      brain_id,
      prospect_id,
      property_id,
      stage,
      use_case: plan.selected_use_case,
      next_action: "queued",
      send_queue_app_id: APP_IDS.send_queue,
      queue_status: "Queued",
      template_id: queued.template_id || null,
      queue_item_id: queued.queue_item_id || queued.queue_result?.item_id || null,
      scheduled_for_utc: schedule.scheduled_for_utc,
      dedupe_key: queued.queue_result?.queue_id || null,
      pipeline: queued.pipeline || "sms_engine_v2",
    });
  } else {
    logDeps.warn("seller_queue.create_failed", {
      inbound_from,
      phone_id,
      brain_id,
      prospect_id,
      property_id,
      stage,
      use_case: plan.selected_use_case,
      next_action: queued?.stage || "unknown",
      action: queued?.action || null,
      send_queue_app_id: APP_IDS.send_queue,
      queue_status: "Queued",
      scheduled_for_utc: schedule.scheduled_for_utc,
      rotation_key,
      reason: queued?.reason || "unknown",
      error_description: queued?.reason || null,
    });
  }

  return {
    ok: Boolean(queued?.ok),
    queued: Boolean(queued?.ok),
    handled: true,
    reason: queued?.ok ? "seller_flow_reply_queued" : queued?.reason || "seller_flow_queue_failed",
    plan,
    queue_result: queued,
    schedule,
    response_window,
    brain_stage: queued?.ok ? brainStageForUseCase(plan.selected_use_case) : null,
  };
}

export default maybeQueueSellerStageReply;
