import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";
import { resolveLatencyAwareQueueSchedule } from "@/lib/domain/queue/queue-schedule.js";
import {
  brainStageForUseCase,
  SELLER_FLOW_STAGES,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { routeSellerConversation } from "@/lib/domain/seller-flow/route-seller-conversation.js";

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
  now = new Date().toISOString(),
  queue_message = queueOutboundMessage,
  schedule_resolver = resolveLatencyAwareQueueSchedule,
} = {}) {
  const plan = routeSellerConversation({
    context,
    classification,
    message,
    previous_outbound_use_case,
    maybe_offer,
    existing_offer,
  });

  if (!plan?.handled) {
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

  const queued = await queue_message({
    inbound_from,
    create_brain_if_missing: false,
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
    },
  });

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
