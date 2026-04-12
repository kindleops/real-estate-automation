// ─── maybe-queue-underwriting-follow-up.js ───────────────────────────────
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";
import { getNumberValue } from "@/lib/providers/podio.js";
import { collapseConversationStageToLegacy } from "@/lib/domain/communications-engine/state-machine.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

function asBoolean(value) {
  return value === true;
}

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function unique(values = []) {
  return [...new Set((values || []).filter(Boolean))];
}

function deriveLatestOutboundUseCase(context = null) {
  const recent_events = Array.isArray(context?.recent?.recent_events)
    ? context.recent.recent_events
    : [];

  const latest_outbound = recent_events.find(
    (event) => lower(event?.direction) === "outbound"
  );

  return lower(
    latest_outbound?.selected_use_case ||
      latest_outbound?.template_use_case ||
      latest_outbound?.metadata?.selected_use_case ||
      latest_outbound?.metadata?.template_use_case ||
      ""
  );
}

function isNegativeNoInfoMessage(message = "") {
  const text = lower(message);

  return (
    /^(?:no|nope|nah|none)\b/i.test(clean(message)) ||
    includesAny(text, [
      "don't know",
      "dont know",
      "not sure",
      "no idea",
      "don't have that",
      "dont have that",
      "don't have it",
      "dont have it",
      "not off hand",
      "not offhand",
      "unknown",
    ])
  );
}

function deriveStrategyName({ strategy = null, signals = {} } = {}) {
  return clean(strategy?.strategy || signals.underwriting_strategy);
}

function deriveUnitCount({ strategy = null, signals = {}, context = null } = {}) {
  return (
    asNumber(signals.unit_count) ??
    asNumber(strategy?.unit_count) ??
    getNumberValue(context?.items?.property_item || null, "number-of-units", null) ??
    null
  );
}

function derivePropertyType({ strategy = null, signals = {}, route = null, context = null } = {}) {
  return clean(
    signals.property_type ||
      strategy?.property_type ||
      context?.summary?.property_type ||
      route?.primary_category ||
      "Residential"
  );
}

function isMultifamily({ property_type = "", unit_count = null, route = null } = {}) {
  return (
    includesAny(property_type, [
      "multifamily",
      "multi family",
      "apartment",
      "apartments",
      "5+ unit",
      "commercial multifamily",
    ]) ||
    (unit_count !== null && unit_count >= 5) ||
    route?.is_multifamily_like === true
  );
}

function isCreativeTrack({ strategy_name = "", signals = {} } = {}) {
  return (
    includesAny(strategy_name, ["creative", "subject", "seller"]) ||
    signals.creative_terms_interest === true
  );
}

function isNovationTrack({ strategy_name = "", signals = {} } = {}) {
  return (
    includesAny(strategy_name, ["novation"]) ||
    signals.novation_interest === true
  );
}

function determineAlternativeTrack({
  strategy_name = "",
  classification = null,
  signals = {},
} = {}) {
  const normalized_strategy = lower(strategy_name);
  const objection = clean(classification?.objection).toLowerCase();

  if (normalized_strategy.includes("novation")) return "novation";
  if (normalized_strategy.includes("creative")) return "creative";

  if (
    normalized_strategy === "creative_or_novation_review" &&
    (signals.novation_interest === true ||
      objection === "wants_retail" ||
      objection === "has_other_buyer")
  ) {
    return "novation";
  }

  if (signals.creative_terms_interest === true) return "creative";
  if (signals.novation_interest === true) return "novation";

  return null;
}

function allowsFollowUpWhileManualReview(strategy_name = "") {
  return [
    "mf_creative_review",
    "creative_review",
    "creative_or_novation_review",
    "novation_review",
  ].includes(clean(strategy_name));
}

function hasDetailedCreativeTerms(signals = {}) {
  return [
    signals.down_payment,
    signals.monthly_payment,
    signals.interest_rate,
    signals.loan_terms_months,
    signals.balloon_payment,
    signals.existing_mortgage_balance,
    signals.existing_mortgage_payment,
  ].some((value) => value !== null && value !== undefined && value !== "");
}

function buildFollowUpDecision({
  underwriting = null,
  classification = null,
  route = null,
  context = null,
  message = "",
} = {}) {
  if (!context?.found) {
    return {
      should_queue: false,
      reason: "context_not_found",
    };
  }

  if (classification?.compliance_flag === "stop_texting") {
    return {
      should_queue: false,
      reason: "compliance_stop",
    };
  }

  const signals = underwriting?.signals || {};
  const strategy = underwriting?.strategy || {};
  const strategy_name = deriveStrategyName({ strategy, signals });
  const route_stage = collapseConversationStageToLegacy(
    route?.stage || signals.route_stage || context?.summary?.conversation_stage || null,
    null
  );
  const route_use_case = route?.use_case || signals.route_use_case || null;

  if (route_stage === "Contract") {
    return {
      should_queue: false,
      reason: "already_in_contract_stage",
    };
  }

  if (
    includesAny(route_use_case, [
      "not_interested",
      "wrong_person",
      "who_is_this",
    ])
  ) {
    return {
      should_queue: false,
      reason: "non_progressive_use_case",
    };
  }

  const needs_manual_review =
    asBoolean(strategy?.needs_manual_review) ||
    asBoolean(signals.underwriting_needs_manual_review);

  if (needs_manual_review && !allowsFollowUpWhileManualReview(strategy_name)) {
    return {
      should_queue: false,
      reason: "manual_review_required",
    };
  }

  const unit_count = deriveUnitCount({ strategy, signals, context });
  const property_type = derivePropertyType({ strategy, signals, route, context });
  const multifamily = isMultifamily({ property_type, unit_count, route });
  const alternative_track = determineAlternativeTrack({
    strategy_name,
    classification,
    signals,
  });
  const creative_track = alternative_track === "creative";
  const novation_track = alternative_track === "novation";
  const latest_outbound_use_case = deriveLatestOutboundUseCase(context);
  const missing_fields = [];

  if (multifamily) {
    if (!unit_count) missing_fields.push("unit_count");
    if (!clean(signals.occupancy_status)) missing_fields.push("occupancy_status");
    if (!signals.rents_present) missing_fields.push("rents");
    if (!signals.expenses_present) missing_fields.push("expenses");
  } else if (novation_track) {
    if (!signals.novation_listing_readiness_present) {
      missing_fields.push("listing_readiness");
    }
    if (
      asNumber(signals.target_net_to_seller) === null &&
      asNumber(signals.asking_price) === null
    ) {
      missing_fields.push("target_net_to_seller");
    }
    if (!clean(signals.estimated_repair_scope) && !clean(signals.condition_level)) {
      missing_fields.push("condition_scope");
    }
    if (!clean(signals.timeline)) {
      missing_fields.push("timeline");
    }
  } else if (creative_track) {
    if (!signals.creative_terms_present) {
      missing_fields.push("creative_probe");
    } else if (!hasDetailedCreativeTerms(signals)) {
      missing_fields.push("creative_followup");
    }
  } else {
    if (!clean(signals.condition_level)) {
      missing_fields.push("condition_level");
    }
    if (!clean(signals.occupancy_status)) {
      missing_fields.push("occupancy_status");
    }
  }

  const unique_missing_fields = unique(missing_fields);

  if (multifamily && latest_outbound_use_case === "mf_underwriting_ack") {
    return {
      should_queue: false,
      reason: "multifamily_waiting_on_internal_offer",
      missing_fields: unique_missing_fields,
      offer_ready: true,
    };
  }

  const multifamily_completion_ack =
    multifamily &&
    (
      unique_missing_fields.length === 0 ||
      (
        isNegativeNoInfoMessage(message) &&
        ["mf_rents", "mf_expenses"].includes(latest_outbound_use_case)
      )
    );

  if (multifamily_completion_ack) {
    return {
      should_queue: true,
      reason:
        unique_missing_fields.length === 0
          ? "multifamily_underwriting_complete"
          : "multifamily_ready_to_run_numbers",
      missing_fields: unique_missing_fields,
      unit_count,
      property_type,
      is_multifamily: multifamily,
      creative_track,
      novation_track,
      strategy_name,
      signals,
      offer_ready: true,
      completion_ack: true,
      latest_outbound_use_case,
    };
  }

  if (!unique_missing_fields.length) {
    return {
      should_queue: false,
      reason: "no_follow_up_needed",
      missing_fields: [],
      offer_ready: false,
    };
  }

  return {
    should_queue: true,
    reason: "missing_underwriting_inputs",
    missing_fields: unique_missing_fields,
    unit_count,
    property_type,
    is_multifamily: multifamily,
    creative_track,
    novation_track,
    strategy_name,
    signals,
    offer_ready: false,
    completion_ack: false,
    latest_outbound_use_case,
  };
}

function buildMultifamilyFollowUp({ decision = {} } = {}) {
  const unit_count = asNumber(decision.unit_count);
  const missing_fields = unique(decision.missing_fields);
  const render_overrides = {
    units: unit_count !== null ? String(unit_count) : null,
  };

  if (decision.completion_ack) {
    return {
      use_case: "mf_underwriting_ack",
      category: "Landlord / Multifamily",
      secondary_category: "Underwriting",
      variant_group: "Multifamily Underwrite — Acknowledgment",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Specialist-Landlord / Market-Local",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides,
      offer_ready: true,
    };
  }

  if (missing_fields.includes("unit_count")) {
    const has_unit_guess = unit_count !== null;

    return has_unit_guess
      ? {
          use_case: "mf_confirm_units",
          category: "Landlord / Multifamily",
          secondary_category: "Underwriting",
          variant_group: "Multifamily Underwrite — Units",
          tone: "Neutral",
          sequence_position: "V1",
          paired_with_agent_type: "Specialist-Landlord / Market-Local",
          fallback_agent_type: "Fallback / Market-Local",
          render_overrides,
        }
      : {
          use_case: "mf_confirm_units",
          category: "Landlord / Multifamily",
          secondary_category: "Underwriting",
          variant_group: "Multifamily Underwrite - Units (Open)",
          tone: "Neutral",
          sequence_position: "V1",
          paired_with_agent_type: "Specialist-Landlord / Market-Local",
          fallback_agent_type: "Fallback / Market-Local",
          render_overrides,
        };
  }

  if (missing_fields.includes("occupancy_status")) {
    return {
      use_case: "mf_occupancy",
      category: "Landlord / Multifamily",
      secondary_category: "Underwriting",
      variant_group: "Multifamily Underwrite — Occupancy",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Specialist-Landlord / Market-Local",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides,
    };
  }

  if (missing_fields.includes("rents")) {
    return {
      use_case: "mf_rents",
      category: "Landlord / Multifamily",
      secondary_category: "Underwriting",
      variant_group: "Multifamily Underwrite — Rents",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Specialist-Landlord / Market-Local",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides,
    };
  }

  if (missing_fields.includes("expenses")) {
    return {
      use_case: "mf_expenses",
      category: "Landlord / Multifamily",
      secondary_category: "Underwriting",
      variant_group: "Multifamily Underwrite — Expenses",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Specialist-Landlord / Market-Local",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides,
    };
  }

  return null;
}

function buildCreativeFollowUp({ decision = {} } = {}) {
  const use_followup = decision.missing_fields.includes("creative_followup");

  return use_followup
    ? {
        use_case: "creative_followup",
        category: "Residential",
        secondary_category: "Negotiation",
        variant_group: "Creative Finance Follow-Up",
        tone: "Neutral",
        sequence_position: "V1",
        paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
        fallback_agent_type: "Fallback / Market-Local",
        render_overrides: {},
      }
    : {
        use_case: "creative_probe",
        category: decision.is_multifamily ? "Landlord / Multifamily" : "Residential",
        secondary_category: "Negotiation",
        variant_group: "Creative Finance Probe",
        tone: "Neutral",
        sequence_position: "V1",
        paired_with_agent_type: decision.is_multifamily
          ? "Specialist-Landlord / Market-Local"
          : "Fallback / Market-Local",
        fallback_agent_type: "Fallback / Market-Local",
        render_overrides: {},
      };
}

function buildNovationFollowUp({ decision = {} } = {}) {
  const missing_fields = unique(decision.missing_fields);

  if (missing_fields.includes("listing_readiness")) {
    return {
      use_case: "novation_listing_readiness",
      category: "Residential",
      secondary_category: "Underwriting",
      variant_group: "Novation Underwrite - Listing Readiness",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides: {},
    };
  }

  if (missing_fields.includes("target_net_to_seller")) {
    return {
      use_case: "novation_net_to_seller",
      category: "Residential",
      secondary_category: "Negotiation",
      variant_group: "Novation Underwrite - Seller Net",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides: {},
    };
  }

  if (missing_fields.includes("condition_scope")) {
    return {
      use_case: "novation_condition_scope",
      category: "Residential",
      secondary_category: "Underwriting",
      variant_group: "Novation Underwrite - Condition Scope",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides: {},
    };
  }

  if (missing_fields.includes("timeline")) {
    return {
      use_case: "novation_timeline",
      category: "Residential",
      secondary_category: "Underwriting",
      variant_group: "Novation Underwrite - Timeline",
      tone: "Neutral",
      sequence_position: "V1",
      paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
      fallback_agent_type: "Fallback / Market-Local",
      render_overrides: {},
    };
  }

  return {
    use_case: "novation_probe",
    category: "Residential",
    secondary_category: "Negotiation",
    variant_group: "Novation Probe",
    tone: "Neutral",
    sequence_position: "V1",
    paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
    fallback_agent_type: "Fallback / Market-Local",
    render_overrides: {},
  };
}

function buildResidentialFollowUp() {
  return {
    use_case: "condition_question_set",
    category: "Residential",
    secondary_category: "Underwriting",
    variant_group: "SMS-Only Underwriting",
    tone: "Neutral",
    sequence_position: "V1",
    paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
    fallback_agent_type: "Fallback / Market-Local",
    render_overrides: {},
  };
}

function buildFollowUpTemplateRequest(decision = {}) {
  if (decision.is_multifamily) {
    return buildMultifamilyFollowUp({ decision });
  }

  if (decision.novation_track) {
    return buildNovationFollowUp({ decision });
  }

  if (decision.creative_track) {
    return buildCreativeFollowUp({ decision });
  }

  return buildResidentialFollowUp();
}

export async function maybeQueueUnderwritingFollowUp({
  inbound_from = null,
  underwriting = null,
  classification = null,
  route = null,
  context = null,
  message = "",
  create_brain_if_missing = true,
  queue_status = "Queued",
  created_by = "Underwriting Follow-Up Engine",
  queue_message = queueOutboundMessage,
} = {}) {
  const decision = buildFollowUpDecision({
    underwriting,
    classification,
    route,
    context,
    message,
  });

  if (!decision.should_queue) {
    return {
      ok: true,
      queued: false,
      reason: decision.reason,
      missing_fields: decision.missing_fields || [],
    };
  }

  const follow_up = buildFollowUpTemplateRequest(decision);

  if (!follow_up?.use_case) {
    return {
      ok: true,
      queued: false,
      reason: "follow_up_template_not_resolved",
      missing_fields: decision.missing_fields || [],
    };
  }

  const queue_result = await queue_message({
    inbound_from:
      inbound_from ||
      context?.summary?.phone_hidden ||
      context?.inbound_from ||
      null,
    seed_message: "",
    create_brain_if_missing,
    category: follow_up.category,
    secondary_category: follow_up.secondary_category,
    use_case: follow_up.use_case,
    variant_group: follow_up.variant_group,
    tone: follow_up.tone,
    sequence_position: follow_up.sequence_position,
    paired_with_agent_type: follow_up.paired_with_agent_type,
    fallback_agent_type: follow_up.fallback_agent_type,
    template_render_overrides: follow_up.render_overrides || {},
    message_type: "Follow-Up",
    queue_status,
  });

  return {
    ok: Boolean(queue_result?.ok),
    queued: Boolean(queue_result?.ok),
    reason: queue_result?.ok
      ? "underwriting_follow_up_queued"
      : queue_result?.reason || "underwriting_follow_up_failed",
    missing_fields: decision.missing_fields || [],
    offer_ready: decision.offer_ready === true,
    follow_up: {
      ...follow_up,
      strategy_name: decision.strategy_name,
      created_by,
    },
    queue_result,
    created_by,
  };
}

export default maybeQueueUnderwritingFollowUp;
