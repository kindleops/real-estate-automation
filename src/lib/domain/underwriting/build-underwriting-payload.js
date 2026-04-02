// ─── build-underwriting-payload.js ───────────────────────────────────────
import {
  UNDERWRITING_FIELDS,
} from "@/lib/podio/apps/underwriting.js";

function clean(value) {
  return String(value ?? "").trim();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function asArrayAppRef(value) {
  if (!value) return undefined;
  return [value];
}

function buildReasonSentToUnderwriting(signals = {}) {
  const property_type = clean(signals.property_type).toLowerCase();
  const strategy = clean(signals.underwriting_strategy).toLowerCase();
  const objection = clean(signals.classification_objection).toLowerCase();
  const condition = clean(signals.condition_level).toLowerCase();

  if (property_type === "multifamily" || strategy.includes("mf_")) {
    return "Multifamily Deal";
  }

  if (strategy.includes("novation") || signals.novation_interest === true) {
    return "Novation Opportunity";
  }

  if (
    objection === "need_more_money" ||
    objection === "wants_retail"
  ) {
    return "Wants More Than Cash";
  }

  if (signals.creative_terms_interest === true) {
    return "Asked for Terms";
  }

  if (condition === "moderate" || condition === "heavy") {
    return "Complex Condition";
  }

  return "AI Escalation";
}

function buildUnderwritingType(signals = {}) {
  const property_type = clean(signals.property_type).toLowerCase();
  const strategy = clean(signals.underwriting_strategy).toLowerCase();

  if (property_type === "multifamily" || strategy.includes("mf_")) {
    return "Multifamily";
  }

  if (strategy.includes("novation") || signals.novation_interest === true) {
    return "Novation";
  }

  if (
    signals.creative_terms_interest === true ||
    strategy.includes("creative") ||
    strategy.includes("subject") ||
    strategy.includes("seller")
  ) {
    return "Creative";
  }

  return null;
}

function buildAutomationResult(signals = {}) {
  if (signals.underwriting_auto_offer_ready === true) return "Approved";
  if (signals.underwriting_needs_manual_review === true) {
    return "Needs Alternative Strategy";
  }
  return null;
}

function buildUnderwritingStatus({ signals = {}, offer_item_id = null } = {}) {
  if (offer_item_id) return "Sent to Offers";
  if (signals.underwriting_needs_manual_review === true) return "Queued";
  if (signals.underwriting_auto_offer_ready === true) return "Completed";
  return "Running";
}

function buildAutomationStatus(signals = {}) {
  if (signals.underwriting_auto_offer_ready === true) return "Completed";
  if (signals.underwriting_needs_manual_review === true) return "Running";
  return "Running";
}

function buildCurrentEngineStep({ signals = {}, offer_item_id = null } = {}) {
  if (offer_item_id) return "Sent to Offers";
  if (signals.underwriting_auto_offer_ready === true) return "Finalizing Output";
  if (signals.underwriting_needs_manual_review === true) return "Structuring Terms";
  return "Evaluating";
}

function buildRiskSummary(signals = {}) {
  const parts = [];

  if (Array.isArray(signals.distress_tags) && signals.distress_tags.length) {
    parts.push(`Distress Tags: ${signals.distress_tags.join(", ")}`);
  }

  if (clean(signals.classification_objection)) {
    parts.push(`Objection: ${clean(signals.classification_objection)}`);
  }

  if (clean(signals.classification_emotion)) {
    parts.push(`Emotion: ${clean(signals.classification_emotion)}`);
  }

  return parts.join("\n");
}

function buildTermsJustification(signals = {}) {
  return [
    clean(signals.underwriting_reason)
      ? `Strategy Reason: ${clean(signals.underwriting_reason)}`
      : "",
    clean(signals.timeline) ? `Timeline: ${clean(signals.timeline)}` : "",
    clean(signals.occupancy_status)
      ? `Occupancy: ${clean(signals.occupancy_status)}`
      : "",
    clean(signals.condition_level)
      ? `Condition: ${clean(signals.condition_level)}`
      : "",
    clean(signals.creative_terms_summary)
      ? `Creative Terms: ${clean(signals.creative_terms_summary)}`
      : "",
    asNumber(signals.current_gross_rents) !== null
      ? `Gross Rents: $${asNumber(signals.current_gross_rents)}`
      : "",
    asNumber(signals.estimated_expenses) !== null
      ? `Expenses: $${asNumber(signals.estimated_expenses)}`
      : "",
    asNumber(signals.target_net_to_seller) !== null
      ? `Seller Net Target: $${asNumber(signals.target_net_to_seller)}`
      : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function buildEscalationSummary(signals = {}, notes = "", source_channel = "SMS") {
  return [
    clean(notes),
    clean(signals.raw_message) ? `Source Message: ${clean(signals.raw_message)}` : "",
    clean(source_channel) ? `Source Channel: ${clean(source_channel)}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function buildMultifamilySummary(signals = {}) {
  return [
    asNumber(signals.unit_count) !== null ? `Units: ${asNumber(signals.unit_count)}` : "",
    clean(signals.occupancy_status) ? `Occupancy: ${clean(signals.occupancy_status)}` : "",
    asNumber(signals.current_gross_rents) !== null
      ? `Gross Rents: $${asNumber(signals.current_gross_rents)}`
      : "",
    asNumber(signals.estimated_expenses) !== null
      ? `Expenses: $${asNumber(signals.estimated_expenses)}`
      : "",
    asNumber(signals.current_gross_rents) !== null && asNumber(signals.estimated_expenses) !== null
      ? `NOI: $${asNumber(signals.current_gross_rents) - asNumber(signals.estimated_expenses)}`
      : "",
  ]
    .filter(Boolean)
    .join("; ");
}

function buildNovationSummary(signals = {}) {
  if (clean(signals.novation_summary)) return clean(signals.novation_summary);

  return [
    asNumber(signals.asking_price) !== null ? `Ask/List Anchor: $${asNumber(signals.asking_price)}` : "",
    asNumber(signals.target_net_to_seller) !== null
      ? `Target Net: $${asNumber(signals.target_net_to_seller)}`
      : "",
    clean(signals.listing_readiness) ? `Readiness: ${clean(signals.listing_readiness)}` : "",
    clean(signals.estimated_repair_scope)
      ? `Repairs: ${clean(signals.estimated_repair_scope)}`
      : "",
  ]
    .filter(Boolean)
    .join("; ");
}

export function buildUnderwritingPayload({
  context = null,
  signals = {},
  offer_item_id = null,
  pipeline_item_id = null,
  underwriting_id = null,
  source_channel = "SMS",
  notes = "",
} = {}) {
  const ids = context?.ids || {};
  const underwriting_type = buildUnderwritingType(signals);
  const underwriting_status = buildUnderwritingStatus({ signals, offer_item_id });
  const automation_status = buildAutomationStatus(signals);
  const current_engine_step = buildCurrentEngineStep({ signals, offer_item_id });
  const automation_result = buildAutomationResult(signals);
  const reason_sent_to_underwriting = buildReasonSentToUnderwriting(signals);
  const now = new Date().toISOString();
  const current_gross_rents = asNumber(signals.current_gross_rents);
  const estimated_expenses = asNumber(signals.estimated_expenses);
  const noi =
    current_gross_rents !== null && estimated_expenses !== null
      ? current_gross_rents - estimated_expenses
      : null;
  const title =
    underwriting_id ||
    [
      clean(context?.summary?.owner_name || ""),
      clean(context?.summary?.property_address || ""),
      clean(underwriting_type || "Underwriting"),
    ]
      .filter(Boolean)
      .join(" - ");

  const payload = {
    [UNDERWRITING_FIELDS.title]: title || undefined,
    [UNDERWRITING_FIELDS.underwriting_id]: underwriting_id || undefined,
    [UNDERWRITING_FIELDS.underwriting_type]: underwriting_type || undefined,
    [UNDERWRITING_FIELDS.underwriting_status]: underwriting_status,

    ...(ids.master_owner_id
      ? { [UNDERWRITING_FIELDS.master_owner]: asArrayAppRef(ids.master_owner_id) }
      : {}),
    ...(ids.prospect_id
      ? { [UNDERWRITING_FIELDS.prospect]: asArrayAppRef(ids.prospect_id) }
      : {}),
    ...(ids.property_id
      ? { [UNDERWRITING_FIELDS.property]: asArrayAppRef(ids.property_id) }
      : {}),
    ...(ids.brain_item_id
      ? { [UNDERWRITING_FIELDS.conversation]: asArrayAppRef(ids.brain_item_id) }
      : {}),
    ...(ids.phone_item_id
      ? { [UNDERWRITING_FIELDS.phone_number]: asArrayAppRef(ids.phone_item_id) }
      : {}),
    ...(offer_item_id
      ? { [UNDERWRITING_FIELDS.offer]: asArrayAppRef(offer_item_id) }
      : {}),
    ...(ids.market_id
      ? { [UNDERWRITING_FIELDS.market]: asArrayAppRef(ids.market_id) }
      : {}),
    [UNDERWRITING_FIELDS.reason_sent_to_underwriting]:
      reason_sent_to_underwriting || undefined,
    [UNDERWRITING_FIELDS.seller_asking_price]:
      asNumber(signals.asking_price) ?? undefined,
    [UNDERWRITING_FIELDS.seller_counter_offer]:
      asNumber(signals.desired_price) ?? undefined,
    [UNDERWRITING_FIELDS.escalation_summary]:
      buildEscalationSummary(signals, notes, source_channel) || undefined,
    [UNDERWRITING_FIELDS.creative_strategy]:
      clean(signals.creative_strategy) || undefined,
    [UNDERWRITING_FIELDS.purchase_price]:
      asNumber(signals.asking_price) ?? undefined,
    [UNDERWRITING_FIELDS.down_payment]:
      asNumber(signals.down_payment) ?? undefined,
    [UNDERWRITING_FIELDS.monthly_payment]:
      asNumber(signals.monthly_payment) ?? undefined,
    [UNDERWRITING_FIELDS.interest_rate]:
      asNumber(signals.interest_rate) ?? undefined,
    [UNDERWRITING_FIELDS.loan_terms_months]:
      asNumber(signals.loan_terms_months) ?? undefined,
    [UNDERWRITING_FIELDS.balloon_payment]:
      asNumber(signals.balloon_payment) ?? undefined,
    [UNDERWRITING_FIELDS.existing_mortgage_balance]:
      asNumber(signals.existing_mortgage_balance) ?? undefined,
    [UNDERWRITING_FIELDS.existing_mortgage_payment]:
      asNumber(signals.existing_mortgage_payment) ?? undefined,
    [UNDERWRITING_FIELDS.creative_terms_summary]:
      clean(signals.creative_terms_summary) || undefined,
    [UNDERWRITING_FIELDS.number_of_units_snapshot]:
      asNumber(signals.unit_count) ?? undefined,
    [UNDERWRITING_FIELDS.occupancy_at_underwriting]:
      clean(signals.occupancy_status) || undefined,
    [UNDERWRITING_FIELDS.current_gross_rents]:
      current_gross_rents ?? undefined,
    [UNDERWRITING_FIELDS.estimated_expenses]:
      estimated_expenses ?? undefined,
    [UNDERWRITING_FIELDS.noi]:
      noi ?? undefined,
    [UNDERWRITING_FIELDS.mf_summary]:
      buildMultifamilySummary(signals) || undefined,
    [UNDERWRITING_FIELDS.novation_list_price]:
      asNumber(signals.asking_price) ?? undefined,
    [UNDERWRITING_FIELDS.target_net_to_seller]:
      asNumber(signals.target_net_to_seller) ?? undefined,
    [UNDERWRITING_FIELDS.estimated_repair_scope]:
      clean(signals.estimated_repair_scope) || undefined,
    [UNDERWRITING_FIELDS.estimated_repair_cost]:
      asNumber(signals.estimated_repair_cost) ?? undefined,
    [UNDERWRITING_FIELDS.estimated_days_to_sell]:
      asNumber(signals.estimated_days_to_sell) ?? undefined,
    [UNDERWRITING_FIELDS.novation_summary]:
      buildNovationSummary(signals) || undefined,
    [UNDERWRITING_FIELDS.ai_recommended_strategy]:
      clean(signals.underwriting_strategy) || underwriting_type || undefined,
    [UNDERWRITING_FIELDS.ai_recommended_next_move]:
      clean(signals.route_use_case) || undefined,
    [UNDERWRITING_FIELDS.ai_risk_summary]:
      buildRiskSummary(signals) || undefined,
    [UNDERWRITING_FIELDS.ai_offer_terms_justification]:
      buildTermsJustification(signals) || undefined,
    [UNDERWRITING_FIELDS.ai_confidence_score]:
      asNumber(signals.motivation_score) ?? undefined,
    [UNDERWRITING_FIELDS.automation_result]:
      automation_result || undefined,
    [UNDERWRITING_FIELDS.rejection_failure_reason]:
      clean(signals.underwriting_needs_manual_review ? signals.underwriting_reason : "") ||
      undefined,
    [UNDERWRITING_FIELDS.automation_status]:
      automation_status || undefined,
    [UNDERWRITING_FIELDS.current_engine_step]:
      current_engine_step || undefined,
    [UNDERWRITING_FIELDS.triggered_at]: { start: now },
    [UNDERWRITING_FIELDS.completed_at]:
      automation_status === "Completed" ? { start: now } : undefined,
    [UNDERWRITING_FIELDS.sent_to_offers_date]:
      offer_item_id ? { start: now } : undefined,
    [UNDERWRITING_FIELDS.retry_count]: 0,
  };

  Object.keys(payload).forEach((key) => {
    if (payload[key] === undefined || payload[key] === null || payload[key] === "") {
      delete payload[key];
    }
  });

  return {
    ok: true,
    payload,
    field_count: Object.keys(payload).length,
  };
}

export default buildUnderwritingPayload;
