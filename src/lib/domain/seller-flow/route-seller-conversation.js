import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";
import { formatUsd } from "@/lib/utils/money.js";
import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import {
  SELLER_FLOW_STAGES,
  normalizeSellerFlowUseCase,
  canonicalStageForUseCase,
  inferCanonicalUseCaseFromOutboundText,
  normalizeSellerFlowTone,
  preferredAgentTypeForSellerFlow,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";

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

function hasAffirmative(message = "") {
  return /^(yes|yeah|yep|yup|correct|that'?s right|sure|ok|okay)\b/i.test(clean(message));
}

function hasTimelineHesitation(message = "", classification = null) {
  if (clean(classification?.objection) === "need_time") return true;

  return includesAny(message, [
    "not sure yet",
    "need some time",
    "let me think",
    "have to think about it",
    "need to think about it",
    "not ready yet",
    "maybe later",
    "circle back later",
    "not there yet",
  ]);
}

function detectIdentityRoute(message = "", classification = null) {
  if (
    includesAny(message, [
      "how did you get my number",
      "where did you get my number",
      "how you got my number",
      "why do you have my number",
    ])
  ) {
    return "how_got_number";
  }

  if (
    classification?.objection === "who_is_this" ||
    includesAny(message, [
      "who is this",
      "who's this",
      "who are you",
      "what company is this",
      "what is this about",
    ])
  ) {
    return "who_is_this";
  }

  return null;
}

function detectWrongPerson(message = "", classification = null) {
  if (classification?.objection === "wrong_number") return true;

  return includesAny(message, [
    "wrong person",
    "wrong number",
    "not me",
    "i don't own",
    "i dont own",
    "not my property",
    "don't own that",
    "do not own that",
  ]);
}

function detectNotInterested(message = "", classification = null) {
  if (classification?.objection === "not_interested") return true;

  return includesAny(message, [
    "not interested",
    "not selling",
    "no thanks",
    "leave me alone",
    "stop bothering me",
  ]);
}

function detectOptOut(message = "", classification = null) {
  if (classification?.compliance_flag === "stop_texting") return true;

  return includesAny(message, [
    "stop",
    "unsubscribe",
    "remove me",
    "quit texting",
    "do not text",
    "don't text",
  ]);
}

function detectOpenToSelling(message = "", previous_stage = null) {
  if (
    includesAny(message, [
      "would consider selling",
      "open to selling",
      "open to an offer",
      "open to offer",
      "if the price was right",
      "if the number made sense",
      "i'd consider it",
      "i would consider it",
      "maybe",
      "possibly",
      "depends",
    ])
  ) {
    return true;
  }

  return previous_stage === SELLER_FLOW_STAGES.CONSIDER_SELLING && hasAffirmative(message);
}

function detectOwnershipConfirmed(message = "", previous_stage = null) {
  if (
    includesAny(message, [
      "i own it",
      "i am the owner",
      "that's my property",
      "that is my property",
      "yes i own",
      "yes that's mine",
      "yes thats mine",
      "yes, i do",
    ])
  ) {
    return true;
  }

  return previous_stage === SELLER_FLOW_STAGES.OWNERSHIP_CHECK && hasAffirmative(message);
}

function hasReverseOfferRequest(message = "", classification = null) {
  if (classification?.objection === "send_offer_first") return true;

  return includesAny(message, [
    "make me an offer",
    "what are you offering",
    "what's your offer",
    "what is your offer",
    "what's your number",
    "what is your number",
    "you tell me",
    "send me your offer",
    "what can you pay",
    "what would you pay",
    "i don't have a number",
    "i dont have a number",
    "no idea",
  ]);
}

function hasCounterSignal(message = "", classification = null) {
  if (
    ["need_more_money", "has_other_buyer", "wants_retail"].includes(
      classification?.objection || ""
    )
  ) {
    return true;
  }

  return includesAny(message, [
    "too low",
    "seems low",
    "can you do better",
    "need more",
    "come up",
    "meet me at",
    "my floor is",
    "lowest i can do",
    "best you can do",
  ]);
}

function hasHandoffTrigger(message = "", classification = null) {
  if (
    ["wants_written_offer", "wants_proof_of_funds"].includes(
      clean(classification?.objection)
    )
  ) {
    return true;
  }

  return includesAny(message, [
    "send the contract",
    "send contract",
    "send me the contract",
    "what's next",
    "what is next",
    "next step",
    "move forward",
    "move ahead",
    "let's do it",
    "lets do it",
    "ready to move forward",
    "ready to sell",
    "we have a deal",
    "that works",
  ]);
}

function hasEmotionalResistance(message = "", classification = null) {
  if (["frustrated", "guarded", "skeptical"].includes(clean(classification?.emotion))) {
    return true;
  }

  return includesAny(message, [
    "ridiculous",
    "insulting",
    "crazy",
    "joke",
    "laughable",
    "wasting my time",
    "waste my time",
    "not serious",
    "lowball",
  ]);
}

function hasSoftCounterSignal(message = "") {
  return includesAny(message, [
    "too low",
    "seems low",
    "can you do better",
    "can you do a little better",
    "a little better",
    "do better",
    "come up",
    "come up a bit",
    "a little higher",
    "any room",
    "best you can do",
  ]);
}

function hasHardCounterSignal({
  message = "",
  counter_amount = null,
} = {}) {
  if (
    counter_amount !== null &&
    counter_amount !== undefined &&
    counter_amount !== "" &&
    Number.isFinite(Number(counter_amount))
  ) {
    return true;
  }

  return includesAny(message, [
    "bottom line",
    "my floor",
    "firm at",
    "firm on",
    "won't take less",
    "wont take less",
    "minimum i would take",
    "lowest i can do",
    "at least",
  ]);
}

function hasPropertyInfo(signals = {}) {
  return Boolean(
    signals.occupancy_status ||
      signals.condition_level ||
      signals.estimated_repair_cost ||
      signals.timeline ||
      signals.unit_count ||
      signals.rents_present ||
      signals.expenses_present
  );
}

function hasEnoughPropertyFactsForNegotiation(signals = {}) {
  return Boolean(
    signals.occupancy_status &&
      (signals.condition_level || signals.estimated_repair_cost || signals.timeline)
  );
}

function normalizePropertyType(value = "") {
  const text = lower(value);

  if (
    includesAny(text, [
      "multifamily",
      "multi family",
      "apartment",
      "apartments",
      "5+ unit",
      "commercial multifamily",
    ])
  ) {
    return "Multifamily";
  }

  return clean(value) || "Residential";
}

function derivePropertyType({ context = null, signals = {} } = {}) {
  return normalizePropertyType(
    signals.property_type ||
      getCategoryValue(context?.items?.property_item || null, "property-type", null) ||
      context?.summary?.property_type ||
      "Residential"
  );
}

function isMultifamilyLike({ context = null, signals = {} } = {}) {
  const unit_count =
    Number.isFinite(Number(signals.unit_count))
      ? Number(signals.unit_count)
      : getNumberValue(context?.items?.property_item || null, "number-of-units", null);
  const property_type = derivePropertyType({ context, signals });

  return (
    property_type === "Multifamily" ||
    (Number.isFinite(Number(unit_count)) && Number(unit_count) >= 5)
  );
}

function chooseConversationalTone({
  classification = null,
  previous_tone = null,
  selected_use_case = null,
} = {}) {
  if (selected_use_case === "who_is_this") return "Neutral";
  if (selected_use_case === "how_got_number") return "Calm";
  if (selected_use_case === "wrong_person") return "Neutral";
  if (selected_use_case === "not_interested") return "Calm";

  const stable_previous = normalizeSellerFlowTone(previous_tone);
  if (stable_previous && ["Warm", "Human", "Direct", "Empathetic"].includes(stable_previous)) {
    return stable_previous;
  }

  switch (clean(classification?.emotion)) {
    case "motivated":
    case "tired_landlord":
      return "Direct";
    case "skeptical":
    case "guarded":
      return "Human";
    case "frustrated":
    case "overwhelmed":
    case "grieving":
      return "Empathetic";
    default:
      return "Warm";
  }
}

function derivePreviousOutboundPlan({
  context = null,
  previous_outbound_use_case = null,
} = {}) {
  const explicit_use_case = normalizeSellerFlowUseCase(previous_outbound_use_case);

  if (explicit_use_case) {
    return {
      selected_use_case: explicit_use_case,
      next_expected_stage: canonicalStageForUseCase(explicit_use_case),
      selected_tone: null,
    };
  }

  const recent_events = Array.isArray(context?.recent?.recent_events)
    ? context.recent.recent_events
    : [];

  const latest_outbound = recent_events.find(
    (event) => lower(event?.direction) === "outbound"
  );

  const metadata = latest_outbound?.metadata || {};
  const selected_use_case = normalizeSellerFlowUseCase(
    metadata.selected_use_case ||
      latest_outbound?.selected_use_case ||
      metadata.canonical_use_case,
    metadata.selected_variant_group ||
      metadata.variant_group ||
      latest_outbound?.selected_variant_group ||
      latest_outbound?.variant_group
  );
  const next_expected_stage = clean(
    metadata.next_expected_stage ||
      latest_outbound?.next_expected_stage ||
      canonicalStageForUseCase(selected_use_case)
  );

  if (selected_use_case || next_expected_stage) {
    return {
      selected_use_case: selected_use_case || null,
      next_expected_stage: next_expected_stage || null,
      selected_tone: clean(metadata.selected_tone || latest_outbound?.selected_tone) || null,
    };
  }

  const last_outbound_message = clean(context?.summary?.last_outbound_message);
  const inferred_use_case = inferCanonicalUseCaseFromOutboundText(last_outbound_message);

  if (inferred_use_case) {
    return {
      selected_use_case: inferred_use_case,
      next_expected_stage: canonicalStageForUseCase(inferred_use_case),
      selected_tone: null,
    };
  }

  const conversation_stage = clean(context?.summary?.conversation_stage);
  if (conversation_stage === "Ownership") {
    return {
      selected_use_case: "ownership_check",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
      selected_tone: "Warm",
    };
  }

  if (conversation_stage === "Offer") {
    return {
      selected_use_case: "consider_selling",
      next_expected_stage: SELLER_FLOW_STAGES.CONSIDER_SELLING,
      selected_tone: "Warm",
    };
  }

  if (conversation_stage === "Follow-Up") {
    return {
      selected_use_case: "reengagement",
      next_expected_stage: SELLER_FLOW_STAGES.REENGAGEMENT,
      selected_tone: "Warm",
    };
  }

  return {
    selected_use_case: "ownership_check",
    next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    selected_tone: "Warm",
  };
}

function detectIntent({
  message = "",
  classification = null,
  previous_stage = null,
  signals = {},
}) {
  const post_offer_negotiation = isPostOfferNegotiationStage(previous_stage);

  if (detectOptOut(message, classification)) return "Opt Out";
  if (detectWrongPerson(message, classification)) return "Ownership Denied / Wrong Person";

  const identity_route = detectIdentityRoute(message, classification);
  if (identity_route === "how_got_number") return "Identity Challenge / Number Source";
  if (identity_route === "who_is_this") return "Identity Challenge";

  if (detectNotInterested(message, classification)) return "Not Interested";
  if (hasCounterSignal(message, classification)) return "Counter / Negotiation";
  if (Number.isFinite(signals.asking_price)) return "Asking Price Provided";
  if (hasReverseOfferRequest(message, classification)) {
    return "No Asking Price / Reverse Offer Request";
  }
  if (
    post_offer_negotiation &&
    (
      hasHandoffTrigger(message, classification) ||
      hasEmotionalResistance(message, classification) ||
      hasTimelineHesitation(message, classification) ||
      hasHardCounterSignal({
        message,
        counter_amount: signals.asking_price,
      }) ||
      hasSoftCounterSignal(message)
    )
  ) {
    return "Counter / Negotiation";
  }
  if (hasPropertyInfo(signals)) return "Property Info Provided";
  if (detectOpenToSelling(message, previous_stage)) return "Open to Selling";
  if (detectOwnershipConfirmed(message, previous_stage)) return "Ownership Confirmed";

  return "Unknown";
}

function formatOfferDisplay({
  maybe_offer = null,
  existing_offer = null,
  context = null,
} = {}) {
  const maybe_offer_amount =
    maybe_offer?.offer?.offer_amount ??
    maybe_offer?.offer_amount ??
    null;
  const existing_offer_amount =
    getNumberValue(existing_offer, "offer-sent-price-2", null) ??
    getNumberValue(existing_offer, "seller-counter-offer-3", null) ??
    null;
  const property_offer_amount =
    getNumberValue(context?.items?.property_item || null, "smart-cash-offer-2", null) ??
    null;

  const resolved = maybe_offer_amount ?? existing_offer_amount ?? property_offer_amount;
  if (!Number.isFinite(Number(resolved))) return null;
  return formatUsd(resolved);
}

function resolveOfferAmount({
  maybe_offer = null,
  existing_offer = null,
  context = null,
} = {}) {
  const maybe_offer_amount =
    maybe_offer?.offer?.offer_amount ??
    maybe_offer?.offer_amount ??
    null;
  const existing_offer_amount =
    getNumberValue(existing_offer, "offer-sent-price-2", null) ??
    getNumberValue(existing_offer, "seller-counter-offer-3", null) ??
    null;
  const property_offer_amount =
    getNumberValue(context?.items?.property_item || null, "smart-cash-offer-2", null) ??
    null;

  const resolved = maybe_offer_amount ?? existing_offer_amount ?? property_offer_amount;
  return Number.isFinite(Number(resolved)) ? Number(resolved) : null;
}

function isPostOfferNegotiationStage(stage = null) {
  return [
    SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION,
    SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO,
    SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION,
    SELLER_FLOW_STAGES.MF_OFFER_REVEAL,
    SELLER_FLOW_STAGES.JUSTIFY_PRICE,
    SELLER_FLOW_STAGES.ASK_TIMELINE,
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
    SELLER_FLOW_STAGES.NARROW_RANGE,
    SELLER_FLOW_STAGES.CLOSE_HANDOFF,
  ].includes(clean(stage));
}

function determineCreativeRevealUseCase(signals = {}) {
  const creative_strategy = lower(signals.creative_strategy);

  if (signals.novation_interest === true || creative_strategy.includes("novation")) {
    return SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION;
  }

  if (creative_strategy.includes("lease option") || creative_strategy.includes("lease purchase")) {
    return SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION;
  }

  if (
    signals.creative_terms_interest === true ||
    creative_strategy.includes("subject") ||
    creative_strategy.includes("seller finance") ||
    creative_strategy.includes("creative")
  ) {
    return SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO;
  }

  return SELLER_FLOW_STAGES.OFFER_REVEAL_CASH;
}

function determineOfferRevealUseCase({ signals = {} } = {}) {
  return determineCreativeRevealUseCase(signals);
}

function isCreativeEligible({ signals = {}, asking_price = null, max_cash_offer = null } = {}) {
  if (!Number.isFinite(Number(asking_price)) || !Number.isFinite(Number(max_cash_offer))) {
    return false;
  }

  if (Number(asking_price) <= Number(max_cash_offer)) return false;

  return Boolean(
    signals.creative_terms_interest === true ||
      signals.novation_interest === true ||
      clean(signals.creative_strategy)
  );
}

function variantGroupForUseCase(use_case = null) {
  switch (normalizeSellerFlowUseCase(use_case)) {
    case SELLER_FLOW_STAGES.CONSIDER_SELLING:
      return "Stage 2 Consider Selling";
    case SELLER_FLOW_STAGES.ASKING_PRICE:
      return "Stage 3 Asking Price";
    case SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS:
      return "Stage 4A Confirm Basics";
    case SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE:
      return "Stage 4B Condition Probe";
    case SELLER_FLOW_STAGES.CREATIVE_PROBE:
      return "Stage 4C Creative Probe";
    case SELLER_FLOW_STAGES.OFFER_REVEAL_CASH:
      return "Stage 5A Cash Offer Reveal";
    case SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION:
      return "Stage 5B Lease Option Reveal";
    case SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO:
      return "Stage 5C Subject-To Reveal";
    case SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION:
      return "Stage 5D Novation Reveal";
    case SELLER_FLOW_STAGES.MF_CONFIRM_UNITS:
      return "MF1 Confirm Units";
    case SELLER_FLOW_STAGES.MF_OCCUPANCY:
      return "MF2 Occupancy";
    case SELLER_FLOW_STAGES.MF_RENTS:
      return "MF3 Rents";
    case SELLER_FLOW_STAGES.MF_EXPENSES:
      return "MF4 Expenses";
    case SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK:
      return "MF5 Underwriting Ack";
    case SELLER_FLOW_STAGES.MF_OFFER_REVEAL:
      return "MF6 Offer Reveal";
    case SELLER_FLOW_STAGES.JUSTIFY_PRICE:
      return "Stage 6A Justify Price";
    case SELLER_FLOW_STAGES.ASK_TIMELINE:
      return "Stage 6B Ask Timeline";
    case SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER:
      return "Stage 6C Ask Condition Clarifier";
    case SELLER_FLOW_STAGES.NARROW_RANGE:
      return "Stage 6D Narrow Range";
    case SELLER_FLOW_STAGES.CLOSE_HANDOFF:
      return "Stage 6E Close Handoff";
    default:
      return null;
  }
}

function buildPlan({
  detected_language,
  current_stage,
  detected_intent,
  selected_use_case,
  template_use_case = null,
  template_lookup_use_case = undefined,
  selected_variant_group,
  selected_tone,
  next_expected_stage,
  reasoning_summary,
  should_queue_reply = true,
  handled = true,
  response_tier = "neutral",
  offer_price_display = null,
} = {}) {
  const resolved_template_lookup_use_case =
    template_lookup_use_case !== undefined
      ? template_lookup_use_case
      : template_use_case ?? selected_use_case ?? null;

  return {
    detected_language,
    current_stage,
    detected_intent,
    selected_use_case,
    template_use_case,
    template_lookup_use_case: resolved_template_lookup_use_case,
    selected_variant_group,
    selected_tone,
    next_expected_stage,
    reasoning_summary,
    should_queue_reply,
    handled,
    response_tier,
    offer_price_display,
    paired_with_agent_type: preferredAgentTypeForSellerFlow({
      tone: selected_tone,
      template_use_case,
    }),
  };
}

function buildNegotiationPlan({
  detected_language,
  current_stage,
  detected_intent,
  classification = null,
  previous_tone = null,
  message = "",
  signals = {},
  offer_price_display = null,
} = {}) {
  const counter_amount =
    signals.asking_price !== null &&
    signals.asking_price !== undefined &&
    signals.asking_price !== "" &&
    Number.isFinite(Number(signals.asking_price))
    ? Number(signals.asking_price)
    : null;
  const hard_counter = hasHardCounterSignal({
    message,
    counter_amount,
  });
  const soft_counter = hasSoftCounterSignal(message);
  const emotional_resistance = hasEmotionalResistance(message, classification);
  const timeline_hesitation = hasTimelineHesitation(message, classification);
  const has_enough_property_facts = hasEnoughPropertyFactsForNegotiation(signals);
  const timeline_present = Boolean(signals.timeline);

  if (hasHandoffTrigger(message, classification)) {
    return buildPlan({
      detected_language,
      current_stage,
      detected_intent,
      selected_use_case: SELLER_FLOW_STAGES.CLOSE_HANDOFF,
      template_use_case: SELLER_FLOW_STAGES.CLOSE_HANDOFF,
      selected_variant_group: variantGroupForUseCase(SELLER_FLOW_STAGES.CLOSE_HANDOFF),
      selected_tone: "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.CLOSE_HANDOFF,
      reasoning_summary: "Seller is signaling readiness for the next step, so the flow should move toward handoff instead of re-trading price.",
      response_tier: "hot",
      offer_price_display,
    });
  }

  if (
    !has_enough_property_facts &&
    !timeline_present &&
    (hard_counter || soft_counter || emotional_resistance || timeline_hesitation)
  ) {
    return buildPlan({
      detected_language,
      current_stage,
      detected_intent,
      selected_use_case: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
      template_use_case: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
      selected_variant_group: variantGroupForUseCase(
        SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER
      ),
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
      }),
      next_expected_stage: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
      reasoning_summary: "Seller pushed back on price before enough property facts were confirmed, so the next move is to clarify condition and occupancy.",
      response_tier: "hot",
      offer_price_display,
    });
  }

  if (hard_counter) {
    return buildPlan({
      detected_language,
      current_stage,
      detected_intent,
      selected_use_case: SELLER_FLOW_STAGES.NARROW_RANGE,
      template_use_case: SELLER_FLOW_STAGES.NARROW_RANGE,
      selected_variant_group: variantGroupForUseCase(SELLER_FLOW_STAGES.NARROW_RANGE),
      selected_tone: "Direct",
      next_expected_stage: SELLER_FLOW_STAGES.NARROW_RANGE,
      reasoning_summary: "Seller gave a firm counter, so the next move is to narrow the gap instead of repeating the same anchor.",
      response_tier: "hot",
      offer_price_display,
    });
  }

  if ((timeline_hesitation || emotional_resistance) && !soft_counter) {
    return buildPlan({
      detected_language,
      current_stage,
      detected_intent,
      selected_use_case: SELLER_FLOW_STAGES.ASK_TIMELINE,
      template_use_case: SELLER_FLOW_STAGES.ASK_TIMELINE,
      selected_variant_group: variantGroupForUseCase(SELLER_FLOW_STAGES.ASK_TIMELINE),
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case: SELLER_FLOW_STAGES.ASK_TIMELINE,
      }),
      next_expected_stage: SELLER_FLOW_STAGES.ASK_TIMELINE,
      reasoning_summary: "Seller is resisting the number emotionally without giving a clear counter, so the next move is to qualify timeline and urgency.",
      response_tier: "neutral",
      offer_price_display,
    });
  }

  return buildPlan({
    detected_language,
    current_stage,
    detected_intent,
    selected_use_case: SELLER_FLOW_STAGES.JUSTIFY_PRICE,
    template_use_case: SELLER_FLOW_STAGES.JUSTIFY_PRICE,
    selected_variant_group: variantGroupForUseCase(SELLER_FLOW_STAGES.JUSTIFY_PRICE),
    selected_tone: chooseConversationalTone({
      classification,
      previous_tone,
      selected_use_case: SELLER_FLOW_STAGES.JUSTIFY_PRICE,
    }),
    next_expected_stage: SELLER_FLOW_STAGES.JUSTIFY_PRICE,
    reasoning_summary: "Seller is negotiating around price, so the next move is to justify the range and keep the conversation moving.",
    response_tier: "hot",
    offer_price_display,
  });
}

export function routeSellerConversation({
  context = null,
  classification = null,
  message = "",
  previous_outbound_use_case = null,
  maybe_offer = null,
  existing_offer = null,
} = {}) {
  const detected_language =
    clean(classification?.language) ||
    clean(context?.summary?.language_preference) ||
    "English";

  const previous = derivePreviousOutboundPlan({
    context,
    previous_outbound_use_case,
  });

  const previous_stage =
    clean(previous?.next_expected_stage) || SELLER_FLOW_STAGES.OWNERSHIP_CHECK;
  const previous_tone = clean(previous?.selected_tone) || null;

  const extracted = extractUnderwritingSignals({
    message,
    classification,
    context,
  });
  const signals = extracted?.signals || {};
  const asking_price = Number.isFinite(signals.asking_price) ? signals.asking_price : null;
  const multifamily_like = isMultifamilyLike({
    context,
    signals,
  });
  const max_cash_offer = getNumberValue(
    context?.items?.property_item || null,
    "smart-cash-offer-2",
    null
  );
  const offer_price_display = formatOfferDisplay({
    maybe_offer,
    existing_offer,
    context,
  });
  const offer_amount = resolveOfferAmount({
    maybe_offer,
    existing_offer,
    context,
  });

  const detected_intent = detectIntent({
    message,
    classification,
    previous_stage,
    signals,
  });

  const shouldDeferToUnderwriting =
    multifamily_like &&
    ["Asking Price Provided", "No Asking Price / Reverse Offer Request", "Property Info Provided"].includes(
      detected_intent
    );

  if (shouldDeferToUnderwriting) {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: null,
      template_use_case: null,
      selected_variant_group: null,
      selected_tone: null,
      next_expected_stage: previous_stage,
      reasoning_summary:
        "Multifamily and apartment leads stay in underwriting until unit, occupancy, rent, and expense context is collected.",
      should_queue_reply: false,
      handled: false,
      response_tier: "neutral",
    });
  }

  if (detected_intent === "Opt Out") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "stop_or_opt_out",
      template_use_case: null,
      selected_variant_group: "Objection — Stop / Opt Out",
      selected_tone: "Calm",
      next_expected_stage: SELLER_FLOW_STAGES.TERMINAL,
      reasoning_summary: "Seller opted out, so the promotional flow stops immediately.",
      should_queue_reply: false,
      handled: true,
      response_tier: "cold",
    });
  }

  if (detected_intent === "Ownership Denied / Wrong Person") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "wrong_person",
      template_use_case: "wrong_person",
      selected_variant_group: "Stage 1 — Ownership Check",
      selected_tone: "Neutral",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
      reasoning_summary: "Seller denied ownership, so the flow closes out politely instead of advancing.",
      response_tier: "cold",
    });
  }

  if (detected_intent === "Identity Challenge / Number Source") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "how_got_number",
      template_use_case: "how_got_number",
      selected_variant_group: "Stage 1 — Identity / Trust",
      selected_tone: "Calm",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
      reasoning_summary: "Seller asked how the number was sourced, so the reply should explain contact sourcing before re-selling the conversation.",
      response_tier: "neutral",
    });
  }

  if (detected_intent === "Identity Challenge") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "who_is_this",
      template_use_case: "who_is_this",
      selected_variant_group: "Stage 1 — Identity / Trust",
      selected_tone: "Neutral",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
      reasoning_summary: "Seller challenged identity, so the next text should establish who we are before moving forward.",
      response_tier: "neutral",
    });
  }

  if (detected_intent === "Not Interested") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "not_interested",
      template_use_case: "not_interested",
      selected_variant_group: "Soft Close",
      selected_tone: "Calm",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
      reasoning_summary: "Seller declined, so the flow uses a short polite close instead of continuing the pitch.",
      response_tier: "cold",
    });
  }

  if (detected_intent === "Counter / Negotiation") {
    if (isPostOfferNegotiationStage(previous_stage)) {
      return buildNegotiationPlan({
        detected_language,
        current_stage: previous_stage,
        detected_intent,
        classification,
        previous_tone,
        message,
        signals,
        offer_price_display,
        offer_amount,
      });
    }

    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
        ? SELLER_FLOW_STAGES.CLOSE_HANDOFF
        : SELLER_FLOW_STAGES.JUSTIFY_PRICE,
      template_use_case: previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
        ? SELLER_FLOW_STAGES.CLOSE_HANDOFF
        : SELLER_FLOW_STAGES.JUSTIFY_PRICE,
      selected_variant_group: variantGroupForUseCase(
        previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
          ? SELLER_FLOW_STAGES.CLOSE_HANDOFF
          : SELLER_FLOW_STAGES.JUSTIFY_PRICE
      ),
      selected_tone:
        previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
          ? "Warm"
          : chooseConversationalTone({
              classification,
              previous_tone,
              selected_use_case: SELLER_FLOW_STAGES.JUSTIFY_PRICE,
            }),
      next_expected_stage: previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
        ? SELLER_FLOW_STAGES.CLOSE_HANDOFF
        : SELLER_FLOW_STAGES.JUSTIFY_PRICE,
      reasoning_summary: "Seller is negotiating against a prior number, so the next move stays in negotiation rather than restarting discovery.",
      response_tier: "hot",
    });
  }

  if (detected_intent === "Asking Price Provided") {
    if (isPostOfferNegotiationStage(previous_stage)) {
      return buildNegotiationPlan({
        detected_language,
        current_stage: previous_stage,
        detected_intent,
        classification,
        previous_tone,
        message,
        signals,
        offer_price_display,
        offer_amount,
      });
    }

    const selected_use_case =
      Number.isFinite(max_cash_offer) &&
      Number.isFinite(asking_price) &&
      asking_price <= max_cash_offer
        ? SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
        : isCreativeEligible({
            signals,
            asking_price,
            max_cash_offer,
          })
          ? SELLER_FLOW_STAGES.CREATIVE_PROBE
          : SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE;

    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case,
      template_use_case: null,
      template_lookup_use_case: null,
      selected_variant_group: variantGroupForUseCase(selected_use_case),
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case,
      }),
      next_expected_stage: canonicalStageForUseCase(selected_use_case),
      reasoning_summary:
        selected_use_case === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
          ? "Seller gave a price that fits the current buy box, so the next text confirms basics instead of countering."
          : selected_use_case === SELLER_FLOW_STAGES.CREATIVE_PROBE
            ? "Seller is above the cash buy box but showed creative-fit signals, so the next move is to test lease-option, subject-to, or novation openness."
          : "Seller gave a price above the current buy box or no internal ceiling was available, so the next text gathers condition and occupancy before countering.",
      response_tier: "hot",
    });
  }

  if (detected_intent === "No Asking Price / Reverse Offer Request") {
    const reveal_use_case = determineOfferRevealUseCase({ signals });

    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: reveal_use_case,
      template_use_case: reveal_use_case,
      selected_variant_group: variantGroupForUseCase(reveal_use_case),
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case: reveal_use_case,
      }),
      next_expected_stage: canonicalStageForUseCase(reveal_use_case),
      reasoning_summary: "Seller asked us for the number, so the flow can reveal a rough as-is offer without forcing them to set price first.",
      response_tier: "hot",
      offer_price_display,
    });
  }

  if (detected_intent === "Property Info Provided") {
    if (
      [
        SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
        SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
        SELLER_FLOW_STAGES.CREATIVE_PROBE,
      ].includes(previous_stage)
    ) {
      const reveal_use_case =
        previous_stage === SELLER_FLOW_STAGES.CREATIVE_PROBE
          ? determineOfferRevealUseCase({ signals })
          : SELLER_FLOW_STAGES.OFFER_REVEAL_CASH;

      return buildPlan({
        detected_language,
        current_stage: previous_stage,
        detected_intent,
        selected_use_case: reveal_use_case,
        template_use_case: reveal_use_case,
        selected_variant_group: variantGroupForUseCase(reveal_use_case),
        selected_tone: chooseConversationalTone({
          classification,
          previous_tone,
          selected_use_case: reveal_use_case,
        }),
        next_expected_stage: canonicalStageForUseCase(reveal_use_case),
        reasoning_summary:
          previous_stage === SELLER_FLOW_STAGES.CREATIVE_PROBE
            ? "Seller engaged on the creative branch, so the next move is to reveal the matching creative structure."
            : "Seller answered the property-fact questions, so the flow now has enough context to reveal the number.",
        response_tier: "hot",
        offer_price_display,
      });
    }
  }

  if (detected_intent === "Open to Selling") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "asking_price",
      template_use_case: null,
      template_lookup_use_case: null,
      selected_variant_group: variantGroupForUseCase(SELLER_FLOW_STAGES.ASKING_PRICE),
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case: "asking_price",
      }),
      next_expected_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
      reasoning_summary: "Seller is open to selling, so the next text asks what number they have in mind instead of repeating the selling question.",
      response_tier: "neutral",
    });
  }

  if (detected_intent === "Ownership Confirmed") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "consider_selling",
      template_use_case: null,
      template_lookup_use_case: null,
      selected_variant_group: variantGroupForUseCase(SELLER_FLOW_STAGES.CONSIDER_SELLING),
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case: "consider_selling",
      }),
      next_expected_stage: SELLER_FLOW_STAGES.CONSIDER_SELLING,
      reasoning_summary: "Seller confirmed ownership, so the next text checks openness to selling before asking price.",
      response_tier: "neutral",
    });
  }

  return buildPlan({
    detected_language,
    current_stage: previous_stage,
    detected_intent,
    selected_use_case: null,
    template_use_case: null,
    selected_variant_group: null,
    selected_tone: null,
    next_expected_stage: previous_stage,
    reasoning_summary: "The inbound message did not cleanly map to the seller flow, so no automatic seller reply was queued.",
    should_queue_reply: false,
    handled: false,
    response_tier: "neutral",
  });
}

export default routeSellerConversation;
