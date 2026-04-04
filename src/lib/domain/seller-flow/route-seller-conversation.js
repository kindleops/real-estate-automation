import { getNumberValue } from "@/lib/providers/podio.js";
import { formatUsd } from "@/lib/utils/money.js";
import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import {
  SELLER_FLOW_STAGES,
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
    "can you do better",
    "need more",
    "come up",
    "meet me at",
    "my floor is",
    "lowest i can do",
    "best you can do",
  ]);
}

function hasPropertyInfo(signals = {}) {
  return Boolean(
    signals.occupancy_status ||
      signals.condition_level ||
      signals.timeline ||
      signals.unit_count ||
      signals.rents_present ||
      signals.expenses_present
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
  if (clean(previous_outbound_use_case)) {
    return {
      selected_use_case: clean(previous_outbound_use_case),
      next_expected_stage: canonicalStageForUseCase(previous_outbound_use_case),
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
  const selected_use_case = clean(
    metadata.selected_use_case ||
      latest_outbound?.selected_use_case ||
      metadata.canonical_use_case
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
      next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
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

  const detected_intent = detectIntent({
    message,
    classification,
    previous_stage,
    signals,
  });

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
      next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
      reasoning_summary: "Seller declined, so the flow uses a short polite close instead of continuing the pitch.",
      response_tier: "cold",
    });
  }

  if (detected_intent === "Counter / Negotiation") {
    const template_use_case =
      previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS
        ? "close_ask_soft"
        : clean(classification?.objection) === "need_more_money"
          ? "can_you_do_better"
          : "price_too_low";

    const selected_variant_group =
      template_use_case === "close_ask_soft"
        ? "Stage 6 — Close / Handoff"
        : template_use_case === "can_you_do_better"
          ? "Negotiation — Improve Offer"
          : "Objection — Price Too Low";

    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "negotiation_follow_up",
      template_use_case,
      selected_variant_group,
      selected_tone:
        template_use_case === "close_ask_soft"
          ? "Warm"
          : chooseConversationalTone({
              classification,
              previous_tone,
              selected_use_case: "negotiation_follow_up",
            }),
      next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
      reasoning_summary: "Seller is negotiating against a prior number, so the next move stays in negotiation rather than restarting discovery.",
      response_tier: "hot",
    });
  }

  if (detected_intent === "Asking Price Provided") {
    const selected_use_case =
      Number.isFinite(max_cash_offer) && Number.isFinite(asking_price) && asking_price <= max_cash_offer
        ? "price_works_confirm_basics"
        : "price_high_condition_probe";

    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case,
      template_use_case: null,
      template_lookup_use_case: null,
      selected_variant_group:
        selected_use_case === "price_works_confirm_basics"
          ? "Stage 4A Confirm Basics"
          : "Stage 4B Condition Probe",
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case,
      }),
      next_expected_stage: canonicalStageForUseCase(selected_use_case),
      reasoning_summary:
        selected_use_case === "price_works_confirm_basics"
          ? "Seller gave a price that fits the current buy box, so the next text confirms basics instead of countering."
          : "Seller gave a price above the current buy box or no internal ceiling was available, so the next text gathers condition and occupancy before countering.",
      response_tier: "hot",
    });
  }

  if (detected_intent === "No Asking Price / Reverse Offer Request") {
    return buildPlan({
      detected_language,
      current_stage: previous_stage,
      detected_intent,
      selected_use_case: "offer_reveal",
      template_use_case: "offer_reveal",
      selected_variant_group: "Stage 5 Offer Reveal",
      selected_tone: chooseConversationalTone({
        classification,
        previous_tone,
        selected_use_case: "offer_reveal",
      }),
      next_expected_stage: SELLER_FLOW_STAGES.OFFER_REVEAL,
      reasoning_summary: "Seller asked us for the number, so the flow can reveal a rough as-is offer without forcing them to set price first.",
      response_tier: "hot",
      offer_price_display,
    });
  }

  if (detected_intent === "Property Info Provided") {
    if (previous_stage === SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE) {
      return buildPlan({
        detected_language,
        current_stage: previous_stage,
        detected_intent,
        selected_use_case: "offer_reveal",
        template_use_case: "offer_reveal",
        selected_variant_group: "Stage 5 Offer Reveal",
        selected_tone: chooseConversationalTone({
          classification,
          previous_tone,
          selected_use_case: "offer_reveal",
        }),
        next_expected_stage: SELLER_FLOW_STAGES.OFFER_REVEAL,
        reasoning_summary: "Seller answered the condition probe, so the flow has enough context to reveal the number.",
        response_tier: "hot",
        offer_price_display,
      });
    }

    if (previous_stage === SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS) {
      return buildPlan({
        detected_language,
        current_stage: previous_stage,
        detected_intent,
        selected_use_case: "negotiation_follow_up",
        template_use_case: "close_ask_soft",
        selected_variant_group: "Stage 6 — Close / Handoff",
        selected_tone: "Warm",
        next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
        reasoning_summary: "Seller confirmed the basics after an acceptable ask, so the flow moves toward the next step instead of re-asking discovery questions.",
        response_tier: "hot",
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
      selected_variant_group: "Stage 3 Asking Price",
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
      selected_variant_group: "Stage 2 Consider Selling",
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
