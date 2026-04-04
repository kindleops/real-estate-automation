import { STAGES } from "@/lib/config/stages.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export const SELLER_FLOW_STAGES = Object.freeze({
  OWNERSHIP_CHECK: "Stage 1 — Ownership Check",
  CONSIDER_SELLING: "Stage 2 — Consider Selling",
  ASKING_PRICE: "Stage 3 — Asking Price",
  PRICE_WORKS_CONFIRM_BASICS: "Stage 4A — Price Works / Confirm Basics",
  PRICE_HIGH_CONDITION_PROBE: "Stage 4B — Price High / Condition Probe",
  OFFER_REVEAL: "Stage 5 — Offer Reveal",
  NEGOTIATION: "Stage 6 — Negotiation / Follow-Up / Close",
  TERMINAL: "Terminal",
});

export function canonicalStageForUseCase(use_case = null) {
  switch (clean(use_case)) {
    case "ownership_check":
    case "wrong_person":
    case "who_is_this":
    case "how_got_number":
      return SELLER_FLOW_STAGES.OWNERSHIP_CHECK;
    case "consider_selling":
      return SELLER_FLOW_STAGES.CONSIDER_SELLING;
    case "asking_price":
      return SELLER_FLOW_STAGES.ASKING_PRICE;
    case "price_works_confirm_basics":
      return SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS;
    case "price_high_condition_probe":
      return SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE;
    case "offer_reveal":
      return SELLER_FLOW_STAGES.OFFER_REVEAL;
    case "follow_up":
    case "negotiation_follow_up":
    case "reengagement":
    case "not_interested":
      return SELLER_FLOW_STAGES.NEGOTIATION;
    case "stop_or_opt_out":
      return SELLER_FLOW_STAGES.TERMINAL;
    default:
      return null;
  }
}

export function brainStageForUseCase(use_case = null) {
  switch (clean(use_case)) {
    case "ownership_check":
    case "wrong_person":
    case "who_is_this":
    case "how_got_number":
    case "not_interested":
    case "stop_or_opt_out":
      return STAGES.OWNERSHIP;
    case "consider_selling":
    case "asking_price":
    case "price_works_confirm_basics":
    case "price_high_condition_probe":
    case "offer_reveal":
      return STAGES.OFFER;
    case "follow_up":
    case "negotiation_follow_up":
    case "reengagement":
      return STAGES.FOLLOW_UP;
    default:
      return STAGES.OWNERSHIP;
  }
}

export function normalizeSellerFlowTone(value = null) {
  const raw = clean(value);
  const allowed = new Set(["Warm", "Human", "Direct", "Empathetic", "Neutral", "Calm"]);
  return allowed.has(raw) ? raw : null;
}

export function preferredAgentTypeForSellerFlow({
  tone = null,
  template_use_case = null,
} = {}) {
  switch (clean(template_use_case)) {
    case "who_is_this":
    case "how_got_number":
    case "wrong_person":
    case "not_interested":
      return "Fallback / Market-Local";
    case "can_you_do_better":
      return "Fallback / Market-Local / Specialist-Close";
    case "price_too_low":
    case "justify_price":
    case "ask_timeline":
    case "ask_condition_clarifier":
    case "narrow_range":
      return "Fallback / Market-Local";
    case "close_ask_soft":
    case "close_handoff":
      return "Soft Closer / Hard Closer / Ultra-Short";
    default:
      break;
  }

  switch (normalizeSellerFlowTone(tone)) {
    case "Human":
      return "Casual Human";
    case "Direct":
      return "Straight Shooter";
    case "Empathetic":
      return "Empathetic";
    case "Neutral":
    case "Calm":
      return "Fallback / Market-Local";
    case "Warm":
    default:
      return "Warm Professional";
  }
}

export function inferCanonicalUseCaseFromOutboundText(message = "") {
  const text = lower(message);

  if (!text) return null;

  if (text.includes("who handles") || text.includes("wrong person")) {
    return "wrong_person";
  }

  if (text.includes("public property") || text.includes("public records")) {
    return "how_got_number";
  }

  if (text.includes("this is") && text.includes("buying")) {
    return "who_is_this";
  }

  if (
    text.includes("owner of") ||
    text.includes("your property") ||
    text.includes("right person for")
  ) {
    return "ownership_check";
  }

  if (
    text.includes("open to selling") ||
    text.includes("consider selling") ||
    text.includes("open to an offer")
  ) {
    return "consider_selling";
  }

  if (
    text.includes("what number") ||
    text.includes("what would you want") ||
    text.includes("what price") ||
    text.includes("how much would you want") ||
    text.includes("price in mind")
  ) {
    return "asking_price";
  }

  if (
    text.includes("before i respond to that price") ||
    text.includes("before i talk price") ||
    text.includes("needs repairs") ||
    text.includes("vacant or occupied")
  ) {
    return "price_high_condition_probe";
  }

  if (
    text.includes("that price we might have room") ||
    text.includes("that could work") ||
    text.includes("might be in range")
  ) {
    return "price_works_confirm_basics";
  }

  if (
    text.includes("rough cash number") ||
    text.includes("likely be around") ||
    text.includes("my rough number") ||
    text.includes("$")
  ) {
    return "offer_reveal";
  }

  if (
    text.includes("next step") ||
    text.includes("paperwork") ||
    text.includes("move ahead") ||
    text.includes("move forward")
  ) {
    return "negotiation_follow_up";
  }

  return null;
}

export function deriveCanonicalSellerFlowFromTemplate(template = null) {
  const use_case = clean(template?.use_case);
  const variant_group = clean(template?.variant_group);
  const selected_tone = normalizeSellerFlowTone(template?.tone);

  const follow_up_variant_map = {
    "Stage 1 — Ownership Confirmation Follow-Up": {
      selected_use_case: "ownership_check",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    },
    "Stage 2 — Consider Selling Follow-Up": {
      selected_use_case: "consider_selling",
      next_expected_stage: SELLER_FLOW_STAGES.CONSIDER_SELLING,
    },
    "Stage 3 — Asking Price Follow-Up": {
      selected_use_case: "asking_price",
      next_expected_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
    },
    "Stage 4A — Confirm Basics Follow-Up": {
      selected_use_case: "price_works_confirm_basics",
      next_expected_stage: SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
    },
    "Stage 4B — Condition Probe Follow-Up": {
      selected_use_case: "price_high_condition_probe",
      next_expected_stage: SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
    },
    "Stage 5 — Offer Reveal Follow-Up": {
      selected_use_case: "offer_reveal",
      next_expected_stage: SELLER_FLOW_STAGES.OFFER_REVEAL,
    },
  };

  if (follow_up_variant_map[variant_group] && (use_case === "follow_up" || !use_case)) {
    return {
      selected_use_case: follow_up_variant_map[variant_group].selected_use_case,
      template_use_case: use_case || "follow_up",
      selected_variant_group: variant_group,
      selected_tone: selected_tone || "Warm",
      next_expected_stage: follow_up_variant_map[variant_group].next_expected_stage,
    };
  }

  if (
    use_case === "ownership_check" ||
    variant_group === "Stage 1 — Ownership Confirmation" ||
    variant_group === "Stage 1 Ownership Check" ||
    variant_group === "Stage 1 — Ownership Check"
  ) {
    return {
      selected_use_case: "ownership_check",
      template_use_case: use_case || "ownership_check",
      selected_variant_group: "Stage 1 — Ownership Confirmation",
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    };
  }

  if (variant_group === "Stage 2 Consider Selling") {
    return {
      selected_use_case: "consider_selling",
      template_use_case: null,
      selected_variant_group: variant_group,
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.CONSIDER_SELLING,
    };
  }

  if (variant_group === "Stage 3 Asking Price") {
    return {
      selected_use_case: "asking_price",
      template_use_case: null,
      selected_variant_group: variant_group,
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
    };
  }

  if (variant_group === "Stage 4A Confirm Basics") {
    return {
      selected_use_case: "price_works_confirm_basics",
      template_use_case: null,
      selected_variant_group: variant_group,
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
    };
  }

  if (variant_group === "Stage 4B Condition Probe") {
    return {
      selected_use_case: "price_high_condition_probe",
      template_use_case: null,
      selected_variant_group: variant_group,
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
    };
  }

  if (use_case === "offer_reveal") {
    return {
      selected_use_case: "offer_reveal",
      template_use_case: "offer_reveal",
      selected_variant_group: variant_group || "Stage 5 Offer Reveal",
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.OFFER_REVEAL,
    };
  }

  if (use_case === "wrong_person") {
    return {
      selected_use_case: "wrong_person",
      template_use_case: "wrong_person",
      selected_variant_group: variant_group || "Stage 1 — Ownership Check",
      selected_tone: selected_tone || "Neutral",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    };
  }

  if (use_case === "who_is_this") {
    return {
      selected_use_case: "who_is_this",
      template_use_case: "who_is_this",
      selected_variant_group: variant_group || "Stage 1 — Identity / Trust",
      selected_tone: selected_tone || "Neutral",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    };
  }

  if (use_case === "how_got_number") {
    return {
      selected_use_case: "how_got_number",
      template_use_case: "how_got_number",
      selected_variant_group: variant_group || "Stage 1 — Identity / Trust",
      selected_tone: selected_tone || "Calm",
      next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    };
  }

  if (use_case === "not_interested" || variant_group === "Soft Close") {
    return {
      selected_use_case: "not_interested",
      template_use_case: use_case || "not_interested",
      selected_variant_group: variant_group || "Soft Close",
      selected_tone: selected_tone || "Calm",
      next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
    };
  }

  if (
    variant_group === "Objection — Stop / Opt Out" ||
    lower(use_case).includes("stop_texting")
  ) {
    return {
      selected_use_case: "stop_or_opt_out",
      template_use_case: use_case || null,
      selected_variant_group: variant_group || "Objection — Stop / Opt Out",
      selected_tone: selected_tone || "Calm",
      next_expected_stage: SELLER_FLOW_STAGES.TERMINAL,
    };
  }

  if (
    [
      "can_you_do_better",
      "price_too_low",
      "justify_price",
      "ask_timeline",
      "ask_condition_clarifier",
      "narrow_range",
      "best_price",
      "close_ask_soft",
      "close_handoff",
    ].includes(
      use_case
    )
  ) {
    return {
      selected_use_case: "negotiation_follow_up",
      template_use_case: use_case,
      selected_variant_group:
        variant_group ||
        (use_case === "close_ask_soft" || use_case === "close_handoff"
          ? "Stage 6 — Close / Handoff"
          : "Negotiation — Improve Offer"),
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
    };
  }

  if (use_case === "reengagement") {
    return {
      selected_use_case: "reengagement",
      template_use_case: "reengagement",
      selected_variant_group: variant_group || "Stage 5 — Re-engagement",
      selected_tone: selected_tone || "Warm",
      next_expected_stage: SELLER_FLOW_STAGES.NEGOTIATION,
    };
  }

  return null;
}
