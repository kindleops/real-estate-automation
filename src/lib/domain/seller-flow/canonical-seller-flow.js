import { CONVERSATION_STAGES } from "@/lib/domain/communications-engine/state-machine.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export const SELLER_FLOW_STAGES = Object.freeze({
  OWNERSHIP_CHECK: "ownership_check",
  OWNERSHIP_CHECK_FOLLOW_UP: "ownership_check_follow_up",
  CONSIDER_SELLING: "consider_selling",
  CONSIDER_SELLING_FOLLOW_UP: "consider_selling_follow_up",
  ASKING_PRICE: "asking_price",
  ASKING_PRICE_FOLLOW_UP: "asking_price_follow_up",
  PRICE_WORKS_CONFIRM_BASICS: "price_works_confirm_basics",
  PRICE_WORKS_CONFIRM_BASICS_FOLLOW_UP: "price_works_confirm_basics_follow_up",
  PRICE_HIGH_CONDITION_PROBE: "price_high_condition_probe",
  PRICE_HIGH_CONDITION_PROBE_FOLLOW_UP: "price_high_condition_probe_follow_up",
  CREATIVE_PROBE: "creative_probe",
  CREATIVE_FOLLOW_UP: "creative_followup",
  OFFER_REVEAL_CASH: "offer_reveal_cash",
  OFFER_REVEAL_CASH_FOLLOW_UP: "offer_reveal_cash_follow_up",
  OFFER_REVEAL_LEASE_OPTION: "offer_reveal_lease_option",
  OFFER_REVEAL_SUBJECT_TO: "offer_reveal_subject_to",
  OFFER_REVEAL_NOVATION: "offer_reveal_novation",
  MF_CONFIRM_UNITS: "mf_confirm_units",
  MF_CONFIRM_UNITS_FOLLOW_UP: "mf_confirm_units_follow_up",
  MF_OCCUPANCY: "mf_occupancy",
  MF_OCCUPANCY_FOLLOW_UP: "mf_occupancy_follow_up",
  MF_RENTS: "mf_rents",
  MF_RENTS_FOLLOW_UP: "mf_rents_follow_up",
  MF_EXPENSES: "mf_expenses",
  MF_EXPENSES_FOLLOW_UP: "mf_expenses_follow_up",
  MF_UNDERWRITING_ACK: "mf_underwriting_ack",
  MF_OFFER_REVEAL: "mf_offer_reveal",
  JUSTIFY_PRICE: "justify_price",
  ASK_TIMELINE: "ask_timeline",
  ASK_CONDITION_CLARIFIER: "ask_condition_clarifier",
  NARROW_RANGE: "narrow_range",
  CLOSE_HANDOFF: "close_handoff",
  WRONG_PERSON: "wrong_person",
  WHO_IS_THIS: "who_is_this",
  HOW_GOT_NUMBER: "how_got_number",
  NOT_INTERESTED: "not_interested",
  STOP_OR_OPT_OUT: "stop_or_opt_out",
  REENGAGEMENT: "reengagement",
  TERMINAL: "terminal",
});

const FOLLOW_UP_STAGE_MAP = Object.freeze({
  [SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP]: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  [SELLER_FLOW_STAGES.CONSIDER_SELLING_FOLLOW_UP]: SELLER_FLOW_STAGES.CONSIDER_SELLING,
  [SELLER_FLOW_STAGES.ASKING_PRICE_FOLLOW_UP]: SELLER_FLOW_STAGES.ASKING_PRICE,
  [SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS_FOLLOW_UP]:
    SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
  [SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE_FOLLOW_UP]:
    SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
  [SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP]: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  [SELLER_FLOW_STAGES.MF_CONFIRM_UNITS_FOLLOW_UP]: SELLER_FLOW_STAGES.MF_CONFIRM_UNITS,
  [SELLER_FLOW_STAGES.MF_OCCUPANCY_FOLLOW_UP]: SELLER_FLOW_STAGES.MF_OCCUPANCY,
  [SELLER_FLOW_STAGES.MF_RENTS_FOLLOW_UP]: SELLER_FLOW_STAGES.MF_RENTS,
  [SELLER_FLOW_STAGES.MF_EXPENSES_FOLLOW_UP]: SELLER_FLOW_STAGES.MF_EXPENSES,
});

const LEGACY_USE_CASE_ALIASES = Object.freeze({
  // ── Stage 1 / first-touch aliases ───────────────────────────────────────────
  // The Podio Templates app uses "First Message" as the Use Case label for
  // cold outbound Stage 1 ownership-check templates in some schema revisions.
  // Map it directly so canonical resolution never depends on variant-group lookup.
  "First Message": SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  // ── Offer-reveal / closing aliases ──────────────────────────────────────────
  offer_reveal: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  close_ask_soft: SELLER_FLOW_STAGES.CLOSE_HANDOFF,
  can_you_do_better: SELLER_FLOW_STAGES.NARROW_RANGE,
  best_price: SELLER_FLOW_STAGES.NARROW_RANGE,
  price_too_low: SELLER_FLOW_STAGES.NARROW_RANGE,
  offer_no_response_followup: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  followup_soft: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  followup_hard: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  persona_warm_professional_followup: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  persona_neighborly_followup: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  persona_empathetic_followup: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  persona_investor_direct_followup: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  "persona_no-nonsense_closer_followup": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  text_me_later_specific: SELLER_FLOW_STAGES.ASK_TIMELINE,
  seller_stalling_after_yes: SELLER_FLOW_STAGES.ASK_TIMELINE,
  not_ready: SELLER_FLOW_STAGES.ASK_TIMELINE,
  condition_question_set: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  walkthrough_or_condition: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  occupied_asset: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  vacant_boarded_probe: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  has_tenants: SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  mf_units: SELLER_FLOW_STAGES.MF_CONFIRM_UNITS,
  mf_units_unknown: SELLER_FLOW_STAGES.MF_CONFIRM_UNITS,
  mf_finalize_to_offer: SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK,
});

const FOLLOW_UP_VARIANT_MAP = Object.freeze({
  "Stage 1 Follow-Up": SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP,
  "Stage 1 — Ownership Confirmation Follow-Up": SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP,
  "Stage 2 Follow-Up": SELLER_FLOW_STAGES.CONSIDER_SELLING_FOLLOW_UP,
  "Stage 2 — Consider Selling Follow-Up":
    SELLER_FLOW_STAGES.CONSIDER_SELLING_FOLLOW_UP,
  "Stage 3 Follow-Up": SELLER_FLOW_STAGES.ASKING_PRICE_FOLLOW_UP,
  "Stage 3 — Asking Price Follow-Up": SELLER_FLOW_STAGES.ASKING_PRICE_FOLLOW_UP,
  "Stage 4A — Confirm Basics Follow-Up":
    SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS_FOLLOW_UP,
  "Stage 4B — Condition Probe Follow-Up":
    SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE_FOLLOW_UP,
  "Stage 5 — Offer Reveal Follow-Up": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP,
  "Multifamily Underwrite — Units Follow-Up": SELLER_FLOW_STAGES.MF_CONFIRM_UNITS_FOLLOW_UP,
  "Multifamily Underwrite — Occupancy Follow-Up":
    SELLER_FLOW_STAGES.MF_OCCUPANCY_FOLLOW_UP,
  "Multifamily Underwrite — Rents Follow-Up": SELLER_FLOW_STAGES.MF_RENTS_FOLLOW_UP,
  "Multifamily Underwrite — Expenses Follow-Up":
    SELLER_FLOW_STAGES.MF_EXPENSES_FOLLOW_UP,
});

const VARIANT_GROUP_USE_CASE_MAP = Object.freeze({
  "Stage 1 — Ownership Check": SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  "Stage 1 Ownership Check": SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  "Stage 1 — Ownership Confirmation": SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  "Stage 2 Consider Selling": SELLER_FLOW_STAGES.CONSIDER_SELLING,
  "Stage 2 — Consider Selling": SELLER_FLOW_STAGES.CONSIDER_SELLING,
  "Stage 3 Asking Price": SELLER_FLOW_STAGES.ASKING_PRICE,
  "Stage 3 — Asking Price": SELLER_FLOW_STAGES.ASKING_PRICE,
  "Stage 4A Confirm Basics": SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
  "Stage 4A — Confirm Basics": SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
  "Stage 4B Condition Probe": SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
  "Stage 4B — Condition Probe": SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
  "Stage 4C Creative Probe": SELLER_FLOW_STAGES.CREATIVE_PROBE,
  "Stage 4C — Creative Probe": SELLER_FLOW_STAGES.CREATIVE_PROBE,
  "Stage 5A Cash Offer Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  "Stage 5A — Cash Offer Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  "Stage 5 Offer Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  "Stage 5 — Offer Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  "Stage 5B Lease Option Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION,
  "Stage 5B — Lease Option Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION,
  "Stage 5C Subject-To Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO,
  "Stage 5C — Subject-To Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO,
  "Stage 5D Novation Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION,
  "Stage 5D — Novation Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION,
  "MF1 Confirm Units": SELLER_FLOW_STAGES.MF_CONFIRM_UNITS,
  "MF2 Occupancy": SELLER_FLOW_STAGES.MF_OCCUPANCY,
  "MF3 Rents": SELLER_FLOW_STAGES.MF_RENTS,
  "MF4 Expenses": SELLER_FLOW_STAGES.MF_EXPENSES,
  "MF5 Underwriting Ack": SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK,
  "MF6 Offer Reveal": SELLER_FLOW_STAGES.MF_OFFER_REVEAL,
  "Stage 6A Justify Price": SELLER_FLOW_STAGES.JUSTIFY_PRICE,
  "Stage 6A — Justify Price": SELLER_FLOW_STAGES.JUSTIFY_PRICE,
  "Negotiation — Justify Price": SELLER_FLOW_STAGES.JUSTIFY_PRICE,
  "Stage 6B Ask Timeline": SELLER_FLOW_STAGES.ASK_TIMELINE,
  "Stage 6B — Ask Timeline": SELLER_FLOW_STAGES.ASK_TIMELINE,
  "Re-engagement / Timing": SELLER_FLOW_STAGES.ASK_TIMELINE,
  "Objection — Not Ready": SELLER_FLOW_STAGES.ASK_TIMELINE,
  "Negotiation — Stalling After Yes": SELLER_FLOW_STAGES.ASK_TIMELINE,
  "Negotiation — Timeline": SELLER_FLOW_STAGES.ASK_TIMELINE,
  "Stage 6C Ask Condition Clarifier":
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "Stage 6C — Ask Condition Clarifier":
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "Property Condition / Distress":
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "SMS-Only Underwriting": SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "Stage 6 — Condition / Walkthrough":
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "Stage 6 — Inspection / Walkthrough":
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "Negotiation — Condition Clarifier":
    SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER,
  "Stage 6D Narrow Range": SELLER_FLOW_STAGES.NARROW_RANGE,
  "Stage 6D — Narrow Range": SELLER_FLOW_STAGES.NARROW_RANGE,
  "Negotiation — Improve Offer": SELLER_FLOW_STAGES.NARROW_RANGE,
  "Negotiation — Best Price": SELLER_FLOW_STAGES.NARROW_RANGE,
  "Objection — Price Too Low": SELLER_FLOW_STAGES.NARROW_RANGE,
  "Negotiation — Narrow Range": SELLER_FLOW_STAGES.NARROW_RANGE,
  "Multifamily Underwrite — Acknowledgment":
    SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK,
  "Stage 5 Offer Reveal": SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
  "Stage 6E Close Handoff": SELLER_FLOW_STAGES.CLOSE_HANDOFF,
  "Stage 6E — Close Handoff": SELLER_FLOW_STAGES.CLOSE_HANDOFF,
  "Stage 6 — Close / Handoff": SELLER_FLOW_STAGES.CLOSE_HANDOFF,
  "Objection — Not Interested": SELLER_FLOW_STAGES.NOT_INTERESTED,
  "Objection — Stop / Opt Out": SELLER_FLOW_STAGES.STOP_OR_OPT_OUT,
  "Stage 1 — Identity / Trust": SELLER_FLOW_STAGES.WHO_IS_THIS,
  "Wrong Number / Referral": SELLER_FLOW_STAGES.WRONG_PERSON,
});

function normalizeVariantGroup(value = null) {
  const raw = clean(value);
  return raw || null;
}

export function normalizeSellerFlowTone(value = null) {
  const raw = clean(value);
  const allowed = new Set(["Warm", "Human", "Direct", "Empathetic", "Neutral", "Calm"]);
  return allowed.has(raw) ? raw : null;
}

export function normalizeSellerFlowUseCase(use_case = null, variant_group = null) {
  const raw_use_case = clean(use_case);
  const normalized_use_case = LEGACY_USE_CASE_ALIASES[raw_use_case] || raw_use_case || null;

  if (normalized_use_case && FOLLOW_UP_STAGE_MAP[normalized_use_case]) {
    return normalized_use_case;
  }

  if (normalized_use_case && SELLER_FLOW_STAGES[normalized_use_case?.toUpperCase?.()]) {
    return normalized_use_case;
  }

  if (normalized_use_case && Object.values(SELLER_FLOW_STAGES).includes(normalized_use_case)) {
    return normalized_use_case;
  }

  const normalized_variant_group = normalizeVariantGroup(variant_group);

  if (normalized_use_case === "follow_up" && normalized_variant_group) {
    return FOLLOW_UP_VARIANT_MAP[normalized_variant_group] || null;
  }

  if (!normalized_use_case && normalized_variant_group) {
    return (
      FOLLOW_UP_VARIANT_MAP[normalized_variant_group] ||
      VARIANT_GROUP_USE_CASE_MAP[normalized_variant_group] ||
      null
    );
  }

  if (normalized_variant_group && VARIANT_GROUP_USE_CASE_MAP[normalized_variant_group]) {
    return VARIANT_GROUP_USE_CASE_MAP[normalized_variant_group];
  }

  return normalized_use_case;
}

export function canonicalStageForUseCase(use_case = null, variant_group = null) {
  const normalized = normalizeSellerFlowUseCase(use_case, variant_group);

  if (!normalized) return null;
  if (normalized === SELLER_FLOW_STAGES.STOP_OR_OPT_OUT) return SELLER_FLOW_STAGES.TERMINAL;
  if (normalized === SELLER_FLOW_STAGES.WRONG_PERSON) return SELLER_FLOW_STAGES.TERMINAL;
  if (FOLLOW_UP_STAGE_MAP[normalized]) return FOLLOW_UP_STAGE_MAP[normalized];

  switch (normalized) {
    case SELLER_FLOW_STAGES.WHO_IS_THIS:
    case SELLER_FLOW_STAGES.HOW_GOT_NUMBER:
    case SELLER_FLOW_STAGES.NOT_INTERESTED:
      return SELLER_FLOW_STAGES.OWNERSHIP_CHECK;
    default:
      return normalized;
  }
}

export function baseSellerFlowStage(use_case = null, variant_group = null) {
  return canonicalStageForUseCase(use_case, variant_group);
}

export function followUpUseCaseForStage(use_case = null) {
  const stage = canonicalStageForUseCase(use_case);

  switch (stage) {
    case SELLER_FLOW_STAGES.OWNERSHIP_CHECK:
      return SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP;
    case SELLER_FLOW_STAGES.CONSIDER_SELLING:
      return SELLER_FLOW_STAGES.CONSIDER_SELLING_FOLLOW_UP;
    case SELLER_FLOW_STAGES.ASKING_PRICE:
      return SELLER_FLOW_STAGES.ASKING_PRICE_FOLLOW_UP;
    case SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS:
      return SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS_FOLLOW_UP;
    case SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE:
      return SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE_FOLLOW_UP;
    case SELLER_FLOW_STAGES.OFFER_REVEAL_CASH:
      return SELLER_FLOW_STAGES.OFFER_REVEAL_CASH_FOLLOW_UP;
    case SELLER_FLOW_STAGES.MF_CONFIRM_UNITS:
      return SELLER_FLOW_STAGES.MF_CONFIRM_UNITS_FOLLOW_UP;
    case SELLER_FLOW_STAGES.MF_OCCUPANCY:
      return SELLER_FLOW_STAGES.MF_OCCUPANCY_FOLLOW_UP;
    case SELLER_FLOW_STAGES.MF_RENTS:
      return SELLER_FLOW_STAGES.MF_RENTS_FOLLOW_UP;
    case SELLER_FLOW_STAGES.MF_EXPENSES:
      return SELLER_FLOW_STAGES.MF_EXPENSES_FOLLOW_UP;
    default:
      return null;
  }
}

export function brainStageForUseCase(use_case = null, variant_group = null) {
  const stage = canonicalStageForUseCase(use_case, variant_group);

  switch (stage) {
    case SELLER_FLOW_STAGES.OWNERSHIP_CHECK:
      return CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION;
    case SELLER_FLOW_STAGES.CONSIDER_SELLING:
      return CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION;
    case SELLER_FLOW_STAGES.ASKING_PRICE:
      return CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY;
    case SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS:
    case SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE:
    case SELLER_FLOW_STAGES.MF_CONFIRM_UNITS:
    case SELLER_FLOW_STAGES.MF_OCCUPANCY:
    case SELLER_FLOW_STAGES.MF_RENTS:
    case SELLER_FLOW_STAGES.MF_EXPENSES:
    case SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK:
      return CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY;
    case SELLER_FLOW_STAGES.CREATIVE_PROBE:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_CASH:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION:
      return CONVERSATION_STAGES.OFFER_POSITIONING;
    case SELLER_FLOW_STAGES.JUSTIFY_PRICE:
    case SELLER_FLOW_STAGES.ASK_TIMELINE:
    case SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER:
    case SELLER_FLOW_STAGES.NARROW_RANGE:
      return CONVERSATION_STAGES.NEGOTIATION;
    case SELLER_FLOW_STAGES.CLOSE_HANDOFF:
      return CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK;
    case SELLER_FLOW_STAGES.REENGAGEMENT:
      return CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION;
    case SELLER_FLOW_STAGES.TERMINAL:
      return CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME;
    default:
      return CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION;
  }
}

export function preferredAgentTypeForSellerFlow({
  tone = null,
  template_use_case = null,
} = {}) {
  const normalized_use_case = normalizeSellerFlowUseCase(template_use_case);

  switch (normalized_use_case) {
    case SELLER_FLOW_STAGES.WHO_IS_THIS:
    case SELLER_FLOW_STAGES.HOW_GOT_NUMBER:
    case SELLER_FLOW_STAGES.WRONG_PERSON:
    case SELLER_FLOW_STAGES.NOT_INTERESTED:
    case SELLER_FLOW_STAGES.OWNERSHIP_CHECK:
    case SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP:
      return "Fallback / Market-Local";
    case SELLER_FLOW_STAGES.MF_CONFIRM_UNITS:
    case SELLER_FLOW_STAGES.MF_OCCUPANCY:
    case SELLER_FLOW_STAGES.MF_RENTS:
    case SELLER_FLOW_STAGES.MF_EXPENSES:
    case SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK:
    case SELLER_FLOW_STAGES.MF_OFFER_REVEAL:
    case SELLER_FLOW_STAGES.MF_CONFIRM_UNITS_FOLLOW_UP:
    case SELLER_FLOW_STAGES.MF_OCCUPANCY_FOLLOW_UP:
    case SELLER_FLOW_STAGES.MF_RENTS_FOLLOW_UP:
    case SELLER_FLOW_STAGES.MF_EXPENSES_FOLLOW_UP:
      return "Specialist-Landlord / Market-Local";
    case SELLER_FLOW_STAGES.CREATIVE_PROBE:
    case SELLER_FLOW_STAGES.CREATIVE_FOLLOW_UP:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO:
    case SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION:
    case SELLER_FLOW_STAGES.JUSTIFY_PRICE:
    case SELLER_FLOW_STAGES.ASK_TIMELINE:
    case SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER:
    case SELLER_FLOW_STAGES.NARROW_RANGE:
      return "Fallback / Market-Local / Specialist-Close";
    case SELLER_FLOW_STAGES.CLOSE_HANDOFF:
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

  if (text.includes("wrong person") || text.includes("wrong number")) {
    return SELLER_FLOW_STAGES.WRONG_PERSON;
  }

  if (text.includes("public property") || text.includes("public records")) {
    return SELLER_FLOW_STAGES.HOW_GOT_NUMBER;
  }

  if (text.includes("who is this") || (text.includes("this is") && text.includes("buying"))) {
    return SELLER_FLOW_STAGES.WHO_IS_THIS;
  }

  if (text.includes("just following up") && text.includes("owner")) {
    return SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP;
  }

  if (text.includes("circling back") && text.includes("open to")) {
    return SELLER_FLOW_STAGES.CONSIDER_SELLING_FOLLOW_UP;
  }

  if (
    text.includes("ballpark number") ||
    (text.includes("following up") && text.includes("number in mind"))
  ) {
    return SELLER_FLOW_STAGES.ASKING_PRICE_FOLLOW_UP;
  }

  if (text.includes("lease option")) return SELLER_FLOW_STAGES.OFFER_REVEAL_LEASE_OPTION;
  if (text.includes("subject to") || text.includes("take over payments")) {
    return SELLER_FLOW_STAGES.OFFER_REVEAL_SUBJECT_TO;
  }
  if (text.includes("novation")) return SELLER_FLOW_STAGES.OFFER_REVEAL_NOVATION;

  if (text.includes("how many total units")) return SELLER_FLOW_STAGES.MF_CONFIRM_UNITS;
  if (text.includes("currently occupied")) return SELLER_FLOW_STAGES.MF_OCCUPANCY;
  if (text.includes("monthly rents")) return SELLER_FLOW_STAGES.MF_RENTS;
  if (text.includes("monthly expenses")) return SELLER_FLOW_STAGES.MF_EXPENSES;
  if (text.includes("run numbers") && text.includes("circle back")) {
    return SELLER_FLOW_STAGES.MF_UNDERWRITING_ACK;
  }

  if (
    text.includes("owner of") ||
    text.includes("your property") ||
    text.includes("right person for")
  ) {
    return SELLER_FLOW_STAGES.OWNERSHIP_CHECK;
  }

  if (
    text.includes("open to selling") ||
    text.includes("consider selling") ||
    text.includes("open to an offer")
  ) {
    return SELLER_FLOW_STAGES.CONSIDER_SELLING;
  }

  if (
    text.includes("what number") ||
    text.includes("what would you want") ||
    text.includes("what price") ||
    text.includes("how much would you want") ||
    text.includes("price in mind")
  ) {
    return SELLER_FLOW_STAGES.ASKING_PRICE;
  }

  if (text.includes("would you be open to") && text.includes("option")) {
    return SELLER_FLOW_STAGES.CREATIVE_PROBE;
  }

  if (
    text.includes("before i respond to that price") ||
    text.includes("before i talk price") ||
    text.includes("needs repairs") ||
    text.includes("vacant or occupied")
  ) {
    return SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE;
  }

  if (
    text.includes("that price we might have room") ||
    text.includes("that could work") ||
    text.includes("might be in range")
  ) {
    return SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS;
  }

  if (
    text.includes("rough cash number") ||
    text.includes("likely be around") ||
    text.includes("my rough number") ||
    text.includes("$")
  ) {
    return SELLER_FLOW_STAGES.OFFER_REVEAL_CASH;
  }

  if (text.includes("repairs") || text.includes("closing costs")) {
    return SELLER_FLOW_STAGES.JUSTIFY_PRICE;
  }

  if (text.includes("timeline")) return SELLER_FLOW_STAGES.ASK_TIMELINE;
  if (text.includes("occupied") && text.includes("work it needs")) {
    return SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER;
  }
  if (text.includes("real number") || text.includes("keep it moving")) {
    return SELLER_FLOW_STAGES.NARROW_RANGE;
  }
  if (
    text.includes("next step") ||
    text.includes("paperwork") ||
    text.includes("move ahead") ||
    text.includes("move forward")
  ) {
    return SELLER_FLOW_STAGES.CLOSE_HANDOFF;
  }

  return null;
}

export function deriveCanonicalSellerFlowFromTemplate(template = null) {
  const use_case = normalizeSellerFlowUseCase(template?.use_case, template?.variant_group);
  const variant_group = normalizeVariantGroup(template?.variant_group);
  const selected_tone = normalizeSellerFlowTone(template?.tone);
  const next_expected_stage = canonicalStageForUseCase(use_case, variant_group);

  if (!use_case && !next_expected_stage) return null;

  return {
    selected_use_case: use_case || null,
    template_use_case: use_case || null,
    selected_variant_group: variant_group || null,
    selected_tone,
    next_expected_stage: next_expected_stage || null,
  };
}

export default {
  SELLER_FLOW_STAGES,
  normalizeSellerFlowTone,
  normalizeSellerFlowUseCase,
  canonicalStageForUseCase,
  baseSellerFlowStage,
  followUpUseCaseForStage,
  brainStageForUseCase,
  preferredAgentTypeForSellerFlow,
  inferCanonicalUseCaseFromOutboundText,
  deriveCanonicalSellerFlowFromTemplate,
};
