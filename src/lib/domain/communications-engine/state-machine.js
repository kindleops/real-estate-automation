import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";
import { addDays, toPodioDateField } from "@/lib/utils/dates.js";

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

function hasAnyTruthy(...values) {
  return values.some(Boolean);
}

function finiteNumber(value) {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function compactList(values = []) {
  return [...new Set(values.map((value) => clean(value)).filter(Boolean))];
}

export const LEGACY_STAGE_BUCKETS = Object.freeze({
  OWNERSHIP: "Ownership",
  OFFER: "Offer",
  QA: "Q/A",
  CONTRACT: "Contract",
  FOLLOW_UP: "Follow-Up",
});

export const CONVERSATION_STAGES = Object.freeze({
  OWNERSHIP_CONFIRMATION: "Ownership Confirmation",
  OFFER_INTEREST_CONFIRMATION: "Offer Interest Confirmation",
  SELLER_PRICE_DISCOVERY: "Seller Price Discovery",
  CONDITION_TIMELINE_DISCOVERY: "Condition / Timeline Discovery",
  OFFER_POSITIONING: "Offer Positioning",
  NEGOTIATION: "Negotiation",
  VERBAL_ACCEPTANCE_LOCK: "Verbal Acceptance / Lock",
  CONTRACT_OUT: "Contract Out",
  SIGNED_CLOSING: "Signed / Closing",
  CLOSED_DEAD_OUTCOME: "Closed / Dead Outcome",
});

export const CONVERSATION_STAGE_LIST = Object.freeze([
  CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION,
  CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY,
  CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
  CONVERSATION_STAGES.OFFER_POSITIONING,
  CONVERSATION_STAGES.NEGOTIATION,
  CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK,
  CONVERSATION_STAGES.CONTRACT_OUT,
  CONVERSATION_STAGES.SIGNED_CLOSING,
  CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
]);

export const CONVERSATION_BRANCHES = Object.freeze({
  OWNERSHIP_CONFIRMATION: "Ownership Confirmation",
  OFFER_INTEREST: "Offer Interest",
  PRICE_DISCOVERY: "Price Discovery",
  CONDITION_DISCOVERY: "Condition Discovery",
  OFFER_POSITIONING: "Offer Positioning",
  NEGOTIATION: "Negotiation",
  OBJECTION_HANDLING: "Objection Handling",
  REENGAGEMENT: "Re-Engagement",
  CONTRACT_PUSH: "Contract Push",
  DEAD_LEAD_HANDLING: "Dead Lead Handling",
  WRONG_NUMBER: "Wrong Number",
  DNC: "DNC",
  UNKNOWN: "Unknown",
});

export const SELLER_STATES = Object.freeze({
  UNCONFIRMED_OWNER: "Unconfirmed Owner",
  CONFIRMED_OWNER: "Confirmed Owner",
  NO_LONGER_OWNER: "No Longer Owner",
  OPEN_TO_OFFER: "Open To Offer",
  MAYBE_OPEN: "Maybe Open",
  NOT_INTERESTED: "Not Interested",
  WANTS_OFFER_FIRST: "Wants Offer First",
  PRICE_GIVEN: "Price Given",
  NO_PRICE_GIVEN: "No Price Given",
  CONDITION_UNKNOWN: "Condition Unknown",
  CONDITION_KNOWN: "Condition Known",
  NEAR_RANGE: "Near Range",
  ABOVE_RANGE: "Above Range",
  NEGOTIATING: "Negotiating",
  READY_FOR_CONTRACT: "Ready For Contract",
  SIGNED: "Signed",
  CLOSED: "Closed",
  DEAD: "Dead",
  DNC: "DNC",
  WRONG_NUMBER: "Wrong Number",
  UNKNOWN: "Unknown",
});

export const FOLLOW_UP_STEPS = Object.freeze({
  A: "A",
  B: "B",
  C: "C",
  D: "D",
  FINAL: "Final",
  NONE: "None",
});

export const LAST_DETECTED_INTENTS = Object.freeze({
  OWNERSHIP_CONFIRMED: "Ownership Confirmed",
  OWNERSHIP_DENIED: "Ownership Denied",
  OPEN_TO_OFFER: "Open To Offer",
  NOT_INTERESTED: "Not Interested",
  WANTS_OFFER: "Wants Offer",
  ASKING_PRICE_GIVEN: "Asking Price Given",
  WANTS_HIGHER_PRICE: "Wants Higher Price",
  CONDITION_MENTIONED: "Condition Mentioned",
  TIMELINE_MENTIONED: "Timeline Mentioned",
  NEGOTIATION: "Negotiation",
  CONTRACT_READY: "Contract Ready",
  WRONG_NUMBER: "Wrong Number",
  DNC: "DNC",
  UNKNOWN: "Unknown",
});

export const DEAL_STRATEGY_BRANCHES = Object.freeze({
  CASH: "Cash",
  SELLER_FINANCE: "Seller Finance",
  SUBJECT_TO: "Subject-To",
  NOVATION: "Novation",
  LEASE_OPTION: "Lease Option",
  HYBRID: "Hybrid",
  NURTURE: "Nurture",
  DNC: "DNC",
  WRONG_NUMBER: "Wrong Number",
  UNKNOWN: "Unknown",
});

export const CREATIVE_BRANCH_ELIGIBILITY = Object.freeze({
  YES: "Yes",
  NO: "No",
  MAYBE: "Maybe",
  UNKNOWN: "Unknown",
});

export const SELLER_EMOTIONAL_TONES = Object.freeze({
  CALM: "Calm",
  ANXIOUS: "Anxious",
  MOTIVATED: "Motivated",
  RESISTANT: "Resistant",
  GRIEVING: "Grieving",
  CONFUSED: "Confused",
  ANGRY: "Angry",
  EXCITED: "Excited",
  INDIFFERENT: "Indifferent",
  UNKNOWN: "Unknown",
});

export const RESPONSE_STYLE_MODES = Object.freeze({
  EMPATHETIC: "Empathetic",
  DIRECT: "Direct",
  FORMAL: "Formal",
  CASUAL: "Casual",
  SPIRITUAL: "Spiritual",
  URGENT: "Urgent",
  HUMOROUS: "Humorous",
  UNKNOWN: "Unknown",
});

export const PRIMARY_OBJECTION_TYPES = Object.freeze({
  PRICE_TOO_LOW: "Price Too Low",
  NOT_READY_TO_SELL: "Not Ready to Sell",
  HAS_AGENT: "Has Agent",
  INHERITED_DISPUTE: "Inherited Dispute",
  MARKET_COMPARING: "Market Comparing",
  WANTS_RETAIL: "Wants Retail",
  PROBATE_PENDING: "Probate Pending",
  NO_OBJECTION: "No Objection",
  UNKNOWN: "Unknown",
});

export const AI_MANAGED_STATUSES = Object.freeze({
  ACTIVE_NEGOTIATION: "Active Negotiation",
  WARM_LEAD: "Warm Lead",
  HOT_OPPORTUNITY: "Hot Opportunity",
  WAITING_ON_SELLER: "Waiting on Seller",
  AI_FOLLOW_UP_RUNNING: "AI Follow-Up Running",
  COLD_NO_RESPONSE: "Cold / No Response",
  UNDER_CONTRACT: "Under Contract",
  CLOSED: "Closed",
  DNC: "DNC",
  WRONG_NUMBER: "Wrong Number",
  PAUSED: "Paused",
  MANUAL_REVIEW: "Manual Review",
});

export const FOLLOW_UP_TRIGGER_STATES = Object.freeze({
  AI_RUNNING: "AI Running",
  WAITING: "Waiting",
  PAUSED: "Paused",
  MANUAL_OVERRIDE: "Manual Override",
  COMPLETED: "Completed",
  EXPIRED: "Expired",
});

export const EXECUTION_BRAIN_MILESTONES = Object.freeze({
  CONTRACT_CREATED: "contract_created",
  CONTRACT_SENT: "contract_sent",
  CONTRACT_VIEWED: "contract_viewed",
  CONTRACT_SIGNED: "contract_signed",
  CONTRACT_FULLY_EXECUTED: "contract_fully_executed",
  CONTRACT_CANCELLED: "contract_cancelled",
  TITLE_ROUTED: "title_routed",
  TITLE_OPENED: "title_opened",
  TITLE_REVIEWING: "title_reviewing",
  TITLE_WAITING_ON_DOCS: "title_waiting_on_docs",
  TITLE_WAITING_ON_PROBATE: "title_waiting_on_probate",
  TITLE_WAITING_ON_PAYOFF: "title_waiting_on_payoff",
  TITLE_WAITING_ON_SELLER: "title_waiting_on_seller",
  TITLE_WAITING_ON_BUYER: "title_waiting_on_buyer",
  TITLE_CLEAR_TO_CLOSE: "title_clear_to_close",
  TITLE_CLOSED: "title_closed",
  TITLE_CANCELLED: "title_cancelled",
  CLOSING_SCHEDULED: "closing_scheduled",
  CLOSING_CONFIRMED: "closing_confirmed",
  CLOSING_PENDING_DOCS: "closing_pending_docs",
  CLOSING_COMPLETED: "closing_completed",
  CLOSING_CANCELLED: "closing_cancelled",
  REVENUE_CONFIRMED: "revenue_confirmed",
});

const CONVERSATION_STAGE_NUMBER_MAP = Object.freeze({
  [CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION]: 1,
  [CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION]: 2,
  [CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY]: 3,
  [CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY]: 4,
  [CONVERSATION_STAGES.OFFER_POSITIONING]: 5,
  [CONVERSATION_STAGES.NEGOTIATION]: 6,
  [CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK]: 7,
  [CONVERSATION_STAGES.CONTRACT_OUT]: 8,
  [CONVERSATION_STAGES.SIGNED_CLOSING]: 9,
  [CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME]: 10,
});

const LOCKED_STAGE_NORMALIZATION_MAP = Object.freeze({
  "ownership confirmation": CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  ownership: CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  "offer interest confirmation": CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION,
  offer: CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION,
  "seller price discovery": CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY,
  "price discovery": CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY,
  "condition / timeline discovery": CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
  "condition timeline discovery": CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
  "condition discovery": CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
  "q/a": CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
  qa: CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
  "offer positioning": CONVERSATION_STAGES.OFFER_POSITIONING,
  negotiation: CONVERSATION_STAGES.NEGOTIATION,
  "verbal acceptance / lock": CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK,
  "verbal acceptance lock": CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK,
  "contract out": CONVERSATION_STAGES.CONTRACT_OUT,
  contract: CONVERSATION_STAGES.CONTRACT_OUT,
  "signed / closing": CONVERSATION_STAGES.SIGNED_CLOSING,
  "signed closing": CONVERSATION_STAGES.SIGNED_CLOSING,
  "closed / dead outcome": CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
  "closed dead outcome": CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
  "follow-up": CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  "follow up": CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  followup: CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
});

const LOCKED_TO_LEGACY_STAGE_MAP = Object.freeze({
  [CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION]: LEGACY_STAGE_BUCKETS.OWNERSHIP,
  [CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION]: LEGACY_STAGE_BUCKETS.OFFER,
  [CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY]: LEGACY_STAGE_BUCKETS.OFFER,
  [CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY]: LEGACY_STAGE_BUCKETS.QA,
  [CONVERSATION_STAGES.OFFER_POSITIONING]: LEGACY_STAGE_BUCKETS.OFFER,
  [CONVERSATION_STAGES.NEGOTIATION]: LEGACY_STAGE_BUCKETS.OFFER,
  [CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK]: LEGACY_STAGE_BUCKETS.CONTRACT,
  [CONVERSATION_STAGES.CONTRACT_OUT]: LEGACY_STAGE_BUCKETS.CONTRACT,
  [CONVERSATION_STAGES.SIGNED_CLOSING]: LEGACY_STAGE_BUCKETS.CONTRACT,
  [CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME]: LEGACY_STAGE_BUCKETS.FOLLOW_UP,
});

const FOLLOW_UP_DELAYS_BY_STAGE_NUMBER = Object.freeze({
  1: Object.freeze({ A: 2, B: 4, C: 7, D: 10, Final: 14 }),
  2: Object.freeze({ A: 2, B: 4, C: 7, D: 10, Final: 14 }),
  3: Object.freeze({ A: 1, B: 3, C: 5, D: 7, Final: 10 }),
  4: Object.freeze({ A: 1, B: 3, C: 5, D: 7, Final: 10 }),
  5: Object.freeze({ A: 1, B: 2, C: 4, D: 6, Final: 8 }),
  6: Object.freeze({ A: 1, B: 2, C: 3, D: 5, Final: 7 }),
  7: Object.freeze({ A: 1, B: 1, C: 2, D: 3, Final: 5 }),
  8: Object.freeze({ A: 1, B: 1, C: 2, D: 3, Final: 5 }),
  9: Object.freeze({ A: 1, B: 1, C: 2, D: 3, Final: 5 }),
});

const FOLLOW_UP_STEP_ORDER = Object.freeze([
  FOLLOW_UP_STEPS.NONE,
  FOLLOW_UP_STEPS.A,
  FOLLOW_UP_STEPS.B,
  FOLLOW_UP_STEPS.C,
  FOLLOW_UP_STEPS.D,
  FOLLOW_UP_STEPS.FINAL,
]);

function mapEmotionToTone(emotion = null) {
  switch (lower(emotion)) {
    case "motivated":
      return SELLER_EMOTIONAL_TONES.MOTIVATED;
    case "grieving":
      return SELLER_EMOTIONAL_TONES.GRIEVING;
    case "frustrated":
      return SELLER_EMOTIONAL_TONES.ANGRY;
    case "guarded":
    case "skeptical":
      return SELLER_EMOTIONAL_TONES.RESISTANT;
    case "overwhelmed":
      return SELLER_EMOTIONAL_TONES.ANXIOUS;
    case "curious":
      return SELLER_EMOTIONAL_TONES.CALM;
    case "tired_landlord":
      return SELLER_EMOTIONAL_TONES.INDIFFERENT;
    case "calm":
      return SELLER_EMOTIONAL_TONES.CALM;
    default:
      return SELLER_EMOTIONAL_TONES.UNKNOWN;
  }
}

function deriveResponseStyleMode({ message = "", classification = null } = {}) {
  const text = clean(message);
  const normalized = lower(text);
  const word_count = text.split(/\s+/).filter(Boolean).length;
  const emoji_count = (text.match(/[\u{1F300}-\u{1FAFF}]/gu) || []).length;

  if (includesAny(normalized, ["god bless", "bless you", "praying", "pray"])) {
    return RESPONSE_STYLE_MODES.SPIRITUAL;
  }

  if (includesAny(normalized, ["haha", "lol", "lmao"])) {
    return RESPONSE_STYLE_MODES.HUMOROUS;
  }

  if (includesAny(normalized, ["asap", "right now", "today", "immediately"])) {
    return RESPONSE_STYLE_MODES.URGENT;
  }

  if (
    emoji_count >= 2 ||
    /[!?]{2,}/.test(text) ||
    includesAny(normalized, ["bro", "hey", "yep", "nah", "lol", "haha"])
  ) {
    return RESPONSE_STYLE_MODES.CASUAL;
  }

  if (word_count <= 4) {
    return RESPONSE_STYLE_MODES.DIRECT;
  }

  if (word_count >= 20) {
    return RESPONSE_STYLE_MODES.FORMAL;
  }

  if (includesAny(normalized, ["please", "thank you", "kindly"])) {
    return RESPONSE_STYLE_MODES.FORMAL;
  }

  switch (lower(classification?.emotion)) {
    case "grieving":
    case "overwhelmed":
      return RESPONSE_STYLE_MODES.EMPATHETIC;
    case "motivated":
      return RESPONSE_STYLE_MODES.DIRECT;
    default:
      return RESPONSE_STYLE_MODES.UNKNOWN;
  }
}

function derivePrimaryObjectionType(classification = null) {
  switch (clean(classification?.objection)) {
    case "need_more_money":
      return PRIMARY_OBJECTION_TYPES.PRICE_TOO_LOW;
    case "need_family_ok":
      return PRIMARY_OBJECTION_TYPES.INHERITED_DISPUTE;
    case "need_time":
      return PRIMARY_OBJECTION_TYPES.NOT_READY_TO_SELL;
    case "already_listed":
    case "has_other_buyer":
      return PRIMARY_OBJECTION_TYPES.HAS_AGENT;
    case "probate":
      return PRIMARY_OBJECTION_TYPES.PROBATE_PENDING;
    case "divorce":
      return PRIMARY_OBJECTION_TYPES.INHERITED_DISPUTE;
    case "wants_retail":
      return PRIMARY_OBJECTION_TYPES.WANTS_RETAIL;
    case "":
    case "null":
    case null:
      return PRIMARY_OBJECTION_TYPES.NO_OBJECTION;
    default:
      return PRIMARY_OBJECTION_TYPES.UNKNOWN;
  }
}

function isExplicitWrongNumber(message = "") {
  return includesAny(message, [
    "wrong number",
    "wrong person",
    "not me",
    "do not own",
    "don't own",
    "dont own",
    "not my property",
    "no longer own",
  ]);
}

function isExplicitOwnershipDenied(message = "") {
  return includesAny(message, [
    "sold it",
    "sold the property",
    "i no longer own",
    "we no longer own",
    "not the owner",
    "not an owner",
    "don't own it anymore",
    "dont own it anymore",
  ]);
}

function isExplicitWantsOffer(message = "") {
  return includesAny(message, [
    "make me an offer",
    "send me an offer",
    "what are you offering",
    "what is your offer",
    "what's your offer",
    "give me an offer",
    "what can you pay",
    "what would you pay",
  ]);
}

function isExplicitOpenToOffer(message = "") {
  return includesAny(message, [
    "open to an offer",
    "open to offer",
    "open to selling",
    "would consider selling",
    "would consider an offer",
    "depends on the offer",
    "depends on the price",
    "maybe",
    "possibly",
    "if the price is right",
  ]);
}

function isExplicitOwnershipConfirmed(message = "") {
  return includesAny(message, [
    "i own it",
    "i am the owner",
    "yes i own",
    "yes that's mine",
    "yes thats mine",
    "that is my property",
    "that's my property",
  ]);
}

function isExplicitContractReady(message = "") {
  return includesAny(message, [
    "send the contract",
    "send contract",
    "send me the contract",
    "ready for contract",
    "ready to move forward",
    "move forward",
    "next step",
    "lets do it",
    "let's do it",
  ]);
}

function hasTimelineSignal(signals = {}) {
  return hasAnyTruthy(signals.timeline);
}

function hasConditionSignal(signals = {}) {
  return hasAnyTruthy(
    signals.condition_level,
    signals.occupancy_status,
    signals.estimated_repair_cost,
    signals.unit_count,
    signals.rents_present,
    signals.expenses_present
  );
}

function hasCreativeSignal(message = "", signals = {}) {
  return Boolean(
    signals.creative_terms_interest ||
      signals.novation_interest ||
      includesAny(message, [
        "seller finance",
        "seller financing",
        "owner finance",
        "owner financing",
        "subject to",
        "subto",
        "monthly payment",
        "monthly payments",
        "lease option",
        "novation",
        "terms",
      ])
  );
}

function getCashOfferTarget(context = null) {
  return (
    finiteNumber(getNumberValue(context?.items?.property_item || null, "smart-cash-offer-2", null)) ||
    finiteNumber(context?.summary?.offer_price) ||
    finiteNumber(context?.summary?.smart_cash_offer) ||
    null
  );
}

function isPricingNegotiationIntent(intent) {
  return [
    LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN,
    LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE,
    LAST_DETECTED_INTENTS.NEGOTIATION,
  ].includes(intent);
}

export function normalizeLockedConversationStage(
  value,
  fallback = CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION
) {
  return LOCKED_STAGE_NORMALIZATION_MAP[lower(value)] || fallback;
}

export function collapseConversationStageToLegacy(
  value,
  fallback = LEGACY_STAGE_BUCKETS.OWNERSHIP
) {
  const locked = normalizeLockedConversationStage(value, null);
  return LOCKED_TO_LEGACY_STAGE_MAP[locked] || fallback;
}

export function getConversationStageNumber(value) {
  const locked = normalizeLockedConversationStage(value, null);
  return CONVERSATION_STAGE_NUMBER_MAP[locked] || 1;
}

export function stageForNumber(number) {
  return (
    CONVERSATION_STAGE_LIST.find(
      (stage) => CONVERSATION_STAGE_NUMBER_MAP[stage] === Number(number)
    ) || CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION
  );
}

export function normalizeFollowUpStep(value, fallback = FOLLOW_UP_STEPS.NONE) {
  const raw = lower(value);
  if (raw === "a") return FOLLOW_UP_STEPS.A;
  if (raw === "b") return FOLLOW_UP_STEPS.B;
  if (raw === "c") return FOLLOW_UP_STEPS.C;
  if (raw === "d") return FOLLOW_UP_STEPS.D;
  if (raw === "final") return FOLLOW_UP_STEPS.FINAL;
  if (raw === "none") return FOLLOW_UP_STEPS.NONE;
  return fallback;
}

export function advanceFollowUpStep(value = FOLLOW_UP_STEPS.NONE) {
  const normalized = normalizeFollowUpStep(value);
  const index = FOLLOW_UP_STEP_ORDER.indexOf(normalized);
  if (index === -1 || index >= FOLLOW_UP_STEP_ORDER.length - 1) {
    return FOLLOW_UP_STEPS.FINAL;
  }
  return FOLLOW_UP_STEP_ORDER[index + 1];
}

export function shouldUseAiConversationAssist({
  classification = null,
  message = "",
  signals = {},
} = {}) {
  if (clean(classification?.source) === "ai") return true;
  if (clean(classification?.notes)) return true;

  const normalized_message = clean(message);
  if (!normalized_message) return false;

  const word_count = normalized_message.split(/\s+/).filter(Boolean).length;
  const deterministic_intent = deriveDeterministicIntent({
    classification,
    message,
    signals,
  });

  if (deterministic_intent !== LAST_DETECTED_INTENTS.UNKNOWN) return false;

  return word_count >= 8;
}

export function computePricingContext({
  context = null,
  seller_ask_price = null,
} = {}) {
  const cash_offer_target = getCashOfferTarget(context);
  const ask = finiteNumber(seller_ask_price);

  return {
    cash_offer_target,
    seller_ask_price: ask,
    price_gap_to_target:
      ask !== null && cash_offer_target !== null
        ? ask - cash_offer_target
        : null,
  };
}

function deriveDealPriorityTag({
  intent,
  conversation_stage,
  price_gap_to_target = null,
  motivation_score = null,
  signals = {},
} = {}) {
  if (
    [
      LAST_DETECTED_INTENTS.DNC,
      LAST_DETECTED_INTENTS.WRONG_NUMBER,
      LAST_DETECTED_INTENTS.OWNERSHIP_DENIED,
      LAST_DETECTED_INTENTS.NOT_INTERESTED,
    ].includes(intent)
  ) {
    return "Low Priority";
  }

  if (
    intent === LAST_DETECTED_INTENTS.CONTRACT_READY ||
    [
      CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK,
      CONVERSATION_STAGES.CONTRACT_OUT,
      CONVERSATION_STAGES.SIGNED_CLOSING,
    ].includes(conversation_stage)
  ) {
    return "Urgent";
  }

  if (
    conversation_stage === CONVERSATION_STAGES.NEGOTIATION ||
    finiteNumber(price_gap_to_target) !== null ||
    (finiteNumber(motivation_score) !== null && motivation_score >= 70) ||
    hasTimelineSignal(signals)
  ) {
    return "High Priority";
  }

  if (intent === LAST_DETECTED_INTENTS.OPEN_TO_OFFER) {
    return "Medium Priority";
  }

  return "Medium Priority";
}

function deriveRiskFlags({
  message = "",
  classification = null,
  intent,
  price_gap_to_target = null,
  seller_emotional_tone = null,
  primary_objection_type = null,
} = {}) {
  const flags = [];
  const normalized_message = lower(message);
  const objection = clean(classification?.objection);
  const word_count = clean(message).split(/\s+/).filter(Boolean).length;

  if (
    finiteNumber(price_gap_to_target) !== null &&
    price_gap_to_target > 25_000
  ) {
    flags.push("Wants Too High");
  }

  if (
    [
      PRIMARY_OBJECTION_TYPES.PRICE_TOO_LOW,
      PRIMARY_OBJECTION_TYPES.WANTS_RETAIL,
    ].includes(primary_objection_type)
  ) {
    flags.push("Wants Too High");
  }

  if (objection === "need_family_ok") {
    flags.push("Not Decision Maker");
  }

  if (
    seller_emotional_tone === SELLER_EMOTIONAL_TONES.ANGRY ||
    (seller_emotional_tone === SELLER_EMOTIONAL_TONES.RESISTANT &&
      word_count <= 5)
  ) {
    flags.push("Angry / Short Replies");
  }

  if (
    [SELLER_EMOTIONAL_TONES.ANXIOUS, SELLER_EMOTIONAL_TONES.GRIEVING].includes(
      seller_emotional_tone
    )
  ) {
    flags.push("Emotional Volatility");
  }

  if (
    includesAny(normalized_message, [
      "attorney",
      "lawyer",
      "legal",
      "cease and desist",
      "sue",
      "lawsuit",
    ])
  ) {
    flags.push("Legal Threat");
  }

  if (
    [PRIMARY_OBJECTION_TYPES.HAS_AGENT].includes(primary_objection_type) ||
    includesAny(normalized_message, ["my agent", "my realtor", "listed"])
  ) {
    flags.push("Represented by Agent");
  }

  if (intent === LAST_DETECTED_INTENTS.WRONG_NUMBER && objection !== "wrong_number") {
    flags.push("Not Decision Maker");
  }

  const compacted = compactList(flags);
  return compacted.length ? compacted : ["Unknown"];
}

export function deriveDeterministicIntent({
  classification = null,
  message = "",
  signals = {},
} = {}) {
  const objection = clean(classification?.objection);
  const compliance_flag = clean(classification?.compliance_flag);
  const normalized_message = clean(message);

  if (compliance_flag === "stop_texting") return LAST_DETECTED_INTENTS.DNC;
  if (isExplicitOwnershipDenied(normalized_message)) {
    return LAST_DETECTED_INTENTS.OWNERSHIP_DENIED;
  }
  if (objection === "wrong_number" || isExplicitWrongNumber(normalized_message)) {
    return LAST_DETECTED_INTENTS.WRONG_NUMBER;
  }
  if (objection === "not_interested") return LAST_DETECTED_INTENTS.NOT_INTERESTED;
  if (isExplicitContractReady(normalized_message)) {
    return LAST_DETECTED_INTENTS.CONTRACT_READY;
  }
  // Objection-based higher-price demand takes priority over a bare price extract
  if (
    ["need_more_money", "has_other_buyer", "wants_retail"].includes(objection) ||
    includesAny(normalized_message, ["too low", "can you do better", "need more", "come up"])
  ) {
    return LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE;
  }
  if (finiteNumber(signals.asking_price) !== null) {
    return LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN;
  }
  if (objection === "send_offer_first" || isExplicitWantsOffer(normalized_message)) {
    return LAST_DETECTED_INTENTS.WANTS_OFFER;
  }
  if (hasTimelineSignal(signals)) {
    return LAST_DETECTED_INTENTS.TIMELINE_MENTIONED;
  }
  if (hasConditionSignal(signals)) {
    return LAST_DETECTED_INTENTS.CONDITION_MENTIONED;
  }
  if (
    isExplicitOpenToOffer(normalized_message) ||
    (Array.isArray(classification?.positive_signals) &&
      classification.positive_signals.includes("affirmative"))
  ) {
    return LAST_DETECTED_INTENTS.OPEN_TO_OFFER;
  }
  if (isExplicitOwnershipConfirmed(normalized_message)) {
    return LAST_DETECTED_INTENTS.OWNERSHIP_CONFIRMED;
  }

  return LAST_DETECTED_INTENTS.UNKNOWN;
}

function deriveCreativeEligibility({
  intent,
  message = "",
  signals = {},
  price_gap_to_target = null,
} = {}) {
  if ([LAST_DETECTED_INTENTS.DNC, LAST_DETECTED_INTENTS.WRONG_NUMBER].includes(intent)) {
    return CREATIVE_BRANCH_ELIGIBILITY.NO;
  }

  if (hasCreativeSignal(message, signals)) {
    return CREATIVE_BRANCH_ELIGIBILITY.YES;
  }

  if (
    isPricingNegotiationIntent(intent) &&
    finiteNumber(price_gap_to_target) !== null &&
    price_gap_to_target > 25_000
  ) {
    return CREATIVE_BRANCH_ELIGIBILITY.MAYBE;
  }

  if (intent === LAST_DETECTED_INTENTS.NOT_INTERESTED) {
    return CREATIVE_BRANCH_ELIGIBILITY.MAYBE;
  }

  return CREATIVE_BRANCH_ELIGIBILITY.UNKNOWN;
}

function deriveDealStrategyBranch({
  intent,
  message = "",
  signals = {},
  creative_branch_eligibility = CREATIVE_BRANCH_ELIGIBILITY.UNKNOWN,
} = {}) {
  const normalized_message = lower(message);

  if (intent === LAST_DETECTED_INTENTS.DNC) return DEAL_STRATEGY_BRANCHES.DNC;
  if (intent === LAST_DETECTED_INTENTS.WRONG_NUMBER) {
    return DEAL_STRATEGY_BRANCHES.WRONG_NUMBER;
  }

  if (includesAny(normalized_message, ["subject to", "subto"])) {
    return DEAL_STRATEGY_BRANCHES.SUBJECT_TO;
  }
  if (
    includesAny(normalized_message, [
      "seller finance",
      "seller financing",
      "owner finance",
      "owner financing",
      "monthly payment",
      "monthly payments",
    ])
  ) {
    return DEAL_STRATEGY_BRANCHES.SELLER_FINANCE;
  }
  if (includesAny(normalized_message, ["lease option"])) {
    return DEAL_STRATEGY_BRANCHES.LEASE_OPTION;
  }
  if (signals.novation_interest || includesAny(normalized_message, ["novation", "list it", "retail"])) {
    return DEAL_STRATEGY_BRANCHES.NOVATION;
  }
  if (creative_branch_eligibility === CREATIVE_BRANCH_ELIGIBILITY.YES) {
    return DEAL_STRATEGY_BRANCHES.HYBRID;
  }
  if (
    creative_branch_eligibility === CREATIVE_BRANCH_ELIGIBILITY.MAYBE &&
    intent !== LAST_DETECTED_INTENTS.NOT_INTERESTED
  ) {
    return DEAL_STRATEGY_BRANCHES.CASH;
  }
  if (intent === LAST_DETECTED_INTENTS.NOT_INTERESTED) {
    return DEAL_STRATEGY_BRANCHES.NURTURE;
  }

  return DEAL_STRATEGY_BRANCHES.CASH;
}

function deriveConversationBranch({
  intent,
  stage,
  seller_state,
} = {}) {
  if (intent === LAST_DETECTED_INTENTS.DNC) return CONVERSATION_BRANCHES.DNC;
  if (intent === LAST_DETECTED_INTENTS.WRONG_NUMBER) {
    return CONVERSATION_BRANCHES.WRONG_NUMBER;
  }
  if (
    [SELLER_STATES.NOT_INTERESTED, SELLER_STATES.DEAD, SELLER_STATES.NO_LONGER_OWNER].includes(
      seller_state
    )
  ) {
    return CONVERSATION_BRANCHES.DEAD_LEAD_HANDLING;
  }
  if (
    [LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE, LAST_DETECTED_INTENTS.NEGOTIATION].includes(intent)
  ) {
    return CONVERSATION_BRANCHES.OBJECTION_HANDLING;
  }

  switch (stage) {
    case CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION:
      return CONVERSATION_BRANCHES.OWNERSHIP_CONFIRMATION;
    case CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION:
      return CONVERSATION_BRANCHES.OFFER_INTEREST;
    case CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY:
      return CONVERSATION_BRANCHES.PRICE_DISCOVERY;
    case CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY:
      return CONVERSATION_BRANCHES.CONDITION_DISCOVERY;
    case CONVERSATION_STAGES.OFFER_POSITIONING:
      return CONVERSATION_BRANCHES.OFFER_POSITIONING;
    case CONVERSATION_STAGES.NEGOTIATION:
      return CONVERSATION_BRANCHES.NEGOTIATION;
    case CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK:
    case CONVERSATION_STAGES.CONTRACT_OUT:
    case CONVERSATION_STAGES.SIGNED_CLOSING:
      return CONVERSATION_BRANCHES.CONTRACT_PUSH;
    case CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME:
      return CONVERSATION_BRANCHES.DEAD_LEAD_HANDLING;
    default:
      return CONVERSATION_BRANCHES.UNKNOWN;
  }
}

function deriveSellerState({
  intent,
  stage,
  price_gap_to_target = null,
  signals = {},
} = {}) {
  if (intent === LAST_DETECTED_INTENTS.DNC) return SELLER_STATES.DNC;
  if (intent === LAST_DETECTED_INTENTS.WRONG_NUMBER) return SELLER_STATES.WRONG_NUMBER;
  if (intent === LAST_DETECTED_INTENTS.OWNERSHIP_DENIED) {
    return SELLER_STATES.NO_LONGER_OWNER;
  }
  if (intent === LAST_DETECTED_INTENTS.CONTRACT_READY) {
    return SELLER_STATES.READY_FOR_CONTRACT;
  }
  if (
    finiteNumber(price_gap_to_target) !== null &&
    price_gap_to_target <= 0 &&
    stage === CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK
  ) {
    return SELLER_STATES.READY_FOR_CONTRACT;
  }
  if (stage === CONVERSATION_STAGES.SIGNED_CLOSING) return SELLER_STATES.SIGNED;
  if (stage === CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME) return SELLER_STATES.CLOSED;
  if (intent === LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE) {
    return SELLER_STATES.ABOVE_RANGE;
  }
  if (finiteNumber(price_gap_to_target) !== null) {
    return price_gap_to_target <= 15_000
      ? SELLER_STATES.NEAR_RANGE
      : SELLER_STATES.ABOVE_RANGE;
  }
  if (intent === LAST_DETECTED_INTENTS.NEGOTIATION || stage === CONVERSATION_STAGES.NEGOTIATION) {
    return SELLER_STATES.NEGOTIATING;
  }
  if (intent === LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN) return SELLER_STATES.PRICE_GIVEN;
  if (intent === LAST_DETECTED_INTENTS.WANTS_OFFER) return SELLER_STATES.WANTS_OFFER_FIRST;
  if (intent === LAST_DETECTED_INTENTS.NOT_INTERESTED) return SELLER_STATES.NOT_INTERESTED;
  if (intent === LAST_DETECTED_INTENTS.OPEN_TO_OFFER) return SELLER_STATES.OPEN_TO_OFFER;
  if (intent === LAST_DETECTED_INTENTS.OWNERSHIP_CONFIRMED) {
    return SELLER_STATES.CONFIRMED_OWNER;
  }
  if (hasConditionSignal(signals)) return SELLER_STATES.CONDITION_KNOWN;
  if (stage === CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION) {
    return SELLER_STATES.UNCONFIRMED_OWNER;
  }
  if (
    [
      CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY,
      CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY,
      CONVERSATION_STAGES.OFFER_POSITIONING,
      CONVERSATION_STAGES.NEGOTIATION,
    ].includes(stage)
  ) {
    return SELLER_STATES.NO_PRICE_GIVEN;
  }

  return SELLER_STATES.UNKNOWN;
}

function deriveStageCandidate({
  intent,
  signals = {},
  price_gap_to_target = null,
  previous_stage = CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
} = {}) {
  if ([LAST_DETECTED_INTENTS.DNC, LAST_DETECTED_INTENTS.WRONG_NUMBER].includes(intent)) {
    return CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME;
  }

  if (intent === LAST_DETECTED_INTENTS.OWNERSHIP_DENIED) {
    return CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME;
  }

  if (intent === LAST_DETECTED_INTENTS.CONTRACT_READY) {
    return CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK;
  }

  if (
    finiteNumber(signals.asking_price) !== null &&
    (hasConditionSignal(signals) || hasTimelineSignal(signals))
  ) {
    return CONVERSATION_STAGES.OFFER_POSITIONING;
  }

  if (
    finiteNumber(price_gap_to_target) !== null &&
    price_gap_to_target <= 0 &&
    intent === LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN
  ) {
    return CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK;
  }

  // Only an explicit higher-price demand (objection-driven) escalates to NEGOTIATION.
  // A first-ask price with a large gap stays in price discovery / offer positioning.
  if (intent === LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE) {
    return CONVERSATION_STAGES.NEGOTIATION;
  }

  if (
    finiteNumber(price_gap_to_target) !== null &&
    price_gap_to_target > 0 &&
    intent === LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN
  ) {
    return CONVERSATION_STAGES.NEGOTIATION;
  }

  if (hasConditionSignal(signals) || hasTimelineSignal(signals)) {
    return CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY;
  }

  if (finiteNumber(signals.asking_price) !== null) {
    return CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY;
  }

  if (
    [LAST_DETECTED_INTENTS.OPEN_TO_OFFER, LAST_DETECTED_INTENTS.WANTS_OFFER].includes(intent)
  ) {
    return CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION;
  }

  if (intent === LAST_DETECTED_INTENTS.OWNERSHIP_CONFIRMED) {
    return CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION;
  }

  return normalizeLockedConversationStage(previous_stage);
}

function chooseProgressedStage(previous_stage, candidate_stage) {
  const previous_number = getConversationStageNumber(previous_stage);
  const candidate_number = getConversationStageNumber(candidate_stage);

  if (candidate_number === 10) return CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME;
  return stageForNumber(Math.max(previous_number, candidate_number));
}

function deriveAiManagedStatus({
  detected_intent,
  conversation_stage,
  risk_flags = [],
} = {}) {
  if (detected_intent === LAST_DETECTED_INTENTS.DNC) {
    return AI_MANAGED_STATUSES.DNC;
  }
  if (detected_intent === LAST_DETECTED_INTENTS.WRONG_NUMBER) {
    return AI_MANAGED_STATUSES.WRONG_NUMBER;
  }
  if (risk_flags.includes("Legal Threat")) {
    return AI_MANAGED_STATUSES.MANUAL_REVIEW;
  }
  if (detected_intent === LAST_DETECTED_INTENTS.OWNERSHIP_DENIED) {
    return AI_MANAGED_STATUSES.PAUSED;
  }
  if (
    conversation_stage === CONVERSATION_STAGES.SIGNED_CLOSING ||
    conversation_stage === CONVERSATION_STAGES.CONTRACT_OUT
  ) {
    return AI_MANAGED_STATUSES.UNDER_CONTRACT;
  }
  if (conversation_stage === CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME) {
    return AI_MANAGED_STATUSES.COLD_NO_RESPONSE;
  }
  if (
    [
      CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK,
      CONVERSATION_STAGES.NEGOTIATION,
    ].includes(conversation_stage)
  ) {
    return AI_MANAGED_STATUSES.HOT_OPPORTUNITY;
  }
  return AI_MANAGED_STATUSES.WARM_LEAD;
}

function deriveFollowUpTriggerState({
  detected_intent,
  risk_flags = [],
} = {}) {
  if (
    [LAST_DETECTED_INTENTS.DNC, LAST_DETECTED_INTENTS.WRONG_NUMBER].includes(
      detected_intent
    )
  ) {
    return FOLLOW_UP_TRIGGER_STATES.COMPLETED;
  }

  if (risk_flags.includes("Legal Threat")) {
    return FOLLOW_UP_TRIGGER_STATES.PAUSED;
  }

  if (detected_intent === LAST_DETECTED_INTENTS.OWNERSHIP_DENIED) {
    return FOLLOW_UP_TRIGGER_STATES.COMPLETED;
  }

  return FOLLOW_UP_TRIGGER_STATES.AI_RUNNING;
}

function deriveLastMessageSummary({
  message = "",
  detected_intent,
  conversation_stage,
  current_seller_state,
  seller_ask_price = null,
  price_gap_to_target = null,
} = {}) {
  const parts = [
    `Seller intent: ${detected_intent || LAST_DETECTED_INTENTS.UNKNOWN}.`,
    `Stage: ${conversation_stage}.`,
    `State: ${current_seller_state}.`,
  ];

  if (finiteNumber(seller_ask_price) !== null) {
    parts.push(`Ask: $${finiteNumber(seller_ask_price)}.`);
  }
  if (finiteNumber(price_gap_to_target) !== null) {
    parts.push(`Gap to target: $${finiteNumber(price_gap_to_target)}.`);
  }

  const trimmed_message = clean(message);
  if (trimmed_message) {
    parts.push(`Latest seller message: ${trimmed_message.slice(0, 120)}${trimmed_message.length > 120 ? "..." : ""}`);
  }

  return parts.join(" ");
}

function deriveFullConversationSummary({
  context = null,
  conversation_stage,
  current_conversation_branch,
  current_seller_state,
  last_detected_intent,
  deal_strategy_branch,
  deal_priority_tag,
  seller_ask_price = null,
  cash_offer_target = null,
  risk_flags = [],
} = {}) {
  const property_address = clean(context?.summary?.property_address);
  const owner_name = clean(context?.summary?.owner_name);
  const parts = [
    owner_name ? `Seller: ${owner_name}.` : "",
    property_address ? `Property: ${property_address}.` : "",
    `Stage ${getConversationStageNumber(conversation_stage)} ${conversation_stage}.`,
    `Branch: ${current_conversation_branch}.`,
    `Seller state: ${current_seller_state}.`,
    `Intent: ${last_detected_intent}.`,
    `Strategy: ${deal_strategy_branch}.`,
    `Priority: ${deal_priority_tag}.`,
  ];

  if (finiteNumber(seller_ask_price) !== null) {
    parts.push(`Ask: $${finiteNumber(seller_ask_price)}.`);
  }
  if (finiteNumber(cash_offer_target) !== null) {
    parts.push(`Cash target: $${finiteNumber(cash_offer_target)}.`);
  }
  if (risk_flags.length && !risk_flags.includes("Unknown")) {
    parts.push(`Risks: ${risk_flags.join(", ")}.`);
  }

  return parts.filter(Boolean).join(" ");
}

function deriveAiRecommendedNextMove({
  detected_intent,
  conversation_stage,
  current_seller_state,
  creative_branch_eligibility,
  deal_strategy_branch,
  price_gap_to_target = null,
  risk_flags = [],
} = {}) {
  if (detected_intent === LAST_DETECTED_INTENTS.DNC) {
    return "Stop all seller follow-up and leave the thread in a terminal DNC state.";
  }
  if (detected_intent === LAST_DETECTED_INTENTS.WRONG_NUMBER) {
    return "Stop follow-up, mark the thread as wrong number, and avoid any further seller automation.";
  }
  if (risk_flags.includes("Legal Threat")) {
    return "Pause automation and route this seller thread to manual review immediately.";
  }
  if (detected_intent === LAST_DETECTED_INTENTS.OWNERSHIP_DENIED) {
    return "Treat the thread as a dead lead, stop automated follow-up, and avoid further offer messaging.";
  }
  if (detected_intent === LAST_DETECTED_INTENTS.CONTRACT_READY) {
    return "Move into lock flow, confirm decision makers, and prepare contract delivery immediately.";
  }
  if (
    finiteNumber(price_gap_to_target) !== null &&
    price_gap_to_target <= 0 &&
    current_seller_state !== SELLER_STATES.WRONG_NUMBER
  ) {
    return "Press toward verbal acceptance, confirm condition and timing quickly, and prepare the next lock step.";
  }
  if (
    conversation_stage === CONVERSATION_STAGES.NEGOTIATION &&
    creative_branch_eligibility !== CREATIVE_BRANCH_ELIGIBILITY.NO &&
    [
      DEAL_STRATEGY_BRANCHES.SELLER_FINANCE,
      DEAL_STRATEGY_BRANCHES.SUBJECT_TO,
      DEAL_STRATEGY_BRANCHES.NOVATION,
      DEAL_STRATEGY_BRANCHES.LEASE_OPTION,
      DEAL_STRATEGY_BRANCHES.HYBRID,
    ].includes(deal_strategy_branch)
  ) {
    return `Stay in negotiation, justify the cash position, and test ${deal_strategy_branch.toLowerCase()} as the next structured option.`;
  }
  if (
    detected_intent === LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE ||
    current_seller_state === SELLER_STATES.ABOVE_RANGE
  ) {
    return "Acknowledge the gap, justify pricing with condition and market logic, and probe whether timing or terms can narrow the spread.";
  }
  if (detected_intent === LAST_DETECTED_INTENTS.WANTS_OFFER) {
    return "Keep the seller engaged, frame the cash position simply, and gather only the missing condition or timeline details needed to position the offer.";
  }
  if (conversation_stage === CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY) {
    return "Clarify condition and timing, then position the next pricing step once the missing facts are confirmed.";
  }
  if (conversation_stage === CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION) {
    return "Confirm motivation and transition into price discovery or offer positioning without losing the seller's momentum.";
  }

  return "Keep the thread moving one stage forward with a concise seller-safe message and preserve deterministic follow-up control.";
}

function deriveAiNextMessage({
  detected_intent,
  conversation_stage,
  current_seller_state,
  deal_strategy_branch,
  language_preference = "English",
} = {}) {
  if (
    [LAST_DETECTED_INTENTS.DNC, LAST_DETECTED_INTENTS.WRONG_NUMBER].includes(
      detected_intent
    )
  ) {
    return null;
  }

  const is_spanish = lower(language_preference) === "spanish";

  if (detected_intent === LAST_DETECTED_INTENTS.CONTRACT_READY) {
    return is_spanish
      ? "Perfecto. El siguiente paso es enviarte el contrato y confirmar cualquier detalle final antes de firmar."
      : "Perfect. The next step is getting the contract over to you and confirming any final details before signing.";
  }

  if (
    detected_intent === LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE ||
    current_seller_state === SELLER_STATES.ABOVE_RANGE
  ) {
    return is_spanish
      ? `Entiendo. Para ver si podemos cerrar la diferencia, ¿me puedes contar un poco de la condición y el plazo que tienes en mente?`
      : "I understand. To see if we can close the gap, can you tell me a little more about the condition and the timeline you're working with?";
  }

  if (detected_intent === LAST_DETECTED_INTENTS.WANTS_OFFER) {
    return is_spanish
      ? "Claro. Antes de darte un rango serio, ¿qué me puedes decir sobre la condición y qué tan pronto te gustaría vender?"
      : "Absolutely. Before I give you a serious range, what can you tell me about the condition and how soon you'd want to sell?";
  }

  if (conversation_stage === CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION) {
    return is_spanish
      ? "Gracias por confirmarlo. Si el número tuviera sentido, ¿estarías abierto a ver una oferta?"
      : "Thanks for confirming that. If the number made sense, would you be open to looking at an offer?";
  }

  if (conversation_stage === CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY) {
    return is_spanish
      ? "Perfecto. ¿Cómo está la propiedad ahora y qué plazo te gustaría para vender?"
      : "Got it. How is the property in its current condition, and what kind of timeline would you want to sell on?";
  }

  if (
    [
      DEAL_STRATEGY_BRANCHES.SELLER_FINANCE,
      DEAL_STRATEGY_BRANCHES.SUBJECT_TO,
      DEAL_STRATEGY_BRANCHES.NOVATION,
      DEAL_STRATEGY_BRANCHES.LEASE_OPTION,
      DEAL_STRATEGY_BRANCHES.HYBRID,
    ].includes(deal_strategy_branch)
  ) {
    return is_spanish
      ? "Si el efectivo no encaja, puede haber otra estructura que funcione mejor. ¿Qué resultado necesitas para sentirte bien con el trato?"
      : "If straight cash doesn't fit, there may be another structure that works better. What outcome do you need to feel good about the deal?";
  }

  return is_spanish
    ? "Entendido. Dame un poco más de contexto y te digo cuál sería el mejor siguiente paso desde aquí."
    : "Understood. Give me a little more context and I’ll tell you the best next step from here.";
}

export function buildInboundConversationState({
  context = null,
  classification = null,
  route = null,
  message = "",
  signals: signals_raw = {},
} = {}) {
  // Accept both the inner signals object and the extractUnderwritingSignals wrapper
  const signals =
    signals_raw &&
    typeof signals_raw === "object" &&
    signals_raw.signals &&
    typeof signals_raw.signals === "object"
      ? signals_raw.signals
      : (signals_raw || {});
  const previous_stage = normalizeLockedConversationStage(
    context?.summary?.conversation_stage,
    CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION
  );
  const detected_intent = deriveDeterministicIntent({
    classification,
    message,
    signals,
  });
  const pricing_context = computePricingContext({
    context,
    seller_ask_price: signals.asking_price,
  });
  const creative_branch_eligibility = deriveCreativeEligibility({
    intent: detected_intent,
    message,
    signals,
    price_gap_to_target: pricing_context.price_gap_to_target,
  });
  const deal_strategy_branch = deriveDealStrategyBranch({
    intent: detected_intent,
    message,
    signals,
    creative_branch_eligibility,
  });
  const stage_candidate = deriveStageCandidate({
    intent: detected_intent,
    signals,
    price_gap_to_target: pricing_context.price_gap_to_target,
    previous_stage,
  });
  const conversation_stage = chooseProgressedStage(previous_stage, stage_candidate);
  const current_seller_state = deriveSellerState({
    intent: detected_intent,
    stage: conversation_stage,
    price_gap_to_target: pricing_context.price_gap_to_target,
    signals,
  });
  const current_conversation_branch = deriveConversationBranch({
    intent: detected_intent,
    stage: conversation_stage,
    seller_state: current_seller_state,
  });
  const seller_emotional_tone = mapEmotionToTone(classification?.emotion);
  const primary_objection_type = derivePrimaryObjectionType(classification);
  const risk_flags_ai = deriveRiskFlags({
    message,
    classification,
    intent: detected_intent,
    price_gap_to_target: pricing_context.price_gap_to_target,
    seller_emotional_tone,
    primary_objection_type,
  });
  const follow_up_trigger_state = deriveFollowUpTriggerState({
    detected_intent,
    risk_flags: risk_flags_ai,
  });
  const status_ai_managed = deriveAiManagedStatus({
    detected_intent,
    conversation_stage,
    risk_flags: risk_flags_ai,
  });
  const deal_priority_tag = deriveDealPriorityTag({
    intent: detected_intent,
    conversation_stage,
    price_gap_to_target: pricing_context.price_gap_to_target,
    motivation_score:
      finiteNumber(classification?.motivation_score) ??
      finiteNumber(context?.summary?.motivation_score) ??
      null,
    signals,
  });
  const last_message_summary_ai = deriveLastMessageSummary({
    message,
    detected_intent,
    conversation_stage,
    current_seller_state,
    seller_ask_price: pricing_context.seller_ask_price,
    price_gap_to_target: pricing_context.price_gap_to_target,
  });
  const full_conversation_summary_ai = deriveFullConversationSummary({
    context,
    conversation_stage,
    current_conversation_branch,
    current_seller_state,
    last_detected_intent: detected_intent,
    deal_strategy_branch,
    deal_priority_tag,
    seller_ask_price: pricing_context.seller_ask_price,
    cash_offer_target: pricing_context.cash_offer_target,
    risk_flags: risk_flags_ai,
  });
  const ai_recommended_next_move = deriveAiRecommendedNextMove({
    detected_intent,
    conversation_stage,
    current_seller_state,
    creative_branch_eligibility,
    deal_strategy_branch,
    price_gap_to_target: pricing_context.price_gap_to_target,
    risk_flags: risk_flags_ai,
  });
  const ai_next_message = deriveAiNextMessage({
    detected_intent,
    conversation_stage,
    current_seller_state,
    deal_strategy_branch,
    language_preference:
      clean(classification?.language) || context?.summary?.language_preference || "English",
  });

  return {
    lifecycle_stage_number: getConversationStageNumber(conversation_stage),
    conversation_stage,
    current_conversation_branch,
    current_seller_state,
    follow_up_step: FOLLOW_UP_STEPS.NONE,
    next_follow_up_due_at: null,
    last_detected_intent: detected_intent,
    seller_profile: route?.seller_profile || context?.summary?.seller_profile || null,
    language_preference:
      clean(classification?.language) || context?.summary?.language_preference || "English",
    status_ai_managed,
    deal_priority_tag,
    seller_motivation_score:
      finiteNumber(classification?.motivation_score) ??
      finiteNumber(context?.summary?.motivation_score) ??
      null,
    seller_emotional_tone,
    response_style_mode: deriveResponseStyleMode({ message, classification }),
    primary_objection_type,
    seller_ask_price: pricing_context.seller_ask_price,
    cash_offer_target: pricing_context.cash_offer_target,
    price_gap_to_target: pricing_context.price_gap_to_target,
    creative_branch_eligibility,
    deal_strategy_branch,
    risk_flags_ai,
    follow_up_trigger_state,
    last_message_summary_ai,
    full_conversation_summary_ai,
    ai_recommended_next_move,
    ai_next_message,
    should_use_ai_assist: shouldUseAiConversationAssist({
      classification,
      message,
      signals,
    }),
  };
}

export function buildOutboundFollowUpState({
  conversation_stage = CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  current_follow_up_step = FOLLOW_UP_STEPS.NONE,
  status_ai_managed = null,
  now = new Date().toISOString(),
} = {}) {
  const normalized_stage = normalizeLockedConversationStage(conversation_stage);
  const stage_number = getConversationStageNumber(normalized_stage);
  const next_step = advanceFollowUpStep(current_follow_up_step);

  if (stage_number >= 10) {
    return {
      follow_up_step: FOLLOW_UP_STEPS.NONE,
      next_follow_up_due_at: null,
      follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.COMPLETED,
      status_ai_managed: AI_MANAGED_STATUSES.CLOSED,
    };
  }

  const schedule = FOLLOW_UP_DELAYS_BY_STAGE_NUMBER[stage_number] || FOLLOW_UP_DELAYS_BY_STAGE_NUMBER[1];
  const delay_days = schedule[next_step] || schedule.Final || 7;
  const normalized_status = lower(status_ai_managed);
  const next_status =
    [
      lower(AI_MANAGED_STATUSES.HOT_OPPORTUNITY),
      lower(AI_MANAGED_STATUSES.ACTIVE_NEGOTIATION),
      lower(AI_MANAGED_STATUSES.UNDER_CONTRACT),
      lower(AI_MANAGED_STATUSES.DNC),
      lower(AI_MANAGED_STATUSES.WRONG_NUMBER),
      lower(AI_MANAGED_STATUSES.CLOSED),
      lower(AI_MANAGED_STATUSES.PAUSED),
      lower(AI_MANAGED_STATUSES.MANUAL_REVIEW),
    ].includes(normalized_status)
      ? status_ai_managed
      : next_step === FOLLOW_UP_STEPS.FINAL
        ? AI_MANAGED_STATUSES.COLD_NO_RESPONSE
        : AI_MANAGED_STATUSES.WAITING_ON_SELLER;

  return {
    follow_up_step: next_step,
    next_follow_up_due_at: toPodioDateField(addDays(now, delay_days)),
    follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.WAITING,
    status_ai_managed: next_status,
  };
}

function normalizeExecutionMilestone(value = "") {
  const normalized = lower(value);

  return (
    Object.values(EXECUTION_BRAIN_MILESTONES).find(
      (milestone) => milestone === normalized
    ) || null
  );
}

function isProtectedExecutionTerminalState(current_state = {}) {
  const normalized_status = lower(current_state?.status_ai_managed);
  const normalized_seller_state = lower(current_state?.current_seller_state);

  return (
    [lower(AI_MANAGED_STATUSES.DNC), lower(AI_MANAGED_STATUSES.WRONG_NUMBER)].includes(
      normalized_status
    ) ||
    [lower(SELLER_STATES.DNC), lower(SELLER_STATES.WRONG_NUMBER)].includes(
      normalized_seller_state
    )
  );
}

function isClosedExecutionTerminalState(current_state = {}) {
  return (
    lower(current_state?.status_ai_managed) === lower(AI_MANAGED_STATUSES.CLOSED) ||
    lower(current_state?.current_seller_state) === lower(SELLER_STATES.CLOSED)
  );
}

function compactRisks(value) {
  if (Array.isArray(value)) return compactList(value);
  if (clean(value)) return compactList([value]);
  return [];
}

function appendExecutionSummary(existing_summary = "", next_summary = "") {
  const prior = clean(existing_summary);
  const next = clean(next_summary);

  if (!next) return prior || "";
  if (!prior) return next;
  if (lower(prior).includes(lower(next))) return prior;

  return `${prior} ${next}`.slice(0, 900).trim();
}

function mapExecutionMilestoneTarget(milestone) {
  switch (milestone) {
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_CREATED:
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_SENT:
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_VIEWED:
      return {
        conversation_stage: CONVERSATION_STAGES.CONTRACT_OUT,
        current_conversation_branch: CONVERSATION_BRANCHES.CONTRACT_PUSH,
        current_seller_state: SELLER_STATES.READY_FOR_CONTRACT,
        last_detected_intent: LAST_DETECTED_INTENTS.CONTRACT_READY,
        status_ai_managed: AI_MANAGED_STATUSES.WAITING_ON_SELLER,
        deal_priority_tag: "High Priority",
        follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.WAITING,
        follow_up_step: FOLLOW_UP_STEPS.NONE,
        next_follow_up_due_at: null,
      };
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_SIGNED:
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_FULLY_EXECUTED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_ROUTED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_OPENED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_REVIEWING:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_DOCS:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PROBATE:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PAYOFF:
    case EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_SCHEDULED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_CONFIRMED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_PENDING_DOCS:
      return {
        conversation_stage: CONVERSATION_STAGES.SIGNED_CLOSING,
        current_conversation_branch: CONVERSATION_BRANCHES.CONTRACT_PUSH,
        current_seller_state: SELLER_STATES.SIGNED,
        last_detected_intent: LAST_DETECTED_INTENTS.CONTRACT_READY,
        status_ai_managed: AI_MANAGED_STATUSES.UNDER_CONTRACT,
        deal_priority_tag:
          [
            EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE,
            EXECUTION_BRAIN_MILESTONES.CLOSING_SCHEDULED,
            EXECUTION_BRAIN_MILESTONES.CLOSING_CONFIRMED,
          ].includes(milestone)
            ? "Urgent"
            : "High Priority",
        follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.PAUSED,
        follow_up_step: FOLLOW_UP_STEPS.NONE,
        next_follow_up_due_at: null,
      };
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_SELLER:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_BUYER:
      return {
        conversation_stage: CONVERSATION_STAGES.SIGNED_CLOSING,
        current_conversation_branch: CONVERSATION_BRANCHES.CONTRACT_PUSH,
        current_seller_state: SELLER_STATES.SIGNED,
        last_detected_intent: LAST_DETECTED_INTENTS.CONTRACT_READY,
        status_ai_managed: AI_MANAGED_STATUSES.UNDER_CONTRACT,
        deal_priority_tag: "High Priority",
        follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.WAITING,
        follow_up_step: FOLLOW_UP_STEPS.NONE,
        next_follow_up_due_at: null,
      };
    case EXECUTION_BRAIN_MILESTONES.TITLE_CLOSED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_COMPLETED:
    case EXECUTION_BRAIN_MILESTONES.REVENUE_CONFIRMED:
      return {
        conversation_stage: CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
        current_conversation_branch: CONVERSATION_BRANCHES.DEAD_LEAD_HANDLING,
        current_seller_state: SELLER_STATES.CLOSED,
        status_ai_managed: AI_MANAGED_STATUSES.CLOSED,
        deal_priority_tag: "Low Priority",
        follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.COMPLETED,
        follow_up_step: FOLLOW_UP_STEPS.NONE,
        next_follow_up_due_at: null,
      };
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_CANCELLED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_CANCELLED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_CANCELLED:
      return {
        conversation_stage: CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
        current_conversation_branch: CONVERSATION_BRANCHES.DEAD_LEAD_HANDLING,
        current_seller_state: SELLER_STATES.DEAD,
        status_ai_managed: AI_MANAGED_STATUSES.PAUSED,
        deal_priority_tag: "Low Priority",
        follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.COMPLETED,
        follow_up_step: FOLLOW_UP_STEPS.NONE,
        next_follow_up_due_at: null,
      };
    default:
      return null;
  }
}

function describeExecutionMilestone(milestone) {
  switch (milestone) {
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_CREATED:
      return "Contract created.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_SENT:
      return "Contract sent for signature.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_VIEWED:
      return "Contract viewed but not yet signed.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_SIGNED:
      return "Seller signature completed.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_FULLY_EXECUTED:
      return "Contract fully executed.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_CANCELLED:
      return "Contract cancelled.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_ROUTED:
      return "Deal routed to title.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_OPENED:
      return "Title file opened.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_REVIEWING:
      return "Title reviewing file and curative items.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_DOCS:
      return "Title waiting on docs.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PROBATE:
      return "Title waiting on probate items.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PAYOFF:
      return "Title waiting on payoff information.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_SELLER:
      return "Title waiting on seller items.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_BUYER:
      return "Title waiting on buyer items.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE:
      return "File is clear to close.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_CLOSED:
      return "Title confirmed the deal closed.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_CANCELLED:
      return "Title marked the file cancelled.";
    case EXECUTION_BRAIN_MILESTONES.CLOSING_SCHEDULED:
      return "Closing scheduled.";
    case EXECUTION_BRAIN_MILESTONES.CLOSING_CONFIRMED:
      return "Closing confirmed / clear to close.";
    case EXECUTION_BRAIN_MILESTONES.CLOSING_PENDING_DOCS:
      return "Closing waiting on documents.";
    case EXECUTION_BRAIN_MILESTONES.CLOSING_COMPLETED:
      return "Closing completed successfully.";
    case EXECUTION_BRAIN_MILESTONES.CLOSING_CANCELLED:
      return "Closing cancelled permanently.";
    case EXECUTION_BRAIN_MILESTONES.REVENUE_CONFIRMED:
      return "Revenue confirmed after close.";
    default:
      return "Execution milestone recorded.";
  }
}

function deriveExecutionRiskFlags({
  milestone,
  note = "",
  current_risk_flags = [],
} = {}) {
  const next_flags = compactRisks(current_risk_flags);
  const normalized_note = lower(note);

  if (
    [
      EXECUTION_BRAIN_MILESTONES.CONTRACT_CANCELLED,
      EXECUTION_BRAIN_MILESTONES.TITLE_CANCELLED,
      EXECUTION_BRAIN_MILESTONES.CLOSING_CANCELLED,
    ].includes(milestone) &&
    includesAny(normalized_note, [
      "seller backed out",
      "seller canceled",
      "seller cancelled",
      "no response",
      "ghosted",
    ])
  ) {
    next_flags.push("Seller Hesitation");
  }

  if (
    includesAny(normalized_note, [
      "attorney",
      "lawyer",
      "legal",
      "cease and desist",
    ])
  ) {
    next_flags.push("Legal Threat");
  }

  return compactList(next_flags);
}

function deriveExecutionRecommendation({
  milestone,
  note = "",
  conversation_stage,
  current_seller_state,
} = {}) {
  const detail = clean(note);

  switch (milestone) {
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_CREATED:
      return "Send the contract immediately, confirm delivery, and keep the thread focused on signature completion.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_SENT:
      return "Follow up for signature completion and keep the seller moving through the contract step without reopening negotiation.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_VIEWED:
      return "Stay in contract follow-up mode and push for signature completion without regressing the seller thread.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_SIGNED:
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_FULLY_EXECUTED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_ROUTED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_OPENED:
      return "Coordinate title and closing logistics, confirm any remaining signatures or docs, and keep automation from sending generic nurture messages.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_REVIEWING:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_DOCS:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PROBATE:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PAYOFF:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_SELLER:
    case EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_BUYER:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_PENDING_DOCS:
      return detail
        ? `Work the active execution blocker: ${detail}`
        : "Work the active title or docs blocker and keep the thread in execution mode until the file is clear to close.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_SCHEDULED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_CONFIRMED:
      return "Treat the thread as urgent, confirm the exact close timeline, and keep all parties aligned through the finish line.";
    case EXECUTION_BRAIN_MILESTONES.TITLE_CLOSED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_COMPLETED:
    case EXECUTION_BRAIN_MILESTONES.REVENUE_CONFIRMED:
      return "Archive the seller thread as a closed success and only surface post-close or referral follow-up if ops explicitly wants it.";
    case EXECUTION_BRAIN_MILESTONES.CONTRACT_CANCELLED:
    case EXECUTION_BRAIN_MILESTONES.TITLE_CANCELLED:
    case EXECUTION_BRAIN_MILESTONES.CLOSING_CANCELLED:
      return detail
        ? `Stop seller automation, preserve the dead-outcome reason, and leave the thread paused: ${detail}`
        : "Stop seller automation, preserve the dead-outcome reason, and leave the thread paused unless a human intentionally reopens it.";
    default:
      return `Keep the seller thread in ${conversation_stage || "execution"} mode and preserve the current ${current_seller_state || "seller"} state.`;
  }
}

export function buildExecutionConversationState({
  milestone = null,
  current_state = {},
  note = "",
} = {}) {
  const normalized_milestone = normalizeExecutionMilestone(milestone);

  if (!normalized_milestone) {
    return {
      blocked_reason: "missing_execution_milestone",
    };
  }

  if (isProtectedExecutionTerminalState(current_state)) {
    return {
      blocked_reason: "protected_terminal_state",
    };
  }

  const current_stage = normalizeLockedConversationStage(
    current_state?.conversation_stage,
    CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION
  );
  const current_stage_number =
    finiteNumber(current_state?.lifecycle_stage_number) ??
    getConversationStageNumber(current_stage);
  const target = mapExecutionMilestoneTarget(normalized_milestone);

  if (!target) {
    return {
      blocked_reason: "unsupported_execution_milestone",
    };
  }

  const target_stage_number = getConversationStageNumber(target.conversation_stage);
  const should_preserve_later_stage =
    current_stage_number > target_stage_number && target_stage_number < 10;
  const should_preserve_closed_terminal =
    isClosedExecutionTerminalState(current_state) &&
    target.current_seller_state === SELLER_STATES.DEAD;

  const preserve_stage_truth =
    should_preserve_later_stage || should_preserve_closed_terminal;

  let conversation_stage = target.conversation_stage;
  let lifecycle_stage_number = target_stage_number;
  let current_conversation_branch = target.current_conversation_branch;
  let current_seller_state = target.current_seller_state;
  let status_ai_managed = target.status_ai_managed;
  let follow_up_trigger_state = target.follow_up_trigger_state;
  let deal_priority_tag = target.deal_priority_tag;

  if (preserve_stage_truth) {
    conversation_stage = current_stage;
    lifecycle_stage_number = current_stage_number;
    current_conversation_branch =
      clean(current_state?.current_conversation_branch) ||
      current_conversation_branch;
    current_seller_state =
      clean(current_state?.current_seller_state) || current_seller_state;
    status_ai_managed =
      clean(current_state?.status_ai_managed) || status_ai_managed;
    follow_up_trigger_state =
      clean(current_state?.follow_up_trigger_state) || follow_up_trigger_state;
    deal_priority_tag =
      clean(current_state?.deal_priority_tag) || deal_priority_tag;
  } else if (target_stage_number < 10) {
    const normalized_status = lower(current_state?.status_ai_managed);
    const normalized_trigger = lower(current_state?.follow_up_trigger_state);

    if (normalized_status === lower(AI_MANAGED_STATUSES.MANUAL_REVIEW)) {
      status_ai_managed = AI_MANAGED_STATUSES.MANUAL_REVIEW;
      follow_up_trigger_state =
        normalized_trigger === lower(FOLLOW_UP_TRIGGER_STATES.MANUAL_OVERRIDE)
          ? FOLLOW_UP_TRIGGER_STATES.MANUAL_OVERRIDE
          : FOLLOW_UP_TRIGGER_STATES.PAUSED;
    } else if (normalized_status === lower(AI_MANAGED_STATUSES.PAUSED)) {
      status_ai_managed = AI_MANAGED_STATUSES.PAUSED;
      follow_up_trigger_state = FOLLOW_UP_TRIGGER_STATES.PAUSED;
    }
  }

  const summary_prefix = preserve_stage_truth
    ? `Execution milestone received without stage regression. Brain stayed in Stage ${lifecycle_stage_number} ${conversation_stage}.`
    : `Execution milestone moved Brain to Stage ${lifecycle_stage_number} ${conversation_stage}.`;
  const milestone_summary = `${summary_prefix} ${describeExecutionMilestone(normalized_milestone)}${
    clean(note) ? ` ${clean(note)}` : ""
  }`.trim();
  const risk_flags_ai = deriveExecutionRiskFlags({
    milestone: normalized_milestone,
    note,
    current_risk_flags: current_state?.risk_flags_ai,
  });

  return {
    lifecycle_stage_number,
    conversation_stage,
    current_conversation_branch,
    current_seller_state,
    follow_up_step: FOLLOW_UP_STEPS.NONE,
    next_follow_up_due_at: null,
    last_detected_intent:
      lifecycle_stage_number < 10
        ? LAST_DETECTED_INTENTS.CONTRACT_READY
        : clean(current_state?.last_detected_intent) || undefined,
    status_ai_managed,
    deal_priority_tag,
    risk_flags_ai: risk_flags_ai.length ? risk_flags_ai : undefined,
    follow_up_trigger_state,
    last_message_summary_ai: milestone_summary,
    full_conversation_summary_ai: appendExecutionSummary(
      current_state?.full_conversation_summary_ai,
      milestone_summary
    ),
    ai_recommended_next_move: deriveExecutionRecommendation({
      milestone: normalized_milestone,
      note,
      conversation_stage,
      current_seller_state,
    }),
  };
}

export function deriveQueueCurrentStage({
  route_stage = null,
  conversation_stage = null,
  use_case = null,
} = {}) {
  const route_label = clean(route_stage);
  if (route_label && CONVERSATION_STAGE_LIST.includes(route_label)) {
    return route_label;
  }

  const normalized_conversation_stage = normalizeLockedConversationStage(
    conversation_stage,
    null
  );
  if (normalized_conversation_stage) return normalized_conversation_stage;

  const normalized_use_case = lower(use_case);

  if (normalized_use_case.includes("ownership")) {
    return CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION;
  }
  if (
    normalized_use_case.includes("consider_selling") ||
    normalized_use_case.includes("offer_interest")
  ) {
    return CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION;
  }
  if (normalized_use_case.includes("asking_price")) {
    return CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY;
  }
  if (
    normalized_use_case.includes("condition") ||
    normalized_use_case.includes("timeline")
  ) {
    return CONVERSATION_STAGES.CONDITION_TIMELINE_DISCOVERY;
  }
  if (normalized_use_case.includes("offer_reveal")) {
    return CONVERSATION_STAGES.OFFER_POSITIONING;
  }
  if (
    normalized_use_case.includes("justify_price") ||
    normalized_use_case.includes("narrow_range")
  ) {
    return CONVERSATION_STAGES.NEGOTIATION;
  }
  if (
    normalized_use_case.includes("close_handoff") ||
    normalized_use_case.includes("contract")
  ) {
    return CONVERSATION_STAGES.CONTRACT_OUT;
  }

  return CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION;
}

export function buildBrainCreateDefaults({
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  phone_link_enabled = false,
} = {}) {
  return {
    "master-owner": master_owner_id,
    prospect: prospect_id,
    "conversation-stage": CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
    number: 1,
    "ai-route": CONVERSATION_BRANCHES.OWNERSHIP_CONFIRMATION,
    "current-seller-state": SELLER_STATES.UNCONFIRMED_OWNER,
    "follow-up-step": FOLLOW_UP_STEPS.NONE,
    "last-detected-intent": LAST_DETECTED_INTENTS.UNKNOWN,
    "seller-profile": "Unknown",
    "language-preference": "English",
    "status-ai-managed": AI_MANAGED_STATUSES.WARM_LEAD,
    "follow-up-trigger-state": FOLLOW_UP_TRIGGER_STATES.WAITING,
    "last-contact-timestamp": toPodioDateField(new Date()),
    "category": SELLER_EMOTIONAL_TONES.UNKNOWN,
    "category-2": RESPONSE_STYLE_MODES.UNKNOWN,
    "category-3": PRIMARY_OBJECTION_TYPES.UNKNOWN,
    "category-4": CREATIVE_BRANCH_ELIGIBILITY.UNKNOWN,
    "category-5": DEAL_STRATEGY_BRANCHES.CASH,
    ...(property_id ? { properties: [property_id] } : {}),
    ...(phone_link_enabled && phone_item_id ? { "phone-number": phone_item_id } : {}),
  };
}
