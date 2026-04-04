// ─── resolve-route.js ────────────────────────────────────────────────────
import {
  getCategoryValue,
  normalizeLanguage,
  normalizeStage,
  safeCategoryEquals,
} from "@/lib/providers/podio.js";
import {
  LIFECYCLE_STAGES,
  STAGES,
  collapseLifecycleStage,
  normalizeLifecycleStage,
} from "@/lib/config/stages.js";

// ─────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function hasAny(value, list = []) {
  const text = lower(value);
  return list.some((item) => text.includes(lower(item)));
}

function normalizePersona(value) {
  const raw = clean(value);

  const allowed = [
    "Warm Professional",
    "No-Nonsense Closer",
    "Neighborly",
    "Empathetic",
    "Investor Direct",
    "Specialist-Spanish",
    "Specialist-Probate",
    "Specialist-Corporate",
    "Specialist-Landlord",
    "Specialist-Portuguese",
    "Specialist-Italian",
    "Specialist-Hebrew",
    "Specialist-Mandarin",
    "Specialist-Korean",
    "Specialist-Vietnamese",
    "Specialist-Polish",
  ];

  return allowed.includes(raw) ? raw : "Warm Professional";
}

const TITLE_USE_CASES = new Set([
  "title_intro",
  "title_by_text_update",
  "title_company",
  "seller_docs_needed",
  "probate_doc_needed",
  "title_delay_followup",
  "title_issue_soft",
  "title_issue_discovered",
  "lien_issue_detected",
]);

const CLOSING_USE_CASES = new Set([
  "clear_to_close",
  "day_before_close",
  "closing_date_moved",
  "closing_date_locked",
  "earnest_pending",
  "closing_timeline",
  "walkthrough_or_condition",
  "close_handoff",
]);

const DISPOSITION_USE_CASES = new Set([
  "disposition_access_coordination",
  "disposition_marketing_update",
  "buyer_referral_transition",
]);

const POST_CLOSE_USE_CASES = new Set([
  "post_close_referral",
]);

const CONTRACT_USE_CASES = new Set([
  "asks_contract",
  "contract_sent",
  "contract_not_signed_followup",
  "send_package",
  "proof_of_funds",
  "email_for_docs",
]);

const USE_CASE_ROUTE_OVERRIDES = Object.freeze({
  call_me_later_redirect: Object.freeze({
    category: "Residential",
    secondary_category: "Identity / Trust",
    variant_group: "SMS-Only Preference",
    tone: "Neutral",
  }),
  asks_contract: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Contract Request",
    tone: "Direct",
  }),
  contract_sent: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Contract Sent",
    tone: "Neutral",
  }),
  contract_not_signed_followup: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Contract Unsigned Follow-Up",
    tone: "Warm",
  }),
  proof_of_funds: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Proof of Funds",
    tone: "Neutral",
  }),
  email_for_docs: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "SMS-Only Docs Exchange",
    tone: "Neutral",
  }),
  send_package: Object.freeze({
    category: "Corporate / Institutional",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Package Send",
    tone: "Corporate",
  }),
  title_intro: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title Intro",
    tone: "Neutral",
  }),
  title_by_text_update: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title / Closing",
    tone: "Neutral",
  }),
  title_company: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title / Closing",
    tone: "Neutral",
  }),
  clear_to_close: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Clear to Close",
    tone: "Direct",
  }),
  day_before_close: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Close Reminder",
    tone: "Warm",
  }),
  seller_docs_needed: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Seller Docs Needed",
    tone: "Direct",
  }),
  probate_doc_needed: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Seller Docs Needed",
    tone: "Calm",
  }),
  title_delay_followup: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title Delay",
    tone: "Neutral",
  }),
  closing_date_moved: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Closing Date Changed",
    tone: "Neutral",
  }),
  closing_date_locked: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Closing Date Locked",
    tone: "Direct",
  }),
  earnest_pending: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Earnest Pending",
    tone: "Neutral",
  }),
  closing_timeline: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Close Timing",
    tone: "Direct",
  }),
  walkthrough_or_condition: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Condition / Walkthrough",
    tone: "Neutral",
  }),
  close_handoff: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Close / Handoff",
    tone: "Direct",
  }),
  title_issue_soft: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title Issue",
    tone: "Calm",
  }),
  title_issue_discovered: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title Issue",
    tone: "Calm",
  }),
  lien_issue_detected: Object.freeze({
    category: "Residential",
    secondary_category: "Close / Handoff",
    variant_group: "Stage 6 — Title Issue",
    tone: "Calm",
  }),
  buyer_referral_transition: Object.freeze({
    category: "Residential",
    secondary_category: "Post-Close",
    variant_group: "Disposition / Referral",
    tone: "Neutral",
  }),
  post_close_referral: Object.freeze({
    category: "Residential",
    secondary_category: "Post-Close",
    variant_group: "Post-Close / Referral",
    tone: "Warm",
  }),
  disposition_access_coordination: Object.freeze({
    category: "Residential",
    secondary_category: "Disposition",
    variant_group: "Disposition - Access Coordination",
    tone: "Neutral",
  }),
  disposition_marketing_update: Object.freeze({
    category: "Residential",
    secondary_category: "Disposition",
    variant_group: "Disposition - Marketing Update",
    tone: "Neutral",
  }),
});

function routeOverrideForUseCase({ use_case, seller_profile = null } = {}) {
  const base = USE_CASE_ROUTE_OVERRIDES[use_case] || null;
  if (!base) return null;

  if (use_case === "close_handoff" && safeCategoryEquals(seller_profile, "Probate")) {
    return {
      ...base,
      category: "Probate / Trust",
      tone: "Soft",
    };
  }

  return base;
}

function isInitialOwnershipOutbound({
  message = "",
  compliance_flag = null,
  objection = null,
  use_case = null,
  stage = null,
  lifecycle_stage = null,
} = {}) {
  return (
    !clean(message) &&
    !compliance_flag &&
    !objection &&
    use_case === "ownership_check" &&
    stage === STAGES.OWNERSHIP &&
    lifecycle_stage === LIFECYCLE_STAGES.OWNERSHIP
  );
}

function isCloseLifecycleStage(lifecycle_stage) {
  return [
    LIFECYCLE_STAGES.TITLE,
    LIFECYCLE_STAGES.CLOSING,
    LIFECYCLE_STAGES.DISPOSITION,
    LIFECYCLE_STAGES.POST_CLOSE,
  ].includes(lifecycle_stage);
}

function genericTemplateAgentType({
  language,
  seller_profile = null,
  primary_category = "Residential",
} = {}) {
  if (language === "Spanish") return "Specialist-Spanish / Market-Local";
  if (language === "Portuguese") {
    return "Specialist-Portuguese / Specialist-Portuguese-Corporate";
  }
  if (language === "Italian") {
    return "Specialist-Italian / Specialist-Italian-Family";
  }
  if (language === "Hebrew") return "Specialist-Hebrew";
  if (language === "Mandarin") return "Specialist-Mandarin";
  if (language === "Korean") return "Specialist-Korean";
  if (language === "Vietnamese") return "Specialist-Vietnamese";
  if (language === "Polish") return "Specialist-Polish";

  if (safeCategoryEquals(seller_profile, "Probate")) return "Specialist-Probate";
  if (
    safeCategoryEquals(seller_profile, "Tired Landlord") ||
    primary_category === "Landlord / Multifamily"
  ) {
    return "Specialist-Landlord / Market-Local";
  }

  return "Fallback / Market-Local";
}

function closeTemplateAgentType({
  language,
  seller_profile = null,
  use_case = null,
} = {}) {
  if (use_case === "send_package") {
    if (language === "Spanish") return "Specialist-Corporate-Spanish";
    return "Specialist-Corporate";
  }

  if (use_case === "close_handoff") {
    return genericTemplateAgentType({
      language,
      seller_profile,
      primary_category: "Residential",
    });
  }

  if (language === "Spanish") return "Specialist-Spanish / Specialist-Close";
  if (language === "Portuguese") return "Specialist-Portuguese / Specialist-Close";
  if (language === "Italian") return "Specialist-Italian / Specialist-Close";
  if (language === "Hebrew") return "Specialist-Hebrew / Specialist-Close";
  if (language === "Mandarin") return "Specialist-Mandarin / Specialist-Close";
  if (language === "Korean") return "Specialist-Korean / Specialist-Close";
  if (language === "Vietnamese") return "Specialist-Vietnamese / Specialist-Close";
  if (language === "Polish") return "Specialist-Polish / Specialist-Close";

  if (
    safeCategoryEquals(seller_profile, "Probate") &&
    (use_case === "probate_doc_needed" || use_case === "lien_issue_detected")
  ) {
    return "Fallback / Market-Local / Specialist-Close / Specialist-Probate";
  }

  return "Fallback / Market-Local / Specialist-Close";
}

function lifecycleStageFromUseCase({ use_case = null, stage = STAGES.OWNERSHIP } = {}) {
  if (TITLE_USE_CASES.has(use_case)) return LIFECYCLE_STAGES.TITLE;
  if (CLOSING_USE_CASES.has(use_case)) return LIFECYCLE_STAGES.CLOSING;
  if (DISPOSITION_USE_CASES.has(use_case)) return LIFECYCLE_STAGES.DISPOSITION;
  if (POST_CLOSE_USE_CASES.has(use_case)) return LIFECYCLE_STAGES.POST_CLOSE;
  if (CONTRACT_USE_CASES.has(use_case)) return LIFECYCLE_STAGES.CONTRACT;
  return normalizeLifecycleStage(stage);
}

function templateAgentTypeForRoute({
  lifecycle_stage,
  use_case,
  language,
  seller_profile = null,
  primary_category = "Residential",
  persona = "Warm Professional",
} = {}) {
  if (isCloseLifecycleStage(lifecycle_stage) || CONTRACT_USE_CASES.has(use_case)) {
    return closeTemplateAgentType({
      language,
      seller_profile,
      use_case,
    });
  }

  return persona || genericTemplateAgentType({
    language,
    seller_profile,
    primary_category,
  });
}

function fallbackTemplateAgentTypeForRoute({
  lifecycle_stage,
  use_case,
  language,
  seller_profile = null,
  primary_category = "Residential",
} = {}) {
  if (isCloseLifecycleStage(lifecycle_stage) || CONTRACT_USE_CASES.has(use_case)) {
    return closeTemplateAgentType({
      language,
      seller_profile,
      use_case,
    });
  }

  return genericTemplateAgentType({
    language,
    seller_profile,
    primary_category,
  });
}

// ─────────────────────────────────────────────────────────────
// PERSONA LOGIC
// ─────────────────────────────────────────────────────────────

function personaFromBrain(brain_item) {
  const seller_profile = clean(getCategoryValue(brain_item, "seller-profile", ""));
  const language = normalizeLanguage(
    getCategoryValue(brain_item, "language-preference", "English")
  );

  if (language === "Spanish") return "Specialist-Spanish";
  if (language === "Portuguese") return "Specialist-Portuguese";
  if (language === "Italian") return "Specialist-Italian";
  if (language === "Hebrew") return "Specialist-Hebrew";
  if (language === "Mandarin") return "Specialist-Mandarin";
  if (language === "Korean") return "Specialist-Korean";
  if (language === "Vietnamese") return "Specialist-Vietnamese";
  if (language === "Polish") return "Specialist-Polish";

  if (safeCategoryEquals(seller_profile, "Probate")) return "Specialist-Probate";
  if (safeCategoryEquals(seller_profile, "Tired Landlord")) return "Specialist-Landlord";
  if (safeCategoryEquals(seller_profile, "Strategic Seller")) return "No-Nonsense Closer";

  return "Warm Professional";
}

function personaFromSignals({
  language,
  objection,
  emotion,
  seller_profile,
}) {
  if (language === "Spanish") return "Specialist-Spanish";
  if (language === "Portuguese") return "Specialist-Portuguese";
  if (language === "Italian") return "Specialist-Italian";
  if (language === "Hebrew") return "Specialist-Hebrew";
  if (language === "Mandarin") return "Specialist-Mandarin";
  if (language === "Korean") return "Specialist-Korean";
  if (language === "Vietnamese") return "Specialist-Vietnamese";
  if (language === "Polish") return "Specialist-Polish";

  if (objection === "probate" || safeCategoryEquals(seller_profile, "Probate")) {
    return "Specialist-Probate";
  }

  if (
    objection === "tenant_issue" ||
    emotion === "tired_landlord" ||
    safeCategoryEquals(seller_profile, "Tired Landlord")
  ) {
    return "Specialist-Landlord";
  }

  if (
    objection === "need_more_money" ||
    objection === "has_other_buyer" ||
    objection === "wants_retail"
  ) {
    return "No-Nonsense Closer";
  }

  if (
    emotion === "overwhelmed" ||
    emotion === "grieving" ||
    emotion === "frustrated"
  ) {
    return "Empathetic";
  }

  if (emotion === "curious") return "Warm Professional";
  if (emotion === "motivated") return "No-Nonsense Closer";
  if (emotion === "tired_landlord") return "Investor Direct";

  return "Warm Professional";
}

// ─────────────────────────────────────────────────────────────
// TONE / VARIANT / STAGE
// ─────────────────────────────────────────────────────────────

function toneFromEmotion(emotion, objection) {
  if (
    objection === "need_more_money" ||
    objection === "send_offer_first" ||
    objection === "has_other_buyer"
  ) {
    return "Direct";
  }

  if (
    objection === "wants_proof_of_funds" ||
    objection === "wants_written_offer" ||
    objection === "who_is_this" ||
    objection === "already_listed"
  ) {
    return "Professional";
  }

  if (objection === "wrong_number") return "Neutral";
  if (objection === "probate" || objection === "divorce") return "Soft";
  if (objection === "financial_distress") return "Calm";

  if (emotion === "frustrated") return "Calm";
  if (emotion === "skeptical") return "Professional";
  if (emotion === "guarded") return "Neutral";
  if (emotion === "curious") return "Warm";
  if (emotion === "motivated") return "Direct";
  if (emotion === "tired_landlord") return "Neutral";
  if (emotion === "overwhelmed" || emotion === "grieving") return "Soft";

  return "Warm";
}

function variantGroupFromStage(stage, objection, emotion, lifecycle_stage = null) {
  const objection_variant_group_map = {
    wrong_number: "Wrong Number / Referral",
    who_is_this: "Stage 1 — Identity / Trust",
    not_interested: "Objection — Not Interested",
    already_listed: "Objection — Already Listed",
    need_more_money: "Objection — Need More Money",
    need_time: "Objection — Need Time",
    need_family_ok: "Objection — Family Approval",
    send_offer_first: "Objection — Send Offer First",
    tenant_issue: "Objection — Tenant Issue",
    condition_bad: "Objection — Bad Condition",
    probate: "Probate / Sensitivity",
    divorce: "Sensitive Situation",
    financial_distress: "Distress / Timing",
    has_other_buyer: "Objection — Already Have Someone",
    wants_retail: "Negotiation — Seller Price",
    needs_call: "Channel Shift",
    needs_email: "Channel Shift",
    wants_written_offer: "Stage 6 — Package Send",
    wants_proof_of_funds: "Stage 6 — Proof of Funds",
  };

  const emotion_variant_group_map = {
    skeptical: "Emotion — Skeptical",
    guarded: "Emotion — Guarded",
    frustrated: "Emotion — Frustrated",
    curious: "Emotion — Curious",
    motivated: "Emotion — Motivated",
    tired_landlord: "Emotion — Tired Landlord",
    overwhelmed: "Emotion — Overwhelmed",
  };

  if (objection_variant_group_map[objection]) {
    return objection_variant_group_map[objection];
  }

  if (emotion_variant_group_map[emotion]) {
    return emotion_variant_group_map[emotion];
  }

  if (lifecycle_stage === LIFECYCLE_STAGES.TITLE) return "Stage 6 — Title Intro";
  if (lifecycle_stage === LIFECYCLE_STAGES.CLOSING) return "Stage 6 — Close / Handoff";
  if (lifecycle_stage === LIFECYCLE_STAGES.DISPOSITION) {
    return "Disposition - Access Coordination";
  }
  if (lifecycle_stage === LIFECYCLE_STAGES.POST_CLOSE) return "Post-Close / Referral";

  if (stage === STAGES.OWNERSHIP) return "Stage 1 — Ownership Confirmation";
  if (stage === STAGES.OFFER) return "Stage 3 — Offer Reveal";
  if (stage === STAGES.QA) return "Human Soft";
  if (stage === STAGES.CONTRACT) return "Stage 6 — Contract Request";
  if (stage === STAGES.FOLLOW_UP) return "Stage 5 — Re-engagement";

  return "Human Soft";
}

function stageFromSignals({ existing_stage, stage_hint, objection, message }) {
  const msg = lower(message);

  if (objection === "wrong_number" || objection === "who_is_this") {
    return STAGES.OWNERSHIP;
  }

  if (
    objection === "send_offer_first" ||
    objection === "need_more_money" ||
    objection === "wants_proof_of_funds" ||
    objection === "has_other_buyer" ||
    objection === "wants_retail"
  ) {
    return STAGES.OFFER;
  }

  if (
    objection === "needs_call" ||
    objection === "needs_email" ||
    objection === "probate" ||
    objection === "divorce" ||
    objection === "wants_written_offer" ||
    objection === "financial_distress"
  ) {
    return STAGES.QA;
  }

  if (
    hasAny(msg, [
      "contract",
      "agreement",
      "docusign",
      "sign",
      "paperwork",
      "title company",
      "closing date",
      "earnest money",
      "escrow",
      "close on",
      "clear to close",
    ])
  ) {
    return STAGES.CONTRACT;
  }

  if (
    hasAny(msg, [
      "offer",
      "price",
      "number",
      "how much",
      "cash offer",
      "best you can do",
      "what can you pay",
      "what will you pay",
    ])
  ) {
    return STAGES.OFFER;
  }

  if (
    hasAny(msg, [
      "later",
      "not now",
      "next week",
      "check back",
      "follow up",
      "circle back",
      "not ready yet",
    ])
  ) {
    return STAGES.FOLLOW_UP;
  }

  if (stage_hint) return collapseLifecycleStage(stage_hint, STAGES.OWNERSHIP);
  return normalizeStage(existing_stage || STAGES.OWNERSHIP);
}

function sequencePositionFromStage(stage, objection, lifecycle_stage = null) {
  if (objection) return "V1";
  if (isCloseLifecycleStage(lifecycle_stage)) return "V1";
  if (stage === STAGES.OWNERSHIP) return "1st Touch";
  if (stage === STAGES.OFFER) return "V1";
  if (stage === STAGES.QA) return "V1";
  if (stage === STAGES.CONTRACT) return "V1";
  if (stage === STAGES.FOLLOW_UP) return "2nd Touch";
  return "V1";
}

// ─────────────────────────────────────────────────────────────
// USE CASE + ROUTE
// ─────────────────────────────────────────────────────────────

function useCaseFromSignals({
  compliance_flag,
  objection,
  stage,
  stage_hint = null,
  emotion,
  message,
  seller_profile = null,
}) {
  const msg = lower(message);
  const is_probate = safeCategoryEquals(seller_profile, "Probate") || objection === "probate";
  const hinted_lifecycle_stage = normalizeLifecycleStage(stage_hint, stage);

  const asks_title_company = hasAny(msg, [
    "who is the title company",
    "what is the title company",
    "which title company",
    "who's the title company",
    "title company info",
  ]);
  const mentions_title = hasAny(msg, [
    "title company",
    "title update",
    "title says",
    "escrow",
    "settlement",
    "closing attorney",
  ]);
  const mentions_title_update = hasAny(msg, [
    "title update",
    "title says",
    "escrow update",
    "escrow says",
    "opened with title",
  ]);
  const mentions_docs = hasAny(msg, [
    "need docs",
    "need documents",
    "send docs",
    "send documents",
    "driver license",
    "photo id",
    "death certificate",
    "letters testamentary",
    "probate docs",
    "payoff",
    "voided check",
    "w9",
    "wire instructions",
  ]);
  const mentions_delay = hasAny(msg, [
    "title delay",
    "closing delay",
    "delayed",
    "hold up",
    "waiting on title",
    "still waiting on title",
    "title is behind",
  ]);
  const mentions_lien = hasAny(msg, [
    "lien",
    "judgment",
    "tax lien",
    "hoa lien",
    "code violation",
    "municipal lien",
  ]);
  const mentions_soft_title_issue =
    hasAny(msg, ["title issue", "issue with title", "title problem", "cloud on title"]) &&
    hasAny(msg, ["working through", "sorting through", "minor", "small", "soft"]);
  const mentions_title_issue = hasAny(msg, [
    "title issue",
    "issue with title",
    "title problem",
    "cloud on title",
    "probate issue",
  ]);
  const mentions_clear_to_close = hasAny(msg, ["clear to close", "ctc"]);
  const mentions_day_before_close = hasAny(msg, [
    "closing tomorrow",
    "close tomorrow",
    "day before close",
    "closing tomorrow morning",
  ]);
  const mentions_closing_moved = hasAny(msg, [
    "closing moved",
    "closing rescheduled",
    "closing pushed",
    "date moved",
    "close moved",
  ]);
  const mentions_closing_locked = hasAny(msg, [
    "closing is set",
    "closing confirmed",
    "closing locked",
    "scheduled to close",
    "closing on",
    "close on",
  ]);
  const mentions_earnest = hasAny(msg, [
    "earnest money",
    "emd",
    "escrow deposit",
    "deposit is due",
  ]);
  const mentions_closing_timeline = hasAny(msg, [
    "closing timeline",
    "when are we closing",
    "when do we close",
    "how long to close",
    "closing timing",
  ]);
  const mentions_walkthrough = hasAny(msg, [
    "walkthrough",
    "walk through",
    "final walk",
    "condition check",
    "access for inspection",
  ]);
  const mentions_disposition_access = hasAny(msg, [
    "showing access",
    "access for showings",
    "lockbox",
    "photo access",
    "access for photos",
    "buyer walkthrough",
    "showing window",
    "schedule a showing",
  ]);
  const mentions_disposition_marketing = hasAny(msg, [
    "marketing update",
    "buyer feedback",
    "buyer traffic",
    "listing traffic",
    "showing feedback",
    "we launched it",
    "we listed it",
  ]);
  const mentions_buyer_referral_transition = hasAny(msg, [
    "buyer referral",
    "end buyer",
    "send this to buyers",
    "move to the buyer side",
  ]);
  const mentions_post_close_referral = hasAny(msg, [
    "anyone else looking to sell",
    "anyone you know",
    "know anyone else",
    "referral bonus",
    "send us a referral",
  ]);
  const asks_proof_of_funds = hasAny(msg, ["proof of funds", "pof"]);
  const asks_email_for_docs =
    hasAny(msg, ["email me", "my email is", "send it to my email", "email address"]) &&
    hasAny(msg, ["doc", "docs", "package", "contract", "offer", "paperwork"]);
  const asks_send_package = hasAny(msg, [
    "send the package",
    "send the offer",
    "send paperwork",
    "send the docs",
    "send it over",
  ]);
  const mentions_contract_sent = hasAny(msg, [
    "signed",
    "sent back",
    "done signing",
    "completed signing",
    "signed it",
  ]);
  const mentions_contract = hasAny(msg, [
    "contract",
    "agreement",
    "docusign",
    "paperwork",
  ]);
  const asks_call_later = hasAny(msg, [
    "call me later",
    "text me later",
    "reach out later",
    "circle back later",
  ]);

  if (compliance_flag === "stop_texting") return "not_interested";

  if (objection) {
    const objection_map = {
      wrong_number: "wrong_person",
      who_is_this: "who_is_this",
      not_interested: "not_interested",
      already_listed: "already_listed",
      need_more_money: "justify_price",
      need_time: "not_ready",
      need_family_ok: "family_discussion",
      send_offer_first: "offer_reveal_cash",
      tenant_issue: "has_tenants",
      condition_bad: "condition_question_set",
      probate: "death_sensitivity",
      divorce: "divorce_sensitivity",
      financial_distress: "foreclosure_pressure",
      has_other_buyer: "narrow_range",
      wants_retail: "seller_asking_price",
      needs_call: "call_me_later_redirect",
      needs_email: "email_for_docs",
      wants_written_offer: "send_package",
      wants_proof_of_funds: "proof_of_funds",
    };

    return objection_map[objection] || "send_info";
  }

  if (asks_call_later) return "call_me_later_redirect";
  if (mentions_post_close_referral) return "post_close_referral";
  if (mentions_buyer_referral_transition) return "buyer_referral_transition";
  if (mentions_disposition_access) return "disposition_access_coordination";
  if (mentions_disposition_marketing) return "disposition_marketing_update";
  if (mentions_clear_to_close) return "clear_to_close";
  if (mentions_day_before_close) return "day_before_close";
  if (mentions_closing_moved) return "closing_date_moved";
  if (mentions_closing_locked) return "closing_date_locked";
  if (mentions_earnest) return "earnest_pending";
  if (mentions_closing_timeline) return "closing_timeline";
  if (mentions_walkthrough) return "walkthrough_or_condition";
  if (asks_title_company) return "title_company";
  if (mentions_lien) return "lien_issue_detected";
  if (mentions_docs) return is_probate ? "probate_doc_needed" : "seller_docs_needed";
  if (mentions_delay) return "title_delay_followup";
  if (mentions_soft_title_issue) return "title_issue_soft";
  if (mentions_title_issue) return "title_issue_discovered";
  if (mentions_title_update) return "title_by_text_update";
  if (asks_proof_of_funds) return "proof_of_funds";
  if (asks_email_for_docs) return "email_for_docs";
  if (asks_send_package) return "send_package";
  if (hinted_lifecycle_stage === LIFECYCLE_STAGES.TITLE) return "title_intro";
  if (hinted_lifecycle_stage === LIFECYCLE_STAGES.CLOSING) return "closing_timeline";
  if (hinted_lifecycle_stage === LIFECYCLE_STAGES.DISPOSITION) {
    return "disposition_access_coordination";
  }
  if (hinted_lifecycle_stage === LIFECYCLE_STAGES.POST_CLOSE) {
    return "post_close_referral";
  }
  if (stage === STAGES.OWNERSHIP) return "ownership_check";
  if (stage === STAGES.OFFER) return "offer_reveal_cash";

  if (stage === STAGES.CONTRACT) {
    if (mentions_contract_sent) return "contract_sent";
    if (mentions_title) return "title_intro";
    if (mentions_contract) return "asks_contract";
    return "contract_not_signed_followup";
  }

  if (stage === STAGES.FOLLOW_UP) return "reengagement";

  if (emotion === "curious") return "send_info";
  if (emotion === "motivated") return "best_price";
  if (emotion === "tired_landlord") return "pain_probe";
  if (emotion === "overwhelmed") return "send_info";
  if (emotion === "grieving") return "death_sensitivity";

  return "send_info";
}

function aiRouteFromSignals({ compliance_flag, objection, emotion, stage, seller_profile }) {
  if (compliance_flag === "stop_texting") return "Soft";

  if (safeCategoryEquals(seller_profile, "Probate")) return "Soft";
  if (safeCategoryEquals(seller_profile, "Tired Landlord")) return "Deep Motivational";
  if (safeCategoryEquals(seller_profile, "Strategic Seller")) return "Aggressive";

  if (objection === "need_more_money") return "Aggressive";
  if (objection === "send_offer_first") return "Quick Offer";
  if (objection === "has_other_buyer") return "Aggressive";
  if (objection === "wants_retail") return "Deep Motivational";
  if (objection === "financial_distress") return "Quick Offer";
  if (objection === "wants_proof_of_funds") return "Quick Offer";
  if (objection === "not_interested") return "Soft";
  if (objection === "who_is_this") return "Soft";
  if (objection === "already_listed") return "Soft";
  if (objection === "probate") return "Soft";
  if (objection === "divorce") return "Soft";

  if (emotion === "motivated") return "Quick Offer";
  if (
    emotion === "skeptical" ||
    emotion === "frustrated" ||
    emotion === "overwhelmed" ||
    emotion === "grieving"
  ) {
    return "Soft";
  }

  if (emotion === "tired_landlord") return "Deep Motivational";
  if (stage === STAGES.OFFER) return "Quick Offer";

  return "Soft";
}

function nextMoveFromSignals({ compliance_flag, objection, stage, emotion, use_case }) {
  if (compliance_flag === "stop_texting") {
    return "Confirm opt-out, suppress future outreach, mark phone as DNC.";
  }

  if (objection === "wrong_number") {
    return "Acknowledge wrong number, do not continue pitch, mark lineage if needed.";
  }

  if (objection === "who_is_this") {
    return "Lead with identity/trust clarification before discussing price.";
  }

  if (objection === "need_more_money") {
    return "Re-anchor value, gather seller expectation, determine cash vs creative fit.";
  }

  if (objection === "need_family_ok") {
    return "Keep pressure low, give clean summary seller can share with decision-makers.";
  }

  if (objection === "tenant_issue") {
    return "Acknowledge occupancy, confirm whether seller wants to sell as-is with tenants.";
  }

  if (objection === "financial_distress") {
    return "Move toward timeline, pain point, and fast-close fit.";
  }

  if (objection === "wants_proof_of_funds") {
    return "Provide legitimacy and proof path, then move back toward offer/contract.";
  }

  if (objection === "needs_email") {
    return "Get best email and send the requested written info package.";
  }

  if (objection === "needs_call") {
    return "Acknowledge preference and decide whether to park, redirect, or escalate.";
  }

  if (use_case === "title_intro" || TITLE_USE_CASES.has(use_case)) {
    return "Keep seller aligned on title status, required docs, and any issue resolution.";
  }

  if (use_case === "clear_to_close" || CLOSING_USE_CASES.has(use_case)) {
    return "Lock the seller into the next closing milestone and remove any last-minute blockers.";
  }

  if (DISPOSITION_USE_CASES.has(use_case)) {
    return "Coordinate access and marketing updates cleanly without losing seller trust.";
  }

  if (POST_CLOSE_USE_CASES.has(use_case)) {
    return "Close the loop cleanly and transition into referral or repeat-business posture.";
  }

  if (stage === STAGES.CONTRACT) {
    return "Move seller toward signature, docs, title coordination, or closing milestone.";
  }

  if (emotion === "motivated") {
    return "Keep momentum high and narrow toward number and timeline.";
  }

  if (emotion === "curious") {
    return "Answer clearly and move toward qualification or offer.";
  }

  if (emotion === "overwhelmed") {
    return "Keep it simple, low-pressure, and easy to follow.";
  }

  if (emotion === "grieving") {
    return "Use high care, low pressure, and keep the process very simple.";
  }

  if (use_case === "ownership_check") {
    return "Confirm ownership cleanly before moving deeper.";
  }

  if (use_case === "offer_reveal_cash") {
    return "Present number cleanly and guide toward the next step.";
  }

  return "Keep the conversation moving with a simple, relevant next step.";
}

// ─────────────────────────────────────────────────────────────
// CATEGORY / FLAGS
// ─────────────────────────────────────────────────────────────

function secondaryCategoryFromSignals({
  compliance_flag,
  objection,
  emotion,
  stage,
  message = "",
  lifecycle_stage = null,
}) {
  if (!clean(message) && stage === STAGES.OWNERSHIP) return "Outbound Initial";
  if (compliance_flag === "stop_texting") return "Compliance";
  if (objection) return "Objection Handling";
  if (lifecycle_stage === LIFECYCLE_STAGES.DISPOSITION) return "Disposition";
  if (lifecycle_stage === LIFECYCLE_STAGES.POST_CLOSE) return "Post-Close";
  if (lifecycle_stage === LIFECYCLE_STAGES.TITLE) return "Close / Handoff";
  if (lifecycle_stage === LIFECYCLE_STAGES.CLOSING) return "Close / Handoff";
  if (stage === STAGES.OFFER) return "Offer";
  if (stage === STAGES.CONTRACT) return "Close / Handoff";
  if (stage === STAGES.FOLLOW_UP) return "Re-engagement";
  if (emotion === "overwhelmed" || emotion === "grieving") return "Sensitive Situation";
  if (emotion && emotion !== "calm") return "Emotion Routing";
  return "Inbound Reply";
}

function primaryCategoryFromSignals({ seller_profile, objection }) {
  if (safeCategoryEquals(seller_profile, "Probate")) return "Probate / Trust";
  if (safeCategoryEquals(seller_profile, "Tired Landlord")) return "Landlord / Multifamily";
  if (objection === "tenant_issue") return "Landlord / Multifamily";
  if (objection === "probate") return "Probate / Trust";
  return "Residential";
}

function computeRouteFlags({ objection, emotion, seller_profile, message }) {
  const text = lower(message);

  const is_multifamily_like =
    safeCategoryEquals(seller_profile, "Tired Landlord") ||
    objection === "tenant_issue" ||
    hasAny(text, ["units", "doors", "occupied", "rent roll", "rents", "expenses"]);

  const needs_creative_review =
    objection === "need_more_money" ||
    objection === "wants_retail" ||
    hasAny(text, [
      "monthly payments",
      "seller finance",
      "owner finance",
      "subject to",
      "subto",
      "terms",
      "carry the note",
      "lease option",
    ]);

  const needs_docs_flow =
    objection === "needs_email" ||
    objection === "wants_written_offer" ||
    objection === "wants_proof_of_funds" ||
    hasAny(text, ["email", "docs", "paperwork", "contract", "proof of funds"]);

  const needs_human_review =
    objection === "needs_call" ||
    objection === "divorce" ||
    emotion === "grieving";

  return {
    is_multifamily_like,
    needs_creative_review,
    needs_docs_flow,
    needs_human_review,
  };
}

// ─────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────

export function resolveRoute({
  classification,
  brain_item = null,
  phone_item = null,
  message = "",
}) {
  const resolved_message =
    clean(classification?.message) ||
    clean(message);

  const language = normalizeLanguage(
    classification?.language ||
      getCategoryValue(brain_item, "language-preference", "English")
  );

  const existing_stage = normalizeStage(
    getCategoryValue(brain_item, "conversation-stage", "Ownership")
  );

  const seller_profile = getCategoryValue(brain_item, "seller-profile", null);
  const compliance_flag = classification?.compliance_flag || null;
  const objection = classification?.objection || null;
  const emotion = classification?.emotion || "calm";

  const detected_stage = stageFromSignals({
    existing_stage,
    stage_hint: classification?.stage_hint,
    objection,
    message: resolved_message,
  });

  const base_persona = normalizePersona(
    personaFromSignals({
      language,
      objection,
      emotion,
      seller_profile,
    }) || personaFromBrain(brain_item)
  );

  const use_case = useCaseFromSignals({
    compliance_flag,
    objection,
    stage: detected_stage,
    stage_hint: classification?.stage_hint,
    emotion,
    message: resolved_message,
    seller_profile,
  });

  const hinted_lifecycle_stage = normalizeLifecycleStage(
    classification?.stage_hint || existing_stage,
    detected_stage
  );
  const lifecycle_stage = lifecycleStageFromUseCase({
    use_case,
    stage: hinted_lifecycle_stage,
  });
  const stage = collapseLifecycleStage(lifecycle_stage, detected_stage);
  const is_initial_ownership_outbound = isInitialOwnershipOutbound({
    message: resolved_message,
    compliance_flag,
    objection,
    use_case,
    stage,
    lifecycle_stage,
  });
  const route_override = routeOverrideForUseCase({
    use_case,
    seller_profile,
  });
  const resolved_persona = is_initial_ownership_outbound
    ? "Warm Professional"
    : base_persona;

  const brain_ai_route = is_initial_ownership_outbound
    ? "Soft"
    : aiRouteFromSignals({
        compliance_flag,
        objection,
        emotion,
        stage,
        seller_profile,
      });

  const primary_category = primaryCategoryFromSignals({
    seller_profile,
    objection,
  });

  const secondary_category =
    route_override?.secondary_category ||
    secondaryCategoryFromSignals({
      compliance_flag,
      objection,
      emotion,
      stage,
      message: resolved_message,
      lifecycle_stage,
    });

  const resolved_primary_category =
    route_override?.category ||
    primary_category;

  const resolved_tone =
    route_override?.tone ||
    (is_initial_ownership_outbound ? "Warm" : toneFromEmotion(emotion, objection));

  const resolved_variant_group =
    route_override?.variant_group ||
    (is_initial_ownership_outbound
      ? "Stage 1 — Ownership Confirmation"
      : variantGroupFromStage(stage, objection, emotion, lifecycle_stage));

  const sequence_position = sequencePositionFromStage(
    stage,
    objection,
    lifecycle_stage
  );

  const resolved_template_agent_type = is_initial_ownership_outbound
    ? "Warm Professional"
    : templateAgentTypeForRoute({
        lifecycle_stage,
        use_case,
        language,
        seller_profile,
        primary_category: resolved_primary_category,
        persona: resolved_persona,
      });

  const resolved_fallback_agent_type = is_initial_ownership_outbound
    ? "Warm Professional"
    : fallbackTemplateAgentTypeForRoute({
        lifecycle_stage,
        use_case,
        language,
        seller_profile,
        primary_category: resolved_primary_category,
      });

  const next_move = is_initial_ownership_outbound
    ? "Confirm ownership cleanly before moving deeper."
    : nextMoveFromSignals({
        compliance_flag,
        objection,
        stage,
        emotion,
        use_case,
      });

  const flags = computeRouteFlags({
    objection,
    emotion,
    seller_profile,
    message: resolved_message,
  });

  return {
    compliance_flag,
    language,
    stage,
    lifecycle_stage,
    objection,
    emotion,
    persona: resolved_persona,
    tone: resolved_tone,
    variant_group: resolved_variant_group,
    use_case,
    brain_ai_route,
    next_move,
    primary_category: resolved_primary_category,
    secondary_category,
    sequence_position,
    seller_profile,
    phone_activity_status:
      classification?.phone_activity_status ||
      getCategoryValue(phone_item, "phone-activity-status", "Unknown"),
    ...flags,
    template_filters: {
      category: resolved_primary_category,
      secondary_category,
      use_case,
      variant_group: resolved_variant_group,
      tone: resolved_tone,
      gender_variant: "Neutral",
      language,
      sequence_position,
      paired_with_agent_type: resolved_template_agent_type,
      fallback_agent_type: resolved_fallback_agent_type,
      lifecycle_stage,
    },
  };
}

export default resolveRoute;
