// ─── flow_map.js ──────────────────────────────────────────────────────────
// Central flow map: turns inbound classify result + brain state into the
// next outbound use_case / stage intent.  Pure config-driven, no spaghetti
// if/else scattered across route files.

// ══════════════════════════════════════════════════════════════════════════
// ACTIONS
// ══════════════════════════════════════════════════════════════════════════

export const ACTIONS = Object.freeze({
  QUEUE_REPLY: "queue_reply",
  STOP: "stop",
  ESCALATE: "escalate",
  WAIT: "wait",
  AI_FREEFORM: "ai_freeform",
});

// ══════════════════════════════════════════════════════════════════════════
// STAGE CODE CONSTANTS
// ══════════════════════════════════════════════════════════════════════════

export const STAGES = Object.freeze({
  S1: "S1",     S1F: "S1F",
  S2: "S2",     S2F: "S2F",
  S3: "S3",     S3F: "S3F",
  S4A: "S4A",   S4B: "S4B",   S4C: "S4C",
  S5A: "S5A",   S5B: "S5B",   S5C: "S5C",   S5D: "S5D",
  MF1: "MF1",   MF2: "MF2",   MF3: "MF3",   MF4: "MF4",   MF5: "MF5",
});

// ══════════════════════════════════════════════════════════════════════════
// COMPLIANCE RULES (absolute priority)
// ══════════════════════════════════════════════════════════════════════════

function resolveCompliance(classify_result) {
  const flag = String(classify_result?.compliance_flag ?? "").toLowerCase().trim();
  if (flag === "stop_texting" || flag === "opt_out" || flag === "do_not_contact") {
    return {
      action: ACTIONS.STOP,
      use_case: null,
      stage_code: null,
      reason: "compliance_stop",
      cancel_queued: true,
      human_review: false,
    };
  }
  return null;
}

// ══════════════════════════════════════════════════════════════════════════
// OBJECTION ROUTING TABLE
// ══════════════════════════════════════════════════════════════════════════

// Each key is an objection value from classify().
// Values are functions that take context and return { use_case, stage_code?, notes? }.
const OBJECTION_ROUTES = Object.freeze({
  wrong_number: (ctx) => {
    const msg = String(ctx.message ?? "").toLowerCase();
    if (msg.includes("know") || msg.includes("owner") || msg.includes("their")) {
      return { use_case: "wrong_number_knows_owner" };
    }
    return { use_case: "wrong_person" };
  },

  who_is_this: (ctx) => {
    if (ctx.skepticism_dominant) return { use_case: "seller_asks_legit" };
    if (ctx.wants_reviews) return { use_case: "website_reviews_request" };
    return { use_case: "who_is_this" };
  },

  not_interested: (ctx) => {
    if (ctx.agent_style_fit === "Warm Professional" || ctx.emotion === "frustrated" || ctx.emotion === "overwhelmed") {
      return { use_case: "obj_empathetic_not_interested" };
    }
    return { use_case: "not_interested" };
  },

  already_listed: (ctx) => {
    if (ctx.emotion === "frustrated" || ctx.emotion === "overwhelmed") {
      return { use_case: "obj_empathetic_already_listed" };
    }
    return { use_case: "already_listed" };
  },

  need_more_money: (ctx) => {
    const style = String(ctx.agent_style_fit ?? "").toLowerCase();
    if (style.includes("investor") || style.includes("hard")) {
      return { use_case: "price_low_hard" };
    }
    if (ctx.emotion === "motivated" || ctx.emotion === "curious") {
      return { use_case: "can_you_do_better" };
    }
    return { use_case: "price_low_soft" };
  },

  need_time: (ctx) => {
    if (ctx.asks_specific_later) return { use_case: "text_me_later_specific" };
    return { use_case: "not_ready" };
  },

  need_family_ok: (ctx) => {
    if (ctx.is_sibling_conflict) return { use_case: "sibling_conflict" };
    if (ctx.is_spouse) return { use_case: "need_spouse_signoff" };
    return { use_case: "family_discussion" };
  },

  tenant_issue: (ctx) => {
    if (ctx.tenants_ok) return { use_case: "tenants_ok" };
    if (ctx.is_multifamily) return { use_case: "occupied_asset" };
    if (ctx.emotion === "frustrated" || ctx.emotion === "overwhelmed") {
      return { use_case: "obj_empathetic_tenant_issue" };
    }
    return { use_case: "has_tenants" };
  },

  condition_bad: (ctx) => {
    if (ctx.is_code_violation) return { use_case: "code_violation_probe" };
    if (ctx.is_vacant_boarded) return { use_case: "vacant_boarded_probe" };
    if (ctx.needs_walkthrough) return { use_case: "walkthrough_or_condition" };
    if (ctx.needs_photos) return { use_case: "photo_request" };
    if (ctx.emotion === "frustrated" || ctx.agent_style_fit === "Warm Professional") {
      return { use_case: "obj_neighborly_condition_bad" };
    }
    return { use_case: "walkthrough_or_condition" };
  },

  probate: (ctx) => {
    if (ctx.is_recent_death) return { use_case: "death_sensitivity" };
    if (ctx.needs_probate_doc) return { use_case: "probate_doc_needed" };
    if (ctx.needs_family_discussion) return { use_case: "family_discussion" };
    return { use_case: "not_ready" };
  },

  divorce: (ctx) => {
    if (ctx.is_spouse) return { use_case: "need_spouse_signoff" };
    return { use_case: "divorce_sensitivity" };
  },

  financial_distress: (ctx) => {
    if (ctx.is_bankruptcy) return { use_case: "bankruptcy_sensitivity" };
    return { use_case: "foreclosure_pressure" };
  },

  has_other_buyer: () => ({ use_case: "already_have_someone" }),

  wants_retail: (ctx) => {
    if (ctx.creative_allowed) return { use_case: "creative_probe" };
    return { use_case: "price_too_low" };
  },

  needs_call: (ctx) => {
    if (ctx.human_review_configured) {
      return { use_case: null, action_override: ACTIONS.ESCALATE, reason: "seller_wants_call" };
    }
    return { use_case: "call_me_later_redirect" };
  },

  needs_email: (ctx) => {
    if (ctx.is_docs_stage) return { use_case: "email_for_docs" };
    return { use_case: "email_me_instead" };
  },

  wants_written_offer: (ctx) => {
    if (ctx.is_docs_stage) return { use_case: "send_package" };
    if (ctx.wants_proof_of_funds) return { use_case: "proof_of_funds" };
    return { use_case: "send_info" };
  },

  wants_proof_of_funds: (ctx) => {
    if (ctx.skepticism_dominant) return { use_case: "seller_asks_legit" };
    return { use_case: "proof_of_funds" };
  },

  send_offer_first: (ctx) => {
    if (ctx.underwriting_ready) {
      return { use_case: "offer_reveal_cash", stage_code: STAGES.S5A };
    }
    if (ctx.needs_condition_info) return { use_case: "condition_question_set" };
    return { use_case: "photo_request" };
  },

  stop_texting: () => ({
    use_case: null,
    action_override: ACTIONS.STOP,
    reason: "compliance_stop",
    cancel_queued: true,
  }),
});

// ══════════════════════════════════════════════════════════════════════════
// STAGE-BASED FLOW PROGRESSION
// ══════════════════════════════════════════════════════════════════════════

/**
 * Determine the next outbound based on current conversation stage and
 * positive seller signals.
 */
function resolveStageProgression(brain_state, classify_result, property_context) {
  const current_stage = String(brain_state?.conversation_stage ?? "").toLowerCase();
  const signals = classify_result?.positive_signals || [];
  const signal_set = new Set(signals.map((s) => String(s).toLowerCase()));

  // New conversation or Ownership stage
  if (!current_stage || current_stage === "ownership" || current_stage === "s1") {
    if (signal_set.has("confirms_ownership") || signal_set.has("affirmative")) {
      return { use_case: "consider_selling", stage_code: STAGES.S2 };
    }
    return { use_case: "ownership_check", stage_code: STAGES.S1 };
  }

  // Consider selling
  if (current_stage === "consider_selling" || current_stage === "s2") {
    if (signal_set.has("open_to_offer") || signal_set.has("affirmative") || signal_set.has("interested")) {
      return { use_case: "seller_asking_price", stage_code: STAGES.S3 };
    }
    return { use_case: "consider_selling_follow_up", stage_code: STAGES.S2F };
  }

  // Asking price
  if (current_stage === "asking_price" || current_stage === "s3") {
    if (signal_set.has("price_given") || signal_set.has("price_curious")) {
      // Price received — branch based on viability
      if (property_context?.price_works) {
        return { use_case: "price_works_confirm_basics", stage_code: STAGES.S4A };
      }
      if (property_context?.needs_condition_info) {
        return { use_case: "price_high_condition_probe", stage_code: STAGES.S4B };
      }
      if (property_context?.creative_allowed) {
        return { use_case: "creative_probe", stage_code: STAGES.S4C };
      }
      return { use_case: "price_high_condition_probe", stage_code: STAGES.S4B };
    }
    if (signal_set.has("send_offer_first")) {
      if (property_context?.underwriting_ready) {
        return { use_case: "offer_reveal_cash", stage_code: STAGES.S5A };
      }
      return { use_case: "photo_request" };
    }
    return { use_case: "asking_price_follow_up", stage_code: STAGES.S3F };
  }

  // Post-price / condition confirmation
  if (current_stage === "s4a" || current_stage === "s4b" || current_stage === "confirm_basics" || current_stage === "condition_probe") {
    if (signal_set.has("affirmative") || signal_set.has("condition_given")) {
      return resolveOfferReveal(property_context);
    }
    return { use_case: "condition_question_set" };
  }

  // Creative probe
  if (current_stage === "s4c" || current_stage === "creative_probe") {
    if (signal_set.has("creative_interest") || signal_set.has("affirmative")) {
      if (property_context?.is_lease_option) return { use_case: "offer_reveal_lease_option", stage_code: STAGES.S5B };
      if (property_context?.is_subject_to) return { use_case: "offer_reveal_subject_to", stage_code: STAGES.S5C };
      if (property_context?.is_novation) return { use_case: "offer_reveal_novation", stage_code: STAGES.S5D };
      return { use_case: "creative_followup", stage_code: STAGES.S4C };
    }
    return { use_case: "creative_followup", stage_code: STAGES.S4C };
  }

  // Multifamily underwriting flow
  if (current_stage === "mf1") return { use_case: "mf_occupancy", stage_code: STAGES.MF2 };
  if (current_stage === "mf2") return { use_case: "mf_rents", stage_code: STAGES.MF3 };
  if (current_stage === "mf3") return { use_case: "mf_expenses", stage_code: STAGES.MF4 };
  if (current_stage === "mf4") return { use_case: "mf_underwriting_ack", stage_code: STAGES.MF5 };

  // Offer stage / post-offer
  if (current_stage === "offer" || current_stage === "s5a" || current_stage === "s5b" || current_stage === "s5c" || current_stage === "s5d") {
    if (signal_set.has("verbal_yes") || signal_set.has("accepts_offer") || signal_set.has("affirmative")) {
      return { use_case: "asks_contract" };
    }
    return { use_case: "justify_price" };
  }

  // Contract / close lane
  if (current_stage === "contract" || current_stage === "close") {
    return resolveCloseLane(brain_state, classify_result);
  }

  // Follow-up / re-engagement
  if (current_stage === "follow-up" || current_stage === "follow_up" || current_stage === "re_engagement") {
    return { use_case: "reengagement" };
  }

  // Default: escalate for unrecognized stage
  return {
    use_case: null,
    action_override: ACTIONS.ESCALATE,
    reason: "unrecognized_stage",
  };
}

function resolveOfferReveal(property_context) {
  if (property_context?.is_lease_option) return { use_case: "offer_reveal_lease_option", stage_code: STAGES.S5B };
  if (property_context?.is_subject_to) return { use_case: "offer_reveal_subject_to", stage_code: STAGES.S5C };
  if (property_context?.is_novation) return { use_case: "offer_reveal_novation", stage_code: STAGES.S5D };
  return { use_case: "offer_reveal_cash", stage_code: STAGES.S5A };
}

function resolveCloseLane(brain_state, classify_result) {
  const signals = new Set((classify_result?.positive_signals || []).map((s) => String(s).toLowerCase()));
  const sub_stage = String(brain_state?.close_sub_stage ?? "").toLowerCase();

  if (signals.has("contract_signed")) return { use_case: "title_intro" };
  if (sub_stage === "contract_sent") return { use_case: "contract_not_signed_followup" };
  if (sub_stage === "title_intro") return { use_case: "title_by_text_update" };
  if (sub_stage === "title_delay") return { use_case: "title_delay_followup" };
  if (sub_stage === "title_issue") return { use_case: "title_issue_soft" };
  if (sub_stage === "lien_issue") return { use_case: "lien_issue_detected" };
  if (sub_stage === "closing_timeline") return { use_case: "day_before_close" };
  if (sub_stage === "clear_to_close") return { use_case: "clear_to_close" };
  if (sub_stage === "post_close") return { use_case: "post_close_referral" };

  return { use_case: "contract_sent" };
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN FLOW MAP
// ══════════════════════════════════════════════════════════════════════════

/**
 * Map classify result + brain state → next outbound action.
 *
 * @param {object} params
 * @param {object} params.classify_result - Output from classify()
 * @param {object} params.brain_state - Current AI Conversation Brain state
 * @param {object} [params.property_context] - Property/offer context
 * @param {string} [params.agent_style_fit] - Resolved agent style
 * @returns {object} { action, use_case, stage_code, delay_profile, human_review, reason, cancel_queued }
 */
export function mapNextAction({
  classify_result = {},
  brain_state = {},
  property_context = {},
  agent_style_fit = null,
} = {}) {
  // 1. Compliance is absolute
  const compliance = resolveCompliance(classify_result);
  if (compliance) return compliance;

  // 2. Determine delay profile
  const delay_profile = resolveDelayProfile(classify_result);

  // Build routing context
  const route_ctx = {
    ...property_context,
    message: classify_result.notes || "",
    emotion: classify_result.emotion || null,
    agent_style_fit,
    skepticism_dominant: classify_result.emotion === "skeptical",
    asks_specific_later: (classify_result.positive_signals || []).includes("text_me_later"),
    is_spouse: (classify_result.notes || "").toLowerCase().includes("spouse"),
    is_sibling_conflict: (classify_result.notes || "").toLowerCase().includes("sibling"),
    is_recent_death: (classify_result.notes || "").toLowerCase().includes("death") || (classify_result.notes || "").toLowerCase().includes("passed"),
    needs_probate_doc: (classify_result.notes || "").toLowerCase().includes("probate"),
    needs_family_discussion: (classify_result.objection || "").includes("family"),
    is_bankruptcy: classify_result.objection === "financial_distress" && (classify_result.notes || "").toLowerCase().includes("bankrupt"),
    is_code_violation: (classify_result.notes || "").toLowerCase().includes("code violation"),
    is_vacant_boarded: (classify_result.notes || "").toLowerCase().includes("vacant") || (classify_result.notes || "").toLowerCase().includes("boarded"),
    needs_walkthrough: (classify_result.notes || "").toLowerCase().includes("walkthrough"),
    needs_photos: (classify_result.notes || "").toLowerCase().includes("photo"),
    is_docs_stage: String(brain_state.conversation_stage ?? "").toLowerCase().includes("contract"),
    is_multifamily: property_context?.unit_count >= 2 || String(property_context?.property_type ?? "").toLowerCase().includes("multi"),
    tenants_ok: (classify_result.notes || "").toLowerCase().includes("tenants ok") || (classify_result.notes || "").toLowerCase().includes("no tenant issue"),
    wants_reviews: (classify_result.notes || "").toLowerCase().includes("review"),
    wants_proof_of_funds: classify_result.objection === "wants_proof_of_funds",
    human_review_configured: property_context?.human_review_configured ?? false,
    creative_allowed: property_context?.creative_allowed ?? false,
    needs_condition_info: property_context?.needs_condition_info ?? false,
    underwriting_ready: property_context?.underwriting_ready ?? false,
  };

  // 3. Objection routing takes priority over stage progression
  const objection = String(classify_result.objection ?? "").trim();
  if (objection && OBJECTION_ROUTES[objection]) {
    const result = OBJECTION_ROUTES[objection](route_ctx);

    if (result.action_override) {
      return {
        action: result.action_override,
        use_case: result.use_case || null,
        stage_code: result.stage_code || null,
        delay_profile,
        human_review: result.action_override === ACTIONS.ESCALATE,
        reason: result.reason || `objection_${objection}`,
        cancel_queued: result.cancel_queued || false,
      };
    }

    return {
      action: ACTIONS.QUEUE_REPLY,
      use_case: result.use_case,
      stage_code: result.stage_code || null,
      delay_profile,
      human_review: false,
      reason: `objection_${objection}`,
      cancel_queued: false,
    };
  }

  // 4. Stage-based progression
  const progression = resolveStageProgression(brain_state, classify_result, property_context);

  if (progression.action_override) {
    return {
      action: progression.action_override,
      use_case: progression.use_case || null,
      stage_code: progression.stage_code || null,
      delay_profile,
      human_review: progression.action_override === ACTIONS.ESCALATE,
      reason: progression.reason || "stage_progression",
      cancel_queued: false,
    };
  }

  return {
    action: ACTIONS.QUEUE_REPLY,
    use_case: progression.use_case,
    stage_code: progression.stage_code || null,
    delay_profile,
    human_review: false,
    reason: "stage_progression",
    cancel_queued: false,
  };
}

// ══════════════════════════════════════════════════════════════════════════
// DELAY PROFILE
// ══════════════════════════════════════════════════════════════════════════

function resolveDelayProfile(classify_result) {
  const emotion = String(classify_result?.emotion ?? "").toLowerCase();
  const signals = new Set((classify_result?.positive_signals || []).map((s) => String(s).toLowerCase()));
  const objection = String(classify_result?.objection ?? "").toLowerCase();

  // HOT: emotion motivated, positive urgency, affirmative, price_curious,
  //       send_offer_first, wants_proof_of_funds, needs_email, asks_contract
  const hot_triggers = [
    emotion === "motivated",
    signals.has("urgency"),
    signals.has("affirmative"),
    signals.has("price_curious"),
    objection === "send_offer_first",
    objection === "wants_proof_of_funds",
    objection === "needs_email",
    objection === "asks_contract",
  ];
  if (hot_triggers.some(Boolean)) return "hot";

  // COLD: skeptical, frustrated, guarded with very short ambiguous reply
  const cold_triggers = [
    emotion === "skeptical",
    emotion === "frustrated",
    emotion === "guarded",
  ];
  if (cold_triggers.some(Boolean)) return "cold";

  return "neutral";
}

export {
  resolveCompliance,
  resolveStageProgression,
  resolveDelayProfile,
  resolveCloseLane,
  OBJECTION_ROUTES,
};

export default { mapNextAction, ACTIONS, STAGES };
