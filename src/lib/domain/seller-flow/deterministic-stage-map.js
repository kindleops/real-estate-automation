import {
  SELLER_FLOW_SAFETY_POLICY,
  SELLER_FLOW_SAFETY_TIERS,
} from "@/lib/domain/seller-flow/seller-flow-safety-policy.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

const EXPLICIT_INTENT_RULES = Object.freeze({
  ownership_confirmed: Object.freeze({
    next_stage: "consider_selling",
    template_use_case: "consider_selling",
    safety_tier: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    auto_send_eligible: false,
    should_queue_reply: true,
    suppression_reason: null,
  }),
  info_request: Object.freeze({
    next_stage: "info_source_explanation",
    template_use_case: "info_source_explanation",
    template_use_case_candidates: Object.freeze(["info_source_explanation", "who_is_this"]),
    safety_tier: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    auto_send_eligible: false,
    should_queue_reply: true,
    suppression_reason: null,
  }),
  wrong_person: Object.freeze({
    next_stage: "wrong_person",
    template_use_case: "wrong_person",
    safety_tier: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "wrong_person_intent",
  }),
  opt_out: Object.freeze({
    next_stage: "stop_or_opt_out",
    template_use_case: null,
    safety_tier: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "opt_out_intent_no_marketing",
  }),
  not_interested: Object.freeze({
    next_stage: "not_interested",
    template_use_case: "not_interested",
    safety_tier: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "not_interested_intent",
  }),
  listed_or_unavailable: Object.freeze({
    next_stage: "listed_or_unavailable",
    template_use_case: "listed_or_unavailable",
    safety_tier: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "listed_or_unavailable_intent",
  }),
  tenant_or_occupancy: Object.freeze({
    next_stage: "tenant_or_occupancy",
    template_use_case: "tenant_or_occupancy",
    safety_tier: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "tenant_or_occupancy_intent",
  }),
  hostile_or_legal: Object.freeze({
    next_stage: "hostile_or_legal",
    template_use_case: null,
    safety_tier: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "hostile_or_legal_intent",
  }),
  timing_complaint: Object.freeze({
    next_stage: "hostile_or_legal",
    template_use_case: null,
    safety_tier: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    auto_send_eligible: false,
    should_queue_reply: false,
    suppression_reason: "timing_complaint_manual_review",
  }),
});

function normalizeCurrentStage(stage = null) {
  const value = lower(stage);
  if (!value) return null;
  if (["ownership confirmation", "ownership_check", "s1"].includes(value)) {
    return "ownership_check";
  }
  return value;
}

export function resolveDeterministicStageTransition({
  current_stage = null,
  inbound_intent = "unclear",
  should_queue_reply = true,
  autopilot_enabled = true,
} = {}) {
  const stage_key = normalizeCurrentStage(current_stage);
  const intent_key = lower(inbound_intent) || "unclear";

  const explicit = EXPLICIT_INTENT_RULES[intent_key] || null;
  if (explicit) {
    return {
      current_stage: stage_key,
      inbound_intent: intent_key,
      next_stage: explicit.next_stage,
      template_use_case: explicit.template_use_case,
      template_use_case_candidates: explicit.template_use_case_candidates || null,
      safety_tier: explicit.safety_tier,
      auto_send_eligible: false,
      should_queue_reply: Boolean(explicit.should_queue_reply),
      suppression_reason: explicit.suppression_reason || null,
      policy_source: "explicit",
      deterministic_match: true,
    };
  }

  const stage_policy =
    (stage_key && SELLER_FLOW_SAFETY_POLICY[stage_key]?.[intent_key]) || null;
  const global_policy = SELLER_FLOW_SAFETY_POLICY.global?.[intent_key] || null;
  const selected_policy = stage_policy || global_policy || null;

  let safety_tier = selected_policy?.safety || SELLER_FLOW_SAFETY_TIERS.REVIEW;

  if (!autopilot_enabled && safety_tier === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND) {
    safety_tier = SELLER_FLOW_SAFETY_TIERS.REVIEW;
  }

  if (safety_tier === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND && !should_queue_reply) {
    safety_tier = SELLER_FLOW_SAFETY_TIERS.REVIEW;
  }

  return {
    current_stage: stage_key,
    inbound_intent: intent_key,
    next_stage: selected_policy?.next_stage || null,
    template_use_case: selected_policy?.template || null,
    template_use_case_candidates: null,
    safety_tier,
    auto_send_eligible: safety_tier === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    should_queue_reply: Boolean(should_queue_reply),
    suppression_reason: null,
    policy_source: stage_policy ? "stage" : global_policy ? "global" : "default",
    deterministic_match: Boolean(selected_policy),
  };
}

export function buildDeterministicStageMap({ current_stage = null } = {}) {
  const stage_filter = normalizeCurrentStage(current_stage);
  const rows = [];

  for (const [intent, rule] of Object.entries(EXPLICIT_INTENT_RULES)) {
    rows.push({
      current_stage: "global",
      inbound_intent: intent,
      next_stage: rule.next_stage,
      template_use_case: rule.template_use_case,
      safety_tier: rule.safety_tier,
      auto_send_eligible: false,
      policy_source: "explicit",
    });
  }

  for (const [stage_key, intents] of Object.entries(SELLER_FLOW_SAFETY_POLICY)) {
    if (stage_filter && normalizeCurrentStage(stage_key) !== stage_filter) continue;

    for (const [intent, transition] of Object.entries(intents || {})) {
      if (EXPLICIT_INTENT_RULES[intent]) continue;
      rows.push({
        current_stage: stage_key,
        inbound_intent: intent,
        next_stage: transition?.next_stage || null,
        template_use_case: transition?.template || null,
        safety_tier: transition?.safety || SELLER_FLOW_SAFETY_TIERS.REVIEW,
        auto_send_eligible:
          transition?.safety === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
        policy_source: "policy",
      });
    }
  }

  return rows;
}
