// ─── intentMap.js ─────────────────────────────────────────────────────────
// Deterministic intent → stage → action map.

import { STAGES } from "./negotiationEngine.js";

export const ACTIONS = Object.freeze({
  QUEUE_REPLY: "queue_reply",
  STOP: "stop",
  ESCALATE: "escalate",
  WAIT: "wait",
  AI_FREEFORM: "ai_freeform",
  SUPPRESS: "suppress",
});

export const INTENT_MAP = Object.freeze({
  opt_out: {
    stage: STAGES.DNC,
    action: ACTIONS.STOP,
    reason: "compliance_stop",
  },
  wrong_number: {
    stage: STAGES.DEAD_LEAD,
    action: ACTIONS.SUPPRESS,
    reason: "wrong_number_detected",
  },
  hostile_or_legal: {
    stage: STAGES.LEGAL_REVIEW,
    action: ACTIONS.ESCALATE,
    reason: "hostile_or_legal_content",
  },
  not_interested: {
    stage: STAGES.NURTURE,
    action: ACTIONS.WAIT, // or schedule nurture
    reason: "not_interested",
  },
  ownership_confirmed: {
    stage: STAGES.CONSIDER_SELLING,
    action: ACTIONS.QUEUE_REPLY,
    use_case: "consider_selling",
  },
  seller_interested: {
    stage: STAGES.ASKING_PRICE,
    action: ACTIONS.QUEUE_REPLY,
    use_case: "seller_asking_price",
  },
  asks_offer: {
    stage: STAGES.UNDERWRITING,
    action: ACTIONS.QUEUE_REPLY,
    use_case: "send_info", // or wait for underwriting
  },
  asking_price_provided: {
    stage: STAGES.CONDITION_COLLECTION,
    action: ACTIONS.QUEUE_REPLY,
    use_case: "price_works_confirm_basics",
  },
  condition_disclosed: {
    stage: STAGES.UNDERWRITING,
    action: ACTIONS.QUEUE_REPLY,
    use_case: "walkthrough_or_condition",
  },
  tenant_occupied: {
    stage: STAGES.TENANT_RESOLUTION,
    action: ACTIONS.ESCALATE, // approval required for tenants
    reason: "tenant_occupied_review",
  },
  needs_call: {
    stage: "operator_callback", // Generic stage or STAGES.FOLLOW_UP
    action: ACTIONS.ESCALATE,
    reason: "seller_requests_call",
  },
  who_is_this: {
    stage: STAGES.OWNERSHIP_CHECK,
    action: ACTIONS.QUEUE_REPLY,
    use_case: "who_is_this",
  },
  unclear: {
    stage: "needs_review",
    action: ACTIONS.ESCALATE,
    reason: "low_confidence_unclear",
  },
});


export function getIntentRoute(intent) {
  return INTENT_MAP[intent] || INTENT_MAP.unclear;
}

export default { ACTIONS, INTENT_MAP, getIntentRoute };
