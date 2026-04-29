import { SELLER_FLOW_STAGES } from "./canonical-seller-flow.js";

export const SELLER_FLOW_SAFETY_TIERS = Object.freeze({
  AUTO_SEND: "auto_send",   // High confidence, low risk, autopilot enabled
  REVIEW: "review",         // Low confidence, complex intent, or autopilot disabled
  SUPPRESS: "suppress",     // Explicit negative intent, hostile, or terminal state
});

/**
 * Deterministic Seller Flow Safety Policy
 * current_stage + inbound_intent → { next_stage, template, safety }
 */
export const SELLER_FLOW_SAFETY_POLICY = Object.freeze({
  // --- Stage 1: Ownership Check ---
  [SELLER_FLOW_STAGES.OWNERSHIP_CHECK]: {
    ownership_confirmed: {
      next_stage: SELLER_FLOW_STAGES.CONSIDER_SELLING,
      template: SELLER_FLOW_STAGES.CONSIDER_SELLING,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    info_request: {
      next_stage: SELLER_FLOW_STAGES.WHO_IS_THIS,
      template: SELLER_FLOW_STAGES.WHO_IS_THIS,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    wrong_person: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.WRONG_PERSON,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    opt_out: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.STOP_OR_OPT_OUT,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    },
    not_interested: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.NOT_INTERESTED,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    },
    unclear: {
      next_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
      template: "unclear_clarifier",
      safety: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    }
  },

  // --- Stage 2: Consider Selling ---
  [SELLER_FLOW_STAGES.CONSIDER_SELLING]: {
    ownership_confirmed: {
      next_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
      template: SELLER_FLOW_STAGES.ASKING_PRICE,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    asks_offer: {
      next_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
      template: SELLER_FLOW_STAGES.ASKING_PRICE,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    not_interested: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.NOT_INTERESTED,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    },
    opt_out: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.STOP_OR_OPT_OUT,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    }
  },

  // --- Stage 3: Asking Price ---
  [SELLER_FLOW_STAGES.ASKING_PRICE]: {
    asking_price_value: {
      next_stage: SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
      template: SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
      safety: SELLER_FLOW_SAFETY_TIERS.REVIEW, // Price values usually need a human look before auto-confirm
    },
    condition_signal: {
      next_stage: SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
      template: SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    asks_offer: {
      next_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
      template: SELLER_FLOW_STAGES.ASKING_PRICE,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    }
  },

  // --- Stage 4A: Confirm Basics ---
  [SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS]: {
    condition_signal: {
      next_stage: SELLER_FLOW_STAGES.CREATIVE_PROBE,
      template: SELLER_FLOW_STAGES.CREATIVE_PROBE,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    ownership_confirmed: {
      next_stage: SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
      template: SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    }
  },

  // --- Stage 5A: Offer Reveal Cash ---
  [SELLER_FLOW_STAGES.OFFER_REVEAL_CASH]: {
    not_interested: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.NOT_INTERESTED,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    },
    asks_offer: {
      next_stage: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
      template: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
      safety: SELLER_FLOW_SAFETY_TIERS.REVIEW,
    }
  },

  // --- Default Fallbacks ---
  global: {
    opt_out: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.STOP_OR_OPT_OUT,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    },
    wrong_person: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: SELLER_FLOW_STAGES.WRONG_PERSON,
      safety: SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    },
    hostile_or_legal: {
      next_stage: SELLER_FLOW_STAGES.TERMINAL,
      template: null,
      safety: SELLER_FLOW_SAFETY_TIERS.SUPPRESS,
    }
  }
});

/**
 * Resolves the safety tier for a given plan.
 */
export function resolveSafetyTier(plan, autopilot_enabled = false) {
  if (!autopilot_enabled) return SELLER_FLOW_SAFETY_TIERS.REVIEW;
  
  const policy = SELLER_FLOW_SAFETY_POLICY[plan.current_stage]?.[plan.inbound_intent] 
    || SELLER_FLOW_SAFETY_POLICY.global[plan.inbound_intent]
    || { safety: SELLER_FLOW_SAFETY_TIERS.REVIEW };

  // Even if policy says AUTO_SEND, if we didn't queue a reply (e.g. template missing), it's a REVIEW
  if (policy.safety === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND && !plan.should_queue_reply) {
    return SELLER_FLOW_SAFETY_TIERS.REVIEW;
  }

  return policy.safety;
}
