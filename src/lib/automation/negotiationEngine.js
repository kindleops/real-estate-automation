// ─── negotiationEngine.js ─────────────────────────────────────────────────
/**
 * NegotiationEngine
 *
 * Handles stage progression, seller temperature, and repeated question prevention.
 * Uses historical memory to drive deterministic routing decisions.
 */

export const STAGES = Object.freeze({
  OWNERSHIP_CHECK: "ownership_check",
  CONSIDER_SELLING: "consider_selling",
  ASKING_PRICE: "asking_price",
  CONDITION_COLLECTION: "condition_collection",
  TENANT_RESOLUTION: "tenant_resolution",
  UNDERWRITING: "underwriting",
  OFFER_PREPARATION: "offer_preparation",
  FOLLOW_UP: "follow_up",
  NURTURE: "nurture",
  DEAD_LEAD: "dead_lead",
  DNC: "dnc",
  LEGAL_REVIEW: "legal_review",
});

export const TEMPERATURES = Object.freeze({
  COLD: "cold",
  WARMING: "warming",
  ENGAGED: "engaged",
  HOT: "hot",
  DEAD: "dead",
});

/**
 * Calculates next stage based on memory and current classification.
 */
export function resolveNextStage(current_stage, classification, memory = {}) {
  const { primary_intent } = classification;
  const latest_state = memory.latest_state || {};

  // 1. Compliance / Hard Stops
  if (primary_intent === "opt_out") return STAGES.DNC;
  if (primary_intent === "wrong_number") return STAGES.DEAD_LEAD;
  if (primary_intent === "hostile_or_legal") return STAGES.LEGAL_REVIEW;
  if (primary_intent === "not_interested") return STAGES.NURTURE;

  // 2. Progression
  if (primary_intent === "ownership_confirmed") {
    if (latest_state.ownership_confirmed) return STAGES.CONSIDER_SELLING; // Already confirmed, move to probing interest
    return STAGES.OWNERSHIP_CHECK;
  }

  if (primary_intent === "seller_interested" || primary_intent === "latent_interest") {
    return STAGES.ASKING_PRICE;
  }

  if (primary_intent === "asking_price_provided") {
    return STAGES.CONDITION_COLLECTION;
  }

  if (primary_intent === "asks_offer") {
    return STAGES.UNDERWRITING;
  }

  if (primary_intent === "condition_disclosed") {
    return STAGES.UNDERWRITING;
  }

  if (primary_intent === "tenant_occupied") {
    return STAGES.TENANT_RESOLUTION;
  }

  return current_stage || STAGES.OWNERSHIP_CHECK;
}

/**
 * Calculates seller temperature based on memory and classification.
 */
export function calculateTemperature(classification, memory = {}) {
  const { primary_intent, emotion, motivation_score = 50 } = classification;
  const latest_state = memory.latest_state || {};
  
  let score = motivation_score;

  // 1. Intent Multipliers
  const intent_boosts = {
    ownership_confirmed: 10,
    seller_interested: 20,
    asks_offer: 25,
    asking_price_provided: 30,
    callback_requested: 15,
  };
  score += (intent_boosts[primary_intent] || 0);

  // 2. Compliance Penalties
  if (primary_intent === "opt_out" || primary_intent === "wrong_number") return TEMPERATURES.DEAD;
  if (primary_intent === "not_interested") return TEMPERATURES.COLD;
  if (primary_intent === "hostile_or_legal") return TEMPERATURES.DEAD;

  // 3. Memory Factors
  if (latest_state.seller_interest === 'high') score += 10;
  if (latest_state.timeline === 'immediate') score += 15;
  
  // 4. Resolve Temperature
  if (score >= 90) return TEMPERATURES.HOT;
  if (score >= 70) return TEMPERATURES.ENGAGED;
  if (score >= 50) return TEMPERATURES.WARMING;
  return TEMPERATURES.COLD;
}

/**
 * Checks if a template use_case is redundant based on memory.
 * Prevents asking "Are you the owner?" if we already know they are.
 */
export function isQuestionRedundant(use_case, memory = {}) {
  const latest_state = memory.latest_state || {};
  
  if (use_case === 'ownership_check' && latest_state.ownership_confirmed) return true;
  if (use_case === 'asking_price' && latest_state.price_mentioned) return true;
  if (use_case === 'occupancy_probe' && latest_state.tenant_occupied !== undefined) return true;
  if (use_case === 'condition_probe' && memory.memory?.condition_status) return true;

  return false;
}

export default {
  STAGES,
  TEMPERATURES,
  resolveNextStage,
  calculateTemperature,
  isQuestionRedundant,
};
