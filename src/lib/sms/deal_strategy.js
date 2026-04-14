// ─── deal_strategy.js ─────────────────────────────────────────────────────
// Resolve deal strategy from conversation/offer context.

const VALID_STRATEGIES = Object.freeze([
  "Cash",
  "Creative",
  "Lease Option",
  "Subject To",
  "Novation",
  "Multifamily Underwrite",
  "Negotiation",
]);

function lc(val) {
  return String(val ?? "").toLowerCase().trim();
}

/**
 * Resolve Deal Strategy based on conversation/offer context.
 *
 * @param {object} context
 * @param {string} [context.current_strategy] - Explicit strategy from brain/offer
 * @param {string} [context.objection] - Current objection type
 * @param {string} [context.stage_code] - Current stage code
 * @param {string} [context.underwriting_mode] - "multifamily", "cash", etc.
 * @param {boolean} [context.is_multifamily_underwriting]
 * @param {boolean} [context.seller_wants_creative]
 * @param {boolean} [context.seller_wants_lease_option]
 * @param {boolean} [context.seller_wants_subject_to]
 * @param {boolean} [context.seller_wants_novation]
 * @param {boolean} [context.is_negotiation] - Active negotiation / price pushback
 * @returns {string} One of the valid deal strategy values
 */
export function resolveDealStrategy(context = {}) {
  const current = lc(context.current_strategy);

  // Explicit strategy pass-through if it's already a valid CSV value
  if (current) {
    const matched = VALID_STRATEGIES.find((s) => s.toLowerCase() === current);
    if (matched) return matched;
  }

  // Multifamily underwriting
  if (context.is_multifamily_underwriting || lc(context.underwriting_mode) === "multifamily") {
    return "Multifamily Underwrite";
  }

  // Multifamily stage codes
  const sc = lc(context.stage_code);
  if (sc && sc.startsWith("mf")) {
    return "Multifamily Underwrite";
  }

  // Negotiation / price pushback
  if (context.is_negotiation) {
    return "Negotiation";
  }
  const negotiation_objections = new Set([
    "need_more_money", "price_too_low", "can_you_do_better",
    "lowball_accusation", "best_price",
  ]);
  if (context.objection && negotiation_objections.has(lc(context.objection))) {
    return "Negotiation";
  }

  // Creative strategies from seller signals
  if (context.seller_wants_lease_option) return "Lease Option";
  if (context.seller_wants_subject_to) return "Subject To";
  if (context.seller_wants_novation) return "Novation";
  if (context.seller_wants_creative) return "Creative";

  // Default
  return "Cash";
}

export { VALID_STRATEGIES };

export default { resolveDealStrategy, VALID_STRATEGIES };
