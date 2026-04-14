// ─── property_scope.js ────────────────────────────────────────────────────
// Resolve template Property Type Scope from property/owner context.

const VALID_SCOPES = Object.freeze([
  "Any Residential",
  "Residential",
  "Probate / Trust",
  "Corporate / Institutional",
  "Landlord / Multifamily",
  "Follow-Up",
  "Heavy Negotiation",
  "5+ Units",
  "Duplex",
  "Triplex",
  "Fourplex",
]);

function lc(val) {
  return String(val ?? "").toLowerCase().trim();
}

/**
 * Resolve Property Type Scope based on context.
 *
 * @param {object} context
 * @param {string} [context.use_case]
 * @param {boolean} [context.is_follow_up]
 * @param {string} [context.owner_type] - "Corporate", "Trust / Estate", "Individual", etc.
 * @param {string} [context.property_type] - "Single Family", "Multi-Family", etc.
 * @param {number} [context.unit_count] - Number of units
 * @param {boolean} [context.is_probate]
 * @param {boolean} [context.is_trust]
 * @param {boolean} [context.is_corporate]
 * @param {boolean} [context.is_heavy_negotiation]
 * @returns {string} One of the valid property type scope values
 */
export function resolvePropertyTypeScope(context = {}) {
  const uc = lc(context.use_case);
  const owner = lc(context.owner_type);
  const units = typeof context.unit_count === "number" ? context.unit_count : null;

  // 1. Follow-up explicit template rows
  if (context.is_follow_up && isFollowUpUseCase(uc)) {
    return "Follow-Up";
  }

  // 2. Probate / Trust
  if (context.is_probate || context.is_trust || owner === "trust / estate" || owner === "trust" || owner === "estate") {
    return "Probate / Trust";
  }

  // 3. Corporate / Institutional
  if (context.is_corporate || owner === "corporate" || owner === "bank / lender" || owner === "government") {
    return "Corporate / Institutional";
  }

  // Heavy negotiation
  if (context.is_heavy_negotiation) {
    return "Heavy Negotiation";
  }

  // 4. Unit-count based scoping
  if (units !== null) {
    if (units >= 5) return "5+ Units";
    if (units === 4) return "Fourplex";
    if (units === 3) return "Triplex";
    if (units === 2) return "Duplex";
  }

  // Multi-family property type without specific unit count
  if (lc(context.property_type) === "multi-family" || lc(context.property_type) === "apartment") {
    return "Landlord / Multifamily";
  }

  // 8. Default residential
  return "Residential";
}

const FOLLOW_UP_USE_CASE_FRAGMENTS = new Set([
  "follow_up", "followup", "follow-up",
  "reengagement", "re_engagement", "re-engagement",
]);

function isFollowUpUseCase(use_case) {
  const uc = lc(use_case);
  for (const frag of FOLLOW_UP_USE_CASE_FRAGMENTS) {
    if (uc.includes(frag)) return true;
  }
  return false;
}

export { VALID_SCOPES };

export default { resolvePropertyTypeScope, VALID_SCOPES };
