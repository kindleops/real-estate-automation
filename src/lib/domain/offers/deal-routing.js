/**
 * deal-routing.js
 *
 * Determines whether a deal should be routed to the Podio Underwriting app
 * rather than proceeding as a single-family cash offer.
 *
 * Rules (any one is sufficient to trigger underwriting routing):
 *
 *   Property-level signals:
 *     • property_type contains multifamily / apartment / commercial
 *     • property_class indicates multifamily or commercial
 *     • unit_count >= 5
 *
 *   Deal strategy signals:
 *     • deal_strategy is Creative, Seller Finance, Subject To, Novation,
 *       Multifamily, or any equivalent alias
 *
 *   Seller message signals:
 *     • message body contains financing/lease/debt language:
 *       terms, mortgage, payment, debt, rents, occupancy, noi, cash flow,
 *       tenants, monthly income, cap rate, dscr, lease, seller finance,
 *       subject to, novation, owner finance, owner financing, wrap, wrap around
 *
 * Exported API:
 *   shouldRouteToUnderwriting({ property, sellerMessage, dealStrategy })
 *     → boolean
 *
 *   getUnderwritingRouteReason({ property, sellerMessage, dealStrategy })
 *     → string (human-readable reason) | null if no routing trigger found
 */

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function includesAny(text, needles) {
  const norm = lower(text);
  return needles.some((n) => norm.includes(lower(n)));
}

// ---------------------------------------------------------------------------
// Property-type / class multifamily detection
// ---------------------------------------------------------------------------

const MULTIFAMILY_PROPERTY_TYPE_KEYWORDS = [
  "multifamily",
  "multi-family",
  "multi family",
  "apartment",
  "apartments",
  "duplex",
  "triplex",
  "quadplex",
  "4-plex",
  "4 plex",
  "5-plex",
  "6-plex",
  "8-plex",
  "mixed use",
  "mixed-use",
];

const MULTIFAMILY_PROPERTY_CLASS_KEYWORDS = [
  "multifamily",
  "multi-family",
  "commercial",
  "apartment",
  "class a",
  "class b",
  "class c",
  "mf",
];

const COMMERCIAL_PROPERTY_TYPE_KEYWORDS = [
  "commercial",
  "office",
  "retail",
  "warehouse",
  "industrial",
  "hotel",
  "motel",
  "storage",
  "mobile home park",
  "manufactured housing",
];

/**
 * True if the property type / class indicates multifamily or commercial.
 *
 * @param {{ property_type?: string, property_class?: string }} property
 * @returns {boolean}
 */
function isMultifamilyProperty(property = {}) {
  const pt = lower(property?.property_type ?? "");
  const pc = lower(property?.property_class ?? "");

  if (includesAny(pt, MULTIFAMILY_PROPERTY_TYPE_KEYWORDS)) return true;
  if (includesAny(pt, COMMERCIAL_PROPERTY_TYPE_KEYWORDS))   return true;
  if (includesAny(pc, MULTIFAMILY_PROPERTY_CLASS_KEYWORDS)) return true;

  return false;
}

/**
 * True if the unit count is >= 5 (threshold for commercial / MF financing).
 *
 * @param {{ unit_count?: number|string|null }} property
 * @returns {boolean}
 */
function hasHighUnitCount(property = {}) {
  const uc = property?.unit_count ?? property?.number_of_units ?? null;
  if (uc === null || uc === undefined) return false;
  const n = Number(uc);
  return Number.isFinite(n) && n >= 5;
}

// ---------------------------------------------------------------------------
// Deal strategy detection
// ---------------------------------------------------------------------------

const UNDERWRITING_DEAL_STRATEGIES = [
  "creative",
  "seller finance",
  "seller financing",
  "owner finance",
  "owner financing",
  "subject to",
  "subject-to",
  "novation",
  "wrap",
  "wrap around",
  "wraparound",
  "multifamily",
  "multi-family",
  "multi family",
  "mf_",
  "creative_finance",
  "seller_finance",
  "subject_to",
];

/**
 * True if deal strategy explicitly targets an underwriting workflow.
 *
 * @param {string|null} dealStrategy
 * @returns {boolean}
 */
function isUnderwritingDealStrategy(dealStrategy) {
  if (!dealStrategy) return false;
  return includesAny(String(dealStrategy), UNDERWRITING_DEAL_STRATEGIES);
}

// ---------------------------------------------------------------------------
// Seller message signal detection
// ---------------------------------------------------------------------------

const UNDERWRITING_MESSAGE_SIGNALS = [
  // Financing / creative terms
  "terms",
  "seller finance",
  "seller financing",
  "owner finance",
  "subject to",
  "novation",
  "wrap around",
  "wraparound",
  "owner will carry",

  // Debt / mortgage
  "mortgage",
  "mortgage balance",
  "existing mortgage",
  "debt",
  "payoff",
  "loan balance",

  // MF / rental income
  "rents",
  "rental income",
  "monthly income",
  "cash flow",
  "noi",
  "net operating income",
  "cap rate",
  "dscr",
  "occupancy",
  "tenants",
  "rent roll",

  // Payments
  "monthly payment",
  "payments",

  // Lease
  "lease",
  "leased",
  "section 8",
];

/**
 * True if the seller's message body contains signals that indicate a
 * creative-finance or multifamily deal rather than a plain cash purchase.
 *
 * @param {string|null} sellerMessage
 * @returns {boolean}
 */
function messageHasUnderwritingSignals(sellerMessage) {
  if (!sellerMessage) return false;
  return includesAny(String(sellerMessage), UNDERWRITING_MESSAGE_SIGNALS);
}

// ---------------------------------------------------------------------------
// Route reason builder
// ---------------------------------------------------------------------------

/**
 * Return a human-readable description of why a deal should go to underwriting,
 * or null if no trigger was found.
 *
 * First-matching-rule wins (priority order matches shouldRouteToUnderwriting).
 *
 * @param {{ property?: object, sellerMessage?: string|null, dealStrategy?: string|null }}
 * @returns {string|null}
 */
export function getUnderwritingRouteReason({
  property      = {},
  sellerMessage = null,
  dealStrategy  = null,
} = {}) {
  if (isMultifamilyProperty(property)) {
    const pt = lower(property?.property_type ?? "");
    if (includesAny(pt, COMMERCIAL_PROPERTY_TYPE_KEYWORDS)) {
      return "Commercial property type — not eligible for single-family cash offer";
    }
    return "Multifamily or apartment property — must use Underwriting app";
  }

  if (hasHighUnitCount(property)) {
    const uc = property?.unit_count ?? property?.number_of_units;
    return `Unit count (${uc}) >= 5 — must use Underwriting app`;
  }

  if (isUnderwritingDealStrategy(dealStrategy)) {
    return `Deal strategy "${clean(dealStrategy)}" requires Underwriting intake`;
  }

  if (messageHasUnderwritingSignals(sellerMessage)) {
    return "Seller message contains creative-finance or multifamily signals";
  }

  return null;
}

/**
 * Determine whether this deal should be routed to the Podio Underwriting app
 * instead of the single-family cash offer flow.
 *
 * Returns true if ANY routing trigger is detected.
 *
 * @param {{ property?: object, sellerMessage?: string|null, dealStrategy?: string|null }}
 * @returns {boolean}
 */
export function shouldRouteToUnderwriting({
  property      = {},
  sellerMessage = null,
  dealStrategy  = null,
} = {}) {
  return getUnderwritingRouteReason({ property, sellerMessage, dealStrategy }) !== null;
}
