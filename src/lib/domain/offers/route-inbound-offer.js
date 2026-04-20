/**
 * route-inbound-offer.js
 *
 * Stateless routing decision module for inbound seller offer requests.
 *
 * Decision tree:
 *   1. Detect offer intent in message/classification → no_offer_signal if absent.
 *   2. Check deal type (property + message signals) for MF/creative.
 *      → underwriting if MF/creative/commercial.
 *   3. Type-guard: block SFH cash offer path if route use_case is non-SFH-cash.
 *      → type_guard_blocked
 *   4. SFH cash path:
 *      a. Active cash snapshot found → sfh_cash_preview
 *      b. No snapshot, property_id present → condition_clarifier
 *         (ask condition/repair clarifier questions)
 *      c. No snapshot, no property_id → manual_review
 *
 * Invariants enforced:
 *   • Never returns an offer amount — routing decision only.
 *   • Never routes an SFH cash offer to a multifamily/creative lead.
 *
 * Exported API:
 *   routeInboundOffer({ message, classification, context, route })
 *     → Promise<{ ok, offer_route, reason, meta }>
 *
 *   offer_route values:
 *     no_offer_signal     – no offer-related intent detected
 *     underwriting        – MF/creative/commercial → Podio Underwriting app
 *     type_guard_blocked  – prior route labelled deal as non-SFH-cash
 *     sfh_cash_preview    – SFH cash deal, active snapshot → can preview offer
 *     condition_clarifier – SFH cash, no snapshot, property_id known
 *     manual_review       – SFH cash, no snapshot, no property_id
 *
 * Test injection:
 *   __setRouteInboundOfferDeps / __resetRouteInboundOfferDeps
 */

import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";
import {
  getUnderwritingRouteReason,
} from "@/lib/domain/offers/deal-routing.js";
import { getActivePropertyCashOffer } from "@/lib/domain/offers/property-cash-offer-cache.js";

// ── Dependency Injection ─────────────────────────────────────────────────────

let _deps = {
  get_active_property_cash_offer: null,
};

export function __setRouteInboundOfferDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

export function __resetRouteInboundOfferDeps() {
  _deps = { get_active_property_cash_offer: null };
}

function getSnapshotFn() {
  return _deps.get_active_property_cash_offer ?? getActivePropertyCashOffer;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function clean(v) {
  return String(v ?? "").trim();
}

function lower(v) {
  return clean(v).toLowerCase();
}

function includesAny(text, needles = []) {
  const n = lower(text);
  return needles.some((needle) => n.includes(lower(needle)));
}

// ── Offer Intent Detection ───────────────────────────────────────────────────

// These classification objections signal that the seller is expecting an offer.
const OFFER_REQUEST_OBJECTIONS = new Set([
  "send_offer_first",
  "need_more_money",
  "has_other_buyer",
  "wants_retail",
  "wants_written_offer",
  "wants_proof_of_funds",
]);

// Phrases that indicate the seller is requesting or expecting a cash offer.
const OFFER_REQUEST_PHRASES = [
  "make me an offer",
  "what's your offer",
  "what is your offer",
  "send me an offer",
  "what can you offer",
  "what can you pay",
  "how much can you pay",
  "what would you pay",
  "give me a number",
  "send it in writing",
  "written offer",
  "what's your number",
  "what is your number",
  "what number are you thinking",
  "you tell me your number",
  "you tell me",
  "cash offer",
  "your best offer",
  "best offer",
  "highest offer",
  "top offer",
  "send offer",
  "send the offer",
  "show me the offer",
  "let's see the offer",
  "what's the offer",
  "an offer on my",
];

/**
 * True if the inbound message or classification signals that the seller
 * is requesting or expecting an offer.
 *
 * @param {string} message
 * @param {object|null} classification
 * @returns {boolean}
 */
function hasOfferIntent(message = "", classification = null) {
  if (OFFER_REQUEST_OBJECTIONS.has(clean(classification?.objection))) return true;
  if (lower(clean(classification?.emotion)) === "motivated") return true;
  return includesAny(message, OFFER_REQUEST_PHRASES);
}

// ── Property Extraction ──────────────────────────────────────────────────────

/**
 * Build a normalised property descriptor from the inbound context.
 * Values are taken from the Podio property item fields where available,
 * falling back to context.summary for summary-level values.
 *
 * @param {object|null} context
 * @returns {{ property_type, property_class, unit_count, property_id,
 *             podio_property_item_id, property_address }}
 */
function extractPropertyDescriptor(context = null) {
  const property_item = context?.items?.property_item ?? null;
  const summary = context?.summary ?? {};

  return {
    property_type:
      getCategoryValue(property_item, "property-type", null) ||
      summary.primary_category ||
      null,
    property_class:
      getCategoryValue(property_item, "property-class", null) || null,
    unit_count:
      getNumberValue(property_item, "number-of-units", null) ??
      getNumberValue(property_item, "units", null) ??
      null,
    property_id: context?.ids?.property_id ?? null,
    podio_property_item_id: property_item?.item_id ?? null,
    property_address: summary.property_address ?? null,
  };
}

// ── Route Decision ───────────────────────────────────────────────────────────

// Route use_case keywords that indicate the deal is NOT a plain SFH cash deal.
// If a prior routing pass has already labelled the deal as one of these,
// the SFH cash offer path must be blocked (type_guard_blocked).
const NON_SFH_CASH_USE_CASE_KEYWORDS = [
  "mf_offer_reveal",
  "multifamily",
  "creative",
  "subject_to",
  "novation",
  "seller_finance",
  "owner_finance",
  "creative_finance",
];

/**
 * Determine the correct offer-routing path for an inbound seller reply.
 *
 * @param {{
 *   message?:        string,
 *   classification?: object|null,
 *   context?:        object|null,
 *   route?:          object|null,
 * }}
 * @returns {Promise<{ ok: boolean, offer_route: string, reason: string, meta: object }>}
 */
export async function routeInboundOffer({
  message = "",
  classification = null,
  context = null,
  route = null,
} = {}) {
  // ── Step 1: Extract property descriptor + deal strategy ─────────────────
  const prop = extractPropertyDescriptor(context);
  const deal_strategy =
    route?.deal_strategy ||
    route?.variant_group ||
    context?.summary?.deal_strategy ||
    null;

  // ── Step 2: Message + deal_strategy underwriting check ──────────────────
  // Check message signals and deal strategy BEFORE the offer-intent gate so
  // that a seller mentioning creative/MF terms is routed to underwriting even
  // when there is no explicit "what's your offer" phrase in the same message.
  const message_underwriting_reason = getUnderwritingRouteReason({
    property: {},
    sellerMessage: message,
    dealStrategy: deal_strategy,
  });

  if (message_underwriting_reason) {
    return {
      ok: true,
      offer_route: "underwriting",
      reason: message_underwriting_reason,
      meta: {
        property_type: prop.property_type,
        deal_strategy: deal_strategy || null,
        underwriting_reason: message_underwriting_reason,
      },
    };
  }

  // ── Step 3: Offer intent gate ────────────────────────────────────────────
  if (!hasOfferIntent(message, classification)) {
    return {
      ok: true,
      offer_route: "no_offer_signal",
      reason: "no_offer_intent_detected",
      meta: {},
    };
  }

  // ── Step 4: Property-level underwriting check ────────────────────────────
  // Message and deal_strategy were already checked above. Here we only check
  // property type / class / unit_count so we don't double-count message signals.
  const property_underwriting_reason = getUnderwritingRouteReason({
    property: prop,
    sellerMessage: null,
    dealStrategy: null,
  });

  if (property_underwriting_reason) {
    return {
      ok: true,
      offer_route: "underwriting",
      reason: property_underwriting_reason,
      meta: {
        property_type: prop.property_type,
        deal_strategy: deal_strategy || null,
        underwriting_reason: property_underwriting_reason,
      },
    };
  }

  // ── Step 5: Type guard ───────────────────────────────────────────────────
  // If a prior routing pass already labelled this deal as non-SFH-cash,
  // block the SFH cash offer path to prevent misrouting.
  const route_use_case = lower(clean(route?.use_case ?? ""));
  if (NON_SFH_CASH_USE_CASE_KEYWORDS.some((kw) => route_use_case.includes(kw))) {
    return {
      ok: true,
      offer_route: "type_guard_blocked",
      reason: "route_use_case_is_non_sfh_cash",
      meta: { route_use_case: route?.use_case || route_use_case },
    };
  }

  // ── Step 5: SFH cash path — snapshot lookup ──────────────────────────────
  const snapshot_result = await getSnapshotFn()({
    property_id: prop.property_id,
    podio_property_item_id: prop.podio_property_item_id,
  });

  if (snapshot_result?.ok && snapshot_result?.snapshot) {
    return {
      ok: true,
      offer_route: "sfh_cash_preview",
      reason: "active_cash_snapshot_found",
      meta: {
        cash_offer: snapshot_result.snapshot.cash_offer ?? null,
        property_id: prop.property_id,
        snapshot_id: snapshot_result.snapshot.id ?? null,
      },
    };
  }

  // ── Step 6: No snapshot — condition clarifier vs manual review ───────────
  // condition_clarifier: we know which property → ask about condition/repairs
  // manual_review: no property identity → flag for human review
  if (prop.property_id) {
    return {
      ok: true,
      offer_route: "condition_clarifier",
      reason: "no_snapshot_property_id_present",
      meta: {
        property_id: prop.property_id,
        property_address: prop.property_address ?? null,
      },
    };
  }

  return {
    ok: true,
    offer_route: "manual_review",
    reason: "no_snapshot_no_property_id",
    meta: {
      podio_property_item_id: prop.podio_property_item_id ?? null,
    },
  };
}
