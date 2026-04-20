/**
 * transfer-to-underwriting.js
 *
 * Creates or updates a Podio Underwriting app record when a deal must be
 * routed away from the single-family cash offer flow.
 *
 * Use cases:
 *   • Multifamily or commercial property
 *   • Creative finance / Subject To / Novation deal strategy
 *   • Seller message contains financing/rental-income language
 *
 * This module NEVER blocks the normal inbound SMS handling pipeline.
 * If the Podio transfer fails for any reason, the error is logged and
 * diagnostic information is returned — the caller's pipeline continues.
 *
 * Exported API:
 *   transferDealToUnderwriting({ owner, property, prospect, phone,
 *     sellerMessage, routeReason, sourceMessageEventId })
 *     → { ok, underwriting_item_id?, created?, updated?, diagnostics }
 *
 * Test injection:
 *   __setUnderwritingTransferDeps / __resetUnderwritingTransferDeps
 */

import {
  UNDERWRITING_FIELDS,
  createUnderwritingItem,
  findUnderwritingItems,
  updateUnderwritingItem,
} from "@/lib/podio/apps/underwriting.js";

import { warn } from "@/lib/logging/logger.js";

// ---------------------------------------------------------------------------
// Test dependency injection
// ---------------------------------------------------------------------------

let _deps = {
  create_underwriting_item:  null,
  find_underwriting_items:   null,
  update_underwriting_item:  null,
};

/** Override Podio helpers for unit tests. */
export function __setUnderwritingTransferDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

/** Reset injected dependencies to production defaults. */
export function __resetUnderwritingTransferDeps() {
  _deps = {
    create_underwriting_item:  null,
    find_underwriting_items:   null,
    update_underwriting_item:  null,
  };
}

function getCreate()  { return _deps.create_underwriting_item  ?? createUnderwritingItem; }
function getFind()    { return _deps.find_underwriting_items   ?? findUnderwritingItems; }
function getUpdate()  { return _deps.update_underwriting_item  ?? updateUnderwritingItem; }

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function safeNum(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * Determine "Multifamily" or "Creative" underwriting type from context.
 */
function deriveUnderwritingType({ property = {}, dealStrategy = null, routeReason = "" } = {}) {
  const pt       = lower(property?.property_type ?? "");
  const strategy = lower(dealStrategy ?? "");
  const reason   = lower(routeReason ?? "");

  const mf_keywords = [
    "multifamily", "multi-family", "multi family",
    "apartment", "duplex", "triplex", "quadplex", "4-plex",
    "commercial", "mixed use",
  ];
  const is_mf = mf_keywords.some((kw) => pt.includes(kw) || reason.includes(kw));
  if (is_mf) return "Multifamily";

  const creative_keywords = [
    "creative", "seller finance", "subject to", "subject-to",
    "novation", "owner finance", "owner financing", "wrap",
  ];
  const is_creative = creative_keywords.some((kw) => strategy.includes(kw) || reason.includes(kw));
  if (is_creative) return "Creative";

  return "Creative"; // safe fallback — anything routed here needs review
}

/**
 * Look for an existing Underwriting item linked to this deal.
 * Resolution order: property_id → prospect_id → master_owner_id.
 */
async function findExistingItem({ property_id, prospect_id, master_owner_id } = {}) {
  const findFn = getFind();

  if (property_id) {
    const matches = await findFn({ [UNDERWRITING_FIELDS.property]: property_id }, 10, 0)
      .catch(() => []);
    if (matches?.length) {
      return [...matches].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0];
    }
  }

  if (prospect_id) {
    const matches = await findFn({ [UNDERWRITING_FIELDS.prospect]: prospect_id }, 10, 0)
      .catch(() => []);
    if (matches?.length) {
      return [...matches].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0];
    }
  }

  if (master_owner_id) {
    const matches = await findFn({ [UNDERWRITING_FIELDS.master_owner]: master_owner_id }, 10, 0)
      .catch(() => []);
    if (matches?.length) {
      return [...matches].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0];
    }
  }

  return null;
}

/**
 * Extract lightweight numeric signals from seller message text without
 * pulling in the full extractUnderwritingSignals module (to keep this
 * file focused and independently testable).
 */
function extractMessageSignals(sellerMessage = "") {
  const text = lower(sellerMessage);
  const signals = {};

  // Unit count
  const unit_match = text.match(/(\d+)\s*(?:unit|units|door|doors)\b/);
  if (unit_match) signals.units_mentioned = safeNum(unit_match[1]);

  // Mortgage balance
  const mortgage_match = text.match(/mortgage(?:\s*balance)?\s*(?:is|of|about|around)?\s*\$?\s*(\d[\d,]*)/i);
  if (mortgage_match) signals.mortgage_balance = safeNum(mortgage_match[1].replace(/,/g, ""));

  // Monthly payment
  const payment_match = text.match(/(?:payment|paying)\s*(?:is|of|about|around)?\s*\$?\s*(\d[\d,]*)/i);
  if (payment_match) signals.monthly_payment = safeNum(payment_match[1].replace(/,/g, ""));

  // Monthly rents
  const rents_match = text.match(/(?:rents?|rental income)\s*(?:is|of|are|about|around)?\s*\$?\s*(\d[\d,]*)/i);
  if (rents_match) signals.gross_rents = safeNum(rents_match[1].replace(/,/g, ""));

  // Occupancy %
  const occ_match = text.match(/(\d+)\s*%\s*(?:occupied|occupancy)/i);
  if (occ_match) signals.occupancy_pct = safeNum(occ_match[1]);

  // NOI
  const noi_match = text.match(/noi\s*(?:is|of|about|around)?\s*\$?\s*(\d[\d,]*)/i);
  if (noi_match) signals.noi = safeNum(noi_match[1].replace(/,/g, ""));

  return signals;
}

/**
 * Build the Podio field map for a new or updated Underwriting item.
 */
function buildPayload({
  owner,
  property,
  prospect,
  phone,
  underwriting_type,
  routeReason,
  sellerMessage,
  sourceMessageEventId,
  message_signals,
} = {}) {
  const fields = {};

  // Core classification
  if (underwriting_type) {
    fields[UNDERWRITING_FIELDS.underwriting_type]   = underwriting_type;
  }
  fields[UNDERWRITING_FIELDS.underwriting_status]   = "Intake Started";

  // Source attribution
  fields[UNDERWRITING_FIELDS.reason_sent_to_underwriting] = clean(routeReason).slice(0, 500) || "SMS Automation";

  if (clean(sellerMessage)) {
    // latest_seller_message → escalation_summary field
    fields[UNDERWRITING_FIELDS.escalation_summary]  = clean(sellerMessage).slice(0, 2000);
  }

  // Relationships (use array format expected by Podio app refs)
  const property_id     = property?.item_id ?? property?.property_id ?? null;
  const master_owner_id = owner?.item_id    ?? owner?.master_owner_id ?? null;
  const prospect_id     = prospect?.item_id ?? prospect?.prospect_id  ?? null;
  const phone_id        = phone?.item_id    ?? phone?.phone_id        ?? null;

  if (property_id)     fields[UNDERWRITING_FIELDS.property]     = [Number(property_id)];
  if (master_owner_id) fields[UNDERWRITING_FIELDS.master_owner] = [Number(master_owner_id)];
  if (prospect_id)     fields[UNDERWRITING_FIELDS.prospect]     = [Number(prospect_id)];
  if (phone_id)        fields[UNDERWRITING_FIELDS.phone_number] = [Number(phone_id)];

  // Numeric signals extracted from the seller message
  if (safeNum(message_signals?.units_mentioned) !== null) {
    fields[UNDERWRITING_FIELDS.number_of_units_snapshot] = message_signals.units_mentioned;
  }
  if (safeNum(message_signals?.gross_rents) !== null) {
    fields[UNDERWRITING_FIELDS.current_gross_rents] = message_signals.gross_rents;
  }
  if (safeNum(message_signals?.occupancy_pct) !== null) {
    fields[UNDERWRITING_FIELDS.occupancy_at_underwriting] = message_signals.occupancy_pct;
  }
  if (safeNum(message_signals?.noi) !== null) {
    fields[UNDERWRITING_FIELDS.noi] = message_signals.noi;
  }
  if (safeNum(message_signals?.mortgage_balance) !== null) {
    fields[UNDERWRITING_FIELDS.existing_mortgage_balance] = message_signals.mortgage_balance;
  }
  if (safeNum(message_signals?.monthly_payment) !== null) {
    fields[UNDERWRITING_FIELDS.existing_mortgage_payment] = message_signals.monthly_payment;
  }

  // Automation metadata
  fields[UNDERWRITING_FIELDS.automation_status] = "Routed";
  fields[UNDERWRITING_FIELDS.triggered_at]      = nowIso();

  return fields;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Transfer a deal to the Podio Underwriting app.
 *
 * This function NEVER throws.  All errors are caught, logged, and returned
 * as structured diagnostics.  The calling inbound pipeline must continue
 * even if this call fails.
 *
 * @param {{
 *   owner?:              object - Podio master owner item or { item_id, master_owner_id }
 *   property?:           object - Podio property item or { item_id, property_id }
 *   prospect?:           object - Podio prospect item or { item_id, prospect_id }
 *   phone?:              object - Podio phone item or { item_id }
 *   sellerMessage?:      string - Latest inbound message body
 *   routeReason?:        string - Human-readable reason from getUnderwritingRouteReason()
 *   dealStrategy?:       string - e.g. "Creative", "Multifamily", "Subject To"
 *   sourceMessageEventId?: string
 * }}
 * @returns {Promise<{
 *   ok: boolean,
 *   underwriting_item_id: number|null,
 *   created: boolean,
 *   updated: boolean,
 *   underwriting_type: string|null,
 *   diagnostics: object,
 * }>}
 */
export async function transferDealToUnderwriting({
  owner               = null,
  property            = null,
  prospect            = null,
  phone               = null,
  sellerMessage       = null,
  routeReason         = "",
  dealStrategy        = null,
  sourceMessageEventId = null,
} = {}) {
  const diagnostics = {
    route_reason:            clean(routeReason) || null,
    deal_strategy:           clean(dealStrategy) || null,
    source_message_event_id: clean(sourceMessageEventId) || null,
    property_id:             property?.item_id ?? property?.property_id ?? null,
    master_owner_id:         owner?.item_id    ?? owner?.master_owner_id ?? null,
    prospect_id:             prospect?.item_id ?? prospect?.prospect_id  ?? null,
    attempted_at:            nowIso(),
  };

  try {
    const underwriting_type = deriveUnderwritingType({ property, dealStrategy, routeReason });
    diagnostics.underwriting_type = underwriting_type;

    const message_signals = extractMessageSignals(sellerMessage ?? "");
    diagnostics.extracted_signals = message_signals;

    const payload = buildPayload({
      owner,
      property,
      prospect,
      phone,
      underwriting_type,
      routeReason,
      sellerMessage,
      sourceMessageEventId,
      message_signals,
    });

    // Try to find existing Underwriting item for this deal
    const existing = await findExistingItem({
      property_id:     property?.item_id     ?? property?.property_id     ?? null,
      prospect_id:     prospect?.item_id     ?? prospect?.prospect_id     ?? null,
      master_owner_id: owner?.item_id        ?? owner?.master_owner_id    ?? null,
    });

    if (existing?.item_id) {
      await getUpdate()(existing.item_id, payload);
      return {
        ok: true,
        underwriting_item_id: existing.item_id,
        created: false,
        updated: true,
        underwriting_type,
        diagnostics,
      };
    }

    const created = await getCreate()(payload);
    return {
      ok: true,
      underwriting_item_id: created?.item_id ?? null,
      created: true,
      updated: false,
      underwriting_type,
      diagnostics,
    };
  } catch (err) {
    // Log but never propagate — inbound pipeline must not be blocked.
    warn("transfer_to_underwriting.failed", {
      error:          String(err?.message ?? err),
      route_reason:   routeReason,
      property_id:    property?.item_id ?? null,
    });

    return {
      ok: false,
      underwriting_item_id: null,
      created: false,
      updated: false,
      underwriting_type: null,
      diagnostics: {
        ...diagnostics,
        error: String(err?.message ?? err),
      },
    };
  }
}
