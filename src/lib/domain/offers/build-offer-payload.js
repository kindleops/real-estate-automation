// ─── build-offer-payload.js ──────────────────────────────────────────────
import { OFFER_FIELDS, normalizeOfferStatus } from "@/lib/podio/apps/offers.js";
import { getNumberValue } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function toNumber(value, fallback = null) {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return fallback;
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function nowIso() {
  return new Date().toISOString();
}

function firstNonNull(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }
  return null;
}

function toAppRef(item_id) {
  return item_id ? [item_id] : undefined;
}

function normalizeOfferType(strategy = "standard_cash") {
  const normalized = clean(strategy).toLowerCase();

  if (normalized.includes("creative") || normalized.includes("subject")) {
    return "Subject To";
  }
  if (normalized.includes("novation")) return "Novation";
  if (normalized.includes("multifamily")) return "Multi Family";
  return "Cash";
}

function buildRationaleText(rationale = []) {
  return safeArray(rationale)
    .map((item) => clean(item))
    .filter(Boolean)
    .join(" | ");
}

function buildOfferNotes({
  notes = "",
  strategy = "",
  strategy_source = "",
  motivation_band = "",
  created_by = "",
  rationale = [],
  signals = {},
} = {}) {
  const parts = [
    clean(notes),
    clean(strategy) ? `Strategy: ${clean(strategy)}` : "",
    clean(strategy_source) ? `Strategy Source: ${clean(strategy_source)}` : "",
    clean(motivation_band) ? `Motivation Band: ${clean(motivation_band)}` : "",
    clean(created_by) ? `Created By: ${clean(created_by)}` : "",
    buildRationaleText(rationale) ? `Rationale: ${buildRationaleText(rationale)}` : "",
    signals && Object.keys(signals).length
      ? `Signals: ${JSON.stringify(signals)}`
      : "",
  ];

  return parts.filter(Boolean).join("\n");
}

function resolveSellerAskingPrice({ underwriting_result = null } = {}) {
  return (
    toNumber(underwriting_result?.seller_asking_price, null) ??
    toNumber(underwriting_result?.asking_price, null) ??
    null
  );
}

function resolveSellerCounterOffer({ underwriting_result = null } = {}) {
  return (
    toNumber(underwriting_result?.seller_counter_offer, null) ??
    toNumber(underwriting_result?.counter_offer, null) ??
    null
  );
}

function resolveOfferAmount({
  strategy_result = null,
  property_item = null,
  explicit_offer_amount = null,
  underwriting_result = null,
} = {}) {
  if (explicit_offer_amount !== null && explicit_offer_amount !== undefined) {
    return toNumber(explicit_offer_amount, null);
  }

  const strategy = clean(strategy_result?.strategy).toLowerCase();
  const use_existing_property_offer = Boolean(
    strategy_result?.flags?.use_existing_property_offer
  );

  const underwriting_offer =
    toNumber(underwriting_result?.offer_amount, null) ??
    toNumber(underwriting_result?.recommended_offer, null) ??
    null;

  if (underwriting_offer !== null) {
    return underwriting_offer;
  }

  if (!property_item) return null;

  const smart_cash_offer =
    getNumberValue(property_item, "smart-cash-offer-2", null) ??
    null;

  const multifamily_cash_offer =
    getNumberValue(property_item, "smart-cash-offer-2", null) ??
    null;

  if (strategy.includes("multifamily")) {
    return multifamily_cash_offer ?? smart_cash_offer;
  }

  if (use_existing_property_offer) {
    return smart_cash_offer;
  }

  return smart_cash_offer;
}

function resolveOfferMetrics({
  property_item = null,
  underwriting_result = null,
} = {}) {
  const underwriting = underwriting_result || {};

  return {
    offer_ppsf:
      toNumber(underwriting.offer_ppsf, null) ??
      getNumberValue(property_item, "offer-ppsf", null) ??
      null,

    offer_ppu:
      toNumber(underwriting.offer_ppu, null) ??
      getNumberValue(property_item, "offer-ppu", null) ??
      null,

    offer_ppls:
      toNumber(underwriting.offer_ppls, null) ??
      getNumberValue(property_item, "offer-ppls", null) ??
      null,

    offer_ppbd:
      toNumber(underwriting.offer_ppbd, null) ??
      getNumberValue(property_item, "offer-ppbd", null) ??
      null,
  };
}

export function buildOfferPayload({
  context = null,
  strategy_result = null,
  property_item = null,
  underwriting_result = null,
  explicit_offer_amount = null,
  offer_status = "Offer Sent",
  offer_id = null,
  offer_label = null,
  notes = "",
  created_by = "AI Offer Engine",
} = {}) {
  const ids = context?.ids || {};
  const summary = context?.summary || {};

  const strategy = strategy_result?.strategy || "standard_cash";
  const strategy_source = strategy_result?.strategy_source || "default";
  const motivation_band = strategy_result?.motivation_band || "unknown";
  const rationale = safeArray(strategy_result?.rationale);
  const flags = strategy_result?.flags || {};
  const signals = strategy_result?.signals || {};

  const resolved_property_item = property_item || context?.items?.property_item || null;
  const resolved_offer_amount = resolveOfferAmount({
    strategy_result,
    property_item: resolved_property_item,
    explicit_offer_amount,
    underwriting_result,
  });

  const offer_type = normalizeOfferType(strategy);
  const normalized_status = normalizeOfferStatus(offer_status);
  const seller_asking_price = resolveSellerAskingPrice({ underwriting_result });
  const seller_counter_offer = resolveSellerCounterOffer({ underwriting_result });
  const offer_notes = buildOfferNotes({
    notes,
    strategy,
    strategy_source,
    motivation_band,
    created_by,
    rationale,
    signals,
  });

  const payload = {
    [OFFER_FIELDS.title]:
      offer_label ||
      offer_id ||
      [
        clean(summary.owner_name || "Seller"),
        clean(summary.property_address || "Property"),
        clean(strategy),
      ]
        .filter(Boolean)
        .join(" - "),

    [OFFER_FIELDS.offer_id]: offer_id || undefined,
    [OFFER_FIELDS.offer_status]: normalized_status,
    [OFFER_FIELDS.offer_type]: offer_type,
    [OFFER_FIELDS.offer_sent_price]:
      resolved_offer_amount !== null ? resolved_offer_amount : undefined,
    [OFFER_FIELDS.seller_asking_price]:
      seller_asking_price !== null ? seller_asking_price : undefined,
    [OFFER_FIELDS.seller_counter_offer]:
      seller_counter_offer !== null ? seller_counter_offer : undefined,
    [OFFER_FIELDS.offer_date]: { start: nowIso() },
    [OFFER_FIELDS.notes]: offer_notes || undefined,
    [OFFER_FIELDS.master_owner]: toAppRef(ids.master_owner_id),
    [OFFER_FIELDS.prospect]: toAppRef(ids.prospect_id),
    [OFFER_FIELDS.property]: toAppRef(ids.property_id),
    [OFFER_FIELDS.market]: toAppRef(ids.market_id),
    [OFFER_FIELDS.phone_number]: toAppRef(ids.phone_item_id),
    [OFFER_FIELDS.conversation]: toAppRef(ids.brain_item_id),
    [OFFER_FIELDS.assigned_agent]: toAppRef(ids.assigned_agent_id),
  };

  Object.keys(payload).forEach((key) => {
    if (payload[key] === undefined || payload[key] === null || payload[key] === "") {
      delete payload[key];
    }
  });

  return {
    ok: true,
    offer_amount: resolved_offer_amount,
    offer_type,
    payload,
  };
}

export default buildOfferPayload;
