// ─── create-offer.js ─────────────────────────────────────────────────────
import {
  createOfferItem,
  OFFER_FIELDS,
} from "@/lib/podio/apps/offers.js";
import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";

import { selectOfferStrategy } from "@/lib/domain/offers/select-offer-strategy.js";
import { buildOfferPayload } from "@/lib/domain/offers/build-offer-payload.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function toNumber(value, fallback = null) {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return fallback;
}

function getPropertyOfferSignals(property_item = null) {
  return {
    has_sfr_cash_offer:
      getNumberValue(property_item, "smart-cash-offer-2", null) !== null,

    has_multifamily_cash_offer:
      getNumberValue(property_item, "smart-cash-offer-2", null) !== null,
  };
}

export async function createOffer({
  context = null,
  property_item = null,
  underwriting_result = null,

  property_type = "Residential",
  seller_profile = null,
  motivation_score = null,
  tags = [],
  notes = "",
  unit_count = null,

  requested_strategy = null,
  allow_creative = false,
  allow_novation = false,

  explicit_offer_amount = null,
  offer_status = "Offer Sent",
  offer_id = null,
  offer_label = null,
  created_by = "AI Offer Engine",
} = {}) {
  const resolved_property_item = property_item || context?.items?.property_item || null;
  const summary = context?.summary || {};

  const property_offer_signals = getPropertyOfferSignals(resolved_property_item);

  const strategy_result = selectOfferStrategy({
    property_type,
    seller_profile: seller_profile || summary.seller_profile || null,
    motivation_score:
      motivation_score ??
      summary.motivation_score ??
      null,
    tags,
    notes,
    unit_count,
    requested_strategy,
    has_sfr_cash_offer: property_offer_signals.has_sfr_cash_offer,
    has_multifamily_cash_offer: property_offer_signals.has_multifamily_cash_offer,
    allow_creative,
    allow_novation,
  });

  const payload_result = buildOfferPayload({
    context,
    strategy_result,
    property_item: resolved_property_item,
    underwriting_result,
    explicit_offer_amount,
    offer_status,
    offer_id,
    offer_label,
    notes,
    created_by,
  });

  const created = await createOfferItem(payload_result.payload);
  const pipeline = await syncPipelineState({
    property_id:
      resolved_property_item?.item_id ||
      context?.ids?.property_id ||
      null,
    master_owner_id: context?.ids?.master_owner_id || null,
    prospect_id: context?.ids?.prospect_id || null,
    conversation_item_id: context?.ids?.brain_item_id || null,
    offer_item_id: created?.item_id || null,
    assigned_agent_id: context?.ids?.assigned_agent_id || null,
    market_id:
      context?.ids?.market_id ||
      context?.ids?.market_item_id ||
      null,
    notes: `Offer created by ${String(created_by || "AI Offer Engine").trim()}.`,
  });

  return {
    ok: true,
    offer_item_id: created?.item_id ?? null,
    pipeline,
    strategy_result,
    payload: payload_result.payload,
    offer_amount: payload_result.offer_amount,
    offer_type: payload_result.offer_type,
    strategy: payload_result.strategy,
    strategy_source: payload_result.strategy_source,
    raw: created,
  };
}

export default createOffer;
