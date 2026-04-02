import { getPropertyItem } from "@/lib/podio/apps/properties.js";
import { buildOfferPayload } from "@/lib/domain/offers/build-offer-payload.js";
import { selectOfferStrategy } from "@/lib/domain/offers/select-offer-strategy.js";
import {
  getCategoryValue,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";

function buildPreviewContext(property_item = null, property_id = null) {
  return {
    found: true,
    ids: {
      property_id,
    },
    items: {
      property_item,
    },
    summary: {
      property_address:
        getTextValue(property_item, "property-address", "") ||
        getTextValue(property_item, "full-name", ""),
    },
  };
}

export async function calculateOffer({
  property_id = null,
  strategy = null,
  arv = null,
  repairs = null,
} = {}) {
  if (!property_id) {
    return {
      ok: false,
      reason: "missing_property_id",
    };
  }

  const property_item = await getPropertyItem(property_id);

  if (!property_item?.item_id) {
    return {
      ok: false,
      reason: "property_not_found",
      property_id,
    };
  }

  const strategy_result = selectOfferStrategy({
    property_type: getCategoryValue(property_item, "property-type", "Residential"),
    unit_count: getNumberValue(property_item, "number-of-units", null),
    requested_strategy: strategy || null,
    has_sfr_cash_offer: getNumberValue(property_item, "smart-cash-offer-2", null) !== null,
    has_multifamily_cash_offer:
      getNumberValue(property_item, "smart-cash-offer-2", null) !== null,
  });

  const preview = buildOfferPayload({
    context: buildPreviewContext(property_item, property_id),
    strategy_result,
    property_item,
    notes: [arv !== null ? `ARV: ${arv}` : "", repairs !== null ? `Repairs: ${repairs}` : ""]
      .filter(Boolean)
      .join("\n"),
    created_by: "Internal Offer Preview",
  });

  return {
    ok: true,
    calculated: true,
    reason: "offer_preview_built",
    property_id,
    strategy_result,
    offer_amount: preview.offer_amount,
    offer_type: preview.offer_type,
    payload: preview.payload,
    inputs: {
      property_id,
      strategy: strategy || null,
      arv,
      repairs,
    },
  };
}

export default calculateOffer;
