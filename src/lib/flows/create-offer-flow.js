import { getPropertyItem } from "@/lib/podio/apps/properties.js";
import { createOffer } from "@/lib/domain/offers/create-offer.js";
import {
  getCategoryValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";

function buildOfferContext({
  property_item = null,
  property_id = null,
  master_owner_id = null,
  prospect_id = null,
} = {}) {
  const market_id =
    getFirstAppReferenceId(property_item, "market-2", null) ??
    getFirstAppReferenceId(property_item, "market", null) ??
    null;

  return {
    found: true,
    ids: {
      property_id,
      master_owner_id,
      prospect_id,
      market_id,
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

export async function createOfferFlow({
  property_id = null,
  master_owner_id = null,
  prospect_id = null,
  strategy = null,
} = {}) {
  if (!property_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_property_id",
    };
  }

  const property_item = await getPropertyItem(property_id);

  if (!property_item?.item_id) {
    return {
      ok: false,
      created: false,
      reason: "property_not_found",
      property_id,
    };
  }

  const context = buildOfferContext({
    property_item,
    property_id,
    master_owner_id,
    prospect_id,
  });

  const result = await createOffer({
    context,
    property_item,
    property_type: getCategoryValue(property_item, "property-type", "Residential"),
    requested_strategy: strategy || null,
    created_by: "Internal Offer Flow",
  });

  return {
    ok: Boolean(result?.ok),
    created: Boolean(result?.offer_item_id),
    reason: result?.ok ? "offer_created" : "offer_create_failed",
    property_id,
    offer: result,
  };
}

export default createOfferFlow;
