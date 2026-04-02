import { findLatestOpenOffer } from "@/lib/podio/apps/offers.js";

export async function findOpenOffer({
  offer_id = null,
  prospect_id = null,
  master_owner_id = null,
  property_id = null,
} = {}) {
  return findLatestOpenOffer({
    offer_id,
    prospect_id,
    master_owner_id,
    property_id,
  });
}

export default findOpenOffer;
