import {
  findProspectBySellerId,
  findProspectItems,
} from "@/lib/podio/apps/prospects.js";

export async function findProspect({ seller_id = null, filters = null }) {
  if (seller_id) {
    const found = await findProspectBySellerId(seller_id);
    if (found) return found;
  }

  if (filters) {
    const res = await findProspectItems(filters, 1, 0);
    return res?.items?.[0] ?? res?.[0] ?? null;
  }

  return null;
}

export default findProspect;