import {
  findMasterOwnerBySellerId,
  findMasterOwnerItems,
} from "@/lib/podio/apps/master-owners.js";

export async function findOwner({ seller_id = null, filters = null }) {
  if (seller_id) {
    const found = await findMasterOwnerBySellerId(seller_id);
    if (found) return found;
  }

  if (filters) {
    const res = await findMasterOwnerItems(filters, 1, 0);
    return res?.items?.[0] ?? res?.[0] ?? null;
  }

  return null;
}

export default findOwner;