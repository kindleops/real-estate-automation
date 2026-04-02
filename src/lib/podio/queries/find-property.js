import {
  findPropertyByPropertyId,
  findPropertyItems,
} from "@/lib/podio/apps/properties.js";

export async function findProperty({ property_id = null, filters = null }) {
  if (property_id) {
    const found = await findPropertyByPropertyId(property_id);
    if (found) return found;
  }

  if (filters) {
    const res = await findPropertyItems(filters, 1, 0);
    return res?.items?.[0] ?? res?.[0] ?? null;
  }

  return null;
}

export default findProperty;