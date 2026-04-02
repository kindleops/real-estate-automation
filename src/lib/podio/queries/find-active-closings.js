import {
  CLOSING_FIELDS,
  findClosingById,
  findClosingItems,
} from "@/lib/podio/apps/closings.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function isActiveClosingStatus(status = "") {
  const normalized = String(status || "").trim().toLowerCase();
  return !["completed", "cancelled"].includes(normalized);
}

export async function findActiveClosings({
  closing_id = null,
  contract_id = null,
  title_routing_id = null,
} = {}) {
  if (closing_id) {
    const direct = await findClosingById(closing_id);
    if (direct && isActiveClosingStatus(getCategoryValue(direct, CLOSING_FIELDS.closing_status, ""))) {
      return [direct];
    }
  }

  const filters = {};

  if (contract_id) filters[CLOSING_FIELDS.contract] = contract_id;
  if (title_routing_id) filters[CLOSING_FIELDS.title_routing] = title_routing_id;
  if (!Object.keys(filters).length) return [];

  return sortNewestFirst(await findClosingItems(filters, 50, 0)).filter((item) =>
    isActiveClosingStatus(getCategoryValue(item, CLOSING_FIELDS.closing_status, ""))
  );
}

export default findActiveClosings;
