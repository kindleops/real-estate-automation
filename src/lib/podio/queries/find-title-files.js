import {
  TITLE_ROUTING_FIELDS,
  findTitleRoutingById,
  findTitleRoutingItems,
} from "@/lib/podio/apps/title-routing.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function isActiveTitleFile(status = "") {
  const normalized = String(status || "").trim().toLowerCase();
  return !["closed", "cancelled"].includes(normalized);
}

export async function findTitleFiles({
  title_routing_id = null,
  contract_id = null,
  closing_id = null,
} = {}) {
  if (title_routing_id) {
    const direct = await findTitleRoutingById(title_routing_id);
    if (direct && isActiveTitleFile(getCategoryValue(direct, TITLE_ROUTING_FIELDS.routing_status, ""))) {
      return [direct];
    }
  }

  const filters = {};

  if (contract_id) filters[TITLE_ROUTING_FIELDS.contract] = contract_id;
  if (closing_id) filters[TITLE_ROUTING_FIELDS.closing] = closing_id;
  if (!Object.keys(filters).length) return [];

  return sortNewestFirst(await findTitleRoutingItems(filters, 50, 0)).filter((item) =>
    isActiveTitleFile(getCategoryValue(item, TITLE_ROUTING_FIELDS.routing_status, ""))
  );
}

export default findTitleFiles;
