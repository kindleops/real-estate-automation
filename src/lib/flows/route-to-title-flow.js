import { getContractItem, CONTRACT_FIELDS } from "@/lib/podio/apps/contracts.js";
import {
  TITLE_ROUTING_FIELDS,
  findTitleRoutingItems,
} from "@/lib/podio/apps/title-routing.js";
import { createTitleRoutingFromContract } from "@/lib/domain/title/create-title-routing-from-contract.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function isRouteableStatus(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return [
    "fully executed",
    "sent to title",
    "opened",
    "clear to close",
    "closed",
  ].includes(normalized);
}

export async function routeToTitleFlow({
  contract_id = null,
} = {}) {
  if (!contract_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_contract_id",
    };
  }

  const contract_item = await getContractItem(contract_id);

  if (!contract_item?.item_id) {
    return {
      ok: false,
      created: false,
      reason: "contract_not_found",
      contract_id,
    };
  }

  const contract_status = getCategoryValue(contract_item, CONTRACT_FIELDS.contract_status, null);

  if (!isRouteableStatus(contract_status)) {
    return {
      ok: false,
      created: false,
      reason: "contract_not_routeable",
      contract_item_id: contract_item.item_id,
      contract_status,
    };
  }

  const existing = sortNewestFirst(
    await findTitleRoutingItems(
      { [TITLE_ROUTING_FIELDS.contract]: contract_item.item_id },
      50,
      0
    )
  )[0] || null;

  if (existing?.item_id) {
    return {
      ok: true,
      created: false,
      reason: "existing_title_routing_found",
      contract_item_id: contract_item.item_id,
      title_routing_item_id: existing.item_id,
      existing_title_routing: existing,
    };
  }

  return createTitleRoutingFromContract({
    contract_item_id: contract_item.item_id,
    contract_item,
    source: "Internal Title Flow",
  });
}

export default routeToTitleFlow;
