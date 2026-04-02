import {
  TITLE_ROUTING_FIELDS,
  findTitleRoutingItems,
  getTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import { maybeCreateClosingFromTitleRouting } from "@/lib/domain/closings/maybe-create-closing-from-title-routing.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

async function resolveTitleRouting({
  title_routing_id = null,
  contract_id = null,
} = {}) {
  if (title_routing_id) {
    return getTitleRoutingItem(title_routing_id);
  }

  if (!contract_id) return null;

  return sortNewestFirst(
    await findTitleRoutingItems(
      { [TITLE_ROUTING_FIELDS.contract]: contract_id },
      50,
      0
    )
  )[0] || null;
}

export async function createClosingFlow({
  contract_id = null,
  title_routing_id = null,
} = {}) {
  const title_routing_item = await resolveTitleRouting({
    title_routing_id,
    contract_id,
  });

  if (!title_routing_item?.item_id) {
    return {
      ok: false,
      created: false,
      reason: "title_routing_not_found",
      contract_id,
      title_routing_id,
    };
  }

  return maybeCreateClosingFromTitleRouting({
    title_routing_item_id: title_routing_item.item_id,
    title_routing_item,
    source: "Internal Closing Flow",
  });
}

export default createClosingFlow;
