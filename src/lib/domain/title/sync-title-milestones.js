import {
  TITLE_ROUTING_FIELDS,
  findTitleRoutingItems,
  findTitleRoutingById,
  getTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import { updateTitleRoutingStatus } from "@/lib/domain/title/update-title-routing-status.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

async function resolveTitleRouting({ title_routing_id = null, contract_id = null } = {}) {
  if (title_routing_id) {
    return (await getTitleRoutingItem(title_routing_id)) || (await findTitleRoutingById(String(title_routing_id))) || null;
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

export async function syncTitleMilestones({
  title_routing_id = null,
  contract_id = null,
  status = null,
  notes = "",
} = {}) {
  const title_routing_item = await resolveTitleRouting({
    title_routing_id,
    contract_id,
  });

  if (!title_routing_item?.item_id) {
    return {
      ok: false,
      updated: false,
      reason: "title_routing_not_found",
      title_routing_id,
      contract_id,
    };
  }

  if (status) {
    return updateTitleRoutingStatus({
      title_routing_item_id: title_routing_item.item_id,
      title_routing_item,
      status,
      notes,
    });
  }

  return {
    ok: true,
    updated: false,
    reason: "title_routing_snapshot_only",
    title_routing_item_id: title_routing_item.item_id,
    routing_status: getCategoryValue(title_routing_item, TITLE_ROUTING_FIELDS.routing_status, null),
    title_routing_item,
  };
}

export default syncTitleMilestones;
