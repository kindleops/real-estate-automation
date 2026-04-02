import {
  CLOSING_FIELDS,
  findClosingById,
  findClosingItems,
  getClosingItem,
} from "@/lib/podio/apps/closings.js";
import { updateClosingStatus } from "@/lib/domain/closings/update-closing-status.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

async function resolveClosing({ closing_id = null, contract_id = null } = {}) {
  if (closing_id) {
    return (await getClosingItem(closing_id)) || (await findClosingById(String(closing_id))) || null;
  }

  if (!contract_id) return null;

  return sortNewestFirst(
    await findClosingItems(
      { [CLOSING_FIELDS.contract]: contract_id },
      50,
      0
    )
  )[0] || null;
}

export async function syncClosingMilestones({
  closing_id = null,
  contract_id = null,
  status = null,
  notes = "",
} = {}) {
  const closing_item = await resolveClosing({
    closing_id,
    contract_id,
  });

  if (!closing_item?.item_id) {
    return {
      ok: false,
      updated: false,
      reason: "closing_not_found",
      closing_id,
      contract_id,
    };
  }

  if (status) {
    return updateClosingStatus({
      closing_item_id: closing_item.item_id,
      closing_item,
      status,
      notes,
    });
  }

  return {
    ok: true,
    updated: false,
    reason: "closing_snapshot_only",
    closing_item_id: closing_item.item_id,
    closing_status: getCategoryValue(closing_item, CLOSING_FIELDS.closing_status, null),
    closing_item,
  };
}

export default syncClosingMilestones;
