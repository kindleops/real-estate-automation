import { CLOSING_FIELDS, findClosingItems, getClosingItem } from "@/lib/podio/apps/closings.js";
import { createDealRevenueFromClosedClosing } from "@/lib/domain/revenue/create-deal-revenue-from-closed-closing.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

async function resolveClosing({ closing_id = null, contract_id = null } = {}) {
  if (closing_id) {
    return getClosingItem(closing_id);
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

export async function createDealRevenueFlow({
  closing_id = null,
  contract_id = null,
  assignment_fee = null,
  purchase_price = null,
  resale_price = null,
} = {}) {
  const closing_item = await resolveClosing({
    closing_id,
    contract_id,
  });

  if (!closing_item?.item_id) {
    return {
      ok: false,
      created: false,
      reason: "closing_not_found",
      closing_id,
      contract_id,
    };
  }

  return createDealRevenueFromClosedClosing({
    closing_item_id: closing_item.item_id,
    closing_item,
    assignment_fee,
    purchase_price,
    resale_price,
  });
}

export default createDealRevenueFlow;
