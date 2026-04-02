import {
  DEAL_REVENUE_FIELDS,
  findDealRevenueById,
  findDealRevenueItems,
  getDealRevenueItem,
} from "@/lib/podio/apps/deal-revenue.js";
import { createDealRevenueFromClosedClosing } from "@/lib/domain/revenue/create-deal-revenue-from-closed-closing.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

async function resolveDealRevenue({ deal_revenue_id = null, closing_id = null } = {}) {
  if (deal_revenue_id) {
    return (await getDealRevenueItem(deal_revenue_id)) || (await findDealRevenueById(String(deal_revenue_id))) || null;
  }

  if (!closing_id) return null;

  return sortNewestFirst(
    await findDealRevenueItems(
      { [DEAL_REVENUE_FIELDS.closing]: closing_id },
      50,
      0
    )
  )[0] || null;
}

export async function updateDealRevenue({
  deal_revenue_id = null,
  closing_id = null,
} = {}) {
  let deal_revenue_item = await resolveDealRevenue({
    deal_revenue_id,
    closing_id,
  });

  if (!deal_revenue_item?.item_id && closing_id) {
    const created = await createDealRevenueFromClosedClosing({
      closing_item_id: closing_id,
    });

    if (created?.deal_revenue_item_id) {
      deal_revenue_item = await getDealRevenueItem(created.deal_revenue_item_id);
    }

    return {
      ok: Boolean(created?.ok),
      updated: Boolean(created?.created),
      reason: created?.reason || "deal_revenue_sync_attempted",
      deal_revenue_item_id: created?.deal_revenue_item_id || null,
      result: created,
    };
  }

  if (!deal_revenue_item?.item_id) {
    return {
      ok: false,
      updated: false,
      reason: "deal_revenue_not_found",
      deal_revenue_id,
      closing_id,
    };
  }

  return {
    ok: true,
    updated: false,
    reason: "deal_revenue_snapshot_only",
    deal_revenue_item_id: deal_revenue_item.item_id,
    revenue_status: getCategoryValue(deal_revenue_item, DEAL_REVENUE_FIELDS.revenue_status, null),
    deal_revenue_item,
  };
}

export default updateDealRevenue;
