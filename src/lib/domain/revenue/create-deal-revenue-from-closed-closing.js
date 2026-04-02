// ─── create-deal-revenue-from-closed-closing.js ──────────────────────────
import { CLOSING_FIELDS, getClosingItem } from "@/lib/podio/apps/closings.js";
import {
  DEAL_REVENUE_FIELDS,
  createDealRevenueItem,
  findDealRevenueItems,
} from "@/lib/podio/apps/deal-revenue.js";
import { getDateValue, getFirstAppReferenceId } from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const normalized = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(normalized) ? normalized : null;
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

function asAppRef(value) {
  if (!value) return undefined;
  return [value];
}

function buildDealRevenueId({
  closing_item_id = null,
  property_id = null,
} = {}) {
  const stamp = Date.now();

  if (closing_item_id) return `REV-${closing_item_id}-${stamp}`;
  if (property_id) return `REV-P-${property_id}-${stamp}`;

  return `REV-${stamp}`;
}

function isClosedClosing(closing_item = null) {
  const fields = Array.isArray(closing_item?.fields) ? closing_item.fields : [];
  const statusField = fields.find((entry) => entry?.external_id === CLOSING_FIELDS.closing_status);
  const first = statusField?.values?.[0];
  const value = first?.value?.text || first?.value || "";
  return clean(value).toLowerCase() === "completed";
}

async function findLatestRevenueByClosingId(closing_item_id) {
  if (!closing_item_id) return null;

  const matches = await findDealRevenueItems(
    { [DEAL_REVENUE_FIELDS.closing]: closing_item_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

function deriveRefs(closing_item = null) {
  return {
    contract_item_id: getFirstAppReferenceId(closing_item, CLOSING_FIELDS.contract, null),
    buyer_match_item_id: getFirstAppReferenceId(closing_item, CLOSING_FIELDS.buyer_match, null),
    master_owner_id: getFirstAppReferenceId(closing_item, CLOSING_FIELDS.master_owner, null),
    property_id: getFirstAppReferenceId(closing_item, CLOSING_FIELDS.property, null),
    title_company_item_id: getFirstAppReferenceId(closing_item, CLOSING_FIELDS.title_company, null),
    market_item_id: getFirstAppReferenceId(closing_item, CLOSING_FIELDS.market, null),
    actual_closing_date: getDateValue(closing_item, CLOSING_FIELDS.actual_closing_date, null),
  };
}

export async function createDealRevenueFromClosedClosing({
  closing_item_id = null,
  closing_item = null,
  assignment_fee = null,
  purchase_price = null,
  resale_price = null,
  deal_revenue_id = null,
  revenue_status = "Expected Soon",
} = {}) {
  let resolved_closing_item = closing_item || null;

  if (!resolved_closing_item && closing_item_id) {
    resolved_closing_item = await getClosingItem(closing_item_id);
  }

  const resolved_closing_item_id =
    resolved_closing_item?.item_id ||
    closing_item_id ||
    null;

  if (!resolved_closing_item_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_closing_item_id",
    };
  }

  if (!resolved_closing_item || !isClosedClosing(resolved_closing_item)) {
    return {
      ok: true,
      created: false,
      reason: "closing_not_closed",
      closing_item_id: resolved_closing_item_id,
    };
  }

  const existing_revenue = await findLatestRevenueByClosingId(
    resolved_closing_item_id
  );

  if (existing_revenue?.item_id) {
    const pipeline = await syncPipelineState({
      closing_item_id: resolved_closing_item_id,
      deal_revenue_item_id: existing_revenue.item_id,
      notes: "Existing deal revenue found for closed closing.",
    });

    return {
      ok: true,
      created: false,
      reason: "existing_revenue_found",
      closing_item_id: resolved_closing_item_id,
      deal_revenue_item_id: existing_revenue.item_id,
      existing_revenue,
      pipeline,
    };
  }

  const refs = deriveRefs(resolved_closing_item);
  const generated_deal_revenue_id =
    clean(deal_revenue_id) ||
    buildDealRevenueId({
      closing_item_id: resolved_closing_item_id,
      property_id: refs.property_id,
    });

  const purchase_amount = asNumber(purchase_price);
  const sold_amount = asNumber(resale_price);
  const assignment_amount =
    asNumber(assignment_fee) ??
    (
      purchase_amount !== null && sold_amount !== null
        ? sold_amount - purchase_amount
        : null
    );

  const payload = {
    [DEAL_REVENUE_FIELDS.revenue_id]: generated_deal_revenue_id,
    [DEAL_REVENUE_FIELDS.revenue_status]: clean(revenue_status) || "Expected Soon",
    [DEAL_REVENUE_FIELDS.wire_received]: "No",
    ...(refs.actual_closing_date
      ? { [DEAL_REVENUE_FIELDS.expected_wire_date]: { start: refs.actual_closing_date } }
      : {}),
    ...(refs.contract_item_id
      ? { [DEAL_REVENUE_FIELDS.contract]: asAppRef(refs.contract_item_id) }
      : {}),
    ...(resolved_closing_item_id
      ? { [DEAL_REVENUE_FIELDS.closing]: asAppRef(resolved_closing_item_id) }
      : {}),
    ...(refs.property_id
      ? { [DEAL_REVENUE_FIELDS.property]: asAppRef(refs.property_id) }
      : {}),
    ...(refs.master_owner_id
      ? { [DEAL_REVENUE_FIELDS.master_owner]: asAppRef(refs.master_owner_id) }
      : {}),
    ...(refs.buyer_match_item_id
      ? { [DEAL_REVENUE_FIELDS.buyer]: asAppRef(refs.buyer_match_item_id) }
      : {}),
    ...(refs.title_company_item_id
      ? { [DEAL_REVENUE_FIELDS.title_company]: asAppRef(refs.title_company_item_id) }
      : {}),
    ...(refs.market_item_id
      ? { [DEAL_REVENUE_FIELDS.market]: asAppRef(refs.market_item_id) }
      : {}),
    ...(purchase_amount !== null
      ? { [DEAL_REVENUE_FIELDS.purchase_price]: purchase_amount }
      : {}),
    ...(sold_amount !== null
      ? { [DEAL_REVENUE_FIELDS.sold_price]: sold_amount }
      : {}),
    ...(assignment_amount !== null
      ? { [DEAL_REVENUE_FIELDS.assignment_fee]: assignment_amount }
      : {}),
  };

  const created = await createDealRevenueItem(payload);
  const pipeline = await syncPipelineState({
    contract_item_id: refs.contract_item_id,
    closing_item_id: resolved_closing_item_id,
    deal_revenue_item_id: created?.item_id || null,
    property_id: refs.property_id,
    master_owner_id: refs.master_owner_id,
    market_id: refs.market_item_id,
    notes: "Deal revenue created from closed closing.",
  });

  return {
    ok: true,
    created: true,
    reason: "deal_revenue_created_from_closed_closing",
    closing_item_id: resolved_closing_item_id,
    deal_revenue_item_id: created?.item_id || null,
    deal_revenue_id: generated_deal_revenue_id,
    pipeline,
    payload,
    raw: created,
  };
}

export default createDealRevenueFromClosedClosing;
