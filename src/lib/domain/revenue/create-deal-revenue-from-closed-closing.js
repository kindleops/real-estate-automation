// ─── create-deal-revenue-from-closed-closing.js ──────────────────────────
import { CLOSING_FIELDS, getClosingItem } from "@/lib/podio/apps/closings.js";
import {
  BUYER_MATCH_FIELDS,
  getBuyerMatchItem,
  updateBuyerMatchItem,
} from "@/lib/podio/apps/buyer-match.js";
import {
  CONTRACT_FIELDS,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  DEAL_REVENUE_FIELDS,
  createDealRevenueItem,
  findDealRevenueItems,
  updateDealRevenueItem,
} from "@/lib/podio/apps/deal-revenue.js";
import { updateBrainFromExecution } from "@/lib/domain/brain/update-brain-from-execution.js";
import {
  getDateValue,
  getFirstAppReferenceId,
  getMoneyValue,
  getNumberValue,
} from "@/lib/providers/podio.js";
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

  const matches = await runtimeDeps.findDealRevenueItems(
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

function buildRevenuePayload({
  existing_item = null,
  refs = {},
  purchase_amount = null,
  sold_amount = null,
  assignment_amount = null,
  buyer_item_id = null,
  generated_deal_revenue_id = null,
  revenue_status = "Expected Soon",
} = {}) {
  const payload = {
    [DEAL_REVENUE_FIELDS.revenue_id]: generated_deal_revenue_id,
    ...(refs.actual_closing_date
      ? { [DEAL_REVENUE_FIELDS.expected_wire_date]: { start: refs.actual_closing_date } }
      : {}),
    ...(refs.contract_item_id
      ? { [DEAL_REVENUE_FIELDS.contract]: asAppRef(refs.contract_item_id) }
      : {}),
    ...(refs.closing_item_id
      ? { [DEAL_REVENUE_FIELDS.closing]: asAppRef(refs.closing_item_id) }
      : {}),
    ...(refs.property_id
      ? { [DEAL_REVENUE_FIELDS.property]: asAppRef(refs.property_id) }
      : {}),
    ...(refs.master_owner_id
      ? { [DEAL_REVENUE_FIELDS.master_owner]: asAppRef(refs.master_owner_id) }
      : {}),
    ...(buyer_item_id
      ? { [DEAL_REVENUE_FIELDS.buyer]: asAppRef(buyer_item_id) }
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

  if (!existing_item?.item_id) {
    payload[DEAL_REVENUE_FIELDS.revenue_status] = clean(revenue_status) || "Expected Soon";
    payload[DEAL_REVENUE_FIELDS.wire_received] = "No";
  }

  return payload;
}

function deriveFinancials({
  contract_item = null,
  buyer_match_item = null,
  purchase_price = null,
  resale_price = null,
  assignment_fee = null,
} = {}) {
  const purchase_amount =
    asNumber(purchase_price) ??
    getMoneyValue(contract_item, CONTRACT_FIELDS.purchase_price_final, null) ??
    getNumberValue(buyer_match_item, BUYER_MATCH_FIELDS.final_acquisition_price, null) ??
    null;
  const sold_amount =
    asNumber(resale_price) ??
    getNumberValue(buyer_match_item, BUYER_MATCH_FIELDS.final_disposition_price, null) ??
    null;
  const assignment_amount =
    asNumber(assignment_fee) ??
    getNumberValue(buyer_match_item, BUYER_MATCH_FIELDS.assignment_fee, null) ??
    (purchase_amount !== null && sold_amount !== null
      ? sold_amount - purchase_amount
      : null);
  const buyer_item_id =
    getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.selected_buyer, null) ||
    getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.primary_buyer, null) ||
    null;

  return {
    purchase_amount,
    sold_amount,
    assignment_amount,
    buyer_item_id,
  };
}

async function syncLinkedRecords({
  buyer_match_item_id = null,
  deal_revenue_item_id = null,
} = {}) {
  if (!buyer_match_item_id || !deal_revenue_item_id) return;

  await runtimeDeps.updateBuyerMatchItem(buyer_match_item_id, {
    [BUYER_MATCH_FIELDS.deal_revenue]: asAppRef(deal_revenue_item_id),
  });
}

const defaultDeps = {
  getClosingItem,
  findDealRevenueItems,
  createDealRevenueItem,
  updateDealRevenueItem,
  getContractItem,
  getBuyerMatchItem,
  updateBuyerMatchItem,
  syncPipelineState,
  updateBrainFromExecution,
};

let runtimeDeps = { ...defaultDeps };

export function __setDealRevenueFromClosingTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetDealRevenueFromClosingTestDeps() {
  runtimeDeps = { ...defaultDeps };
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
    resolved_closing_item = await runtimeDeps.getClosingItem(closing_item_id);
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
  const refs = {
    ...deriveRefs(resolved_closing_item),
    closing_item_id: resolved_closing_item_id,
  };
  const contract_item = refs.contract_item_id
    ? await runtimeDeps.getContractItem(refs.contract_item_id).catch(() => null)
    : null;
  const buyer_match_item = refs.buyer_match_item_id
    ? await runtimeDeps.getBuyerMatchItem(refs.buyer_match_item_id).catch(() => null)
    : null;
  const financials = deriveFinancials({
    contract_item,
    buyer_match_item,
    purchase_price,
    resale_price,
    assignment_fee,
  });

  if (existing_revenue?.item_id) {
    const payload = buildRevenuePayload({
      existing_item: existing_revenue,
      refs,
      purchase_amount: financials.purchase_amount,
      sold_amount: financials.sold_amount,
      assignment_amount: financials.assignment_amount,
      buyer_item_id: financials.buyer_item_id,
      generated_deal_revenue_id:
        clean(deal_revenue_id) ||
        clean(existing_revenue?.title) ||
        buildDealRevenueId({
          closing_item_id: resolved_closing_item_id,
          property_id: refs.property_id,
        }),
      revenue_status,
    });

    if (Object.keys(payload).length) {
      await runtimeDeps.updateDealRevenueItem(existing_revenue.item_id, payload);
    }

    await syncLinkedRecords({
      buyer_match_item_id: refs.buyer_match_item_id,
      deal_revenue_item_id: existing_revenue.item_id,
    });
    const pipeline = await runtimeDeps.syncPipelineState({
      closing_item_id: resolved_closing_item_id,
      deal_revenue_item_id: existing_revenue.item_id,
      notes: "Existing deal revenue found for closed closing.",
    });
    const brain_update = await runtimeDeps.updateBrainFromExecution({
      source: "revenue",
      closing_item: resolved_closing_item,
      notes: "Revenue exists for a closed closing.",
    });

    return {
      ok: true,
      created: false,
      updated: Object.keys(payload).length > 0,
      reason: "existing_revenue_found",
      closing_item_id: resolved_closing_item_id,
      deal_revenue_item_id: existing_revenue.item_id,
      existing_revenue,
      payload,
      pipeline,
      brain_update,
    };
  }

  const generated_deal_revenue_id =
    clean(deal_revenue_id) ||
    buildDealRevenueId({
      closing_item_id: resolved_closing_item_id,
      property_id: refs.property_id,
    });
  const payload = buildRevenuePayload({
    refs,
    purchase_amount: financials.purchase_amount,
    sold_amount: financials.sold_amount,
    assignment_amount: financials.assignment_amount,
    buyer_item_id: financials.buyer_item_id,
    generated_deal_revenue_id,
    revenue_status,
  });

  const created = await runtimeDeps.createDealRevenueItem(payload);
  await syncLinkedRecords({
    buyer_match_item_id: refs.buyer_match_item_id,
    deal_revenue_item_id: created?.item_id || null,
  });
  const pipeline = await runtimeDeps.syncPipelineState({
    contract_item_id: refs.contract_item_id,
    closing_item_id: resolved_closing_item_id,
    deal_revenue_item_id: created?.item_id || null,
    property_id: refs.property_id,
    master_owner_id: refs.master_owner_id,
    market_id: refs.market_item_id,
    notes: "Deal revenue created from closed closing.",
  });
  const brain_update = await runtimeDeps.updateBrainFromExecution({
    source: "revenue",
    closing_item: resolved_closing_item,
    notes: "Deal revenue created from a closed closing.",
  });

  return {
    ok: true,
    created: true,
    reason: "deal_revenue_created_from_closed_closing",
    closing_item_id: resolved_closing_item_id,
    deal_revenue_item_id: created?.item_id || null,
    deal_revenue_id: generated_deal_revenue_id,
    pipeline,
    brain_update,
    payload,
    raw: created,
  };
}

export default createDealRevenueFromClosedClosing;
