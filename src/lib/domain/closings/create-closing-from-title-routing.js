// ─── create-closing-from-title-routing.js ────────────────────────────────
import {
  CLOSING_FIELDS,
  createClosingItem,
} from "@/lib/podio/apps/closings.js";
import {
  CONTRACT_FIELDS,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  TITLE_ROUTING_FIELDS,
  getTitleRoutingItem,
  updateTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import { getDateValue, getFirstAppReferenceId } from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function asAppRef(value) {
  if (!value) return undefined;
  return [value];
}

function buildClosingId({
  title_routing_item_id = null,
  property_id = null,
} = {}) {
  const stamp = Date.now();

  if (title_routing_item_id) return `CLS-${title_routing_item_id}-${stamp}`;
  if (property_id) return `CLS-P-${property_id}-${stamp}`;

  return `CLS-${stamp}`;
}

function appendNotes(...values) {
  return values
    .map((value) => clean(value))
    .filter(Boolean)
    .join("\n");
}

function deriveRefs({
  title_routing_item = null,
} = {}) {
  return {
    contract_item_id:
      getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.contract, null) ||
      null,
    master_owner_id:
      getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.master_owner, null) ||
      null,
    prospect_id:
      getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.prospect, null) ||
      null,
    property_id:
      getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.property, null) ||
      null,
    title_company_item_id:
      getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.title_company, null) ||
      null,
    market_item_id:
      getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.market, null) ||
      null,
    closing_date_time:
      getDateValue(title_routing_item, TITLE_ROUTING_FIELDS.expected_closing_date, null) ||
      null,
  };
}

function normalizeClosingStatus(value = "Not Scheduled") {
  const raw = clean(value).toLowerCase();

  if (!raw) return "Not Scheduled";
  if (raw.includes("confirm")) return "Confirmed";
  if (raw.includes("resched")) return "Rescheduled";
  if (raw.includes("cancel")) return "Cancelled";
  if (raw.includes("complete") || raw.includes("closed")) return "Completed";
  if (raw.includes("schedule")) return "Scheduled";
  return "Not Scheduled";
}

const defaultDeps = {
  getTitleRoutingItem,
  getContractItem,
  createClosingItem,
  updateTitleRoutingItem,
  syncPipelineState,
};

let runtimeDeps = { ...defaultDeps };

export function __setCreateClosingFromTitleRoutingTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetCreateClosingFromTitleRoutingTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function createClosingFromTitleRouting({
  title_routing_item_id = null,
  title_routing_item = null,
  closing_id = null,
  closing_status = "Not Scheduled",
  source = "Title Routing Engine",
  notes = "",
} = {}) {
  let resolved_title_routing_item = title_routing_item || null;

  if (!resolved_title_routing_item && title_routing_item_id) {
    resolved_title_routing_item = await runtimeDeps.getTitleRoutingItem(
      title_routing_item_id
    );
  }

  const resolved_title_routing_item_id =
    resolved_title_routing_item?.item_id ||
    title_routing_item_id ||
    null;

  if (!resolved_title_routing_item_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_title_routing_item_id",
    };
  }

  const refs = deriveRefs({
    title_routing_item: resolved_title_routing_item,
  });
  const contract_item = refs.contract_item_id
    ? await runtimeDeps.getContractItem(refs.contract_item_id).catch(() => null)
    : null;
  const buyer_match_item_id = getFirstAppReferenceId(
    contract_item,
    CONTRACT_FIELDS.buyer_match,
    null
  );

  const generated_closing_id =
    clean(closing_id) ||
    buildClosingId({
      title_routing_item_id: resolved_title_routing_item_id,
      property_id: refs.property_id,
    });

  const normalized_requested_status = normalizeClosingStatus(closing_status);
  const normalized_closing_status =
    normalized_requested_status === "Not Scheduled" && refs.closing_date_time
      ? "Scheduled"
      : normalized_requested_status;
  const payload = {
    [CLOSING_FIELDS.closing_id]: generated_closing_id,
    [CLOSING_FIELDS.closing_title]: generated_closing_id,
    [CLOSING_FIELDS.closing_status]: normalized_closing_status,
    [CLOSING_FIELDS.title_routing]: asAppRef(resolved_title_routing_item_id),
    ...(refs.contract_item_id
      ? { [CLOSING_FIELDS.contract]: asAppRef(refs.contract_item_id) }
      : {}),
    ...(refs.master_owner_id
      ? { [CLOSING_FIELDS.master_owner]: asAppRef(refs.master_owner_id) }
      : {}),
    ...(refs.prospect_id
      ? { [CLOSING_FIELDS.prospect]: asAppRef(refs.prospect_id) }
      : {}),
    ...(refs.property_id
      ? { [CLOSING_FIELDS.property]: asAppRef(refs.property_id) }
      : {}),
    ...(refs.title_company_item_id
      ? { [CLOSING_FIELDS.title_company]: asAppRef(refs.title_company_item_id) }
      : {}),
    ...(refs.market_item_id
      ? { [CLOSING_FIELDS.market]: asAppRef(refs.market_item_id) }
      : {}),
    ...(buyer_match_item_id
      ? { [CLOSING_FIELDS.buyer_match]: asAppRef(buyer_match_item_id) }
      : {}),
    ...(refs.closing_date_time
      ? { [CLOSING_FIELDS.closing_date_time]: { start: refs.closing_date_time } }
      : {}),
    [CLOSING_FIELDS.pre_close_notes]: appendNotes(
      `[${nowIso()}] Closing created from title routing.`,
      source,
      notes
    ) || undefined,
  };

  const created = await runtimeDeps.createClosingItem(payload);
  await runtimeDeps.updateTitleRoutingItem(resolved_title_routing_item_id, {
    [TITLE_ROUTING_FIELDS.closing]: asAppRef(created?.item_id || null),
  });
  const pipeline = await runtimeDeps.syncPipelineState({
    contract_item_id: refs.contract_item_id,
    title_routing_item_id: resolved_title_routing_item_id,
    closing_item_id: created?.item_id || null,
    buyer_match_item_id,
    property_id: refs.property_id,
    master_owner_id: refs.master_owner_id,
    prospect_id: refs.prospect_id,
    market_id: refs.market_item_id,
    notes: "Closing created from title routing.",
  });

  return {
    ok: true,
    created: true,
    reason: "closing_created_from_title_routing",
    closing_item_id: created?.item_id || null,
    closing_id: generated_closing_id,
    title_routing_item_id: resolved_title_routing_item_id,
    closing_status: normalized_closing_status,
    pipeline,
    payload,
    raw: created,
  };
}

export default createClosingFromTitleRouting;
