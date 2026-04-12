// ─── create-title-routing-from-contract.js ───────────────────────────────
import {
  CONTRACT_FIELDS,
  getContractItem,
  updateContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  TITLE_ROUTING_FIELDS,
  createTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import {
  TITLE_COMPANY_FIELDS,
  getTitleCompanyItem,
} from "@/lib/podio/apps/title-companies.js";
import {
  getDateValue,
  getFirstAppReferenceId,
  getPhoneValue,
  getTextValue,
} from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { syncContractStatus } from "@/lib/domain/contracts/sync-contract-status.js";

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

function buildTitleRoutingId({
  contract_item_id = null,
  property_id = null,
} = {}) {
  const stamp = Date.now();

  if (contract_item_id) return `TR-${contract_item_id}-${stamp}`;
  if (property_id) return `TR-P-${property_id}-${stamp}`;

  return `TR-${stamp}`;
}

function appendNotes(...values) {
  return values
    .map((value) => clean(value))
    .filter(Boolean)
    .join("\n");
}

function deriveRefs({
  contract_item = null,
  title_company_item_id = null,
} = {}) {
  return {
    master_owner_id:
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.master_owner, null) || null,
    prospect_id:
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.prospect, null) || null,
    property_id:
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.property, null) || null,
    title_company_item_id:
      title_company_item_id ||
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.title_company_legacy, null) ||
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.title_company, null) ||
      null,
    market_item_id:
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.market, null) || null,
    assigned_agent_id:
      getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.assigned_agent, null) || null,
    expected_closing_date:
      getDateValue(contract_item, CONTRACT_FIELDS.closing_date_target, null) || null,
  };
}

function normalizeRoutingStatus(value = "Not Routed") {
  const raw = clean(value).toLowerCase();

  if (!raw) return "Not Routed";
  if (raw.includes("opened")) return "Opened";
  if (raw.includes("routed")) return "Routed";
  if (raw.includes("review")) return "Title Reviewing";
  if (raw.includes("payoff")) return "Waiting on Payoff";
  if (raw.includes("probate")) return "Waiting on Probate";
  if (raw.includes("seller")) return "Waiting on Seller";
  if (raw.includes("buyer")) return "Waiting on Buyer";
  if (raw.includes("doc")) return "Waiting on Docs";
  if (raw.includes("clear")) return "Clear to Close";
  if (raw.includes("closed")) return "Closed";
  if (raw.includes("cancel")) return "Cancelled";
  return "Not Routed";
}

function mapContractStatusFromRoutingStatus(status = "") {
  switch (clean(status)) {
    case "Routed":
      return "Sent To Title";
    case "Opened":
      return "Opened";
    case "Clear to Close":
      return "Clear To Close";
    case "Closed":
      return "Closed";
    case "Cancelled":
      return "Cancelled";
    default:
      return null;
  }
}

const defaultDeps = {
  getContractItem,
  createTitleRoutingItem,
  getTitleCompanyItem,
  updateContractItem,
  syncPipelineState,
  syncContractStatus,
};

let runtimeDeps = { ...defaultDeps };

export function __setCreateTitleRoutingFromContractTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetCreateTitleRoutingFromContractTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function createTitleRoutingFromContract({
  contract_item_id = null,
  contract_item = null,
  title_company_item_id = null,
  title_routing_id = null,
  routing_status = "Not Routed",
  source = "Contract Engine",
  notes = "",
} = {}) {
  let resolved_contract_item = contract_item || null;

  if (!resolved_contract_item && contract_item_id) {
    resolved_contract_item = await runtimeDeps.getContractItem(contract_item_id);
  }

  const resolved_contract_item_id =
    resolved_contract_item?.item_id ||
    contract_item_id ||
    null;

  if (!resolved_contract_item_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_contract_item_id",
    };
  }

  const refs = deriveRefs({
    contract_item: resolved_contract_item,
    title_company_item_id,
  });

  const generated_title_routing_id =
    clean(title_routing_id) ||
    buildTitleRoutingId({
      contract_item_id: resolved_contract_item_id,
      property_id: refs.property_id,
    });

  const normalized_routing_status = normalizeRoutingStatus(routing_status);
  const title_company_item = refs.title_company_item_id
    ? await runtimeDeps.getTitleCompanyItem(refs.title_company_item_id).catch(() => null)
    : null;
  const payload = {
    [TITLE_ROUTING_FIELDS.title]: generated_title_routing_id,
    [TITLE_ROUTING_FIELDS.title_routing_id]: generated_title_routing_id,
    [TITLE_ROUTING_FIELDS.routing_status]: normalized_routing_status,
    [TITLE_ROUTING_FIELDS.contract]: asAppRef(resolved_contract_item_id),
    ...(refs.master_owner_id
      ? { [TITLE_ROUTING_FIELDS.master_owner]: asAppRef(refs.master_owner_id) }
      : {}),
    ...(refs.prospect_id
      ? { [TITLE_ROUTING_FIELDS.prospect]: asAppRef(refs.prospect_id) }
      : {}),
    ...(refs.property_id
      ? { [TITLE_ROUTING_FIELDS.property]: asAppRef(refs.property_id) }
      : {}),
    ...(refs.title_company_item_id
      ? { [TITLE_ROUTING_FIELDS.title_company]: asAppRef(refs.title_company_item_id) }
      : {}),
    ...(refs.market_item_id
      ? { [TITLE_ROUTING_FIELDS.market]: asAppRef(refs.market_item_id) }
      : {}),
    ...(refs.assigned_agent_id
      ? { [TITLE_ROUTING_FIELDS.assigned_agent]: asAppRef(refs.assigned_agent_id) }
      : {}),
    ...(refs.expected_closing_date
      ? { [TITLE_ROUTING_FIELDS.expected_closing_date]: { start: refs.expected_closing_date } }
      : {}),
    ...(normalized_routing_status === "Routed"
      ? { [TITLE_ROUTING_FIELDS.file_routed_date]: { start: nowIso() } }
      : {}),
    ...(clean(getTextValue(title_company_item, TITLE_COMPANY_FIELDS.contact_manager, ""))
      ? {
          [TITLE_ROUTING_FIELDS.primary_title_contact]: getTextValue(
            title_company_item,
            TITLE_COMPANY_FIELDS.contact_manager,
            ""
          ),
        }
      : {}),
    ...(clean(getTextValue(title_company_item, TITLE_COMPANY_FIELDS.new_order_email, ""))
      ? {
          [TITLE_ROUTING_FIELDS.title_contact_email]: getTextValue(
            title_company_item,
            TITLE_COMPANY_FIELDS.new_order_email,
            ""
          ),
        }
      : {}),
    ...(clean(getPhoneValue(title_company_item, TITLE_COMPANY_FIELDS.phone, ""))
      ? {
          [TITLE_ROUTING_FIELDS.title_contact_phone]: getPhoneValue(
            title_company_item,
            TITLE_COMPANY_FIELDS.phone,
            ""
          ),
        }
      : {}),
    [TITLE_ROUTING_FIELDS.internal_notes]: appendNotes(
      `[${nowIso()}] Title routing created from contract.`,
      source,
      notes
    ) || undefined,
  };

  const created = await runtimeDeps.createTitleRoutingItem(payload);
  await runtimeDeps.updateContractItem(resolved_contract_item_id, {
    [CONTRACT_FIELDS.title_routing]: asAppRef(created?.item_id || null),
  });

  const contract_sync = await runtimeDeps.syncContractStatus({
    contract_item_id: resolved_contract_item_id,
    status: mapContractStatusFromRoutingStatus(normalized_routing_status),
  });
  const pipeline = await runtimeDeps.syncPipelineState({
    contract_item_id: resolved_contract_item_id,
    title_routing_item_id: created?.item_id || null,
    property_id: refs.property_id,
    master_owner_id: refs.master_owner_id,
    prospect_id: refs.prospect_id,
    assigned_agent_id: refs.assigned_agent_id,
    market_id: refs.market_item_id,
    current_engine: "Title Routing",
    blocked: refs.title_company_item_id ? "No" : "Yes",
    blocker_type: refs.title_company_item_id ? null : "Other",
    blocker_summary: refs.title_company_item_id
      ? null
      : "Title routing is missing a linked title company.",
    next_system_action: refs.title_company_item_id ? null : "assign_title_company",
    notes: "Title routing created from contract.",
  });

  return {
    ok: true,
    created: true,
    reason: "title_routing_created_from_contract",
    title_routing_item_id: created?.item_id || null,
    title_routing_id: generated_title_routing_id,
    contract_item_id: resolved_contract_item_id,
    contract_sync,
    pipeline,
    payload,
    raw: created,
  };
}

export default createTitleRoutingFromContract;
