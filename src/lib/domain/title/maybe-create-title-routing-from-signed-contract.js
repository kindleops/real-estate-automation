// ─── maybe-create-title-routing-from-signed-contract.js ──────────────────
import {
  CONTRACT_FIELDS,
} from "@/lib/podio/apps/contracts.js";
import {
  TITLE_ROUTING_FIELDS,
  findTitleRoutingItems,
} from "@/lib/podio/apps/title-routing.js";
import { createTitleRoutingFromContract } from "@/lib/domain/title/create-title-routing-from-contract.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (first?.value?.item_id) return first.value.item_id;
  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.start) return first.start;

  return null;
}

function isSignedContract({
  contract_item = null,
  contract_status = null,
  docusign_status = null,
  webhook_result = null,
} = {}) {
  const normalized_contract_status =
    clean(contract_status) ||
    clean(getFieldValue(contract_item, CONTRACT_FIELDS.contract_status));

  const normalized_docusign_status =
    clean(docusign_status) ||
    clean(webhook_result?.normalized_status);

  if (normalized_contract_status.toLowerCase() === "fully executed") {
    return true;
  }

  if (normalized_docusign_status.toLowerCase() === "completed") {
    return true;
  }

  return false;
}

async function findLatestTitleRoutingByContractId(contract_item_id) {
  if (!contract_item_id) return null;

  const matches = await findTitleRoutingItems(
    { [TITLE_ROUTING_FIELDS.contract]: contract_item_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

function isTerminalTitleRoutingStatus(status = "") {
  const normalized = clean(status).toLowerCase();
  return ["closed", "cancelled"].includes(normalized);
}

export async function maybeCreateTitleRoutingFromSignedContract({
  contract_item = null,
  contract_item_id = null,
  contract_status = null,
  docusign_status = null,
  webhook_result = null,
  title_company_item_id = null,
  title_routing_id = null,
  routing_status = "Routed",
  source = "DocuSign Webhook",
  notes = "",
} = {}) {
  const resolved_contract_item_id =
    contract_item?.item_id ||
    contract_item_id ||
    null;

  if (!resolved_contract_item_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_contract_item_id",
    };
  }

  const signed = isSignedContract({
    contract_item,
    contract_status,
    docusign_status,
    webhook_result,
  });

  if (!signed) {
    return {
      ok: true,
      created: false,
      reason: "contract_not_signed",
      contract_item_id: resolved_contract_item_id,
    };
  }

  const existing_title_routing = await findLatestTitleRoutingByContractId(
    resolved_contract_item_id
  );

  if (existing_title_routing?.item_id) {
    const existing_status = clean(
      getFieldValue(existing_title_routing, TITLE_ROUTING_FIELDS.routing_status)
    );

    if (!isTerminalTitleRoutingStatus(existing_status)) {
      const pipeline = await syncPipelineState({
        contract_item_id: resolved_contract_item_id,
        title_routing_item_id: existing_title_routing.item_id,
        notes: "Existing title routing found for signed contract.",
      });

      return {
        ok: true,
        created: false,
        reason: "existing_title_routing_found",
        contract_item_id: resolved_contract_item_id,
        title_routing_item_id: existing_title_routing.item_id,
        existing_title_routing,
        pipeline,
      };
    }
  }

  const created = await createTitleRoutingFromContract({
    contract_item_id: resolved_contract_item_id,
    contract_item,
    title_company_item_id,
    title_routing_id,
    routing_status,
    source,
    notes,
  });

  return {
    ok: Boolean(created?.ok),
    created: Boolean(created?.created),
    reason: created?.reason || "title_routing_create_attempted",
    contract_item_id: resolved_contract_item_id,
    title_routing_item_id: created?.title_routing_item_id || null,
    result: created,
  };
}

export default maybeCreateTitleRoutingFromSignedContract;
