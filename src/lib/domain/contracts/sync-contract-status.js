import {
  CONTRACT_FIELDS,
  findContractByContractId,
  getContractItem,
  updateContractItem,
} from "@/lib/podio/apps/contracts.js";
import { getCategoryValue, getTextValue } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

export const CONTRACT_STATUS_ORDER = Object.freeze({
  Draft: 0,
  Sent: 1,
  Viewed: 2,
  "Seller Signed": 3,
  "Buyer Signed": 3,
  "Fully Executed": 4,
  "Sent To Title": 5,
  Opened: 6,
  "Clear To Close": 7,
  Closed: 8,
  Cancelled: 8,
});

export function shouldAdvanceContractStatus(current_status = null, next_status = null) {
  const current = clean(current_status) || null;
  const next = clean(next_status) || null;

  if (!next) return false;
  if (!current) return true;
  if (current === next) return false;
  if (["Closed", "Cancelled"].includes(current)) return false;
  if (
    next === "Cancelled" &&
    ["Fully Executed", "Sent To Title", "Opened", "Clear To Close", "Closed"].includes(
      current
    )
  ) {
    return false;
  }
  if (
    ["Sent To Title", "Opened", "Clear To Close", "Closed"].includes(current) &&
    ["Draft", "Sent", "Viewed", "Seller Signed", "Buyer Signed", "Fully Executed"].includes(
      next
    )
  ) {
    return false;
  }

  const current_rank = CONTRACT_STATUS_ORDER[current] ?? -1;
  const next_rank = CONTRACT_STATUS_ORDER[next] ?? -1;

  if (current_rank < next_rank) return true;
  if (
    current_rank === next_rank &&
    ["Seller Signed", "Buyer Signed"].includes(current) &&
    ["Seller Signed", "Buyer Signed"].includes(next)
  ) {
    return true;
  }

  return false;
}

export async function syncContractStatus({
  contract_item_id = null,
  contract_id = null,
  contract_item = null,
  status = null,
  extra_fields = {},
} = {}) {
  if (!contract_item_id && !contract_id && !contract_item?.item_id) {
    return {
      ok: false,
      updated: false,
      reason: "missing_contract_identifier",
    };
  }

  const resolved_contract_item =
    contract_item ||
    (contract_item_id ? await getContractItem(contract_item_id) : null) ||
    (contract_id ? await getContractItem(contract_id) : null) ||
    (contract_id ? await findContractByContractId(String(contract_id)) : null) ||
    null;

  if (!resolved_contract_item?.item_id) {
    return {
      ok: false,
      updated: false,
      reason: "contract_not_found",
      contract_id,
    };
  }

  if (!status) {
    return {
      ok: true,
      updated: false,
      reason: "contract_snapshot_only",
      contract_item_id: resolved_contract_item.item_id,
      contract_status: getCategoryValue(
        resolved_contract_item,
        CONTRACT_FIELDS.contract_status,
        null
      ),
      envelope_id: getTextValue(
        resolved_contract_item,
        CONTRACT_FIELDS.docusign_envelope_id,
        ""
      ),
      contract_item: resolved_contract_item,
    };
  }

  const current_status = getCategoryValue(
    resolved_contract_item,
    CONTRACT_FIELDS.contract_status,
    null
  );
  const next_status = clean(status);
  const advance = shouldAdvanceContractStatus(current_status, next_status);
  const payload = { ...(extra_fields || {}) };

  if (advance) {
    payload[CONTRACT_FIELDS.contract_status] = next_status;
  } else if (next_status === clean(current_status)) {
    payload[CONTRACT_FIELDS.contract_status] = next_status;
  }

  if (!Object.keys(payload).length) {
    return {
      ok: true,
      updated: false,
      reason: "contract_status_not_advanced",
      contract_item_id: resolved_contract_item.item_id,
      contract_status: current_status,
      requested_status: next_status,
    };
  }

  await updateContractItem(resolved_contract_item.item_id, payload);

  return {
    ok: true,
    updated: advance || Object.keys(extra_fields || {}).length > 0,
    reason: advance ? "contract_status_updated" : "contract_fields_synced",
    contract_item_id: resolved_contract_item.item_id,
    contract_status: payload[CONTRACT_FIELDS.contract_status] || current_status,
    requested_status: next_status,
    payload,
  };
}

export default syncContractStatus;
