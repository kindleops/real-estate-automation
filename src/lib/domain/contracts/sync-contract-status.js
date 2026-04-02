import {
  CONTRACT_FIELDS,
  findContractByContractId,
  getContractItem,
  updateContractItem,
} from "@/lib/podio/apps/contracts.js";
import { getCategoryValue, getTextValue } from "@/lib/providers/podio.js";

export async function syncContractStatus({
  contract_id = null,
  status = null,
} = {}) {
  if (!contract_id) {
    return {
      ok: false,
      updated: false,
      reason: "missing_contract_id",
    };
  }

  const contract_item =
    (await getContractItem(contract_id)) ||
    (await findContractByContractId(String(contract_id))) ||
    null;

  if (!contract_item?.item_id) {
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
      contract_item_id: contract_item.item_id,
      contract_status: getCategoryValue(contract_item, CONTRACT_FIELDS.contract_status, null),
      envelope_id: getTextValue(contract_item, CONTRACT_FIELDS.docusign_envelope_id, ""),
      contract_item,
    };
  }

  await updateContractItem(contract_item.item_id, {
    [CONTRACT_FIELDS.contract_status]: status,
  });

  return {
    ok: true,
    updated: true,
    reason: "contract_status_updated",
    contract_item_id: contract_item.item_id,
    contract_status: status,
  };
}

export default syncContractStatus;
