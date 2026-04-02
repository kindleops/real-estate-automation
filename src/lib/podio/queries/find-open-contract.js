import {
  CONTRACT_FIELDS,
  findContractByContractId,
  findContractItems,
} from "@/lib/podio/apps/contracts.js";
import { getCategoryValue } from "@/lib/providers/podio.js";

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function isOpenContractStatus(status = "") {
  const normalized = String(status || "").trim().toLowerCase();
  return !["fully executed", "closed", "cancelled"].includes(normalized);
}

export async function findOpenContract({
  contract_id = null,
  offer_item_id = null,
} = {}) {
  if (contract_id) {
    const direct = await findContractByContractId(contract_id);
    if (direct && isOpenContractStatus(getCategoryValue(direct, CONTRACT_FIELDS.contract_status, ""))) {
      return direct;
    }
  }

  if (!offer_item_id) return null;

  const matches = await findContractItems(
    { [CONTRACT_FIELDS.offer]: offer_item_id },
    50,
    0
  );

  return sortNewestFirst(matches).find((item) =>
    isOpenContractStatus(getCategoryValue(item, CONTRACT_FIELDS.contract_status, ""))
  ) || null;
}

export default findOpenContract;
