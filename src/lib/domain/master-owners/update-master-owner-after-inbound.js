import {
  MASTER_OWNER_FIELDS,
  updateMasterOwnerItem,
} from "@/lib/podio/apps/master-owners.js";
import { toPodioDateField } from "@/lib/utils/dates.js";

export async function updateMasterOwnerAfterInbound({
  master_owner_id = null,
  received_at = null,
} = {}) {
  if (!master_owner_id) {
    return {
      ok: false,
      reason: "missing_master_owner_id",
    };
  }

  const received_date = received_at ? new Date(received_at) : new Date();

  const fields = {
    [MASTER_OWNER_FIELDS.last_inbound]: toPodioDateField(received_date),
    [MASTER_OWNER_FIELDS.last_contacted_at]: toPodioDateField(received_date),
    [MASTER_OWNER_FIELDS.contact_status_2]: "Received",
    [MASTER_OWNER_FIELDS.next_follow_up_at]: null,
  };

  await updateMasterOwnerItem(master_owner_id, fields);

  return {
    ok: true,
    master_owner_id,
    updated_fields: fields,
  };
}

export default updateMasterOwnerAfterInbound;
