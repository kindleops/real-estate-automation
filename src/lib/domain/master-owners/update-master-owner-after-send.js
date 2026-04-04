import {
  getMasterOwnerItem,
  MASTER_OWNER_FIELDS,
  updateMasterOwnerItem,
} from "@/lib/podio/apps/master-owners.js";
import { toPodioDateField } from "@/lib/utils/dates.js";
import {
  addDaysIso,
  deriveFollowUpCadenceDays,
  isNoReplyFollowUpEligible,
} from "@/lib/domain/master-owners/follow-up-timing.js";

export async function updateMasterOwnerAfterSend({
  master_owner_id = null,
  sent_at = null,
  selected_use_case = null,
} = {}) {
  if (!master_owner_id) {
    return {
      ok: false,
      reason: "missing_master_owner_id",
    };
  }

  const master_owner_item = await getMasterOwnerItem(master_owner_id);
  const sent_date = sent_at ? new Date(sent_at) : new Date();
  const next_follow_up_at = isNoReplyFollowUpEligible(selected_use_case)
    ? addDaysIso(sent_date, deriveFollowUpCadenceDays(master_owner_item))
    : null;

  const fields = {
    [MASTER_OWNER_FIELDS.last_outbound]: toPodioDateField(sent_date),
    [MASTER_OWNER_FIELDS.last_contacted_at]: toPodioDateField(sent_date),
    [MASTER_OWNER_FIELDS.contact_status_2]:
      next_follow_up_at ? "Follow-Up Scheduled" : "Sent",
    [MASTER_OWNER_FIELDS.next_follow_up_at]:
      next_follow_up_at ? toPodioDateField(next_follow_up_at) : null,
  };

  await updateMasterOwnerItem(master_owner_id, fields);

  return {
    ok: true,
    master_owner_id,
    next_follow_up_at,
    updated_fields: fields,
  };
}

export default updateMasterOwnerAfterSend;
