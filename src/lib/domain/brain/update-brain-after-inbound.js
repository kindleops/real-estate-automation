// ─── update-brain-after-inbound.js ───────────────────────────────────────
import {
  BRAIN_FIELDS,
  updateBrainItem,
} from "@/lib/podio/apps/ai-conversation-brain.js";
import { toPodioDateField } from "@/lib/utils/dates.js";

export async function updateBrainAfterInbound({
  brain_id = null,
  message_body = "",
  follow_up_trigger_state = "AI Running",
} = {}) {
  if (!brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  const fields = {
    [BRAIN_FIELDS.last_inbound_message]: String(message_body || ""),
    [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(new Date()),
    [BRAIN_FIELDS.follow_up_trigger_state]: follow_up_trigger_state,
  };

  await updateBrainItem(brain_id, fields);

  return {
    ok: true,
    brain_id,
    updated_fields: fields,
  };
}

export default updateBrainAfterInbound;
