// ─── update-brain-after-send.js ──────────────────────────────────────────
import {
  BRAIN_FIELDS,
  updateBrainItem,
} from "@/lib/podio/apps/ai-conversation-brain.js";
import {
  PHONE_FIELDS,
  updatePhoneNumberItem,
} from "@/lib/podio/apps/phone-numbers.js";
import { toPodioDateField } from "@/lib/utils/dates.js";

export async function updateBrainAfterSend({
  brain_id = null,
  phone_item_id = null,
  message_body = "",
  template_id = null,
  current_total_messages_sent = null,
} = {}) {
  if (!brain_id && !phone_item_id) {
    return {
      ok: false,
      reason: "missing_brain_and_phone_item_id",
    };
  }

  const next_count =
    typeof current_total_messages_sent === "number"
      ? current_total_messages_sent + 1
      : 1;

  const brain_fields = {
    [BRAIN_FIELDS.last_outbound_message]: String(message_body || ""),
    [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(new Date()),
    [BRAIN_FIELDS.last_sent_time]: toPodioDateField(new Date()),
    ...(template_id ? { [BRAIN_FIELDS.last_template_sent]: template_id } : {}),
  };

  const phone_fields = {
    [PHONE_FIELDS.total_messages_sent]: next_count,
  };

  await Promise.all([
    brain_id ? updateBrainItem(brain_id, brain_fields) : Promise.resolve(null),
    phone_item_id
      ? updatePhoneNumberItem(phone_item_id, phone_fields)
      : Promise.resolve(null),
  ]);

  return {
    ok: true,
    brain_id,
    phone_item_id,
    total_messages_sent: next_count,
    updated_fields: {
      ...(brain_id ? { brain: brain_fields } : {}),
      ...(phone_item_id ? { phone: phone_fields } : {}),
    },
  };
}

export default updateBrainAfterSend;
