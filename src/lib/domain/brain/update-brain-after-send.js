// ─── update-brain-after-send.js ──────────────────────────────────────────
import {
  PHONE_FIELDS,
  updatePhoneNumberItem,
} from "@/lib/podio/apps/phone-numbers.js";
import {
  applyBrainStateUpdate,
  buildBrainRelationshipFields,
  buildOutboundBrainStateFields,
} from "@/lib/domain/brain/brain-authority.js";

export async function updateBrainAfterSend({
  brain_id = null,
  phone_item_id = null,
  message_body = "",
  template_id = null,
  current_total_messages_sent = null,
  conversation_stage = null,
  current_follow_up_step = null,
  status_ai_managed = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  sms_agent_id = null,
  now = new Date().toISOString(),
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
  const brain_fields = buildOutboundBrainStateFields({
    message_body,
    template_id,
    conversation_stage,
    current_follow_up_step,
    status_ai_managed,
    now,
    extra_fields: buildBrainRelationshipFields({
      phone_item_id,
      master_owner_id,
      prospect_id,
      property_id,
      sms_agent_id,
      ai_agent_assigned_id: sms_agent_id,
    }),
  });

  const phone_fields = {
    [PHONE_FIELDS.total_messages_sent]: next_count,
  };

  const brain_result = await Promise.all([
    brain_id
      ? applyBrainStateUpdate({
          brain_id,
          reason: "outbound_message_sent",
          fields: brain_fields,
        })
      : Promise.resolve(null),
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
    brain_update: brain_result?.[0] || null,
  };
}

export default updateBrainAfterSend;
