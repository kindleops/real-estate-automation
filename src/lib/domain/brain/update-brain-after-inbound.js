// ─── update-brain-after-inbound.js ───────────────────────────────────────
import {
  applyBrainStateUpdate,
  buildInboundBrainStateFields,
} from "@/lib/domain/brain/brain-authority.js";

export async function updateBrainAfterInbound({
  brain_id = null,
  message_body = "",
  follow_up_trigger_state = "AI Running",
  deterministic_state = null,
  extra_fields = {},
  now = new Date(),
} = {}) {
  return applyBrainStateUpdate({
    brain_id,
    reason: "inbound_message_received",
    fields: buildInboundBrainStateFields({
      message_body,
      follow_up_trigger_state,
      deterministic_state,
      extra_fields,
      now,
    }),
  });
}

export default updateBrainAfterInbound;
