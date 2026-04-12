// ─── update-brain-after-delivery.js ──────────────────────────────────────
import {
  applyBrainStateUpdate,
  buildDeliveryBrainStateFields,
} from "@/lib/domain/brain/brain-authority.js";

export async function updateBrainAfterDelivery({
  brain_id = null,
  delivery_status = null,
  failure_bucket = null,
} = {}) {
  const fields = buildDeliveryBrainStateFields({
    delivery_status,
  });

  const result = await applyBrainStateUpdate({
    brain_id,
    reason: "delivery_status_received",
    fields,
  });

  if (!result.ok && result.reason === "no_brain_fields_to_update") {
    return {
      ok: false,
      reason: "no_delivery_brain_updates",
      brain_id,
      delivery_status: String(delivery_status ?? "").trim().toLowerCase() || null,
      failure_bucket: String(failure_bucket ?? "").trim() || null,
    };
  }

  return {
    ...result,
    delivery_status: String(delivery_status ?? "").trim().toLowerCase() || null,
    failure_bucket: String(failure_bucket ?? "").trim() || null,
  };
}

export default updateBrainAfterDelivery;
