// ─── update-brain-after-delivery.js ──────────────────────────────────────
import {
  BRAIN_FIELDS,
  updateBrainItem,
} from "@/lib/podio/apps/ai-conversation-brain.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export async function updateBrainAfterDelivery({
  brain_id = null,
  delivery_status = null,
  failure_bucket = null,
} = {}) {
  if (!brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  const normalized_status = lower(delivery_status);
  const normalized_failure_bucket = clean(failure_bucket);

  const fields = {};

  if (normalized_status === "delivered") {
    fields[BRAIN_FIELDS.follow_up_trigger_state] = "Waiting";
  } else if (normalized_status === "failed") {
    fields[BRAIN_FIELDS.follow_up_trigger_state] = "Paused";
  }

  if (!Object.keys(fields).length) {
    return {
      ok: false,
      reason: "no_delivery_brain_updates",
      brain_id,
      delivery_status: normalized_status || null,
      failure_bucket: normalized_failure_bucket || null,
    };
  }

  await updateBrainItem(brain_id, fields);

  return {
    ok: true,
    brain_id,
    delivery_status: normalized_status,
    failure_bucket: normalized_failure_bucket || null,
    updated_fields: fields,
  };
}

export default updateBrainAfterDelivery;
