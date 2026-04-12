// ─── update-brain-seller-profile.js ──────────────────────────────────────
import { BRAIN_FIELDS } from "@/lib/podio/apps/ai-conversation-brain.js";
import { applyBrainStateUpdate } from "@/lib/domain/brain/brain-authority.js";

function clean(value) {
  return String(value ?? "").trim();
}

export async function updateBrainSellerProfile({
  brain_id = null,
  seller_profile = null,
} = {}) {
  if (!brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  const normalized_profile = clean(seller_profile);

  if (!normalized_profile) {
    return {
      ok: false,
      reason: "missing_seller_profile",
      brain_id,
    };
  }

  const result = await applyBrainStateUpdate({
    brain_id,
    reason: "brain_seller_profile_updated",
    fields: {
      [BRAIN_FIELDS.seller_profile]: normalized_profile,
    },
  });

  return {
    ...result,
    brain_id,
    seller_profile: normalized_profile,
  };
}

export default updateBrainSellerProfile;
