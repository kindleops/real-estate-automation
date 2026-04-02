// ─── update-brain-seller-profile.js ──────────────────────────────────────
import { updateBrain } from "@/lib/providers/podio.js";

const BRAIN_FIELDS = {
  seller_profile: "seller-profile",
};

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

  await updateBrain(brain_id, {
    [BRAIN_FIELDS.seller_profile]: normalized_profile,
  });

  return {
    ok: true,
    brain_id,
    seller_profile: normalized_profile,
  };
}

export default updateBrainSellerProfile;