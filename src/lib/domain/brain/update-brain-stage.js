// ─── update-brain-stage.js ───────────────────────────────────────────────
import { updateBrain, normalizeStage } from "@/lib/providers/podio.js";

const BRAIN_FIELDS = {
  conversation_stage: "conversation-stage",
};

function clean(value) {
  return String(value ?? "").trim();
}

export async function updateBrainStage({
  brain_id = null,
  stage = null,
} = {}) {
  if (!brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  const normalized_input = clean(stage);

  if (!normalized_input) {
    return {
      ok: false,
      reason: "missing_stage",
      brain_id,
    };
  }

  const normalized_stage = normalizeStage(normalized_input);

  await updateBrain(brain_id, {
    [BRAIN_FIELDS.conversation_stage]: normalized_stage,
  });

  return {
    ok: true,
    brain_id,
    stage: normalized_stage,
  };
}

export default updateBrainStage;