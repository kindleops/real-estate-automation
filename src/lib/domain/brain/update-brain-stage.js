// ─── update-brain-stage.js ───────────────────────────────────────────────
import {
  applyBrainStateUpdate,
  buildStageBrainStateFields,
} from "@/lib/domain/brain/brain-authority.js";

export async function updateBrainStage({
  brain_id = null,
  stage = null,
} = {}) {
  if (!String(stage ?? "").trim()) {
    return {
      ok: false,
      reason: "missing_stage",
      brain_id,
    };
  }

  const result = await applyBrainStateUpdate({
    brain_id,
    reason: "conversation_stage_changed",
    fields: buildStageBrainStateFields({ stage }),
  });

  return {
    ...result,
    stage: result.updated_fields?.["conversation-stage"] || null,
  };
}

export default updateBrainStage;
