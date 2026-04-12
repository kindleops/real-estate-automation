// ─── update-brain-language.js ────────────────────────────────────────────
import { normalizeLanguage } from "@/lib/providers/podio.js";
import { BRAIN_FIELDS } from "@/lib/podio/apps/ai-conversation-brain.js";
import { applyBrainStateUpdate } from "@/lib/domain/brain/brain-authority.js";

function clean(value) {
  return String(value ?? "").trim();
}

export async function updateBrainLanguage({
  brain_id = null,
  language = null,
} = {}) {
  if (!brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  const normalized_input = clean(language);

  if (!normalized_input) {
    return {
      ok: false,
      reason: "missing_language",
      brain_id,
    };
  }

  const normalized_language = normalizeLanguage(normalized_input);

  const result = await applyBrainStateUpdate({
    brain_id,
    reason: "brain_language_updated",
    fields: {
      [BRAIN_FIELDS.language_preference]: normalized_language,
    },
  });

  return {
    ...result,
    brain_id,
    language: normalized_language,
  };
}

export default updateBrainLanguage;
