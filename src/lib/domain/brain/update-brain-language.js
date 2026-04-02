// ─── update-brain-language.js ────────────────────────────────────────────
import { updateBrain, normalizeLanguage } from "@/lib/providers/podio.js";

const BRAIN_FIELDS = {
  language_preference: "language-preference",
};

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

  await updateBrain(brain_id, {
    [BRAIN_FIELDS.language_preference]: normalized_language,
  });

  return {
    ok: true,
    brain_id,
    language: normalized_language,
  };
}

export default updateBrainLanguage;