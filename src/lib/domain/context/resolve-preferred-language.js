import {
  getCategoryValue,
  normalizeLanguage,
} from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

export function resolvePreferredContactLanguage({
  master_owner_item = null,
  owner_item = null,
  prospect_item = null,
  brain_item = null,
  fallback = "English",
} = {}) {
  const raw_language =
    getCategoryValue(master_owner_item, "language-primary", null) ||
    getCategoryValue(prospect_item, "language", null) ||
    getCategoryValue(owner_item, "language", null) ||
    getCategoryValue(brain_item, "language-preference", null) ||
    fallback;

  const normalized_language = normalizeLanguage(raw_language);
  return clean(normalized_language) || clean(fallback) || "English";
}

export default resolvePreferredContactLanguage;
