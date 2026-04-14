// ─── language_aliases.js ──────────────────────────────────────────────────
// Canonical language normalization across classifier output, AI Conversation
// Brain language-preference, agent language values, template CSV language
// values, and Podio language category values.

const CANONICAL_LANGUAGES = Object.freeze([
  "English",
  "Spanish",
  "Portuguese",
  "Italian",
  "French",
  "German",
  "Greek",
  "Hebrew",
  "Mandarin",
  "Japanese",
  "Korean",
  "Russian",
  "Arabic",
  "Polish",
  "Vietnamese",
  "Asian Indian (Hindi or Other)",
]);

const UNSUPPORTED_TEMPLATE_LANGUAGES = Object.freeze(new Set([
  "Thai",
  "Farsi",
  "Pashto",
]));

// Key = lowercased alias, Value = canonical language string
const ALIAS_MAP = new Map();

// Self-mapping for all canonical values
for (const lang of CANONICAL_LANGUAGES) {
  ALIAS_MAP.set(lang.toLowerCase(), lang);
}

// Hindi family → "Asian Indian (Hindi or Other)"
const HINDI_CANONICAL = "Asian Indian (Hindi or Other)";
ALIAS_MAP.set("hindi", HINDI_CANONICAL);
ALIAS_MAP.set("indian (hindi or other)", HINDI_CANONICAL);
ALIAS_MAP.set("asian indian (hindi or other)", HINDI_CANONICAL);
ALIAS_MAP.set("asian indian", HINDI_CANONICAL);
ALIAS_MAP.set("indian", HINDI_CANONICAL);

// Additional common aliases
ALIAS_MAP.set("chinese", "Mandarin");
ALIAS_MAP.set("mandarin chinese", "Mandarin");
ALIAS_MAP.set("zh", "Mandarin");
ALIAS_MAP.set("cn", "Mandarin");
ALIAS_MAP.set("es", "Spanish");
ALIAS_MAP.set("pt", "Portuguese");
ALIAS_MAP.set("it", "Italian");
ALIAS_MAP.set("fr", "French");
ALIAS_MAP.set("de", "German");
ALIAS_MAP.set("el", "Greek");
ALIAS_MAP.set("he", "Hebrew");
ALIAS_MAP.set("ja", "Japanese");
ALIAS_MAP.set("ko", "Korean");
ALIAS_MAP.set("ru", "Russian");
ALIAS_MAP.set("ar", "Arabic");
ALIAS_MAP.set("pl", "Polish");
ALIAS_MAP.set("vi", "Vietnamese");
ALIAS_MAP.set("en", "English");
ALIAS_MAP.set("hi", HINDI_CANONICAL);

/**
 * Normalize any language string to the canonical runtime language.
 * Returns null if the input is empty or unrecognized.
 */
export function normalizeLanguage(value) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return null;
  return ALIAS_MAP.get(trimmed.toLowerCase()) || null;
}

/**
 * Returns true if the language has templates in the CSV catalog.
 * Thai, Farsi, Pashto are recognized by classify.js but have no templates.
 */
export function isUnsupportedTemplateLanguage(value) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return false;
  return UNSUPPORTED_TEMPLATE_LANGUAGES.has(trimmed);
}

/**
 * Normalize and return the canonical language. If the raw value is an
 * unsupported template language, return { canonical, unsupported: true }.
 * If the raw value normalizes to a supported language, return { canonical, unsupported: false }.
 * If the raw value is unrecognized, return { canonical: null, unsupported: false }.
 */
export function resolveLanguage(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return { canonical: null, unsupported: false };

  const canonical = normalizeLanguage(raw);
  if (canonical) return { canonical, unsupported: false };

  if (isUnsupportedTemplateLanguage(raw)) {
    return { canonical: raw, unsupported: true };
  }

  return { canonical: null, unsupported: false };
}

export const CANONICAL_LANGUAGE_SET = Object.freeze(new Set(CANONICAL_LANGUAGES));

export default {
  normalizeLanguage,
  isUnsupportedTemplateLanguage,
  resolveLanguage,
  CANONICAL_LANGUAGES,
  CANONICAL_LANGUAGE_SET,
  UNSUPPORTED_TEMPLATE_LANGUAGES,
};
