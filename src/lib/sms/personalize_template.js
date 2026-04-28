// ─── personalize_template.js ──────────────────────────────────────────────
// Safely replace placeholders in template text.
// Fail cleanly if required placeholders are missing —
// never ship unreplaced {{...}} in an outbound message.

import { sanitizeSmsTextMap, sanitizeSmsTextValue } from "@/lib/sms/sanitize.js";

// ══════════════════════════════════════════════════════════════════════════
// PLACEHOLDER DEFINITIONS
// ══════════════════════════════════════════════════════════════════════════

const PLACEHOLDER_PATTERN = /\{\{([^}]+)\}\}/g;

const KNOWN_PLACEHOLDERS = Object.freeze([
  "seller_first_name",
  "agent_name",
  "agent_first_name",
  "sms_agent_name",
  "sender_name",
  "rep_name",
  "property_address",
  "property_city",
  "city",
  "offer_price",
  "repair_cost",
  "closing_date",
  "unit_count",
]);

const KNOWN_SET = new Set(KNOWN_PLACEHOLDERS);

// ══════════════════════════════════════════════════════════════════════════
// VALUE FORMATTING
// ══════════════════════════════════════════════════════════════════════════

function formatCurrency(value) {
  if (value == null) return null;
  const safe_value = typeof value === "number" ? value : sanitizeSmsTextValue(value);
  const num =
    typeof safe_value === "number"
      ? safe_value
      : Number(String(safe_value).replace(/[^0-9.-]/g, ""));
  if (!Number.isFinite(num)) return null;
  // No decimals for round numbers, 2 decimals otherwise
  const formatted = num % 1 === 0
    ? `$${num.toLocaleString("en-US")}`
    : `$${num.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  return formatted;
}

function formatDate(value) {
  if (!value) return null;
  const safe_value = value instanceof Date ? value : sanitizeSmsTextValue(value);
  const d = safe_value instanceof Date ? safe_value : new Date(safe_value);
  if (Number.isNaN(d.getTime())) return null;
  // MM/DD/YYYY local format
  return `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
}

function formatInteger(value) {
  if (value == null) return null;
  const safe_value = typeof value === "number" ? value : sanitizeSmsTextValue(value);
  const num = typeof safe_value === "number" ? safe_value : Number(safe_value);
  if (!Number.isFinite(num)) return null;
  return String(Math.round(num));
}

function cleanText(value) {
  if (value == null || value === "") return null;
  return sanitizeSmsTextValue(value) || null;
}

function firstNameOnly(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  return raw
    .replace(/\s+/g, " ")
    .split(" ")[0]
    .replace(/[^\p{L}\p{M}'-]/gu, "")
    .trim();
}

// ══════════════════════════════════════════════════════════════════════════
// SMART PUNCTUATION NORMALIZATION
// ══════════════════════════════════════════════════════════════════════════

function normalizePunctuation(text) {
  return text
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'")  // smart single quotes
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"')   // smart double quotes
    .replace(/[\u2013\u2014]/g, "-")                // em/en dashes
    .replace(/\u2026/g, "...")                      // ellipsis
    .replace(/\u00A0/g, " ");                       // non-breaking space
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN PERSONALIZATION
// ══════════════════════════════════════════════════════════════════════════

/**
 * Build the personalization values map from context.
 *
 * @param {object} context
 * @param {string} [context.seller_first_name]
 * @param {string} [context.agent_name]
 * @param {string} [context.agent_first_name]
 * @param {string} [context.sms_agent_name]
 * @param {string} [context.sender_name]
 * @param {string} [context.rep_name]
 * @param {string} [context.property_address]
 * @param {string} [context.property_city]
 * @param {string} [context.city] - Alias for property_city
 * @param {number|string} [context.offer_price]
 * @param {number|string} [context.repair_cost]
 * @param {string|Date} [context.closing_date]
 * @param {number} [context.unit_count]
 * @returns {Map<string, string|null>}
 */
function buildValueMap(context = {}) {
  const safe_context = sanitizeSmsTextMap(context);
  const agent_name_raw = cleanText(
    safe_context.agent_first_name ||
      safe_context.sms_agent_name ||
      safe_context.sender_name ||
      safe_context.rep_name ||
      safe_context.agent_name ||
      safe_context.agent_name_raw ||
      safe_context.agent_full_name_raw ||
      safe_context.selected_agent_display_name
  );
  const agent_first_name = cleanText(firstNameOnly(agent_name_raw));
  const map = new Map();
  map.set("seller_first_name", cleanText(safe_context.seller_first_name));
  map.set("agent_name", agent_first_name);
  map.set("agent_first_name", agent_first_name);
  map.set("sms_agent_name", agent_first_name);
  map.set("sender_name", agent_first_name);
  map.set("rep_name", agent_first_name);
  map.set("property_address", cleanText(safe_context.property_address));
  map.set(
    "property_city",
    cleanText(safe_context.property_city) || cleanText(safe_context.city)
  );
  map.set(
    "city",
    cleanText(safe_context.city) || cleanText(safe_context.property_city)
  );
  map.set("offer_price", formatCurrency(safe_context.offer_price));
  map.set("repair_cost", formatCurrency(safe_context.repair_cost));
  map.set("closing_date", formatDate(context.closing_date));
  map.set("unit_count", formatInteger(safe_context.unit_count));
  return map;
}

/**
 * Detect all placeholders in a template text string.
 *
 * @param {string} template_text
 * @returns {string[]} Array of placeholder names found
 */
export function detectPlaceholders(template_text) {
  const found = [];
  const text = String(template_text ?? "");
  let match;
  const re = new RegExp(PLACEHOLDER_PATTERN.source, PLACEHOLDER_PATTERN.flags);
  while ((match = re.exec(text)) !== null) {
    const name = match[1].trim();
    if (!found.includes(name)) found.push(name);
  }
  return found;
}

/**
 * Personalize a template: replace all {{...}} placeholders.
 *
 * Returns:
 * - { ok: true, text, placeholders_used } on success
 * - { ok: false, text: null, missing, reason } if required placeholders can't be filled
 *
 * @param {string} template_text
 * @param {object} context - Values for placeholder resolution
 * @returns {{ ok: boolean, text: string|null, placeholders_used?: string[], missing?: string[], reason?: string }}
 */
export function personalizeTemplate(template_text, context = {}) {
  const text = String(template_text ?? "");
  if (!text) {
    return { ok: false, text: null, missing: [], reason: "empty_template" };
  }

  const placeholders = detectPlaceholders(text);
  if (placeholders.length === 0) {
    // No placeholders — return as-is after cleanup
    const cleaned = normalizePunctuation(text).trim();
    return { ok: true, text: cleaned, placeholders_used: [] };
  }

  const value_map = buildValueMap(context);
  const missing = [];
  const used = [];

  let result = text;
  for (const ph of placeholders) {
    const value = value_map.get(ph);
    if (value == null) {
      missing.push(ph);
    } else {
      const re = new RegExp(`\\{\\{\\s*${ph.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*\\}\\}`, "g");
      result = result.replace(re, value);
      used.push(ph);
    }
  }

  if (missing.length > 0) {
    return {
      ok: false,
      text: null,
      missing,
      placeholders_used: used,
      reason: `missing_placeholder_values: ${missing.join(", ")}`,
    };
  }

  // Final safety check: no unresolved placeholders should remain
  if (PLACEHOLDER_PATTERN.test(result)) {
    const remaining = detectPlaceholders(result);
    return {
      ok: false,
      text: null,
      missing: remaining,
      placeholders_used: used,
      reason: `unresolved_placeholders: ${remaining.join(", ")}`,
    };
  }

  const cleaned = normalizePunctuation(result).trim();

  return {
    ok: true,
    text: cleaned,
    placeholders_used: used,
  };
}

/**
 * Count SMS segments for a message.
 * GSM-7: 160 chars / segment (70 if UCS-2 needed).
 * Concatenated: 153 / 67 chars per segment.
 */
export function countSegments(text) {
  const str = String(text ?? "");
  if (!str) return 0;
  // Simple UCS-2 detection: any char outside GSM-7 basic set
  const gsm7 = /^[@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !"#¤%&'()*+,\-.\/0-9:;<=>?¡A-ZÄÖÑÜa-zäöñüà§\u000C^{}\\[~\]|€\r\n]*$/;
  const is_gsm = gsm7.test(str);
  const single_limit = is_gsm ? 160 : 70;
  const multi_limit = is_gsm ? 153 : 67;

  if (str.length <= single_limit) return 1;
  return Math.ceil(str.length / multi_limit);
}

export {
  buildValueMap,
  formatCurrency,
  formatDate,
  formatInteger,
  normalizePunctuation,
  KNOWN_PLACEHOLDERS,
};

export default { personalizeTemplate, detectPlaceholders, countSegments };
