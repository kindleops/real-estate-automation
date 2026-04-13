import APP_IDS from "@/lib/config/app-ids.js";
import { normalizeSellerFlowUseCase } from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import {
  getItem,
  updateItem,
  filterAppItems,
  getCategoryValue,
  getCategoryValues,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.templates;
const MAX_FETCH_LIMIT = 200;

function firstPresentCategory(item, external_ids = [], fallback = null) {
  for (const external_id of external_ids) {
    const value = getCategoryValue(item, external_id, null);
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      return value;
    }
  }

  return fallback;
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function clean(value) {
  return String(value ?? "").trim();
}

function normalizeFieldLabel(value) {
  return clean(value)
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function firstPresentCategoryByLabel(item, labels = [], fallback = null) {
  const wanted = new Set(labels.map((label) => normalizeFieldLabel(label)).filter(Boolean));
  if (!wanted.size) return fallback;

  for (const field of safeArray(item?.fields)) {
    const candidates = [
      field?.label,
      field?.config?.label,
      field?.field?.label,
    ];
    const matches = candidates.some((value) => wanted.has(normalizeFieldLabel(value)));
    if (!matches) continue;

    const value = safeArray(field?.values)
      .map((entry) => entry?.value?.text ?? (typeof entry?.value === "string" ? entry.value : null))
      .find((entry) => clean(entry));

    if (clean(value)) return value;
  }

  return fallback;
}

function readTemplateCategory(item, external_ids = [], labels = [], fallback = null) {
  return firstPresentCategory(
    item,
    external_ids,
    firstPresentCategoryByLabel(item, labels, fallback)
  );
}

function deriveTemplateUseCase(item, variant_group = null) {
  const use_case_label = getCategoryValue(item, "use-case-2", null);
  const canonical_routing_slug = getCategoryValue(item, "use-case", null);
  const canonical_slug_root =
    clean(canonical_routing_slug).split("__").filter(Boolean)[0] || null;

  return (
    normalizeSellerFlowUseCase(
      use_case_label || canonical_slug_root || canonical_routing_slug,
      variant_group
    ) ||
    use_case_label ||
    canonical_slug_root ||
    canonical_routing_slug ||
    null
  );
}

export function normalizeTemplateItem(item) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const variant_group = readTemplateCategory(
    item,
    ["stage", "stage-label"],
    ["Variant Group", "Stage Label"],
    null
  );
  const use_case_label = getCategoryValue(item, "use-case-2", null);
  const canonical_routing_slug = getCategoryValue(item, "use-case", null);
  const property_type_scope = readTemplateCategory(
    item,
    ["property-type-scope", "property-type"],
    ["Property Type Scope", "Property Type"],
    null
  );

  return {
    item_id: item?.item_id || null,
    app_id: item?.app?.app_id || item?.app_id || APP_ID,
    raw: item,
    template_id: getNumberValue(item, "template-id", null),
    title: getTextValue(item, "title", "") || cleanTemplateTitle(item),
    use_case: deriveTemplateUseCase(item, variant_group),
    use_case_label,
    canonical_routing_slug,
    variant_group,
    stage_code: getCategoryValue(item, "stage-code", null),
    stage_label: readTemplateCategory(item, ["stage-label"], ["Stage Label"], null),
    tone: getCategoryValue(item, "tone", null),
    gender_variant: getCategoryValue(item, "gender-variant", null),
    language: getCategoryValue(item, "language", "English"),
    sequence_position: readTemplateCategory(
      item,
      ["sequence-position"],
      ["Sequence Position"],
      null
    ),
    paired_with_agent_type: getCategoryValue(item, "paired-with-agent-type", null),
    text: getTextValue(item, "text", ""),
    english_translation: getTextValue(item, "english-translation", ""),
    active: getCategoryValue(item, "active", "No"),
    is_first_touch: readTemplateCategory(
      item,
      ["is-first-touch", "first-touch"],
      ["Is First Touch", "First Touch"],
      null
    ),
    is_ownership_check: getCategoryValue(item, "is-ownership-check", "No"),
    property_type_scope,
    category_primary: property_type_scope,
    category_secondary: firstPresentCategory(item, ["category-2", "category"], null),
    personalization_tags: getCategoryValues(item, "personalization-tags", []),
    deliverability_score: getNumberValue(item, "deliverability-score", 0),
    spam_risk: getNumberValue(item, "spam-risk", null),
    historical_reply_rate: getNumberValue(item, "historical-reply-rate", 0),
    total_sends: getNumberValue(item, "total-sends", 0),
    total_replies: getNumberValue(item, "total-replies", 0),
    total_conversations: getNumberValue(item, "total-conversations", 0),
    cooldown_days: getNumberValue(item, "cooldown-days", 0),
    version: getNumberValue(item, "version", 1),
    last_used:
      fields.find((f) => f?.external_id === "last-used")?.values?.[0]?.start || null,
  };
}

function cleanTemplateTitle(item = null) {
  return String(item?.title ?? "").trim();
}

export async function getTemplateItem(item_id) {
  return getItem(item_id);
}

export async function updateTemplateItem(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export async function findTemplates(filters = {}, limit = MAX_FETCH_LIMIT, offset = 0) {
  return filterAppItems(APP_ID, filters, { limit, offset });
}

export async function findActiveTemplates(limit = MAX_FETCH_LIMIT, offset = 0) {
  return filterAppItems(APP_ID, { active: "Yes" }, { limit, offset });
}

export async function fetchTemplates(filters = {}, limit = MAX_FETCH_LIMIT, offset = 0) {
  const res = await filterAppItems(APP_ID, filters, { limit, offset });
  return safeArray(res?.items).map(normalizeTemplateItem);
}

export default {
  APP_ID,
  MAX_FETCH_LIMIT,
  normalizeTemplateItem,
  getTemplateItem,
  updateTemplateItem,
  findTemplates,
  findActiveTemplates,
  fetchTemplates,
};
