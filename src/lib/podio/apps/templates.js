import APP_IDS from "@/lib/config/app-ids.js";
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

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

export function normalizeTemplateItem(item) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];

  return {
    item_id: item?.item_id || null,
    app_id: item?.app?.app_id || item?.app_id || APP_ID,
    raw: item,
    template_id: getNumberValue(item, "template-id", null),
    title: getTextValue(item, "title", "") || cleanTemplateTitle(item),
    use_case: getCategoryValue(item, "use-case", null),
    variant_group: getCategoryValue(item, "stage", null),
    tone: getCategoryValue(item, "tone", null),
    gender_variant: getCategoryValue(item, "gender-variant", null),
    language: getCategoryValue(item, "language", "English"),
    sequence_position: getCategoryValue(item, "sequence-position", null),
    paired_with_agent_type: getCategoryValue(item, "paired-with-agent-type", null),
    text: getTextValue(item, "text", ""),
    english_translation: getTextValue(item, "english-translation", ""),
    active: getCategoryValue(item, "active", "No"),
    is_ownership_check: getCategoryValue(item, "is-ownership-check", "No"),
    category_primary: getCategoryValue(item, "property-type", null),
    category_secondary: getCategoryValue(item, "category", null),
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
