// ─── choose-template.js ───────────────────────────────────────────────────
import {
  fetchAllItems,
  getNumberValue,
  getTextValue,
  getCategoryValue,
  getField,
} from "@/lib/providers/podio.js";
import APP_IDS from "@/lib/config/app-ids.js";

import { info } from "@/lib/logging/logger.js";

const DEFAULT_TEMPLATE_APP_ID = APP_IDS.templates;

const TEMPLATE_FIELDS = {
  template_id: ["template-id"],
  title: ["title"],
  body: ["text", "english-translation"],
  category: ["property-type", "category"],
  use_case: ["use-case"],
  stage: ["stage"],
  language: ["language"],
  status: ["active"],
};

function normalize(value) {
  return String(value ?? "").trim().toLowerCase();
}

function firstPresentText(item, external_ids = []) {
  for (const eid of external_ids) {
    const value = getTextValue(item, eid, "");
    if (value) return value;
  }
  return "";
}

function firstPresentCategory(item, external_ids = []) {
  for (const eid of external_ids) {
    const value = getCategoryValue(item, eid, null);
    if (value) return value;
  }
  return null;
}

function firstPresentNumber(item, external_ids = []) {
  for (const eid of external_ids) {
    const value = getNumberValue(item, eid, null);
    if (value !== null && value !== undefined) return value;
  }
  return null;
}

function hasAnyField(item, external_ids = []) {
  return external_ids.some((eid) => Boolean(getField(item, eid)));
}

function normalizeStageLabel(value) {
  const raw = normalize(value);
  if (!raw) return null;

  if (["ownership"].includes(raw)) return "ownership";
  if (["offer"].includes(raw)) return "offer";
  if (["q/a", "qa", "q&a"].includes(raw)) return "q/a";
  if (["contract"].includes(raw)) return "contract";
  if (["follow-up", "follow up", "followup"].includes(raw)) return "follow-up";

  return raw;
}

function normalizeMessageType(value) {
  const raw = normalize(value);
  if (!raw) return null;

  if (["cold outbound"].includes(raw)) return "cold outbound";
  if (["follow-up", "follow up", "followup"].includes(raw)) return "follow-up";
  if (["re-engagement", "reengagement"].includes(raw)) return "re-engagement";
  if (["opt-out confirm", "opt out confirm"].includes(raw)) return "opt-out confirm";

  return raw;
}

function buildTemplateRecord(item) {
  const template_id = firstPresentNumber(item, TEMPLATE_FIELDS.template_id);
  const title = firstPresentText(item, TEMPLATE_FIELDS.title);
  const body = firstPresentText(item, TEMPLATE_FIELDS.body);
  const category = firstPresentCategory(item, TEMPLATE_FIELDS.category);
  const use_case = firstPresentCategory(item, TEMPLATE_FIELDS.use_case) || firstPresentText(item, TEMPLATE_FIELDS.use_case);
  const stage = firstPresentCategory(item, TEMPLATE_FIELDS.stage) || firstPresentText(item, TEMPLATE_FIELDS.stage);
  const language = firstPresentCategory(item, TEMPLATE_FIELDS.language) || firstPresentText(item, TEMPLATE_FIELDS.language);
  const status = firstPresentCategory(item, TEMPLATE_FIELDS.status) || firstPresentText(item, TEMPLATE_FIELDS.status);

  return {
    item,
    item_id: item?.item_id ?? null,
    template_id,
    title,
    body,
    category,
    use_case,
    stage,
    language,
    status,
  };
}

function scoreTemplate(template, desired) {
  let score = 0;
  const reasons = [];

  if (!template.body || !String(template.body).trim()) {
    return { score: -9999, reasons: ["missing_body"] };
  }

  const desired_template_id = desired.template_id ?? null;
  const desired_template_name = desired.template_name ?? null;
  const desired_category = desired.category ?? null;
  const desired_message_type = desired.message_type ?? null;
  const desired_stage = desired.stage ?? null;
  const desired_language = desired.language ?? null;

  if (desired_template_id !== null && template.template_id === desired_template_id) {
    score += 10_000;
    reasons.push("exact_template_id");
  }

  const title_norm = normalize(template.title);
  const desired_name_norm = normalize(desired_template_name);

  if (desired_name_norm) {
    if (title_norm === desired_name_norm) {
      score += 500;
      reasons.push("exact_name");
    } else if (title_norm.includes(desired_name_norm) || desired_name_norm.includes(title_norm)) {
      score += 250;
      reasons.push("partial_name");
    }
  }

  const category_norm = normalize(template.category);
  const desired_category_norm = normalize(desired_category);

  if (desired_category_norm) {
    if (category_norm === desired_category_norm) {
      score += 150;
      reasons.push("category_match");
    } else if (title_norm.includes(desired_category_norm)) {
      score += 60;
      reasons.push("category_in_title");
    }
  }

  const use_case_norm = normalize(template.use_case);
  const desired_message_type_norm = normalizeMessageType(desired_message_type);

  if (desired_message_type_norm) {
    if (use_case_norm === desired_message_type_norm) {
      score += 140;
      reasons.push("message_type_match");
    } else if (title_norm.includes(desired_message_type_norm)) {
      score += 60;
      reasons.push("message_type_in_title");
    }
  }

  const stage_norm = normalizeStageLabel(template.stage);
  const desired_stage_norm = normalizeStageLabel(desired_stage);

  if (desired_stage_norm) {
    if (stage_norm === desired_stage_norm) {
      score += 120;
      reasons.push("stage_match");
    } else if (title_norm.includes(desired_stage_norm)) {
      score += 40;
      reasons.push("stage_in_title");
    }
  }

  const lang_norm = normalize(template.language);
  const desired_lang_norm = normalize(desired_language);

  if (desired_lang_norm) {
    if (lang_norm === desired_lang_norm) {
      score += 80;
      reasons.push("language_match");
    }
  }

  // Mild preference for records that actually expose structured fields
  if (hasAnyField(template.item, TEMPLATE_FIELDS.category)) score += 5;
  if (hasAnyField(template.item, TEMPLATE_FIELDS.use_case)) score += 5;
  if (hasAnyField(template.item, TEMPLATE_FIELDS.stage)) score += 5;

  return { score, reasons };
}

export async function chooseTemplate({
  template_app_id = DEFAULT_TEMPLATE_APP_ID,
  template_id = null,
  template_name = null,
  category = null,
  message_type = null,
  stage = null,
  language = null,
  context = null,
} = {}) {
  const desired = {
    template_id,
    template_name,
    category,
    message_type,
    stage,
    language: language || context?.summary?.language_preference || null,
  };

  info("template.choose_started", {
    template_app_id,
    template_id,
    template_name,
    category,
    message_type,
    stage,
    language: desired.language,
  });

  const items = await fetchAllItems(template_app_id, {}, { page_size: 200 });

  if (!items.length) {
    throw new Error(`chooseTemplate: no templates found in app ${template_app_id}`);
  }

  const candidates = items
    .map(buildTemplateRecord)
    .map((template) => {
      const { score, reasons } = scoreTemplate(template, desired);
      return { ...template, score, reasons };
    })
    .filter((template) => template.score > -9999)
    .sort((a, b) => b.score - a.score);

  if (!candidates.length) {
    throw new Error("chooseTemplate: no usable template candidates found");
  }

  const winner = candidates[0];

  info("template.choose_completed", {
    template_app_id,
    selected_item_id: winner.item_id,
    selected_template_id: winner.template_id,
    selected_title: winner.title,
    selected_score: winner.score,
    selected_reasons: winner.reasons,
  });

  return {
    ok: true,
    template_app_id,
    selected_item_id: winner.item_id,
    selected_template_id: winner.template_id,
    selected_title: winner.title,
    selected_body: winner.body,
    selected_category: winner.category,
    selected_use_case: winner.use_case,
    selected_stage: winner.stage,
    selected_language: winner.language,
    score: winner.score,
    reasons: winner.reasons,
    top_candidates: candidates.slice(0, 5).map((x) => ({
      item_id: x.item_id,
      template_id: x.template_id,
      title: x.title,
      score: x.score,
      reasons: x.reasons,
    })),
    raw: winner.item,
  };
}

export default chooseTemplate;
