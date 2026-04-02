import APP_IDS from "@/lib/config/app-ids.js";
import { safeCategoryEquals } from "@/lib/providers/podio.js";
import { getAttachedFieldSchema } from "@/lib/podio/schema.js";
import { fetchTemplates } from "@/lib/podio/apps/templates.js";
import { LOCAL_TEMPLATE_CANDIDATES } from "@/lib/domain/templates/local-template-registry.js";

const HARD_SPAM_RISK_CUTOFF = 35;
const TEMPLATE_APP_ID = APP_IDS.templates;
const TEMPLATE_FILTER_ALIAS_MAP = Object.freeze({
  stage: Object.freeze({
    "stage 1 6 scripts": "Stage 1 — Ownership Confirmation",
    "negotiation variants": "Stage 3 — Offer Reveal",
  }),
  category: Object.freeze({
    "distress timing": "Distress",
  }),
  "paired-with-agent-type": Object.freeze({
    "specialist spanish": "Specialist-Spanish / Market-Local",
    "specialist landlord": "Specialist-Landlord / Market-Local",
    "specialist portuguese":
      "Specialist-Portuguese / Specialist-Portuguese-Corporate",
    "specialist italian": "Specialist-Italian / Specialist-Italian-Family",
  }),
});

function hashString(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function clean(value) {
  return String(value ?? "").trim();
}

const TEMPLATE_BATCH_CACHE = new Map();

function normalizeCategoryText(value) {
  return clean(value)
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function getTemplateCategoryValue(external_id, value = null) {
  const raw = clean(value);
  if (!raw) return null;

  const field = getAttachedFieldSchema(TEMPLATE_APP_ID, external_id);
  if (!field?.options?.length) return raw;

  const normalized = normalizeCategoryText(raw);
  const aliased =
    TEMPLATE_FILTER_ALIAS_MAP[external_id]?.[normalized] ?? raw;
  const normalized_alias = normalizeCategoryText(aliased);

  return (
    field.options.find(
      (option) => normalizeCategoryText(option.text) === normalized_alias
    )?.text || null
  );
}

function normalizeTemplateFilters({
  category = null,
  secondary_category = null,
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = null,
  language = null,
  sequence_position = null,
  paired_with_agent_type = null,
  active = "Yes",
}) {
  return {
    category: getTemplateCategoryValue("property-type", category),
    secondary_category: getTemplateCategoryValue("category", secondary_category),
    use_case: getTemplateCategoryValue("use-case", use_case),
    variant_group: getTemplateCategoryValue("stage", variant_group),
    tone: getTemplateCategoryValue("tone", tone),
    gender_variant: getTemplateCategoryValue("gender-variant", gender_variant),
    language: getTemplateCategoryValue("language", language),
    sequence_position: getTemplateCategoryValue("sequence-position", sequence_position),
    paired_with_agent_type: getTemplateCategoryValue(
      "paired-with-agent-type",
      paired_with_agent_type
    ),
    active: getTemplateCategoryValue("active", active),
  };
}

function rotateVariant(templates, rotation_key = null) {
  if (!templates.length) return null;
  if (!rotation_key) return templates[0];

  const index = Math.abs(hashString(String(rotation_key))) % templates.length;
  return templates[index];
}

function buildFilterPayload({
  category = null,
  secondary_category = null,
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = null,
  language = null,
  sequence_position = null,
  paired_with_agent_type = null,
  active = "Yes",
}) {
  const normalized = normalizeTemplateFilters({
    category,
    secondary_category,
    use_case,
    variant_group,
    tone,
    gender_variant,
    language,
    sequence_position,
    paired_with_agent_type,
    active,
  });

  const filters = {};

  if (normalized.category) filters["property-type"] = normalized.category;
  if (normalized.secondary_category) filters["category"] = normalized.secondary_category;
  if (normalized.use_case) filters["use-case"] = normalized.use_case;
  if (normalized.variant_group) filters["stage"] = normalized.variant_group;
  if (normalized.tone) filters["tone"] = normalized.tone;
  if (normalized.gender_variant) filters["gender-variant"] = normalized.gender_variant;
  if (normalized.language) filters["language"] = normalized.language;
  if (normalized.sequence_position) {
    filters["sequence-position"] = normalized.sequence_position;
  }
  if (normalized.paired_with_agent_type) {
    filters["paired-with-agent-type"] = normalized.paired_with_agent_type;
  }
  if (normalized.active) filters["active"] = normalized.active;

  return filters;
}

function removeEmptyTemplates(templates) {
  return templates.filter(
    (t) =>
      safeCategoryEquals(t.active, "Yes") &&
      t.text &&
      t.text.trim().length > 0
  );
}

function applySpamGuard(templates) {
  return templates.filter((t) => t.spam_risk <= HARD_SPAM_RISK_CUTOFF);
}

function dedupeTemplates(templates) {
  const seen = new Set();

  return templates.filter((template) => {
    const key = `${template.item_id}:${template.text}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function applyCooldownFilter(templates, recently_used_template_ids = []) {
  const blocked = new Set(recently_used_template_ids.filter(Boolean));
  return templates.filter((template) => !blocked.has(template.item_id));
}

function scoreTemplate(template, preferences = {}) {
  let score = 0;

  score += template.deliverability_score * 3;
  score += template.historical_reply_rate * 2;
  score += template.total_conversations * 0.2;
  score += template.total_replies * 0.1;
  score -= template.spam_risk * 4;

  if (
    preferences.preferred_tone &&
    safeCategoryEquals(template.tone, preferences.preferred_tone)
  ) {
    score += 12;
  }

  if (
    preferences.preferred_sequence_position &&
    safeCategoryEquals(template.sequence_position, preferences.preferred_sequence_position)
  ) {
    score += 10;
  }

  if (
    preferences.preferred_variant_group &&
    safeCategoryEquals(template.variant_group, preferences.preferred_variant_group)
  ) {
    score += 10;
  }

  if (
    preferences.preferred_agent_type &&
    safeCategoryEquals(template.paired_with_agent_type, preferences.preferred_agent_type)
  ) {
    score += 20;
  }

  if (
    preferences.preferred_use_case &&
    safeCategoryEquals(template.use_case, preferences.preferred_use_case)
  ) {
    score += 22;
  }

  if (
    preferences.preferred_language &&
    safeCategoryEquals(template.language, preferences.preferred_language)
  ) {
    score += 18;
  }

  if (
    preferences.preferred_category &&
    safeCategoryEquals(template.category_primary, preferences.preferred_category)
  ) {
    score += 10;
  }

  if (
    preferences.preferred_secondary_category &&
    safeCategoryEquals(template.category_secondary, preferences.preferred_secondary_category)
  ) {
    score += 8;
  }

  if (safeCategoryEquals(template.active, "Yes")) score += 50;
  if (template.text?.length > 0) score += 25;

  return score;
}

function buildCandidateSets({
  category = "Residential",
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  language = "English",
  sequence_position = null,
  paired_with_agent_type = "Warm Professional",
  fallback_agent_type = "Warm Professional",
  secondary_category = null,
}) {
  const use_cases = uniq([use_case]);
  const sequences = uniq([
    sequence_position,
    "V1",
    "V2",
    "V3",
    "1st Touch",
    "2nd Touch",
    "3rd Touch",
    "4th Touch",
    "Final",
  ]);
  const categories = uniq([category]);
  const secondary_categories = secondary_category ? [secondary_category, null] : [null];

  const sets = [];

  for (const c of categories) {
    for (const sc of secondary_categories) {
      for (const uc of use_cases) {
        for (const seq of sequences) {
          sets.push(
            buildFilterPayload({
              category: c,
              secondary_category: sc,
              use_case: uc,
              variant_group,
              tone,
              gender_variant,
              language,
              sequence_position: seq,
              paired_with_agent_type,
              active: "Yes",
            })
          );

          sets.push(
            buildFilterPayload({
              category: c,
              secondary_category: sc,
              use_case: uc,
              variant_group,
              tone,
              gender_variant,
              language,
              sequence_position: seq,
              paired_with_agent_type: fallback_agent_type,
              active: "Yes",
            })
          );

          sets.push(
            buildFilterPayload({
              category: c,
              secondary_category: sc,
              use_case: uc,
              tone,
              gender_variant,
              language,
              sequence_position: seq,
              active: "Yes",
            })
          );
        }
      }

      sets.push(
        buildFilterPayload({
          category: c,
          secondary_category: sc,
          variant_group,
          tone,
          gender_variant,
          language,
          active: "Yes",
        })
      );
    }
  }

  sets.push(
    buildFilterPayload({
      category,
      secondary_category,
      use_case,
      variant_group,
      tone,
      gender_variant: "Neutral",
      language: "English",
      paired_with_agent_type: "Warm Professional",
      active: "Yes",
    })
  );

  return sets;
}

function buildLocalFilterPayload({
  category = null,
  secondary_category = null,
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = null,
  language = null,
  sequence_position = null,
  paired_with_agent_type = null,
  active = "Yes",
}) {
  return {
    category_primary: category || null,
    category_secondary: secondary_category || null,
    use_case: use_case || null,
    variant_group: variant_group || null,
    tone: tone || null,
    gender_variant: gender_variant || null,
    language: language || null,
    sequence_position: sequence_position || null,
    paired_with_agent_type: paired_with_agent_type || null,
    active: active || null,
  };
}

function buildLocalCandidateSets({
  category = "Residential",
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  language = "English",
  sequence_position = null,
  paired_with_agent_type = "Warm Professional",
  fallback_agent_type = "Warm Professional",
  secondary_category = null,
}) {
  const use_cases = uniq([use_case]);
  const sequences = uniq([
    sequence_position,
    "V1",
    "V2",
    "V3",
    "1st Touch",
    "2nd Touch",
    "3rd Touch",
    "4th Touch",
    "Final",
  ]);
  const categories = uniq([category]);
  const secondary_categories = secondary_category ? [secondary_category, null] : [null];
  const sets = [];

  for (const c of categories) {
    for (const sc of secondary_categories) {
      for (const uc of use_cases) {
        for (const seq of sequences) {
          sets.push(
            buildLocalFilterPayload({
              category: c,
              secondary_category: sc,
              use_case: uc,
              variant_group,
              tone,
              gender_variant,
              language,
              sequence_position: seq,
              paired_with_agent_type,
              active: "Yes",
            })
          );

          sets.push(
            buildLocalFilterPayload({
              category: c,
              secondary_category: sc,
              use_case: uc,
              variant_group,
              tone,
              gender_variant,
              language,
              sequence_position: seq,
              paired_with_agent_type: fallback_agent_type,
              active: "Yes",
            })
          );

          sets.push(
            buildLocalFilterPayload({
              category: c,
              secondary_category: sc,
              use_case: uc,
              tone,
              gender_variant,
              language,
              sequence_position: seq,
              active: "Yes",
            })
          );
        }
      }

      sets.push(
        buildLocalFilterPayload({
          category: c,
          secondary_category: sc,
          variant_group,
          tone,
          gender_variant,
          language,
          active: "Yes",
        })
      );
    }
  }

  sets.push(
    buildLocalFilterPayload({
      category,
      secondary_category,
      use_case,
      variant_group,
      tone,
      gender_variant: "Neutral",
      language: "English",
      paired_with_agent_type: "Warm Professional",
      active: "Yes",
    })
  );

  return sets;
}

function matchesLocalTemplateFilter(template, filter_set = {}) {
  return Object.entries(filter_set).every(([key, value]) => {
    if (!value) return true;
    return safeCategoryEquals(template?.[key], value);
  });
}

function fetchLocalTemplates(filter_set = {}) {
  return LOCAL_TEMPLATE_CANDIDATES.filter((template) =>
    matchesLocalTemplateFilter(template, filter_set)
  );
}

function stableTemplateBatchCacheKey(filter_set = {}) {
  const entries = Object.entries(filter_set)
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .sort(([left], [right]) => left.localeCompare(right));

  return JSON.stringify(entries);
}

export function clearTemplateBatchCache() {
  TEMPLATE_BATCH_CACHE.clear();
}

export async function fetchTemplatesCached(
  filter_set = {},
  {
    fetcher = fetchTemplates,
    cache = TEMPLATE_BATCH_CACHE,
  } = {}
) {
  const cache_key = stableTemplateBatchCacheKey(filter_set);
  if (cache.has(cache_key)) {
    return cache.get(cache_key);
  }

  const batch = await fetcher(filter_set);
  cache.set(cache_key, batch);
  return batch;
}

export async function loadTemplateCandidates({
  category = "Residential",
  secondary_category = null,
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  language = "English",
  sequence_position = null,
  paired_with_agent_type = "Warm Professional",
  recently_used_template_ids = [],
  fallback_agent_type = "Warm Professional",
}) {
  const normalized_preferences = normalizeTemplateFilters({
    category,
    secondary_category,
    use_case,
    variant_group,
    tone,
    gender_variant,
    language,
    sequence_position,
    paired_with_agent_type,
  });

  const candidate_sets = buildCandidateSets({
    category,
    secondary_category,
    use_case,
    variant_group,
    tone,
    gender_variant,
    language,
    sequence_position,
    paired_with_agent_type,
    fallback_agent_type,
  });
  const local_candidate_sets = buildLocalCandidateSets({
    category,
    secondary_category,
    use_case,
    variant_group,
    tone,
    gender_variant,
    language,
    sequence_position,
    paired_with_agent_type,
    fallback_agent_type,
  });

  let all_candidates = [];

  for (const filter_set of candidate_sets) {
    const batch = await fetchTemplatesCached(filter_set);
    all_candidates.push(...batch);

    if (batch.length > 0 && all_candidates.length >= 20) break;
  }

  for (const filter_set of local_candidate_sets) {
    const batch = fetchLocalTemplates(filter_set);
    all_candidates.push(...batch);

    if (batch.length > 0 && all_candidates.length >= 20) break;
  }

  all_candidates = dedupeTemplates(removeEmptyTemplates(all_candidates));
  all_candidates = applySpamGuard(all_candidates);
  all_candidates = applyCooldownFilter(all_candidates, recently_used_template_ids);

  const scored = all_candidates
    .map((template) => ({
      ...template,
      score: scoreTemplate(template, {
        preferred_tone: normalized_preferences.tone,
        preferred_sequence_position:
          normalized_preferences.sequence_position || sequence_position,
        preferred_variant_group:
          normalized_preferences.variant_group || variant_group,
        preferred_agent_type:
          normalized_preferences.paired_with_agent_type || paired_with_agent_type,
        preferred_use_case: normalized_preferences.use_case || use_case,
        preferred_language: normalized_preferences.language || language,
        preferred_category: normalized_preferences.category || category,
        preferred_secondary_category:
          normalized_preferences.secondary_category || secondary_category,
      }),
    }))
    .sort((a, b) => b.score - a.score);

  return scored;
}

/**
 * Fallback order:
 * 1. exact-ish match: language + use_case + persona + stage
 * 2. fallback persona
 * 3. same language + use_case
 * 4. same language + stage
 * 5. English + Warm Professional
 */
export async function loadTemplate({
  category = "Residential",
  secondary_category = null,
  use_case = null,
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  language = "English",
  sequence_position = null,
  paired_with_agent_type = "Warm Professional",
  recently_used_template_ids = [],
  rotation_key = null,
  fallback_agent_type = "Warm Professional",
}) {
  const scored = await loadTemplateCandidates({
    category,
    secondary_category,
    use_case,
    variant_group,
    tone,
    gender_variant,
    language,
    sequence_position,
    paired_with_agent_type,
    recently_used_template_ids,
    fallback_agent_type,
  });

  if (!scored.length) return null;

  const top_score = scored[0].score;
  const top_cluster = scored.filter((t) => t.score >= top_score - 10);

  return rotateVariant(top_cluster, rotation_key);
}

export default {
  loadTemplateCandidates,
  loadTemplate,
};
