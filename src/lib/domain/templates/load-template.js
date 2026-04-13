import APP_IDS from "@/lib/config/app-ids.js";
import { normalizeSellerFlowUseCase } from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { evaluateTemplatePlaceholders } from "@/lib/domain/templates/render-template.js";
import { info, warn } from "@/lib/logging/logger.js";
import { getCategoryValue, safeCategoryEquals } from "@/lib/providers/podio.js";
import { getAttachedFieldSchema } from "@/lib/podio/schema.js";
import { fetchTemplates } from "@/lib/podio/apps/templates.js";
import { LOCAL_TEMPLATE_CANDIDATES } from "@/lib/domain/templates/local-template-registry.js";

const HARD_SPAM_RISK_CUTOFF = 35;
const TEMPLATE_APP_ID = APP_IDS.templates;
const TEMPLATE_FILTER_ALIAS_MAP = Object.freeze({
  stage: Object.freeze({
    "stage 1 ownership check": "Stage 1 — Ownership Confirmation",
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

function getTemplateSecondaryCategoryFieldExternalId() {
  return "category-2";
}

const TEMPLATE_LOOKUP_USE_CASE_ALIASES = Object.freeze({
  ask_timeline: Object.freeze([
    "text_me_later_specific",
    "not_ready",
    "seller_stalling_after_yes",
  ]),
  ask_condition_clarifier: Object.freeze([
    "condition_question_set",
    "walkthrough_or_condition",
    "occupied_asset",
    "vacant_boarded_probe",
    "has_tenants",
  ]),
  narrow_range: Object.freeze([
    "can_you_do_better",
    "best_price",
    "price_too_low",
  ]),
  mf_offer_reveal: Object.freeze([
    "offer_reveal_cash",
  ]),
  offer_reveal_cash_follow_up: Object.freeze([
    "offer_no_response_followup",
    "followup_soft",
    "followup_hard",
    "persona_warm_professional_followup",
    "persona_neighborly_followup",
    "persona_empathetic_followup",
    "persona_investor_direct_followup",
    "persona_no-nonsense_closer_followup",
  ]),
  price_works_confirm_basics_follow_up: Object.freeze([
    "followup_soft",
  ]),
  price_high_condition_probe_follow_up: Object.freeze([
    "followup_hard",
    "followup_soft",
  ]),
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

function normalizeFieldLabel(value) {
  return clean(value)
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function resolveNormalizedTemplateUseCase(template = null) {
  return (
    normalizeSellerFlowUseCase(
      clean(template?.use_case_label) ||
        clean(template?.use_case) ||
        clean(template?.canonical_routing_slug),
      clean(template?.variant_group) || clean(template?.stage_label) || null
    ) || null
  );
}

function getFirstKnownTemplateFieldExternalId(external_ids = []) {
  return (
    external_ids.find((external_id) =>
      Boolean(getAttachedFieldSchema(TEMPLATE_APP_ID, external_id))
    ) || null
  );
}

function getRawCategoryValueByFieldLabels(item, labels = [], fallback = null) {
  const wanted = new Set(labels.map((label) => normalizeFieldLabel(label)).filter(Boolean));
  if (!wanted.size || !Array.isArray(item?.fields)) return fallback;

  for (const field of item.fields) {
    const candidates = [
      field?.label,
      field?.config?.label,
      field?.field?.label,
    ];
    const matches = candidates.some((value) => wanted.has(normalizeFieldLabel(value)));
    if (!matches) continue;

    const value = Array.isArray(field?.values)
      ? field.values
          .map((entry) => entry?.value?.text ?? (typeof entry?.value === "string" ? entry.value : null))
          .find((entry) => clean(entry))
      : null;

    if (clean(value)) return value;
  }

  return fallback;
}

function readTemplateValue(
  template = null,
  {
    direct_values = [],
    raw_external_ids = [],
    raw_labels = [],
  } = {},
  fallback = null
) {
  for (const value of direct_values) {
    if (clean(value)) return value;
  }

  const raw_item = template?.raw || null;
  for (const external_id of raw_external_ids) {
    const value = getCategoryValue(raw_item, external_id, null);
    if (clean(value)) return value;
  }

  const labeled_value = getRawCategoryValueByFieldLabels(raw_item, raw_labels, null);
  if (clean(labeled_value)) return labeled_value;

  return fallback;
}

function resolveTouchOneTemplateFirstTouch(template = null) {
  return (
    readTemplateValue(
      template,
      {
        direct_values: [
          template?.is_first_touch,
          template?.is_first_contact,
          template?.first_touch,
        ],
        raw_external_ids: ["is-first-touch", "first-touch"],
        raw_labels: ["Is First Touch", "First Touch"],
      },
      null
    ) || null
  );
}

function resolveTouchOneTemplatePropertyScope(template = null) {
  return (
    readTemplateValue(
      template,
      {
        direct_values: [template?.property_type_scope, template?.category_primary],
        raw_external_ids: ["property-type-scope", "property-type"],
        raw_labels: ["Property Type Scope", "Property Type"],
      },
      null
    ) || null
  );
}

function resolveTouchOneRequestedPropertyType({
  category = null,
  context = null,
} = {}) {
  const property_item = context?.items?.property_item || null;

  return (
    clean(
      getCategoryValue(property_item, "property-type", null) ||
        getCategoryValue(property_item, "property-class", null) ||
        context?.summary?.property_type ||
        context?.summary?.property_class ||
        category ||
        null
    ) || null
  );
}

const TOUCH_ONE_RESIDENTIAL_LEAD_TYPES = new Set([
  "any residential",
  "condo",
  "condominium",
  "mobile home",
  "residential",
  "sfr",
  "single family",
  "single-family",
  "townhouse",
]);

const TOUCH_ONE_MULTIFAMILY_ONLY_SCOPE_MARKERS = [
  "5 units",
  "5 units plus",
  "5+ units",
  "apartment",
  "duplex",
  "fourplex",
  "landlord multifamily",
  "multi family",
  "multifamily",
  "quadplex",
  "triplex",
];

function isTouchOneResidentialLead(requested_property_type = null) {
  const normalized = normalizeCategoryText(requested_property_type);
  return !normalized || TOUCH_ONE_RESIDENTIAL_LEAD_TYPES.has(normalized);
}

function isExplicitTouchOnePropertyScopeIncompatible(property_type_scope = null) {
  const normalized_scope = normalizeCategoryText(property_type_scope);
  if (!normalized_scope || normalized_scope === "any residential") return false;

  return TOUCH_ONE_MULTIFAMILY_ONLY_SCOPE_MARKERS.some(
    (marker) => normalized_scope === marker || normalized_scope.includes(marker)
  );
}

function isTouchOnePropertyScopeRejected({
  requested_property_type = null,
  property_type_scope = null,
} = {}) {
  if (!isTouchOneResidentialLead(requested_property_type)) return false;
  return isExplicitTouchOnePropertyScopeIncompatible(property_type_scope);
}

function touchOneLanguageBonus(template_language = null, preferred_language = "English") {
  if (clean(template_language) && safeCategoryEquals(template_language, preferred_language)) {
    return 300;
  }

  if (clean(template_language) && safeCategoryEquals(template_language, "English")) {
    return 200;
  }

  return 100;
}

function touchOnePropertyScopeBonus(property_type_scope = null) {
  return safeCategoryEquals(property_type_scope, "Any Residential") ? 20 : 0;
}

function touchOneSequenceBonus(sequence_position = null) {
  return safeCategoryEquals(sequence_position, "1st Touch") ? 11 : 0;
}

function touchOneQualityTiebreaker(template = null) {
  const deliverability = Number.isFinite(Number(template?.deliverability_score))
    ? Number(template.deliverability_score) * 0.001
    : 0;
  const reply_rate = Number.isFinite(Number(template?.historical_reply_rate))
    ? Number(template.historical_reply_rate) * 0.0001
    : 0;
  const conversations = Number.isFinite(Number(template?.total_conversations))
    ? Math.min(Number(template.total_conversations), 99) * 0.00001
    : 0;

  return deliverability + reply_rate + conversations;
}

function summarizeTouchOneCandidate(template = null) {
  return {
    template_id: template?.item_id ?? null,
    active: clean(template?.active) || null,
    use_case: clean(template?.resolved_touch_one_use_case || template?.use_case) || null,
    is_first_touch: clean(template?.resolved_touch_one_is_first_touch) || null,
    language: clean(template?.language) || null,
    property_type_scope: clean(template?.resolved_touch_one_property_type_scope) || null,
    sequence_position: clean(template?.sequence_position) || null,
    stage_label: clean(template?.stage_label) || clean(template?.variant_group) || null,
    rejection_reasons: Array.isArray(template?.rejection_reasons)
      ? template.rejection_reasons
      : [],
  };
}

function countTouchOneRejectionReasons(candidates = []) {
  return candidates.reduce((counts, candidate) => {
    for (const reason of candidate?.rejection_reasons || []) {
      counts[reason] = (counts[reason] || 0) + 1;
    }

    return counts;
  }, {});
}

function filterStrictTouchOneCandidates(
  templates = [],
  {
    preferred_language = "English",
    requested_property_type = null,
    owner_id = null,
    touch_number = 1,
    recently_used_template_ids = [],
  } = {}
) {
  const recently_used = new Set(recently_used_template_ids.filter(Boolean));
  const evaluated_candidates = templates.map((template) => {
    const rejection_reasons = [];
    const resolved_use_case = resolveNormalizedTemplateUseCase(template);
    const resolved_is_first_touch = resolveTouchOneTemplateFirstTouch(template);
    const resolved_property_type_scope = resolveTouchOneTemplatePropertyScope(template);

    if (!safeCategoryEquals(template?.active, "Yes")) {
      rejection_reasons.push("inactive");
    }

    if (!clean(template?.text)) {
      rejection_reasons.push("empty_text");
    }

    if (Number.isFinite(Number(template?.spam_risk)) && Number(template.spam_risk) > HARD_SPAM_RISK_CUTOFF) {
      rejection_reasons.push("spam_risk_exceeded");
    }

    if (recently_used.has(template?.item_id)) {
      rejection_reasons.push("recently_used");
    }

    if (resolved_use_case !== "ownership_check") {
      rejection_reasons.push("use_case_mismatch");
    }

    if (!safeCategoryEquals(resolved_is_first_touch, "Yes")) {
      rejection_reasons.push("not_first_touch");
    }

    if (
      isTouchOnePropertyScopeRejected({
        requested_property_type,
        property_type_scope: resolved_property_type_scope,
      })
    ) {
      rejection_reasons.push("property_type_scope_incompatible");
    }

    return {
      ...template,
      resolved_touch_one_use_case: resolved_use_case,
      resolved_touch_one_is_first_touch: resolved_is_first_touch,
      resolved_touch_one_property_type_scope: resolved_property_type_scope,
      rejection_reasons,
      score:
        touchOneLanguageBonus(template?.language, preferred_language) +
        touchOnePropertyScopeBonus(resolved_property_type_scope) +
        touchOneSequenceBonus(template?.sequence_position) +
        touchOneQualityTiebreaker(template),
    };
  });

  const survivors = evaluated_candidates
    .filter((candidate) => candidate.rejection_reasons.length === 0)
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return String(left.item_id ?? "").localeCompare(String(right.item_id ?? ""));
    });

  const audit_payload = {
    owner_id,
    touch_number,
    requested_language: preferred_language || "English",
    requested_property_type,
    total_candidates: evaluated_candidates.length,
    candidates: evaluated_candidates.map(summarizeTouchOneCandidate),
    survivors: survivors.map((candidate) => candidate.item_id),
  };

  info("template.touch_one_candidate_audit", audit_payload);

  if (!survivors.length) {
    warn("template.strict_touch_one_filter_empty", {
      owner_id,
      touch_number,
      requested_language: preferred_language || "English",
      requested_property_type,
      total_input: templates.length,
      final_candidates: 0,
      rejection_counts: countTouchOneRejectionReasons(evaluated_candidates),
      audit_payload,
    });
  }

  return survivors;
}

const TEMPLATE_BATCH_CACHE = new Map();
const TEMPLATE_BATCH_CACHE_TTL_MS = 2 * 60_000;

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
    )?.text || aliased
  );
}

function expandTemplateLookupUseCases(use_case = null) {
  const normalized_use_case =
    normalizeSellerFlowUseCase(use_case) || clean(use_case) || null;

  return uniq([
    normalized_use_case,
    ...(TEMPLATE_LOOKUP_USE_CASE_ALIASES[normalized_use_case] || []),
  ]);
}

function buildLanguagePriority(language = "English", allow_language_fallback = true) {
  const requested_language = clean(language) || "English";

  return uniq([
    requested_language,
    allow_language_fallback && !safeCategoryEquals(requested_language, "English")
      ? "English"
      : null,
  ]);
}

function isTemplateFilterValidationError(error) {
  const message = clean(error?.message).toLowerCase();

  return (
    Number(error?.status || error?.response?.status || 0) === 400 &&
    (
      message.includes("invalid category value") ||
      message.includes("invalid value")
    )
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
  const secondary_category_external_id = getTemplateSecondaryCategoryFieldExternalId();

  return {
    category: getTemplateCategoryValue("property-type", category),
    secondary_category: getTemplateCategoryValue(
      secondary_category_external_id,
      secondary_category
    ),
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
  const secondary_category_external_id = getTemplateSecondaryCategoryFieldExternalId();
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
  if (normalized.secondary_category) {
    filters[secondary_category_external_id] = normalized.secondary_category;
  }
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
  return templates.filter((t) => {
    if (!Number.isFinite(t?.spam_risk)) return true;
    return t.spam_risk <= HARD_SPAM_RISK_CUTOFF;
  });
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
  const spam_risk = Number.isFinite(template?.spam_risk) ? template.spam_risk : 0;
  const preferred_use_cases = Array.isArray(preferences.preferred_use_cases)
    ? preferences.preferred_use_cases.filter(Boolean)
    : [];

  score += template.deliverability_score * 3;
  score += template.historical_reply_rate * 2;
  score += template.total_conversations * 0.2;
  score += template.total_replies * 0.1;
  score -= spam_risk * 4;

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

  if (preferred_use_cases.some((value) => safeCategoryEquals(template.use_case, value))) {
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
  if (template.source === "local_registry") score -= 250;

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
  const categories = category ? [category] : [null];
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
  const categories = category ? [category] : [null];
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
    cache_ttl_ms = TEMPLATE_BATCH_CACHE_TTL_MS,
  } = {}
) {
  const cache_key = stableTemplateBatchCacheKey(filter_set);
  const cached = cache.get(cache_key);

  if (
    cached &&
    Number.isFinite(Number(cached.expires_at)) &&
    Number(cached.expires_at) > Date.now()
  ) {
    return cached.value;
  }

  const batch = await fetcher(filter_set);
  cache.set(cache_key, {
    value: batch,
    expires_at: Date.now() + Math.max(Number(cache_ttl_ms) || 0, 0),
  });
  return batch;
}

function withTemplateSource(templates = [], source = "podio") {
  return templates.map((template) => ({
    ...template,
    source: template?.source || source,
  }));
}

function createTemplateResolutionDiagnostics() {
  return {
    podio_fetch_failures: 0,
    podio_filter_validation_failures: 0,
    podio_batches_with_results: 0,
    podio_candidates_considered: 0,
    local_candidates_considered: 0,
    selected_bucket_source: null,
  };
}

function summarizeTemplateResolutionDiagnostics(diagnostics = {}) {
  return {
    podio_fetch_failures: diagnostics.podio_fetch_failures || 0,
    podio_filter_validation_failures:
      diagnostics.podio_filter_validation_failures || 0,
    podio_batches_with_results: diagnostics.podio_batches_with_results || 0,
    podio_candidates_considered: diagnostics.podio_candidates_considered || 0,
    local_candidates_considered: diagnostics.local_candidates_considered || 0,
    selected_bucket_source: diagnostics.selected_bucket_source || null,
  };
}

function filterRenderableTemplates(
  templates = [],
  {
    context = null,
    template_render_overrides = {},
  } = {}
) {
  return templates.filter((template) =>
    evaluateTemplatePlaceholders({
      template_text: template?.text || "",
      use_case: template?.use_case || null,
      variant_group: template?.variant_group || null,
      context,
      overrides: template_render_overrides,
    }).ok
  );
}

async function collectBucketCandidates({
  source = "podio",
  filter_sets = [],
  remote_fetcher = fetchTemplatesCached,
  local_fetcher = fetchLocalTemplates,
  recently_used_template_ids = [],
  context = null,
  template_render_overrides = {},
  preferences = {},
  // When provided (Set), only templates whose variant_group is null/empty OR
  // is in this set are kept.  Used by first-touch callers to exclude follow-up
  // and later-stage templates from the scoring pool without needing a separate
  // Podio query.
  allowed_variant_groups = null,
  required_use_cases = null,
  required_variant_groups = null,
  require_explicit_variant_group = false,
  strict_touch_one_podio_only = false,
  strict_touch_one_language = "English",
  strict_touch_one_property_type = null,
  strict_touch_one_owner_id = null,
  strict_touch_one_touch_number = 1,
  diagnostics = null,
}) {
  let all_candidates = [];

  for (const filter_set of filter_sets) {
    let batch = [];

    if (source === "podio") {
      try {
        batch = await remote_fetcher(filter_set);
        batch = withTemplateSource(batch, "podio");
      } catch (error) {
        if (isTemplateFilterValidationError(error)) {
          if (diagnostics) diagnostics.podio_filter_validation_failures += 1;
          batch = [];
        } else {
          if (diagnostics) diagnostics.podio_fetch_failures += 1;
          batch = [];
        }
      }
    } else {
      batch = withTemplateSource(local_fetcher(filter_set), "local_registry");
    }

    if (diagnostics && source === "podio" && batch.length > 0) {
      diagnostics.podio_batches_with_results += 1;
    }

    all_candidates.push(...batch);

    if (batch.length > 0 && all_candidates.length >= 20) break;
  }

  all_candidates = dedupeTemplates(all_candidates);

  if (strict_touch_one_podio_only) {
    all_candidates = filterStrictTouchOneCandidates(all_candidates, {
      preferred_language: strict_touch_one_language,
      requested_property_type: strict_touch_one_property_type,
      owner_id: strict_touch_one_owner_id,
      touch_number: strict_touch_one_touch_number,
      recently_used_template_ids,
    });
  } else {
    all_candidates = removeEmptyTemplates(all_candidates);
    all_candidates = applySpamGuard(all_candidates);
    all_candidates = applyCooldownFilter(all_candidates, recently_used_template_ids);

    // FIX 1: Touch 1 templates MUST NOT be dropped due to missing placeholders.
    // Stage 1 ownership_check templates are sent with safe rendering (missing
    // values replaced with empty strings) so placeholder availability must not
    // gate template selection.  Non-Touch-1 paths still filter by renderability.
    all_candidates = filterRenderableTemplates(all_candidates, {
      context,
      template_render_overrides,
    });
  }

  // Restrict to allowed variant groups when a caller-supplied allow-list is present.
  // Templates with null/empty variant_group are always permitted — they carry no
  // stage metadata and therefore cannot be classified as a forbidden stage.
  if (allowed_variant_groups?.size > 0) {
    all_candidates = all_candidates.filter(
      (t) => !t.variant_group || allowed_variant_groups.has(t.variant_group)
    );
  }

  if (required_use_cases?.size > 0) {
    const normalized_required_use_cases = new Set(
      [...required_use_cases].map((value) => clean(value).toLowerCase()).filter(Boolean)
    );
    all_candidates = all_candidates.filter((template) =>
      normalized_required_use_cases.has(clean(template?.use_case).toLowerCase())
    );
  }

  if (required_variant_groups?.size > 0) {
    all_candidates = all_candidates.filter((template) => {
      const variant_group = clean(template?.variant_group);
      if (!variant_group) return !require_explicit_variant_group;
      return required_variant_groups.has(variant_group);
    });
  }

  if (diagnostics) {
    if (source === "podio") {
      diagnostics.podio_candidates_considered += all_candidates.length;
    } else {
      diagnostics.local_candidates_considered += all_candidates.length;
    }
  }

  if (!all_candidates.length) return [];

  if (strict_touch_one_podio_only) {
    return all_candidates;
  }

  return all_candidates
    .map((template) => ({
      ...template,
      score: scoreTemplate(template, preferences),
    }))
    .sort((a, b) => b.score - a.score);
}

function buildPriorityFilterSets({
  source = "podio",
  category = "Residential",
  secondary_category = null,
  use_cases = [],
  variant_group = null,
  tone = null,
  gender_variant = "Neutral",
  languages = [],
  sequence_position = null,
  paired_with_agent_type = "Warm Professional",
  fallback_agent_type = "Warm Professional",
  include_variant_group = false,
}) {
  const builder =
    source === "podio" ? buildCandidateSets : buildLocalCandidateSets;
  const categories = [
    {
      category,
      secondary_category,
    },
  ];

  if (category || secondary_category) {
    categories.push({
      category: null,
      secondary_category: null,
    });
  }
  const filters = [];

  for (const language of languages) {
    for (const requested of categories) {
      if (use_cases.length) {
        for (const use_case of use_cases) {
          filters.push(
            ...builder({
              category: requested?.category ?? null,
              secondary_category: requested?.secondary_category ?? null,
              use_case,
              variant_group: include_variant_group ? variant_group : null,
              tone,
              gender_variant,
              language,
              sequence_position,
              paired_with_agent_type,
              fallback_agent_type,
            })
          );
        }
        continue;
      }

      if (include_variant_group && variant_group) {
        filters.push(
          ...builder({
            category: requested?.category ?? null,
            secondary_category: requested?.secondary_category ?? null,
            use_case: null,
            variant_group,
            tone,
            gender_variant,
            language,
            sequence_position,
            paired_with_agent_type,
            fallback_agent_type,
          })
        );
      }
    }
  }

  const seen = new Set();
  return filters.filter((filter_set) => {
    const key = stableTemplateBatchCacheKey(filter_set);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function buildStrictTouchOnePodioFilterSets() {
  const filters = [
    buildFilterPayload({
      use_case: "ownership_check",
      active: "Yes",
    }),
  ];
  const first_touch_external_id = getFirstKnownTemplateFieldExternalId([
    "is-first-touch",
    "first-touch",
  ]);

  if (first_touch_external_id) {
    const first_touch_filter = {
      ...filters[0],
    };
    const normalized_value = getTemplateCategoryValue(first_touch_external_id, "Yes");
    if (normalized_value) {
      first_touch_filter[first_touch_external_id] = normalized_value;
      filters.unshift(first_touch_filter);
    }
  }

  const seen = new Set();
  return filters.filter((filter_set) => {
    const key = stableTemplateBatchCacheKey(filter_set);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
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
  context = null,
  template_render_overrides = {},
  allow_language_fallback = true,
  allow_variant_group_fallback = false,
  remote_fetcher = fetchTemplatesCached,
  local_fetcher = fetchLocalTemplates,
  allowed_variant_groups = null,
  required_use_cases = null,
  required_variant_groups = null,
  require_explicit_variant_group = false,
  strict_touch_one_podio_only = false,
}) {
  const resolution_diagnostics = createTemplateResolutionDiagnostics();
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
  const use_cases = expandTemplateLookupUseCases(use_case);
  const languages = buildLanguagePriority(language, allow_language_fallback);
  const requested_language = languages[0] || "English";
  const requested_property_type = resolveTouchOneRequestedPropertyType({
    category,
    context,
  });
  const fallback_languages = languages.slice(1);
  const preferences = {
    preferred_tone: normalized_preferences.tone,
    preferred_sequence_position:
      normalized_preferences.sequence_position || sequence_position,
    preferred_variant_group:
      normalized_preferences.variant_group || variant_group,
    preferred_agent_type:
      normalized_preferences.paired_with_agent_type || paired_with_agent_type,
    preferred_use_cases: use_cases,
    preferred_language: requested_language,
    preferred_category: normalized_preferences.category || category,
    preferred_secondary_category:
      normalized_preferences.secondary_category || secondary_category,
  };
  const buckets = [];

  if (strict_touch_one_podio_only) {
    buckets.push({
      source: "podio",
      filter_sets: buildStrictTouchOnePodioFilterSets(),
    });
  } else if (use_cases.length) {
    buckets.push({
      source: "podio",
      filter_sets: buildPriorityFilterSets({
        source: "podio",
        category,
        secondary_category,
        use_cases,
        tone,
        gender_variant,
        languages: [requested_language],
        sequence_position,
        paired_with_agent_type,
        fallback_agent_type,
        include_variant_group: false,
      }),
    });

    if (fallback_languages.length) {
      buckets.push({
        source: "podio",
        filter_sets: buildPriorityFilterSets({
          source: "podio",
          category,
          secondary_category,
          use_cases,
          tone,
          gender_variant,
          languages: fallback_languages,
          sequence_position,
          paired_with_agent_type,
          fallback_agent_type,
          include_variant_group: false,
        }),
      });
    }
  }

  if (allow_variant_group_fallback && variant_group) {
    buckets.push({
      source: "podio",
      filter_sets: buildPriorityFilterSets({
        source: "podio",
        category,
        secondary_category,
        use_cases: [],
        variant_group,
        tone,
        gender_variant,
        languages: [requested_language],
        sequence_position,
        paired_with_agent_type,
        fallback_agent_type,
        include_variant_group: true,
      }),
    });

    if (fallback_languages.length) {
      buckets.push({
        source: "podio",
        filter_sets: buildPriorityFilterSets({
          source: "podio",
          category,
          secondary_category,
          use_cases: [],
          variant_group,
          tone,
          gender_variant,
          languages: fallback_languages,
          sequence_position,
          paired_with_agent_type,
          fallback_agent_type,
          include_variant_group: true,
        }),
      });
    }
  }

  if (!strict_touch_one_podio_only) {
    buckets.push({
      source: "local_registry",
      filter_sets: buildPriorityFilterSets({
        source: "local_registry",
        category,
        secondary_category,
        use_cases,
        tone,
        gender_variant,
        languages,
        sequence_position,
        paired_with_agent_type,
        fallback_agent_type,
        include_variant_group: false,
      }),
    });

    if (allow_variant_group_fallback && variant_group) {
      buckets.push({
        source: "local_registry",
        filter_sets: buildPriorityFilterSets({
          source: "local_registry",
          category,
          secondary_category,
          use_cases: [],
          variant_group,
          tone,
          gender_variant,
          languages,
          sequence_position,
          paired_with_agent_type,
          fallback_agent_type,
          include_variant_group: true,
        }),
      });
    }
  }

  for (const bucket of buckets) {
    const scored = await collectBucketCandidates({
      source: bucket.source,
      filter_sets: bucket.filter_sets,
      remote_fetcher,
      local_fetcher,
      recently_used_template_ids,
      context,
      template_render_overrides,
      preferences,
      allowed_variant_groups,
      required_use_cases,
      required_variant_groups,
      require_explicit_variant_group,
      strict_touch_one_podio_only,
      strict_touch_one_language: requested_language,
      strict_touch_one_property_type: requested_property_type,
      strict_touch_one_owner_id:
        context?.ids?.master_owner_id ?? context?.ids?.owner_id ?? null,
      strict_touch_one_touch_number: strict_touch_one_podio_only ? 1 : null,
      diagnostics: resolution_diagnostics,
    });

    if (scored.length) {
      resolution_diagnostics.selected_bucket_source = bucket.source;
      const template_resolution_source =
        bucket.source === "podio"
          ? "podio_template"
          : "local_template_fallback";
      const template_fallback_reason =
        bucket.source === "local_registry"
          ? resolution_diagnostics.podio_fetch_failures > 0
            ? "podio_template_fetch_failed"
            : "no_podio_template_match"
          : null;
      const selection_diagnostics = summarizeTemplateResolutionDiagnostics(
        resolution_diagnostics
      );

      return scored.map((template) => ({
        ...template,
        template_resolution_source,
        template_fallback_reason,
        template_selection_diagnostics: selection_diagnostics,
      }));
    }
  }

  if (strict_touch_one_podio_only) {
    const diagnostics = {
      owner_id: context?.ids?.master_owner_id ?? context?.ids?.owner_id ?? null,
      touch_number: 1,
      requested_language,
      requested_property_type,
      use_case: use_case || null,
      variant_group: variant_group || null,
      category: category || null,
      secondary_category: secondary_category || null,
      selection_diagnostics: summarizeTemplateResolutionDiagnostics(
        resolution_diagnostics
      ),
    };
    warn("template.touch_one_template_missing", {
      reason: "NO_STAGE_1_TEMPLATE_FOUND",
      ...diagnostics,
    });
    const err = new Error("NO_STAGE_1_TEMPLATE_FOUND");
    err.code = "NO_STAGE_1_TEMPLATE_FOUND";
    err.diagnostics = diagnostics;
    throw err;
  }

  return [];
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
  context = null,
  template_render_overrides = {},
  allow_language_fallback = true,
  allow_variant_group_fallback = false,
  remote_fetcher = fetchTemplatesCached,
  local_fetcher = fetchLocalTemplates,
  allowed_variant_groups = null,
  required_use_cases = null,
  required_variant_groups = null,
  require_explicit_variant_group = false,
  strict_touch_one_podio_only = false,
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
    context,
    template_render_overrides,
    allow_language_fallback,
    allow_variant_group_fallback,
    remote_fetcher,
    local_fetcher,
    allowed_variant_groups,
    required_use_cases,
    required_variant_groups,
    require_explicit_variant_group,
    strict_touch_one_podio_only,
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
