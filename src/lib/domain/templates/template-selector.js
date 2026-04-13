import { normalizeSellerFlowUseCase } from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { getCategoryValue, safeCategoryEquals } from "@/lib/providers/podio.js";

export const TEMPLATE_TOUCH_TYPES = Object.freeze({
  FIRST_TOUCH: "First Touch",
  FOLLOW_UP: "Follow-Up",
  ANY: "Any",
});

export const TEMPLATE_METADATA_ONLY_FIELDS = Object.freeze([
  "canonical_routing_slug",
  "variant_group",
  "stage_code",
  "stage_label",
  "legacy_use_case",
  "legacy_variant_group",
  "category",
  "secondary_category",
  "paired_with_agent_type",
  "tone",
  "gender_variant",
]);

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

const DEAL_STRATEGY_FAMILY_MAP = Object.freeze({
  creative: new Set(["creative", "lease_option", "subject_to", "novation"]),
  cash: new Set(["cash"]),
  lease_option: new Set(["lease_option", "creative"]),
  subject_to: new Set(["subject_to", "creative"]),
  novation: new Set(["novation", "creative"]),
  multifamily: new Set(["multifamily"]),
});

const RESIDENTIAL_SCOPE_MARKERS = Object.freeze([
  "any residential",
  "residential",
  "single family",
  "single-family",
  "sfr",
  "condo",
  "condominium",
  "townhouse",
  "mobile home",
]);

const MULTIFAMILY_SCOPE_MARKERS = Object.freeze([
  "duplex",
  "triplex",
  "fourplex",
  "quadplex",
  "5 units",
  "5+ units",
  "five plus units",
  "multifamily",
  "multi family",
  "apartment",
  "apartments",
  "landlord / multifamily",
]);

function clean(value) {
  return String(value ?? "").trim();
}

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

export function normalizeSelectorText(value) {
  return clean(value)
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function containsMarker(value = null, markers = []) {
  const normalized = normalizeSelectorText(value);
  return markers.some(
    (marker) =>
      normalized === normalizeSelectorText(marker) ||
      normalized.includes(normalizeSelectorText(marker))
  );
}

function isTruthyYes(value = null) {
  return safeCategoryEquals(clean(value), "Yes") || normalizeSelectorText(value) === "true";
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

  const wanted_labels = new Set(raw_labels.map((label) => normalizeSelectorText(label)).filter(Boolean));
  if (!wanted_labels.size) return fallback;

  for (const field of Array.isArray(raw_item?.fields) ? raw_item.fields : []) {
    const candidates = [field?.label, field?.config?.label, field?.field?.label];
    const matches = candidates.some((value) => wanted_labels.has(normalizeSelectorText(value)));
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

export function normalizeTemplateSelectorUseCase(template = null) {
  return (
    clean(
      template?.selector_use_case ||
        template?.use_case_label ||
        template?.use_case ||
        template?.canonical_routing_slug
    ) || null
  );
}

export function canonicalizeTemplateUseCase(use_case = null, variant_group = null) {
  return normalizeSellerFlowUseCase(clean(use_case) || null, clean(variant_group) || null) || clean(use_case) || null;
}

export function expandSelectorUseCases(use_case = null, variant_group = null) {
  const exact_use_case = clean(use_case) || null;
  const canonical_use_case = canonicalizeTemplateUseCase(exact_use_case, variant_group);

  return uniq([
    exact_use_case,
    canonical_use_case,
    ...(TEMPLATE_LOOKUP_USE_CASE_ALIASES[canonical_use_case] || []),
  ]);
}

export function normalizeRequestedTouchType({
  touch_type = null,
  template_selector = null,
  touch_number = null,
  message_type = null,
  sequence_position = null,
  secondary_category = null,
  use_case = null,
  strict_touch_one_podio_only = false,
} = {}) {
  const explicit =
    clean(template_selector?.touch_type) ||
    clean(touch_type) ||
    null;

  switch (normalizeSelectorText(explicit)) {
    case "first":
    case "first touch":
      return TEMPLATE_TOUCH_TYPES.FIRST_TOUCH;
    case "follow up":
    case "follow-up":
    case "followup":
      return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;
    case "any":
      return TEMPLATE_TOUCH_TYPES.ANY;
    default:
      break;
  }

  if (strict_touch_one_podio_only || Number(touch_number || 0) === 1) {
    return TEMPLATE_TOUCH_TYPES.FIRST_TOUCH;
  }

  if (Number(touch_number || 0) > 1) {
    return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;
  }

  if (safeCategoryEquals(sequence_position, "1st Touch")) {
    return TEMPLATE_TOUCH_TYPES.FIRST_TOUCH;
  }

  if (
    containsMarker(sequence_position, ["2nd touch", "3rd touch", "4th touch", "final"]) ||
    containsMarker(secondary_category, ["follow-up", "re-engagement"]) ||
    containsMarker(message_type, ["follow-up", "re-engagement"]) ||
    normalizeSelectorText(use_case).includes("follow up") ||
    normalizeSelectorText(use_case).includes("followup")
  ) {
    return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;
  }

  return TEMPLATE_TOUCH_TYPES.ANY;
}

export function normalizeTemplateTouchType(template = null) {
  const explicit = clean(
    readTemplateValue(
      template,
      {
        direct_values: [template?.touch_type],
        raw_external_ids: ["touch-type"],
        raw_labels: ["Touch Type"],
      },
      null
    )
  );

  switch (normalizeSelectorText(explicit)) {
    case "first":
    case "first touch":
      return TEMPLATE_TOUCH_TYPES.FIRST_TOUCH;
    case "follow up":
    case "follow-up":
    case "followup":
      return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;
    case "any":
      return TEMPLATE_TOUCH_TYPES.ANY;
    default:
      break;
  }

  const is_first_touch = readTemplateValue(
    template,
    {
      direct_values: [template?.is_first_touch, template?.is_first_contact, template?.first_touch],
      raw_external_ids: ["is-first-touch", "first-touch"],
      raw_labels: ["Is First Touch", "First Touch"],
    },
    null
  );
  if (isTruthyYes(is_first_touch)) return TEMPLATE_TOUCH_TYPES.FIRST_TOUCH;
  if (clean(is_first_touch) && safeCategoryEquals(is_first_touch, "No")) {
    return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;
  }

  const is_follow_up = readTemplateValue(
    template,
    {
      direct_values: [template?.is_follow_up],
      raw_external_ids: ["is-follow-up", "follow-up"],
      raw_labels: ["Is Follow-Up", "Is Follow Up"],
    },
    null
  );
  if (isTruthyYes(is_follow_up)) return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;

  const follow_up_attempt = readTemplateValue(
    template,
    {
      direct_values: [template?.follow_up_attempt, template?.followup_attempt],
      raw_external_ids: ["follow-up-attempt", "follow-up-touch", "followup-attempt"],
      raw_labels: ["Follow Up Attempt", "Follow-Up Attempt"],
    },
    null
  );
  if (clean(follow_up_attempt)) return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;

  if (safeCategoryEquals(template?.sequence_position, "1st Touch")) {
    return TEMPLATE_TOUCH_TYPES.FIRST_TOUCH;
  }

  if (
    containsMarker(template?.sequence_position, ["2nd touch", "3rd touch", "4th touch", "final"]) ||
    containsMarker(template?.category_secondary, ["follow-up", "offer follow-up", "re-engagement"]) ||
    normalizeSelectorText(normalizeTemplateSelectorUseCase(template)).includes("follow up") ||
    normalizeSelectorText(normalizeTemplateSelectorUseCase(template)).includes("followup")
  ) {
    return TEMPLATE_TOUCH_TYPES.FOLLOW_UP;
  }

  return TEMPLATE_TOUCH_TYPES.ANY;
}

export function readExplicitFirstTouchValue(template = null) {
  return (
    clean(
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
      )
    ) || null
  );
}

export function isExplicitFirstTouch(template = null) {
  return isTruthyYes(readExplicitFirstTouchValue(template));
}

export function normalizePropertyTypeScope(value = null) {
  return clean(value) || null;
}

export function normalizeTemplatePropertyTypeScope(template = null) {
  return (
    normalizePropertyTypeScope(
      readTemplateValue(
        template,
        {
          direct_values: [template?.property_type_scope, template?.category_primary],
          raw_external_ids: ["property-type-scope", "property-type"],
          raw_labels: ["Property Type Scope", "Property Type"],
        },
        null
      )
    ) || null
  );
}

function isResidentialScope(value = null) {
  return containsMarker(value, RESIDENTIAL_SCOPE_MARKERS);
}

function isResidentialAnyScope(value = null) {
  return safeCategoryEquals(value, "Any Residential");
}

function isMultifamilyScope(value = null) {
  return containsMarker(value, MULTIFAMILY_SCOPE_MARKERS);
}

function normalizeRequestedPropertyTypeScope({
  property_type_scope = null,
  template_selector = null,
  category = null,
  route = null,
  context = null,
} = {}) {
  const property_item = context?.items?.property_item || null;

  return (
    normalizePropertyTypeScope(
      template_selector?.property_type_scope ||
        property_type_scope ||
        getCategoryValue(property_item, "property-type-scope", null) ||
        getCategoryValue(property_item, "property-type", null) ||
        getCategoryValue(property_item, "property-class", null) ||
        context?.summary?.property_type ||
        context?.summary?.property_class ||
        route?.template_selector?.property_type_scope ||
        route?.primary_category ||
        category
    ) || null
  );
}

export function isPropertyTypeScopeCompatible({
  requested_property_type_scope = null,
  template_property_type_scope = null,
} = {}) {
  return describePropertyTypeScopeCompatibility({
    requested_property_type_scope,
    template_property_type_scope,
  }).compatible;
}

export function scorePropertyTypeScopeMatch({
  requested_property_type_scope = null,
  template_property_type_scope = null,
} = {}) {
  const requested = normalizeRequestedPropertyTypeScope({
    property_type_scope: requested_property_type_scope,
  });
  const template_scope = normalizePropertyTypeScope(template_property_type_scope);

  if (!template_scope) return 30;
  if (!requested) return 40;
  if (safeCategoryEquals(requested, template_scope)) return 100;

  if (isResidentialScope(requested)) {
    if (isResidentialAnyScope(template_scope)) return 150;
    if (isResidentialScope(template_scope)) return 85;
    return 0;
  }

  if (isMultifamilyScope(requested)) {
    if (safeCategoryEquals(requested, template_scope)) return 100;
    if (isMultifamilyScope(template_scope)) return 85;
    return 0;
  }

  return 45;
}

export function describePropertyTypeScopeCompatibility({
  requested_property_type_scope = null,
  template_property_type_scope = null,
} = {}) {
  const requested = normalizeRequestedPropertyTypeScope({
    property_type_scope: requested_property_type_scope,
  });
  const template_scope = normalizePropertyTypeScope(template_property_type_scope);

  if (!requested && !template_scope) {
    return {
      compatible: true,
      reason: "requested_and_template_scope_missing",
      requested_scope: null,
      template_scope: null,
    };
  }

  if (!requested) {
    return {
      compatible: true,
      reason: "requested_scope_missing",
      requested_scope: null,
      template_scope,
    };
  }

  if (!template_scope) {
    return {
      compatible: true,
      reason: "template_scope_missing",
      requested_scope: requested,
      template_scope: null,
    };
  }

  if (safeCategoryEquals(requested, template_scope)) {
    return {
      compatible: true,
      reason: "exact_scope_match",
      requested_scope: requested,
      template_scope,
    };
  }

  if (isResidentialScope(requested) || isResidentialAnyScope(requested)) {
    if (isResidentialAnyScope(template_scope)) {
      return {
        compatible: true,
        reason: "any_residential_match",
        requested_scope: requested,
        template_scope,
      };
    }

    if (isResidentialScope(template_scope)) {
      return {
        compatible: true,
        reason: "residential_scope_family_match",
        requested_scope: requested,
        template_scope,
      };
    }

    return {
      compatible: false,
      reason: "residential_scope_rejected_multifamily_only",
      requested_scope: requested,
      template_scope,
    };
  }

  if (isMultifamilyScope(requested)) {
    if (isMultifamilyScope(template_scope)) {
      return {
        compatible: true,
        reason: "multifamily_scope_family_match",
        requested_scope: requested,
        template_scope,
      };
    }

    return {
      compatible: false,
      reason: "multifamily_scope_incompatible",
      requested_scope: requested,
      template_scope,
    };
  }

  return {
    compatible: true,
    reason: "scope_compatibility_default_pass",
    requested_scope: requested,
    template_scope,
  };
}

export function normalizeDealStrategyValue(value = null) {
  const raw = normalizeSelectorText(value);
  if (!raw || raw === "unknown" || raw === "any") return null;
  if (raw.includes("novation")) return "novation";
  if (raw.includes("lease") && raw.includes("option")) return "lease_option";
  if (raw.includes("subject")) return "subject_to";
  if (raw.includes("multi") || raw.includes("mf") || raw.includes("apartment")) {
    return "multifamily";
  }
  if (
    raw.includes("creative") ||
    raw.includes("seller finance") ||
    raw.includes("owner finance") ||
    raw.includes("hybrid")
  ) {
    return "creative";
  }
  if (raw.includes("cash")) return "cash";
  return clean(value) || null;
}

function deriveDealStrategyFromUseCase(use_case = null) {
  const normalized = normalizeSelectorText(use_case);
  if (!normalized) return null;
  if (normalized.startsWith("mf ")) return "multifamily";
  if (normalized.startsWith("mf_")) return "multifamily";
  if (normalized.includes("novation")) return "novation";
  if (normalized.includes("lease option")) return "lease_option";
  if (normalized.includes("lease_option")) return "lease_option";
  if (normalized.includes("subject to") || normalized.includes("subject_to")) {
    return "subject_to";
  }
  if (normalized.includes("creative")) return "creative";
  if (normalized.includes("offer reveal cash")) return "cash";
  if (normalized.includes("offer_reveal_cash")) return "cash";
  return null;
}

export function normalizeTemplateDealStrategy(template = null) {
  return (
    normalizeDealStrategyValue(
      readTemplateValue(
        template,
        {
          direct_values: [template?.deal_strategy],
          raw_external_ids: [
            "deal-strategy",
            "deal-strategy-branch",
            "strategy",
            "creative-strategy",
            "mf-exit-strategy",
          ],
          raw_labels: [
            "Deal Strategy",
            "Deal Strategy Branch",
            "Strategy",
            "Creative Strategy",
            "MF Exit Strategy",
          ],
        },
        null
      )
    ) ||
    deriveDealStrategyFromUseCase(normalizeTemplateSelectorUseCase(template))
  );
}

export function isDealStrategyCompatible({
  requested_deal_strategy = null,
  template_deal_strategy = null,
} = {}) {
  const requested = normalizeDealStrategyValue(requested_deal_strategy);
  const template_strategy = normalizeDealStrategyValue(template_deal_strategy);

  if (!requested || !template_strategy) return true;
  if (requested === template_strategy) return true;

  return Boolean(DEAL_STRATEGY_FAMILY_MAP[requested]?.has(template_strategy));
}

export function scoreDealStrategyMatch({
  requested_deal_strategy = null,
  template_deal_strategy = null,
} = {}) {
  const requested = normalizeDealStrategyValue(requested_deal_strategy);
  const template_strategy = normalizeDealStrategyValue(template_deal_strategy);

  if (!requested) return 0;
  if (!template_strategy) return 25;
  if (requested === template_strategy) return 80;
  if (DEAL_STRATEGY_FAMILY_MAP[requested]?.has(template_strategy)) return 60;
  return 0;
}

export function buildTemplateSelectorInput({
  template_selector = null,
  use_case = null,
  language = null,
  property_type_scope = null,
  deal_strategy = null,
  touch_type = null,
  touch_number = null,
  message_type = null,
  category = null,
  secondary_category = null,
  sequence_position = null,
  route = null,
  context = null,
  strict_touch_one_podio_only = false,
} = {}) {
  const resolved_use_case =
    clean(template_selector?.use_case) ||
    clean(use_case) ||
    null;
  const resolved_language =
    clean(template_selector?.language) ||
    clean(language) ||
    clean(route?.language) ||
    clean(context?.summary?.language_preference) ||
    "English";
  const resolved_property_type_scope = normalizeRequestedPropertyTypeScope({
    property_type_scope:
      template_selector?.property_type_scope ||
      property_type_scope ||
      null,
    category,
    route,
    context,
  });
  const resolved_touch_type = normalizeRequestedTouchType({
    touch_type,
    template_selector,
    touch_number,
    message_type,
    sequence_position,
    secondary_category,
    use_case: resolved_use_case,
    strict_touch_one_podio_only,
  });
  const resolved_deal_strategy =
    normalizeDealStrategyValue(
      template_selector?.deal_strategy ||
        deal_strategy ||
        route?.template_selector?.deal_strategy ||
        context?.summary?.deal_strategy_branch ||
        null
    ) ||
    deriveDealStrategyFromUseCase(resolved_use_case);

  return {
    active: "Yes",
    use_case: resolved_use_case,
    language: resolved_language,
    property_type_scope: resolved_property_type_scope,
    deal_strategy: resolved_deal_strategy,
    touch_type: resolved_touch_type,
  };
}

export function summarizeTemplateSelectorMetadata(template = null) {
  return {
    selector_use_case: normalizeTemplateSelectorUseCase(template),
    canonical_use_case: canonicalizeTemplateUseCase(
      normalizeTemplateSelectorUseCase(template),
      clean(template?.variant_group) || clean(template?.stage_label) || null
    ),
    variant_group: clean(template?.variant_group) || null,
    stage_code: clean(template?.stage_code) || null,
    stage_label: clean(template?.stage_label) || null,
    canonical_routing_slug: clean(template?.canonical_routing_slug) || null,
    legacy_use_case: clean(template?.legacy_use_case) || null,
    legacy_variant_group: clean(template?.legacy_variant_group) || null,
    category: clean(template?.category_primary) || null,
    secondary_category: clean(template?.category_secondary) || null,
    paired_with_agent_type: clean(template?.paired_with_agent_type) || null,
    tone: clean(template?.tone) || null,
    gender_variant: clean(template?.gender_variant) || null,
    sequence_position: clean(template?.sequence_position) || null,
  };
}
