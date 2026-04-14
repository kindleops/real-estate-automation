import APP_IDS from "@/lib/config/app-ids.js";
import { evaluateTemplatePlaceholders } from "@/lib/domain/templates/render-template.js";
import { LOCAL_TEMPLATE_CANDIDATES } from "@/lib/domain/templates/local-template-registry.js";
import {
  buildTemplateSelectorInput,
  canonicalizeTemplateUseCase,
  describePropertyTypeScopeCompatibility,
  expandSelectorUseCases,
  isDealStrategyCompatible,
  isExplicitFirstTouch,
  normalizeSelectorText,
  readExplicitFirstTouchValue,
  normalizeTemplateDealStrategy,
  normalizeTemplatePropertyTypeScope,
  normalizeTemplateSelectorUseCase,
  normalizeTemplateTouchType,
  scoreDealStrategyMatch,
  scorePropertyTypeScopeMatch,
  summarizeTemplateSelectorMetadata,
  TEMPLATE_TOUCH_TYPES,
} from "@/lib/domain/templates/template-selector.js";
import { info, warn } from "@/lib/logging/logger.js";
import { safeCategoryEquals } from "@/lib/providers/podio.js";
import { fetchTemplates } from "@/lib/podio/apps/templates.js";
import { getAttachedFieldSchema } from "@/lib/podio/schema.js";

const HARD_SPAM_RISK_CUTOFF = 35;
const TEMPLATE_APP_ID = APP_IDS.templates;
const TEMPLATE_BATCH_CACHE = new Map();
const TEMPLATE_BATCH_CACHE_TTL_MS = 2 * 60_000;
const TOUCH_ONE_ACTIVE_SWEEP_PAGE_LIMIT = 200;
const TOUCH_ONE_ACTIVE_SWEEP_MAX_PAGES = 5;

function clean(value) {
  return String(value ?? "").trim();
}

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function hashString(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

function getTemplateCategoryValue(external_id, value = null) {
  const raw = clean(value);
  if (!raw) return null;

  const field = getAttachedFieldSchema(TEMPLATE_APP_ID, external_id);
  if (!field?.options?.length) return raw;

  const normalized = normalizeSelectorText(raw);
  return (
    field.options.find((option) => normalizeSelectorText(option.text) === normalized)?.text || raw
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
    fetch_limit = null,
    fetch_offset = 0,
  } = {}
) {
  const cache_key = stableTemplateBatchCacheKey({
    ...filter_set,
    __fetch_limit: fetch_limit ?? null,
    __fetch_offset: fetch_offset ?? 0,
  });
  const cached = cache.get(cache_key);

  if (
    cached &&
    Number.isFinite(Number(cached.expires_at)) &&
    Number(cached.expires_at) > Date.now()
  ) {
    return cached.value;
  }

  const batch = await fetcher(
    filter_set,
    Number.isFinite(Number(fetch_limit)) ? Number(fetch_limit) : undefined,
    Number.isFinite(Number(fetch_offset)) ? Number(fetch_offset) : 0
  );
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

function dedupeTemplates(templates = []) {
  const seen = new Set();

  return templates.filter((template) => {
    const key = `${template?.item_id || "no-id"}:${clean(template?.text)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function buildRemoteFilterRequests({
  selector_input = null,
  use_case_candidates = [],
  strict_touch_one_podio_only = false,
} = {}) {
  const active_value = getTemplateCategoryValue("active", "Yes") || "Yes";
  const requested_use_case = clean(selector_input?.use_case) || null;
  const requests = [];

  const addRequest = (label, filter_set = {}, options = {}) => {
    requests.push({
      label,
      filter_set,
      paginate: Boolean(options.paginate),
    });
  };

  if (
    strict_touch_one_podio_only &&
    safeCategoryEquals(requested_use_case, "ownership_check") &&
    selector_input?.touch_type === TEMPLATE_TOUCH_TYPES.FIRST_TOUCH
  ) {
    addRequest("touch_one_use_case_template_first_touch", {
      "use-case-2": requested_use_case,
      "is-first-touch": "Yes",
      active: active_value,
    });
    addRequest("touch_one_use_case_legacy_first_touch", {
      "use-case": getTemplateCategoryValue("use-case", requested_use_case) || requested_use_case,
      "is-first-touch": "Yes",
      active: active_value,
    });
    addRequest("touch_one_use_case_template", {
      "use-case-2": requested_use_case,
      active: active_value,
    });
    addRequest("touch_one_use_case_legacy", {
      "use-case": getTemplateCategoryValue("use-case", requested_use_case) || requested_use_case,
      active: active_value,
    });
    addRequest(
      "touch_one_active_only",
      {
        active: active_value,
      },
      { paginate: true }
    );
  } else {
    for (const use_case of use_case_candidates) {
      addRequest(`use_case_template:${use_case}`, {
        "use-case-2": use_case,
        active: active_value,
      });
      addRequest(`use_case_legacy:${use_case}`, {
        "use-case": getTemplateCategoryValue("use-case", use_case) || use_case,
        active: active_value,
      });
    }

    addRequest("active_only", {
      active: active_value,
    });
  }

  const seen = new Set();
  return requests.filter((request) => {
    const key = stableTemplateBatchCacheKey(request.filter_set);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function matchesLocalTemplateFilter(template, filter_set = {}) {
  const requested_use_case = clean(filter_set?.["use-case"] || filter_set?.use_case) || null;
  if (!requested_use_case) return safeCategoryEquals(template?.active, filter_set?.active || "Yes");

  const template_use_case = normalizeTemplateSelectorUseCase(template);
  const template_canonical_use_case = canonicalizeTemplateUseCase(
    template_use_case,
    template?.variant_group || template?.stage_label || null
  );

  return (
    safeCategoryEquals(template?.active, filter_set?.active || "Yes") &&
    (
      safeCategoryEquals(template_use_case, requested_use_case) ||
      safeCategoryEquals(template_canonical_use_case, requested_use_case)
    )
  );
}

function fetchLocalTemplates(filter_set = {}) {
  return LOCAL_TEMPLATE_CANDIDATES.filter((template) =>
    matchesLocalTemplateFilter(template, filter_set)
  );
}

function createTemplateResolutionDiagnostics() {
  return {
    podio_fetch_failures: 0,
    podio_filter_validation_failures: 0,
    podio_batches_with_results: 0,
    podio_candidates_considered: 0,
    podio_raw_candidates_loaded: 0,
    podio_prefetch_candidates_excluded: 0,
    podio_prefetch_candidates_survived: 0,
    podio_filter_requests: [],
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
    podio_raw_candidates_loaded: diagnostics.podio_raw_candidates_loaded || 0,
    podio_prefetch_candidates_excluded:
      diagnostics.podio_prefetch_candidates_excluded || 0,
    podio_prefetch_candidates_survived:
      diagnostics.podio_prefetch_candidates_survived || 0,
    podio_filter_requests: Array.isArray(diagnostics.podio_filter_requests)
      ? diagnostics.podio_filter_requests
      : [],
    local_candidates_considered: diagnostics.local_candidates_considered || 0,
    selected_bucket_source: diagnostics.selected_bucket_source || null,
  };
}

function isTemplateFilterValidationError(error) {
  const message = clean(error?.message).toLowerCase();

  return (
    Number(error?.status || error?.response?.status || 0) === 400 &&
    (message.includes("invalid category value") || message.includes("invalid value"))
  );
}

function rotateVariant(templates, rotation_key = null) {
  if (!templates.length) return null;
  if (!rotation_key) return templates[0];

  const index = Math.abs(hashString(String(rotation_key))) % templates.length;
  return templates[index];
}

function scoreLanguage(template_language = null, requested_language = "English") {
  if (clean(template_language) && safeCategoryEquals(template_language, requested_language)) {
    return 200;
  }

  if (clean(template_language) && safeCategoryEquals(template_language, "English")) {
    return 140;
  }

  return clean(template_language) ? 90 : 60;
}

function scoreTouchType(template_touch_type = TEMPLATE_TOUCH_TYPES.ANY, requested_touch_type = TEMPLATE_TOUCH_TYPES.ANY) {
  if (requested_touch_type === TEMPLATE_TOUCH_TYPES.ANY) {
    return template_touch_type === TEMPLATE_TOUCH_TYPES.ANY ? 40 : 35;
  }

  if (template_touch_type === requested_touch_type) return 260;
  if (template_touch_type === TEMPLATE_TOUCH_TYPES.ANY) return 180;
  return 0;
}

function scoreSequencePreference(template = null, requested_touch_type = TEMPLATE_TOUCH_TYPES.ANY) {
  if (
    requested_touch_type === TEMPLATE_TOUCH_TYPES.FIRST_TOUCH &&
    safeCategoryEquals(template?.sequence_position, "1st Touch")
  ) {
    return 45;
  }

  if (
    requested_touch_type === TEMPLATE_TOUCH_TYPES.FOLLOW_UP &&
    !safeCategoryEquals(template?.sequence_position, "1st Touch") &&
    clean(template?.sequence_position)
  ) {
    return 8;
  }

  return 0;
}

function scorePerformance(template = null) {
  const deliverability = Number.isFinite(Number(template?.deliverability_score))
    ? Number(template.deliverability_score) * 0.01
    : 0;
  const reply_rate = Number.isFinite(Number(template?.historical_reply_rate))
    ? Number(template.historical_reply_rate) * 0.01
    : 0;
  const conversations = Number.isFinite(Number(template?.total_conversations))
    ? Number(template.total_conversations) * 0.001
    : 0;
  const replies = Number.isFinite(Number(template?.total_replies))
    ? Number(template.total_replies) * 0.001
    : 0;
  const spam_penalty = Number.isFinite(Number(template?.spam_risk))
    ? Number(template.spam_risk) * 0.01
    : 0;
  const local_penalty = clean(template?.source) === "local_registry" ? 2 : 0;

  return deliverability + reply_rate + conversations + replies - spam_penalty - local_penalty;
}

function buildUseCaseMatch(template = null, requested_use_case = null) {
  const requested_exact = clean(requested_use_case) || null;
  if (!requested_exact) {
    return {
      matched: false,
      score: 0,
      rejection_reason: "requested_use_case_missing",
    };
  }

  const template_exact = normalizeTemplateSelectorUseCase(template);
  const template_canonical = canonicalizeTemplateUseCase(
    template_exact,
    template?.variant_group || template?.stage_label || null
  );
  const requested_canonical = canonicalizeTemplateUseCase(requested_exact, null);
  const requested_candidates = new Set(
    expandSelectorUseCases(requested_exact).map((value) => normalizeSelectorText(value)).filter(Boolean)
  );

  if (safeCategoryEquals(template_exact, requested_exact)) {
    return { matched: true, score: 400, match_type: "exact" };
  }

  if (safeCategoryEquals(template_canonical, requested_exact)) {
    return { matched: true, score: 360, match_type: "canonical_exact" };
  }

  if (requested_canonical && safeCategoryEquals(template_canonical, requested_canonical)) {
    return { matched: true, score: 320, match_type: "canonical" };
  }

  if (
    requested_candidates.has(normalizeSelectorText(template_exact)) ||
    requested_candidates.has(normalizeSelectorText(template_canonical))
  ) {
    return { matched: true, score: 280, match_type: "alias" };
  }

  return {
    matched: false,
    score: 0,
    rejection_reason: "use_case_mismatch",
  };
}

function evaluateTouchOnePrefetchCandidate(template = null, selector_input = null) {
  const template_use_case = normalizeTemplateSelectorUseCase(template);
  const canonical_use_case = canonicalizeTemplateUseCase(
    template_use_case,
    template?.variant_group || template?.stage_label || null
  );
  const explicit_is_first_touch = readExplicitFirstTouchValue(template);
  const property_type_scope = normalizeTemplatePropertyTypeScope(template);
  const prefetch_rejection_reasons = [];

  if (!safeCategoryEquals(template?.active, "Yes")) {
    prefetch_rejection_reasons.push("inactive");
  }

  if (
    !safeCategoryEquals(template_use_case, selector_input?.use_case || null) &&
    !safeCategoryEquals(canonical_use_case, selector_input?.use_case || null)
  ) {
    prefetch_rejection_reasons.push("use_case_mismatch");
  }

  if (!isExplicitFirstTouch(template)) {
    prefetch_rejection_reasons.push("is_first_touch_mismatch");
  }

  return {
    ...template,
    selector_use_case: template_use_case,
    canonical_use_case,
    explicit_is_first_touch,
    property_type_scope,
    prefetch_rejection_reasons,
  };
}

function isTouchTypeCompatible(template_touch_type = TEMPLATE_TOUCH_TYPES.ANY, requested_touch_type = TEMPLATE_TOUCH_TYPES.ANY) {
  if (requested_touch_type === TEMPLATE_TOUCH_TYPES.ANY) return true;
  if (template_touch_type === requested_touch_type) return true;
  return template_touch_type === TEMPLATE_TOUCH_TYPES.ANY;
}

function isRenderableTemplate(
  template = null,
  {
    context = null,
    template_render_overrides = {},
    strict_touch_one_podio_only = false,
  } = {}
) {
  if (strict_touch_one_podio_only) return true;

  const renderability = evaluateTemplatePlaceholders({
    template_text: template?.text || "",
    use_case:
      template?.canonical_use_case ||
      canonicalizeTemplateUseCase(
        normalizeTemplateSelectorUseCase(template),
        template?.variant_group || template?.stage_label || null
      ),
    variant_group: template?.variant_group || null,
    context,
    overrides: template_render_overrides,
  });

  return Boolean(renderability?.ok);
}

function evaluateTemplateCandidate(
  template = null,
  {
    selector_input = null,
    recently_used_template_ids = [],
    context = null,
    template_render_overrides = {},
    strict_touch_one_podio_only = false,
  } = {}
) {
  const template_use_case = normalizeTemplateSelectorUseCase(template);
  const canonical_use_case = canonicalizeTemplateUseCase(
    template_use_case,
    template?.variant_group || template?.stage_label || null
  );
  const touch_type = normalizeTemplateTouchType(template);
  const property_type_scope = normalizeTemplatePropertyTypeScope(template);
  const explicit_is_first_touch = readExplicitFirstTouchValue(template);
  const deal_strategy = normalizeTemplateDealStrategy({
    ...template,
    use_case: template_use_case || template?.use_case || null,
    canonical_use_case,
  });
  const metadata = summarizeTemplateSelectorMetadata({
    ...template,
    canonical_use_case,
    property_type_scope,
    deal_strategy,
  });

  const rejection_reasons = [];
  const operational_rejection_reasons = [];
  const recently_used = new Set(recently_used_template_ids.filter(Boolean));
  const property_type_scope_compatibility = describePropertyTypeScopeCompatibility({
    requested_property_type_scope: selector_input?.property_type_scope || null,
    template_property_type_scope: property_type_scope,
  });

  if (!safeCategoryEquals(template?.active, "Yes")) {
    rejection_reasons.push("inactive");
  }

  const use_case_match = buildUseCaseMatch(template, selector_input?.use_case || null);
  if (!use_case_match.matched) {
    rejection_reasons.push(use_case_match.rejection_reason);
  }

  if (!isTouchTypeCompatible(touch_type, selector_input?.touch_type || TEMPLATE_TOUCH_TYPES.ANY)) {
    rejection_reasons.push("touch_type_mismatch");
  }

  if (!property_type_scope_compatibility.compatible) {
    rejection_reasons.push("property_type_scope_incompatible");
  }

  if (
    !isDealStrategyCompatible({
      requested_deal_strategy: selector_input?.deal_strategy || null,
      template_deal_strategy: deal_strategy,
    })
  ) {
    rejection_reasons.push("deal_strategy_mismatch");
  }

  if (!clean(template?.text)) {
    operational_rejection_reasons.push("empty_text");
  }

  if (Number.isFinite(Number(template?.spam_risk)) && Number(template.spam_risk) > HARD_SPAM_RISK_CUTOFF) {
    operational_rejection_reasons.push("spam_risk_exceeded");
  }

  if (recently_used.has(template?.item_id)) {
    operational_rejection_reasons.push("recently_used");
  }

  if (
    !isRenderableTemplate(template, {
      context,
      template_render_overrides,
      strict_touch_one_podio_only,
    })
  ) {
    operational_rejection_reasons.push("render_validation_failed");
  }

  const score =
    use_case_match.score +
    scoreTouchType(touch_type, selector_input?.touch_type || TEMPLATE_TOUCH_TYPES.ANY) +
    scoreLanguage(template?.language, selector_input?.language || "English") +
    scorePropertyTypeScopeMatch({
      requested_property_type_scope: selector_input?.property_type_scope || null,
      template_property_type_scope: property_type_scope,
    }) +
    scoreDealStrategyMatch({
      requested_deal_strategy: selector_input?.deal_strategy || null,
      template_deal_strategy: deal_strategy,
    }) +
    scoreSequencePreference(template, selector_input?.touch_type || TEMPLATE_TOUCH_TYPES.ANY) +
    scorePerformance(template);

  return {
    ...template,
    selector_use_case: template_use_case,
    canonical_use_case,
    touch_type,
    property_type_scope,
    explicit_is_first_touch,
    property_type_scope_match_reason: property_type_scope_compatibility.reason,
    deal_strategy,
    selection_metadata: metadata,
    rejection_reasons,
    operational_rejection_reasons,
    score,
  };
}

function countRejectionReasons(candidates = [], key = "rejection_reasons") {
  return candidates.reduce((counts, candidate) => {
    for (const reason of candidate?.[key] || []) {
      counts[reason] = (counts[reason] || 0) + 1;
    }

    return counts;
  }, {});
}

function buildCandidateAudit(candidate = null) {
  return {
    template_id: candidate?.item_id ?? null,
    active: clean(candidate?.active) || null,
    use_case: clean(candidate?.selector_use_case || candidate?.use_case) || null,
    canonical_use_case: clean(candidate?.canonical_use_case) || null,
    explicit_is_first_touch: clean(candidate?.explicit_is_first_touch) || null,
    touch_type: clean(candidate?.touch_type) || null,
    language: clean(candidate?.language) || null,
    property_type_scope: clean(candidate?.property_type_scope) || null,
    property_type_scope_match_reason:
      clean(candidate?.property_type_scope_match_reason) || null,
    deal_strategy: clean(candidate?.deal_strategy) || null,
    sequence_position: clean(candidate?.sequence_position) || null,
    stage_label: clean(candidate?.stage_label || candidate?.variant_group) || null,
    prefetch_rejection_reasons: Array.isArray(candidate?.prefetch_rejection_reasons)
      ? candidate.prefetch_rejection_reasons
      : [],
    rejection_reasons: Array.isArray(candidate?.rejection_reasons)
      ? candidate.rejection_reasons
      : [],
    operational_rejection_reasons: Array.isArray(candidate?.operational_rejection_reasons)
      ? candidate.operational_rejection_reasons
      : [],
    metadata: candidate?.selection_metadata || summarizeTemplateSelectorMetadata(candidate),
  };
}

function logTemplatePrefetchAudit({
  source = "podio",
  selector_input = null,
  filter_requests = [],
  raw_candidates = [],
  prefetch_candidates = [],
  context = null,
  strict_touch_one_podio_only = false,
} = {}) {
  if (source !== "podio") return null;

  const owner_id = context?.ids?.master_owner_id ?? context?.ids?.owner_id ?? null;
  const payload = {
    owner_id,
    source,
    requested_core_selector: selector_input,
    filter_requests,
    raw_candidate_count: raw_candidates.length,
    raw_candidate_ids: raw_candidates.map((candidate) => candidate?.item_id).filter(Boolean),
    prefetch_excluded_count: Math.max(raw_candidates.length - prefetch_candidates.length, 0),
    prefetch_survivor_count: prefetch_candidates.length,
    prefetch_survivor_ids: prefetch_candidates
      .map((candidate) => candidate?.item_id)
      .filter(Boolean),
    prefetch_candidates: raw_candidates.map((candidate) => ({
      template_id: candidate?.item_id ?? null,
      active: clean(candidate?.active) || null,
      use_case: clean(candidate?.selector_use_case || candidate?.use_case) || null,
      canonical_use_case: clean(candidate?.canonical_use_case) || null,
      explicit_is_first_touch: clean(candidate?.explicit_is_first_touch) || null,
      language: clean(candidate?.language) || null,
      property_type_scope: clean(candidate?.property_type_scope) || null,
      prefetch_rejection_reasons: Array.isArray(candidate?.prefetch_rejection_reasons)
        ? candidate.prefetch_rejection_reasons
        : [],
    })),
  };

  info("template.podio_prefetch_audit", payload);

  if (strict_touch_one_podio_only) {
    info("template.touch_one_prefetch_audit", payload);
  }

  return payload;
}

async function collectSourceCandidates({
  source = "podio",
  filter_requests = [],
  selector_input = null,
  strict_touch_one_podio_only = false,
  remote_fetcher = fetchTemplatesCached,
  local_fetcher = fetchLocalTemplates,
  diagnostics = null,
  context = null,
} = {}) {
  let all_candidates = [];
  const filter_request_audit = [];

  for (const request of filter_requests) {
    const filter_set = request?.filter_set || {};
    const paginate = Boolean(source === "podio" && request?.paginate);
    const page_limit = paginate ? TOUCH_ONE_ACTIVE_SWEEP_PAGE_LIMIT : null;
    let offset = 0;
    let page_number = 0;

    while (true) {
      let batch = [];
      let status = "ok";
      let error_type = null;
      let error_message = null;

      if (source === "podio") {
        try {
          batch = await remote_fetcher(filter_set, {
            fetch_limit: page_limit,
            fetch_offset: offset,
          });
          batch = withTemplateSource(batch, "podio");
        } catch (error) {
          if (isTemplateFilterValidationError(error)) {
            if (diagnostics) diagnostics.podio_filter_validation_failures += 1;
            status = "validation_failed";
            error_type = "filter_validation_failed";
          } else {
            if (diagnostics) diagnostics.podio_fetch_failures += 1;
            status = "fetch_failed";
            error_type = "fetch_failed";
          }

          error_message = clean(error?.message) || null;
          batch = [];
        }
      } else {
        batch = withTemplateSource(local_fetcher(filter_set), "local_registry");
      }

      if (diagnostics && source === "podio" && batch.length > 0) {
        diagnostics.podio_batches_with_results += 1;
      }

      filter_request_audit.push({
        label: request?.label || null,
        filter_set,
        fetch_limit: page_limit,
        fetch_offset: offset,
        page_number,
        returned_count: batch.length,
        status,
        error_type,
        error_message,
      });

      all_candidates.push(...batch);

      if (!paginate) break;
      if (status !== "ok") break;
      if (batch.length < TOUCH_ONE_ACTIVE_SWEEP_PAGE_LIMIT) break;

      page_number += 1;
      offset += TOUCH_ONE_ACTIVE_SWEEP_PAGE_LIMIT;

      if (page_number >= TOUCH_ONE_ACTIVE_SWEEP_MAX_PAGES) {
        filter_request_audit.push({
          label: request?.label || null,
          filter_set,
          fetch_limit: page_limit,
          fetch_offset: offset,
          page_number,
          returned_count: 0,
          status: "pagination_cap_reached",
          error_type: null,
          error_message: null,
        });
        break;
      }
    }
  }

  const raw_candidates = dedupeTemplates(all_candidates);
  const prefetch_candidates = strict_touch_one_podio_only
    ? raw_candidates
        .map((template) => evaluateTouchOnePrefetchCandidate(template, selector_input))
        .filter((candidate) => candidate.prefetch_rejection_reasons.length === 0)
    : raw_candidates;
  const raw_prefetch_candidates = strict_touch_one_podio_only
    ? raw_candidates.map((template) => evaluateTouchOnePrefetchCandidate(template, selector_input))
    : raw_candidates;

  if (diagnostics) {
    if (source === "podio") {
      diagnostics.podio_raw_candidates_loaded += raw_candidates.length;
      diagnostics.podio_prefetch_candidates_excluded += Math.max(
        raw_candidates.length - prefetch_candidates.length,
        0
      );
      diagnostics.podio_prefetch_candidates_survived += prefetch_candidates.length;
      diagnostics.podio_filter_requests.push(...filter_request_audit);
      diagnostics.podio_candidates_considered += prefetch_candidates.length;
    } else {
      diagnostics.local_candidates_considered += prefetch_candidates.length;
    }
  }

  const prefetch_audit_payload = logTemplatePrefetchAudit({
    source,
    selector_input,
    filter_requests: filter_request_audit,
    raw_candidates: raw_prefetch_candidates,
    prefetch_candidates,
    context,
    strict_touch_one_podio_only,
  });

  return {
    raw_candidates: raw_prefetch_candidates,
    candidates: prefetch_candidates,
    prefetch_audit_payload,
  };
}

function logTemplateSelectorAudit({
  source = "podio",
  selector_input = null,
  candidates = [],
  survivors = [],
  context = null,
  strict_touch_one_podio_only = false,
} = {}) {
  const owner_id = context?.ids?.master_owner_id ?? context?.ids?.owner_id ?? null;
  const audit_payload = {
    owner_id,
    source,
    requested_core_selector: selector_input,
    total_candidates: candidates.length,
    candidate_template_ids: candidates.map((candidate) => candidate?.item_id).filter(Boolean),
    candidates: candidates.map(buildCandidateAudit),
    survivors: survivors.map((candidate) => candidate?.item_id).filter(Boolean),
  };

  info("template.selector_candidate_audit", audit_payload);

  if (
    strict_touch_one_podio_only &&
    safeCategoryEquals(selector_input?.use_case, "ownership_check") &&
    selector_input?.touch_type === TEMPLATE_TOUCH_TYPES.FIRST_TOUCH
  ) {
    info("template.touch_one_candidate_audit", {
      owner_id,
      touch_number: 1,
      requested_language: selector_input?.language || "English",
      requested_property_type: selector_input?.property_type_scope || null,
      total_candidates: candidates.length,
      candidates: candidates.map((candidate) => ({
        template_id: candidate?.item_id ?? null,
        active: clean(candidate?.active) || null,
        use_case: clean(candidate?.selector_use_case || candidate?.use_case) || null,
        is_first_touch: clean(candidate?.explicit_is_first_touch) || "No",
        language: clean(candidate?.language) || null,
        property_type_scope: clean(candidate?.property_type_scope) || null,
        property_type_scope_match_reason:
          clean(candidate?.property_type_scope_match_reason) || null,
        sequence_position: clean(candidate?.sequence_position) || null,
        stage_label: clean(candidate?.stage_label || candidate?.variant_group) || null,
        prefetch_rejection_reasons: Array.isArray(candidate?.prefetch_rejection_reasons)
          ? candidate.prefetch_rejection_reasons
          : [],
        rejection_reasons: Array.isArray(candidate?.rejection_reasons)
          ? candidate.rejection_reasons
          : [],
        operational_rejection_reasons: Array.isArray(candidate?.operational_rejection_reasons)
          ? candidate.operational_rejection_reasons
          : [],
      })),
      survivors: survivors.map((candidate) => candidate?.item_id).filter(Boolean),
    });
  }

  return audit_payload;
}

async function evaluateSourceSelection({
  source = "podio",
  filter_requests = [],
  selector_input = null,
  recently_used_template_ids = [],
  context = null,
  template_render_overrides = {},
  strict_touch_one_podio_only = false,
  remote_fetcher = fetchTemplatesCached,
  local_fetcher = fetchLocalTemplates,
  diagnostics = null,
} = {}) {
  const collected = await collectSourceCandidates({
    source,
    filter_requests,
    selector_input,
    strict_touch_one_podio_only,
    remote_fetcher,
    local_fetcher,
    diagnostics,
    context,
  });
  const candidates = collected.candidates;

  const evaluated = candidates
    .map((template) =>
      evaluateTemplateCandidate(template, {
        selector_input,
        recently_used_template_ids,
        context,
        template_render_overrides,
        strict_touch_one_podio_only,
      })
    )
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return String(left?.item_id ?? "").localeCompare(String(right?.item_id ?? ""));
    });

  const survivors = evaluated.filter(
    (candidate) =>
      candidate.rejection_reasons.length === 0 &&
      candidate.operational_rejection_reasons.length === 0
  );

  const audit_payload = logTemplateSelectorAudit({
    source,
    selector_input,
    candidates: evaluated,
    survivors,
    context,
    strict_touch_one_podio_only,
  });

  return {
    raw_candidates: collected.raw_candidates,
    candidates: evaluated,
    survivors,
    audit_payload,
    prefetch_audit_payload: collected.prefetch_audit_payload,
  };
}

export async function loadTemplateCandidates({
  template_selector = null,
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
  touch_type = null,
  touch_number = null,
  message_type = null,
  property_type_scope = null,
  deal_strategy = null,
} = {}) {
  const resolution_diagnostics = createTemplateResolutionDiagnostics();
  const selector_input = buildTemplateSelectorInput({
    template_selector,
    use_case,
    language,
    property_type_scope,
    deal_strategy,
    touch_type,
    touch_number,
    message_type,
    category,
    secondary_category,
    sequence_position,
    route: context?.route || null,
    context,
    strict_touch_one_podio_only,
  });
  const requested_use_cases = uniq([
    selector_input.use_case,
    ...(Array.isArray(required_use_cases) ? required_use_cases : []),
  ]);
  const use_case_candidates = requested_use_cases.flatMap((requested) =>
    expandSelectorUseCases(requested, variant_group)
  );
  const filter_requests = buildRemoteFilterRequests({
    selector_input,
    use_case_candidates,
    strict_touch_one_podio_only,
  });
  const sources = strict_touch_one_podio_only
    ? ["podio"]
    : ["podio", "local_registry"];

  let last_audit_payload = null;
  let last_prefetch_audit_payload = null;

  for (const source of sources) {
    const result = await evaluateSourceSelection({
      source,
      filter_requests,
      selector_input,
      recently_used_template_ids,
      context,
      template_render_overrides,
      strict_touch_one_podio_only,
      remote_fetcher,
      local_fetcher,
      diagnostics: resolution_diagnostics,
    });

    last_audit_payload = result.audit_payload;
    last_prefetch_audit_payload = result.prefetch_audit_payload;

    if (result.survivors.length) {
      resolution_diagnostics.selected_bucket_source = source;
      const template_resolution_source =
        source === "podio" ? "podio_template" : "local_template_fallback";
      const template_fallback_reason =
        source === "local_registry"
          ? resolution_diagnostics.podio_fetch_failures > 0
            ? "podio_template_fetch_failed"
            : "no_podio_template_match"
          : null;
      const selection_diagnostics = {
        selector_input,
        requested_use_cases,
        use_case_candidates,
        ignored_metadata_filters: {
          allow_variant_group_fallback: Boolean(allow_variant_group_fallback),
          allowed_variant_groups: Array.isArray(allowed_variant_groups)
            ? allowed_variant_groups
            : allowed_variant_groups instanceof Set
              ? [...allowed_variant_groups]
              : [],
          required_variant_groups: Array.isArray(required_variant_groups)
            ? required_variant_groups
            : required_variant_groups instanceof Set
              ? [...required_variant_groups]
              : [],
          require_explicit_variant_group: Boolean(require_explicit_variant_group),
          variant_group: clean(variant_group) || null,
          tone: clean(tone) || null,
          gender_variant: clean(gender_variant) || null,
          paired_with_agent_type: clean(paired_with_agent_type) || null,
          fallback_agent_type: clean(fallback_agent_type) || null,
        },
        resolution: summarizeTemplateResolutionDiagnostics(resolution_diagnostics),
        audit_summary: {
          source,
          raw_candidate_count: Array.isArray(result.raw_candidates)
            ? result.raw_candidates.length
            : 0,
          total_candidates: result.candidates.length,
          survivor_count: result.survivors.length,
          rejection_counts: countRejectionReasons(result.candidates, "rejection_reasons"),
          prefetch_rejection_counts: countRejectionReasons(
            result.raw_candidates,
            "prefetch_rejection_reasons"
          ),
          operational_rejection_counts: countRejectionReasons(
            result.candidates,
            "operational_rejection_reasons"
          ),
        },
      };

      return result.survivors.map((template) => ({
        ...template,
        rotation_key,
        template_resolution_source,
        template_fallback_reason,
        template_selection_diagnostics: selection_diagnostics,
      }));
    }
  }

  // ── Reengagement fallback ladder ─────────────────────────────────────────────
  // When no templates survived exact + alias matching, retry with progressively
  // degraded criteria before giving up.  Order:
  //   1. "reengagement" use_case  (generic follow-up for any stage)
  //   2. "ownership_check_follow_up"  (most common first-stage follow-up)
  // Only runs for Follow-Up touch_type and non-strict modes.
  if (
    !strict_touch_one_podio_only &&
    selector_input.touch_type === TEMPLATE_TOUCH_TYPES.FOLLOW_UP
  ) {
    const fallback_use_cases = ["reengagement", "ownership_check_follow_up"];
    const already_tried = new Set(use_case_candidates.map((uc) => normalizeSelectorText(uc)));

    for (const fallback_uc of fallback_use_cases) {
      if (already_tried.has(normalizeSelectorText(fallback_uc))) continue;

      const fallback_selector = { ...selector_input, use_case: fallback_uc, deal_strategy: null };
      const fallback_candidates = expandSelectorUseCases(fallback_uc, variant_group);
      const fallback_filters = buildRemoteFilterRequests({
        selector_input: fallback_selector,
        use_case_candidates: fallback_candidates,
        strict_touch_one_podio_only: false,
      });

      for (const source of sources) {
        const result = await evaluateSourceSelection({
          source,
          filter_requests: fallback_filters,
          selector_input: fallback_selector,
          recently_used_template_ids,
          context,
          template_render_overrides,
          strict_touch_one_podio_only: false,
          remote_fetcher,
          local_fetcher,
          diagnostics: resolution_diagnostics,
        });

        if (result.survivors.length) {
          resolution_diagnostics.selected_bucket_source = source;
          const template_resolution_source =
            source === "podio" ? "podio_template" : "local_template_fallback";

          info("template.reengagement_fallback_matched", {
            original_use_case: selector_input.use_case,
            fallback_use_case: fallback_uc,
            source,
            survivor_count: result.survivors.length,
          });

          return result.survivors.map((template) => ({
            ...template,
            rotation_key,
            template_resolution_source,
            template_fallback_reason: "reengagement_fallback",
            template_fallback_use_case: fallback_uc,
            template_selection_diagnostics: {
              selector_input: fallback_selector,
              requested_use_cases: [fallback_uc],
              use_case_candidates: fallback_candidates,
              resolution: summarizeTemplateResolutionDiagnostics(resolution_diagnostics),
              audit_summary: {
                source,
                total_candidates: result.candidates.length,
                survivor_count: result.survivors.length,
                rejection_counts: countRejectionReasons(result.candidates, "rejection_reasons"),
                operational_rejection_counts: countRejectionReasons(
                  result.candidates,
                  "operational_rejection_reasons"
                ),
              },
            },
          }));
        }
      }
    }
  }

  const failure_diagnostics = {
    selector_input,
    requested_use_cases,
    use_case_candidates,
    selection_diagnostics: summarizeTemplateResolutionDiagnostics(
      resolution_diagnostics
    ),
    filter_requests,
    prefetch_audit_payload: last_prefetch_audit_payload,
    audit_payload: last_audit_payload,
    elimination_summary: last_audit_payload
      ? {
          total_candidates: last_audit_payload.total_candidates || 0,
          survivors: (last_audit_payload.survivors || []).length,
          rejection_counts: countRejectionReasons(
            last_audit_payload.candidates || [],
            "rejection_reasons"
          ),
          operational_rejection_counts: countRejectionReasons(
            last_audit_payload.candidates || [],
            "operational_rejection_reasons"
          ),
        }
      : null,
  };

  if (strict_touch_one_podio_only) {
    warn("template.touch_one_template_missing", {
      reason: "NO_STAGE_1_TEMPLATE_FOUND",
      ...failure_diagnostics,
    });
    const err = new Error("NO_STAGE_1_TEMPLATE_FOUND");
    err.code = "NO_STAGE_1_TEMPLATE_FOUND";
    err.diagnostics = failure_diagnostics;
    throw err;
  }

  warn("template.template_missing", failure_diagnostics);
  return [];
}

export async function loadTemplate({
  template_selector = null,
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
  touch_type = null,
  touch_number = null,
  message_type = null,
  property_type_scope = null,
  deal_strategy = null,
} = {}) {
  const scored = await loadTemplateCandidates({
    template_selector,
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
    rotation_key,
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
    touch_type,
    touch_number,
    message_type,
    property_type_scope,
    deal_strategy,
  });

  if (!scored.length) return null;

  const top_score = scored[0].score;
  const top_cluster = scored.filter((template) => template.score >= top_score - 10);

  return rotateVariant(top_cluster, rotation_key);
}

export default {
  clearTemplateBatchCache,
  fetchTemplatesCached,
  loadTemplateCandidates,
  loadTemplate,
};
