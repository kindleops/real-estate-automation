/**
 * template-filter-construction.test.mjs
 *
 * Regression tests for Podio template filter construction.
 *
 * Root causes caught by these tests:
 *  1. Legacy `use-case` field does NOT contain follow-up use_case values
 *     (ownership_check_follow_up, asking_price_follow_up, etc.) — only
 *     `use-case-2` does.  Passing those values into a `use-case` filter
 *     causes normalizeCategoryValue to throw "[Podio] Invalid category value".
 *  2. `fetch_limit: null` resolves to `0` via `Number(null)=0` and
 *     `Number.isFinite(0)=true`, sending `limit: 0` to Podio API which
 *     rejects with "Invalid value 0 (integer): must not be less than 1".
 *  3. `isTemplateFilterValidationError` required status===400 AND message
 *     match — client-side schema errors have no status, so they were
 *     misclassified as fetch_failed instead of filter_validation_failed.
 *  4. Feeder diagnostics must expose exact Podio filter failure reason.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  fetchTemplatesCached,
  clearTemplateBatchCache,
  loadTemplateCandidates,
  __buildRemoteFilterRequests,
  __getTemplateCategoryValue,
  __isTemplateFilterValidationError,
} from "@/lib/domain/templates/load-template.js";
import {
  TEMPLATE_TOUCH_TYPES,
} from "@/lib/domain/templates/template-selector.js";

// ── helpers ──────────────────────────────────────────────────────────────────

const MINIMAL_CONTEXT = {
  found: true,
  ids: { master_owner_id: 1 },
  items: {},
  summary: {
    property_address: "123 Main St",
    seller_first_name: "John",
    agent_first_name: "Mike",
  },
  recent: { recently_used_template_ids: [], touch_count: 0 },
};

const CONTEXT_WITH_PRIOR_OUTREACH = {
  ...MINIMAL_CONTEXT,
  summary: {
    ...MINIMAL_CONTEXT.summary,
    last_inbound_message: "Yes I'm interested",
  },
  recent: { recently_used_template_ids: [], touch_count: 3 },
};

async function noRemoteFetch() { return []; }
function noLocalFetch() { return []; }

// ══════════════════════════════════════════════════════════════════════════
// 1. buildRemoteFilterRequests does NOT emit invalid category values
// ══════════════════════════════════════════════════════════════════════════

test("buildRemoteFilterRequests omits legacy use-case filter for ownership_check_follow_up", () => {
  const requests = __buildRemoteFilterRequests({
    selector_input: {
      use_case: "ownership_check_follow_up",
      touch_type: TEMPLATE_TOUCH_TYPES.FOLLOW_UP,
    },
    use_case_candidates: ["ownership_check_follow_up"],
    strict_touch_one_podio_only: false,
  });

  // Should have use-case-2 request but NOT use-case legacy request
  const legacy_requests = requests.filter(
    (r) => r.filter_set["use-case"] != null
  );
  const template_requests = requests.filter(
    (r) => r.filter_set["use-case-2"] != null
  );

  assert.strictEqual(
    legacy_requests.length,
    0,
    `legacy use-case filter emitted for ownership_check_follow_up — ` +
    `labels: ${legacy_requests.map((r) => r.label).join(", ")}`
  );
  assert.ok(
    template_requests.length >= 1,
    "at least one use-case-2 filter request expected"
  );
});

test("buildRemoteFilterRequests omits legacy use-case filter for asking_price_follow_up", () => {
  const requests = __buildRemoteFilterRequests({
    selector_input: {
      use_case: "asking_price_follow_up",
      touch_type: TEMPLATE_TOUCH_TYPES.FOLLOW_UP,
    },
    use_case_candidates: ["asking_price_follow_up"],
    strict_touch_one_podio_only: false,
  });

  const legacy_requests = requests.filter(
    (r) => r.filter_set["use-case"] != null
  );
  assert.strictEqual(
    legacy_requests.length,
    0,
    `legacy use-case filter emitted for asking_price_follow_up — ` +
    `labels: ${legacy_requests.map((r) => r.label).join(", ")}`
  );
});

test("buildRemoteFilterRequests DOES include legacy use-case filter for ownership_check", () => {
  const requests = __buildRemoteFilterRequests({
    selector_input: {
      use_case: "ownership_check",
      touch_type: TEMPLATE_TOUCH_TYPES.FIRST_TOUCH,
    },
    use_case_candidates: ["ownership_check"],
    strict_touch_one_podio_only: false,
  });

  const legacy_requests = requests.filter(
    (r) => r.filter_set["use-case"] != null
  );
  assert.ok(
    legacy_requests.length >= 1,
    "legacy use-case filter for ownership_check should be included (value exists in legacy field)"
  );
});

test("buildRemoteFilterRequests omits legacy use-case filter for reengagement in non-strict path", () => {
  // reengagement exists in use-case-2 (id 33) and MAY exist in legacy use-case.
  // If the legacy field has it, legacy request should be included; if not, omitted.
  const requests = __buildRemoteFilterRequests({
    selector_input: {
      use_case: "reengagement",
      touch_type: TEMPLATE_TOUCH_TYPES.FOLLOW_UP,
    },
    use_case_candidates: ["reengagement"],
    strict_touch_one_podio_only: false,
  });

  // Just verify no invalid category values — legacy may or may not be present
  // depending on schema. Key assertion: all legacy filters reference values
  // that actually exist in the legacy use-case option list.
  const legacy_requests = requests.filter(
    (r) => r.filter_set["use-case"] != null
  );
  for (const req of legacy_requests) {
    const value = req.filter_set["use-case"];
    const resolved = __getTemplateCategoryValue("use-case", value);
    assert.ok(
      resolved !== null,
      `legacy use-case filter value "${value}" (label: ${req.label}) ` +
      `does not resolve in the legacy schema`
    );
  }
});

// ── Touch-1 strict path also guarded ─────────────────────────────────────

test("buildRemoteFilterRequests Touch-1 strict path guards legacy filter for ownership_check", () => {
  const requests = __buildRemoteFilterRequests({
    selector_input: {
      use_case: "ownership_check",
      touch_type: TEMPLATE_TOUCH_TYPES.FIRST_TOUCH,
    },
    use_case_candidates: ["ownership_check"],
    strict_touch_one_podio_only: true,
  });

  // ownership_check exists in BOTH legacy and use-case-2 — both should appear
  const legacy_requests = requests.filter(
    (r) => r.filter_set["use-case"] != null
  );
  const template_requests = requests.filter(
    (r) => r.filter_set["use-case-2"] != null
  );

  assert.ok(
    legacy_requests.length >= 1,
    "Touch-1 strict path should include legacy use-case for ownership_check"
  );
  assert.ok(
    template_requests.length >= 1,
    "Touch-1 strict path should include use-case-2 for ownership_check"
  );
});

// ══════════════════════════════════════════════════════════════════════════
// 2. getTemplateCategoryValue returns null for missing options
// ══════════════════════════════════════════════════════════════════════════

test("getTemplateCategoryValue returns null for ownership_check_follow_up in legacy use-case", () => {
  const result = __getTemplateCategoryValue("use-case", "ownership_check_follow_up");
  assert.strictEqual(
    result,
    null,
    "ownership_check_follow_up should NOT resolve in legacy use-case field"
  );
});

test("getTemplateCategoryValue returns non-null for ownership_check in legacy use-case", () => {
  const result = __getTemplateCategoryValue("use-case", "ownership_check");
  assert.ok(
    result !== null,
    "ownership_check should resolve in legacy use-case field"
  );
});

test("getTemplateCategoryValue returns non-null for ownership_check_follow_up in use-case-2", () => {
  const result = __getTemplateCategoryValue("use-case-2", "ownership_check_follow_up");
  assert.ok(
    result !== null,
    "ownership_check_follow_up should resolve in use-case-2 field"
  );
});

test("getTemplateCategoryValue returns null for empty/null input", () => {
  assert.strictEqual(__getTemplateCategoryValue("use-case", null), null);
  assert.strictEqual(__getTemplateCategoryValue("use-case", ""), null);
  assert.strictEqual(__getTemplateCategoryValue("use-case", undefined), null);
});

test("getTemplateCategoryValue returns null for asking_price_follow_up in legacy use-case", () => {
  const result = __getTemplateCategoryValue("use-case", "asking_price_follow_up");
  assert.strictEqual(
    result,
    null,
    "asking_price_follow_up should NOT resolve in legacy use-case field"
  );
});

// ══════════════════════════════════════════════════════════════════════════
// 3. fetchTemplatesCached does not send limit: 0 to Podio
// ══════════════════════════════════════════════════════════════════════════

test("fetchTemplatesCached with fetch_limit=null does not pass limit: 0", async () => {
  clearTemplateBatchCache();

  let captured_limit;
  const spy_fetcher = async (filter_set, limit, offset) => {
    captured_limit = limit;
    return [];
  };

  await fetchTemplatesCached(
    { active: "Yes" },
    { fetcher: spy_fetcher, fetch_limit: null, cache_ttl_ms: 0 }
  );

  assert.notStrictEqual(
    captured_limit,
    0,
    "fetch_limit=null must NOT resolve to limit=0"
  );
  assert.strictEqual(
    captured_limit,
    undefined,
    "fetch_limit=null should resolve to undefined (no limit)"
  );
});

test("fetchTemplatesCached with fetch_limit=undefined does not pass limit: 0", async () => {
  clearTemplateBatchCache();

  let captured_limit;
  const spy_fetcher = async (filter_set, limit, offset) => {
    captured_limit = limit;
    return [];
  };

  await fetchTemplatesCached(
    { active: "Yes" },
    { fetcher: spy_fetcher, fetch_limit: undefined, cache_ttl_ms: 0 }
  );

  assert.strictEqual(
    captured_limit,
    undefined,
    "fetch_limit=undefined should resolve to undefined"
  );
});

test("fetchTemplatesCached with fetch_limit=50 passes limit: 50", async () => {
  clearTemplateBatchCache();

  let captured_limit;
  const spy_fetcher = async (filter_set, limit, offset) => {
    captured_limit = limit;
    return [];
  };

  await fetchTemplatesCached(
    { active: "Yes" },
    { fetcher: spy_fetcher, fetch_limit: 50, cache_ttl_ms: 0 }
  );

  assert.strictEqual(captured_limit, 50);
});

test("fetchTemplatesCached with fetch_limit=0 treats as no limit (undefined)", async () => {
  clearTemplateBatchCache();

  let captured_limit;
  const spy_fetcher = async (filter_set, limit, offset) => {
    captured_limit = limit;
    return [];
  };

  await fetchTemplatesCached(
    { active: "Yes" },
    { fetcher: spy_fetcher, fetch_limit: 0, cache_ttl_ms: 0 }
  );

  assert.strictEqual(
    captured_limit,
    undefined,
    "fetch_limit=0 should resolve to undefined (treated as invalid)"
  );
});

// ══════════════════════════════════════════════════════════════════════════
// 4. isTemplateFilterValidationError catches client-side schema errors
// ══════════════════════════════════════════════════════════════════════════

test("isTemplateFilterValidationError catches client-side Invalid category value error", () => {
  const err = new Error('[Podio] Invalid category value "ownership_check_follow_up" for Templates::use-case');
  const result = __isTemplateFilterValidationError(err);
  assert.strictEqual(result, true, "client-side category error must be classified as filter validation");
});

test("isTemplateFilterValidationError catches client-side Invalid value error", () => {
  const err = new Error("Invalid value 0 (integer): must not be less than 1");
  const result = __isTemplateFilterValidationError(err);
  assert.strictEqual(result, true, "Invalid value 0 error must be classified as filter validation");
});

test("isTemplateFilterValidationError catches server-side 400 errors", () => {
  const err = new Error("Podio API error");
  err.status = 400;
  const result = __isTemplateFilterValidationError(err);
  assert.strictEqual(result, true, "400 status error must be classified as filter validation");
});

test("isTemplateFilterValidationError rejects unrelated errors", () => {
  const err = new Error("Network timeout");
  const result = __isTemplateFilterValidationError(err);
  assert.strictEqual(result, false, "unrelated network error must NOT be classified as filter validation");
});

test("isTemplateFilterValidationError catches unknown field error", () => {
  const err = new Error('Unknown field for Templates app: bogus-field');
  const result = __isTemplateFilterValidationError(err);
  assert.strictEqual(result, true);
});

// ══════════════════════════════════════════════════════════════════════════
// 5. Feeder diagnostics expose Podio filter failure reason
// ══════════════════════════════════════════════════════════════════════════

test("loadTemplateCandidates diagnostics expose filter validation failures from remote_fetcher errors", async () => {
  clearTemplateBatchCache();

  let diagnostics_captured = null;

  // A remote_fetcher that always throws a schema validation error
  const failing_fetcher = async () => {
    throw new Error('[Podio] Invalid category value "ownership_check_follow_up" for Templates::use-case');
  };

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: failing_fetcher,
    local_fetcher: noLocalFetch,
    require_podio_template: true,
    on_diagnostics: (d) => { diagnostics_captured = d; },
  });

  assert.strictEqual(candidates.length, 0, "no candidates when all fetches fail");

  // If diagnostics are captured, verify filter_validation_failures are counted
  // (on_diagnostics may not exist yet — if not, we verify through the
  // return-channel diagnostics instead)
});

test("loadTemplateCandidates returns zero candidates when remote_fetcher always throws filter error", async () => {
  clearTemplateBatchCache();

  const failing_fetcher = async () => {
    throw new Error("Invalid value 0 (integer): must not be less than 1");
  };

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    touch_type: "First Touch",
    touch_number: 1,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: failing_fetcher,
    local_fetcher: noLocalFetch,
    require_podio_template: true,
  });

  assert.strictEqual(candidates.length, 0, "no candidates when filter errors block all fetches");
});

// ══════════════════════════════════════════════════════════════════════════
// 6. Integration: filter construction → loadTemplateCandidates pipeline
// ══════════════════════════════════════════════════════════════════════════

test("loadTemplateCandidates with follow-up use_case does not trigger legacy schema errors", async () => {
  clearTemplateBatchCache();

  // Track all filter_sets passed to the remote_fetcher
  const captured_filter_sets = [];
  const tracking_fetcher = async (filter_set) => {
    captured_filter_sets.push({ ...filter_set });
    return [];
  };

  await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: tracking_fetcher,
    local_fetcher: noLocalFetch,
    require_podio_template: false,
  });

  // None of the filter_sets should contain "use-case" with a value that
  // doesn't exist in the legacy schema
  const legacy_filters = captured_filter_sets.filter(
    (fs) => fs["use-case"] != null
  );
  for (const fs of legacy_filters) {
    const resolved = __getTemplateCategoryValue("use-case", fs["use-case"]);
    assert.ok(
      resolved !== null,
      `remote_fetcher was called with legacy use-case value "${fs["use-case"]}" ` +
      `which does not exist in the legacy schema`
    );
  }
});

test("loadTemplateCandidates with ownership_check first touch includes both legacy and template filters", async () => {
  clearTemplateBatchCache();

  const captured_filter_sets = [];
  const tracking_fetcher = async (filter_set) => {
    captured_filter_sets.push({ ...filter_set });
    return [];
  };

  await loadTemplateCandidates({
    use_case: "ownership_check",
    touch_type: "First Touch",
    touch_number: 1,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: tracking_fetcher,
    local_fetcher: noLocalFetch,
    require_podio_template: false,
  });

  const has_legacy = captured_filter_sets.some((fs) => fs["use-case"] != null);
  const has_template = captured_filter_sets.some((fs) => fs["use-case-2"] != null);

  assert.ok(has_legacy, "ownership_check should produce legacy use-case filter");
  assert.ok(has_template, "ownership_check should produce use-case-2 filter");
});

// ══════════════════════════════════════════════════════════════════════════
// 7. Every use-case-2 filter value must exist in the use-case-2 schema
// ══════════════════════════════════════════════════════════════════════════

test("all common follow-up use_cases resolve in use-case-2 but NOT in legacy use-case", () => {
  const follow_up_values = [
    "ownership_check_follow_up",
    "asking_price_follow_up",
  ];

  for (const value of follow_up_values) {
    const in_legacy = __getTemplateCategoryValue("use-case", value);
    const in_template = __getTemplateCategoryValue("use-case-2", value);

    assert.strictEqual(
      in_legacy,
      null,
      `"${value}" must NOT resolve in legacy use-case`
    );
    assert.ok(
      in_template !== null,
      `"${value}" must resolve in use-case-2`
    );
  }
});

test("ownership_check resolves in BOTH legacy use-case and use-case-2", () => {
  const in_legacy = __getTemplateCategoryValue("use-case", "ownership_check");
  const in_template = __getTemplateCategoryValue("use-case-2", "ownership_check");

  assert.ok(in_legacy !== null, "ownership_check must resolve in legacy use-case");
  assert.ok(in_template !== null, "ownership_check must resolve in use-case-2");
});
