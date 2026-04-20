/**
 * supabase-template-source.test.mjs
 *
 * Guards the Supabase sms_templates first-class runtime source integration.
 *
 * Architecture under test:
 *   loadTemplateCandidates queries sms_templates BEFORE Podio Templates.
 *   Podio Templates is demoted to fallback (KPI / metrics mirroring).
 *   Local registry is emergency fallback in dev / test only.
 *
 * All tests use an injected `supabase_fetcher` param to bypass real Supabase I/O.
 * The injected fetcher returns already-normalised rows (via normalizeSupabaseTemplateRow)
 * so we also cover that normaliser's Stage 1 signal derivation.
 *
 * Tests:
 *   1. use_case=ownership_check resolves from sms_templates
 *   2. stage_code=S1 (is_first_touch=null) derives is_first_touch=Yes
 *   3. is_first_touch=true boolean is normalised to "Yes"
 *   4. inactive row is excluded from survivors
 *   5. empty template_body is excluded (operational_rejection: empty_text)
 *   6. English ranks above Spanish when both are present
 *   7. podio_template_id is preserved in selected_podio_template_id
 *   8. supabase_raw_candidates_loaded and supabase_survivor_count are accurate
 *   9. local_fetcher fallback fires when supabase_fetcher returns empty
 */

import test from "node:test";
import assert from "node:assert/strict";

import { loadTemplateCandidates } from "@/lib/domain/templates/load-template.js";
import { normalizeSupabaseTemplateRow } from "@/lib/domain/templates/load-supabase-template-candidates.js";

// ── test helpers ──────────────────────────────────────────────────────────────

/**
 * Build a minimal sms_templates-style DB row.  Individual tests override fields
 * as needed via the `overrides` param.
 */
function makeRow(overrides = {}) {
  return {
    id:                  "sb-row-001",
    template_id:         "sbt-001",
    podio_template_id:   null,
    use_case:            "ownership_check",
    stage_code:          "S1",
    stage_label:         "Stage 1 — Ownership Confirmation",
    is_active:           true,
    is_first_touch:      true,
    is_follow_up:        false,
    language:            "English",
    template_body:       "Hi {{first_name}}, I own property at {{address}}.",
    template_name:       "Test Stage 1 English",
    property_type_scope: null,
    deal_strategy:       null,
    agent_persona:       null,
    english_translation: null,
    usage_count:         42,
    metadata:            {},
    variables:           [],
    ...overrides,
  };
}

/** No-op remote fetcher (prevents outbound Podio calls in tests). */
async function noRemoteFetch() {
  return [];
}

/** No-op local fetcher (prevents local_registry interference in tests). */
function noLocalFetch() {
  return [];
}

// ── 1. use_case=ownership_check resolves from sms_templates ──────────────────

test("Supabase row with use_case=ownership_check is returned as a first-class candidate", async () => {
  const normalized = normalizeSupabaseTemplateRow(
    makeRow({ use_case: "ownership_check", is_first_touch: true })
  );

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    supabase_fetcher:       async () => [normalized],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          noLocalFetch,
  });

  assert.ok(candidates.length > 0, "must return at least one candidate");
  assert.equal(
    candidates[0].selected_template_source,
    "supabase_sms_templates",
    "selected_template_source must be supabase_sms_templates"
  );
  assert.equal(
    candidates[0].template_resolution_source,
    "supabase_sms_templates",
    "template_resolution_source must be supabase_sms_templates"
  );
});

// ── 2. stage_code=S1 (is_first_touch=null) derives is_first_touch=Yes ─────────

test("stage_code=S1 with null is_first_touch derives is_first_touch=Yes in the normaliser", () => {
  const normalized = normalizeSupabaseTemplateRow(
    makeRow({ stage_code: "S1", is_first_touch: null, use_case: null })
  );

  assert.equal(
    normalized.is_first_touch,
    "Yes",
    "stage_code=S1 must produce is_first_touch=Yes even when the boolean column is null"
  );
  assert.equal(normalized.source, "supabase");
});

// ── 3. is_first_touch=true boolean is normalised to "Yes" ──────────────────────

test("is_first_touch=true boolean is normalised to the string Yes", () => {
  const normalized = normalizeSupabaseTemplateRow(makeRow({ is_first_touch: true }));
  assert.equal(normalized.is_first_touch, "Yes");
  assert.equal(normalized.active, "Yes");
});

// ── 4. inactive row is excluded from survivors ────────────────────────────────

test("inactive Supabase row (is_active=false) is excluded from the candidate pool", async () => {
  const normalized = normalizeSupabaseTemplateRow(makeRow({ is_active: false }));

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    supabase_fetcher:       async () => [normalized],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          noLocalFetch,
  });

  // Supabase pre-check skips the inactive template; no Podio / local candidates exist.
  const supabase_hits = candidates.filter(
    (c) => c.template_resolution_source === "supabase_sms_templates"
  );
  assert.equal(
    supabase_hits.length,
    0,
    "inactive Supabase row must not appear in survived candidates"
  );
});

// ── 5. empty template_body receives empty_text operational rejection ────────────

test("row with empty template_body is excluded from the candidate pool", async () => {
  const normalized = normalizeSupabaseTemplateRow(makeRow({ template_body: "" }));

  assert.equal(normalized.text, "", "normaliser must map empty template_body to empty text");

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    supabase_fetcher:       async () => [normalized],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          noLocalFetch,
  });

  const supabase_hits = candidates.filter(
    (c) => c.template_resolution_source === "supabase_sms_templates"
  );
  assert.equal(
    supabase_hits.length,
    0,
    "empty-body Supabase template must not appear in survivors (empty_text rejection)"
  );
});

// ── 6. English ranks above Spanish when both are present ─────────────────────

test("English Supabase candidate scores above Spanish candidate in the same request", async () => {
  const en = normalizeSupabaseTemplateRow(
    makeRow({ id: "sb-en", template_id: "t-en", language: "English" })
  );
  const es = normalizeSupabaseTemplateRow(
    makeRow({ id: "sb-es", template_id: "t-es", language: "Spanish" })
  );

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    // Inject Spanish first to verify sorting is driven by score, not insertion order.
    supabase_fetcher:       async () => [es, en],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          noLocalFetch,
  });

  assert.ok(candidates.length >= 2, "both language variants must be present in candidates");
  assert.equal(
    candidates[0].item_id,
    "t-en",
    "English candidate must rank first in an English-language request"
  );
});

// ── 7. podio_template_id is preserved in selected_podio_template_id ───────────

test("selected_podio_template_id mirrors the Supabase row's podio_template_id", async () => {
  const normalized = normalizeSupabaseTemplateRow(
    makeRow({ podio_template_id: "podio-99", template_id: "sbt-with-podio" })
  );

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    supabase_fetcher:       async () => [normalized],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          noLocalFetch,
  });

  assert.ok(candidates.length > 0, "must resolve at least one candidate");
  assert.equal(
    candidates[0].selected_podio_template_id,
    "podio-99",
    "selected_podio_template_id must carry the linked Podio template id"
  );
});

// ── 8. supabase diagnostic counters are accurate ─────────────────────────────

test("supabase_raw_candidates_loaded and supabase_survivor_count in selection_diagnostics are accurate", async () => {
  const active = normalizeSupabaseTemplateRow(
    makeRow({ id: "sb-active", template_id: "sbt-active", is_active: true })
  );
  const inactive = normalizeSupabaseTemplateRow(
    makeRow({ id: "sb-inactive", template_id: "sbt-inactive", is_active: false })
  );

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    supabase_fetcher:       async () => [active, inactive],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          noLocalFetch,
  });

  assert.ok(candidates.length > 0, "the active candidate must survive");

  const diag = candidates[0]?.template_selection_diagnostics?.resolution;
  assert.ok(diag, "template_selection_diagnostics.resolution must be present");
  assert.equal(
    diag.supabase_raw_candidates_loaded,
    2,
    "both rows were fetched from sms_templates"
  );
  assert.equal(
    diag.supabase_survivor_count,
    1,
    "only the active row must survive evaluation"
  );
  assert.equal(
    diag.supabase_template_lookup_enabled,
    true,
    "supabase_template_lookup_enabled must be true"
  );
});

// ── 9. local_fetcher fallback fires when supabase_fetcher returns empty ───────

test("loadTemplateCandidates falls back to local_fetcher when supabase_fetcher returns no candidates", async () => {
  const local_template = {
    item_id:               "local-001",
    use_case:              "ownership_check",
    variant_group:         "Stage 1 — Ownership Confirmation",
    stage_label:           "Stage 1 — Ownership Confirmation",
    tone:                  "Warm",
    gender_variant:        "Neutral",
    language:              "English",
    sequence_position:     "1st Touch",
    paired_with_agent_type: "Warm Professional",
    text:                  "Local fallback body text {{first_name}}.",
    english_translation:   "Local fallback body text {{first_name}}.",
    active:                "Yes",
    is_first_touch:        "Yes",
    is_ownership_check:    "No",
    property_type_scope:   "Any Residential",
    category_primary:      "Any Residential",
    category_secondary:    "Outreach",
    deliverability_score:  90,
    spam_risk:             3,
    total_sends:           0,
    total_replies:         0,
    total_conversations:   0,
    source:                "local_registry",
  };

  const candidates = await loadTemplateCandidates({
    use_case:               "ownership_check",
    language:               "English",
    skip_render_validation: true,
    supabase_fetcher:       async () => [],
    remote_fetcher:         noRemoteFetch,
    local_fetcher:          () => [local_template],
  });

  assert.ok(candidates.length > 0, "must fall back to local template when Supabase returns empty");
  assert.ok(
    candidates.some((c) => c.item_id === "local-001"),
    "local_registry template must appear in the fallback candidate pool"
  );
});
