/**
 * stage1-template-resolver.test.mjs
 *
 * Verifies Stage 1 / first-touch / ownership-check template resolution using
 * the live Podio Templates app schema fields:
 *
 *   - use-case          (field external_id; options include "ownership_check",
 *                        "First Message")
 *   - is-ownership-check (Yes/No — replaces legacy is-first-touch in newer apps)
 *   - stage             (Variant Group external_id; e.g. "Stage 1 — Ownership
 *                        Confirmation", "Stage 1 — Identity / Trust")
 *   - property-type     (category value may include "Ownership Verification")
 *   - category          (secondary category; may include "Outbound Initial")
 *   - active            (Yes/No)
 *   - text              (Template Text; HTML-wrapped values tolerated)
 *   - language          (English / Spanish / …)
 *
 * These tests guard the resolver against schema drift where is-first-touch is
 * absent and use-case options differ from the internal canonical names.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { loadTemplateCandidates } from "@/lib/domain/templates/load-template.js";
import { isStage1Template } from "@/lib/domain/templates/template-selector.js";
import { normalizeTemplateTouchType, TEMPLATE_TOUCH_TYPES } from "@/lib/domain/templates/template-selector.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function noRemoteFetch() {
  return Promise.resolve([]);
}

function makeLocalFetcher(templates) {
  return () => templates;
}

/**
 * Build a minimal template that looks like a Podio-normalised object (the shape
 * returned by normalizeTemplateItem in templates.js).  Fields not supplied here
 * default to safe non-matching values.
 */
function makeTemplate(overrides = {}) {
  return {
    item_id: overrides.item_id ?? "t-test",
    active: overrides.active ?? "Yes",
    use_case: overrides.use_case ?? null,
    selector_use_case: overrides.selector_use_case ?? overrides.use_case ?? null,
    use_case_label: overrides.use_case_label ?? null,
    canonical_use_case: overrides.canonical_use_case ?? overrides.use_case ?? null,
    canonical_routing_slug: overrides.canonical_routing_slug ?? null,
    variant_group: overrides.variant_group ?? null,
    stage_label: overrides.stage_label ?? null,
    is_first_touch: overrides.is_first_touch ?? null,
    is_ownership_check: overrides.is_ownership_check ?? "No",
    property_type_scope: overrides.property_type_scope ?? "Any Residential",
    category_primary: overrides.category_primary ?? null,
    category_secondary: overrides.category_secondary ?? null,
    language: overrides.language ?? "English",
    sequence_position: overrides.sequence_position ?? null,
    text: overrides.text ?? "Hi {{seller_first_name}}, checking on {{property_address}}. Are you the owner?",
    tone: overrides.tone ?? "Warm",
    spam_risk: overrides.spam_risk ?? 5,
    deliverability_score: overrides.deliverability_score ?? 80,
    historical_reply_rate: overrides.historical_reply_rate ?? 20,
    total_sends: overrides.total_sends ?? 0,
    total_replies: overrides.total_replies ?? 0,
    total_conversations: overrides.total_conversations ?? 0,
    source: overrides.source ?? "podio",
    raw: overrides.raw ?? null,
  };
}

// ── isStage1Template unit tests ───────────────────────────────────────────────

test("isStage1Template: template with use-case=ownership_check resolves as Stage 1", () => {
  const tmpl = makeTemplate({ use_case: "ownership_check" });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: template with Is Ownership Check = Yes resolves as Stage 1", () => {
  const tmpl = makeTemplate({
    use_case: null,
    is_ownership_check: "Yes",
  });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: template with Variant Group = Stage 1 — Ownership Confirmation resolves as Stage 1", () => {
  const tmpl = makeTemplate({
    use_case: null,
    is_ownership_check: "No",
    variant_group: "Stage 1 — Ownership Confirmation",
  });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: template with Use Case = First Message resolves as Stage 1", () => {
  const tmpl = makeTemplate({
    use_case: "ownership_check", // canonical after alias mapping
    selector_use_case: "First Message",
    use_case_label: null,
    canonical_routing_slug: "First Message",
  });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: template with Variant Group = Stage 1 — Identity / Trust resolves as Stage 1", () => {
  const tmpl = makeTemplate({
    use_case: null,
    is_ownership_check: "No",
    variant_group: "Stage 1 — Identity / Trust",
  });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: follow-up Stage 1 variant is NOT a Stage 1 signal", () => {
  const tmpl = makeTemplate({
    use_case: null,
    is_ownership_check: "No",
    variant_group: "Stage 1 — Ownership Confirmation Follow-Up",
  });
  assert.equal(isStage1Template(tmpl), false);
});

test("isStage1Template: category = Ownership Verification resolves as Stage 1", () => {
  const tmpl = makeTemplate({
    use_case: null,
    is_ownership_check: "No",
    variant_group: null,
    property_type_scope: "Ownership Verification",
  });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: secondary category = Outbound Initial resolves as Stage 1", () => {
  const tmpl = makeTemplate({
    use_case: null,
    is_ownership_check: "No",
    variant_group: null,
    category_secondary: "Outbound Initial",
  });
  assert.equal(isStage1Template(tmpl), true);
});

test("isStage1Template: later-stage template is not a Stage 1 signal", () => {
  const tmpl = makeTemplate({
    use_case: "asking_price",
    is_ownership_check: "No",
    variant_group: "Stage 3 — Asking Price",
    category_secondary: "Asking Price",
  });
  assert.equal(isStage1Template(tmpl), false);
});

// ── normalizeTemplateTouchType with Stage 1 signals ──────────────────────────

test("normalizeTemplateTouchType: is_ownership_check=Yes → FIRST_TOUCH", () => {
  const tmpl = makeTemplate({ is_ownership_check: "Yes" });
  assert.equal(normalizeTemplateTouchType(tmpl), TEMPLATE_TOUCH_TYPES.FIRST_TOUCH);
});

test("normalizeTemplateTouchType: variant_group Stage 1 — Ownership Confirmation → FIRST_TOUCH", () => {
  const tmpl = makeTemplate({
    variant_group: "Stage 1 — Ownership Confirmation",
    is_first_touch: null,
    is_ownership_check: "No",
  });
  assert.equal(normalizeTemplateTouchType(tmpl), TEMPLATE_TOUCH_TYPES.FIRST_TOUCH);
});

test("normalizeTemplateTouchType: legacy is_first_touch=No is still respected even with Stage 1 variant", () => {
  // An explicitly-marked follow-up should NOT be overridden by the variant group.
  const tmpl = makeTemplate({
    variant_group: "Stage 1 — Ownership Confirmation Follow-Up",
    is_first_touch: "No",
    is_ownership_check: "No",
  });
  // is_first_touch=No wins before Stage 1 check
  assert.equal(normalizeTemplateTouchType(tmpl), TEMPLATE_TOUCH_TYPES.FOLLOW_UP);
});

// ── loadTemplateCandidates integration tests ──────────────────────────────────

test("loadTemplateCandidates resolves template with use-case=ownership_check (new schema)", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-oc-1",
    use_case: "ownership_check",
    selector_use_case: "ownership_check",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
    skip_render_validation: true,
  });
  assert.ok(candidates.length > 0, "must return at least one candidate");
  assert.equal(candidates[0].item_id, "podio-oc-1");
});

test("loadTemplateCandidates resolves template with Is Ownership Check = Yes", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-ioc-1",
    use_case: "ownership_check",
    is_ownership_check: "Yes",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
    skip_render_validation: true,
  });
  assert.ok(candidates.length > 0, "must return at least one candidate");
  assert.equal(candidates[0].item_id, "podio-ioc-1");
});

test("loadTemplateCandidates resolves template with Variant Group = Stage 1 — Ownership Confirmation", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-vg-1",
    use_case: "ownership_check",
    variant_group: "Stage 1 — Ownership Confirmation",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
    skip_render_validation: true,
  });
  assert.ok(candidates.length > 0, "must return at least one candidate");
  assert.equal(candidates[0].item_id, "podio-vg-1");
});

test("loadTemplateCandidates resolves template with Use Case = First Message (new Podio option)", async () => {
  // Simulate a template where the raw Podio use-case field has "First Message"
  // but the stage/variant group maps it to ownership_check.
  const tmpl = makeTemplate({
    item_id: "podio-fm-1",
    use_case: "ownership_check",           // canonical after alias mapping
    selector_use_case: "First Message",    // raw Podio value
    canonical_routing_slug: "First Message",
    variant_group: "Stage 1 — Ownership Confirmation",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
    skip_render_validation: true,
  });
  assert.ok(candidates.length > 0, "must resolve template with First Message use-case");
  assert.equal(candidates[0].item_id, "podio-fm-1");
});

test("loadTemplateCandidates rejects inactive template", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-inactive-1",
    use_case: "ownership_check",
    active: "No",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
  });
  assert.equal(candidates.length, 0, "inactive template must be rejected");
});

test("loadTemplateCandidates rejects template with missing Template Text", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-notext-1",
    use_case: "ownership_check",
    text: "",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
  });
  assert.equal(candidates.length, 0, "template with empty text must be rejected");
});

test("loadTemplateCandidates rejects HTML-only empty Template Text", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-htmlempty-1",
    use_case: "ownership_check",
    text: "<p></p>",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
  });
  assert.equal(candidates.length, 0, "HTML-only empty template text must be rejected");
});

test("loadTemplateCandidates: HTML-wrapped non-empty Template Text is accepted", async () => {
  const tmpl = makeTemplate({
    item_id: "podio-htmltext-1",
    use_case: "ownership_check",
    text: "<p>Hi {{seller_first_name}}, checking on {{property_address}}. Are you the owner?</p>",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [tmpl],
    local_fetcher: makeLocalFetcher([]),
    skip_render_validation: true,
  });
  assert.ok(candidates.length > 0, "HTML-wrapped non-empty template text must be accepted");
});

test("loadTemplateCandidates: non-English template ranked lower when English requested", async () => {
  const english_tmpl = makeTemplate({
    item_id: "podio-en-1",
    use_case: "ownership_check",
    language: "English",
  });
  const spanish_tmpl = makeTemplate({
    item_id: "podio-es-1",
    use_case: "ownership_check",
    language: "Spanish",
  });
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: async () => [spanish_tmpl, english_tmpl],
    local_fetcher: makeLocalFetcher([]),
    skip_render_validation: true,
  });
  assert.ok(candidates.length >= 2, "both templates must be returned as candidates");
  assert.equal(
    candidates[0].item_id,
    "podio-en-1",
    "English template must rank above Spanish when English is requested"
  );
});

test("loadTemplateCandidates (strict) throws NO_STAGE_1_TEMPLATE_FOUND with rich diagnostics when no match", async () => {
  await assert.rejects(
    () =>
      loadTemplateCandidates({
        use_case: "ownership_check",
        language: "English",
        strict_touch_one_podio_only: true,
        remote_fetcher: noRemoteFetch,
        local_fetcher: makeLocalFetcher([]),
      }),
    (err) => {
      assert.equal(err.code, "NO_STAGE_1_TEMPLATE_FOUND");
      assert.ok(err.diagnostics, "diagnostics object must be attached");
      const diag = err.diagnostics;
      assert.ok("stage1_extended_diagnostics" in diag, "stage1_extended_diagnostics must be present");
      const ext = diag.stage1_extended_diagnostics;
      assert.ok("templates_loaded_count" in ext, "templates_loaded_count must be present");
      assert.ok("active_templates_count" in ext, "active_templates_count must be present");
      assert.ok("language_candidate_count" in ext, "language_candidate_count must be present");
      assert.ok("stage_1_signal_candidate_count" in ext, "stage_1_signal_candidate_count must be present");
      assert.ok("ownership_check_candidate_count" in ext, "ownership_check_candidate_count must be present");
      assert.ok(Array.isArray(ext.first_10_template_ids), "first_10_template_ids must be an array");
      assert.ok(Array.isArray(ext.first_10_rejection_reasons), "first_10_rejection_reasons must be an array");
      return true;
    }
  );
});
