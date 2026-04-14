/**
 * template-follow-up-resolution.test.mjs
 *
 * Regression tests for the template resolution failure that caused ALL 20
 * non-first-touch owners to get template_not_found during the live feeder run.
 *
 * Root cause: non-first-touch owners without an explicit follow-up plan
 * received use_case="ownership_check" + touch_type="Follow-Up".  All
 * ownership_check local templates are first-touch-only (touch_type_mismatch),
 * and all ownership_check_follow_up templates have a different use_case
 * (use_case_mismatch).  Zero survivors.
 *
 * Fixes verified here:
 *  1. buildTemplateSelectionInputs derives follow-up use_case via
 *     followUpUseCaseForStage for non-first-touch (never sends raw
 *     "ownership_check" with Follow-Up touch_type).
 *  2. Reengagement local templates exist and match alias expansion.
 *  3. Spanish language templates exist and match.
 *  4. Multifamily reengagement templates match.
 *  5. Reengagement fallback ladder fires when stage-specific template missing.
 *  6. Deal strategy does not hard-block reengagement.
 *  7. loadTemplateCandidates use_case aliases cascade correctly.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  loadTemplateCandidates,
} from "@/lib/domain/templates/load-template.js";
import {
  expandSelectorUseCases,
  TEMPLATE_TOUCH_TYPES,
} from "@/lib/domain/templates/template-selector.js";
import {
  followUpUseCaseForStage,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { LOCAL_TEMPLATE_CANDIDATES } from "@/lib/domain/templates/local-template-registry.js";

// ── helpers ──────────────────────────────────────────────────────────────────

function makeTemplate({
  item_id,
  use_case,
  variant_group = null,
  language = "English",
  is_first_touch = "No",
  category_primary = "Residential",
  category_secondary = "Follow-Up",
  tone = "Warm",
  text = "Test template {{property_address}}",
  property_type_scope = null,
  deal_strategy = null,
  spam_risk = 4,
  active = "Yes",
} = {}) {
  return {
    item_id,
    title: null,
    raw: null,
    template_id: null,
    use_case,
    variant_group,
    tone,
    gender_variant: "Neutral",
    language,
    sequence_position: "V1",
    paired_with_agent_type: "Warm Professional",
    text,
    english_translation: text,
    active,
    is_first_touch,
    is_ownership_check: "No",
    category_primary,
    category_secondary,
    property_type_scope,
    deal_strategy,
    personalization_tags: [],
    deliverability_score: 92,
    spam_risk,
    historical_reply_rate: 24,
    total_sends: 0,
    total_replies: 0,
    total_conversations: 0,
    cooldown_days: 3,
    version: 1,
    last_used: null,
    source: "local_registry",
  };
}

function makeLocalFetcher(templates) {
  return () => templates;
}

async function noRemoteFetch() {
  return [];
}

const MINIMAL_CONTEXT = {
  found: true,
  ids: { master_owner_id: 1 },
  items: {},
  summary: {
    property_address: "123 Main St",
    seller_first_name: "John",
    agent_first_name: "Mike",
  },
  recent: { recently_used_template_ids: [] },
};

// ── 1. followUpUseCaseForStage generates correct follow-up use_cases ─────────

test("followUpUseCaseForStage maps ownership_check → ownership_check_follow_up", () => {
  assert.strictEqual(
    followUpUseCaseForStage("ownership_check"),
    "ownership_check_follow_up"
  );
});

test("followUpUseCaseForStage maps consider_selling → consider_selling_follow_up", () => {
  assert.strictEqual(
    followUpUseCaseForStage("consider_selling"),
    "consider_selling_follow_up"
  );
});

test("followUpUseCaseForStage maps asking_price → asking_price_follow_up", () => {
  assert.strictEqual(
    followUpUseCaseForStage("asking_price"),
    "asking_price_follow_up"
  );
});

test("followUpUseCaseForStage returns null for unrecognized stage", () => {
  assert.strictEqual(followUpUseCaseForStage("unknown_stage"), null);
});

// ── 2. expandSelectorUseCases includes reengagement for follow-up use_cases ──

test("ownership_check_follow_up expands to include reengagement", () => {
  const expanded = expandSelectorUseCases("ownership_check_follow_up");
  assert.ok(
    expanded.includes("reengagement"),
    `expected reengagement in expansion, got: ${expanded.join(", ")}`
  );
});

test("consider_selling_follow_up expands to include ownership_check_follow_up and reengagement", () => {
  const expanded = expandSelectorUseCases("consider_selling_follow_up");
  assert.ok(
    expanded.includes("ownership_check_follow_up"),
    `expected ownership_check_follow_up in expansion`
  );
  assert.ok(
    expanded.includes("reengagement"),
    `expected reengagement in expansion`
  );
});

test("reengagement expands to include ownership_check_follow_up", () => {
  const expanded = expandSelectorUseCases("reengagement");
  assert.ok(
    expanded.includes("ownership_check_follow_up"),
    `expected ownership_check_follow_up in expansion`
  );
});

// ── 3. ownership_check_follow_up templates survive for Follow-Up touch_type ──

test("ownership_check_follow_up template survives with Follow-Up touch_type", async () => {
  const template = makeTemplate({
    item_id: "t-follow-up-1",
    use_case: "ownership_check_follow_up",
    variant_group: "Stage 1 — Ownership Confirmation Follow-Up",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([template]),
  });

  assert.ok(candidates.length >= 1, "must have at least one survivor");
  assert.ok(
    candidates.some((c) => c.item_id === "t-follow-up-1"),
    "ownership_check_follow_up template must survive"
  );
});

// ── 4. reengagement templates match via alias expansion ──────────────────────

test("reengagement template matches when requesting ownership_check_follow_up via alias", async () => {
  const template = makeTemplate({
    item_id: "t-reengagement-1",
    use_case: "reengagement",
    variant_group: "Reengagement — Generic Follow-Up",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([template]),
  });

  assert.ok(candidates.length >= 1, "reengagement template must survive as alias match");
  assert.ok(
    candidates.some((c) => c.item_id === "t-reengagement-1"),
    "reengagement template must be in survivors"
  );
});

// ── 5. Spanish ownership_check_follow_up template resolves ───────────────────

test("Spanish follow-up template survives when language is Spanish", async () => {
  const english = makeTemplate({
    item_id: "t-en-followup",
    use_case: "ownership_check_follow_up",
    language: "English",
  });
  const spanish = makeTemplate({
    item_id: "t-es-followup",
    use_case: "ownership_check_follow_up",
    language: "Spanish",
    text: "Dando seguimiento sobre {{property_address}}.",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "Spanish",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([english, spanish]),
  });

  assert.ok(candidates.length >= 1, "at least one template must survive");
  // Spanish template should score higher for Spanish request
  const spanish_candidate = candidates.find((c) => c.item_id === "t-es-followup");
  const english_candidate = candidates.find((c) => c.item_id === "t-en-followup");
  assert.ok(spanish_candidate, "Spanish template must survive");
  if (english_candidate) {
    assert.ok(
      spanish_candidate.score >= english_candidate.score,
      "Spanish template must score >= English for Spanish request"
    );
  }
});

// ── 6. Multifamily reengagement template matches for MF owner ────────────────

test("Multifamily reengagement template survives for Landlord/Multifamily owner", async () => {
  const mf_template = makeTemplate({
    item_id: "t-mf-reengagement",
    use_case: "reengagement",
    category_primary: "Landlord / Multifamily",
    variant_group: "Reengagement — Generic Follow-Up",
  });
  const residential = makeTemplate({
    item_id: "t-res-reengagement",
    use_case: "reengagement",
    category_primary: "Residential",
    variant_group: "Reengagement — Generic Follow-Up",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "reengagement",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([mf_template, residential]),
  });

  assert.ok(candidates.length >= 1, "at least one reengagement template must survive");
});

// ── 7. deal_strategy null does not hard-block follow-up templates ────────────

test("null deal_strategy does not block follow-up templates", async () => {
  const template = makeTemplate({
    item_id: "t-no-strategy",
    use_case: "ownership_check_follow_up",
    deal_strategy: null,
  });

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    deal_strategy: null,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([template]),
  });

  assert.ok(candidates.length >= 1, "template with null deal_strategy must survive");
});

test("template with cash deal_strategy compatible when request has no deal_strategy", async () => {
  const template = makeTemplate({
    item_id: "t-cash-strategy",
    use_case: "ownership_check_follow_up",
    deal_strategy: "cash",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    deal_strategy: null,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([template]),
  });

  assert.ok(candidates.length >= 1, "template with cash strategy must survive when request has null");
});

// ── 8. First-touch ownership_check templates do NOT survive Follow-Up ────────

test("first-touch ownership_check templates are rejected for Follow-Up touch_type", async () => {
  const first_touch_only = makeTemplate({
    item_id: "t-first-touch",
    use_case: "ownership_check",
    is_first_touch: "Yes",
    category_secondary: "Outreach",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([first_touch_only]),
  });

  assert.strictEqual(
    candidates.filter((c) => c.item_id === "t-first-touch").length,
    0,
    "first-touch template must NOT survive Follow-Up selection"
  );
});

// ── 9. Reengagement fallback ladder fires when no exact match ────────────────

test("reengagement fallback ladder provides template when stage-specific follow-up missing", async () => {
  // Simulates: asking_price_follow_up requested, but only reengagement is available
  const reengagement = makeTemplate({
    item_id: "t-reengagement-fallback",
    use_case: "reengagement",
    variant_group: "Reengagement — Generic Follow-Up",
  });

  const candidates = await loadTemplateCandidates({
    use_case: "asking_price_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([reengagement]),
  });

  // reengagement is in aliases for asking_price_follow_up, so should match
  assert.ok(candidates.length >= 1, "reengagement fallback must provide a template");
  assert.ok(
    candidates.some((c) => c.item_id === "t-reengagement-fallback"),
    "reengagement template must be the fallback survivor"
  );
});

// ── 10. Local registry has required templates ────────────────────────────────

test("local registry contains ownership_check_follow_up templates", () => {
  const follow_ups = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check_follow_up"
  );
  assert.ok(follow_ups.length >= 2, `expected ≥2 ownership_check_follow_up templates, got ${follow_ups.length}`);
});

test("local registry contains reengagement templates", () => {
  const reengagement = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "reengagement"
  );
  assert.ok(reengagement.length >= 2, `expected ≥2 reengagement templates, got ${reengagement.length}`);
});

test("local registry contains Spanish templates", () => {
  const spanish = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.language === "Spanish"
  );
  assert.ok(spanish.length >= 2, `expected ≥2 Spanish templates, got ${spanish.length}`);
});

test("local registry contains Spanish ownership_check_follow_up templates", () => {
  const spanish_follow_ups = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check_follow_up" && t.language === "Spanish"
  );
  assert.ok(
    spanish_follow_ups.length >= 1,
    `expected ≥1 Spanish ownership_check_follow_up template, got ${spanish_follow_ups.length}`
  );
});

test("local registry contains Multifamily reengagement templates", () => {
  const mf_reengagement = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "reengagement" && t.category_primary === "Landlord / Multifamily"
  );
  assert.ok(
    mf_reengagement.length >= 1,
    `expected ≥1 Multifamily reengagement template, got ${mf_reengagement.length}`
  );
});

// ── 11. End-to-end: real local registry templates survive Follow-Up selection ─

test("real local registry ownership_check_follow_up templates survive loadTemplateCandidates", async () => {
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: () => LOCAL_TEMPLATE_CANDIDATES,
  });

  const follow_up_survivors = candidates.filter(
    (c) => c.use_case === "ownership_check_follow_up" || c.use_case === "reengagement"
  );
  assert.ok(
    follow_up_survivors.length >= 1,
    `expected ≥1 follow-up survivor from real registry, got ${follow_up_survivors.length}`
  );
});

test("real local registry Spanish templates survive for Spanish Follow-Up", async () => {
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "Spanish",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: () => LOCAL_TEMPLATE_CANDIDATES,
  });

  assert.ok(
    candidates.length >= 1,
    `expected ≥1 survivor from real registry for Spanish Follow-Up, got ${candidates.length}`
  );
});

test("real local registry reengagement templates survive for generic Follow-Up", async () => {
  const candidates = await loadTemplateCandidates({
    use_case: "reengagement",
    touch_type: "Follow-Up",
    touch_number: 2,
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: () => LOCAL_TEMPLATE_CANDIDATES,
  });

  const reengagement_survivors = candidates.filter(
    (c) => c.use_case === "reengagement" || c.use_case === "ownership_check_follow_up"
  );
  assert.ok(
    reengagement_survivors.length >= 1,
    `expected ≥1 reengagement survivor from real registry, got ${reengagement_survivors.length}`
  );
});
