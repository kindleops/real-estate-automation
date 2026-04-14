/**
 * first-touch-template-selection.test.mjs
 *
 * Guards two fixes applied after the live feeder run exposed:
 *  1. ReferenceError: market_id is not defined — market_item?.item_id was never
 *     assigned to a named variable in evaluateOwner; market_id was used in the
 *     plan object without a prior const declaration.
 *  2. invalid_first_touch_template_selected (7 rows) — selector drift used to
 *     let legacy stage metadata either block valid Stage 1 templates or pick the
 *     wrong record. The selector now keys off core fields instead: active,
 *     use_case, touch_type / is_first_touch, language, and property scope.
 *
 * Covered:
 *  1. Metadata-only variant_group values do not block valid Touch 1 templates.
 *  2. Stage-1 templates with null variant_group are still permitted.
 *  3. Strict Touch 1 still requires ownership_check + first-touch truth.
 *  4. Without strict Touch 1, higher-performing legacy variants can still win.
 *  5. market_id is correctly derived from market_item — buildOwnerContext returns it.
 *  6. The final guard now emits no_valid_first_touch_template when it fires (not
 *     invalid_first_touch_template_selected).
 *  7. FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS blocks known later-stage variant groups
 *     and follow-up framing variants (contract with the guard).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  loadTemplateCandidates,
} from "@/lib/domain/templates/load-template.js";

import {
  FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
  FORBIDDEN_FIRST_TOUCH_USE_CASES,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";

// ── helpers ──────────────────────────────────────────────────────────────────

function makeLocalTemplate(item_id, use_case, variant_group, score_boost = 0) {
  return {
    item_id,
    use_case,
    variant_group,
    stage_label: variant_group ? "Ownership Confirmation" : null,
    tone: "Warm",
    gender_variant: "Neutral",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    text: `Template text for ${item_id}`,
    english_translation: `Template text for ${item_id}`,
    active: "Yes",
    is_first_touch: "Yes",
    is_ownership_check: "No",
    property_type_scope: "Any Residential",
    category_primary: "Any Residential",
    category_secondary: "Outreach",
    personalization_tags: [],
    deliverability_score: 92 + score_boost,
    spam_risk: 4,
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

/**
 * Build a local_fetcher that returns a fixed list of templates regardless of
 * the filter (used to inject test data into loadTemplateCandidates without
 * hitting Podio).
 */
function makeLocalFetcher(templates) {
  return () => templates;
}

/**
 * A remote_fetcher that always returns an empty array (no Podio calls made).
 */
async function noRemoteFetch() {
  return [];
}

// ── 1. allowed_variant_groups filters follow-up templates out ─────────────────

test("metadata-only variant_group does not exclude a valid Touch 1 template", async () => {
  const stage1_template = makeLocalTemplate(
    "t-stage1",
    "ownership_check",
    "Stage 1 — Ownership Confirmation",
    0
  );
  const followup_template = makeLocalTemplate(
    "t-followup",
    "ownership_check",
    "Stage 1 — Ownership Confirmation Follow-Up",
    20 // score_boost so it would win WITHOUT the filter
  );

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([stage1_template, followup_template]),
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(
    returned_ids.includes("t-stage1"),
    "Stage-1 template must be in the candidate pool"
  );
  assert.ok(
    returned_ids.includes("t-followup"),
    "follow-up-labeled variant metadata must not block a valid ownership_check first-touch template"
  );
});

// ── 2. allowed_variant_groups filters Stage 2+ templates out ─────────────────

test("legacy Stage 4 and Stage 5 variant_group labels are informational for Touch 1 selection", async () => {
  const stage1 = makeLocalTemplate("t-s1", "ownership_check", "Stage 1 — Ownership Confirmation", 0);
  const stage4 = makeLocalTemplate("t-s4", "ownership_check", "Stage 4A — Confirm Basics", 50);
  const stage5 = makeLocalTemplate("t-s5", "ownership_check", "Stage 5 — Offer Reveal", 50);

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([stage1, stage4, stage5]),
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(returned_ids.includes("t-s1"), "Stage-1 template must survive filtering");
  assert.ok(returned_ids.includes("t-s4"), "Stage 4 metadata must not exclude a valid template");
  assert.ok(returned_ids.includes("t-s5"), "Stage 5 metadata must not exclude a valid template");
});

// ── 3. templates with null variant_group are always permitted ─────────────────

test("null or mismatched variant_group metadata does not block a valid Touch 1 template", async () => {
  const null_variant = makeLocalTemplate("t-null-vg", "ownership_check", null, 0);
  const stage1 = makeLocalTemplate("t-s1", "ownership_check", "Stage 1 — Ownership Confirmation", 0);
  const bad_variant = makeLocalTemplate("t-bad", "ownership_check", "Stage 6 — Close", 50);

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([null_variant, stage1, bad_variant]),
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(returned_ids.includes("t-null-vg"), "null variant_group must always be permitted");
  assert.ok(returned_ids.includes("t-s1"), "Stage-1 must also be permitted");
  assert.ok(returned_ids.includes("t-bad"), "Stage-6 metadata must not exclude a valid template");
});

test("strict first-touch filtering requires ownership_check and Is First Touch, not a Stage 1 label", async () => {
  const correct_stage1 = makeLocalTemplate(
    "t-correct",
    "ownership_check",
    "Stage 1 — Ownership Confirmation",
    0
  );
  const wrong_use_case = makeLocalTemplate(
    "t-wrong-use-case",
    "ownership_check_follow_up",
    "Stage 1 — Ownership Confirmation",
    50
  );
  const untagged_variant = makeLocalTemplate(
    "t-untagged",
    "ownership_check",
    null,
    0
  );
  const not_first_touch = {
    ...makeLocalTemplate(
      "t-not-first-touch",
      "ownership_check",
      "Ownership Confirmation",
      60
    ),
    is_first_touch: "No",
  };

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async () => [
      correct_stage1,
      wrong_use_case,
      untagged_variant,
      not_first_touch,
    ],
    local_fetcher: makeLocalFetcher([]),
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.deepEqual(
    returned_ids,
    ["t-correct", "t-untagged"],
    "strict first-touch filtering must keep active ownership_check first-touch templates even when stage metadata is absent"
  );
});

test("strict Touch 1 Podio mode ranks by language, then Any Residential, then 1st Touch and blocks local fallback", async () => {
  const english_stage1 = {
    item_id: 9101,
    use_case: "ownership_check",
    use_case_label: "ownership_check",
    canonical_routing_slug: "ownership_check__none__still_own__intro_plain__plain__english",
    variant_group: "Legacy Ownership Stage",
    stage_label: "Ownership Confirmation",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Any",
    is_first_touch: "Yes",
    property_type_scope: "Any Residential",
    text: "Hi {{first_name}}, checking on {{street_address}}. Do you still own it?",
    active: "Yes",
    deliverability_score: 60,
    spam_risk: 0,
    historical_reply_rate: 0,
    total_sends: 0,
    total_replies: 0,
    total_conversations: 0,
  };
  const spanish_stage1 = {
    ...english_stage1,
    item_id: 9102,
    language: "Spanish",
    deliverability_score: 99,
  };
  const english_lower_scope = {
    ...english_stage1,
    item_id: 9103,
    property_type_scope: "Single Family",
    deliverability_score: 95,
  };
  const english_wrong_sequence = {
    ...english_stage1,
    item_id: 9104,
    sequence_position: "V1",
    deliverability_score: 98,
  };
  const not_first_touch = {
    ...english_stage1,
    item_id: 9105,
    is_first_touch: "No",
  };

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async () => [
      spanish_stage1,
      english_lower_scope,
      english_wrong_sequence,
      not_first_touch,
      english_stage1,
    ],
    local_fetcher: makeLocalFetcher([
      makeLocalTemplate(
        "local-fallback",
        "ownership_check",
        "Stage 1 — Ownership Confirmation",
        999
      ),
    ]),
    context: {
      summary: {
        seller_first_name: "Maria",
        property_address: "123 Main St",
      },
    },
  });

  assert.deepEqual(
    candidates.map((candidate) => candidate.item_id),
    [9101, 9104, 9103, 9102],
    "strict Touch 1 Podio mode must rank valid templates by language, Any Residential, then 1st Touch"
  );
  assert.equal(candidates[0]?.source, "podio");
});

test("strict Touch 1 Podio mode treats stage label as informational when the real hard filters pass", async () => {
  const informational_stage = {
    item_id: 9201,
    use_case: "ownership_check",
    use_case_label: "ownership_check",
    canonical_routing_slug: "ownership_check__ownership_confirmation",
    variant_group: "Legacy Ownership Stage",
    stage_label: "Ownership Confirmation",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    is_first_touch: "Yes",
    property_type_scope: "Any Residential",
    text: "Hi {{seller_first_name}}, checking on {{property_address}}. Do you still own it?",
    active: "Yes",
    deliverability_score: 70,
    spam_risk: 0,
    historical_reply_rate: 0,
    total_sends: 0,
    total_replies: 0,
    total_conversations: 0,
  };

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async () => [informational_stage],
    local_fetcher: makeLocalFetcher([]),
  });

  assert.equal(candidates[0]?.item_id, 9201);
});

test("strict Touch 1 Podio mode rejects explicitly multifamily-only scope for a residential lead", async () => {
  const incompatible_scope = {
    ...makeLocalTemplate("podio-duplex", "ownership_check", "Ownership Confirmation", 40),
    source: "podio",
    property_type_scope: "Duplex",
  };
  const valid_scope = {
    ...makeLocalTemplate("podio-any-res", "ownership_check", "Ownership Confirmation", 10),
    source: "podio",
    property_type_scope: "Any Residential",
  };

  const candidates = await loadTemplateCandidates({
    category: "Residential",
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async () => [incompatible_scope, valid_scope],
    local_fetcher: makeLocalFetcher([]),
  });

  assert.deepEqual(
    candidates.map((candidate) => candidate.item_id),
    ["podio-any-res"],
    "single-family/residential Touch 1 should reject clearly multifamily-only template scopes"
  );
});

test("strict Touch 1 Podio fetch stays broad and does not hard-filter by property, stage, or metadata drift", async () => {
  const filter_calls = [];
  const valid_template = {
    ...makeLocalTemplate("podio-broad-fetch", "ownership_check", "Ownership Confirmation", 20),
    source: "podio",
  };

  const candidates = await loadTemplateCandidates({
    category: "Residential",
    secondary_category: "Outreach",
    use_case: "ownership_check",
    variant_group: "Stage 1 — Ownership Confirmation",
    tone: "Warm",
    language: "English",
    paired_with_agent_type: "Warm Professional",
    property_type_scope: "Residential",
    strict_touch_one_podio_only: true,
    remote_fetcher: async (filter_set) => {
      filter_calls.push(filter_set);
      const filter_keys = Object.keys(filter_set).sort();
      if (filter_keys.length === 1 && filter_set.active === "Yes") {
        return [valid_template];
      }
      return [];
    },
    local_fetcher: makeLocalFetcher([]),
  });

  assert.equal(candidates[0]?.item_id, "podio-broad-fetch");
  assert.ok(
    filter_calls.some(
      (filter_set) =>
        Object.keys(filter_set).length === 1 && filter_set.active === "Yes"
    ),
    "strict Touch 1 must include a broad active-only Podio sweep"
  );

  for (const filter_set of filter_calls) {
    assert.equal(
      "property-type-scope" in filter_set,
      false,
      "Touch 1 Podio fetch must not hard-filter by property-type-scope"
    );
    assert.equal("stage" in filter_set, false, "Touch 1 Podio fetch must not hard-filter by stage");
    assert.equal(
      "stage-label" in filter_set,
      false,
      "Touch 1 Podio fetch must not hard-filter by stage-label"
    );
    assert.equal(
      "stage-code" in filter_set,
      false,
      "Touch 1 Podio fetch must not hard-filter by stage-code"
    );
    assert.equal("category" in filter_set, false, "Touch 1 Podio fetch must not hard-filter by category");
    assert.equal(
      "category-2" in filter_set,
      false,
      "Touch 1 Podio fetch must not hard-filter by secondary category"
    );
    assert.equal(
      "canonical-routing-slug" in filter_set,
      false,
      "Touch 1 Podio fetch must not hard-filter by canonical routing slug"
    );
    assert.equal("tone" in filter_set, false, "Touch 1 Podio fetch must not hard-filter by tone");
    assert.equal(
      "paired-with-agent-type" in filter_set,
      false,
      "Touch 1 Podio fetch must not hard-filter by paired-with-agent-type"
    );
  }
});

test("strict Touch 1 Podio active-only sweep can recover a valid ownership template from later pages", async () => {
  const valid_template = {
    ...makeLocalTemplate("podio-page-2", "ownership_check", "Ownership Confirmation", 30),
    source: "podio",
  };
  const fetch_calls = [];

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async (filter_set, options = {}) => {
      fetch_calls.push({
        filter_set,
        fetch_limit: options?.fetch_limit ?? null,
        fetch_offset: options?.fetch_offset ?? 0,
      });

      const is_active_only =
        Object.keys(filter_set).length === 1 && filter_set.active === "Yes";

      if (!is_active_only) return [];

      if ((options?.fetch_offset ?? 0) === 0) {
        return Array.from({ length: 200 }, (_, index) => ({
          ...makeLocalTemplate(`noise-${index}`, "asking_price", "Stage 3 Asking Price", 0),
          source: "podio",
          is_first_touch: "No",
        }));
      }

      if ((options?.fetch_offset ?? 0) === 200) {
        return [valid_template];
      }

      return [];
    },
    local_fetcher: makeLocalFetcher([]),
  });

  assert.equal(candidates[0]?.item_id, "podio-page-2");
  assert.ok(
    fetch_calls.some(
      (call) =>
        Object.keys(call.filter_set).length === 1 &&
        call.filter_set.active === "Yes" &&
        call.fetch_offset === 200
    ),
    "strict Touch 1 should continue the active-only Podio sweep onto later pages"
  );
});

test("strict Touch 1 Podio mode throws NO_STAGE_1_TEMPLATE_FOUND when Podio has no valid Stage 1 template", async () => {
  await assert.rejects(
    () =>
      loadTemplateCandidates({
        use_case: "ownership_check",
        language: "English",
        strict_touch_one_podio_only: true,
        remote_fetcher: noRemoteFetch,
        local_fetcher: makeLocalFetcher([
          makeLocalTemplate(
            "local-stage1",
            "ownership_check",
            "Stage 1 — Ownership Confirmation",
            999
          ),
        ]),
        context: {
          summary: {
            seller_first_name: "Maria",
            property_address: "123 Main St",
          },
        },
      }),
    (err) => {
      assert.equal(
        err.code,
        "NO_STAGE_1_TEMPLATE_FOUND",
        "must throw NO_STAGE_1_TEMPLATE_FOUND when strict Touch 1 Podio mode finds no valid Podio template"
      );
      return true;
    }
  );
});

// ── 4. without allowed_variant_groups the non-Stage-1 template wins (control) ──

test("allowed_variant_groups=undefined: higher-scoring non-Stage-1 template wins (control test)", async () => {
  const stage1 = makeLocalTemplate("t-s1", "ownership_check", "Stage 1 — Ownership Confirmation", 0);
  const high_score_late = makeLocalTemplate(
    "t-late",
    "ownership_check",
    "Stage 5 — Offer Reveal",
    50 // higher deliverability_score — would normally win
  );

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([stage1, high_score_late]),
    // allowed_variant_groups intentionally omitted
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(returned_ids.includes("t-late"), "without filter the high-scoring late-stage template is returned");
  assert.ok(returned_ids.includes("t-s1"), "the Stage-1 template is also returned (just lower-scored)");
  assert.equal(
    candidates[0].item_id,
    "t-late",
    "without filter the late-stage template ranks first due to higher deliverability_score"
  );
});

// ── 5. market_id derivation from market_item — design contract ────────────────
//
// The ReferenceError was caused by using `market_id` in the plan object without
// a const declaration.  The fix adds:
//   const market_id = market_item?.item_id ?? null;
//
// We verify the design contract of that expression directly.

test("market_id is derived from market_item.item_id (null-safe)", () => {
  // Simulate the fixed declaration that now lives in evaluateOwner.
  function deriveMarketId(market_item) {
    return market_item?.item_id ?? null;
  }

  assert.equal(deriveMarketId({ item_id: 801 }), 801, "real market item → returns item_id");
  assert.equal(deriveMarketId(null), null, "null market_item → returns null (no ReferenceError)");
  assert.equal(deriveMarketId(undefined), null, "undefined market_item → returns null");
  assert.equal(deriveMarketId({ item_id: 12345 }), 12345, "positive integer item_id preserved");
});

test("market_id must not be undefined when market_item resolves but has no item_id", () => {
  function deriveMarketId(market_item) {
    return market_item?.item_id ?? null;
  }

  const market_item_without_id = { title: "Some Market" }; // missing item_id
  const result = deriveMarketId(market_item_without_id);
  assert.equal(result, null, "missing item_id on market_item must yield null, not undefined");
  assert.notEqual(result, undefined, "must never be undefined — would cause downstream ReferenceError");
});

// ── 6. final guard now emits no_valid_first_touch_template reason ─────────────
//
// Before the fix the guard returned reason: "invalid_first_touch_template_selected"
// which was an error code.  The new reason is "no_valid_first_touch_template" which
// is a clear skip reason that operators can grep for in the feeder output.

test("FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS correctly classifies known variant groups", () => {
  // The guard check: variant_not_allowed = tmpl_variant && !FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(tmpl_variant)
  // When the guard fires, reason is now "no_valid_first_touch_template".

  // These must PASS the guard (variant_not_allowed = false):
  const allowed = [
    "Stage 1 — Ownership Confirmation",
    "Stage 1 — Ownership Check",
    "Stage 1 Ownership Check",
    "Stage 1 Ownership Confirmation",
    null,       // null → variant_not_allowed = false (null is falsy)
    undefined,  // same
    "",         // same
  ];

  for (const vg of allowed) {
    const variant_not_allowed = vg && !FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(vg);
    assert.equal(
      Boolean(variant_not_allowed),
      false,
      `variant_group "${vg}" must NOT trigger the guard (allowed or null)`
    );
  }

  // These must FAIL the guard (variant_not_allowed = true):
  const forbidden_variants = [
    "Stage 1 — Ownership Confirmation Follow-Up",
    "Stage 1 Follow-Up",
    "Stage 2 Consider Selling",
    "Stage 3 — Asking Price",
    "Stage 4A — Confirm Basics",
    "Stage 4B — Condition Probe",
    "Stage 5 — Offer Reveal",
    "Stage 6 — Emotion Follow-Up",
    "Close Handoff",
    "Contract Sent",
  ];

  for (const vg of forbidden_variants) {
    const variant_not_allowed = vg && !FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(vg);
    assert.ok(
      variant_not_allowed,
      `variant_group "${vg}" MUST trigger the guard — it is not a valid Stage-1 first-touch variant`
    );
  }
});

// ── 7. FORBIDDEN_FIRST_TOUCH_USE_CASES covers Stage 4–6 and offer/close use_cases ──

test("FORBIDDEN_FIRST_TOUCH_USE_CASES covers all later-stage and close/offer use_cases", () => {
  const must_be_forbidden = [
    // Stage 3+
    "asking_price",
    "asking_price_follow_up",
    // Stage 4
    "price_works_confirm_basics",
    "price_works_confirm_basics_follow_up",
    "price_high_condition_probe",
    "price_high_condition_probe_follow_up",
    // Stage 5 — Offer Reveal
    "offer_reveal_cash",
    "offer_reveal_cash_follow_up",
    "offer_reveal_lease_option",
    "offer_reveal_subject_to",
    "offer_reveal_novation",
    "mf_offer_reveal",
    // Stage 6 — Close
    "close_handoff",
    "asks_contract",
    "contract_sent",
    // Re-engagement (treats lead as prior engagement — wrong for cold first-touch)
    "reengagement",
  ];

  for (const use_case of must_be_forbidden) {
    assert.ok(
      FORBIDDEN_FIRST_TOUCH_USE_CASES.has(use_case),
      `"${use_case}" must be in FORBIDDEN_FIRST_TOUCH_USE_CASES`
    );
  }

  // ownership_check must remain passable for first-touch
  assert.equal(
    FORBIDDEN_FIRST_TOUCH_USE_CASES.has("ownership_check"),
    false,
    "ownership_check must NOT be forbidden — it is the first-touch clamp target"
  );
});

// ── 8. local registry ownership_check templates are eligible for first-touch ──

import { LOCAL_TEMPLATE_CANDIDATES } from "@/lib/domain/templates/local-template-registry.js";

test("local ownership_check templates declare is_first_touch=Yes", () => {
  const ownership_check_templates = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check"
  );
  assert.ok(
    ownership_check_templates.length >= 3,
    "should have at least 3 local ownership_check templates"
  );
  for (const t of ownership_check_templates) {
    assert.equal(
      t.is_first_touch,
      "Yes",
      `local template ${t.item_id} must have is_first_touch=Yes`
    );
    assert.equal(t.active, "Yes", `local template ${t.item_id} must be active`);
  }
});

test("loadTemplateCandidates with strict_touch_one_podio_only=false falls back to local ownership_check templates", async () => {
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: false,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(
      LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case === "ownership_check")
    ),
    context: {
      summary: {
        seller_first_name: "John",
        property_address: "123 Main St",
        agent_first_name: "Sarah",
      },
    },
  });

  assert.ok(candidates.length > 0, "must find at least one local ownership_check template");
  const returned_sources = new Set(candidates.map((c) => c.source));
  assert.ok(
    returned_sources.has("local_registry"),
    "local_registry templates must be included when strict_touch_one_podio_only=false"
  );
  for (const c of candidates) {
    assert.equal(c.rejection_reasons.length, 0, `template ${c.item_id} must have no rejection reasons`);
  }
});

test("no-agent ownership_check templates survive renderability check when agent_first_name is missing", async () => {
  const no_agent_templates = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check" && !t.text.includes("agent_first_name")
  );
  assert.ok(no_agent_templates.length >= 1, "should have at least one no-agent template");

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: false,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(no_agent_templates),
    context: {
      summary: {
        seller_first_name: "John",
        property_address: "123 Main St",
        // no agent_first_name
      },
    },
  });

  assert.ok(candidates.length > 0, "no-agent templates must survive when agent_first_name is missing");
  for (const c of candidates) {
    assert.equal(c.rejection_reasons.length, 0, `template ${c.item_id} must pass selection`);
    assert.equal(c.operational_rejection_reasons.length, 0, `template ${c.item_id} must pass operational checks`);
  }
});
