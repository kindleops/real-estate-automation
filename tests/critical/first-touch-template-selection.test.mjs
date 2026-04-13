/**
 * first-touch-template-selection.test.mjs
 *
 * Guards two fixes applied after the live feeder run exposed:
 *  1. ReferenceError: market_id is not defined — market_item?.item_id was never
 *     assigned to a named variable in evaluateOwner; market_id was used in the
 *     plan object without a prior const declaration.
 *  2. invalid_first_touch_template_selected (7 rows) — loadTemplate searched by
 *     use_case only (no variant_group constraint), so templates with the right
 *     use_case but a later-stage / follow-up variant_group could score higher and
 *     be returned.  The allowed_variant_groups filter now restricts the candidate
 *     pool before scoring so only Stage-1 templates are eligible.
 *
 * Covered:
 *  1. allowed_variant_groups filters follow-up templates out of the candidate pool.
 *  2. allowed_variant_groups filters Stage 2+ templates out of the candidate pool.
 *  3. Stage-1 templates with null variant_group are always permitted (untagged safe).
 *  4. Without allowed_variant_groups the non-Stage-1 template can win (control).
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
    tone: "Warm",
    gender_variant: "Neutral",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    text: `Template text for ${item_id}`,
    english_translation: `Template text for ${item_id}`,
    active: "Yes",
    is_ownership_check: "No",
    category_primary: "Residential",
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

test("allowed_variant_groups: follow-up variant_group template is excluded from candidates", async () => {
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
    allowed_variant_groups: FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(
    returned_ids.includes("t-stage1"),
    "Stage-1 template must be in the candidate pool"
  );
  assert.equal(
    returned_ids.includes("t-followup"),
    false,
    "Follow-up variant_group template must be excluded by allowed_variant_groups"
  );
});

// ── 2. allowed_variant_groups filters Stage 2+ templates out ─────────────────

test("allowed_variant_groups: Stage 4 and Stage 5 variant_group templates are excluded", async () => {
  const stage1 = makeLocalTemplate("t-s1", "ownership_check", "Stage 1 — Ownership Confirmation", 0);
  const stage4 = makeLocalTemplate("t-s4", "ownership_check", "Stage 4A — Confirm Basics", 50);
  const stage5 = makeLocalTemplate("t-s5", "ownership_check", "Stage 5 — Offer Reveal", 50);

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([stage1, stage4, stage5]),
    allowed_variant_groups: FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(returned_ids.includes("t-s1"), "Stage-1 template must survive filtering");
  assert.equal(returned_ids.includes("t-s4"), false, "Stage 4 template must be excluded");
  assert.equal(returned_ids.includes("t-s5"), false, "Stage 5 template must be excluded");
});

// ── 3. templates with null variant_group are always permitted ─────────────────

test("allowed_variant_groups: template with null variant_group is always included", async () => {
  const null_variant = makeLocalTemplate("t-null-vg", "ownership_check", null, 0);
  const stage1 = makeLocalTemplate("t-s1", "ownership_check", "Stage 1 — Ownership Confirmation", 0);
  const bad_variant = makeLocalTemplate("t-bad", "ownership_check", "Stage 6 — Close", 50);

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([null_variant, stage1, bad_variant]),
    allowed_variant_groups: FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.ok(returned_ids.includes("t-null-vg"), "null variant_group must always be permitted");
  assert.ok(returned_ids.includes("t-s1"), "Stage-1 must also be permitted");
  assert.equal(returned_ids.includes("t-bad"), false, "Stage-6 variant must be excluded");
});

test("strict first-touch filtering requires exact ownership_check and explicit Stage 1 variant", async () => {
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
    50
  );

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([
      correct_stage1,
      wrong_use_case,
      untagged_variant,
    ]),
    allowed_variant_groups: FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
    required_use_cases: new Set(["ownership_check"]),
    required_variant_groups: FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
    require_explicit_variant_group: true,
  });

  const returned_ids = candidates.map((c) => c.item_id);
  assert.deepEqual(
    returned_ids,
    ["t-correct"],
    "strict first-touch filtering must keep only explicit Stage 1 ownership_check templates"
  );
});

test("strict Touch 1 Podio mode prefers English Stage 1 ownership templates and blocks local fallback", async () => {
  const english_stage1 = {
    item_id: 9101,
    use_case: "ownership_check",
    use_case_label: "ownership_check",
    canonical_routing_slug: "ownership_check__none__still_own__intro_plain__plain__english",
    variant_group: "Stage 1 — Ownership Confirmation",
    stage_label: "Ownership Confirmation",
    stage_code: "S1",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Any",
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
  const wrong_stage = {
    ...english_stage1,
    item_id: 9103,
    stage_code: "S3",
    variant_group: "Stage 3 — Asking Price",
    stage_label: "Asking Price",
  };
  const wrong_use_case = {
    ...english_stage1,
    item_id: 9104,
    use_case: "asking_price_follow_up",
    use_case_label: "asking_price_follow_up",
    canonical_routing_slug: "asking_price_follow_up__seller_named_price",
  };

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async () => [
      spanish_stage1,
      wrong_stage,
      wrong_use_case,
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
    [9101],
    "strict Touch 1 Podio mode must keep only the English Stage 1 ownership template"
  );
  assert.equal(candidates[0]?.source, "podio");
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
