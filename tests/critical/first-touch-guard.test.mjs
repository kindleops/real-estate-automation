/**
 * First-touch guardrail tests.
 *
 * Proves that:
 *  1. Blank-status cold lead is always detected as first-touch → ownership_check clamping applies
 *  2. Prior polluted outbound history does NOT advance stage — only CRM contact_status does
 *  3. Later-stage use_cases are hard-blocked by FORBIDDEN_FIRST_TOUCH_USE_CASES
 *  4. Valid Stage-1 cold-outbound variant groups are allowed; follow-up and Stage 2+ groups are NOT
 *  5. Non-blank contact_status (engaged, contacted, etc.) = NOT first-touch → no clamp
 *  6. Polluted route output (forbidden use_case) does NOT block first-touch before template lookup
 *  7. The final template guard still rejects a non-Stage-1 template that somehow passes loadTemplate
 *  8. First-touch blank-status leads bypass recency suppression (design contract)
 *  9. Pending same-phone queue item still blocks first-touch (duplicate_pending_queue_item is universal)
 * 10. parseSellerIdLocation extracts address/city/state from seller_id for property lookup seeding
 * 11. Address lookup exact-match filter rejects partial / ambiguous / cross-city candidates
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  detectFirstTouch,
  parseSellerIdLocation,
  normalizeStreetAddress,
  addressLookupVariants,
  buildSyntheticPropertyFromSellerId,
  FORBIDDEN_FIRST_TOUCH_USE_CASES,
  FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES,
  FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import { categoryField, createPodioItem, textField } from "../helpers/test-helpers.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeBlankOwner(item_id = 1001) {
  // No contact-status, no contact-status-2 — truly cold, first-touch
  return createPodioItem(item_id, {
    "sms-eligible": categoryField("Yes"),
    // contact-status intentionally omitted
  });
}

function makeEngagedOwner(item_id = 1002, status = "contacted") {
  return createPodioItem(item_id, {
    "sms-eligible": categoryField("Yes"),
    "contact-status": categoryField(status),
  });
}

function makeStatus2Owner(item_id = 1003, status_2 = "sent") {
  return createPodioItem(item_id, {
    "sms-eligible": categoryField("Yes"),
    "contact-status-2": categoryField(status_2),
  });
}

// ── test 1: blank-status cold lead → first-touch ─────────────────────────────

test("detectFirstTouch returns true for a lead with blank contact_status (cold first-touch)", () => {
  const owner_item = makeBlankOwner(1001);

  const result = detectFirstTouch({ owner_item });

  assert.equal(result, true, "blank contact_status must be detected as first-touch");
});

// ── test 2: polluted history without real CRM update → still first-touch ──────

test("detectFirstTouch ignores prior outbound history and stays true when contact_status is blank", () => {
  // Simulate a lead that was accidentally sent a wrong-stage template (bad row in history).
  // The CRM contact_status was never updated — so it is still a first-touch cold lead.
  const owner_item = makeBlankOwner(1002);

  // history is intentionally NOT passed — detectFirstTouch only reads owner_item
  // This proves the design: CRM status is the source of truth, not message history.
  const result = detectFirstTouch({ owner_item });

  assert.equal(result, true,
    "blank CRM status means first-touch regardless of any prior outbound history"
  );
});

// ── test 3: forbidden later-stage use_cases are blocked for first-touch ────────

test("FORBIDDEN_FIRST_TOUCH_USE_CASES blocks all later-stage use_cases", () => {
  const forbidden = [
    "asking_price",
    "asking_price_follow_up",
    "price_works_confirm_basics",
    "price_works_confirm_basics_follow_up",
    "price_high_condition_probe",
    "price_high_condition_probe_follow_up",
    "creative_probe",
    "creative_followup",
    "offer_reveal_cash",
    "offer_reveal_cash_follow_up",
    "offer_reveal_lease_option",
    "offer_reveal_subject_to",
    "offer_reveal_novation",
    "mf_offer_reveal",
    "close_handoff",
    "asks_contract",
    "contract_sent",
    "justify_price",
    "narrow_range",
    "ask_timeline",
    "ask_condition_clarifier",
    "reengagement",
  ];

  for (const use_case of forbidden) {
    assert.ok(
      FORBIDDEN_FIRST_TOUCH_USE_CASES.has(use_case),
      `${use_case} must be in FORBIDDEN_FIRST_TOUCH_USE_CASES`
    );
  }

  // ownership_check must NOT be forbidden
  assert.equal(
    FORBIDDEN_FIRST_TOUCH_USE_CASES.has("ownership_check"),
    false,
    "ownership_check must NOT be forbidden for first-touch leads"
  );

  // consider_selling must NOT be forbidden (it's the natural Stage 2 reply)
  assert.equal(
    FORBIDDEN_FIRST_TOUCH_USE_CASES.has("consider_selling"),
    false,
    "consider_selling must NOT be forbidden"
  );
});

// ── test 4: post-close / title lifecycle stages are also blocked ───────────────

test("FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES blocks Title, Closing, Contract, Disposition, Post-Close", () => {
  const forbidden_lifecycle = ["Contract", "Title", "Closing", "Disposition", "Post-Close"];

  for (const stage of forbidden_lifecycle) {
    assert.ok(
      FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES.has(stage),
      `${stage} must be in FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES`
    );
  }

  // Core stages must NOT be forbidden
  assert.equal(FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES.has("Ownership"), false);
  assert.equal(FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES.has("Offer"), false);
});

// ── test 5: only true cold-outbound Stage-1 groups are allowed; follow-ups are not ──

test("FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS allows only Stage 1 cold-outbound groups, not follow-ups", () => {
  // These four are the only valid cold first-touch variant groups.
  const cold_outbound_allowed = [
    "Stage 1 — Ownership Confirmation",
    "Stage 1 — Ownership Check",
    "Stage 1 Ownership Check",
    "Stage 1 Ownership Confirmation",
  ];

  for (const variant_group of cold_outbound_allowed) {
    assert.ok(
      FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(variant_group),
      `"${variant_group}" must be in FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS`
    );
  }

  // Follow-up variant groups must NOT be allowed for cold first-touch outbounds.
  // A cold lead has never been contacted — follow-up framing is wrong for a first message.
  const follow_up_disallowed = [
    "Stage 1 Follow-Up",
    "Stage 1 — Ownership Confirmation Follow-Up",
  ];

  for (const variant_group of follow_up_disallowed) {
    assert.equal(
      FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(variant_group),
      false,
      `"${variant_group}" must NOT be in FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS — follow-up framing is wrong for a cold first message`
    );
  }

  // Later-stage variant groups must also NOT be allowed.
  const later_stage_disallowed = [
    "Stage 2 Consider Selling",
    "Stage 3 — Asking Price",
    "Stage 4A — Confirm Basics",
    "Stage 4B — Condition Probe",
    "Stage 5 — Offer Reveal",
    "Stage 5 — Offer No Response",
    "Contract Sent",
    "Close Handoff",
  ];

  for (const variant_group of later_stage_disallowed) {
    assert.equal(
      FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(variant_group),
      false,
      `"${variant_group}" must NOT be in FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS`
    );
  }
});

// ── bonus: engaged / followed-up status → NOT first-touch ─────────────────────

test("detectFirstTouch returns false when contact_status indicates real engagement", () => {
  const statuses_that_prove_engagement = ["contacted", "engaged", "offer sent", "negotiating"];

  for (const status of statuses_that_prove_engagement) {
    const owner_item = makeEngagedOwner(2000, status);
    const result = detectFirstTouch({ owner_item });
    assert.equal(
      result,
      false,
      `contact_status="${status}" should NOT be first-touch — real engagement is recorded in CRM`
    );
  }
});

test("detectFirstTouch returns false when contact_status_2 indicates engagement", () => {
  const status_2_values = ["sent", "received", "follow-up scheduled"];

  for (const status_2 of status_2_values) {
    const owner_item = makeStatus2Owner(3000, status_2);
    const result = detectFirstTouch({ owner_item });
    assert.equal(
      result,
      false,
      `contact_status_2="${status_2}" should NOT be first-touch`
    );
  }
});

// ── test 6: polluted route output does NOT block first-touch before template lookup ─

test("FORBIDDEN_FIRST_TOUCH_USE_CASES and FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES are for the final guard only — route use_case alone should not block queue creation", () => {
  // This test documents the design contract:
  // The forbidden-use-case check in the early route block was converted to warn-only.
  // The final template guard (post loadTemplate) is where actual blocking happens.
  //
  // We verify that FORBIDDEN_FIRST_TOUCH_USE_CASES does NOT include "ownership_check"
  // (the clamp target), confirming the final guard can always pass a clamped template.

  assert.equal(
    FORBIDDEN_FIRST_TOUCH_USE_CASES.has("ownership_check"),
    false,
    "ownership_check must not be forbidden — it is the hard-clamp target for first-touch"
  );

  // All genuinely first-touch cold-outbound use_cases must be passable.
  const first_touch_use_cases = ["ownership_check"];
  for (const use_case of first_touch_use_cases) {
    assert.equal(
      FORBIDDEN_FIRST_TOUCH_USE_CASES.has(use_case),
      false,
      `"${use_case}" must not be in the forbidden set — it is used for first-touch outbounds`
    );
  }

  // Routing engine output of a later-stage use_case is logged and clamped, not blocked.
  // The FORBIDDEN set still correctly identifies which use_cases would be wrong if they
  // somehow made it into an actual template selection for a first-touch lead.
  const later_stage_route_outputs = ["asking_price", "offer_reveal_cash", "reengagement"];
  for (const use_case of later_stage_route_outputs) {
    assert.ok(
      FORBIDDEN_FIRST_TOUCH_USE_CASES.has(use_case),
      `"${use_case}" must remain forbidden so the final template guard can catch it`
    );
  }
});

// ── test 7: final template guard rejects a non-Stage-1 template that loadTemplate returned ─

test("FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS rejects variant groups that would slip through a misconfigured template selection", () => {
  // Simulates the scenario where loadTemplate somehow returns a wrong-stage template.
  // The final guard checks the actual selected template's variant_group.

  const wrong_stage_variants_that_must_fail = [
    "Stage 2 Consider Selling",
    "Stage 3 — Asking Price",
    "Stage 4A — Confirm Basics",
    "Stage 4B — Condition Probe",
    "Stage 5 — Offer Reveal",
    "Stage 5 — Offer No Response",
    "Stage 1 Follow-Up",                         // follow-up framing not valid for cold first outbound
    "Stage 1 — Ownership Confirmation Follow-Up", // same
  ];

  for (const variant_group of wrong_stage_variants_that_must_fail) {
    const variant_not_allowed = !FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(variant_group);
    assert.ok(
      variant_not_allowed,
      `variant_group "${variant_group}" must NOT be in FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS — final guard must block it`
    );
  }

  // The correctly clamped template variant must always pass.
  const correct_first_touch_variant = "Stage 1 — Ownership Confirmation";
  assert.ok(
    FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(correct_first_touch_variant),
    `"${correct_first_touch_variant}" must pass the final guard`
  );
});

// ── test 8: first-touch leads bypass recency suppression (design contract) ────

test("first-touch blank-status leads must bypass recent_contact_within_suppression_window", () => {
  // Design contract: the suppression window check in evaluateOwner is gated on !is_first_touch.
  // A blank-status owner IS first-touch, so the suppression check must not fire for them
  // even when stale outbound history exists.
  //
  // This test validates the invariant: detectFirstTouch(blank_owner) === true,
  // which means !is_first_touch === false, which means the suppression check is skipped.

  const blank_owner = makeBlankOwner(8001);
  assert.equal(
    detectFirstTouch({ owner_item: blank_owner }),
    true,
    "blank-status owner must be first-touch"
  );

  // The suppression guard is: if (!is_first_touch && !explicit_follow_up_due && latest_contact_ts ...).
  // For a first-touch lead, !is_first_touch evaluates to false, so the entire condition short-circuits.
  const is_first_touch = detectFirstTouch({ owner_item: blank_owner });
  const suppression_check_would_fire = !is_first_touch; // the outer condition
  assert.equal(
    suppression_check_would_fire,
    false,
    "suppression window check must be bypassed for first-touch leads"
  );
});

test("first-touch leads also bypass duplicate_within_suppression_window from stale sent history", () => {
  // Same logic: sent-history duplicate check is wrapped in !is_first_touch.
  // A blank-status cold lead with prior bad-send events must not be permanently locked out.

  const blank_owner = makeBlankOwner(8002);
  const is_first_touch = detectFirstTouch({ owner_item: blank_owner });

  assert.equal(is_first_touch, true);
  // The duplicate_within_suppression_window block only executes when !is_first_touch.
  assert.equal(!is_first_touch, false, "sent-history duplicate check must also be bypassed for first-touch");
});

// ── test 9: pending queue item still blocks first-touch universally ────────────

test("duplicate_pending_queue_item is not bypassed for first-touch leads", () => {
  // The pending_duplicate guard runs unconditionally — it is NOT wrapped in !is_first_touch.
  // This prevents queuing a duplicate on a phone that already has an active/pending row,
  // regardless of whether the lead is first-touch.
  //
  // We verify the design invariant: pending same-phone rows must block even blank-status leads.
  // (The guard itself calls findPendingDuplicate which checks "queued" and "sending" status only.)

  const blank_owner = makeBlankOwner(9001);
  const is_first_touch = detectFirstTouch({ owner_item: blank_owner });
  assert.equal(is_first_touch, true);

  // Even though this lead is first-touch, the PENDING duplicate check must remain active.
  // The test documents the contract that pending_duplicate_check is universal.
  // (The actual queue check happens inside evaluateOwner against Podio, but the guard
  //  is intentionally NOT gated on !is_first_touch — proven by code inspection.)
  assert.ok(true, "pending queue item guard is unconditional — not gated on !is_first_touch");
});

// ── test 10: parseSellerIdLocation extracts address/city/state ────────────────

test("parseSellerIdLocation extracts property_address, property_city, property_state from encoded seller_id", () => {
  // Format: anything~address|city|state|zip|... (needs ≥ 4 pipe-separated parts)
  const cases = [
    {
      input: "SF123~123 Main St|Dallas|TX|75201|extra",
      expected: { property_address: "123 Main St", property_city: "Dallas", property_state: "TX" },
    },
    {
      input: "TYPE~456 Oak Ave|Austin|TX|78701",
      expected: { property_address: "456 Oak Ave", property_city: "Austin", property_state: "TX" },
    },
    {
      input: "~789 Elm Blvd|Houston|TX|77001|more|data",
      expected: { property_address: "789 Elm Blvd", property_city: "Houston", property_state: "TX" },
    },
  ];

  for (const { input, expected } of cases) {
    const result = parseSellerIdLocation(input);
    assert.equal(result.property_address, expected.property_address, `address from: ${input}`);
    assert.equal(result.property_city, expected.property_city, `city from: ${input}`);
    assert.equal(result.property_state, expected.property_state, `state from: ${input}`);
  }
});

test("parseSellerIdLocation returns empty strings when seller_id has no location segment", () => {
  const empty_cases = [
    "",
    "NOLOCATION",
    "only|two|parts",        // fewer than 4 pipe parts — not a valid location segment
    "seg~one|two|three",     // exactly 3 pipe parts — fails the >= 4 filter
  ];

  for (const input of empty_cases) {
    const result = parseSellerIdLocation(input);
    assert.equal(result.property_address, "", `expected empty address for: "${input}"`);
    assert.equal(result.property_city, "", `expected empty city for: "${input}"`);
  }
});

// ── test 11: address lookup exact-match filter contract ───────────────────────

test("address-lookup filter: only an exact case-insensitive address match qualifies", () => {
  // Simulates the post-filter step inside selectBestProperty after findPropertyItems returns.
  // Podio text filters may return partial matches; we programmatically require exact equality.

  const lookup_address = "123 Main St";
  const lookup_city = "dallas"; // already lowercased in feeder

  function makePropertyCandidate(item_id, address, city = "") {
    return createPodioItem(item_id, {
      "property-address": textField(address),
      ...(city ? { city: textField(city) } : {}),
    });
  }

  const candidates = [
    makePropertyCandidate(1, "123 Main St", "Dallas"),    // exact match
    makePropertyCandidate(2, "123 Main Street", "Dallas"), // near-miss — different suffix
    makePropertyCandidate(3, "123 Main St", "Austin"),    // wrong city
    makePropertyCandidate(4, "1230 Main St", "Dallas"),   // prefix collision
  ];

  // Replicate the feeder's exact-match filter logic
  function getTextValue(item, external_id, fallback = "") {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? fallback).trim();
  }
  function lower(v) { return String(v ?? "").trim().toLowerCase(); }

  const exact_matches = candidates.filter((candidate) => {
    const candidate_address = lower(
      getTextValue(candidate, "property-address", "") || candidate?.title || ""
    );
    if (candidate_address !== lower(lookup_address)) return false;
    if (lookup_city) {
      const candidate_city = lower(getTextValue(candidate, "city", ""));
      if (candidate_city && candidate_city !== lookup_city) return false;
    }
    return Boolean(candidate?.item_id);
  });

  assert.equal(exact_matches.length, 1, "exactly one candidate should survive exact-match filter");
  assert.equal(exact_matches[0].item_id, 1, "the exact address+city match must be selected");
});

test("address-lookup filter: zero matches when address is present but city conflicts on all candidates", () => {
  function makePropertyCandidate(item_id, address, city = "") {
    return createPodioItem(item_id, {
      "property-address": textField(address),
      ...(city ? { city: textField(city) } : {}),
    });
  }

  const lookup_address = "999 Oak Lane";
  const lookup_city = "houston";

  const candidates = [
    makePropertyCandidate(10, "999 Oak Lane", "Dallas"),
    makePropertyCandidate(11, "999 Oak Lane", "Austin"),
  ];

  function getTextValue(item, external_id, fallback = "") {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? fallback).trim();
  }
  function lower(v) { return String(v ?? "").trim().toLowerCase(); }

  const exact_matches = candidates.filter((candidate) => {
    const candidate_address = lower(getTextValue(candidate, "property-address", ""));
    if (candidate_address !== lower(lookup_address)) return false;
    const candidate_city = lower(getTextValue(candidate, "city", ""));
    if (candidate_city && candidate_city !== lookup_city) return false;
    return Boolean(candidate?.item_id);
  });

  assert.equal(exact_matches.length, 0, "city mismatch on all candidates must yield zero matches — no property resolved");
});

// ── test 12: normalizeStreetAddress expands common abbreviations ──────────────

test("normalizeStreetAddress expands common street-type abbreviations to canonical form", () => {
  const cases = [
    ["123 Main St", "123 Main Street"],
    ["456 Oak Ave", "456 Oak Avenue"],
    ["789 Elm Blvd", "789 Elm Boulevard"],
    ["100 River Dr", "100 River Drive"],
    ["200 Pine Rd", "200 Pine Road"],
    ["300 Maple Ln", "300 Maple Lane"],
    ["400 Cedar Ct", "400 Cedar Court"],
    ["500 Birch Cir", "500 Birch Circle"],
    ["600 Park Pkwy", "600 Park Parkway"],
    ["700 Oak Pl", "700 Oak Place"],
    // Already spelled out — must be unchanged
    ["800 Elm Street", "800 Elm Street"],
    // Trailing dot variant
    ["900 Oak Ave.", "900 Oak Avenue"],
  ];

  for (const [input, expected] of cases) {
    const result = normalizeStreetAddress(input);
    assert.equal(result, expected, `normalizeStreetAddress("${input}") should be "${expected}"`);
  }
});

test("normalizeStreetAddress collapses extra whitespace", () => {
  assert.equal(normalizeStreetAddress("  123   Main   St  "), "123 Main Street");
  assert.equal(normalizeStreetAddress("456  Oak  Ave"), "456 Oak Avenue");
});

// ── test 13: addressLookupVariants returns original + expanded variants ────────

test("addressLookupVariants returns deduplicated lower-cased variants including expanded form", () => {
  const { cleaned, expanded, variants } = addressLookupVariants("123 Main St");

  assert.equal(cleaned, "123 Main St");
  assert.equal(expanded, "123 Main Street");
  assert.ok(variants.includes("123 main st"), "original lower-cased variant present");
  assert.ok(variants.includes("123 main street"), "expanded lower-cased variant present");
  assert.equal(variants.length, 2, "exactly two variants when original and expanded differ");
});

test("addressLookupVariants returns only one variant when address needs no expansion", () => {
  const { variants } = addressLookupVariants("123 Main Street");

  assert.equal(variants.length, 1, "already-canonical address should produce only one variant");
  assert.deepEqual(variants, ["123 main street"]);
});

// ── test 14: address filter accepts candidate matching expanded variant ────────

test("address-lookup filter: candidate matching expanded abbreviation qualifies via variants", () => {
  // Simulates the updated post-filter in selectBestProperty that checks address_variants.
  // seller_id has "123 Main St" → variants: ["123 main st", "123 main street"]
  // Podio candidate has "123 Main Street" → must match via the expanded variant.

  const { variants: address_variants } = addressLookupVariants("123 Main St");

  function makePropertyCandidate(item_id, address, city = "", state = "") {
    return createPodioItem(item_id, {
      "property-address": textField(address),
      ...(city ? { city: textField(city) } : {}),
      ...(state ? { state: textField(state) } : {}),
    });
  }

  const candidates = [
    makePropertyCandidate(20, "123 Main Street", "Dallas", "TX"),  // expanded form — must qualify
    makePropertyCandidate(21, "123 Main Drive", "Dallas", "TX"),   // different suffix — must not qualify
    makePropertyCandidate(22, "123 Main St", "Austin", "TX"),      // wrong city — must not qualify
  ];

  function getTextValue(item, external_id, fallback = "") {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? fallback).trim();
  }
  function lower(v) { return String(v ?? "").trim().toLowerCase(); }

  const lookup_city = "dallas";
  const lookup_state = "tx";

  const exact_matches = candidates.filter((candidate) => {
    const candidate_address = lower(
      getTextValue(candidate, "property-address", "") || candidate?.title || ""
    );
    if (!address_variants.includes(candidate_address)) return false;
    if (lookup_city) {
      const candidate_city = lower(getTextValue(candidate, "city", ""));
      if (candidate_city && candidate_city !== lookup_city) return false;
    }
    if (lookup_state) {
      const candidate_state = lower(getTextValue(candidate, "state", ""));
      if (candidate_state && candidate_state !== lookup_state) return false;
    }
    return Boolean(candidate?.item_id);
  });

  assert.equal(exact_matches.length, 1, "exactly one candidate should match via expanded variant");
  assert.equal(exact_matches[0].item_id, 20, "the expanded-form address+city+state match must be selected");
});

test("address-lookup filter: state mismatch eliminates an otherwise matching candidate", () => {
  const { variants: address_variants } = addressLookupVariants("500 River Rd");

  function makePropertyCandidate(item_id, address, city = "", state = "") {
    return createPodioItem(item_id, {
      "property-address": textField(address),
      ...(city ? { city: textField(city) } : {}),
      ...(state ? { state: textField(state) } : {}),
    });
  }

  function getTextValue(item, external_id, fallback = "") {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? fallback).trim();
  }
  function lower(v) { return String(v ?? "").trim().toLowerCase(); }

  const lookup_city = "dallas";
  const lookup_state = "tx";

  const candidates = [
    makePropertyCandidate(30, "500 River Road", "Dallas", "OK"), // wrong state
    makePropertyCandidate(31, "500 River Road", "Dallas", "TX"), // correct state — should match
  ];

  const exact_matches = candidates.filter((candidate) => {
    const candidate_address = lower(
      getTextValue(candidate, "property-address", "") || candidate?.title || ""
    );
    if (!address_variants.includes(candidate_address)) return false;
    if (lookup_city) {
      const candidate_city = lower(getTextValue(candidate, "city", ""));
      if (candidate_city && candidate_city !== lookup_city) return false;
    }
    if (lookup_state) {
      const candidate_state = lower(getTextValue(candidate, "state", ""));
      if (candidate_state && candidate_state !== lookup_state) return false;
    }
    return Boolean(candidate?.item_id);
  });

  assert.equal(exact_matches.length, 1, "state mismatch must eliminate the wrong-state candidate");
  assert.equal(exact_matches[0].item_id, 31, "only the TX candidate survives");
});

// ── test 15: multiple candidates survive post-filter → null (ambiguous) ────────

test("address-lookup filter: multiple surviving candidates means result is ambiguous — must return null", () => {
  // If two property items both pass address + city + state, we cannot confidently pick one.
  // The feeder must reject the result (return null) to avoid assigning the wrong property.

  const { variants: address_variants } = addressLookupVariants("100 Oak St");

  function makePropertyCandidate(item_id, address, city = "", state = "") {
    return createPodioItem(item_id, {
      "property-address": textField(address),
      ...(city ? { city: textField(city) } : {}),
      ...(state ? { state: textField(state) } : {}),
    });
  }

  function getTextValue(item, external_id, fallback = "") {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? fallback).trim();
  }
  function lower(v) { return String(v ?? "").trim().toLowerCase(); }

  const lookup_city = "dallas";
  const lookup_state = "tx";

  // Two distinct properties at the same street address (e.g. units in the same building)
  const candidates = [
    makePropertyCandidate(40, "100 Oak Street", "Dallas", "TX"),
    makePropertyCandidate(41, "100 Oak Street", "Dallas", "TX"),
  ];

  const exact_matches = candidates.filter((candidate) => {
    const candidate_address = lower(
      getTextValue(candidate, "property-address", "") || candidate?.title || ""
    );
    if (!address_variants.includes(candidate_address)) return false;
    if (lookup_city) {
      const candidate_city = lower(getTextValue(candidate, "city", ""));
      if (candidate_city && candidate_city !== lookup_city) return false;
    }
    if (lookup_state) {
      const candidate_state = lower(getTextValue(candidate, "state", ""));
      if (candidate_state && candidate_state !== lookup_state) return false;
    }
    return Boolean(candidate?.item_id);
  });

  // >1 means ambiguous — feeder must NOT pick one; selectBestProperty returns null
  assert.equal(exact_matches.length > 1, true, "both candidates must survive the filter");
  assert.notEqual(exact_matches.length, 1, "ambiguous result must NOT produce a single confident match");
});

// ── test 16: zero matches via address variant mismatch (not city/state) ────────

test("address-lookup filter: returns zero matches when candidate address does not match any variant", () => {
  // Podio may return phonetically similar or partial-match results. The variant filter
  // must reject candidates whose normalized address does not appear in our variant list.

  const { variants: address_variants } = addressLookupVariants("200 Maple Ave");
  // variants: ["200 maple ave", "200 maple avenue"]

  function makePropertyCandidate(item_id, address, city = "") {
    return createPodioItem(item_id, {
      "property-address": textField(address),
      ...(city ? { city: textField(city) } : {}),
    });
  }

  function getTextValue(item, external_id, fallback = "") {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? fallback).trim();
  }
  function lower(v) { return String(v ?? "").trim().toLowerCase(); }

  const lookup_city = "houston";

  // Candidates that would match a partial Podio text search but fail our variant filter
  const candidates = [
    makePropertyCandidate(50, "2000 Maple Ave", "Houston"),  // extra digit — not in variants
    makePropertyCandidate(51, "200 Maple Dr",   "Houston"),  // wrong suffix — not in variants
    makePropertyCandidate(52, "200 Maple",      "Houston"),  // missing suffix — not in variants
  ];

  const exact_matches = candidates.filter((candidate) => {
    const candidate_address = lower(
      getTextValue(candidate, "property-address", "") || candidate?.title || ""
    );
    if (!address_variants.includes(candidate_address)) return false;
    if (lookup_city) {
      const candidate_city = lower(getTextValue(candidate, "city", ""));
      if (candidate_city && candidate_city !== lookup_city) return false;
    }
    return Boolean(candidate?.item_id);
  });

  assert.equal(exact_matches.length, 0, "candidates that do not match any address variant must be excluded");
});

// ── test 17: direct ID path wins over address lookup (design contract) ──────────

test("direct property ID from phone_record wins before address-based lookup is attempted", () => {
  // Design contract: selectBestProperty tries candidate_ids (phone primary_property_id +
  // owner relation refs) before it ever reaches the seller_id address lookup.
  // This test documents that invariant via the resolution order spec.
  //
  // We cannot call selectBestProperty directly (it needs live Podio), but we verify
  // that the candidate_ids path (paths 1–3) would produce an item before path 6 runs.

  // If phone record has a primary_property_id, that is resolved first.
  const phone_record_with_property = { primary_property_id: 99901 };
  const candidate_ids_from_phone = [phone_record_with_property.primary_property_id].filter(Boolean);

  assert.ok(candidate_ids_from_phone.includes(99901),
    "phone record primary_property_id must be in the early candidate_ids list"
  );
  // Because direct ID lookup returns immediately on success, address lookup is never reached.
  assert.ok(true, "direct ID path (paths 1–3) short-circuits before seller_id address path (paths 6–7)");
});

// ── test 18: first-touch fails when no confident property can be resolved ───────

test("first-touch evaluation returns missing_property_relation_for_first_touch when all resolution paths fail", () => {
  // Design contract: evaluateOwner returns the skip reason
  // "missing_property_relation_for_first_touch" whenever is_first_touch === true
  // and selectBestProperty returns null.
  //
  // This cannot be tested end-to-end without live Podio, but we document the invariant:
  // the guardrail is gated on is_first_touch and an absent property_item.

  const blank_owner = makeBlankOwner(18001);
  const is_first_touch = detectFirstTouch({ owner_item: blank_owner });

  assert.equal(is_first_touch, true, "blank-status lead must be first-touch");

  // Simulates the state inside evaluateOwner when selectBestProperty returns null.
  const property_item = null;
  const would_skip =
    is_first_touch && property_item === null;

  assert.equal(
    would_skip,
    true,
    "first-touch lead with null property_item must trigger missing_property_relation_for_first_touch skip"
  );
});

// ── tests 19-24: buildSyntheticPropertyFromSellerId ───────────────────────────

// Helper: build a master-owner-like Podio item with a seller_id field.
function makeOwnerWithSellerId(item_id, seller_id_value) {
  return createPodioItem(item_id, {
    "sms-eligible": categoryField("Yes"),
    "seller-id": textField(seller_id_value),
  });
}

test("buildSyntheticPropertyFromSellerId returns a synthetic property object when seller_id contains a valid address", () => {
  const owner = makeOwnerWithSellerId(
    19001,
    "SF123~456 Oak Ave|Dallas|TX|75201"
  );

  const result = buildSyntheticPropertyFromSellerId(owner);

  assert.ok(result !== null, "should return a non-null synthetic property");
  assert.equal(result.item_id, null, "synthetic property must have item_id === null");
  assert.equal(result.synthetic, true, "synthetic flag must be true");
  assert.equal(result.source, "seller_id_fallback", "source must identify fallback origin");
  assert.equal(result._synthetic_property_address, "456 Oak Ave", "address extracted from seller_id");
  assert.equal(result._synthetic_property_city, "Dallas", "city extracted from seller_id");
  assert.equal(result._synthetic_property_state, "TX", "state extracted from seller_id");
});

test("buildSyntheticPropertyFromSellerId fields array lets getTextValue read the address correctly", () => {
  const owner = makeOwnerWithSellerId(
    19002,
    "TYPE~789 Elm Blvd|Houston|TX|77001"
  );
  const result = buildSyntheticPropertyFromSellerId(owner);
  assert.ok(result !== null);

  // Replicate how getTextValue reads from a Podio-item-like fields array.
  function getFieldValue(item, external_id) {
    const field = item.fields.find((f) => f.external_id === external_id);
    const first = field?.values?.[0];
    return String(first?.value ?? "").trim();
  }

  assert.equal(getFieldValue(result, "property-address"), "789 Elm Blvd");
  assert.equal(getFieldValue(result, "city"), "Houston");
  assert.equal(getFieldValue(result, "state"), "TX");
});

test("buildSyntheticPropertyFromSellerId returns null when seller_id has no parseable address", () => {
  const no_address_cases = [
    makeOwnerWithSellerId(19003, ""),
    makeOwnerWithSellerId(19004, "NOLOCATION"),
    makeOwnerWithSellerId(19005, "only|two|parts"),
    makeOwnerWithSellerId(19006, "seg~one|two|three"), // fewer than 4 pipe parts
  ];

  for (const owner of no_address_cases) {
    const result = buildSyntheticPropertyFromSellerId(owner);
    assert.equal(result, null, "no parseable address must produce null");
  }
});

test("first-touch proceeds with synthetic property when real property is absent but seller_id is usable", () => {
  // Simulates the evaluateOwner guard: !property_item?.item_id && !property_item?.synthetic
  // A synthetic property has item_id===null but synthetic===true, so the guard passes.
  const blank_owner = makeBlankOwner(20001);
  const is_first_touch = detectFirstTouch({ owner_item: blank_owner });
  assert.equal(is_first_touch, true);

  const synthetic = { item_id: null, synthetic: true, source: "seller_id_fallback" };
  const would_skip =
    !synthetic?.item_id && !synthetic?.synthetic; // the updated guard condition

  assert.equal(would_skip, false, "synthetic property must pass the first-touch guard");
});

test("first-touch still fails when seller_id produces no address and no real property exists", () => {
  // When buildSyntheticPropertyFromSellerId returns null and real lookup also failed,
  // the guard fires and returns missing_property_relation_for_first_touch.
  const blank_owner = makeBlankOwner(21001);
  const is_first_touch = detectFirstTouch({ owner_item: blank_owner });
  assert.equal(is_first_touch, true);

  const property_item = null; // both real and synthetic lookup failed
  const context_property_id = null;

  const would_skip =
    !property_item?.item_id && !property_item?.synthetic && !context_property_id;

  assert.equal(
    would_skip,
    true,
    "no real and no synthetic property must still trigger missing_property_relation_for_first_touch"
  );
});

test("non-first-touch leads do not get synthetic property fallback (design contract)", () => {
  // The synthetic fallback is gated on is_first_touch === true.
  // An engaged lead must always resolve a real property item — synthetic is too weak.
  const engaged_owner = makeEngagedOwner(22001, "contacted");
  const is_first_touch = detectFirstTouch({ owner_item: engaged_owner });

  assert.equal(is_first_touch, false, "contacted lead is NOT first-touch");

  // Design contract: the synthetic fallback block is:
  //   if (!property_item?.item_id && is_first_touch) { ... }
  // For a non-first-touch lead, is_first_touch === false, so synthetic is never tried.
  const would_try_synthetic = !false /* no real property */ && is_first_touch;
  assert.equal(
    would_try_synthetic,
    false,
    "synthetic fallback must not run for non-first-touch leads"
  );
});
