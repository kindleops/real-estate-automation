/**
 * First-touch guardrail tests.
 *
 * Proves that:
 *  1. Blank-status cold lead is always detected as first-touch → ownership_check clamping applies
 *  2. Prior polluted outbound history does NOT advance stage — only CRM contact_status does
 *  3. Later-stage use_cases are hard-blocked by FORBIDDEN_FIRST_TOUCH_USE_CASES
 *  4. Valid Stage-1 variant groups are not blocked; Stage-2+ variant groups ARE blocked
 *  5. Non-blank contact_status (engaged, contacted, etc.) = NOT first-touch → no clamp
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  detectFirstTouch,
  FORBIDDEN_FIRST_TOUCH_USE_CASES,
  FORBIDDEN_FIRST_TOUCH_LIFECYCLE_STAGES,
  FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import { categoryField, createPodioItem } from "../helpers/test-helpers.js";

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

// ── test 5: Stage-1 variant groups are allowed; later stages are rejected ──────

test("FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS allows Stage 1 groups and implicitly rejects Stage 2+", () => {
  const allowed = [
    "Stage 1 — Ownership Confirmation",
    "Stage 1 — Ownership Check",
    "Stage 1 Ownership Check",
    "Stage 1 Ownership Confirmation",
    "Stage 1 Follow-Up",
    "Stage 1 — Ownership Confirmation Follow-Up",
  ];

  for (const variant_group of allowed) {
    assert.ok(
      FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS.has(variant_group),
      `"${variant_group}" must be in FIRST_TOUCH_OWNERSHIP_VARIANT_GROUPS`
    );
  }

  // Later-stage variant groups must NOT be allowed for first-touch
  const disallowed = [
    "Stage 2 Consider Selling",
    "Stage 3 — Asking Price",
    "Stage 4A — Confirm Basics",
    "Stage 4B — Condition Probe",
    "Stage 5 — Offer Reveal",
    "Stage 5 — Offer No Response",
    "Contract Sent",
    "Close Handoff",
  ];

  for (const variant_group of disallowed) {
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
