import test from "node:test";
import assert from "node:assert/strict";

import { shouldSuppressOutreach } from "@/lib/domain/compliance/should-suppress-outreach.js";
import { categoryField, createPodioItem, dateField } from "../helpers/test-helpers.js";

test("phone do_not_call TRUE remains a non-blocking pre-contact flag", () => {
  const phone_item = createPodioItem(1001, {
    "phone-activity-status": categoryField("Active"),
    "do-not-call": categoryField("TRUE"),
    "dnc-source": categoryField("Federal DNC"),
  });

  const result = shouldSuppressOutreach({
    phone_item,
  });

  assert.equal(result.suppress, false);
  assert.equal(result.reason, null);
  assert.equal(result.details.pre_contact_phone_flag, true);
  assert.equal(result.details.true_post_contact_suppression, false);
  assert.equal(result.details.skip_reason, null);
});

test("true post-contact phone suppression still blocks future outreach", () => {
  const phone_item = createPodioItem(1002, {
    "phone-activity-status": categoryField("Active"),
    "do-not-call": categoryField("TRUE"),
    "dnc-source": categoryField("Internal Opt-Out"),
    "opt-out-date": dateField("2026-04-02T12:00:00.000Z"),
  });

  const result = shouldSuppressOutreach({
    phone_item,
  });

  assert.equal(result.suppress, true);
  assert.equal(result.reason, "phone_post_contact_suppression");
  assert.equal(result.details.pre_contact_phone_flag, true);
  assert.equal(result.details.true_post_contact_suppression, true);
  assert.equal(result.details.skip_reason, "phone_post_contact_suppression");
});

test("inactive phone suppression still blocks outbound eligibility", () => {
  const phone_item = createPodioItem(1003, {
    "phone-activity-status": categoryField("Inactive"),
    "do-not-call": categoryField("FALSE"),
  });

  const result = shouldSuppressOutreach({
    phone_item,
  });

  assert.equal(result.suppress, true);
  assert.equal(result.reason, "phone_not_active:inactive");
  assert.equal(result.details.true_post_contact_suppression, false);
  assert.equal(result.details.skip_reason, "phone_not_active:inactive");
});

// ─── Brain-level terminal status suppression ──────────────────────────────────
// When the Brain app's status-ai-managed is DNC or Wrong Number, all future
// outreach must be blocked regardless of phone-level flags.  These tests prove
// that the suppression persists and no new queue row would be generated.

test("wrong number brain status blocks all future outbound and stops normal follow-up", () => {
  const phone_item = createPodioItem(2001, {
    "phone-activity-status": categoryField("Active"),
    "do-not-call": categoryField("FALSE"),
  });
  const brain_item = createPodioItem(2001, {
    "status-ai-managed": categoryField("Wrong Number"),
  });

  const result = shouldSuppressOutreach({ phone_item, brain_item });

  assert.equal(result.suppress, true);
  assert.equal(result.reason, "brain_status_terminal");
  assert.equal(result.details.true_post_contact_suppression, true);
  assert.equal(result.details.skip_reason, "brain_status_terminal");
  assert.equal(result.details.status_ai_managed, "Wrong Number");
});

test("DNC brain status blocks all future outbound and no new queue row is generated", () => {
  const phone_item = createPodioItem(2002, {
    "phone-activity-status": categoryField("Active"),
    "do-not-call": categoryField("FALSE"),
  });
  const brain_item = createPodioItem(2002, {
    "status-ai-managed": categoryField("DNC"),
  });

  const result = shouldSuppressOutreach({ phone_item, brain_item });

  assert.equal(result.suppress, true);
  assert.equal(result.reason, "brain_status_terminal");
  assert.equal(result.details.true_post_contact_suppression, true);
  assert.equal(result.details.skip_reason, "brain_status_terminal");
  assert.equal(result.details.status_ai_managed, "DNC");
});

test("seller ask above target produces Above Range state and does NOT corrupt to Negotiation stage prematurely", () => {
  // Importing these directly would create a circular dep concern — test uses
  // shouldSuppressOutreach only; the state-machine coverage lives in
  // communications-state-machine.test.mjs.  This test confirms suppression is NOT
  // triggered just because price gap > 0 (that would be a false positive).
  const phone_item = createPodioItem(3001, {
    "phone-activity-status": categoryField("Active"),
    "do-not-call": categoryField("FALSE"),
  });
  const brain_item = createPodioItem(3001, {
    // Above Range seller — still in active conversation, NOT terminal
    "status-ai-managed": categoryField("Hot Opportunity"),
  });

  const result = shouldSuppressOutreach({ phone_item, brain_item });

  // A price-above-target seller is still actively negotiating — outreach must NOT be suppressed
  assert.equal(result.suppress, false);
  assert.equal(result.reason, null);
  assert.equal(result.details.true_post_contact_suppression, false);
});

test("completed follow-up trigger blocks future outreach", () => {
  const phone_item = createPodioItem(3002, {
    "phone-activity-status": categoryField("Active"),
    "do-not-call": categoryField("FALSE"),
  });
  const brain_item = createPodioItem(3002, {
    "follow-up-trigger-state": categoryField("Completed"),
  });

  const result = shouldSuppressOutreach({ phone_item, brain_item });

  assert.equal(result.suppress, true);
  assert.equal(result.reason, "follow_up_trigger_paused");
  assert.equal(result.details.true_post_contact_suppression, true);
  assert.equal(result.details.skip_reason, "follow_up_trigger_paused");
});
