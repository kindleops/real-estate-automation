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
