import test from "node:test";
import assert from "node:assert/strict";

import { derivePhoneDisqualification } from "@/lib/domain/context/phone-disqualification.js";
import { categoryField, createPodioItem } from "../helpers/test-helpers.js";

test("derivePhoneDisqualification allows unknown phone activity status", () => {
  const phone_item = createPodioItem(2001, {
    "phone-activity-status": categoryField("Unknown"),
  });

  assert.equal(derivePhoneDisqualification(phone_item), null);
});

test("derivePhoneDisqualification allows missing phone activity status", () => {
  const phone_item = createPodioItem(2002, {});

  assert.equal(derivePhoneDisqualification(phone_item), null);
});

test("derivePhoneDisqualification still blocks explicitly inactive phones", () => {
  const phone_item = createPodioItem(2003, {
    "phone-activity-status": categoryField("Inactive"),
  });

  assert.equal(derivePhoneDisqualification(phone_item), "phone_not_active:inactive");
});
