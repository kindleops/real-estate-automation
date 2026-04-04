import test from "node:test";
import assert from "node:assert/strict";

import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";
import {
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";
test("deriveContextSummary falls back to phone-first-name and title-cases shouting address fields", () => {
  const summary = deriveContextSummary({
    phone_item: createPodioItem(11, {
      "phone-first-name": textField("Sam"),
    }),
    master_owner_item: createPodioItem(10, {
      "seller-id": textField("P~SMITH|ZELFORD~2717 S 124TH EAST AVE|TULSA|OK|74129"),
      "owner-full-name": textField("Zelford Smith Jr"),
    }),
    agent_item: createPodioItem(20, {
      title: textField("Rachel Kim"),
    }),
  });

  assert.equal(summary.owner_name, "Zelford Smith Jr");
  assert.equal(summary.seller_first_name, "Sam");
  assert.equal(summary.property_address, "2717 S 124th East Ave");
  assert.equal(summary.property_city, "Tulsa");
  assert.equal(summary.property_state, "OK");
  assert.equal(summary.agent_name, "Rachel Kim");
  assert.equal(summary.agent_first_name, "Rachel");
});
