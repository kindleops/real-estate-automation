import test from "node:test";
import assert from "node:assert/strict";

import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";
import {
  categoryField,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

test("deriveContextSummary title-cases owner name and resolves agent name when property_item is present", () => {
  const summary = deriveContextSummary({
    phone_item: createPodioItem(11, {
      "phone-first-name": textField("Sam"),
    }),
    master_owner_item: createPodioItem(10, {
      // seller-id has a mailing address encoded — must NOT be used for property fields
      "seller-id": textField("P~SMITH|ZELFORD~2717 S 124TH EAST AVE|TULSA|OK|74129"),
      "owner-full-name": textField("Zelford Smith Jr"),
    }),
    property_item: createPodioItem(50, {
      "property-address": textField("123 MAIN ST"),
      city: textField("SPRINGFIELD"),
      state: textField("IL"),
    }),
    agent_item: createPodioItem(20, {
      title: textField("Rachel Kim"),
    }),
  });

  assert.equal(summary.owner_name, "Zelford Smith Jr");
  assert.equal(summary.seller_first_name, "Sam");
  // Address comes from property_item, not from seller-id
  assert.equal(summary.property_address, "123 Main St");
  assert.equal(summary.property_city, "Springfield");
  assert.equal(summary.property_state, "IL");
  assert.equal(summary.agent_name, "Rachel Kim");
  assert.equal(summary.agent_first_name, "Rachel");
});

test("deriveContextSummary returns empty property fields when property_item is null, even if master_owner has seller-id", () => {
  const summary = deriveContextSummary({
    phone_item: createPodioItem(11, {
      "phone-first-name": textField("Sam"),
    }),
    master_owner_item: createPodioItem(10, {
      "seller-id": textField("P~SMITH|ZELFORD~2717 S 124TH EAST AVE|TULSA|OK|74129"),
      "owner-full-name": textField("Zelford Smith Jr"),
    }),
    property_item: null,
  });

  // Master Owner mailing address must NEVER be used as the source for property fields
  assert.equal(summary.property_address, "");
  assert.equal(summary.property_city, "");
  assert.equal(summary.property_state, "");

  // Other fields still resolve correctly
  assert.equal(summary.owner_name, "Zelford Smith Jr");
  assert.equal(summary.seller_first_name, "Sam");
});

test("deriveContextSummary prefers master owner language over prospect and brain language", () => {
  const summary = deriveContextSummary({
    master_owner_item: createPodioItem(10, {
      "owner-full-name": textField("Jose Martinez"),
      "language-primary": categoryField("Spanish"),
    }),
    prospect_item: createPodioItem(20, {
      language: categoryField("Italian"),
    }),
    brain_item: createPodioItem(30, {
      "language-preference": categoryField("Greek"),
    }),
  });

  assert.equal(summary.language_preference, "Spanish");
});

test("deriveContextSummary falls back to prospect language before stale brain language", () => {
  const summary = deriveContextSummary({
    master_owner_item: createPodioItem(10, {
      "owner-full-name": textField("Neicie Johnson"),
    }),
    prospect_item: createPodioItem(20, {
      language: categoryField("English"),
    }),
    brain_item: createPodioItem(30, {
      "language-preference": categoryField("Greek"),
    }),
  });

  assert.equal(summary.language_preference, "English");
});
