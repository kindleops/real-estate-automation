import test from "node:test";
import assert from "node:assert/strict";

import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";

function textField(external_id, value) {
  return {
    external_id,
    values: [{ value }],
  };
}

test("deriveContextSummary falls back to seller-id location and agent title first name", () => {
  const summary = deriveContextSummary({
    master_owner_item: {
      item_id: 10,
      fields: [
        textField("seller-id", "P~SMITH|ZELFORD~2717 S 124TH EAST AVE|TULSA|OK|74129"),
        textField("owner-full-name", "Zelford Smith Jr"),
      ],
    },
    agent_item: {
      item_id: 20,
      fields: [
        textField("title", "Rachel Kim"),
      ],
    },
  });

  assert.equal(summary.owner_name, "Zelford Smith Jr");
  assert.equal(summary.property_address, "2717 S 124TH EAST AVE");
  assert.equal(summary.property_city, "TULSA");
  assert.equal(summary.property_state, "OK");
  assert.equal(summary.agent_name, "Rachel Kim");
  assert.equal(summary.agent_first_name, "Rachel");
});
