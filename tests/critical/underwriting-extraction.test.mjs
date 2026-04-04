import test from "node:test";
import assert from "node:assert/strict";

import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import { SELLER_FLOW_STAGES } from "@/lib/domain/seller-flow/canonical-seller-flow.js";

function buildPriceContext() {
  return {
    recent: {
      recent_events: [
        {
          direction: "Outbound",
          metadata: {
            selected_use_case: "asking_price",
            next_expected_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
          },
        },
      ],
    },
  };
}

test("underwriting extraction normalizes shorthand asking prices in price context", () => {
  const shorthand = extractUnderwritingSignals({
    message: "I'd take 80k.",
    context: buildPriceContext(),
  });
  const bare = extractUnderwritingSignals({
    message: "80",
    context: buildPriceContext(),
  });

  assert.equal(shorthand.signals.asking_price, 80000);
  assert.equal(bare.signals.asking_price, 80000);
});

test("underwriting extraction avoids bare-number price false positives", () => {
  const tenants = extractUnderwritingSignals({
    message: "2 tenants",
    context: buildPriceContext(),
  });
  const beds = extractUnderwritingSignals({
    message: "3 bed",
    context: buildPriceContext(),
  });
  const year_built = extractUnderwritingSignals({
    message: "built in 80",
    context: buildPriceContext(),
  });
  const reply_time = extractUnderwritingSignals({
    message: "I can reply after 8",
    context: buildPriceContext(),
  });

  assert.equal(tenants.signals.asking_price, null);
  assert.equal(tenants.signals.occupancy_status, "Tenant Occupied");
  assert.equal(beds.signals.asking_price, null);
  assert.equal(year_built.signals.asking_price, null);
  assert.equal(reply_time.signals.asking_price, null);
});

test("underwriting extraction recognizes multifamily edge-case phrasing without treating mixed use as multifamily", () => {
  const occupied_units = extractUnderwritingSignals({
    message: "4 units all occupied",
  });
  const triplex = extractUnderwritingSignals({
    message: "triplex",
  });
  const doors = extractUnderwritingSignals({
    message: "8 doors",
  });
  const apartments = extractUnderwritingSignals({
    message: "apartment building",
  });
  const duplex = extractUnderwritingSignals({
    message: "duplex",
  });
  const mixed_use = extractUnderwritingSignals({
    message: "mixed use",
  });
  const utilities = extractUnderwritingSignals({
    message: "tenant pays electric",
  });

  assert.equal(occupied_units.signals.unit_count, 4);
  assert.equal(occupied_units.signals.occupancy_status, "Tenant Occupied");
  assert.equal(triplex.signals.unit_count, 3);
  assert.equal(triplex.signals.property_type, "Multifamily");
  assert.equal(doors.signals.unit_count, 8);
  assert.equal(apartments.signals.property_type, "Multifamily");
  assert.equal(duplex.signals.unit_count, 2);
  assert.equal(duplex.signals.property_type, "Multifamily");
  assert.equal(mixed_use.signals.property_type, "Commercial");
  assert.equal(utilities.signals.expenses_present, true);
});
