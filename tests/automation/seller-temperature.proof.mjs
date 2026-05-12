// ─── seller-temperature.proof.mjs ────────────────────────────────────────
import { calculateTemperature, TEMPERATURES } from "../../src/lib/automation/negotiationEngine.js";
import assert from "node:assert";

async function runProof() {
  console.log("🚀 Running Seller Temperature Proof...");

  // Case 1: Cold
  console.log("\nCase 1: Neutral Message (Cold/Warming)");
  const t1 = calculateTemperature({ primary_intent: 'unclear', motivation_score: 30 });
  assert.strictEqual(t1, TEMPERATURES.COLD);
  console.log("✅ Neutral message starts cold");

  // Case 2: Warming
  console.log("\nCase 2: Ownership Confirmed (Warming)");
  const t2 = calculateTemperature({ primary_intent: 'ownership_confirmed', motivation_score: 50 });
  assert.strictEqual(t2, TEMPERATURES.WARMING);
  console.log("✅ Ownership confirmation warms the lead");

  // Case 3: Engaged
  console.log("\nCase 3: Interest Shown (Engaged)");
  const t3 = calculateTemperature({ primary_intent: 'seller_interested', motivation_score: 60 });
  assert.strictEqual(t3, TEMPERATURES.ENGAGED);
  console.log("✅ Interest shown makes the lead engaged");

  // Case 4: Hot
  console.log("\nCase 4: Price Provided + Urgency (Hot)");
  const t4 = calculateTemperature(
    { primary_intent: 'asking_price_provided', motivation_score: 70 },
    { latest_state: { timeline: 'immediate' } }
  );
  assert.strictEqual(t4, TEMPERATURES.HOT);
  console.log("✅ Price + immediate timeline makes lead hot");

  // Case 5: Dead
  console.log("\nCase 5: Opt Out (Dead)");
  const t5 = calculateTemperature({ primary_intent: 'opt_out' });
  assert.strictEqual(t5, TEMPERATURES.DEAD);
  console.log("✅ Opt out kills lead temperature");

  console.log("\n✨ Seller Temperature Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
