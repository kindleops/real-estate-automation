// ─── stage-progression.proof.mjs ─────────────────────────────────────────
import { resolveNextStage, STAGES } from "../../src/lib/automation/negotiationEngine.js";
import assert from "node:assert";

async function runProof() {
  console.log("🚀 Running Stage Progression Proof...");

  // Case 1: Initial Progression
  console.log("\nCase 1: Ownership Confirmation");
  const s1 = resolveNextStage(null, { primary_intent: 'ownership_confirmed' });
  assert.strictEqual(s1, STAGES.OWNERSHIP_CHECK);
  console.log("✅ Initial ownership confirmation routes correctly");

  // Case 2: Memory-Aware Progression
  console.log("\nCase 2: Interest Probe after Ownership Known");
  const s2 = resolveNextStage(STAGES.OWNERSHIP_CHECK, { primary_intent: 'ownership_confirmed' }, { latest_state: { ownership_confirmed: true } });
  assert.strictEqual(s2, STAGES.CONSIDER_SELLING);
  console.log("✅ Move to interest probe if ownership already known");

  // Case 3: Price Collection
  console.log("\nCase 3: Interest Shown -> Asking Price");
  const s3 = resolveNextStage(STAGES.CONSIDER_SELLING, { primary_intent: 'seller_interested' });
  assert.strictEqual(s3, STAGES.ASKING_PRICE);
  console.log("✅ Seller interest moves to asking price collection");

  // Case 4: Condition Collection
  console.log("\nCase 4: Price Provided -> Condition Collection");
  const s4 = resolveNextStage(STAGES.ASKING_PRICE, { primary_intent: 'asking_price_provided' });
  assert.strictEqual(s4, STAGES.CONDITION_COLLECTION);
  console.log("✅ Price provided moves to condition collection");

  console.log("\n✨ Stage Progression Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
