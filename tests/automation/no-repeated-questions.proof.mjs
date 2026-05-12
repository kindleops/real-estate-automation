// ─── no-repeated-questions.proof.mjs ─────────────────────────────────────
import { isQuestionRedundant } from "../../src/lib/automation/negotiationEngine.js";
import assert from "node:assert";

async function runProof() {
  console.log("🚀 Running No-Repeated Questions Proof...");

  const memoryWithOwnership = {
    found: true,
    latest_state: { ownership_confirmed: true }
  };

  const memoryWithPrice = {
    found: true,
    latest_state: { price_mentioned: 150000 }
  };

  console.log("\nCase 1: Ownership Check Redundancy");
  assert.strictEqual(isQuestionRedundant('ownership_check', memoryWithOwnership), true, "Should be redundant if already confirmed");
  assert.strictEqual(isQuestionRedundant('ownership_check', { latest_state: {} }), false, "Should NOT be redundant if not confirmed");
  console.log("✅ Ownership redundancy detected correctly");

  console.log("\nCase 2: Asking Price Redundancy");
  assert.strictEqual(isQuestionRedundant('asking_price', memoryWithPrice), true, "Should be redundant if price already provided");
  assert.strictEqual(isQuestionRedundant('asking_price', { latest_state: {} }), false, "Should NOT be redundant if price not provided");
  console.log("✅ Asking price redundancy detected correctly");

  console.log("\n✨ No-Repeated Questions Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
