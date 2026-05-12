// ─── seller-state-persistence.proof.mjs ───────────────────────────────────
import { upsertThread, storeSellerStateSnapshot } from "../../src/lib/automation/conversationMemoryService.js";
import assert from "node:assert";
import crypto from "node:crypto";
import { supabase } from "../../src/lib/supabase/client.js";

async function runProof() {
  console.log("🚀 Running Seller State Persistence Proof...");

  const seller_id = crypto.randomUUID();
  const thread_id = await upsertThread({
    seller_id,
    status: 'active'
  });

  const mockSellerState = {
    ownership_confirmed: true,
    seller_interest: 'high',
    motivation_level: 'medium',
    timeline: '30_days',
    creative_finance_open: true,
    confidence: 0.92,
    next_best_action: 'generate_offer_and_send'
  };

  console.log("\nCase 1: Store Seller State Snapshot");
  await storeSellerStateSnapshot({
    seller_id,
    thread_id,
    state_data: mockSellerState,
    capture_reason: 'test_proof'
  });

  // Verify in DB
  const { data, error } = await supabase
    .from("seller_state_snapshots")
    .select("*")
    .eq("thread_id", thread_id)
    .single();

  assert.ok(data, "Snapshot should be found in DB");
  assert.strictEqual(data.capture_reason, 'test_proof');
  assert.deepStrictEqual(data.state_data, mockSellerState);
  console.log("✅ Seller State persisted and verified");

  console.log("\n✨ Seller State Persistence Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
