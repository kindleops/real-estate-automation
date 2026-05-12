// ─── memory-idempotency.proof.mjs ─────────────────────────────────────────
import { upsertThread, appendTurn, storeRoutingDecision } from "../../src/lib/automation/conversationMemoryService.js";
import assert from "node:assert";
import crypto from "node:crypto";
import { supabase } from "../../src/lib/supabase/client.js";

async function runProof() {
  console.log("🚀 Running Memory Idempotency Proof...");

  const seller_id = crypto.randomUUID();
  const thread_id = await upsertThread({
    seller_id,
    status: 'active'
  });

  const inbound_message_id = 'msg_' + crypto.randomUUID();

  console.log("\nCase 1: Duplicate Turn Prevention");
  const turn1_id = await appendTurn({
    thread_id,
    direction: 'inbound',
    content: 'Duplicate test',
    metadata: { inbound_message_id }
  });

  const turn2_id = await appendTurn({
    thread_id,
    direction: 'inbound',
    content: 'Duplicate test (retry)',
    metadata: { inbound_message_id }
  });

  assert.strictEqual(turn1_id, turn2_id, "Should return the same turn ID for duplicate inbound_message_id");
  console.log("✅ Duplicate Turn blocked and first ID returned");

  console.log("\nCase 2: Duplicate Routing Decision Prevention");
  const decision1_id = await storeRoutingDecision({
    turn_id: turn1_id,
    thread_id,
    decision_type: 'auto_reply_queued',
    routed_to: 'tpl_123',
    confidence: 0.99
  });

  const decision2_id = await storeRoutingDecision({
    turn_id: turn1_id,
    thread_id,
    decision_type: 'auto_reply_queued',
    routed_to: 'tpl_123',
    confidence: 0.99
  });

  assert.strictEqual(decision1_id, decision2_id, "Should return the same decision ID for duplicate turn_id");
  console.log("✅ Duplicate Routing Decision blocked");

  console.log("\n✨ Memory Idempotency Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
