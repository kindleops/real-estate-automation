// ─── conversation-memory.proof.mjs ─────────────────────────────────────────
import { upsertThread, appendTurn } from "../../src/lib/automation/conversationMemoryService.js";
import assert from "node:assert";
import crypto from "node:crypto";

async function runProof() {
  console.log("🚀 Running Conversation Memory Proof...");

  const seller_id = crypto.randomUUID();
  const property_id = crypto.randomUUID();

  // Test Case 1: Upsert Thread
  console.log("\nCase 1: Upsert Thread");
  const thread_id = await upsertThread({
    seller_id,
    property_id,
    status: 'active',
    metadata: { source: 'test_proof' }
  });

  assert.ok(thread_id, "Thread ID should be returned");
  console.log(`✅ Thread created: ${thread_id}`);

  // Test Case 2: Append Turn
  console.log("\nCase 2: Append Turn");
  const turn_id = await appendTurn({
    thread_id,
    direction: 'inbound',
    content: 'Hello, are you interested in my house?',
    intent_detected: 'ownership_confirmed',
    confidence_score: 0.95,
    metadata: { inbound_message_id: 'msg_' + crypto.randomUUID() }
  });


  assert.ok(turn_id, "Turn ID should be returned");
  console.log(`✅ Turn created: ${turn_id}`);

  console.log("\n✨ Conversation Memory Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
