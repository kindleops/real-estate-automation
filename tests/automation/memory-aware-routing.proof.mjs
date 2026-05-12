// ─── memory-aware-routing.proof.mjs ──────────────────────────────────────
import { queueAutoReply, __setQueueDeps, __resetQueueDeps } from "../../src/lib/automation/queueAutoReply.js";
import assert from "node:assert";

async function runProof() {
  console.log("🚀 Running Memory-Aware Routing Proof...");

  const mockSupabase = {
    from: (table) => ({
      select: () => ({
        eq: () => ({
          maybeSingle: async () => ({ data: null, error: null }),
          single: async () => {
             if (table === 'message_events') return { data: { id: 'msg-1', message_body: 'How much?', current_stage: 'ownership_check' }, error: null };
             return { data: null, error: null };
          }
        }),
        in: () => ({
          select: async () => ({ data: [], error: null })
        })
      }),
      insert: () => ({
        select: () => ({
          single: async () => ({ data: { id: 'queue-1' }, error: null })
        })
      })
    })
  };

  __setQueueDeps({
    supabase: mockSupabase,
    classify: async () => ({ 
      primary_intent: "asks_offer", 
      confidence: 0.9,
      seller_state: { seller_interest: 'high' }
    }),
    memory: {
      loadConversationMemory: async () => ({
        found: true,
        thread: { id: 'thread-123' },
        latest_state: { ownership_confirmed: true }
      }),
      storeRoutingDecision: async () => {},
      upsertThread: async () => 'thread-123',
      appendTurn: async () => 'turn-123',
      storeSellerStateSnapshot: async () => {}
    },
    selectNextTemplate: async (ctx) => {
      assert.ok(ctx.memory.found, "Memory should be loaded");
      assert.ok(ctx.memory.latest_state.ownership_confirmed, "Should know ownership is confirmed");
      return { ok: true, action: 'queue_reply', template: { template_id: 'tpl-offer', matches: [] }, use_case: 'offer_reveal' };
    },
    evaluateContactWindow: () => ({ allowed: true }),
    renderSafeTemplate: () => ({ ok: true, text: 'Rendered' }),
    validateTemplateForIntent: () => ({ ok: true })
  });

  const res = await queueAutoReply("thread-123", "msg-1");
  assert.strictEqual(res.ok, true);
  console.log("✅ Memory loaded and used for routing decision");

  __resetQueueDeps();
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
