// ─── auto-reply-routing.proof.mjs ─────────────────────────────────────────
import { queueAutoReply, __setQueueDeps, __resetQueueDeps } from "../../src/lib/automation/queueAutoReply.js";
import assert from "node:assert";

// Mock Supabase
const mockSupabase = {
  from: (table) => ({
    select: () => ({
      eq: () => ({
        maybeSingle: async () => ({ data: null, error: null }),
        single: async () => {
          if (table === "message_events") return { 
            data: { 
              id: "msg-1", 
              message_body: "Yes", 
              from_phone_number: "+12223334444", 
              to_phone_number: "+15556667777",
              metadata: {
                personalization_context: {
                  seller_first_name: "John",
                  property_address: "123 Main St"
                }
              }
            }, 
            error: null 
          };
          return { data: null, error: null };
        }
      })
    }),
    insert: () => ({
      select: () => ({
        single: async () => ({ data: { id: "queue-1" }, error: null })
      })
    })
  })
};

async function runProof() {
  console.log("🚀 Running Auto-Reply Routing Proof...");

  // Case 1: Opt-Out
  console.log("\nCase 1: Opt-Out");
  __setQueueDeps({
    supabase: mockSupabase,
    classify: async () => ({ primary_intent: "opt_out", confidence: 1 }),
    selectNextTemplate: async () => ({ ok: false, action: "stop", reason: "compliance_stop" }),
    evaluateContactWindow: () => ({ allowed: true })
  });

  const res1 = await queueAutoReply("thread-1", "msg-opt-out");
  assert.strictEqual(res1.ok, false);
  assert.strictEqual(res1.action, "stop");
  console.log("✅ Case 1 Passed");

  // Case 2: Ownership Confirmed
  console.log("\nCase 2: Ownership Confirmed");
  __setQueueDeps({
    supabase: mockSupabase,
    classify: async () => ({ primary_intent: "ownership_confirmed", confidence: 1, language: "English" }),
    selectNextTemplate: async () => ({
      ok: true,
      action: "queue_reply",
      template: {
        id: "tpl-1",
        template_id: "840001",
        template_body: "Hi {{seller_first_name}}, are you interested in selling {{property_address}}?",
        use_case: "consider_selling",
        matches: ["language"]
      },
      use_case: "consider_selling",
      stage_code: "S2"
    }),
    renderSafeTemplate: (tpl, vars) => ({ ok: true, text: `Hi ${vars.seller_first_name}, selling ${vars.property_address}?` }),
    validateTemplateForIntent: () => ({ ok: true }),
    evaluateContactWindow: () => ({ allowed: true })
  });

  const res2 = await queueAutoReply("thread-1", "msg-yes");
  assert.strictEqual(res2.ok, true);
  assert.strictEqual(res2.action, "queue_reply");
  assert.strictEqual(res2.queue_id, "queue-1");
  console.log("✅ Case 2 Passed");

  console.log("\n✨ Auto-Reply Routing Proof Completed Successfully!");
  
  __resetQueueDeps();
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
