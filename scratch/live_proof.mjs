import { handleTextgridInbound, __setTextgridInboundTestDeps } from "../src/lib/flows/handle-textgrid-inbound.js";
import { logInboundMessageEvent as logSupabase } from "../src/lib/supabase/sms-engine.js";
import { getDefaultSupabaseClient } from "../src/lib/supabase/default-client.js";

async function runLiveProof() {
  console.log("--- STARTING LIVE PROOF ---");

  const testSid = "sid-proof-" + Date.now();

  // Mock dependencies but use REAL Supabase logger
  const mockDeps = {
    loadContext: async () => ({
      owner: { id: 1, phone: "+14045551234", name: "Proof Seller" },
      property: { id: 101, address: "123 Proof St" },
      brain: { item_id: 201 },
      prospect: { item_id: 301 },
      market: "Atlanta"
    }),
    classify: async () => ({
      intent: "interested",
      objection: "price_inquiry",
      confidence: 0.99,
      language: "en",
      safety_status: "safe",
      priority: "high",
      risk: "low"
    }),
    resolveSellerAutoReplyPlan: async () => ({
      plan: {
        detected_intent: "interested",
        routing_allowed: true,
        selected_use_case: "S2"
      }
    }),
    logInboundMessageEvent: async () => ({ item_id: 501 }), // Mock Podio
    logInboundMessageEventSupabase: logSupabase, // USE REAL SUPABASE LOGGER
    loadContextWithFallback: async (ctx) => ctx,
    createBrain: async () => ({ item_id: 201 }),
    resolveRoute: async () => ({ stage: "ownership_check" }),
    normalizeInboundTextgridPhone: (p) => p,
    updateBrainAfterInbound: async () => {},
    updateBrainStage: async () => {},
    syncPipelineState: async () => ({}),
    isNegativeReply: () => false,
    cancelPendingQueueItemsForOwner: async () => {},
    buildInboundConversationState: () => ({}),
    beginIdempotentProcessing: async () => true,
    completeIdempotentProcessing: async () => {},
    failIdempotentProcessing: async () => {},
    hashIdempotencyPayload: () => "hash",
    info: (...args) => console.log("INFO:", ...args),
    warn: (...args) => console.warn("WARN:", ...args),
    getSupabaseClient: getDefaultSupabaseClient,
    maybeQueueSellerStageReply: async () => ({}),
  };

  __setTextgridInboundTestDeps(mockDeps);

  const payload = {
    message_id: testSid,
    from: "+14045551234",
    to: "+14045550000",
    message_body: "Yes I own it. What is your offer?",
  };

  console.log("SIMULATING INBOUND:", testSid);

  // Simulate the first call that route.js would make
  await logSupabase(payload);
  console.log("FIRST PASS (Raw) COMPLETED");

  // Run the main handler (which now has the second pass)
  await handleTextgridInbound(payload, {
    dry_run: true,
    auto_reply_enabled: false
  });
  console.log("SECOND PASS (Classified) COMPLETED");

  console.log("--- PROOF COMPLETE ---");
}

runLiveProof().catch(console.error);
