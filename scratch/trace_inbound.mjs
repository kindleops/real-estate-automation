import { handleTextgridInbound, __setTextgridInboundTestDeps } from "../src/lib/flows/handle-textgrid-inbound.js";
import { SELLER_FLOW_STAGES } from "../src/lib/domain/seller-flow/canonical-seller-flow.js";

async function runTrace() {
  console.log("--- STARTING TRACE ---");

  // Mock dependencies
  const mockDeps = {
    loadContext: async () => ({
      owner: { id: 1, phone: "+14045551234", name: "Test Seller" },
      property: { id: 101, address: "123 Main St", asset_type: "single_family" },
      brain: { item_id: 201 },
      prospect: { item_id: 301 },
      market: "Atlanta"
    }),
    classify: async () => ({
      intent: "interested",
      objection: "price_inquiry",
      confidence: 0.95,
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
    logInboundMessageEvent: async (fields) => {
      console.log("TRACE: DOMAIN LOG CAPTURED", {
        detected_intent: fields.detected_intent,
        language: fields.language,
        priority: fields.priority
      });
      return { item_id: 501 };
    },
    logInboundMessageEventSupabase: async (payload) => {
      console.log("TRACE: SUPABASE LOG CAPTURED", {
        detected_intent: payload.detected_intent,
        language: payload.language,
        classification_confidence: payload.classification_confidence
      });
      return { id: "supabase-uuid" };
    },
    // Mock other required deps
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
    getSupabaseClient: () => ({}),
    maybeQueueSellerStageReply: async () => ({}),
  };

  __setTextgridInboundTestDeps(mockDeps);

  const payload = {
    message_id: "test-sid-123",
    from: "+14045551234",
    to: "+14045550000",
    message_body: "Yes I own the house. How much do you pay?",
  };

  await handleTextgridInbound(payload, {
    dry_run: true,
    auto_reply_enabled: false
  });

  console.log("--- TRACE COMPLETE ---");
}

runTrace().catch(console.error);
