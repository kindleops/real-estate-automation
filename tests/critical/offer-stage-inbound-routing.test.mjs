import { describe, it } from "node:test";
import assert from "node:assert/strict";

import { isOfferStageTrigger, runOfferStageAI, buildOfferStageMetadata, shouldSkipOfferStageAI } from "@/lib/domain/offers/offer-stage-ai-integration.js";

// Mock fetch helpers (defined locally to avoid cross-file dependency)
function setupMockFetch(content, status = 200) {
  const responseJson = {
    choices: [{ message: { content: typeof content === "string" ? content : JSON.stringify(content) } }],
  };
  global.fetch = async () => {
    return {
      ok: status >= 200 && status < 300,
      status,
      text: async () => JSON.stringify(responseJson),
      json: async () => responseJson,
    };
  };
}

function setupMockFetchError(error) {
  global.fetch = async () => {
    throw error;
  };
}

// ─── Tests ──────────────────────────────────────────────────────────

describe("Offer Stage Trigger Detection", () => {
  it("should trigger on 'how much' question", () => {
    const result = isOfferStageTrigger({
      message: "How much will you pay for my house?",
      classification: {},
      sellerStage: null,
      route: null,
    });
    assert.equal(result.triggered, true);
    assert.match(result.reason, /price_intent/);
  });

  it("should trigger on seller giving asking price", () => {
    const result = isOfferStageTrigger({
      message: "I want $150,000 for the property",
      classification: {},
      sellerStage: null,
      route: null,
    });
    assert.equal(result.triggered, true);
  });

  it("should trigger on 'send offer' request", () => {
    const result = isOfferStageTrigger({
      message: "Send me an offer",
      classification: {},
      sellerStage: null,
      route: null,
    });
    assert.equal(result.triggered, true);
  });

  it("should trigger when seller_stage is offer_reveal", () => {
    const result = isOfferStageTrigger({
      message: "Hello",
      classification: {},
      sellerStage: "offer_reveal",
      route: null,
    });
    assert.equal(result.triggered, true);
    assert.match(result.reason, /seller_stage/);
  });

  it("should trigger when seller_stage is negotiation", () => {
    const result = isOfferStageTrigger({
      message: "That's too low",
      classification: {},
      sellerStage: "negotiation",
      route: null,
    });
    assert.equal(result.triggered, true);
  });

  it("should trigger on route with offer use case", () => {
    const result = isOfferStageTrigger({
      message: "Let's talk price",
      classification: {},
      sellerStage: null,
      route: { use_case: "offer_reveal_cash" },
    });
    assert.equal(result.triggered, true);
  });

  it("should not trigger on non-price inbound", () => {
    const result = isOfferStageTrigger({
      message: "Yes, that's my property",
      classification: {},
      sellerStage: "ownership_check",
      route: null,
    });
    assert.equal(result.triggered, false);
    assert.match(result.reason, /no_offer_trigger/);
  });
});

describe("Run Offer Stage AI", () => {
  it("should run in dry-run mode only", async () => {
    setupMockFetch(JSON.stringify({
      reply: "Based on what I'm seeing, I'd probably be around $75k-$85k cash as-is.",
      tone: "professional",
      next_action: "send",
      reasoning: "SFH offer within range",
    }));

    const result = await runOfferStageAI({
      message: "How much will you pay?",
      property: { asset_type: "single_family", arv_mid: 100000, repairs_mid: 5000 },
      conversationHistory: [],
      sellerName: "Test Seller",
      phone: "555-123-4567",
      sellerStage: null,
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.ok, true);
    assert.equal(result.dry_run, true);
    assert.equal(result.offer_ai_result?.send_mode, "dry_run_offer_ai");
    assert.equal(result.offer_ai_result?.would_queue, false);
  });

  it("should not send live SMS", async () => {
    setupMockFetch(JSON.stringify({
      reply: "I can offer around $80k cash.",
      tone: "direct",
      next_action: "send",
      reasoning: "Offer within range",
    }));

    const result = await runOfferStageAI({
      message: "What's your best offer?",
      property: { asset_type: "single_family", arv_mid: 100000, repairs_mid: 5000 },
      conversationHistory: [],
      sellerName: "Test Seller",
      phone: "555-987-6543",
      sellerStage: null,
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.dry_run, true);
    assert.equal(result.offer_ai_result?.would_auto_send, false);
  });

  it("should not create live queue item", async () => {
    setupMockFetch(JSON.stringify({
      reply: "Let's discuss the numbers.",
      tone: "friendly",
      next_action: "manual_review",
      reasoning: "Need more info",
    }));

    const result = await runOfferStageAI({
      message: "What can you offer?",
      property: { asset_type: "single_family", arv_mid: 100000 },
      conversationHistory: [],
      sellerName: "Test Seller",
      phone: "555-111-2222",
      sellerStage: null,
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.dry_run, true);
    assert.equal(result.offer_ai_result?.would_queue, false);
  });

  it("should include draft and internal numbers in result", async () => {
    setupMockFetch(JSON.stringify({
      reply: "I'm looking at around $75k-$85k for your property.",
      tone: "professional",
      next_action: "send",
      reasoning: "SFH offer",
    }));

    const result = await runOfferStageAI({
      message: "How much?",
      property: { asset_type: "single_family", arv_mid: 100000, repairs_mid: 5000 },
      conversationHistory: [],
      sellerName: "Test Seller",
      phone: "555-333-4444",
      sellerStage: null,
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.ok(result.offer_ai_result?.draft_message);
    assert.ok(result.offer_ai_result?.recommended_opening_offer !== null);
    assert.ok(result.offer_ai_result?.walkaway_internal !== null);
    assert.ok(result.offer_ai_result?.draft_message && !result.offer_ai_result?.draft_message.includes("walkaway"));
  });

  it("should be blocked by suppression", async () => {
    const result = await runOfferStageAI({
      message: "How much?",
      property: { asset_type: "single_family", arv_mid: 100000 },
      conversationHistory: [],
      sellerName: "Suppressed Seller",
      phone: "555-444-5555",
      sellerStage: null,
      suppressionStatus: "opt_out",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.blocked, true);
    assert.ok(result.blocked_reasons && result.blocked_reasons.length > 0);
  });
});

describe("Offer Stage Metadata", () => {
  it("should build metadata from offer result", () => {
    const offerResult = {
      ok: true,
      dry_run: true,
      offer_ai_result: {
        triggered: true,
        trigger_reason: "price_intent_detected",
        asset_type: "single_family",
        recommended_opening_offer: 75000,
        target_contract: 85000,
        walkaway_internal: 90000,
        offer_confidence_score: 0.85,
        safe_to_reveal_offer: true,
        missing_required_info: [],
        draft_message: "I'd offer around $75k-$85k",
        send_mode: "dry_run_offer_ai",
        would_queue: false,
        would_auto_send: false,
        blocked_reason: null,
        action: "offer_reveal",
        route: "contract_path",
        timestamp: new Date().toISOString(),
      },
    };

    const metadata = buildOfferStageMetadata(offerResult);

    assert.equal(metadata.offer_stage_ai_triggered, true);
    assert.equal(metadata.offer_stage_ai_asset_type, "single_family");
    assert.equal(metadata.offer_stage_ai_opening_offer, 75000);
    assert.equal(metadata.offer_stage_ai_target_offer, 85000);
    assert.equal(metadata.offer_stage_ai_confidence, 0.85);
    assert.equal(metadata.offer_stage_ai_send_mode, "dry_run_offer_ai");
  });

  it("should return empty object for null result", () => {
    const metadata = buildOfferStageMetadata({ ok: false, offer_ai_result: null });
    assert.equal(Object.keys(metadata).length, 0);
  });
});

describe("Skip Offer Stage AI", () => {
  it("should skip when suppressed", () => {
    const result = shouldSkipOfferStageAI({
      suppressionStatus: "opt_out",
      contactWindowStatus: "allowed",
    });
    assert.equal(result.skip, true);
    assert.match(result.reason, /suppression/);
  });

  it("should skip when outside contact window", () => {
    const result = shouldSkipOfferStageAI({
      suppressionStatus: "allowed",
      contactWindowStatus: "outside_local_send_window",
    });
    assert.equal(result.skip, true);
    assert.match(result.reason, /contact_window/);
  });

  it("should not skip when all clear", () => {
    const result = shouldSkipOfferStageAI({
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });
    assert.equal(result.skip, false);
  });
});

// Cleanup
global.fetch = undefined;
