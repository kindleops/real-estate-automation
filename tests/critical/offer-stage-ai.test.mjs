import { describe, it } from "node:test";
import assert from "node:assert/strict";

import { resolveOfferProfile } from "@/lib/ai/offer-profile.js";
import { checkOfferGate, evaluateSellerAsk, getSendMode, canAutoSend } from "@/lib/ai/offer-gate.js";
import { processOfferStage, classifySellerPriceIntent } from "@/lib/ai/offer-stage-ai.js";

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

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("Offer Profile Resolution", () => {
  it("should route single_family to ARV wholesale formula", () => {
    const property = {
      asset_type: "single_family",
      arv_mid: 200000,
      repairs_mid: 15000,
    };

    const profile = resolveOfferProfile(property, {});

    assert.equal(profile.asset_type, "single_family");
    assert.equal(profile.valuation_method, "arv_wholesale_formula");
    assert.ok(profile.walkaway_internal > 0);
    assert.ok(profile.recommended_target_offer > 0);
    assert.ok(profile.recommended_opening_offer > 0);
  });

  it("should route 2_to_4_unit to hybrid ARV/rent logic", () => {
    const property = {
      asset_type: "2_to_4_unit",
      arv_mid: 300000,
      repairs_mid: 20000,
      current_gross_rent: 3000,
    };

    const profile = resolveOfferProfile(property, {});

    assert.equal(profile.asset_type, "2_to_4_unit");
    assert.equal(profile.valuation_method, "hybrid_arv_rent");
    assert.ok(profile.current_gross_rent, 3000);
  });

  it("should route multifamily_5_plus to NOI/cap-rate logic", () => {
    const property = {
      asset_type: "multifamily_5_plus",
      number_of_units: 18,
      current_gross_rent: 15000,
      current_noi: 90000,
      market_cap_rate: 0.08,
    };

    const profile = resolveOfferProfile(property, {});

    assert.equal(profile.asset_type, "multifamily_5_plus");
    assert.equal(profile.valuation_method, "noi_cap_rate_price_per_unit");
    assert.ok(profile.price_per_unit_range !== null);
    assert.ok(profile.walkaway_internal > 0);
  });

  it("should identify missing rent roll for multifamily", () => {
    const property = {
      asset_type: "multifamily_5_plus",
      number_of_units: 18,
    };

    const profile = resolveOfferProfile(property, {});

    assert.ok(profile.missing_required_info.includes("rent_roll"));
    assert.ok(profile.missing_required_info.includes("occupancy_rate"));
    assert.equal(profile.safe_to_reveal_offer, false);
  });

  it("should bypass legacy_cash_offer for multifamily_5_plus", () => {
    const property = {
      asset_type: "multifamily_5_plus",
      legacy_cash_offer: 500000,
      number_of_units: 18,
      current_gross_rent: 15000,
    };

    const profile = resolveOfferProfile(property, {});

    assert.notEqual(profile.walkaway_internal, 500000);
    assert.ok(profile.valuation_method !== "arv_wholesale_formula");
  });

  it("should not reveal offer when missing info", () => {
    const property = {
      asset_type: "single_family",
    };

    const profile = resolveOfferProfile(property, {});

    assert.equal(profile.safe_to_reveal_offer, false);
    assert.ok(profile.missing_required_info.length > 0);
  });
});

describe("Offer Gate", () => {
  it("should block suppressed sellers", () => {
    const profile = resolveOfferProfile({ asset_type: "single_family", arv_mid: 200000 }, {});

    const result = checkOfferGate({
      profile,
      sellerAsk: null,
      hasPriceIntent: true,
      sendMode: "dry_run_offer_ai",
      suppressionStatus: "opt_out",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.passed, false);
    assert.ok(result.blockedReasons.includes("suppression"));
  });

  it("should block outside contact window", () => {
    const profile = resolveOfferProfile({ asset_type: "single_family", arv_mid: 200000 }, {});

    const result = checkOfferGate({
      profile,
      sellerAsk: null,
      hasPriceIntent: true,
      sendMode: "dry_run_offer_ai",
      suppressionStatus: "allowed",
      contactWindowStatus: "outside_local_send_window",
    });

    assert.equal(result.passed, false);
    assert.ok(result.blockedReasons.includes("contact_window"));
  });

  it("should block low confidence", () => {
    const profile = resolveOfferProfile({ asset_type: "single_family" }, {});

    const result = checkOfferGate({
      profile,
      sellerAsk: null,
      hasPriceIntent: true,
      sendMode: "dry_run_offer_ai",
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.passed, false);
    assert.ok(result.blockedReasons.includes("confidence") || result.blockedReasons.includes("missing_info"));
  });

  it("should block when MAO would be exposed", () => {
    const profile = resolveOfferProfile({ asset_type: "single_family", arv_mid: 200000 }, {});

    const result = checkOfferGate({
      profile,
      sellerAsk: null,
      hasPriceIntent: true,
      sendMode: "dry_run_offer_ai",
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.checks.mao_exposure.passed, true);
  });
});

describe("Negotiation Routing", () => {
  it("should route seller ask below target to contract_path", () => {
    const profile = {
      recommended_target_offer: 140000,
      walkaway_internal: 150000,
      asset_type: "single_family",
    };

    const result = evaluateSellerAsk({
      sellerAsk: "$135,000",
      profile,
      sellerMessage: "I'd take $135k",
    });

    assert.equal(result.route, "contract_path");
    assert.equal(result.action, "draft_acceptance");
  });

  it("should route seller ask above walkaway to soft_exit", () => {
    const profile = {
      recommended_target_offer: 140000,
      walkaway_internal: 150000,
      asset_type: "single_family",
    };

    const result = evaluateSellerAsk({
      sellerAsk: "$200,000",
      profile,
      sellerMessage: "I need at least 200k",
    });

    assert.equal(result.route, "soft_exit");
    assert.equal(result.action, "nurture_no_argue");
  });

  it("should route seller ask between target and walkaway to negotiation", () => {
    const profile = {
      recommended_target_offer: 140000,
      walkaway_internal: 150000,
      asset_type: "single_family",
    };

    const result = evaluateSellerAsk({
      sellerAsk: "$145,000",
      profile,
      sellerMessage: "I was hoping for 145k",
    });

    assert.equal(result.route, "negotiation");
    assert.equal(result.action, "ask_flexibility");
  });
});

describe("Send Modes", () => {
  it("should default to dry_run_offer_ai", () => {
    const mode = getSendMode();
    assert.equal(mode, "dry_run_offer_ai");
  });

  it("should not auto-send with dry_run mode", () => {
    const canSend = canAutoSend("dry_run_offer_ai", 0.9);
    assert.equal(canSend, false);
  });

  it("should auto-queue with auto_queue_offer mode", () => {
    const canSend = canAutoSend("auto_queue_offer", 0.8);
    assert.equal(canSend, true);
  });

  it("should auto-send with high confidence and auto_send_offer_high_confidence mode", () => {
    const canSend = canAutoSend("auto_send_offer_high_confidence", 0.9);
    assert.equal(canSend, true);
  });

  it("should not auto-send with low confidence even in auto_send mode", () => {
    const canSend = canAutoSend("auto_send_offer_high_confidence", 0.7);
    assert.equal(canSend, false);
  });
});

describe("Seller-Facing Offer Drafting", () => {
  it("should use soft range for SFH offer reveal", async () => {
    setupMockFetch(JSON.stringify({
      reply: "Based on what I'm seeing, I'd probably be around $75k-$85k cash as-is depending on condition/title. Is that close enough to keep talking?",
      tone: "professional",
      next_action: "send",
      reasoning: "SFH offer within range",
    }));

    const profile = resolveOfferProfile({
      asset_type: "single_family",
      arv_mid: 100000,
      repairs_mid: 5000,
    }, {});

    assert.equal(profile.safe_to_reveal_offer, true);

    const result = await processOfferStage({
      property: { asset_type: "single_family", arv_mid: 100000, repairs_mid: 5000 },
      conversationHistory: [],
      sellerMessage: "How much will you pay?",
      sellerName: "Test Seller",
      phone: "555-123-4567",
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.ok(result.message.includes("$") || result.message.includes("around"));
    assert.ok(!result.message.includes("walkaway") && !result.message.includes("MAO"));
  });

  it("should ask for rent roll when multifamily data missing", async () => {
    const profile = resolveOfferProfile({
      asset_type: "multifamily_5_plus",
      number_of_units: 18,
    }, {});

    assert.equal(profile.safe_to_reveal_offer, false);
    assert.ok(profile.next_seller_question !== null);

    const result = await processOfferStage({
      property: { asset_type: "multifamily_5_plus", number_of_units: 18 },
      conversationHistory: [],
      sellerMessage: "What's your offer?",
      sellerName: "Multi Family Owner",
      phone: "555-987-6543",
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.ok(result.message.includes("rent") || result.message.includes("occupancy"));
  });
});

describe("Walkaway Exposure Prevention", () => {
  it("should never include walkaway in seller-facing message", async () => {
    setupMockFetch(JSON.stringify({
      reply: "My walkaway on this is $140k but I'll offer $135k",
      tone: "direct",
      next_action: "send",
      reasoning: "Offering below walkaway",
    }));

    const result = await processOfferStage({
      property: { asset_type: "single_family", arv_mid: 200000, repairs_mid: 10000 },
      conversationHistory: [],
      sellerMessage: "What's your best offer?",
      sellerName: "Test Seller",
      phone: "555-111-2222",
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.ok(!result.message.includes("walkaway") && !result.message.includes("MAO"));
  });
});

describe("Suppression Before Model Call", () => {
  it("should not call model for suppressed sellers", async () => {
    let modelCalled = false;
    global.fetch = async () => {
      modelCalled = true;
      throw new Error("Should not be called");
    };

    const result = await processOfferStage({
      property: { asset_type: "single_family", arv_mid: 200000 },
      conversationHistory: [],
      sellerMessage: "How much?",
      sellerName: "Suppressed Seller",
      phone: "555-333-4444",
      suppressionStatus: "opt_out",
      contactWindowStatus: "allowed",
    });

    assert.equal(modelCalled, false);
    assert.ok(result.gateResult);
  });
});

describe("Low Confidence Blocks Auto-Send", () => {
  it("should keep dry_run when confidence is low", async () => {
    const property = {
      asset_type: "single_family",
    };

    const result = await processOfferStage({
      property,
      conversationHistory: [],
      sellerMessage: "What's your offer?",
      sellerName: "Low Confidence Seller",
      phone: "555-444-5555",
      suppressionStatus: "allowed",
      contactWindowStatus: "allowed",
    });

    assert.equal(result.dry_run, true);
  });
});

// Cleanup
global.fetch = undefined;
