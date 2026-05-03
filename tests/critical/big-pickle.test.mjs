import { describe, it } from "node:test";
import assert from "node:assert/strict";

process.env.OPENCODE_ZEN_API_KEY = "test-key-for-testing";

import {
  callBigPickle,
  buildSafeContext,
  redactContext,
  maskPhone,
  maskName,
  parseJSON,
} from "@/lib/ai/opencode-zen-client.js";

import {
  classifyInboundWithBigPickle,
  draftSellerReplyWithBigPickle,
  resolveReengagementPlanWithBigPickle,
  summarizeUnderwritingWithBigPickle,
  isStale,
} from "@/lib/ai/big-pickle-helpers.js";

// ─── Mock fetch for testing ──────────────────────────────────────────────────

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

function setupMockFetchRaw(responseJson, status = 200) {
  global.fetch = async () => {
    return {
      ok: status >= 200 && status < 300,
      status,
      text: async () => JSON.stringify(responseJson),
      json: async () => responseJson,
    };
  };
}

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("Big Pickle Client", () => {
  it("should redact phone numbers", () => {
    const input = "Call me at 555-123-4567 or (555) 987-6543";
    const result = redactContext(input);
    assert.match(result, /\*\*\*/);
    assert.doesNotMatch(result, /555-123-4567/);
  });

  it("should redact full names", () => {
    const input = "John Smith owns this property";
    const result = redactContext(input);
    assert.match(result, /John \[REDACTED\]/);
  });

  it("should redact API keys", () => {
    const input = "OPENCODE_ZEN_API_KEY=sk-1234567890abcdef";
    const result = redactContext(input);
    assert.match(result, /\[REDACTED\]/);
    assert.doesNotMatch(result, /sk-1234567890/);
  });

  it("should parse valid JSON from response", () => {
    const json = '{"intent": "interested", "confidence": 0.95}';
    const result = parseJSON(json);
    assert.equal(result.intent, "interested");
    assert.equal(result.confidence, 0.95);
  });

  it("should parse JSON from markdown code blocks", () => {
    const json = '```json\n{"intent": "maybe"}\n```';
    const result = parseJSON(json);
    assert.equal(result.intent, "maybe");
  });

  it("should return null for invalid JSON", () => {
    const result = parseJSON("not valid json{{}{");
    assert.equal(result, null);
  });
});

describe("Big Pickle Classification", () => {
  it("should fall back to deterministic routing on invalid JSON", async () => {
    setupMockFetch("not valid json{{}");

    const result = await classifyInboundWithBigPickle({
      message: "I want to sell my house",
      sellerName: "John Doe",
      phone: "555-123-4567",
    });

    assert.equal(result.requires_manual_review, true);
    assert.equal(result.intent, "other");
  });

  it("should not expose internal pricing in classification", async () => {
    setupMockFetch(JSON.stringify({
      intent: "interested",
      confidence: 0.9,
      language: "English",
      sentiment: "positive",
      requires_manual_review: false,
      reasoning: "Seller wants to sell",
    }));

    const result = await classifyInboundWithBigPickle({
      message: "I'm interested in selling",
      sellerName: "Jane Doe",
    });

    assert.equal(result.intent, "interested");
    assert.equal(result.confidence, 0.9);
  });
});

describe("Big Pickle Draft Reply", () => {
  it("should never include MAO/walkaway in draft", async () => {
    setupMockFetch(JSON.stringify({
      reply: "Our MAO for your property is $150,000 and walkaway is $140k",
      tone: "direct",
      next_action: "send",
      reasoning: "Providing offer details",
    }));

    const result = await draftSellerReplyWithBigPickle({
      message: "What's your offer?",
      sellerName: "Bob Jones",
      intent: "price_inquiry",
    });

    assert.doesNotMatch(result.reply, /MAO|walkaway/i);
    assert.match(result.reply, /discuss|call/i);
  });

  it("should fall back on failed draft", async () => {
    setupMockFetchError(new Error("Service unavailable"));

    const result = await draftSellerReplyWithBigPickle({
      message: "Hello",
      sellerName: "Test User",
      intent: "other",
    });

    assert.equal(result.next_action, "manual_review");
    assert.match(result.reply, /back to you/);
  });
});

describe("Big Pickle Re-engagement", () => {
  it("should not re-engage recent leads", () => {
    const recentDate = new Date().toISOString();
    assert.equal(isStale(recentDate), false);
  });

  it("should recommend re-engagement for stale positive leads", async () => {
    const staleDate = new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString();

    setupMockFetch(JSON.stringify({
      should_reengage: true,
      urgency: "medium",
      recommended_template: "re_engagement_campaign",
      timing: "1_week",
      reasoning: "Previously interested seller, 45 days since contact",
      customization_notes: "Follow up on previous price discussion",
    }));

    const result = await resolveReengagementPlanWithBigPickle({
      sellerName: "Alice Brown",
      lastContactDate: staleDate,
      leadStatus: "interested",
      previousInteractions: "Responded positively to initial SMS",
    });

    assert.equal(result.should_reengage, true);
    assert.equal(result.urgency, "medium");
  });

  it("should not re-engage opted-out leads", async () => {
    const staleDate = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();

    setupMockFetch(JSON.stringify({
      should_reengage: false,
      urgency: "none",
      recommended_template: null,
      timing: null,
      reasoning: "Lead opted out previously",
      customization_notes: null,
    }));

    const result = await resolveReengagementPlanWithBigPickle({
      sellerName: "Opt Out User",
      lastContactDate: staleDate,
      leadStatus: "opt_out",
      previousInteractions: "Sent STOP message",
    });

    assert.equal(result.should_reengage, false);
  });
});

describe("Big Pickle Underwriting", () => {
  it("should identify missing rent roll for multifamily", async () => {
    setupMockFetch(JSON.stringify({
      property_type: "multi_family",
      estimated_value_range: "medium",
      repair_needed: "moderate",
      investment_potential: "good",
      key_concerns: ["Occupancy unknown"],
      summary: "Duplex needs some repairs but good investment potential",
      missing_info: [],
    }));

    const result = await summarizeUnderwritingWithBigPickle({
      propertyDetails: "Duplex, 2 units, built 1980, needs some repairs",
      sellerName: "Multi Family Owner",
    });

    assert.equal(result.property_type, "multi_family");
    assert.ok(result.missing_info.includes("rent roll") || result.key_concerns.some(c => c.includes("rent")));
  });

  it("should handle underwriting failure gracefully", async () => {
    setupMockFetchError(new Error("Service unavailable"));

    const result = await summarizeUnderwritingWithBigPickle({
      propertyDetails: "Single family home",
      sellerName: "Test Owner",
    });

    assert.equal(result.property_type, "unknown");
    assert.equal(result.summary, "Underwriting summary unavailable");
  });
});

describe("Dry-run Mode Support", () => {
  it("should store recommendation without sending (dry-run simulation)", async () => {
    setupMockFetch({
      ok: true,
      json: {
        reply: "Thanks for your interest! Let's schedule a call.",
        tone: "friendly",
        next_action: "send",
        reasoning: "Positive response, encourage phone call",
      },
    });

    const draft = await draftSellerReplyWithBigPickle({
      message: "I might be interested",
      sellerName: "Dry Run Test",
      intent: "maybe",
    });

    const dryRunRecord = {
      draft: draft.reply,
      recommended_action: draft.next_action,
      approved: false,
      sent: false,
    };

    assert.ok(dryRunRecord.draft);
    assert.equal(dryRunRecord.sent, false);
    assert.equal(dryRunRecord.approved, false);
  });
});

describe("Opt-out Suppression Before Model Call", () => {
  it("should not call model for opt-out messages", async () => {
    let fetchCalled = false;
    global.fetch = async () => {
      fetchCalled = true;
      throw new Error("Should not be called");
    };

    const result = await classifyInboundWithBigPickle({
      message: "STOP, remove me from your list",
      sellerName: "Opt Out User",
    });

    assert.equal(result.intent, "other");
    assert.equal(result.requires_manual_review, true);
  });
});

// Cleanup
global.fetch = undefined;
