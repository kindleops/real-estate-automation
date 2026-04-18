import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  __resetBrainAuthorityTestDeps,
  __setBrainAuthorityTestDeps,
  applyBrainStateUpdate,
  buildBrainRelationshipFields,
  buildDeterministicBrainStateFields,
  buildDeliveryBrainStateFields,
  buildInboundBrainStateFields,
  buildLinkedMessageEventsFields,
  buildOutboundBrainStateFields,
  buildStageBrainStateFields,
} from "@/lib/domain/brain/brain-authority.js";

afterEach(() => {
  __resetBrainAuthorityTestDeps();
});

test("applyBrainStateUpdate writes a compact authoritative brain patch", async () => {
  let updated = null;

  __setBrainAuthorityTestDeps({
    updateBrainItem: async (brain_id, fields) => {
      updated = { brain_id, fields };
      return { ok: true };
    },
  });

  const result = await applyBrainStateUpdate({
    brain_id: 701,
    reason: "test_patch",
    fields: {
      "conversation-stage": "Ownership Confirmation",
      "ai-route": "Ownership Confirmation",
      "next-follow-up-due-at": null,
      ignored: undefined,
    },
  });

  assert.equal(result.ok, true);
  assert.deepEqual(updated, {
    brain_id: 701,
    fields: {
      "conversation-stage": "Ownership Confirmation",
      "ai-route": "Ownership Confirmation",
      "next-follow-up-due-at": null,
    },
  });
});

test("buildInboundBrainStateFields produces the canonical inbound brain patch", () => {
  const now = new Date("2026-04-11T12:00:00.000Z");
  const fields = buildInboundBrainStateFields({
    message_body: "Still interested",
    follow_up_trigger_state: "AI Running",
    extra_fields: {
      "conversation-stage": "Offer Interest Confirmation",
      "ai-route": "Offer Interest",
    },
    now,
  });

  assert.equal(fields["last-inbound-message"], "Still interested");
  assert.deepEqual(fields["last-contact-timestamp"], { start: "2026-04-11 12:00:00" });
  assert.equal(fields["follow-up-trigger-state"], "AI Running");
  assert.equal(fields["conversation-stage"], "Offer Interest Confirmation");
  assert.equal(fields["ai-route"], "Offer Interest");
});

test("buildDeterministicBrainStateFields maps inbound deterministic state onto live brain fields", () => {
  const fields = buildDeterministicBrainStateFields({
    deterministic_state: {
      conversation_stage: "Negotiation",
      lifecycle_stage_number: 6,
      current_conversation_branch: "Negotiation",
      current_seller_state: "Negotiating",
      follow_up_step: "B",
      next_follow_up_due_at: null,
      last_detected_intent: "Negotiation",
      seller_profile: "Probate",
      language_preference: "Spanish",
      gender: "Neutral",
      status_ai_managed: "Active Negotiation",
      deal_priority_tag: "Urgent",
      seller_motivation_score: 88,
      risk_flags_ai: ["Wants Too High", "Unknown", "Wants Too High"],
      last_message_summary_ai: "Seller wants a higher number.",
      full_conversation_summary_ai: "Stage 6 Negotiation. Seller is above range.",
      ai_recommended_next_move:
        "Justify pricing and test creative flexibility.",
      ai_next_message:
        "I understand. To see if we can bridge the gap, tell me more about condition and timing.",
      seller_emotional_tone: "Motivated",
      response_style_mode: "Direct",
      primary_objection_type: "Price Too Low",
      seller_ask_price: 255000,
      cash_offer_target: 230000,
      creative_branch_eligibility: "Maybe",
      deal_strategy_branch: "Hybrid",
    },
  });

  assert.equal(fields["conversation-stage"], "Negotiation");
  assert.equal(fields.number, 6);
  assert.equal(fields["ai-route"], "Negotiation");
  assert.equal(fields["current-seller-state"], "Negotiating");
  assert.equal(fields["follow-up-step"], "B");
  assert.equal(fields["next-follow-up-due-at"], null);
  assert.equal(fields["last-detected-intent"], "Negotiation");
  assert.equal(fields["seller-profile"], "Probate");
  assert.equal(fields["language-preference"], "Spanish");
  assert.equal(fields.gender, "Neutral");
  assert.equal(fields["status-ai-managed"], "Active Negotiation");
  assert.equal(fields["deal-prioirty-tag"], "Urgent");
  assert.equal(fields["seller-motivation-score"], 88);
  assert.deepEqual(fields["risk-flags-ai"], ["Wants Too High", "Unknown"]);
  assert.equal(fields.transcript, "Seller wants a higher number.");
  assert.equal(
    fields.title,
    "Stage 6 Negotiation. Seller is above range."
  );
  assert.equal(
    fields["ais-recommended-next-move"],
    "Justify pricing and test creative flexibility."
  );
  assert.equal(
    fields["ai-next-message"],
    "I understand. To see if we can bridge the gap, tell me more about condition and timing."
  );
  assert.equal(fields.category, "Motivated");
  assert.equal(fields["category-2"], "Direct");
  assert.equal(fields["category-3"], "Price Too Low");
  assert.equal(fields["seller-asking-price"], 255000);
  assert.equal(fields["cash-offer-target"], 230000);
  assert.equal(fields["category-4"], "Maybe");
  assert.equal(fields["category-5"], "Hybrid");
});

test("buildOutboundBrainStateFields advances follow-up automation from authoritative brain state", () => {
  const fields = buildOutboundBrainStateFields({
    message_body: "Checking back in",
    template_id: 9901,
    conversation_stage: "Ownership Confirmation",
    current_follow_up_step: "None",
    status_ai_managed: "Warm Lead",
    now: "2026-04-11T12:00:00.000Z",
    extra_fields: buildBrainRelationshipFields({
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      sms_agent_id: 701,
    }),
  });

  assert.equal(fields["last-outbound-message"], "Checking back in");
  assert.deepEqual(fields["last-sent-time"], { start: "2026-04-11 12:00:00" });
  assert.equal(fields["last-template-sent"], 9901);
  assert.equal(fields["follow-up-step"], "A");
  assert.equal(fields["follow-up-trigger-state"], "Waiting");
  assert.equal(fields["phone-number"], 401);
  assert.equal(fields["master-owner"], 201);
  assert.equal(fields.prospect, 301);
  assert.deepEqual(fields.properties, [601]);
  assert.equal(fields["sms-agent"], 701);
  assert.equal(fields["conversation-stage"], undefined);
});

test("buildBrainRelationshipFields maps authoritative thread links onto Brain fields", () => {
  assert.deepEqual(
    buildBrainRelationshipFields({
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      sms_agent_id: 701,
      ai_agent_assigned_id: 701,
    }),
    {
      "phone-number": 401,
      "master-owner": 201,
      prospect: 301,
      properties: [601],
      "sms-agent": 701,
      "ai-agent-assigned": 701,
    }
  );
});

test("buildStageBrainStateFields normalizes live stage labels into authoritative brain stage data", () => {
  const fields = buildStageBrainStateFields({
    stage: "Signed / Closing",
  });

  assert.equal(fields["conversation-stage"], "Signed / Closing");
  assert.equal(fields.number, 9);
});

test("buildDeliveryBrainStateFields only mutates authoritative brain state for terminal delivery updates", () => {
  assert.deepEqual(buildDeliveryBrainStateFields({ delivery_status: "delivered" }), {
    "follow-up-trigger-state": "Waiting",
  });
  assert.deepEqual(buildDeliveryBrainStateFields({ delivery_status: "failed" }), {
    "follow-up-trigger-state": "Paused",
    "status-ai-managed": "Paused",
  });
  assert.deepEqual(buildDeliveryBrainStateFields({ delivery_status: "sent" }), {});
});

test("buildInboundBrainStateFields keeps deterministic stage data while refreshing summaries", () => {
  const fields = buildInboundBrainStateFields({
    message_body: "I would take 190000.",
    follow_up_trigger_state: "AI Running",
    deterministic_state: {
      conversation_stage: "Negotiation",
      lifecycle_stage_number: 6,
      current_conversation_branch: "Negotiation",
      current_seller_state: "Above Range",
      last_message_summary_ai: "Seller gave a high ask.",
      full_conversation_summary_ai: "Stage 6 negotiation with above-range ask.",
      ai_recommended_next_move: "Justify pricing and probe flexibility.",
      ai_next_message: "I understand. Can you tell me more about condition and timing?",
    },
  });

  assert.equal(fields["conversation-stage"], "Negotiation");
  assert.equal(fields.number, 6);
  assert.equal(fields["ai-route"], "Negotiation");
  assert.equal(fields["current-seller-state"], "Above Range");
  assert.equal(fields.transcript, "Seller gave a high ask.");
  assert.equal(
    fields.title,
    "Stage 6 negotiation with above-range ask."
  );
  assert.equal(
    fields["ais-recommended-next-move"],
    "Justify pricing and probe flexibility."
  );
  assert.equal(
    fields["ai-next-message"],
    "I understand. Can you tell me more about condition and timing?"
  );
});

test("buildInboundBrainStateFields updates inbound truth without sending blank outbound fields", () => {
  const fields = buildInboundBrainStateFields({
    message_body: "You can text me later this afternoon.",
    follow_up_trigger_state: "AI Running",
    extra_fields: {
      "master-owner": 201,
      prospect: 301,
      properties: [601],
      "sms-agent": 701,
    },
    now: new Date("2026-04-12T15:00:00.000Z"),
  });

  assert.equal(fields["last-inbound-message"], "You can text me later this afternoon.");
  assert.deepEqual(fields["last-contact-timestamp"], { start: "2026-04-12 15:00:00" });
  assert.equal(fields["follow-up-trigger-state"], "AI Running");
  assert.equal(fields["master-owner"], 201);
  assert.equal(fields.prospect, 301);
  assert.deepEqual(fields.properties, [601]);
  assert.equal(fields["sms-agent"], 701);
  assert.equal("last-outbound-message" in fields, false);
  assert.equal("last-sent-time" in fields, false);
  assert.equal("last-template-sent" in fields, false);
});

test("buildLinkedMessageEventsFields appends a new message event without duplicating existing refs", () => {
  assert.deepEqual(
    buildLinkedMessageEventsFields({
      current_message_event_ids: [9001, "9002", 9002],
      message_event_id: 9003,
    }),
    {
      "linked-message-events": [9001, 9002, 9003],
    }
  );
});
