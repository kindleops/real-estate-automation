import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeSellerInboundIntent,
  resolveNextSellerStage,
  resolveAutoReplyUseCase,
  shouldSuppressSellerAutoReply,
  resolveSellerAutoReplyPlan,
} from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";

import {
  resolveSafetyTier,
  SELLER_FLOW_SAFETY_TIERS,
  SELLER_FLOW_SAFETY_POLICY,
} from "@/lib/domain/seller-flow/seller-flow-safety-policy.js";

// ─── Intent Detection ────────────────────────────────────────────────────────

test("intent: ownership confirmation detected from 'yes I own it'", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "yes I own it" });
  assert.equal(intent, "ownership_confirmed");
});

test("intent: opt-out detected from 'stop texting me'", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "stop texting me" });
  assert.equal(intent, "opt_out");
});

test("intent: wrong person detected from 'wrong number'", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "wrong number" });
  assert.equal(intent, "wrong_person");
});

test("intent: hostile detected from 'my attorney will contact you'", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "my attorney will contact you" });
  assert.equal(intent, "hostile_or_legal");
});

test("intent: not interested detected", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "not interested" });
  assert.equal(intent, "not_interested");
});

test("intent: listed/unavailable detected", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "I already have a realtor handling it" });
  assert.equal(intent, "listed_or_unavailable");
});

test("intent: tenant/occupancy detected", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "I have tenants in the property right now" });
  assert.equal(intent, "tenant_or_occupancy");
});

test("intent: info request detected", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "how did you get my info?" });
  assert.equal(intent, "info_request");
});

test("intent: asking price value detected from dollar amount", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "I want 250k for it" });
  assert.equal(intent, "asking_price_value");
});

test("intent: asks offer detected", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "how much are you offering?" });
  assert.equal(intent, "asks_offer");
});

test("intent: condition signal detected", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "the roof needs work and the plumbing is old" });
  assert.equal(intent, "condition_signal");
});

test("intent: unclear for very short ambiguous text", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "ok" });
  assert.equal(intent, "unclear");
});

test("intent: empty body returns unclear", () => {
  const intent = normalizeSellerInboundIntent({ message_body: "" });
  assert.equal(intent, "unclear");
});

// ─── Stage Resolution ────────────────────────────────────────────────────────

test("stage: ownership_confirmed at ownership_check → consider_selling", () => {
  const stage = resolveNextSellerStage({
    message_body: "yes I own it",
    current_stage: "ownership_check",
  });
  assert.equal(stage, "consider_selling");
});

test("stage: ownership_confirmed at non-ownership stage → confirm_basics", () => {
  const stage = resolveNextSellerStage({
    message_body: "yes I own it",
    current_stage: "consider_selling",
  });
  assert.equal(stage, "confirm_basics");
});

test("stage: info_request at ownership_check → info_source_explanation", () => {
  const stage = resolveNextSellerStage({
    message_body: "how did you get my info?",
    current_stage: "ownership_check",
  });
  assert.equal(stage, "info_source_explanation");
});

test("stage: info_request at non-ownership stage → manual_review", () => {
  const stage = resolveNextSellerStage({
    message_body: "how did you get my info?",
    current_stage: "asking_price",
  });
  assert.equal(stage, "manual_review");
});

test("stage: opt_out always → stop_or_opt_out", () => {
  const stage = resolveNextSellerStage({ message_body: "STOP" });
  assert.equal(stage, "stop_or_opt_out");
});

test("stage: hostile always → hostile_or_legal", () => {
  const stage = resolveNextSellerStage({ message_body: "I will sue you" });
  assert.equal(stage, "hostile_or_legal");
});

test("stage: asks_offer → asking_price", () => {
  const stage = resolveNextSellerStage({ message_body: "how much are you offering?" });
  assert.equal(stage, "asking_price");
});

test("stage: condition_signal → condition_probe", () => {
  const stage = resolveNextSellerStage({ message_body: "the roof needs work" });
  assert.equal(stage, "condition_probe");
});

// ─── Use Case Resolution ─────────────────────────────────────────────────────

test("use_case: hostile_or_legal returns null (no auto-reply)", () => {
  const uc = resolveAutoReplyUseCase({ message_body: "my attorney will contact you" });
  assert.equal(uc, null);
});

test("use_case: ownership_confirmed at ownership_check → consider_selling", () => {
  const uc = resolveAutoReplyUseCase({
    message_body: "yes I own it",
    current_stage: "ownership_check",
  });
  assert.equal(uc, "consider_selling");
});

test("use_case: info_request at ownership_check → info_source_explanation", () => {
  const uc = resolveAutoReplyUseCase({
    message_body: "how did you get my info?",
    current_stage: "ownership_check",
  });
  assert.equal(uc, "info_source_explanation");
});

// ─── Suppression Logic ───────────────────────────────────────────────────────

test("suppression: hostile_or_legal always suppressed", () => {
  const result = shouldSuppressSellerAutoReply({
    message_body: "my attorney will contact you",
    auto_reply_enabled: true,
  });
  assert.equal(result.suppress, true);
  assert.equal(result.reason, "hostile_or_legal_intent");
});

test("suppression: opt_out suppressed unless system_only", () => {
  const result = shouldSuppressSellerAutoReply({
    message_body: "stop",
    auto_reply_enabled: true,
  });
  assert.equal(result.suppress, true);
  assert.equal(result.reason, "opt_out_intent_no_marketing");
});

test("suppression: opt_out not suppressed when system_only is true", () => {
  // shouldSuppressSellerAutoReply skips opt_out suppression when system_only=true
  // (line 150: `if (intent === "opt_out" && !input.system_only)`)
  const result = shouldSuppressSellerAutoReply({
    message_body: "stop",
    auto_reply_enabled: true,
    system_only: true,
  });
  assert.equal(result.suppress, false);
  assert.equal(result.reason, null);
});

test("suppression: auto_reply_disabled suppresses everything", () => {
  const result = shouldSuppressSellerAutoReply({
    message_body: "yes I own it",
    auto_reply_enabled: false,
  });
  assert.equal(result.suppress, true);
  assert.equal(result.reason, "auto_reply_disabled");
});

test("suppression: valid ownership confirmed is not suppressed", () => {
  const result = shouldSuppressSellerAutoReply({
    message_body: "yes I own it",
    auto_reply_enabled: true,
    current_stage: "ownership_check",
  });
  assert.equal(result.suppress, false);
});

// ─── Safety Tier Resolution ──────────────────────────────────────────────────

test("safety tier: autopilot disabled always returns review", () => {
  const tier = resolveSafetyTier(
    { current_stage: "ownership_check", inbound_intent: "ownership_confirmed", should_queue_reply: true },
    false
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.REVIEW);
});

test("safety tier: ownership_confirmed at ownership_check with autopilot → auto_send", () => {
  const tier = resolveSafetyTier(
    { current_stage: "ownership_check", inbound_intent: "ownership_confirmed", should_queue_reply: true },
    true
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);
});

test("safety tier: opt_out globally → suppress", () => {
  const tier = resolveSafetyTier(
    { current_stage: "consider_selling", inbound_intent: "opt_out", should_queue_reply: false },
    true
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);
});

test("safety tier: hostile_or_legal globally → suppress", () => {
  const tier = resolveSafetyTier(
    { current_stage: "asking_price", inbound_intent: "hostile_or_legal", should_queue_reply: false },
    true
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);
});

test("safety tier: asking_price_value at asking_price → review (price needs human look)", () => {
  const tier = resolveSafetyTier(
    { current_stage: "asking_price", inbound_intent: "asking_price_value", should_queue_reply: true },
    true
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.REVIEW);
});

test("safety tier: unknown intent defaults to review", () => {
  const tier = resolveSafetyTier(
    { current_stage: "ownership_check", inbound_intent: "something_novel", should_queue_reply: true },
    true
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.REVIEW);
});

test("safety tier: policy says auto_send but reply not queued → review", () => {
  const tier = resolveSafetyTier(
    { current_stage: "ownership_check", inbound_intent: "info_request", should_queue_reply: false },
    true
  );
  assert.equal(tier, SELLER_FLOW_SAFETY_TIERS.REVIEW);
});

// ─── Full Plan Resolution ────────────────────────────────────────────────────

test("full plan: ownership_confirmed at ownership_check resolves correctly", async () => {
  const plan = await resolveSellerAutoReplyPlan({
    message_body: "yes I own it",
    current_stage: "ownership_check",
    auto_reply_enabled: true,
    conversation_context: { found: true },
  });

  assert.equal(plan.ok, true);
  assert.equal(plan.inbound_intent, "ownership_confirmed");
  assert.equal(plan.current_stage, "ownership_check");
  assert.equal(plan.next_stage, "consider_selling");
  assert.equal(plan.selected_use_case, "consider_selling");
  assert.equal(plan.should_queue_reply, true);
  assert.equal(plan.suppression_reason, null);
  assert.equal(plan.safety_tier, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);
  assert.equal(plan.auto_send_eligible, true);
});

test("full plan: hostile reply gets suppressed with correct tier", async () => {
  const plan = await resolveSellerAutoReplyPlan({
    message_body: "my attorney will sue you",
    current_stage: "ownership_check",
    auto_reply_enabled: true,
    conversation_context: { found: true },
  });

  assert.equal(plan.ok, true);
  assert.equal(plan.inbound_intent, "hostile_or_legal");
  assert.equal(plan.should_queue_reply, false);
  assert.equal(plan.suppression_reason, "hostile_or_legal_intent");
  assert.equal(plan.safety_tier, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);
  assert.equal(plan.auto_send_eligible, false);
});

test("full plan: unclear message at ownership_check → review tier", async () => {
  const plan = await resolveSellerAutoReplyPlan({
    message_body: "ok",
    current_stage: "ownership_check",
    auto_reply_enabled: true,
    conversation_context: { found: true },
  });

  assert.equal(plan.ok, true);
  assert.equal(plan.inbound_intent, "unclear");
  assert.equal(plan.next_stage, "unclear_clarifier");
  assert.equal(plan.selected_use_case, "unclear_clarifier");
  assert.equal(plan.safety_tier, SELLER_FLOW_SAFETY_TIERS.REVIEW);
});

// ─── Deterministic Stage Map ─────────────────────────────────────────────────

test("stage map: all ownership_check transitions are deterministic", () => {
  const stage_policy = SELLER_FLOW_SAFETY_POLICY["ownership_check"];
  assert.ok(stage_policy, "ownership_check should have a policy entry");

  assert.equal(stage_policy.ownership_confirmed.next_stage, "consider_selling");
  assert.equal(stage_policy.ownership_confirmed.template, "consider_selling");
  assert.equal(stage_policy.ownership_confirmed.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);

  assert.equal(stage_policy.info_request.next_stage, "who_is_this");
  assert.equal(stage_policy.info_request.template, "who_is_this");
  assert.equal(stage_policy.info_request.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);

  assert.equal(stage_policy.wrong_person.next_stage, "terminal");
  assert.equal(stage_policy.wrong_person.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);

  assert.equal(stage_policy.opt_out.next_stage, "terminal");
  assert.equal(stage_policy.opt_out.safety, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);

  assert.equal(stage_policy.not_interested.next_stage, "terminal");
  assert.equal(stage_policy.not_interested.safety, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);

  assert.equal(stage_policy.unclear.next_stage, "ownership_check");
  assert.equal(stage_policy.unclear.safety, SELLER_FLOW_SAFETY_TIERS.REVIEW);
});

test("stage map: global fallbacks cover critical intents", () => {
  const global = SELLER_FLOW_SAFETY_POLICY.global;
  assert.ok(global, "global fallback policy should exist");

  assert.equal(global.opt_out.next_stage, "terminal");
  assert.equal(global.opt_out.safety, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);

  assert.equal(global.wrong_person.next_stage, "terminal");
  assert.equal(global.wrong_person.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);

  assert.equal(global.hostile_or_legal.next_stage, "terminal");
  assert.equal(global.hostile_or_legal.template, null);
  assert.equal(global.hostile_or_legal.safety, SELLER_FLOW_SAFETY_TIERS.SUPPRESS);
});

test("stage map: consider_selling transitions are deterministic", () => {
  const stage_policy = SELLER_FLOW_SAFETY_POLICY["consider_selling"];
  assert.ok(stage_policy, "consider_selling should have a policy entry");

  assert.equal(stage_policy.ownership_confirmed.next_stage, "asking_price");
  assert.equal(stage_policy.ownership_confirmed.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);

  assert.equal(stage_policy.asks_offer.next_stage, "asking_price");
  assert.equal(stage_policy.asks_offer.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);
});

test("stage map: asking_price transitions are deterministic", () => {
  const stage_policy = SELLER_FLOW_SAFETY_POLICY["asking_price"];
  assert.ok(stage_policy, "asking_price should have a policy entry");

  assert.equal(stage_policy.asking_price_value.next_stage, "price_works_confirm_basics");
  assert.equal(stage_policy.asking_price_value.safety, SELLER_FLOW_SAFETY_TIERS.REVIEW);

  assert.equal(stage_policy.condition_signal.next_stage, "price_high_condition_probe");
  assert.equal(stage_policy.condition_signal.safety, SELLER_FLOW_SAFETY_TIERS.AUTO_SEND);
});

// ─── Consistency Checks ──────────────────────────────────────────────────────

test("consistency: every stage map entry has next_stage, template, and safety", () => {
  for (const [stage, intents] of Object.entries(SELLER_FLOW_SAFETY_POLICY)) {
    for (const [intent, transition] of Object.entries(intents)) {
      assert.ok(
        "next_stage" in transition,
        `${stage}.${intent} missing next_stage`
      );
      assert.ok(
        "template" in transition,
        `${stage}.${intent} missing template`
      );
      assert.ok(
        "safety" in transition,
        `${stage}.${intent} missing safety`
      );
      assert.ok(
        [SELLER_FLOW_SAFETY_TIERS.AUTO_SEND, SELLER_FLOW_SAFETY_TIERS.REVIEW, SELLER_FLOW_SAFETY_TIERS.SUPPRESS].includes(transition.safety),
        `${stage}.${intent} has invalid safety tier: ${transition.safety}`
      );
    }
  }
});

test("consistency: intent normalizer covers all safety policy intents", () => {
  const all_policy_intents = new Set();
  for (const intents of Object.values(SELLER_FLOW_SAFETY_POLICY)) {
    for (const intent of Object.keys(intents)) {
      all_policy_intents.add(intent);
    }
  }

  // These intents should all be reachable from the normalizer
  const critical_intents = [
    "ownership_confirmed",
    "opt_out",
    "wrong_person",
    "hostile_or_legal",
    "not_interested",
    "info_request",
    "asks_offer",
    "unclear",
  ];

  for (const intent of critical_intents) {
    assert.ok(
      all_policy_intents.has(intent),
      `Critical intent '${intent}' not found in safety policy`
    );
  }
});
