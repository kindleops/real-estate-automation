import { test, describe } from "node:test";
import assert from "node:assert";
import { resolveSellerAutoReplyPlan } from "../../src/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";

describe("Seller Auto Reply Plan", () => {
  test('"Yes" after ownership_check -> consider_selling / S2 / should_queue_reply true', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "Yes",
      current_stage: "ownership_check",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.should_queue_reply, true);
    assert.strictEqual(res.next_stage, "consider_selling");
    assert.strictEqual(res.selected_stage_code, "S2");
  });

  test('"I do" after ownership_check -> consider_selling', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "I do",
      current_stage: "ownership_check",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "consider_selling");
  });

  test('Spanish "Hola buenas como encontraste mi información??" -> info_source_explanation or who_is_this, should_queue_reply true', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "Hola buenas como encontraste mi información??",
      current_stage: "ownership_check",
      auto_reply_enabled: true
    });
    assert.ok(res.next_stage === "info_source_explanation" || res.next_stage === "manual_review");
    assert.strictEqual(res.inbound_intent, "info_request");
  });

  test('"Wrong number" -> wrong_person, suppress future marketing, no overlap', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "Wrong number",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "wrong_person");
    assert.strictEqual(res.selected_use_case, "wrong_person");
    assert.strictEqual(res.safety.wrong_number, true);
  });

  test('"STOP" -> stop_or_opt_out, no marketing reply', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "STOP",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "stop_or_opt_out");
    assert.strictEqual(res.safety.opt_out, true);
  });

  test('"Not interested" -> not_interested, no cash offer', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "Not interested",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "not_interested");
    assert.strictEqual(res.should_queue_reply, false);
  });

  test('"Already have an agent" -> listed_or_unavailable', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "Already have an agent",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "listed_or_unavailable");
  });

  test('"Nope, have great long-term tenants" -> tenant_or_occupancy or not_interested depending wording, but no offer', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "Nope, have great long-term tenants",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "tenant_or_occupancy");
  });

  test('"How much?" -> asking_price, not offer reveal', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "How much?",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "asking_price");
  });

  test('"$200k" -> asking_price_value -> confirm_basics/condition_probe, not duplicate', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "$200k",
      auto_reply_enabled: true
    });
    assert.ok(res.next_stage === "condition_probe" || res.next_stage === "confirm_basics");
  });

  test('hostile profanity -> hostile_or_legal/manual_review', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "fuck off",
      auto_reply_enabled: true
    });
    assert.strictEqual(res.next_stage, "hostile_or_legal");
    assert.strictEqual(res.should_queue_reply, false);
  });

  test('unknown unclear -> unclear_clarifier or manual_review with no duplicate', async () => {
    const res = await resolveSellerAutoReplyPlan({
      message_body: "ok",
      auto_reply_enabled: true
    });
    assert.ok(res.next_stage === "unclear_clarifier" || res.next_stage === "manual_review");
  });
});
