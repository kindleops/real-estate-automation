import test from "node:test";
import assert from "node:assert/strict";

import {
  buildVerificationDiagnostics,
  normalizeInboundReplayMode,
} from "@/lib/diagnostics/inbound-replay-verifier.js";

function buildMatchedContext() {
  return {
    found: true,
    ids: {
      brain_item_id: 11,
      conversation_brain_id: 11,
      master_owner_id: 21,
      property_id: 41,
      template_id: 901,
    },
    summary: {
      conversation_stage: "ownership_check",
      seller_first_name: "Chris",
      property_address: "123 Main St",
      property_city: "Dallas",
    },
    recent: {
      outbound_pair_match: {
        matched_queue_id: "sq-123",
        matched_queue_status: "sent",
        matched_sent_at: "2026-04-29T01:00:00.000Z",
        matched_source: "recent_outbound_send_queue",
        match_strategy: "valid_sent_contextual_outbound",
        context_verified: true,
        from_phone_number: "+13235550000",
        to_phone_number: "+17135550000",
        template_id: 901,
        use_case_template: "consider_selling",
      },
      recent_events: [
        {
          direction: "outbound",
          from_phone_number: "+13235550000",
          to_phone_number: "+17135550000",
          template_id: 901,
          use_case_template: "consider_selling",
        },
      ],
    },
  };
}

function buildDeps(overrides = {}) {
  return {
    loadContext: async () => buildMatchedContext(),
    loadContextWithFallback: async () => buildMatchedContext(),
    classify: async () => ({ language: "English", source: "test" }),
    resolveRoute: () => ({ use_case: "consider_selling" }),
    resolveSellerAutoReplyPlan: async () => ({
      inbound_intent: "ownership_confirmed",
      current_stage: "ownership_check",
      next_stage: "consider_selling",
      selected_use_case: "consider_selling",
      selected_language: "English",
      should_queue_reply: true,
      suppression_reason: null,
      safety_tier: "review",
      auto_send_eligible: false,
    }),
    resolveDeterministicStageTransition: () => ({
      inbound_intent: "ownership_confirmed",
      next_stage: "consider_selling",
      template_use_case: "consider_selling",
      safety_tier: "review",
      auto_send_eligible: false,
      should_queue_reply: true,
      policy_source: "explicit",
      deterministic_match: true,
    }),
    loadTemplate: async () => ({ id: "tmpl-id", text: "Hi {{seller_first_name}}" }),
    personalizeTemplate: () => ({ ok: true, text: "Hi Chris" }),
    ...overrides,
  };
}

test("diagnostics/inbound-replay maps ENOENT template preview failures to template_not_found", async () => {
  const diagnostics = await buildVerificationDiagnostics({
    body: "Yes",
    from: "+17135550000",
    to: "+13235550000",
    mode: normalizeInboundReplayMode("verify"),
    auto_reply_enabled: true,
    deps: buildDeps({
    loadTemplate: async () => {
      throw new Error(
        "ENOENT: no such file or directory, open '/vercel/path0/docs/templates/lifecycle-sms-template-pack.csv'"
      );
    },
      personalizeTemplate: () => ({ ok: true, text: "should_not_be_used" }),
    })
  });

  assert.equal(diagnostics.preview_error, "template_not_found");
  assert.equal(diagnostics.selected_template, null);
  assert.equal(diagnostics.verification_write_guard, "no_live_sms_no_queue_mutation");
});

test("diagnostics/inbound-replay opt-out responses select no template and queue nothing", async () => {
  const deps = buildDeps({
    classify: async () => ({ language: "Spanish", source: "test", compliance_flag: "stop_texting" }),
    resolveRoute: () => ({ use_case: "ownership_check" }),
    resolveSellerAutoReplyPlan: async () => ({
      inbound_intent: "opt_out",
      current_stage: "ownership_check",
      next_stage: "stop_or_opt_out",
      selected_use_case: "stop_or_opt_out",
      selected_language: "Spanish",
      should_queue_reply: false,
      suppression_reason: "opt_out_intent_no_marketing",
      safety_tier: "suppress",
      auto_send_eligible: false,
    }),
    resolveDeterministicStageTransition: () => ({
      inbound_intent: "opt_out",
      next_stage: "stop_or_opt_out",
      template_use_case: null,
      safety_tier: "suppress",
      auto_send_eligible: false,
      should_queue_reply: false,
      suppression_reason: "opt_out_intent_no_marketing",
      policy_source: "explicit",
      deterministic_match: true,
    })
  });

  const first = await buildVerificationDiagnostics({
    body: "STOP",
    from: "+17135550000",
    to: "+13235550000",
    mode: normalizeInboundReplayMode("verify"),
    deps,
  });

  const second = await buildVerificationDiagnostics({
    body: "No elimíname de tu lista",
    from: "+17135550000",
    to: "+13235550000",
    mode: normalizeInboundReplayMode("verify"),
    deps,
  });

  for (const diagnostics of [first, second]) {
    assert.equal(diagnostics.detected_intent, "opt_out");
    assert.equal(diagnostics.next_stage, "stop_or_opt_out");
    assert.equal(diagnostics.selected_use_case, null);
    assert.equal(diagnostics.policy_match?.template_use_case, null);
    assert.equal(diagnostics.selected_template, null);
    assert.equal(diagnostics.would_queue_reply, false);
    assert.equal(diagnostics.auto_send_eligible, false);
    assert.equal(diagnostics.safety_tier, "suppress");
    assert.equal(diagnostics.suppression_reason, "opt_out_intent_no_marketing");
    assert.equal(diagnostics.verification_write_guard, "no_live_sms_no_queue_mutation");
  }
});
