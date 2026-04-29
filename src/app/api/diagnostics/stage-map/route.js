import { NextResponse } from "next/server";
import {
  SELLER_FLOW_SAFETY_POLICY,
  SELLER_FLOW_SAFETY_TIERS,
} from "@/lib/domain/seller-flow/seller-flow-safety-policy.js";
import {
  normalizeSellerInboundIntent,
  resolveNextSellerStage,
  resolveAutoReplyUseCase,
  shouldSuppressSellerAutoReply,
} from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";

/**
 * GET /api/diagnostics/stage-map
 *
 * Returns the full deterministic stage map showing:
 *   current_stage + inbound_intent → next_stage → template_use_case → safety_tier
 *
 * Query params:
 *   ?body=<text>          — optional: also run a live intent classification against the map
 *   ?current_stage=<stage> — optional: filter to a specific current stage
 */
export async function GET(request) {
  const { searchParams } = new URL(request.url);
  const body = searchParams.get("body");
  const filter_stage = searchParams.get("current_stage");

  // Build the full stage map from the policy
  const stage_map = [];

  for (const [current_stage, intents] of Object.entries(SELLER_FLOW_SAFETY_POLICY)) {
    if (filter_stage && current_stage !== filter_stage) continue;

    for (const [intent, transition] of Object.entries(intents)) {
      stage_map.push({
        current_stage,
        inbound_intent: intent,
        next_stage: transition.next_stage,
        template_use_case: transition.template,
        safety_tier: transition.safety,
        auto_send_eligible: transition.safety === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
      });
    }
  }

  const result = {
    ok: true,
    total_transitions: stage_map.length,
    safety_tiers: { ...SELLER_FLOW_SAFETY_TIERS },
    stage_map,
  };

  // If body text is provided, also run live classification against the map
  if (body) {
    const input = {
      message_body: body,
      current_stage: filter_stage || null,
      auto_reply_enabled: true,
    };

    const detected_intent = normalizeSellerInboundIntent(input);
    const next_stage = resolveNextSellerStage(input);
    const selected_use_case = resolveAutoReplyUseCase(input);
    const suppression = shouldSuppressSellerAutoReply(input);

    // Find the matching policy entry
    const policy_match =
      SELLER_FLOW_SAFETY_POLICY[filter_stage]?.[detected_intent] ||
      SELLER_FLOW_SAFETY_POLICY.global[detected_intent] ||
      null;

    result.live_classification = {
      input_body: body,
      input_current_stage: filter_stage || "(none)",
      detected_intent,
      resolved_next_stage: next_stage,
      resolved_use_case: selected_use_case,
      suppression,
      policy_match: policy_match
        ? {
            next_stage: policy_match.next_stage,
            template: policy_match.template,
            safety_tier: policy_match.safety,
            auto_send_eligible: policy_match.safety === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
          }
        : { warning: "No policy entry found for this stage+intent combo — defaults to REVIEW" },
      routing_consistent:
        policy_match?.next_stage === next_stage ||
        !policy_match,
    };
  }

  return NextResponse.json(result);
}
