import { NextResponse } from "next/server";
import {
  SELLER_FLOW_SAFETY_TIERS,
} from "@/lib/domain/seller-flow/seller-flow-safety-policy.js";
import {
  normalizeSellerInboundIntent,
  resolveSellerAutoReplyPlan,
} from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";
import {
  buildDeterministicStageMap,
  resolveDeterministicStageTransition,
} from "@/lib/domain/seller-flow/deterministic-stage-map.js";

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

  const stage_map = buildDeterministicStageMap({ current_stage: filter_stage || null });

  const result = {
    ok: true,
    map_type: "deterministic_stage_map_v1",
    total_transitions: stage_map.length,
    safety_tiers: { ...SELLER_FLOW_SAFETY_TIERS },
    stage_map,
  };

  // If body text is provided, also run live classification against the map
  if (body) {
    const planning_input = {
      message_body: body,
      current_stage: filter_stage || null,
      auto_reply_enabled: true,
    };

    const detected_intent = normalizeSellerInboundIntent(planning_input);
    const transition = resolveDeterministicStageTransition({
      current_stage: filter_stage || null,
      inbound_intent: detected_intent,
      should_queue_reply: true,
      autopilot_enabled: true,
    });
    const plan = await resolveSellerAutoReplyPlan(planning_input);

    result.live_classification = {
      input_body: body,
      input_current_stage: filter_stage || "(none)",
      detected_intent,
      resolved_next_stage: plan.next_stage,
      resolved_use_case: plan.selected_use_case,
      suppression_reason: plan.suppression_reason,
      would_queue_reply: Boolean(plan.should_queue_reply),
      safety_tier: plan.safety_tier,
      policy_match: {
        current_stage: transition.current_stage,
        inbound_intent: transition.inbound_intent,
        next_stage: transition.next_stage,
        template_use_case: transition.template_use_case,
        safety_tier: transition.safety_tier,
        auto_send_eligible: transition.auto_send_eligible,
        policy_source: transition.policy_source,
        deterministic_match: transition.deterministic_match,
      },
      routing_consistent:
        !transition.next_stage || transition.next_stage === plan.next_stage,
    };
  }

  return NextResponse.json(result);
}
