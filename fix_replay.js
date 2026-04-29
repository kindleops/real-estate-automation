const fs = require('fs');
const file = 'src/app/api/internal/testing/replay-inbound/route.js';
let content = fs.readFileSync(file, 'utf8');

content = content.replace(
  'import { routeSellerConversation } from "@/lib/domain/seller-flow/route-seller-conversation.js";',
  'import { resolveSellerAutoReplyPlan } from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";'
);

content = content.replace(
  'routeSellerConversation,',
  'resolveSellerAutoReplyPlan,'
);

const old_route_call = `    const plan = replayDeps.routeSellerConversation({
      context,
      classification,
      message: normalized_message_body,
      previous_outbound_use_case: prior_use_case,
      maybe_offer,
      existing_offer,
    });`;

const new_route_call = `    const plan = await replayDeps.resolveSellerAutoReplyPlan({
      inbound_event: { item_id: null, message_id: null, from: normalized_from_number, to: normalized_to_number },
      message_body: normalized_message_body,
      classification,
      route: {}, 
      conversation_context: context,
      current_stage: context?.summary?.conversation_stage || null,
      prior_use_case: prior_use_case,
      recent_outbound: null,
      underwriting_signals: underwriting,
      auto_reply_enabled: true,
      force_queue_reply: false,
      now: new Date().toISOString()
    });
    // Map back some fields for the replay endpoint's assertions and formatting
    plan.next_expected_stage = plan.next_stage;
    plan.detected_intent = plan.inbound_intent;
    plan.template_lookup_use_case = plan.selected_use_case;
    plan.handled = true;`;

content = content.replace(old_route_call, new_route_call);

fs.writeFileSync(file, content);
