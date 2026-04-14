// ─── next_action_from_classification.js ───────────────────────────────────
// Orchestrator: consume classify() result → update brain → decide next action
// → resolve template → personalize → schedule → queue (or stop/escalate).

import { updateBrain, getCategoryValue, getTextValue, getNumberValue } from "@/lib/providers/podio.js";
import { normalizeLanguage, resolveLanguage } from "@/lib/sms/language_aliases.js";
import { normalizeAgentStyleFit } from "@/lib/sms/agent_style.js";
import { resolvePropertyTypeScope } from "@/lib/sms/property_scope.js";
import { resolveDealStrategy } from "@/lib/sms/deal_strategy.js";
import { mapNextAction, ACTIONS } from "@/lib/sms/flow_map.js";
import { resolveTemplate } from "@/lib/sms/template_resolver.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import { computeScheduledSend } from "@/lib/sms/latency.js";
import { queueMessage } from "@/lib/sms/queue_message.js";

// ══════════════════════════════════════════════════════════════════════════
// INJECTABLE DEPS
// ══════════════════════════════════════════════════════════════════════════

const defaultDeps = {
  updateBrain,
  getCategoryValue,
  getTextValue,
  getNumberValue,
  mapNextAction,
  resolveTemplate,
  personalizeTemplate,
  computeScheduledSend,
  queueMessage,
};

let deps = { ...defaultDeps };

export function __setNextActionTestDeps(overrides = {}) {
  deps = { ...deps, ...overrides };
}

export function __resetNextActionTestDeps() {
  deps = { ...defaultDeps };
}

// ══════════════════════════════════════════════════════════════════════════
// BRAIN UPDATE
// ══════════════════════════════════════════════════════════════════════════

function buildBrainUpdate(classify_result, flow_result) {
  const updates = {};

  // Language preference
  const lang_resolved = resolveLanguage(classify_result.language);
  if (lang_resolved.canonical) {
    updates["language-preference"] = lang_resolved.canonical;
  }

  // Conversation stage
  if (flow_result.stage_code) {
    updates["conversation-stage"] = flow_result.stage_code;
  }

  // Seller motivation score
  if (typeof classify_result.motivation_score === "number") {
    updates["seller-motivation-score"] = classify_result.motivation_score;
  }

  // Follow-up trigger state
  if (flow_result.action === ACTIONS.WAIT) {
    updates["follow-up-trigger-state"] = "Waiting";
  } else if (flow_result.action === ACTIONS.STOP) {
    updates["follow-up-trigger-state"] = "Stopped";
  }

  return updates;
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATOR
// ══════════════════════════════════════════════════════════════════════════

/**
 * Process a classify() result and take the appropriate next action.
 *
 * @param {object} params
 * @param {object} params.classify_result - Output from classify()
 * @param {object} params.brain_item - Podio AI Conversation Brain item
 * @param {object} params.agent_item - Podio Agents item (assigned SMS agent)
 * @param {object} params.property_context - Property/offer/underwriting context
 * @param {object} params.personalization - Values for template placeholders
 * @param {object} params.links - Podio item IDs (master_owner_id, prospect_id, etc.)
 * @param {string} params.phone_e164 - Canonical phone number
 * @param {string} params.timezone - Contact timezone
 * @param {string} [params.contact_window] - Contact window string
 * @param {Date|string} [params.now_utc] - Current UTC time
 * @returns {object} Action result
 */
export async function processClassification({
  classify_result = {},
  brain_item = null,
  agent_item = null,
  property_context = {},
  personalization = {},
  links = {},
  phone_e164 = null,
  timezone = null,
  contact_window = null,
  now_utc = null,
} = {}) {
  const brain_id = brain_item?.item_id || null;

  // Resolve agent style
  const agent_style_fit = normalizeAgentStyleFit({
    agent_style: null, // Agents app has no direct agent-style-fit field
    agent_archetype: deps.getTextValue(agent_item, "text", ""),
    agent_family: deps.getCategoryValue(agent_item, "category", null),
  });

  // Brain state
  const brain_state = {
    conversation_stage: deps.getCategoryValue(brain_item, "conversation-stage", null),
    close_sub_stage: null, // Brain app has no close-sub-stage field
  };

  // 1. Flow map → decide next action
  const flow = deps.mapNextAction({
    classify_result,
    brain_state,
    property_context,
    agent_style_fit,
  });

  // 2. Update brain
  const brain_updates = buildBrainUpdate(classify_result, flow);
  if (brain_id && Object.keys(brain_updates).length > 0) {
    try {
      await deps.updateBrain(brain_id, brain_updates);
    } catch (err) {
      // Brain update failure should not block the flow
      flow.brain_update_error = err.message;
    }
  }

  // 3. If action is STOP, WAIT, or ESCALATE — return immediately
  if (flow.action === ACTIONS.STOP) {
    return {
      action: ACTIONS.STOP,
      reason: flow.reason,
      cancel_queued: flow.cancel_queued || false,
      brain_updates,
      template: null,
      queue_result: null,
    };
  }

  if (flow.action === ACTIONS.WAIT) {
    return {
      action: ACTIONS.WAIT,
      reason: flow.reason,
      brain_updates,
      template: null,
      queue_result: null,
    };
  }

  if (flow.action === ACTIONS.ESCALATE) {
    return {
      action: ACTIONS.ESCALATE,
      reason: flow.reason,
      human_review: true,
      brain_updates,
      template: null,
      queue_result: null,
    };
  }

  // 4. Queue reply — resolve template
  const language = normalizeLanguage(classify_result.language) || "English";
  const property_scope = resolvePropertyTypeScope({
    use_case: flow.use_case,
    is_follow_up: flow.use_case?.includes("follow"),
    ...property_context,
  });
  const deal_strategy = resolveDealStrategy({
    ...property_context,
    objection: classify_result.objection,
    stage_code: flow.stage_code,
  });

  const resolution = deps.resolveTemplate({
    use_case: flow.use_case,
    stage_code: flow.stage_code,
    language,
    agent_style_fit,
    property_type_scope: property_scope,
    deal_strategy,
    is_first_touch: property_context?.is_first_touch ?? false,
    is_follow_up: flow.use_case?.includes("follow") ?? false,
    master_owner_id: links.master_owner_id,
    phone_e164,
  });

  if (!resolution.resolved) {
    // No template found — check if AI freeform is warranted
    return {
      action: ACTIONS.AI_FREEFORM,
      reason: resolution.fallback_reason || "no_matching_template",
      use_case: flow.use_case,
      stage_code: flow.stage_code,
      language,
      resolution,
      brain_updates,
      template: null,
      queue_result: null,
    };
  }

  // 5. Personalize
  const render = deps.personalizeTemplate(resolution.template_text, personalization);
  if (!render.ok) {
    return {
      action: "personalization_failed",
      reason: render.reason,
      missing_placeholders: render.missing,
      resolution,
      brain_updates,
      template: resolution,
      queue_result: null,
    };
  }

  // 6. Schedule
  const schedule = deps.computeScheduledSend({
    now_utc: now_utc || new Date(),
    timezone: timezone || "America/New_York",
    assigned_agent: agent_item,
    message_kind: flow.use_case?.includes("follow") ? "follow_up" : "reply",
    stage_code: flow.stage_code,
    classify_result,
    contact_window,
    delay_profile: flow.delay_profile,
    seeded_key: [links.master_owner_id, phone_e164, flow.use_case, flow.stage_code],
  });

  // 7. Queue
  const queue_result = await deps.queueMessage({
    rendered_text: render.text,
    schedule,
    resolution,
    links,
    context: {
      touch_number: property_context?.touch_number ?? 1,
      is_first_touch: property_context?.is_first_touch ?? false,
      is_follow_up: flow.use_case?.includes("follow") ?? false,
      phone_e164,
      contact_window,
      placeholders_used: render.placeholders_used,
      property_address: personalization.property_address,
      property_type: property_context?.property_type,
      owner_type: property_context?.owner_type,
    },
  });

  return {
    action: ACTIONS.QUEUE_REPLY,
    reason: flow.reason,
    use_case: flow.use_case,
    stage_code: flow.stage_code,
    language,
    agent_style_fit,
    delay_profile: flow.delay_profile,
    resolution,
    rendered_text: render.text,
    schedule,
    brain_updates,
    template: resolution,
    queue_result,
  };
}

export default { processClassification };
