import { BRAIN_FIELDS, updateBrainItem } from "@/lib/podio/apps/ai-conversation-brain.js";
import { toPodioDateField } from "@/lib/utils/dates.js";
import {
  buildOutboundFollowUpState,
  getConversationStageNumber,
  normalizeLockedConversationStage,
} from "@/lib/domain/communications-engine/state-machine.js";

const defaultDeps = {
  updateBrainItem,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function toId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function finiteNumber(value) {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function compactFields(fields = {}) {
  return Object.fromEntries(
    Object.entries(fields).filter(([, value]) => value !== undefined)
  );
}

export function __setBrainAuthorityTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetBrainAuthorityTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function applyBrainStateUpdate({
  brain_id = null,
  fields = {},
  reason = null,
} = {}) {
  if (!brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
      update_reason: clean(reason) || null,
    };
  }

  const normalized_fields = compactFields(fields);

  if (!Object.keys(normalized_fields).length) {
    return {
      ok: false,
      reason: "no_brain_fields_to_update",
      brain_id,
      update_reason: clean(reason) || null,
      updated_fields: {},
    };
  }

  await runtimeDeps.updateBrainItem(brain_id, normalized_fields);

  return {
    ok: true,
    brain_id,
    update_reason: clean(reason) || null,
    updated_fields: normalized_fields,
  };
}

export function buildDeterministicBrainStateFields({
  deterministic_state = null,
} = {}) {
  if (!deterministic_state || typeof deterministic_state !== "object") {
    return {};
  }

  const lifecycle_stage_number = finiteNumber(
    deterministic_state.lifecycle_stage_number
  );
  const seller_motivation_score = finiteNumber(
    deterministic_state.seller_motivation_score
  );
  const seller_ask_price = finiteNumber(deterministic_state.seller_ask_price);
  const cash_offer_target = finiteNumber(
    deterministic_state.cash_offer_target
  );
  const risk_flags_ai = Array.isArray(deterministic_state.risk_flags_ai)
    ? [...new Set(deterministic_state.risk_flags_ai.map((value) => clean(value)).filter(Boolean))]
    : clean(deterministic_state.risk_flags_ai)
      ? [clean(deterministic_state.risk_flags_ai)]
      : [];

  return compactFields({
    ...(clean(deterministic_state.conversation_stage)
      ? {
          [BRAIN_FIELDS.conversation_stage]:
            clean(deterministic_state.conversation_stage),
        }
      : {}),
    ...(lifecycle_stage_number !== null
      ? { [BRAIN_FIELDS.lifecycle_stage_number]: lifecycle_stage_number }
      : {}),
    ...(clean(deterministic_state.current_conversation_branch)
      ? {
          [BRAIN_FIELDS.ai_route]: clean(
            deterministic_state.current_conversation_branch
          ),
        }
      : {}),
    ...(clean(deterministic_state.current_seller_state)
      ? {
          [BRAIN_FIELDS.current_seller_state]:
            clean(deterministic_state.current_seller_state),
        }
      : {}),
    ...(clean(deterministic_state.follow_up_step)
      ? {
          [BRAIN_FIELDS.follow_up_step]: clean(
            deterministic_state.follow_up_step
          ),
        }
      : {}),
    ...(deterministic_state.next_follow_up_due_at !== undefined
      ? {
          [BRAIN_FIELDS.next_follow_up_due_at]:
            deterministic_state.next_follow_up_due_at ?? null,
        }
      : {}),
    ...(clean(deterministic_state.last_detected_intent)
      ? {
          [BRAIN_FIELDS.last_detected_intent]:
            clean(deterministic_state.last_detected_intent),
        }
      : {}),
    ...(clean(deterministic_state.seller_profile)
      ? {
          [BRAIN_FIELDS.seller_profile]: clean(
            deterministic_state.seller_profile
          ),
        }
      : {}),
    ...(clean(deterministic_state.language_preference)
      ? {
          [BRAIN_FIELDS.language_preference]: clean(
            deterministic_state.language_preference
          ),
        }
      : {}),
    ...(clean(deterministic_state.status_ai_managed)
      ? {
          [BRAIN_FIELDS.status_ai_managed]: clean(
            deterministic_state.status_ai_managed
          ),
        }
      : {}),
    ...(clean(deterministic_state.deal_priority_tag)
      ? {
          [BRAIN_FIELDS.deal_priority_tag]: clean(
            deterministic_state.deal_priority_tag
          ),
        }
      : {}),
    ...(seller_motivation_score !== null
      ? { [BRAIN_FIELDS.seller_motivation_score]: seller_motivation_score }
      : {}),
    ...(risk_flags_ai.length
      ? { [BRAIN_FIELDS.risk_flags_ai]: risk_flags_ai }
      : {}),
    ...(clean(deterministic_state.last_message_summary_ai)
      ? {
          [BRAIN_FIELDS.last_message_summary_ai]: clean(
            deterministic_state.last_message_summary_ai
          ),
        }
      : {}),
    ...(clean(deterministic_state.full_conversation_summary_ai)
      ? {
          [BRAIN_FIELDS.full_conversation_summary_ai]: clean(
            deterministic_state.full_conversation_summary_ai
          ),
        }
      : {}),
    ...(clean(deterministic_state.ai_recommended_next_move)
      ? {
          [BRAIN_FIELDS.ai_recommended_next_move]: clean(
            deterministic_state.ai_recommended_next_move
          ),
        }
      : {}),
    ...(clean(deterministic_state.ai_next_message)
      ? {
          [BRAIN_FIELDS.ai_next_message]: clean(
            deterministic_state.ai_next_message
          ),
        }
      : {}),
    ...(clean(deterministic_state.seller_emotional_tone)
      ? {
          [BRAIN_FIELDS.seller_emotional_tone]: clean(
            deterministic_state.seller_emotional_tone
          ),
        }
      : {}),
    ...(clean(deterministic_state.response_style_mode)
      ? {
          [BRAIN_FIELDS.response_style_mode]: clean(
            deterministic_state.response_style_mode
          ),
        }
      : {}),
    ...(clean(deterministic_state.primary_objection_type)
      ? {
          [BRAIN_FIELDS.primary_objection_type]: clean(
            deterministic_state.primary_objection_type
          ),
        }
      : {}),
    ...(seller_ask_price !== null
      ? { [BRAIN_FIELDS.seller_ask_price]: seller_ask_price }
      : {}),
    ...(cash_offer_target !== null
      ? { [BRAIN_FIELDS.cash_offer_target]: cash_offer_target }
      : {}),
    ...(clean(deterministic_state.creative_branch_eligibility)
      ? {
          [BRAIN_FIELDS.creative_branch_eligibility]: clean(
            deterministic_state.creative_branch_eligibility
          ),
        }
      : {}),
    ...(clean(deterministic_state.deal_strategy_branch)
      ? {
          [BRAIN_FIELDS.deal_strategy_branch]: clean(
            deterministic_state.deal_strategy_branch
          ),
        }
      : {}),
  });
}

export function buildInboundBrainStateFields({
  message_body = "",
  follow_up_trigger_state = "AI Running",
  deterministic_state = null,
  extra_fields = {},
  now = new Date(),
} = {}) {
  return compactFields({
    [BRAIN_FIELDS.last_inbound_message]: String(message_body || ""),
    [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(now),
    [BRAIN_FIELDS.follow_up_trigger_state]: follow_up_trigger_state,
    ...buildDeterministicBrainStateFields({ deterministic_state }),
    ...(extra_fields || {}),
  });
}

export function buildOutboundBrainStateFields({
  message_body = "",
  template_id = null,
  conversation_stage = null,
  current_follow_up_step = null,
  status_ai_managed = null,
  now = new Date().toISOString(),
} = {}) {
  const follow_up_state = buildOutboundFollowUpState({
    conversation_stage,
    current_follow_up_step,
    status_ai_managed,
    now,
  });

  return compactFields({
    [BRAIN_FIELDS.last_outbound_message]: String(message_body || ""),
    [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(now),
    [BRAIN_FIELDS.last_sent_time]: toPodioDateField(now),
    [BRAIN_FIELDS.follow_up_step]: follow_up_state.follow_up_step,
    [BRAIN_FIELDS.follow_up_trigger_state]: follow_up_state.follow_up_trigger_state,
    [BRAIN_FIELDS.status_ai_managed]: follow_up_state.status_ai_managed,
    [BRAIN_FIELDS.next_follow_up_due_at]: follow_up_state.next_follow_up_due_at ?? null,
    ...(template_id ? { [BRAIN_FIELDS.last_template_sent]: template_id } : {}),
  });
}

export function buildDeliveryBrainStateFields({
  delivery_status = null,
} = {}) {
  const normalized_status = clean(delivery_status).toLowerCase();

  if (normalized_status === "delivered") {
    return {
      [BRAIN_FIELDS.follow_up_trigger_state]: "Waiting",
    };
  }

  if (normalized_status === "failed") {
    return {
      [BRAIN_FIELDS.follow_up_trigger_state]: "Paused",
      [BRAIN_FIELDS.status_ai_managed]: "Paused",
    };
  }

  return {};
}

export function buildStageBrainStateFields({
  stage = null,
} = {}) {
  const normalized_input = clean(stage);
  if (!normalized_input) return {};

  const normalized_stage = normalizeLockedConversationStage(normalized_input);

  return {
    [BRAIN_FIELDS.conversation_stage]: normalized_stage,
    [BRAIN_FIELDS.lifecycle_stage_number]: getConversationStageNumber(normalized_stage),
  };
}

export function buildLinkedMessageEventsFields({
  current_message_event_ids = [],
  message_event_id = null,
} = {}) {
  const existing_ids = Array.isArray(current_message_event_ids)
    ? [
        ...new Set(
          current_message_event_ids.map((id) => toId(id)).filter(Boolean)
        ),
      ]
    : [];
  const resolved_message_event_id = toId(message_event_id);

  if (!resolved_message_event_id) {
    return {};
  }

  const next_message_event_ids = [...existing_ids];
  if (!next_message_event_ids.includes(resolved_message_event_id)) {
    next_message_event_ids.push(resolved_message_event_id);
  }

  return {
    [BRAIN_FIELDS.linked_message_events]: next_message_event_ids,
  };
}

export default {
  __setBrainAuthorityTestDeps,
  __resetBrainAuthorityTestDeps,
  applyBrainStateUpdate,
  buildDeterministicBrainStateFields,
  buildInboundBrainStateFields,
  buildOutboundBrainStateFields,
  buildDeliveryBrainStateFields,
  buildStageBrainStateFields,
  buildLinkedMessageEventsFields,
};
