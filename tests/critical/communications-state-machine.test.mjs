import test from "node:test";
import assert from "node:assert/strict";

import {
  AI_MANAGED_STATUSES,
  CONVERSATION_BRANCHES,
  CONVERSATION_STAGES,
  DEAL_STRATEGY_BRANCHES,
  EXECUTION_BRAIN_MILESTONES,
  FOLLOW_UP_STEPS,
  FOLLOW_UP_TRIGGER_STATES,
  LAST_DETECTED_INTENTS,
  SELLER_STATES,
  buildExecutionConversationState,
  buildInboundConversationState,
  buildOutboundFollowUpState,
} from "@/lib/domain/communications-engine/state-machine.js";
import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import {
  createPodioItem,
  numberField,
} from "../helpers/test-helpers.js";

function buildContext({
  conversation_stage = CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
  cash_offer_target = 150000,
} = {}) {
  return {
    summary: {
      conversation_stage,
      language_preference: "English",
      motivation_score: 55,
    },
    items: {
      property_item: createPodioItem(501, {
        "smart-cash-offer-2": numberField(cash_offer_target),
      }),
    },
  };
}

function buildState({
  message,
  context = buildContext(),
  classification = {},
  route = {},
} = {}) {
  const final_classification = {
    language: "English",
    emotion: "calm",
    objection: null,
    compliance_flag: null,
    motivation_score: 55,
    positive_signals: [],
    source: "heuristic",
    notes: "",
    ...classification,
  };

  const signals = extractUnderwritingSignals({
    message,
    classification: final_classification,
    route,
    context,
  });

  return buildInboundConversationState({
    context,
    classification: final_classification,
    route,
    message,
    signals,
  });
}

function buildExecutionState({
  milestone,
  current_state = {},
  note = "",
} = {}) {
  return buildExecutionConversationState({
    milestone,
    current_state,
    note,
  });
}

test("ownership confirmed advances the brain into stage 2 with cash as the default branch", () => {
  const state = buildState({
    message: "Yes, I own it.",
  });

  assert.equal(state.lifecycle_stage_number, 2);
  assert.equal(
    state.conversation_stage,
    CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION
  );
  assert.equal(
    state.current_conversation_branch,
    CONVERSATION_BRANCHES.OFFER_INTEREST
  );
  assert.equal(state.current_seller_state, SELLER_STATES.CONFIRMED_OWNER);
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.CASH);
  assert.equal(state.should_use_ai_assist, false);
});

test("ownership denied routes the thread into terminal dead handling and stops follow-up", () => {
  const state = buildState({
    message: "I sold it already and I don't own it anymore.",
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.OWNERSHIP_DENIED);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME);
  assert.equal(
    state.current_conversation_branch,
    CONVERSATION_BRANCHES.DEAD_LEAD_HANDLING
  );
  assert.equal(state.current_seller_state, SELLER_STATES.NO_LONGER_OWNER);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.COMPLETED);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.PAUSED);
});

test("seller open to offer deterministically progresses into offer interest confirmation", () => {
  const state = buildState({
    message: "I would consider selling if the price made sense.",
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.OPEN_TO_OFFER);
  assert.equal(
    state.conversation_stage,
    CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION
  );
  assert.equal(
    state.current_conversation_branch,
    CONVERSATION_BRANCHES.OFFER_INTEREST
  );
  assert.equal(state.current_seller_state, SELLER_STATES.OPEN_TO_OFFER);
});

test("wants offer keeps the lead on the cash-first path without forcing AI", () => {
  const state = buildState({
    message: "What would you offer for it?",
    classification: {
      objection: "send_offer_first",
    },
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.WANTS_OFFER);
  assert.equal(
    state.conversation_stage,
    CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION
  );
  assert.equal(state.current_seller_state, SELLER_STATES.WANTS_OFFER_FIRST);
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.CASH);
  assert.equal(state.should_use_ai_assist, false);
});

test("seller gives price stores the ask and stays in price discovery when no target is available", () => {
  const state = buildState({
    message: "I would take 185000.",
    context: {
      summary: {
        conversation_stage: CONVERSATION_STAGES.OFFER_INTEREST_CONFIRMATION,
        language_preference: "English",
        motivation_score: 55,
      },
      items: {
        property_item: createPodioItem(501, {}),
      },
    },
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SELLER_PRICE_DISCOVERY);
  assert.equal(state.seller_ask_price, 185000);
  assert.equal(state.cash_offer_target, null);
});

test("asking price above target pushes the thread into negotiation with an above-range seller state", () => {
  const state = buildState({
    message: "I would take 185000.",
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.NEGOTIATION);
  assert.equal(
    state.current_conversation_branch,
    CONVERSATION_BRANCHES.NEGOTIATION
  );
  assert.equal(state.seller_ask_price, 185000);
  assert.equal(state.cash_offer_target, 150000);
  assert.equal(state.price_gap_to_target, 35000);
  assert.equal(state.current_seller_state, SELLER_STATES.ABOVE_RANGE);
});

test("asking price at or below target moves the thread toward lock flow", () => {
  const state = buildState({
    message: "If you can do 145000 we can make it happen.",
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.ASKING_PRICE_GIVEN);
  assert.equal(
    state.conversation_stage,
    CONVERSATION_STAGES.VERBAL_ACCEPTANCE_LOCK
  );
  assert.equal(state.current_seller_state, SELLER_STATES.READY_FOR_CONTRACT);
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.CASH);
  assert.equal(state.deal_priority_tag, "Urgent");
});

test("price plus condition info safely skips ahead into offer positioning", () => {
  const state = buildState({
    message: "I own it. I need 160000 and it's vacant and needs work.",
  });

  assert.equal(
    state.conversation_stage,
    CONVERSATION_STAGES.OFFER_POSITIONING
  );
  assert.equal(
    state.current_conversation_branch,
    CONVERSATION_BRANCHES.OFFER_POSITIONING
  );
  assert.equal(state.seller_ask_price, 160000);
  assert.equal(state.price_gap_to_target, 10000);
});

test("cash rejection from an engaged seller raises creative eligibility without blindly switching branches", () => {
  const state = buildState({
    message: "That cash number is too low. I'd need 240000.",
    classification: {
      objection: "need_more_money",
      emotion: "guarded",
    },
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.WANTS_HIGHER_PRICE);
  assert.equal(state.current_conversation_branch, CONVERSATION_BRANCHES.OBJECTION_HANDLING);
  assert.equal(state.current_seller_state, SELLER_STATES.ABOVE_RANGE);
  assert.equal(state.creative_branch_eligibility, "Maybe");
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.CASH);
});

test("explicit creative terms move the deal strategy branch deterministically", () => {
  const state = buildState({
    message: "Cash won't work, but I could maybe do seller financing with monthly payments.",
    classification: {
      objection: "need_more_money",
    },
  });

  assert.equal(state.creative_branch_eligibility, "Yes");
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.SELLER_FINANCE);
});

test("wrong number drives the terminal wrong-number branch and status", () => {
  const state = buildState({
    message: "Wrong number.",
    classification: {
      objection: "wrong_number",
    },
  });

  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME);
  assert.equal(state.current_conversation_branch, CONVERSATION_BRANCHES.WRONG_NUMBER);
  assert.equal(state.current_seller_state, SELLER_STATES.WRONG_NUMBER);
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.WRONG_NUMBER);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.WRONG_NUMBER);
});

test("opt out drives the terminal DNC branch and status", () => {
  const state = buildState({
    message: "Stop texting me.",
    classification: {
      compliance_flag: "stop_texting",
    },
  });

  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.DNC);
  assert.equal(state.current_conversation_branch, CONVERSATION_BRANCHES.DNC);
  assert.equal(state.current_seller_state, SELLER_STATES.DNC);
  assert.equal(state.deal_strategy_branch, DEAL_STRATEGY_BRANCHES.DNC);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.DNC);
});

test("state snapshots include concise summaries and a deterministic next move", () => {
  const state = buildState({
    message: "I own it and would take 190000 for it.",
  });

  assert.ok(state.last_message_summary_ai?.includes("Seller intent: Asking Price Given."));
  assert.ok(state.full_conversation_summary_ai?.includes("Stage"));
  assert.ok(state.ai_recommended_next_move?.length > 10);
  assert.ok(state.ai_next_message?.length > 10);
});

test("outbound follow-up state advances deterministically when there is no response", () => {
  const state = buildOutboundFollowUpState({
    conversation_stage: CONVERSATION_STAGES.OWNERSHIP_CONFIRMATION,
    current_follow_up_step: FOLLOW_UP_STEPS.A,
    now: "2026-04-11T12:00:00.000Z",
  });

  assert.equal(state.follow_up_step, FOLLOW_UP_STEPS.B);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.WAITING);
  assert.ok(state.next_follow_up_due_at?.start);
  assert.ok(new Date(state.next_follow_up_due_at.start).getTime() > new Date("2026-04-11T12:00:00.000Z").getTime());
});

test("an inbound seller response resets follow-up state and keeps automation deterministic", () => {
  const state = buildState({
    message: "Yes, I own it and would consider an offer.",
  });

  assert.equal(state.follow_up_step, FOLLOW_UP_STEPS.NONE);
  assert.equal(state.next_follow_up_due_at, null);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.AI_RUNNING);
});

test("contract sent moves Brain into stage 8 Contract Out", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_SENT,
  });

  assert.equal(state.lifecycle_stage_number, 8);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CONTRACT_OUT);
  assert.equal(state.current_conversation_branch, CONVERSATION_BRANCHES.CONTRACT_PUSH);
  assert.equal(state.current_seller_state, SELLER_STATES.READY_FOR_CONTRACT);
  assert.equal(state.last_detected_intent, LAST_DETECTED_INTENTS.CONTRACT_READY);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.WAITING_ON_SELLER);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.WAITING);
});

test("contract viewed remains in stage 8 Contract Out", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_VIEWED,
  });

  assert.equal(state.lifecycle_stage_number, 8);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CONTRACT_OUT);
  assert.equal(state.current_seller_state, SELLER_STATES.READY_FOR_CONTRACT);
});

test("seller signed advances Brain into stage 9 Signed / Closing", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_SIGNED,
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.current_seller_state, SELLER_STATES.SIGNED);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.UNDER_CONTRACT);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.PAUSED);
});

test("fully executed contract confirms stage 9 without falling back to contract out", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_FULLY_EXECUTED,
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.current_seller_state, SELLER_STATES.SIGNED);
});

test("contract cancelled moves Brain into stage 10 dead outcome", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_CANCELLED,
    note: "Seller backed out after review.",
  });

  assert.equal(state.lifecycle_stage_number, 10);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME);
  assert.equal(state.current_seller_state, SELLER_STATES.DEAD);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.COMPLETED);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.PAUSED);
  assert.ok(state.risk_flags_ai.includes("Seller Hesitation"));
});

test("routed to title confirms stage 9 Signed / Closing", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.TITLE_ROUTED,
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.UNDER_CONTRACT);
});

test("title reviewing remains in stage 9 Signed / Closing", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.TITLE_REVIEWING,
    note: "Curative issue under review.",
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.PAUSED);
});

test("clear to close remains in stage 9 and raises urgency", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE,
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.deal_priority_tag, "Urgent");
});

test("fatal title cancellation moves Brain into stage 10 dead outcome", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.TITLE_CANCELLED,
    note: "Title cancelled after fatal issue.",
  });

  assert.equal(state.lifecycle_stage_number, 10);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME);
  assert.equal(state.current_seller_state, SELLER_STATES.DEAD);
});

test("closing scheduled remains in stage 9 Signed / Closing", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CLOSING_SCHEDULED,
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.deal_priority_tag, "Urgent");
});

test("closing completed moves Brain into stage 10 closed outcome", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CLOSING_COMPLETED,
  });

  assert.equal(state.lifecycle_stage_number, 10);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME);
  assert.equal(state.current_seller_state, SELLER_STATES.CLOSED);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.CLOSED);
  assert.equal(state.follow_up_trigger_state, FOLLOW_UP_TRIGGER_STATES.COMPLETED);
});

test("closing cancelled moves Brain into stage 10 dead outcome", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CLOSING_CANCELLED,
    note: "Buyer backed out before the close.",
  });

  assert.equal(state.lifecycle_stage_number, 10);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME);
  assert.equal(state.current_seller_state, SELLER_STATES.DEAD);
});

test("downstream execution updates do not regress a later signed-closing state", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_VIEWED,
    current_state: {
      conversation_stage: CONVERSATION_STAGES.SIGNED_CLOSING,
      lifecycle_stage_number: 9,
      current_conversation_branch: CONVERSATION_BRANCHES.CONTRACT_PUSH,
      current_seller_state: SELLER_STATES.SIGNED,
      status_ai_managed: AI_MANAGED_STATUSES.UNDER_CONTRACT,
      follow_up_trigger_state: FOLLOW_UP_TRIGGER_STATES.PAUSED,
      deal_priority_tag: "Urgent",
    },
  });

  assert.equal(state.lifecycle_stage_number, 9);
  assert.equal(state.conversation_stage, CONVERSATION_STAGES.SIGNED_CLOSING);
  assert.equal(state.current_seller_state, SELLER_STATES.SIGNED);
  assert.equal(state.status_ai_managed, AI_MANAGED_STATUSES.UNDER_CONTRACT);
});

test("DNC and wrong-number terminal states are not overwritten by execution noise", () => {
  const dnc_state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CLOSING_COMPLETED,
    current_state: {
      conversation_stage: CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
      lifecycle_stage_number: 10,
      current_seller_state: SELLER_STATES.DNC,
      status_ai_managed: AI_MANAGED_STATUSES.DNC,
    },
  });

  const wrong_number_state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.CONTRACT_SENT,
    current_state: {
      conversation_stage: CONVERSATION_STAGES.CLOSED_DEAD_OUTCOME,
      lifecycle_stage_number: 10,
      current_seller_state: SELLER_STATES.WRONG_NUMBER,
      status_ai_managed: AI_MANAGED_STATUSES.WRONG_NUMBER,
    },
  });

  assert.equal(dnc_state.blocked_reason, "protected_terminal_state");
  assert.equal(wrong_number_state.blocked_reason, "protected_terminal_state");
});

test("execution summaries stay coherent while preserving rolling context", () => {
  const state = buildExecutionState({
    milestone: EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE,
    current_state: {
      full_conversation_summary_ai:
        "Stage 9 Signed / Closing. Title is working the file.",
    },
    note: "Closing is set for Monday at 10am.",
  });

  assert.ok(state.last_message_summary_ai.includes("Stage 9 Signed / Closing"));
  assert.ok(state.full_conversation_summary_ai.includes("Title is working the file."));
  assert.ok(state.ai_recommended_next_move.includes("close timeline"));
});
