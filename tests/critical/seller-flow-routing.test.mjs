import test from "node:test";
import assert from "node:assert/strict";

import {
  SELLER_FLOW_STAGES,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { routeSellerConversation } from "@/lib/domain/seller-flow/route-seller-conversation.js";
import {
  categoryField,
  createPodioItem,
  numberField,
} from "../helpers/test-helpers.js";

function buildContext({
  previous_use_case = SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  previous_stage = previous_use_case,
  property_type = "Residential",
  max_cash_offer = 155000,
  unit_count = null,
} = {}) {
  const property_fields = {
    "property-type":
      property_type === "Residential"
        ? categoryField("Single Family")
        : categoryField(property_type),
    "smart-cash-offer-2": numberField(max_cash_offer),
  };

  if (unit_count !== null) {
    property_fields["number-of-units"] = numberField(unit_count);
  }

  return {
    items: {
      property_item: createPodioItem(601, property_fields),
    },
    summary: {
      conversation_stage: previous_stage === SELLER_FLOW_STAGES.OWNERSHIP_CHECK
        ? "Ownership"
        : "Offer",
      language_preference: "English",
      property_type,
    },
    recent: {
      recent_events: [
        {
          direction: "Outbound",
          metadata: {
            selected_use_case: previous_use_case,
            next_expected_stage: previous_stage,
            selected_tone: "Warm",
          },
        },
      ],
    },
  };
}

function route({
  previous_use_case,
  previous_stage = previous_use_case,
  message,
  classification = { language: "English", emotion: "calm" },
  property_type,
  max_cash_offer,
  unit_count,
} = {}) {
  return routeSellerConversation({
    context: buildContext({
      previous_use_case,
      previous_stage,
      property_type,
      max_cash_offer,
      unit_count,
    }),
    classification,
    message,
  });
}

test("seller flow advances ownership confirmation into consider selling", () => {
  const plan = route({
    previous_use_case: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    message: "Yes, I own it.",
  });

  assert.equal(plan.selected_use_case, SELLER_FLOW_STAGES.CONSIDER_SELLING);
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.CONSIDER_SELLING);
});

test("seller flow advances consider selling into asking price", () => {
  const plan = route({
    previous_use_case: SELLER_FLOW_STAGES.CONSIDER_SELLING,
    message: "Maybe, if the price was right.",
  });

  assert.equal(plan.selected_use_case, SELLER_FLOW_STAGES.ASKING_PRICE);
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.ASKING_PRICE);
});

test("seller flow routes asking price below buy box to confirm basics", () => {
  const plan = route({
    previous_use_case: SELLER_FLOW_STAGES.ASKING_PRICE,
    max_cash_offer: 155000,
    message: "I'd take 140000.",
  });

  assert.equal(plan.selected_use_case, SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS);
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS);
});

test("seller flow routes asking price above buy box to condition probe unless creative is eligible", () => {
  const standard_plan = route({
    previous_use_case: SELLER_FLOW_STAGES.ASKING_PRICE,
    max_cash_offer: 155000,
    message: "I'd need 210000.",
  });
  const creative_plan = route({
    previous_use_case: SELLER_FLOW_STAGES.ASKING_PRICE,
    max_cash_offer: 155000,
    message: "I'd need 210000, but I could do seller financing.",
  });

  assert.equal(standard_plan.selected_use_case, SELLER_FLOW_STAGES.PRICE_HIGH_CONDITION_PROBE);
  assert.equal(creative_plan.selected_use_case, SELLER_FLOW_STAGES.CREATIVE_PROBE);
});

test("seller flow reveals an offer when the seller asks us to make the offer first", () => {
  const plan = route({
    previous_use_case: SELLER_FLOW_STAGES.ASKING_PRICE,
    message: "Just make me an offer.",
  });

  assert.equal(plan.selected_use_case, SELLER_FLOW_STAGES.OFFER_REVEAL_CASH);
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.OFFER_REVEAL_CASH);
});

test("seller flow keeps multifamily leads in underwriting instead of jumping straight to an offer", () => {
  const plan = route({
    previous_use_case: SELLER_FLOW_STAGES.ASKING_PRICE,
    previous_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
    property_type: "Apartment",
    unit_count: 12,
    message: "It's a 12 unit building.",
  });

  assert.equal(plan.handled, false);
  assert.equal(plan.should_queue_reply, false);
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.ASKING_PRICE);
  assert.match(plan.reasoning_summary, /underwriting/i);
});

test("seller flow routes negotiation branches to the canonical stage-6 handlers", () => {
  const justify_price = route({
    previous_use_case: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    previous_stage: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    message: "That's too low. It's vacant and in good condition.",
  });
  const narrow_range = route({
    previous_use_case: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    previous_stage: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    message: "It's vacant and in good condition, but my floor is 180000.",
  });
  const ask_timeline = route({
    previous_use_case: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    previous_stage: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    message: "Not sure yet. It's vacant and in good condition.",
  });
  const ask_condition = route({
    previous_use_case: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    previous_stage: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    message: "That seems low but it's in good shape.",
  });
  const close_handoff = route({
    previous_use_case: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    previous_stage: SELLER_FLOW_STAGES.OFFER_REVEAL_CASH,
    message: "That might work. What's the next step?",
  });

  assert.equal(justify_price.selected_use_case, SELLER_FLOW_STAGES.JUSTIFY_PRICE);
  assert.equal(narrow_range.selected_use_case, SELLER_FLOW_STAGES.NARROW_RANGE);
  assert.equal(ask_timeline.selected_use_case, SELLER_FLOW_STAGES.ASK_TIMELINE);
  assert.equal(ask_condition.selected_use_case, SELLER_FLOW_STAGES.ASK_CONDITION_CLARIFIER);
  assert.equal(close_handoff.selected_use_case, SELLER_FLOW_STAGES.CLOSE_HANDOFF);
});

test("seller flow routes special handlers without advancing the main stage", () => {
  const wrong_person = route({
    previous_use_case: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    message: "Wrong person.",
  });
  const who_is_this = route({
    previous_use_case: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    message: "Who is this?",
  });
  const how_got_number = route({
    previous_use_case: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    message: "How did you get my number?",
  });
  const stop = route({
    previous_use_case: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
    message: "Stop texting me.",
  });

  assert.equal(wrong_person.selected_use_case, SELLER_FLOW_STAGES.WRONG_PERSON);
  assert.equal(who_is_this.selected_use_case, SELLER_FLOW_STAGES.WHO_IS_THIS);
  assert.equal(how_got_number.selected_use_case, SELLER_FLOW_STAGES.HOW_GOT_NUMBER);
  assert.equal(stop.selected_use_case, SELLER_FLOW_STAGES.STOP_OR_OPT_OUT);
  assert.equal(stop.next_expected_stage, SELLER_FLOW_STAGES.TERMINAL);
});
