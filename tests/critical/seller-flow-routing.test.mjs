import test from "node:test";
import assert from "node:assert/strict";

import { routeSellerConversation } from "@/lib/domain/seller-flow/route-seller-conversation.js";
import { SELLER_FLOW_STAGES } from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import {
  createPodioItem,
  numberField,
} from "../helpers/test-helpers.js";

function buildContext({
  previous_use_case = "ownership_check",
  previous_stage = SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  property_item = null,
} = {}) {
  return {
    items: {
      property_item,
    },
    summary: {
      conversation_stage: "Ownership",
      last_outbound_message: "",
      language_preference: "English",
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

test("seller flow advances ownership confirmation into consider_selling using recent outbound metadata", () => {
  const plan = routeSellerConversation({
    context: buildContext(),
    classification: { language: "English", emotion: "calm" },
    message: "Yes, I own it.",
  });

  assert.equal(plan.detected_intent, "Ownership Confirmed");
  assert.equal(plan.selected_use_case, "consider_selling");
  assert.equal(plan.template_lookup_use_case, null);
  assert.equal(plan.selected_variant_group, "Stage 2 Consider Selling");
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.CONSIDER_SELLING);
});

test("seller flow asks for price after seller is open to selling", () => {
  const plan = routeSellerConversation({
    context: buildContext({
      previous_use_case: "consider_selling",
      previous_stage: SELLER_FLOW_STAGES.CONSIDER_SELLING,
    }),
    classification: { language: "English", emotion: "calm" },
    message: "Maybe, depends on the number.",
  });

  assert.equal(plan.detected_intent, "Open to Selling");
  assert.equal(plan.selected_use_case, "asking_price");
  assert.equal(plan.template_lookup_use_case, null);
  assert.equal(plan.selected_variant_group, "Stage 3 Asking Price");
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.ASKING_PRICE);
});

test("seller flow routes acceptable asking price into confirm basics", () => {
  const property_item = createPodioItem(601, {
    "smart-cash-offer-2": numberField(155000),
  });

  const plan = routeSellerConversation({
    context: buildContext({
      previous_use_case: "asking_price",
      previous_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
      property_item,
    }),
    classification: { language: "English", emotion: "calm" },
    message: "I'd take 140000.",
  });

  assert.equal(plan.detected_intent, "Asking Price Provided");
  assert.equal(plan.selected_use_case, "price_works_confirm_basics");
  assert.equal(plan.template_lookup_use_case, null);
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.PRICE_WORKS_CONFIRM_BASICS);
});

test("seller flow reveals offer after reverse-offer request and preserves offer display", () => {
  const property_item = createPodioItem(601, {
    "smart-cash-offer-2": numberField(155000),
  });

  const plan = routeSellerConversation({
    context: buildContext({
      previous_use_case: "asking_price",
      previous_stage: SELLER_FLOW_STAGES.ASKING_PRICE,
      property_item,
    }),
    classification: { language: "English", emotion: "calm" },
    message: "Just make me an offer.",
  });

  assert.equal(plan.detected_intent, "No Asking Price / Reverse Offer Request");
  assert.equal(plan.selected_use_case, "offer_reveal");
  assert.equal(plan.template_lookup_use_case, "offer_reveal");
  assert.equal(plan.offer_price_display, "$155,000");
  assert.equal(plan.next_expected_stage, SELLER_FLOW_STAGES.OFFER_REVEAL);
});
