import test from "node:test";
import assert from "node:assert/strict";

import { deriveNoReplyFollowUpPlan } from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import {
  createPodioItem,
  dateField,
  textField,
} from "../helpers/test-helpers.js";

function outboundEvent(item_id, metadata, timestamp, message = "") {
  return createPodioItem(item_id, {
    "ai-output": textField(JSON.stringify(metadata)),
    timestamp: dateField(timestamp),
    message: textField(message),
  });
}

test("no-reply recovery resumes from outbound metadata instead of restarting at stage 1", () => {
  const plan = deriveNoReplyFollowUpPlan({
    history: {
      outbound_events: [
        outboundEvent(
          1001,
          {
            selected_use_case: "ownership_check",
            next_expected_stage: "ownership_check",
            selected_tone: "Warm",
          },
          "2026-04-04T10:00:00.000Z",
          "Just following up to see if you're the owner."
        ),
        outboundEvent(
          1002,
          {
            selected_use_case: "asking_price",
            next_expected_stage: "asking_price",
            selected_tone: "Direct",
          },
          "2026-04-04T12:00:00.000Z",
          "Following up to see what number you had in mind."
        ),
      ],
    },
    default_category: "Residential",
    default_tone: "Warm",
  });

  assert.equal(plan?.base_use_case, "asking_price");
  assert.equal(plan?.template_lookup_use_case, "asking_price_follow_up");
  assert.equal(plan?.variant_group, "Stage 3 Follow-Up");
  assert.equal(plan?.next_expected_stage, "asking_price");
  assert.equal(plan?.tone, "Direct");
});

test("no-reply recovery resumes multifamily and offer reveal follow-ups from the last real stage", () => {
  const multifamily_plan = deriveNoReplyFollowUpPlan({
    history: {
      outbound_events: [
        outboundEvent(
          1101,
          {
            selected_use_case: "mf_rents",
            next_expected_stage: "mf_rents",
            selected_tone: "Neutral",
          },
          "2026-04-04T09:00:00.000Z"
        ),
      ],
    },
    default_category: "Landlord / Multifamily",
    default_tone: "Neutral",
  });

  const offer_plan = deriveNoReplyFollowUpPlan({
    history: {
      outbound_events: [
        outboundEvent(
          1201,
          {
            selected_use_case: "offer_reveal_cash",
            next_expected_stage: "offer_reveal_cash",
            selected_tone: "Warm",
          },
          "2026-04-04T13:00:00.000Z"
        ),
      ],
    },
  });

  assert.equal(multifamily_plan?.template_lookup_use_case, "mf_rents_follow_up");
  assert.equal(
    multifamily_plan?.variant_group,
    "Multifamily Underwrite — Rents Follow-Up"
  );
  assert.equal(offer_plan?.template_lookup_use_case, "offer_reveal_cash_follow_up");
  assert.equal(offer_plan?.variant_group, "Stage 5 — Offer No Response");
});
