import test from "node:test";
import assert from "node:assert/strict";

import { maybeQueueSellerStageReply } from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";
import {
  SELLER_FLOW_STAGES,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import {
  createPodioItem,
  numberField,
} from "../helpers/test-helpers.js";

function buildContext() {
  return {
    ids: {
      master_owner_id: 201,
      phone_item_id: 401,
      property_id: 601,
    },
    items: {
      agent_item: createPodioItem(501, {
        "latency-neutral-min": numberField(14),
        "latency-neutral-max": numberField(14),
      }),
      master_owner_item: createPodioItem(201),
      property_item: createPodioItem(601, {
        "smart-cash-offer-2": numberField(155000),
      }),
    },
    summary: {
      contact_window: "12PM-2PM CT",
      market_timezone: "Central",
      total_messages_sent: 1,
      language_preference: "English",
    },
    recent: {
      touch_count: 1,
      recent_events: [
        {
          direction: "Outbound",
          metadata: {
            selected_use_case: "ownership_check",
            next_expected_stage: SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
            selected_tone: "Warm",
          },
        },
      ],
    },
  };
}

test("seller-stage auto queue uses blank-use-case template lookup for stage 2 and agent latency schedule", async () => {
  const queue_calls = [];

  const result = await maybeQueueSellerStageReply({
    inbound_from: "+15550000001",
    context: buildContext(),
    classification: { language: "English", emotion: "calm" },
    message: "Yes, I own it.",
    now: "2026-04-03T16:00:00Z",
    schedule_resolver: ({ delay_min_minutes, delay_max_minutes }) => ({
      scheduled_for_local: "2026-04-03 12:14:00",
      scheduled_for_utc: "2026-04-03 17:14:00",
      timezone_label: "Central",
      contact_window: "12PM-2PM CT",
      agent_delay_minutes: delay_min_minutes,
      delay_min_minutes,
      delay_max_minutes,
    }),
    queue_message: async (payload) => {
      queue_calls.push(payload);
      return { ok: true, queue_item_id: 999 };
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.queued, true);
  assert.equal(result.reason, "seller_flow_reply_queued");
  assert.equal(result.brain_stage, "Offer");
  assert.equal(result.response_window.min_minutes, 14);
  assert.equal(result.response_window.max_minutes, 14);
  assert.equal(queue_calls.length, 1);
  assert.equal(queue_calls[0].use_case, "consider_selling");
  assert.equal(queue_calls[0].template_lookup_use_case, null);
  assert.equal(queue_calls[0].template_lookup_secondary_category, null);
  assert.equal(queue_calls[0].variant_group, "Stage 2 Consider Selling");
  assert.equal(queue_calls[0].send_priority, "_ Normal");
  assert.equal(queue_calls[0].scheduled_for_local, "2026-04-03 12:14:00");
});
