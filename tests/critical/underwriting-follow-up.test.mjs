import test from "node:test";
import assert from "node:assert/strict";

import { maybeQueueUnderwritingFollowUp } from "@/lib/domain/underwriting/maybe-queue-underwriting-follow-up.js";
import {
  categoryField,
  createPodioItem,
  numberField,
} from "../helpers/test-helpers.js";

function buildMultifamilyContext(previous_use_case = "mf_occupancy") {
  return {
    found: true,
    ids: {
      property_id: 601,
    },
    items: {
      property_item: createPodioItem(601, {
        "property-type": categoryField("Apartment"),
        "number-of-units": numberField(12),
      }),
    },
    recent: {
      recent_events: [
        {
          direction: "Outbound",
          selected_use_case: previous_use_case,
        },
      ],
    },
    summary: {
      property_type: "Apartment",
      property_address: "123 Main St",
    },
  };
}

test("multifamily underwriting follow-up asks for rents after occupancy is answered", async () => {
  const result = await maybeQueueUnderwritingFollowUp({
    inbound_from: "+15550000001",
    context: buildMultifamilyContext("mf_occupancy"),
    message: "10 are occupied right now.",
    underwriting: {
      signals: {
        unit_count: 12,
        occupancy_status: "Tenant Occupied",
        rents_present: false,
        expenses_present: false,
      },
      strategy: {
        property_type: "Multifamily",
        strategy: "mf_auto_underwrite",
        needs_manual_review: false,
      },
    },
    queue_status: "Queued",
    create_brain_if_missing: false,
    classification: { language: "English", emotion: "calm" },
    route: { stage: "Offer" },
    created_by: "test",
    queue_message: async () => ({ ok: true }),
  });

  assert.equal(result.ok, true);
  assert.equal(result.queued, true);
  assert.equal(result.offer_ready, false);
  assert.equal(result.follow_up.use_case, "mf_rents");
});

test("multifamily underwriting follow-up finalizes to internal offer when seller does not know expenses", async () => {
  const queue_calls = [];

  const result = await maybeQueueUnderwritingFollowUp({
    inbound_from: "+15550000001",
    context: buildMultifamilyContext("mf_expenses"),
    message: "No, I do not know the expenses.",
    underwriting: {
      signals: {
        unit_count: 12,
        occupancy_status: "Tenant Occupied",
        rents_present: true,
        expenses_present: false,
      },
      strategy: {
        property_type: "Multifamily",
        strategy: "mf_auto_underwrite",
        needs_manual_review: false,
      },
    },
    queue_status: "Queued",
    create_brain_if_missing: false,
    classification: { language: "English", emotion: "calm" },
    route: { stage: "Offer" },
    created_by: "test",
    queue_message: async (payload) => {
      queue_calls.push(payload);
      return { ok: true };
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.queued, true);
  assert.equal(result.offer_ready, true);
  assert.equal(result.follow_up.use_case, "mf_underwriting_ack");
  assert.equal(queue_calls.length, 1);
  assert.equal(queue_calls[0].use_case, "mf_underwriting_ack");
});

test("multifamily underwriting follow-up acknowledges and advances when rents are missing but seller does not know them", async () => {
  const queue_calls = [];

  const result = await maybeQueueUnderwritingFollowUp({
    inbound_from: "+15550000001",
    context: buildMultifamilyContext("mf_rents"),
    message: "I don't know the rents off hand.",
    underwriting: {
      signals: {
        unit_count: 12,
        occupancy_status: "Tenant Occupied",
        rents_present: false,
        expenses_present: true,
      },
      strategy: {
        property_type: "Multifamily",
        strategy: "mf_auto_underwrite",
        needs_manual_review: false,
      },
    },
    queue_status: "Queued",
    create_brain_if_missing: false,
    classification: { language: "English", emotion: "calm" },
    route: { stage: "Offer" },
    created_by: "test",
    queue_message: async (payload) => {
      queue_calls.push(payload);
      return { ok: true };
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.queued, true);
  assert.equal(result.offer_ready, true);
  assert.equal(result.follow_up.use_case, "mf_underwriting_ack");
  assert.equal(queue_calls.length, 1);
  assert.equal(queue_calls[0].use_case, "mf_underwriting_ack");
});
