import test from "node:test";
import assert from "node:assert/strict";

import { finalizeSuccessfulQueueSend } from "@/lib/domain/queue/process-send-queue.js";
import { createPodioItem } from "../helpers/test-helpers.js";

test("finalizeSuccessfulQueueSend records a clean success path", async () => {
  const calls = [];
  const brain_item = createPodioItem(701);

  const result = await finalizeSuccessfulQueueSend(
    {
      queue_item_id: 123,
      phone_item: createPodioItem(401),
      phone_item_id: 401,
      brain_id: 701,
      brain_item,
      conversation_item_id: 701,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      market_id: 801,
      outbound_number_item_id: 501,
      template_id: 901,
      message_body: "Test message",
      message_variant: 2,
      latency_ms: 483,
      send_result: {
        message_id: "provider-1",
        to: "+15550000001",
        from: "+15550000002",
      },
      current_total_messages_sent: 4,
      client_reference_id: "queue-123",
      now: "2026-04-01T12:00:00.000Z",
    },
    {
      updateItem: async (item_id, payload) => {
        calls.push({ type: "updateItem", item_id, payload });
      },
      logOutboundMessageEvent: async (payload) => {
        calls.push({ type: "logOutboundMessageEvent", payload });
      },
      updateBrainAfterSend: async (payload) => {
        calls.push({ type: "updateBrainAfterSend", payload });
      },
      updateMasterOwnerAfterSend: async (payload) => {
        calls.push({ type: "updateMasterOwnerAfterSend", payload });
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.partial, false);
  assert.equal(result.sent, true);
  assert.equal(result.provider_message_id, "provider-1");
  assert.equal(calls.length, 4);
  assert.deepEqual(
    calls.map((entry) => entry.type),
    [
      "updateItem",
      "logOutboundMessageEvent",
      "updateBrainAfterSend",
      "updateMasterOwnerAfterSend",
    ]
  );
  assert.deepEqual(calls[1].payload, {
    brain_item,
    conversation_item_id: 701,
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    phone_item_id: 401,
    outbound_number_item_id: 501,
    sms_agent_id: null,
    property_address: null,
    message_body: "Test message",
    provider_message_id: "provider-1",
    queue_item_id: 123,
    client_reference_id: "queue-123",
    template_id: 901,
    message_variant: 2,
    latency_ms: 483,
    send_result: {
      message_id: "provider-1",
      to: "+15550000001",
      from: "+15550000002",
    },
  });
});

test("finalizeSuccessfulQueueSend reports partial failure if bookkeeping breaks after provider send", async () => {
  const calls = [];
  const brain_item = createPodioItem(701);

  const result = await finalizeSuccessfulQueueSend(
    {
      queue_item_id: 123,
      phone_item_id: 401,
      brain_id: 701,
      brain_item,
      conversation_item_id: 701,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      market_id: 801,
      outbound_number_item_id: 501,
      template_id: 901,
      message_body: "Test message",
      message_variant: 1,
      latency_ms: 215,
      send_result: {
        message_id: "provider-2",
        to: "+15550000001",
        from: "+15550000002",
      },
      current_total_messages_sent: 4,
      client_reference_id: "queue-123",
      now: "2026-04-01T12:00:00.000Z",
    },
    {
      updateItem: async () => {
        throw new Error("queue update unavailable");
      },
      logOutboundMessageEvent: async () => {
        calls.push("logOutboundMessageEvent");
      },
      updateBrainAfterSend: async () => {
        calls.push("updateBrainAfterSend");
      },
      updateMasterOwnerAfterSend: async () => {
        calls.push("updateMasterOwnerAfterSend");
      },
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.partial, true);
  assert.equal(result.sent, true);
  assert.deepEqual(calls, [
    "logOutboundMessageEvent",
    "updateBrainAfterSend",
    "updateMasterOwnerAfterSend",
  ]);
  assert.match(result.bookkeeping_errors[0], /^queue_sent_update_failed:/);
});
