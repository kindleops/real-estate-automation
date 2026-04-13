import test from "node:test";
import assert from "node:assert/strict";

import {
  logInboundMessageEvent,
  __setLogInboundMessageEventTestDeps,
  __resetLogInboundMessageEventTestDeps,
} from "@/lib/domain/events/log-inbound-message-event.js";
import {
  logDeliveryEvent,
  __setLogDeliveryEventTestDeps,
  __resetLogDeliveryEventTestDeps,
} from "@/lib/domain/events/log-delivery-event.js";
import { createPodioItem } from "../helpers/test-helpers.js";

test("inbound message event writes the live conversation field and links the created event to the brain", async (t) => {
  let createdFields = null;
  let linkPayload = null;

  __setLogInboundMessageEventTestDeps({
    getCategoryValue: () => "Ownership Check",
    createMessageEvent: async (fields) => {
      createdFields = fields;
      return { item_id: 991 };
    },
    updateMessageEvent: async () => {},
    linkMessageEventToBrain: async (payload) => {
      linkPayload = payload;
      return { ok: true };
    },
  });

  t.after(() => {
    __resetLogInboundMessageEventTestDeps();
  });

  const result = await logInboundMessageEvent({
    brain_item: createPodioItem(11),
    conversation_item_id: 11,
    master_owner_id: 21,
    prospect_id: 31,
    property_id: 41,
    phone_item_id: 51,
    inbound_number_item_id: 61,
    message_body: "Hello there",
    provider_message_id: "SM123",
    raw_carrier_status: "received",
    received_at: "2026-04-10T00:00:00.000Z",
  });

  assert.equal(result.item_id, 991);
  assert.equal(createdFields["message-id"], "SM123");
  assert.equal(createdFields["text-2"], "SM123");
  assert.equal(createdFields["direction"], "Inbound");
  assert.equal(createdFields["category"], "Seller Inbound SMS");
  assert.equal(createdFields["message"], "Hello there");
  assert.equal(createdFields["character-count"], 11);
  assert.equal(createdFields["ai-output"], "");
  assert.equal(createdFields["source-app"], "External API");
  assert.equal(createdFields["processed-by"], "Manual Sender");
  assert.deepEqual(createdFields["conversation"], [11]);
  assert.deepEqual(linkPayload, {
    brain_item: createPodioItem(11),
    brain_id: 11,
    message_event_id: 991,
  });
});

test("inbound message event updates existing idempotency record when record_item_id provided", async (t) => {
  let updatedId = null;
  let updatedFields = null;
  let linkPayload = null;

  __setLogInboundMessageEventTestDeps({
    getCategoryValue: () => null,
    createMessageEvent: async () => {
      throw new Error("should not create when record_item_id is provided");
    },
    updateMessageEvent: async (id, fields) => {
      updatedId = id;
      updatedFields = fields;
    },
    linkMessageEventToBrain: async (payload) => {
      linkPayload = payload;
      return { ok: true };
    },
  });

  t.after(() => {
    __resetLogInboundMessageEventTestDeps();
  });

  const result = await logInboundMessageEvent({
    record_item_id: 888,
    brain_item: createPodioItem(11),
    conversation_item_id: 11,
    master_owner_id: 21,
    message_body: "yes",
    provider_message_id: "SM456",
    processing_metadata: { provider: "textgrid", inbound_from: "5551230001" },
  });

  // Updates the existing record instead of creating a new one
  assert.equal(result.item_id, 888);
  assert.equal(updatedId, 888);

  // message-id is NOT overwritten (preserves idempotency key)
  assert.equal(updatedFields["message-id"], undefined);
  assert.equal(updatedFields["text-2"], "SM456");

  // Actual seller message is written to Message Body
  assert.equal(updatedFields["message"], "yes");
  assert.equal(updatedFields["character-count"], 3);
  assert.equal(updatedFields["direction"], "Inbound");
  assert.equal(updatedFields["category"], "Seller Inbound SMS");

  // AI Output is cleared (no classification yet)
  assert.equal(updatedFields["ai-output"], "");

  // Processing metadata is written to dedicated field
  const meta = JSON.parse(updatedFields["processing-metadata"]);
  assert.equal(meta.provider, "textgrid");
  assert.equal(meta.inbound_from, "5551230001");

  // Brain link uses the existing record_item_id
  assert.deepEqual(linkPayload, {
    brain_item: createPodioItem(11),
    brain_id: 11,
    message_event_id: 888,
  });
});

test("delivery message event writes the live conversation field and links the created event to the brain", async (t) => {
  let createdFields = null;
  let linkPayload = null;

  __setLogDeliveryEventTestDeps({
    mapTextgridFailureBucket: () => "Other",
    createMessageEvent: async (fields) => {
      createdFields = fields;
      return { item_id: 992 };
    },
    linkMessageEventToBrain: async (payload) => {
      linkPayload = payload;
      return { ok: true };
    },
  });

  t.after(() => {
    __resetLogDeliveryEventTestDeps();
  });

  const result = await logDeliveryEvent({
    provider_message_id: "SM124",
    delivery_status: "delivered",
    raw_carrier_status: "delivered",
    queue_item_id: 71,
    client_reference_id: "client-71",
    master_owner_id: 21,
    prospect_id: 31,
    property_id: 41,
    phone_item_id: 51,
    textgrid_number_item_id: 61,
    conversation_item_id: 11,
  });

  assert.equal(result.item_id, 992);
  assert.equal(createdFields["message-id"], "SM124");
  assert.equal(createdFields["text-2"], "SM124");
  assert.equal(createdFields["direction"], "Outbound");
  assert.equal(createdFields["category"], "Delivery Update");
  assert.equal(createdFields["source-app"], "External API");
  assert.equal(createdFields["processed-by"], "System");
  assert.deepEqual(createdFields["conversation"], [11]);
  assert.deepEqual(linkPayload, {
    brain_id: 11,
    message_event_id: 992,
  });
});
