import test from "node:test";
import assert from "node:assert/strict";

import {
  logInboundMessageEvent,
  __setLogInboundMessageEventTestDeps,
  __resetLogInboundMessageEventTestDeps,
} from "@/lib/domain/events/log-inbound-message-event.js";
import {
  logOutboundMessageEvent,
  __setLogOutboundMessageEventTestDeps,
  __resetLogOutboundMessageEventTestDeps,
} from "@/lib/domain/events/log-outbound-message-event.js";
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
    market_id: 51,
    phone_item_id: 61,
    inbound_number_item_id: 71,
    sms_agent_id: 81,
    property_address: "123 Main St",
    message_body: "Hello there",
    provider_message_id: "SM123",
    raw_carrier_status: "received",
    received_at: "2026-04-10T00:00:00.000Z",
    inbound_from: "+15550000001",
    inbound_to: "+15550000002",
    prior_message_id: "outbound:queue-123",
    response_to_message_id: "outbound:queue-123",
  });

  assert.equal(result.item_id, 991);
  assert.equal(createdFields["message-id"], "inbound:SM123");
  assert.equal(createdFields["text-2"], "SM123");
  assert.equal(createdFields["direction"], "Inbound");
  assert.equal(createdFields["category"], "Seller Inbound SMS");
  assert.equal(createdFields["message"], "Hello there");
  assert.equal(createdFields["character-count"], 11);
  assert.equal(createdFields["property-address"], "123 Main St");
  assert.deepEqual(createdFields["conversation"], [11]);
  assert.deepEqual(createdFields["sms-agent"], [81]);
  assert.equal(createdFields["prior-message-id"], "outbound:queue-123");
  assert.equal(createdFields["response-to-message-id"], "outbound:queue-123");
  assert.deepEqual(linkPayload, {
    brain_item: createPodioItem(11),
    brain_id: 11,
    message_event_id: 991,
  });
});

test("inbound message event updates an existing seller event when record_item_id is provided", async (t) => {
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
    inbound_from: "+15550000001",
    inbound_to: "+15550000002",
  });

  assert.equal(result.item_id, 888);
  assert.equal(updatedId, 888);
  assert.equal(updatedFields["message-id"], "inbound:SM456");
  assert.equal(updatedFields["text-2"], "SM456");
  assert.equal(updatedFields["message"], "yes");
  assert.equal(updatedFields["character-count"], 3);
  assert.equal(updatedFields["direction"], "Inbound");
  assert.equal(updatedFields["category"], "Seller Inbound SMS");
  assert.deepEqual(linkPayload, {
    brain_item: createPodioItem(11),
    brain_id: 11,
    message_event_id: 888,
  });
});

test("outbound message event writes the canonical seller event row and links it to the brain", async (t) => {
  let createdFields = null;
  let linkPayload = null;

  __setLogOutboundMessageEventTestDeps({
    createMessageEvent: async (fields) => {
      createdFields = fields;
      return { item_id: 993 };
    },
    linkMessageEventToBrain: async (payload) => {
      linkPayload = payload;
      return { ok: true };
    },
  });

  t.after(() => {
    __resetLogOutboundMessageEventTestDeps();
  });

  const result = await logOutboundMessageEvent({
    brain_item: createPodioItem(11),
    conversation_item_id: 11,
    master_owner_id: 21,
    prospect_id: 31,
    property_id: 41,
    market_id: 51,
    phone_item_id: 61,
    outbound_number_item_id: 71,
    sms_agent_id: 81,
    template_id: 91,
    property_address: "123 Main St",
    message_body: "Checking in about your property.",
    provider_message_id: "SM124",
    queue_item_id: 123,
    client_reference_id: "queue-123",
    message_variant: 2,
    sent_at: "2026-04-10 00:00:00",
  });

  assert.equal(result.item_id, 993);
  assert.equal(createdFields["message-id"], "outbound:queue-123");
  assert.equal(createdFields["text-2"], "SM124");
  assert.equal(createdFields["direction"], "Outbound");
  assert.equal(createdFields["category"], "Seller Outbound SMS");
  assert.equal(createdFields["message"], "Checking in about your property.");
  assert.equal(createdFields["property-address"], "123 Main St");
  assert.deepEqual(createdFields["master-owner"], [21]);
  assert.deepEqual(createdFields["linked-seller"], [31]);
  assert.deepEqual(createdFields["property"], [41]);
  assert.deepEqual(createdFields["market"], [51]);
  assert.deepEqual(createdFields["phone-number"], [61]);
  assert.deepEqual(createdFields["textgrid-number"], [71]);
  assert.deepEqual(createdFields["sms-agent"], [81]);
  assert.deepEqual(createdFields["template"], [91]);
  assert.deepEqual(createdFields["conversation"], [11]);
  assert.deepEqual(linkPayload, {
    brain_item: createPodioItem(11),
    brain_id: 11,
    message_event_id: 993,
  });
});

test("delivery updates the existing outbound event instead of creating a new Message Event row", async (t) => {
  let statusPayload = null;

  __setLogDeliveryEventTestDeps({
    updateMessageEventStatus: async (payload) => {
      statusPayload = payload;
      return { ok: true, event_item_id: 992 };
    },
  });

  t.after(() => {
    __resetLogDeliveryEventTestDeps();
  });

  const result = await logDeliveryEvent({
    provider_message_id: "SM124",
    delivery_status: "delivered",
    raw_carrier_status: "delivered",
    occurred_at: "2026-04-10T00:10:00.000Z",
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, true);
  assert.equal(result.event_item_id, 992);
  assert.equal(statusPayload.provider_message_id, "SM124");
  assert.equal(statusPayload.delivery_status, "Delivered");
  assert.equal(statusPayload.provider_delivery_status, "delivered");
  assert.equal(statusPayload.delivered_at, "2026-04-10T00:10:00.000Z");
});
