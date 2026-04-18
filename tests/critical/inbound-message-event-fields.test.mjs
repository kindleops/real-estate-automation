import test from "node:test";
import assert from "node:assert/strict";

import {
  buildInboundMessageEventFields,
  logInboundMessageEvent,
  __setLogInboundMessageEventTestDeps,
  __resetLogInboundMessageEventTestDeps,
} from "@/lib/domain/events/log-inbound-message-event.js";
import { normalizePodioFieldMap, hasAttachedSchema, PODIO_ATTACHED_SCHEMA } from "@/lib/podio/schema.js";
import APP_IDS from "@/lib/config/app-ids.js";
import { createPodioItem } from "../helpers/test-helpers.js";

test("buildInboundMessageEventFields writes the canonical seller event key and linked fields", () => {
  const fields = buildInboundMessageEventFields({
    brain_item: createPodioItem(100),
    conversation_item_id: 100,
    master_owner_id: 200,
    prospect_id: 300,
    property_id: 400,
    market_id: 500,
    phone_item_id: 600,
    inbound_number_item_id: 700,
    sms_agent_id: 800,
    property_address: "123 Main St",
    message_body: "STOP",
    provider_message_id: "SM_test_123",
    raw_carrier_status: "received",
    received_at: "2026-06-01T12:00:00.000Z",
    prior_message_id: "outbound:queue-321",
    response_to_message_id: "outbound:queue-321",
  });

  assert.equal(fields["message-id"], "inbound:SM_test_123");
  assert.equal(fields["text-2"], "SM_test_123");
  assert.equal(fields["direction"], "Inbound");
  assert.equal(fields["category"], "Seller Inbound SMS");
  assert.equal(fields["status-3"], "Received");
  assert.equal(fields["message"], "STOP");
  assert.equal(fields["character-count"], 4);
  assert.equal(fields["number-2"], 1);
  assert.equal(fields["property-address"], "123 Main St");
  assert.equal(fields["prior-message-id"], "outbound:queue-321");
  assert.equal(fields["response-to-message-id"], "outbound:queue-321");
  assert.equal(fields["is-opt-out"], "Yes");
  assert.equal(fields["opt-out-keyword"], "STOP");
  assert.deepEqual(fields["master-owner"], [200]);
  assert.deepEqual(fields["linked-seller"], [300]);
  assert.deepEqual(fields["property"], [400]);
  assert.deepEqual(fields["market"], [500]);
  assert.deepEqual(fields["phone-number"], [600]);
  assert.deepEqual(fields["textgrid-number"], [700]);
  assert.deepEqual(fields["sms-agent"], [800]);
  assert.deepEqual(fields["conversation"], [100]);
});

test("logInboundMessageEvent update path rewrites the canonical seller event row", async (t) => {
  let updatedFields = null;

  __setLogInboundMessageEventTestDeps({
    getCategoryValue: () => "Offer",
    createMessageEvent: async () => {
      throw new Error("should not create when record_item_id is provided");
    },
    updateMessageEvent: async (_id, fields) => {
      updatedFields = fields;
    },
    linkMessageEventToBrain: async () => ({ ok: true }),
  });

  t.after(() => __resetLogInboundMessageEventTestDeps());

  await logInboundMessageEvent({
    record_item_id: 999,
    brain_item: createPodioItem(100),
    conversation_item_id: 100,
    master_owner_id: 200,
    prospect_id: 300,
    property_id: 400,
    market_id: 500,
    phone_item_id: 600,
    inbound_number_item_id: 700,
    sms_agent_id: 800,
    property_address: "123 Main St",
    message_body: "I want to sell my house",
    provider_message_id: "SM_test_123",
    raw_carrier_status: "received",
    received_at: "2026-06-01T12:00:00.000Z",
    processed_by: "Manual Sender",
    source_app: "External API",
    trigger_name: "textgrid-inbound",
  });

  assert.equal(updatedFields["message-id"], "inbound:SM_test_123");
  assert.equal(updatedFields["direction"], "Inbound");
  assert.equal(updatedFields["category"], "Seller Inbound SMS");
  assert.equal(updatedFields["status-3"], "Received");
  assert.equal(updatedFields["message"], "I want to sell my house");
  assert.equal(updatedFields["text-2"], "SM_test_123");
  assert.equal(updatedFields["status-2"], "received");
  assert.equal(updatedFields["trigger-name"], "textgrid-inbound");
  assert.equal(updatedFields["character-count"], 23);
  assert.deepEqual(updatedFields["master-owner"], [200]);
  assert.deepEqual(updatedFields["linked-seller"], [300]);
  assert.deepEqual(updatedFields["property"], [400]);
  assert.deepEqual(updatedFields["market"], [500]);
  assert.deepEqual(updatedFields["phone-number"], [600]);
  assert.deepEqual(updatedFields["textgrid-number"], [700]);
  assert.deepEqual(updatedFields["sms-agent"], [800]);
  assert.deepEqual(updatedFields["conversation"], [100]);
  assert.equal(updatedFields["ai-route"], "Offer");
});

test("logInboundMessageEvent excludes null linked records", async (t) => {
  let updatedFields = null;

  __setLogInboundMessageEventTestDeps({
    getCategoryValue: () => null,
    createMessageEvent: async () => {
      throw new Error("should not create");
    },
    updateMessageEvent: async (_id, fields) => {
      updatedFields = fields;
    },
    linkMessageEventToBrain: async () => ({ ok: true }),
  });

  t.after(() => __resetLogInboundMessageEventTestDeps());

  await logInboundMessageEvent({
    record_item_id: 888,
    message_body: "test sms",
    provider_message_id: "SM_null_test",
  });

  assert.equal(updatedFields["direction"], "Inbound");
  assert.equal(updatedFields["category"], "Seller Inbound SMS");
  assert.equal(updatedFields["message"], "test sms");
  assert.equal(updatedFields["master-owner"], undefined);
  assert.equal(updatedFields["linked-seller"], undefined);
  assert.equal(updatedFields["property"], undefined);
  assert.equal(updatedFields["market"], undefined);
  assert.equal(updatedFields["phone-number"], undefined);
  assert.equal(updatedFields["textgrid-number"], undefined);
  assert.equal(updatedFields["conversation"], undefined);
  assert.equal(updatedFields["ai-route"], undefined);
});

test("logInboundMessageEvent fields pass through normalizePodioFieldMap without throwing", () => {
  assert.ok(hasAttachedSchema(APP_IDS.message_events), "message_events schema must be attached");

  const fields = {
    "message-id": "inbound:SM_norm_test",
    "text-2": "SM_norm_test",
    "direction": "Inbound",
    "category": "Seller Inbound SMS",
    "timestamp": { start: "2026-06-01T12:00:00.000Z" },
    "message": "I want to sell",
    "character-count": 14,
    "number-2": 1,
    "status-3": "Received",
    "status-2": "received",
    "processed-by": "Manual Sender",
    "source-app": "External API",
    "trigger-name": "textgrid-inbound",
    "master-owner": [200],
    "linked-seller": [300],
    "property": [400],
    "market": [500],
    "phone-number": [600],
    "textgrid-number": [700],
    "sms-agent": [800],
    "conversation": [100],
  };

  const normalized = normalizePodioFieldMap(APP_IDS.message_events, fields);
  assert.ok(normalized, "normalization should return a result");
  assert.equal(typeof normalized["direction"], "number");
  assert.equal(typeof normalized["status-3"], "number");
  assert.equal(normalized["message"], "I want to sell");
  assert.equal(normalized["trigger-name"], "textgrid-inbound");
  assert.ok(normalized["timestamp"]?.start);
  assert.equal(typeof normalized["character-count"], "number");
  assert.ok(Array.isArray(normalized["master-owner"]));
  assert.ok(Array.isArray(normalized["conversation"]));
});

test("supplement source-app options preserve base schema IDs", () => {
  const schema = PODIO_ATTACHED_SCHEMA[String(APP_IDS.message_events)];
  const sourceApp = schema?.fields?.["source-app"];

  assert.ok(sourceApp, "source-app field must exist");
  assert.equal(sourceApp.type, "category");

  const sendQueue = sourceApp.options.find((o) => o.text === "Send Queue");
  const externalApi = sourceApp.options.find((o) => o.text === "External API");

  assert.ok(sendQueue, "Send Queue option must exist");
  assert.ok(externalApi, "External API option must exist");
  assert.equal(sendQueue.id, 1, "Send Queue should have real Podio ID 1");
  assert.equal(externalApi.id, 3, "External API should have real Podio ID 3");
});

test("supplement processed-by options preserve base schema IDs", () => {
  const schema = PODIO_ATTACHED_SCHEMA[String(APP_IDS.message_events)];
  const processedBy = schema?.fields?.["processed-by"];

  assert.ok(processedBy, "processed-by field must exist");
  assert.equal(processedBy.type, "category");

  const manual = processedBy.options.find((o) => o.text === "Manual Sender");
  assert.ok(manual, "Manual Sender option must exist");
  assert.equal(manual.id, 1, "Manual Sender should have real Podio ID 1");
});

test("normalizePodioFieldMap survives the canonical seller event types and statuses", () => {
  const eventTypes = [
    "Seller Inbound SMS",
    "Seller Outbound SMS",
    "Delivery Update",
    "Send Failure",
    "Seller Opt Out",
    "Seller Stage Transition",
  ];

  for (const eventType of eventTypes) {
    assert.doesNotThrow(() =>
      normalizePodioFieldMap(APP_IDS.message_events, { category: eventType })
    );
  }

  for (const direction of ["Inbound", "Outbound"]) {
    const normalized = normalizePodioFieldMap(APP_IDS.message_events, {
      direction,
    });
    assert.equal(typeof normalized.direction, "number");
  }

  for (const status of ["Pending", "Sent", "Delivered", "Failed", "Received"]) {
    const normalized = normalizePodioFieldMap(APP_IDS.message_events, {
      "status-3": status,
    });
    assert.equal(typeof normalized["status-3"], "number");
  }
});
