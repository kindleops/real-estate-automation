/**
 * outbound-event-relations.test.mjs
 *
 * Guards that outbound Message Events carry property_id and market_id when
 * the queue row has those relations available.
 *
 * Covered:
 *  1. finalizeSuccessfulQueueSend passes property_id + market_id to the
 *     event logger — event fields include the property and market relations.
 *  2. logFailedOutboundMessageEvent (buildFailedOutboundMessageEventFields)
 *     includes property and market app-ref fields.
 *  3. When property_id / market_id are null the fields are omitted (not
 *     written as nulls that break Podio relations).
 */

import test from "node:test";
import assert from "node:assert/strict";

import { finalizeSuccessfulQueueSend, buildFailedOutboundMessageEventFields } from "@/lib/domain/queue/process-send-queue.js";
import { buildOutboundMessageEventFields } from "@/lib/domain/events/log-outbound-message-event.js";
import { createPodioItem } from "../helpers/test-helpers.js";

// ── 1. finalizeSuccessfulQueueSend passes ids to event logger ────────────────

test("finalizeSuccessfulQueueSend: property_id and market_id reach the event logger", async () => {
  let captured_event_payload = null;

  await finalizeSuccessfulQueueSend(
    {
      queue_item_id: 100,
      phone_item: createPodioItem(401),
      phone_item_id: 401,
      brain_id: 701,
      brain_item: createPodioItem(701),
      conversation_item_id: 701,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      market_id: 801,
      outbound_number_item_id: 501,
      template_id: 901,
      message_body: "Hi there, we are interested in your property at 123 Main St.",
      send_result: { message_id: "msg-1", to: "+15550000001", from: "+15550000002" },
      current_total_messages_sent: 0,
      client_reference_id: "queue-100",
      now: "2026-04-06T12:00:00.000Z",
    },
    {
      updateItem: async () => {},
      logOutboundMessageEvent: async (payload) => {
        captured_event_payload = payload;
      },
      updateBrainAfterSend: async () => {},
      updateMasterOwnerAfterSend: async () => {},
    }
  );

  assert.ok(captured_event_payload, "logOutboundMessageEvent must have been called");
  assert.equal(captured_event_payload.property_id, 601, "property_id must be forwarded");
  assert.equal(captured_event_payload.market_id, 801, "market_id must be forwarded");
  assert.equal(captured_event_payload.master_owner_id, 201);
  assert.equal(captured_event_payload.phone_item_id, 401);
});

// ── 2. buildOutboundMessageEventFields includes property + market app-refs ────

test("buildOutboundMessageEventFields: property and market relations are set when ids are valid", () => {
  const fields = buildOutboundMessageEventFields({
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    phone_item_id: 401,
    outbound_number_item_id: 501,
    message_body: "Full message body here.",
    queue_item_id: 100,
    send_result: { ok: true, status: "sent", message_id: "msg-1" },
  });

  assert.deepEqual(fields["property"], [601], "property field must be array app-ref");
  assert.deepEqual(fields["market"], [801], "market field must be array app-ref");
  assert.deepEqual(fields["master-owner"], [201]);
  assert.deepEqual(fields["phone-number"], [401]);
});

test("buildOutboundMessageEventFields: property and market fields are omitted when ids are null", () => {
  const fields = buildOutboundMessageEventFields({
    master_owner_id: 201,
    prospect_id: null,
    property_id: null,
    market_id: null,
    phone_item_id: 401,
    message_body: "Message without property context.",
    queue_item_id: 100,
    send_result: { ok: true, status: "sent", message_id: "msg-2" },
  });

  assert.equal(fields["property"], undefined, "property must be omitted when null");
  assert.equal(fields["market"], undefined, "market must be omitted when null");
});

// ── 3. buildFailedOutboundMessageEventFields includes property + market ───────

test("buildFailedOutboundMessageEventFields: property and market relations are set on failure events", () => {
  const fields = buildFailedOutboundMessageEventFields({
    queue_item_id: 100,
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    phone_item_id: 401,
    outbound_number_item_id: 501,
    template_id: 901,
    message_body: "Hi there.",
    send_result: {
      ok: false,
      provider: "textgrid",
      message_id: null,
      status: "failed",
      error_status: 404,
      error_message: "Not Found",
    },
    retry_count: 0,
    max_retries: 3,
    client_reference_id: "queue-100",
  });

  assert.deepEqual(fields["property"], [601]);
  assert.deepEqual(fields["market"], [801]);
  assert.equal(fields["failure-bucket"], "Hard Bounce", "404 must map to Hard Bounce");
  assert.equal(fields["status-2"], "404", "raw carrier status must be the HTTP status code");
});

test("buildFailedOutboundMessageEventFields: is-final-failure is Yes on last retry", () => {
  const fields = buildFailedOutboundMessageEventFields({
    queue_item_id: 100,
    master_owner_id: 201,
    phone_item_id: 401,
    message_body: "Hi",
    send_result: { ok: false, error_status: 500, error_message: "Server Error", status: "failed" },
    retry_count: 2,
    max_retries: 3,
    client_reference_id: "queue-100",
  });

  assert.equal(fields["is-final-failure"], "Yes");
});

test("buildFailedOutboundMessageEventFields: is-final-failure is No when retries remain", () => {
  const fields = buildFailedOutboundMessageEventFields({
    queue_item_id: 100,
    master_owner_id: 201,
    phone_item_id: 401,
    message_body: "Hi",
    send_result: { ok: false, error_status: 500, error_message: "Server Error", status: "failed" },
    retry_count: 0,
    max_retries: 3,
    client_reference_id: "queue-100",
  });

  assert.equal(fields["is-final-failure"], "No");
});
