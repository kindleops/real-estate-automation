import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  resolveTextgridDeliveryCorrelation,
  __setTextgridDeliveryTestDeps,
  __resetTextgridDeliveryTestDeps,
} from "@/lib/flows/handle-textgrid-delivery.js";
import {
  appRefField,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetTextgridDeliveryTestDeps();
});

test("delivery correlation resolves exact queue item from provider send event metadata", async () => {
  const outboundEvent = createPodioItem(801, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("provider-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        provider_message_id: "provider-1",
      })
    ),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });
  const queueItem = createPodioItem(123);

  __setTextgridDeliveryTestDeps({
    findMessageEventItemsByMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
  });

  const result = await resolveTextgridDeliveryCorrelation({
    message_id: "provider-1",
    client_reference_id: null,
  });

  assert.equal(result.ok, true);
  assert.equal(result.correlation_mode, "provider_message_event");
  assert.deepEqual(result.exact_queue_item_ids, [123]);
  assert.equal(result.queue_items[0]?.item_id, 123);
});

test("delivery correlation refuses ambiguous exact queue matches", async () => {
  const outboundEvents = [
    createPodioItem(801, {
      "trigger-name": textField("queue-send:123"),
      "message-id": textField("provider-2"),
      "ai-output": textField(JSON.stringify({ queue_item_id: 123 })),
    }),
    createPodioItem(802, {
      "trigger-name": textField("queue-send:124"),
      "message-id": textField("provider-2"),
      "ai-output": textField(JSON.stringify({ queue_item_id: 124 })),
    }),
  ];

  __setTextgridDeliveryTestDeps({
    findMessageEventItemsByMessageId: async () => outboundEvents,
    getItem: async () => null,
    fetchAllItems: async () => [],
  });

  const result = await resolveTextgridDeliveryCorrelation({
    message_id: "provider-2",
    client_reference_id: null,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "ambiguous_queue_correlation");
  assert.deepEqual([...result.exact_queue_item_ids].sort((a, b) => a - b), [123, 124]);
});
