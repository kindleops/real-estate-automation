import test from "node:test";
import assert from "node:assert/strict";

import { findPendingQueueDuplicateItem } from "@/lib/flows/queue-outbound-message.js";
import {
  appRefField,
  categoryField,
  numberField,
  createPodioItem,
} from "../helpers/test-helpers.js";

function createQueueItem(item_id, { phone_item_id, queue_status, touch_number }) {
  return createPodioItem(item_id, {
    "phone-number": appRefField(phone_item_id),
    "queue-status": categoryField(queue_status),
    "touch-number": numberField(touch_number),
  });
}

test("findPendingQueueDuplicateItem finds pending same-phone same-touch duplicate", () => {
  const duplicate = findPendingQueueDuplicateItem(
    [
      createQueueItem(1001, { phone_item_id: 401, queue_status: "Queued", touch_number: 2 }),
      createQueueItem(1002, { phone_item_id: 401, queue_status: "Sent", touch_number: 2 }),
    ],
    401,
    2
  );

  assert.ok(duplicate);
  assert.equal(duplicate.item_id, 1001);
});

test("findPendingQueueDuplicateItem ignores rows with different touch-number", () => {
  const duplicate = findPendingQueueDuplicateItem(
    [
      createQueueItem(1001, { phone_item_id: 401, queue_status: "Queued", touch_number: 1 }),
      createQueueItem(1002, { phone_item_id: 401, queue_status: "Sending", touch_number: 3 }),
    ],
    401,
    2
  );

  assert.equal(duplicate, null);
});

