import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  __resetLinkMessageEventToBrainTestDeps,
  __setLinkMessageEventToBrainTestDeps,
  linkMessageEventToBrain,
} from "@/lib/domain/brain/link-message-event-to-brain.js";
import { appRefField, createPodioItem } from "../helpers/test-helpers.js";

afterEach(() => {
  __resetLinkMessageEventToBrainTestDeps();
});

test("linkMessageEventToBrain appends a message event to linked-message-events", async () => {
  let update_payload = null;

  __setLinkMessageEventToBrainTestDeps({
    getBrainItem: async () =>
      createPodioItem(701, {
        "linked-message-events": [appRefField(9001)],
      }),
    applyBrainStateUpdate: async (payload) => {
      update_payload = payload;
      return { ok: true };
    },
  });

  const result = await linkMessageEventToBrain({
    brain_id: 701,
    message_event_id: 9002,
  });

  assert.equal(result.ok, true);
  assert.equal(result.linked, true);
  assert.deepEqual(result.linked_message_event_ids, [9001, 9002]);
  assert.deepEqual(update_payload, {
    brain_id: 701,
    reason: "message_event_linked",
    fields: {
      "linked-message-events": [9001, 9002],
    },
  });
});

test("linkMessageEventToBrain skips duplicate message event refs", async () => {
  let update_called = false;

  __setLinkMessageEventToBrainTestDeps({
    getBrainItem: async () =>
      createPodioItem(701, {
        "linked-message-events": [appRefField(9002)],
      }),
    applyBrainStateUpdate: async () => {
      update_called = true;
      return { ok: true };
    },
  });

  const result = await linkMessageEventToBrain({
    brain_id: 701,
    message_event_id: 9002,
  });

  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "message_event_already_linked");
  assert.equal(update_called, false);
});
