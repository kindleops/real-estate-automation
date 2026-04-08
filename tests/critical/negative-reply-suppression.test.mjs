// ─── negative-reply-suppression.test.mjs ─────────────────────────────────
// Tests for:
//   A. isNegativeReply() detection
//   B. cancelPendingQueueItemsForOwner()
//   C. handle-textgrid-inbound negative-reply queue cancellation path

import test from "node:test";
import assert from "node:assert/strict";

import { isNegativeReply } from "@/lib/domain/classification/is-negative-reply.js";
import { cancelPendingQueueItemsForOwner } from "@/lib/domain/queue/cancel-pending-queue-items.js";

// ── A. isNegativeReply ────────────────────────────────────────────────────

test("isNegativeReply: exact single-word stop signals", () => {
  for (const msg of ["no", "nope", "stop", "unsubscribe", "remove"]) {
    assert.equal(isNegativeReply(msg), true, `expected true for "${msg}"`);
  }
});

test("isNegativeReply: case and punctuation insensitive", () => {
  assert.equal(isNegativeReply("NO"), true);
  assert.equal(isNegativeReply("Stop!"), true);
  assert.equal(isNegativeReply("STOP TEXTING ME"), true);
  assert.equal(isNegativeReply("Not Interested."), true);
});

test("isNegativeReply: multi-word negative phrases", () => {
  const cases = [
    "not interested",
    "I said no",
    "Wrong number",
    "wrong person",
    "Please stop",
    "leave me alone",
    "do not contact me",
    "don't text me",
    "not for sale",
    "not selling",
    "remove me",
    "opt out",
    "I'm not interested in selling",
  ];
  for (const msg of cases) {
    assert.equal(isNegativeReply(msg), true, `expected true for "${msg}"`);
  }
});

test("isNegativeReply: real production negative replies", () => {
  assert.equal(isNegativeReply("I said no"), true);
  assert.equal(isNegativeReply("No"), true);
  assert.equal(isNegativeReply("I already said no"), true);
  assert.equal(isNegativeReply("Please stop texting me"), true);
});

test("isNegativeReply: genuine positive / neutral messages return false", () => {
  const false_cases = [
    "yes",
    "yes please",
    "maybe",
    "ok",
    "sounds good",
    "call me",
    "what's the offer?",
    "I might be interested",
    "Tell me more",
    "How much are you offering?",
    "Can you send over details",
    "We are open to talking",
  ];
  for (const msg of false_cases) {
    assert.equal(isNegativeReply(msg), false, `expected false for "${msg}"`);
  }
});

test("isNegativeReply: empty / whitespace returns false", () => {
  assert.equal(isNegativeReply(""), false);
  assert.equal(isNegativeReply("   "), false);
  assert.equal(isNegativeReply(null), false);
  assert.equal(isNegativeReply(undefined), false);
});

test("isNegativeReply: short ambiguous words that are NOT stop signals", () => {
  // These are real words that could appear in conversational context
  assert.equal(isNegativeReply("ok"), false);
  assert.equal(isNegativeReply("sure"), false);
  assert.equal(isNegativeReply("fine"), false);
});

// ── B. cancelPendingQueueItemsForOwner ────────────────────────────────────

function buildQueueItem(item_id, status = "Queued") {
  return {
    item_id,
    fields: [
      {
        external_id: "queue-status",
        values: [{ value: { text: status } }],
      },
    ],
  };
}

test("cancelPendingQueueItemsForOwner: cancels Queued and Sending items", async () => {
  const items = [
    buildQueueItem(101, "Queued"),
    buildQueueItem(102, "Sending"),
    buildQueueItem(103, "Sent"),     // should NOT be canceled
    buildQueueItem(104, "Blocked"),  // should NOT be canceled
  ];
  const canceled_ids = [];

  const result = await cancelPendingQueueItemsForOwner(
    { master_owner_id: 999, phone_item_id: null },
    {
      filterAppItemsImpl: async () => ({ items }),
      updateItemImpl: async (item_id, _fields) => {
        canceled_ids.push(item_id);
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.canceled_count, 2);
  assert.equal(result.items_checked, 4);
  assert.deepEqual(canceled_ids.sort(), [101, 102].sort());
});

test("cancelPendingQueueItemsForOwner: no cancelable items returns ok with 0 count", async () => {
  const items = [buildQueueItem(201, "Sent"), buildQueueItem(202, "Failed")];

  const result = await cancelPendingQueueItemsForOwner(
    { master_owner_id: 888 },
    {
      filterAppItemsImpl: async () => ({ items }),
      updateItemImpl: async () => {},
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.canceled_count, 0);
  assert.equal(result.reason, "no_cancelable_items_found");
});

test("cancelPendingQueueItemsForOwner: skips when no owner or phone provided", async () => {
  const result = await cancelPendingQueueItemsForOwner({});
  assert.equal(result.ok, true);
  assert.equal(result.canceled_count, 0);
  assert.equal(result.skipped, true);
});

test("cancelPendingQueueItemsForOwner: handles fetch failure gracefully", async () => {
  const result = await cancelPendingQueueItemsForOwner(
    { master_owner_id: 777 },
    {
      filterAppItemsImpl: async () => {
        throw new Error("Podio 429");
      },
      updateItemImpl: async () => {},
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.reason, "fetch_failed");
  assert.equal(result.canceled_count, 0);
});
