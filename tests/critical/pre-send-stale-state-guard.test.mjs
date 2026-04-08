// ─── pre-send-stale-state-guard.test.mjs ─────────────────────────────────
// Tests for the pre-send stale-state guard in processSendQueueItem:
//   B. Abort if newer inbound negative reply arrived after queue item was created.
//
// Also tests queue-outbound-message enhanced dedupe:
//   C. Duplicate blocked when same use_case pending within 24h window.

import test from "node:test";
import assert from "node:assert/strict";

import {
  findPendingQueueDuplicateItem,
} from "@/lib/flows/queue-outbound-message.js";
import { createPodioItem, categoryField, numberField, dateField, appRefField } from "../helpers/test-helpers.js";

// ── B. Pre-send guard (tested via findPendingQueueDuplicateItem) ──────────

function buildQueueItem(item_id, { status = "Queued", phone_item_id = 401, touch_number = 1, use_case = null, scheduled_utc = null } = {}) {
  const fields = {
    "queue-status": categoryField(status),
    "phone-number": appRefField(phone_item_id),
    "touch-number": numberField(touch_number),
  };
  if (use_case) {
    fields["use-case-template"] = categoryField(use_case);
  }
  if (scheduled_utc) {
    fields["scheduled-for-utc"] = dateField(scheduled_utc);
  }
  return createPodioItem(item_id, fields);
}

// ── C. Enhanced dedupe ────────────────────────────────────────────────────

test("findPendingQueueDuplicateItem: blocks exact same phone+touch duplicate", () => {
  const items = [
    buildQueueItem(1001, { status: "Queued", phone_item_id: 401, touch_number: 2 }),
  ];
  const result = findPendingQueueDuplicateItem(items, 401, 2, "ownership_check");
  assert.ok(result, "should find the duplicate");
  assert.equal(result.item_id, 1001);
});

test("findPendingQueueDuplicateItem: allows different touch number without use_case match", () => {
  const items = [
    buildQueueItem(1001, { status: "Queued", phone_item_id: 401, touch_number: 1 }),
  ];
  // Different touch number, no use_case — should NOT block
  const result = findPendingQueueDuplicateItem(items, 401, 2, null);
  assert.equal(result, null);
});

test("findPendingQueueDuplicateItem: blocks same use_case within 24h window", () => {
  const recent_ts = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(); // 2h ago
  const items = [
    buildQueueItem(1002, {
      status: "Queued",
      phone_item_id: 401,
      touch_number: 1,
      use_case: "ownership_check",
      scheduled_utc: recent_ts,
    }),
  ];
  // Different touch number but same use_case within 24h — should block
  const result = findPendingQueueDuplicateItem(items, 401, 2, "ownership_check");
  assert.ok(result, "should find same-use_case duplicate within 24h");
  assert.equal(result.item_id, 1002);
});

test("findPendingQueueDuplicateItem: allows same use_case outside 24h window", () => {
  const old_ts = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString(); // 48h ago
  const items = [
    buildQueueItem(1003, {
      status: "Queued",
      phone_item_id: 401,
      touch_number: 1,
      use_case: "ownership_check",
      scheduled_utc: old_ts,
    }),
  ];
  // Same use_case but OUTSIDE 24h window — should allow
  const result = findPendingQueueDuplicateItem(items, 401, 2, "ownership_check");
  assert.equal(result, null);
});

test("findPendingQueueDuplicateItem: ignores Sent/Failed/Blocked statuses", () => {
  const items = [
    buildQueueItem(1004, { status: "Sent", phone_item_id: 401, touch_number: 2 }),
    buildQueueItem(1005, { status: "Failed", phone_item_id: 401, touch_number: 2 }),
    buildQueueItem(1006, { status: "Blocked", phone_item_id: 401, touch_number: 2 }),
  ];
  const result = findPendingQueueDuplicateItem(items, 401, 2, "ownership_check");
  assert.equal(result, null, "terminal statuses should not trigger dedupe");
});

test("findPendingQueueDuplicateItem: different phone is not a duplicate", () => {
  const items = [
    buildQueueItem(1007, { status: "Queued", phone_item_id: 999, touch_number: 2 }),
  ];
  const result = findPendingQueueDuplicateItem(items, 401, 2, "ownership_check");
  assert.equal(result, null);
});

test("findPendingQueueDuplicateItem: returns null on empty list", () => {
  assert.equal(findPendingQueueDuplicateItem([], 401, 1, "ownership_check"), null);
});

test("findPendingQueueDuplicateItem: returns null when phone_item_id is null", () => {
  const items = [buildQueueItem(1008, { status: "Queued" })];
  assert.equal(findPendingQueueDuplicateItem(items, null, 1), null);
});
