/**
 * failed-send-guardrails.test.mjs
 *
 * Proves the production guardrails added after the one-word-send incident:
 *
 *  1. validateSendQueueItem rejects one-word bodies (junk_message_body).
 *  2. validateSendQueueItem rejects two-word bodies (still junk).
 *  3. validateSendQueueItem accepts a valid ≥3-word body.
 *  4. findRecentDuplicate does NOT count a failed message event as a recent touch.
 *  5. findRecentDuplicate DOES count a sent message event (delivery_status="Sent").
 *  6. findRecentDuplicate DOES count a delivered message event.
 *  7. deriveOwnerTouchCount excludes failed events from the sequence number.
 *  8. deriveOwnerTouchCount counts non-failed events normally.
 *  9. same_day_touch_advancement is NOT blocked by a failed event.
 * 10. finalizeSuccessfulQueueSend is never reached for junk bodies (queue blocked before send).
 */

import test from "node:test";
import assert from "node:assert/strict";

import { validateSendQueueItem } from "@/lib/domain/queue/validate-send-queue-item.js";
import {
  findRecentDuplicate,
  deriveOwnerTouchCount,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import { createPodioItem, categoryField, textField, dateField, numberField, appRefField } from "../helpers/test-helpers.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeQueueItem(item_id, fields = {}) {
  return createPodioItem(item_id, fields);
}

function makeMessageEvent(item_id, { phone_item_id, delivery_status, timestamp_iso } = {}) {
  return createPodioItem(item_id, {
    ...(phone_item_id ? { "phone-number": appRefField(phone_item_id) } : {}),
    ...(delivery_status ? { "status-3": categoryField(delivery_status) } : {}),
    ...(timestamp_iso ? { timestamp: dateField(timestamp_iso) } : {}),
    direction: categoryField("Outbound"),
  });
}

function makeQueueHistory(queue_items = [], outbound_events = []) {
  return { queue_items, outbound_events };
}

const NOW_ISO = "2026-04-06T14:00:00.000Z";
const NOW_TS = new Date(NOW_ISO).getTime();
const RECENT_ISO = "2026-04-06T10:00:00.000Z"; // 4 hours ago — within same-day window
const OLD_ISO = "2026-04-04T10:00:00.000Z";    // 2 days ago — outside same-day window

const PHONE_ID = 5001;
const CUTOFF_TS = NOW_TS - 24 * 60 * 60 * 1000; // 24-hour same-day window

// ── test 1: one-word body rejected ───────────────────────────────────────────

test("validateSendQueueItem rejects a one-word body with junk_message_body", () => {
  const item = makeQueueItem(1001, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Hi"),
    "retry-count": numberField(0),
    "max-retries": numberField(3),
  });

  const result = validateSendQueueItem(item);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "junk_message_body");
  assert.equal(result.word_count, 1);
  assert.equal(result.message_body, "Hi");
});

// ── test 2: two-word body rejected ───────────────────────────────────────────

test("validateSendQueueItem rejects a two-word body (Hola amigo) with junk_message_body", () => {
  const item = makeQueueItem(1002, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Hola amigo"),
    "retry-count": numberField(0),
    "max-retries": numberField(3),
  });

  const result = validateSendQueueItem(item);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "junk_message_body");
  assert.equal(result.word_count, 2);
});

// ── test 3: valid body accepted ───────────────────────────────────────────────

test("validateSendQueueItem accepts a body with 3 or more words", () => {
  const item = makeQueueItem(1003, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Hi there John, I am reaching out about your property."),
    "retry-count": numberField(0),
    "max-retries": numberField(3),
    "template-2": appRefField(9001),
  });

  const result = validateSendQueueItem(item);

  assert.equal(result.ok, true, "valid 3+ word body must pass validation");
});

test("validateSendQueueItem rejects blank seller greeting bodies", () => {
  for (const [index, greeting] of ["Hey", "Hi", "Hello", "Hola"].entries()) {
    const message = `${greeting} , this is Chris. Do you still own 123 Main St?`;
    const item = makeQueueItem(1004 + index, {
      "queue-status": categoryField("Queued"),
      "phone-number": appRefField(401),
      "textgrid-number": appRefField(501),
      "message-text": textField(message),
      "retry-count": numberField(0),
      "max-retries": numberField(3),
      "template-2": appRefField(9001),
    });

    const result = validateSendQueueItem(item);

    assert.equal(result.ok, false);
    assert.equal(result.reason, "blank_greeting_message_body");
    assert.equal(result.message_body, message);
  }
});

test("validateSendQueueItem allows hydrated seller greeting bodies", () => {
  const item = makeQueueItem(1005, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Hey John, this is Chris. Do you still own 123 Main St?"),
    "retry-count": numberField(0),
    "max-retries": numberField(3),
    "template-2": appRefField(9001),
  });

  const result = validateSendQueueItem(item);

  assert.equal(result.ok, true, `expected ok=true, got reason: ${result.reason}`);
});

// ── test 4: failed event does NOT block findRecentDuplicate ───────────────────

test("findRecentDuplicate does not count a failed message event as a recent touch", () => {
  const failed_event = makeMessageEvent(9001, {
    phone_item_id: PHONE_ID,
    delivery_status: "Failed",
    timestamp_iso: RECENT_ISO,
  });

  const history = makeQueueHistory([], [failed_event]);
  const result = findRecentDuplicate(history, PHONE_ID, CUTOFF_TS);

  assert.equal(result, null, "failed event must not trigger same-day touch block");
});

// ── test 5: sent event DOES block findRecentDuplicate ─────────────────────────

test("findRecentDuplicate counts a sent message event (delivery_status=Sent) as a recent touch", () => {
  const sent_event = makeMessageEvent(9002, {
    phone_item_id: PHONE_ID,
    delivery_status: "Sent",
    timestamp_iso: RECENT_ISO,
  });

  const history = makeQueueHistory([], [sent_event]);
  const result = findRecentDuplicate(history, PHONE_ID, CUTOFF_TS);

  assert.notEqual(result, null, "a sent event must be detected as a recent touch");
  assert.equal(result.type, "message_event");
});

// ── test 6: delivered event DOES block findRecentDuplicate ────────────────────

test("findRecentDuplicate counts a delivered message event as a recent touch", () => {
  const delivered_event = makeMessageEvent(9003, {
    phone_item_id: PHONE_ID,
    delivery_status: "Delivered",
    timestamp_iso: RECENT_ISO,
  });

  const history = makeQueueHistory([], [delivered_event]);
  const result = findRecentDuplicate(history, PHONE_ID, CUTOFF_TS);

  assert.notEqual(result, null, "a delivered event must be detected as a recent touch");
  assert.equal(result.type, "message_event");
});

// ── test 7: failed events excluded from deriveOwnerTouchCount ─────────────────

test("deriveOwnerTouchCount excludes failed message events from the touch sequence number", () => {
  const failed1 = makeMessageEvent(9010, { delivery_status: "Failed" });
  const failed2 = makeMessageEvent(9011, { delivery_status: "Failed" });

  const history = makeQueueHistory([], [failed1, failed2]);
  const count = deriveOwnerTouchCount(history);

  assert.equal(count, 0, "two failed events must not inflate touch count");
});

// ── test 8: non-failed events ARE counted by deriveOwnerTouchCount ────────────

test("deriveOwnerTouchCount counts non-failed outbound events normally", () => {
  const sent_event = makeMessageEvent(9020, { delivery_status: "Sent" });
  const delivered_event = makeMessageEvent(9021, { delivery_status: "Delivered" });
  const failed_event = makeMessageEvent(9022, { delivery_status: "Failed" });

  const history = makeQueueHistory([], [sent_event, delivered_event, failed_event]);
  const count = deriveOwnerTouchCount(history);

  // Only the two non-failed events count.
  assert.equal(count, 2);
});

// ── test 9: same-day guard NOT triggered by failed events ─────────────────────

test("same-day touch advancement is not blocked when the only recent event is failed", () => {
  // Simulates the bad-run scenario: a failed (content-filter-blocked) event
  // exists within the 24-hour window for the same phone.
  const failed_event = makeMessageEvent(9030, {
    phone_item_id: PHONE_ID,
    delivery_status: "Failed",
    timestamp_iso: RECENT_ISO,
  });

  const history = makeQueueHistory([], [failed_event]);

  // Same-day cutoff = 24h window
  const same_day_block = findRecentDuplicate(history, PHONE_ID, CUTOFF_TS);

  assert.equal(
    same_day_block,
    null,
    "failed events within 24h must NOT trigger same_day_touch_advancement_blocked"
  );
});

// ── test 10: junk body blocks before finalizeSuccessfulQueueSend ──────────────

test("a junk one-word body is rejected by validateSendQueueItem before any send occurs", () => {
  // This is a contract test: the validation gate in processSendQueueItem reads
  // validateSendQueueItem *before* calling sendTextgridSMS or finalizeSuccessfulQueueSend.
  // We confirm the gate returns the correct shape so the caller can block the send.
  for (const junk of ["Hi", "Hola", "Ciao", "Hey", "Hello"]) {
    const item = makeQueueItem(1010, {
      "queue-status": categoryField("Queued"),
      "phone-number": appRefField(401),
      "textgrid-number": appRefField(501),
      "message-text": textField(junk),
      "retry-count": numberField(0),
      "max-retries": numberField(3),
    });

    const result = validateSendQueueItem(item);

    assert.equal(result.ok, false, `"${junk}" must be rejected`);
    assert.equal(result.reason, "junk_message_body", `"${junk}" reason must be junk_message_body`);
  }
});
