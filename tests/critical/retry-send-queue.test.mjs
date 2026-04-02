import test from "node:test";
import assert from "node:assert/strict";

import {
  buildRetryDecision,
  getRetryBackoffMinutes,
} from "@/lib/domain/queue/retry-send-queue.js";
import {
  categoryField,
  createPodioItem,
  dateField,
  numberField,
} from "../helpers/test-helpers.js";

test("retry decision schedules transient network failures with backoff", () => {
  const item = createPodioItem(123, {
    "queue-status": categoryField("Failed"),
    "failed-reason": categoryField("Network Error"),
    "retry-count": numberField(1),
    "max-retries": numberField(3),
  });

  const decision = buildRetryDecision(item, {
    now: "2026-04-01T12:00:00.000Z",
  });

  assert.equal(getRetryBackoffMinutes({
    failed_reason: "Network Error",
    retry_count: 1,
  }), 60);
  assert.equal(decision.action, "schedule_retry");
  assert.equal(decision.reason, "retry_scheduled");
  assert.equal(decision.next_retry_at, "2026-04-01T13:00:00.000Z");
});

test("retry decision requeues once scheduled backoff is due", () => {
  const item = createPodioItem(124, {
    "queue-status": categoryField("Failed"),
    "failed-reason": categoryField("Network Error"),
    "retry-count": numberField(1),
    "max-retries": numberField(3),
    "scheduled-for-utc": dateField("2026-04-01T11:00:00.000Z"),
  });

  const decision = buildRetryDecision(item, {
    now: "2026-04-01T12:00:00.000Z",
  });

  assert.equal(decision.action, "requeue_now");
  assert.equal(decision.update["queue-status"], "Queued");
  assert.equal(decision.update["delivery-confirmed"], "⏳ Pending");
});

test("retry decision blocks terminal non-retryable failures", () => {
  const item = createPodioItem(125, {
    "queue-status": categoryField("Failed"),
    "failed-reason": categoryField("Carrier Block"),
    "retry-count": numberField(1),
    "max-retries": numberField(3),
  });

  const decision = buildRetryDecision(item, {
    now: "2026-04-01T12:00:00.000Z",
  });

  assert.equal(decision.action, "terminal_non_retryable");
  assert.equal(decision.update["queue-status"], "Blocked");
});
