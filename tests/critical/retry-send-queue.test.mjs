import test from "node:test";
import assert from "node:assert/strict";

import {
  buildRetryDecision,
  getRetryBackoffMinutes,
} from "@/lib/domain/queue/retry-send-queue.js";
import { runRetryRunner } from "@/lib/workers/retry-runner.js";
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

test("retry runner skips safely when Podio cooldown is active", async () => {
  let with_run_lock_called = false;

  const result = await runRetryRunner(
    {
      limit: 10,
      master_owner_id: 201,
    },
    {
      buildPodioCooldownSkipResult: async () => ({
        ok: true,
        skipped: true,
        reason: "podio_rate_limit_cooldown_active",
        retry_after_seconds: 3600,
        retry_after_at: "2026-04-08T20:20:25.000Z",
        podio_cooldown: {
          active: true,
          status: 420,
          path: "/item/app/30541680/filter/",
          operation: "filter_items",
          rate_limit_remaining: 0,
        },
        processed_count: 0,
        retried_count: 0,
        scheduled_count: 0,
        terminal_count: 0,
        skipped_count: 0,
        scanned_count: 0,
        results: [],
      }),
      withRunLock: async () => {
        with_run_lock_called = true;
        throw new Error("withRunLock should not run during Podio cooldown");
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "podio_rate_limit_cooldown_active");
  assert.equal(result.retry_after_seconds, 3600);
  assert.equal(with_run_lock_called, false);
});

test("retry runner skips safely when Podio backpressure is active", async () => {
  let with_run_lock_called = false;

  const result = await runRetryRunner(
    {
      limit: 10,
      master_owner_id: 201,
    },
    {
      buildPodioBackpressureSkipResult: async () => ({
        ok: true,
        skipped: true,
        reason: "podio_rate_limit_low_remaining",
        podio_backpressure: {
          active: true,
          min_remaining: 100,
          observation: {
            path: "/item/app/30541680/filter/",
            operation: "filter_items",
            rate_limit_remaining: 37,
            rate_limit_limit: 1000,
          },
        },
        processed_count: 0,
        retried_count: 0,
        scheduled_count: 0,
        terminal_count: 0,
        skipped_count: 0,
        scanned_count: 0,
        results: [],
      }),
      withRunLock: async () => {
        with_run_lock_called = true;
        throw new Error("withRunLock should not run during Podio backpressure");
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "podio_rate_limit_low_remaining");
  assert.equal(with_run_lock_called, false);
});
