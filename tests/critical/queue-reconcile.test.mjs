import test from "node:test";
import assert from "node:assert/strict";

import {
  classifyQueueEvidence,
  recoverQueueItemFromEvidence,
  runQueueReconcileRunner,
} from "@/lib/workers/queue-reconcile-runner.js";
import {
  categoryField,
  createPodioItem,
  dateField,
  textField,
} from "../helpers/test-helpers.js";

test("reconcile recovers delivered evidence to Sent + Confirmed", async () => {
  const updates = [];
  const evidence = classifyQueueEvidence([
    createPodioItem(501, {
      "status-3": categoryField("Delivered"),
      "message-id": textField("provider-1"),
      timestamp: dateField("2026-04-01T12:00:00.000Z"),
    }),
  ]);

  const result = await recoverQueueItemFromEvidence(
    123,
    evidence,
    "2026-04-01T12:30:00.000Z",
    {
      updateItem: async (item_id, payload) => {
        updates.push({ item_id, payload });
      },
    }
  );

  assert.equal(result.action, "recovered_delivered");
  assert.equal(updates[0].payload["queue-status"], "Sent");
  assert.equal(updates[0].payload["delivery-confirmed"], "✅ Confirmed");
  assert.deepEqual(updates[0].payload["delivered-at"], {
    start: "2026-04-01T12:00:00.000Z",
  });
});

test("reconcile recovers failed evidence to Failed with mapped reason", async () => {
  const updates = [];
  const evidence = classifyQueueEvidence([
    createPodioItem(502, {
      "status-3": categoryField("Failed"),
      "failure-bucket": categoryField("Hard Bounce"),
      "message-id": textField("provider-2"),
    }),
  ]);

  const result = await recoverQueueItemFromEvidence(
    124,
    evidence,
    "2026-04-01T12:30:00.000Z",
    {
      updateItem: async (item_id, payload) => {
        updates.push({ item_id, payload });
      },
    }
  );

  assert.equal(result.action, "recovered_failed");
  assert.equal(updates[0].payload["queue-status"], "Failed");
  assert.equal(updates[0].payload["delivery-confirmed"], "❌ Failed");
  assert.equal(updates[0].payload["failed-reason"], "Invalid Number");
});

test("reconcile recovers accepted evidence to Sent + Pending", async () => {
  const updates = [];
  const evidence = classifyQueueEvidence([
    createPodioItem(503, {
      "status-3": categoryField("Pending"),
      "message-id": textField("provider-3"),
    }),
  ]);

  const result = await recoverQueueItemFromEvidence(
    125,
    evidence,
    "2026-04-01T12:30:00.000Z",
    {
      updateItem: async (item_id, payload) => {
        updates.push({ item_id, payload });
      },
    }
  );

  assert.equal(result.action, "recovered_sent_pending_delivery");
  assert.equal(updates[0].payload["queue-status"], "Sent");
  assert.equal(updates[0].payload["delivery-confirmed"], "⏳ Pending");
});

test("reconcile blocks stale Sending items when no exact evidence exists", async () => {
  const updates = [];
  const evidence = classifyQueueEvidence([]);

  const result = await recoverQueueItemFromEvidence(
    126,
    evidence,
    "2026-04-01T12:30:00.000Z",
    {
      updateItem: async (item_id, payload) => {
        updates.push({ item_id, payload });
      },
    }
  );

  assert.equal(result.action, "blocked_manual_review_provider_verification_incomplete");
  assert.equal(updates[0].payload["queue-status"], "Blocked");
  assert.equal(updates[0].payload["delivery-confirmed"], "⏳ Pending");
});

test("queue reconcile skips safely when Podio cooldown is active", async () => {
  let with_run_lock_called = false;

  const result = await runQueueReconcileRunner(
    {
      limit: 10,
      stale_after_minutes: 20,
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
        scanned_count: 0,
        processed_count: 0,
        recovered_delivered_count: 0,
        recovered_failed_count: 0,
        recovered_sent_count: 0,
        manual_review_count: 0,
        skipped_count: 0,
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

test("queue reconcile skips safely when Podio backpressure is active", async () => {
  let with_run_lock_called = false;

  const result = await runQueueReconcileRunner(
    {
      limit: 10,
      stale_after_minutes: 20,
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
            rate_limit_remaining: 45,
            rate_limit_limit: 1000,
          },
        },
        scanned_count: 0,
        processed_count: 0,
        recovered_delivered_count: 0,
        recovered_failed_count: 0,
        recovered_sent_count: 0,
        manual_review_count: 0,
        skipped_count: 0,
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
