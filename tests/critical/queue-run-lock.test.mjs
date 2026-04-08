import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { forceReleaseStaleLock } from "@/lib/domain/runs/run-locks.js";
import { appRefField, categoryField, createPodioItem } from "../helpers/test-helpers.js";

const NOW = "2026-04-04T15:00:00.000Z";

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeLogger() {
  const entries = [];
  return {
    entries,
    info: (event, meta) => entries.push({ level: "info", event, meta }),
    warn: (event, meta) => entries.push({ level: "warn", event, meta }),
  };
}

function find(entries, event) {
  return entries.find((e) => e.event === event);
}

function makeDeps({ logger, overrides = {} } = {}) {
  return {
    fetchAllItems: async () => [],
    processSendQueueItem: async () => ({ ok: true, sent: true }),
    recordSystemAlert: async () => {},
    resolveSystemAlert: async () => {},
    info: logger.info,
    warn: logger.warn,
    ...overrides,
  };
}

// ─── test 1: lock active → skipped, warn emitted with all fields ──────────────

test("runSendQueue emits queue.run_skipped_lock_active with full lock metadata when lock is already held", async () => {
  const { entries, info, warn } = makeLogger();

  const active_lock = {
    ok: true,
    acquired: false,
    reason: "run_lock_active",
    scope: "queue-run",
    record_item_id: 9900,
    meta: {
      status: "locked",
      lease_token: "tok-abc-123",
      expires_at: "2026-04-04T15:10:00.000Z",
      owner: "queue_runner",
      acquired_at: "2026-04-04T15:00:00.000Z",
      acquisition_count: 3,
    },
  };

  const result = await runSendQueue(
    { limit: 10, now: NOW },
    {
      ...makeDeps({ logger: { entries, info, warn } }),
      withRunLock: async ({ onLocked }) => onLocked(active_lock),
    }
  );

  // Must return skipped=true with the correct reason
  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "queue_runner_lock_active");

  // Warn must have been emitted
  const lock_warn = find(entries, "queue.run_skipped_lock_active");
  assert.ok(lock_warn, "queue.run_skipped_lock_active should be emitted");
  assert.equal(lock_warn.level, "warn");

  const m = lock_warn.meta;
  assert.equal(m.reason, "queue_runner_lock_active");
  assert.equal(m.lock_scope, "queue-run");
  assert.equal(m.lock_record_item_id, 9900);
  assert.equal(m.lock_lease_token, "tok-abc-123");
  assert.equal(m.lock_expires_at, "2026-04-04T15:10:00.000Z");
  assert.equal(m.lock_owner, "queue_runner");
  assert.equal(m.lock_acquired_at, "2026-04-04T15:00:00.000Z");
  assert.equal(m.lock_acquisition_count, 3);
  assert.ok(m.recovery_hint, "recovery_hint should be present");

  // Must NOT have emitted queue.run_started (executeRun was not entered)
  assert.equal(find(entries, "queue.run_started"), undefined, "queue.run_started must NOT be emitted when lock is active");
  assert.equal(find(entries, "queue.run_candidates_loaded"), undefined, "queue.run_candidates_loaded must NOT be emitted");
  assert.equal(find(entries, "queue.run_fetch_started"), undefined, "queue.run_fetch_started must NOT be emitted");
});

// ─── test 2: lock cleared → executeRun entered, logs emitted ─────────────────

test("runSendQueue enters executeRun and emits queue.run_started when lock is cleared", async () => {
  const { entries, info, warn } = makeLogger();
  const processed_ids = [];

  const due_item = createPodioItem(3001, {
    "queue-status": categoryField("Queued"),
    "master-owner": appRefField(5001),
  });

  const result = await runSendQueue(
    { limit: 10, now: NOW },
    {
      fetchAllItems: async () => [due_item],
      processSendQueueItem: async (id) => {
        processed_ids.push(id);
        return { ok: true, sent: true, provider_message_id: "msg-lock-cleared" };
      },
      recordSystemAlert: async () => {},
      resolveSystemAlert: async () => {},
      // Simulate lock successfully acquired → fn() called directly
      withRunLock: async ({ fn }) => fn(),
      info,
      warn,
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.skipped, undefined, "run must not be skipped");
  assert.equal(result.sent_count, 1);
  assert.deepEqual(processed_ids, [3001]);

  // executeRun was entered
  assert.ok(find(entries, "queue.run_started"), "queue.run_started must be emitted");
  assert.ok(find(entries, "queue.run_fetch_started"), "queue.run_fetch_started must be emitted");
  assert.ok(find(entries, "queue.run_candidates_loaded"), "queue.run_candidates_loaded must be emitted");
  assert.ok(find(entries, "queue.run_completed"), "queue.run_completed must be emitted");

  // No lock-active warn
  assert.equal(find(entries, "queue.run_skipped_lock_active"), undefined, "lock_active warn must NOT appear");
});

// ─── test 3: dry_run bypasses lock, still enters executeRun ──────────────────

test("runSendQueue bypasses withRunLock entirely and enters executeRun when dry_run=true", async () => {
  const { entries, info, warn } = makeLogger();
  const lock_calls = [];

  const result = await runSendQueue(
    { limit: 10, now: NOW, dry_run: true },
    {
      fetchAllItems: async () => [
        createPodioItem(3002, {
          "queue-status": categoryField("Queued"),
          "master-owner": appRefField(5001),
        }),
      ],
      processSendQueueItem: async () => ({ ok: true, sent: true }),
      recordSystemAlert: async () => {},
      resolveSystemAlert: async () => {},
      withRunLock: async ({ enabled, fn, onLocked }) => {
        lock_calls.push({ enabled });
        // withRunLock respects enabled=false by calling fn directly
        if (!enabled) return fn();
        return onLocked({ scope: "queue-run", meta: {} });
      },
      info,
      warn,
    }
  );

  // dry_run=true sets enabled=false, so fn() is called and run proceeds
  assert.equal(lock_calls[0]?.enabled, false, "withRunLock must be called with enabled=false for dry_run");
  assert.equal(result.dry_run, true);
  assert.equal(result.ok, true);
  assert.equal(find(entries, "queue.run_skipped_lock_active"), undefined, "lock warn must NOT appear in dry_run");
});

test("runSendQueue skips safely when Podio cooldown is active", async () => {
  const { entries, info, warn } = makeLogger();
  let with_run_lock_called = false;

  const result = await runSendQueue(
    { limit: 10, now: NOW },
    {
      ...makeDeps({ logger: { entries, info, warn } }),
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
        sent_count: 0,
        failed_count: 0,
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

  const cooldown_warn = find(entries, "queue.run_skipped_podio_cooldown");
  assert.ok(cooldown_warn, "queue.run_skipped_podio_cooldown should be emitted");
});

// ─── test 4: forceReleaseStaleLock clears a stuck lock record ────────────────

test("forceReleaseStaleLock releases a stuck lock record", async () => {
  let stored_record = {
    item_id: 9901,
    fields: [
      {
        external_id: "message-id",
        values: [{ value: "run-lock:queue-run" }],
      },
      {
        external_id: "ai-output",
        values: [
          {
            value: JSON.stringify({
              status: "locked",
              scope: "queue-run",
              lease_token: "tok-stale",
              expires_at: "2099-01-01T00:00:00.000Z",
              owner: "queue_runner",
              acquired_at: "2026-04-04T14:55:00.000Z",
              acquisition_count: 1,
            }),
          },
        ],
      },
    ],
  };

  let updated_payload = null;

  const result = await forceReleaseStaleLock(
    { scope: "queue-run", reason: "test_manual_recovery" },
    {
      findRunLockRecord: async () => stored_record,
      updateMessageEvent: async (item_id, fields) => {
        updated_payload = { item_id, fields };
        return { ok: true, item_id };
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.released, true);
  assert.equal(result.reason, "test_manual_recovery");
  assert.equal(result.scope, "queue-run");
  assert.equal(result.record_item_id, 9901);
  assert.equal(result.was_active, true, "was_active should reflect lock was live");
  assert.equal(result.previous_owner, "queue_runner");
  assert.equal(result.previous_expires_at, "2099-01-01T00:00:00.000Z");

  // The update must write status=released
  assert.ok(updated_payload, "updateMessageEvent must be called");
  const written_meta = JSON.parse(updated_payload.fields["ai-output"]);
  assert.equal(written_meta.status, "released");
  assert.equal(written_meta.outcome, "test_manual_recovery");
});

// ─── test 5: forceReleaseStaleLock handles no record gracefully ───────────────

test("forceReleaseStaleLock returns ok=true with released=false when no lock record exists", async () => {
  const result = await forceReleaseStaleLock(
    { scope: "queue-run:999" },
    {
      findRunLockRecord: async () => null,
      updateMessageEvent: async () => { throw new Error("should not be called"); },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.released, false);
  assert.equal(result.reason, "no_lock_record_found");
  assert.equal(result.scope, "queue-run:999");
});
