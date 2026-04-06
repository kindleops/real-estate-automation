/**
 * queue-batch-dedup.test.mjs
 *
 * Guards against duplicate sends when multiple Send Queue rows exist for the
 * same (master_owner_id, phone_item_id, touch_number) in a single batch run.
 *
 * The within-batch dedup key is owner:phone:touch — only the first
 * (earliest-scheduled) item is processed; subsequent duplicates are suppressed
 * with a warning.
 *
 * Covered:
 *  1. Two rows for same owner+phone+touch → only first is dispatched.
 *  2. Two rows for same owner+phone but DIFFERENT touch numbers → both sent.
 *  3. Two rows for different owners, same phone+touch → both sent (separate owners).
 *  4. Row without owner/phone ids is NOT filtered (passes through as-is).
 *  5. Warning log is emitted when duplicates are suppressed.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  numberField,
} from "../helpers/test-helpers.js";

// ── helpers ──────────────────────────────────────────────────────────────────

function makeQueueItem(item_id, { owner_id, phone_id, touch_num, scheduled = "2026-01-01T00:00:00.000Z" } = {}) {
  return createPodioItem(item_id, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": { start: scheduled },
    ...(owner_id ? { "master-owner": appRefField(owner_id) } : {}),
    ...(phone_id ? { "phone-number": appRefField(phone_id) } : {}),
    ...(touch_num !== undefined ? { "touch-number": numberField(touch_num) } : {}),
  });
}

function makeDeps(dispatched_log, warn_log_calls = []) {
  return {
    fetchAllItems: async () => [],
    processSendQueueItem: async (id) => {
      dispatched_log.push(id);
      return { ok: true, sent: true };
    },
    recordSystemAlert: async () => {},
    resolveSystemAlert: async () => {},
    withRunLock: async ({ fn }) => fn(),
    info: () => {},
    warn: (...args) => warn_log_calls.push(args),
  };
}

// ── 1. Duplicate owner+phone+touch → only first dispatched ───────────────────

test("runSendQueue: duplicate owner+phone+touch in batch — only first item is sent", async () => {
  const dispatched = [];
  const warns = [];

  const item_a = makeQueueItem(1001, { owner_id: 201, phone_id: 401, touch_num: 1, scheduled: "2026-01-01T06:00:00.000Z" });
  const item_b = makeQueueItem(1002, { owner_id: 201, phone_id: 401, touch_num: 1, scheduled: "2026-01-01T07:00:00.000Z" });

  const deps = {
    ...makeDeps(dispatched, warns),
    fetchAllItems: async () => [item_a, item_b],
  };

  await runSendQueue({ limit: 50, now: "2026-01-01T12:00:00.000Z" }, deps);

  assert.equal(dispatched.length, 1, "Only one item should be dispatched");
  assert.equal(dispatched[0], 1001, "Earlier-scheduled item 1001 should be dispatched");

  const dedup_warn = warns.find(([code]) => code === "queue.run_batch_duplicates_suppressed");
  assert.ok(dedup_warn, "A dedup warning must be emitted");
  assert.equal(dedup_warn[1].duplicate_count, 1);
});

// ── 2. Same owner+phone but different touch numbers → both sent ───────────────

test("runSendQueue: same owner+phone but different touch numbers — both dispatched", async () => {
  const dispatched = [];

  const item_a = makeQueueItem(2001, { owner_id: 201, phone_id: 401, touch_num: 1 });
  const item_b = makeQueueItem(2002, { owner_id: 201, phone_id: 401, touch_num: 2 });

  const deps = {
    ...makeDeps(dispatched),
    fetchAllItems: async () => [item_a, item_b],
  };

  await runSendQueue({ limit: 50, now: "2026-01-01T12:00:00.000Z" }, deps);

  assert.equal(dispatched.length, 2, "Different touch numbers must not suppress each other");
  assert.ok(dispatched.includes(2001));
  assert.ok(dispatched.includes(2002));
});

// ── 3. Different owners, same phone+touch → both sent ────────────────────────

test("runSendQueue: different owners sharing a phone+touch — both dispatched", async () => {
  const dispatched = [];

  const item_a = makeQueueItem(3001, { owner_id: 201, phone_id: 401, touch_num: 1 });
  const item_b = makeQueueItem(3002, { owner_id: 202, phone_id: 401, touch_num: 1 });

  const deps = {
    ...makeDeps(dispatched),
    fetchAllItems: async () => [item_a, item_b],
  };

  await runSendQueue({ limit: 50, now: "2026-01-01T12:00:00.000Z" }, deps);

  assert.equal(dispatched.length, 2, "Different owners must not suppress each other");
});

// ── 4. Item without owner/phone ids passes through ───────────────────────────

test("runSendQueue: item missing owner/phone ids is not filtered by dedup", async () => {
  const dispatched = [];

  const no_ids = makeQueueItem(4001, { scheduled: "2026-01-01T00:00:00.000Z" });

  const deps = {
    ...makeDeps(dispatched),
    fetchAllItems: async () => [no_ids],
  };

  await runSendQueue({ limit: 50, now: "2026-01-01T12:00:00.000Z" }, deps);

  assert.equal(dispatched.length, 1);
  assert.equal(dispatched[0], 4001);
});

// ── 5. Three duplicates → two suppressed, one sent ───────────────────────────

test("runSendQueue: three identical touch duplicates — first sent, two suppressed", async () => {
  const dispatched = [];
  const warns = [];

  const items = [
    makeQueueItem(5001, { owner_id: 201, phone_id: 401, touch_num: 1, scheduled: "2026-01-01T06:00:00.000Z" }),
    makeQueueItem(5002, { owner_id: 201, phone_id: 401, touch_num: 1, scheduled: "2026-01-01T07:00:00.000Z" }),
    makeQueueItem(5003, { owner_id: 201, phone_id: 401, touch_num: 1, scheduled: "2026-01-01T08:00:00.000Z" }),
  ];

  const deps = {
    ...makeDeps(dispatched, warns),
    fetchAllItems: async () => items,
  };

  await runSendQueue({ limit: 50, now: "2026-01-01T12:00:00.000Z" }, deps);

  assert.equal(dispatched.length, 1);
  assert.equal(dispatched[0], 5001);

  const dedup_warn = warns.find(([code]) => code === "queue.run_batch_duplicates_suppressed");
  assert.ok(dedup_warn);
  assert.equal(dedup_warn[1].duplicate_count, 2);
});
