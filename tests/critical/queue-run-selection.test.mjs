import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  dateField,
} from "../helpers/test-helpers.js";

const NOW = "2026-04-04T15:00:00.000Z";

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeQueue(items) {
  return async () => items;
}

function makeStubs({ processResult = { ok: true, sent: true, provider_message_id: "msg-ok" } } = {}) {
  const info_calls = [];
  const warn_calls = [];
  const processed_ids = [];

  return {
    info_calls,
    warn_calls,
    processed_ids,
    deps: {
      fetchAllItems: makeQueue([]),
      processSendQueueItem: async (id) => {
        processed_ids.push(id);
        return processResult;
      },
      recordSystemAlert: async () => {},
      resolveSystemAlert: async () => {},
      withRunLock: async ({ fn }) => fn(),
      info: (event, meta) => { info_calls.push({ event, meta }); },
      warn: (event, meta) => { warn_calls.push({ event, meta }); },
    },
  };
}

function candidatesLog(info_calls) {
  return info_calls.find((c) => c.event === "queue.run_candidates_loaded")?.meta ?? null;
}

function completedLog(info_calls) {
  return info_calls.find((c) => c.event === "queue.run_completed")?.meta ?? null;
}

// ─── test 1: due row is selected ──────────────────────────────────────────────

test("runSendQueue selects a Queued row whose scheduled_for_utc is in the past", async () => {
  const { info_calls, processed_ids, deps } = makeStubs();

  const queued_item = createPodioItem(2001, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T12:00:00.000Z"), // 3 hours before NOW
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([queued_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.processed_count, 1, "one row should enter the send branch");
  assert.deepEqual(processed_ids, [2001], "processSendQueueItem called with the correct item id");

  const candidates = candidatesLog(info_calls);
  assert.ok(candidates, "queue.run_candidates_loaded was emitted");
  assert.equal(candidates.total_rows_loaded, 1);
  assert.equal(candidates.queued_rows_loaded, 1);
  assert.equal(candidates.due_rows, 1, "one row passes the due check");
  assert.equal(candidates.future_rows, 0, "no future rows");
  assert.equal(candidates.runnable_count, 1);
  assert.equal(candidates.now_utc, NOW);
  assert.deepEqual(candidates.first_10_candidate_item_ids, [2001]);
  assert.deepEqual(candidates.first_10_filter_excluded, []);

  const completed = completedLog(info_calls);
  assert.ok(completed, "queue.run_completed was emitted");
  assert.equal(completed.total_rows_loaded, 1);
  assert.equal(completed.due_rows, 1);
  assert.equal(completed.future_rows, 0);
  assert.equal(completed.sent_rows, 1);
  assert.equal(completed.sent_count, 1);
  assert.equal(completed.blocked_rows, 0);
  assert.equal(completed.now_utc, NOW);
});

// ─── test 2: future row is excluded ───────────────────────────────────────────

test("runSendQueue excludes a Queued row whose scheduled_for_utc is in the future", async () => {
  const { info_calls, processed_ids, deps } = makeStubs();

  const future_item = createPodioItem(2002, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T20:00:00.000Z"), // 5 hours after NOW
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([future_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.processed_count, 0, "no rows should enter the send branch");
  assert.deepEqual(processed_ids, [], "processSendQueueItem must not be called");

  const candidates = candidatesLog(info_calls);
  assert.ok(candidates, "queue.run_candidates_loaded was emitted");
  assert.equal(candidates.total_rows_loaded, 1);
  assert.equal(candidates.due_rows, 0);
  assert.equal(candidates.future_rows, 1, "future row counted");
  assert.equal(candidates.runnable_count, 0);
  assert.deepEqual(candidates.first_10_candidate_item_ids, []);

  // The filter diagnostic should record the excluded item
  assert.equal(candidates.first_10_filter_excluded.length, 1);
  assert.equal(candidates.first_10_filter_excluded[0].item_id, 2002);
  assert.equal(candidates.first_10_filter_excluded[0].reason, "not_due_yet");

  const completed = completedLog(info_calls);
  assert.equal(completed.future_rows, 1);
  assert.equal(completed.sent_rows, 0);
  assert.equal(completed.due_rows, 0);
});

// ─── test 3: due row with no scheduled field is selected ──────────────────────

test("runSendQueue selects a Queued row with no scheduled_for_utc (treated as immediately due)", async () => {
  const { processed_ids, deps } = makeStubs();

  const unscheduled_item = createPodioItem(2003, {
    "queue-status": categoryField("Queued"),
    "master-owner": appRefField(5001),
    // no scheduled-for-utc field
  });

  deps.fetchAllItems = makeQueue([unscheduled_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.processed_count, 1, "row without schedule should be treated as due");
  assert.deepEqual(processed_ids, [2003]);
});

// ─── test 4: due row reaches send branch (ok result) ──────────────────────────

test("runSendQueue passes a due Queued row through to the send branch and records sent_count", async () => {
  const { info_calls, deps } = makeStubs({
    processResult: { ok: true, sent: true, provider_message_id: "msg-abc" },
  });

  const due_item = createPodioItem(2004, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T10:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([due_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.sent_count, 1);
  assert.equal(result.failed_count, 0);
  assert.equal(result.skipped_count, 0);
  assert.equal(result.ok, true);
  assert.equal(result.results[0].ok, true);
  assert.equal(result.results[0].sent, true);
  assert.equal(result.results[0].provider_message_id, "msg-abc");

  const completed = completedLog(info_calls);
  assert.equal(completed.sent_rows, 1);
  assert.equal(completed.blocked_rows, 0);
  assert.deepEqual(completed.first_10_skipped_item_ids_with_reason, []);
});

// ─── test 5: failed dispatch is logged with reason ────────────────────────────

test("runSendQueue logs queue.run_item_not_dispatched when processSendQueueItem returns ok=false", async () => {
  const { info_calls, warn_calls, deps } = makeStubs({
    processResult: { ok: false, reason: "missing_textgrid_number" },
  });

  const due_item = createPodioItem(2005, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T12:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([due_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.sent_count, 0);
  assert.equal(result.failed_count, 1);

  const not_dispatched = warn_calls.find((c) => c.event === "queue.run_item_not_dispatched");
  assert.ok(not_dispatched, "queue.run_item_not_dispatched warn was emitted");
  assert.equal(not_dispatched.meta.queue_item_id, 2005);
  assert.equal(not_dispatched.meta.reason, "missing_textgrid_number");

  const completed = completedLog(info_calls);
  assert.equal(completed.blocked_rows, 1);
  assert.equal(completed.first_10_skipped_item_ids_with_reason.length, 1);
  assert.equal(completed.first_10_skipped_item_ids_with_reason[0].queue_item_id, 2005);
  assert.equal(completed.first_10_skipped_item_ids_with_reason[0].reason, "missing_textgrid_number");
});

// ─── test 6: mixed batch — due and future rows ────────────────────────────────

test("runSendQueue processes the due row and excludes the future row from a mixed batch", async () => {
  const { info_calls, processed_ids, deps } = makeStubs();

  const due_item = createPodioItem(2010, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T08:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  const future_item = createPodioItem(2011, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T22:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([due_item, future_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.processed_count, 1);
  assert.deepEqual(processed_ids, [2010]);
  assert.equal(result.sent_count, 1);

  const candidates = candidatesLog(info_calls);
  assert.equal(candidates.total_rows_loaded, 2);
  assert.equal(candidates.due_rows, 1);
  assert.equal(candidates.future_rows, 1);
  assert.equal(candidates.runnable_count, 1);
  assert.deepEqual(candidates.first_10_candidate_item_ids, [2010]);
});
