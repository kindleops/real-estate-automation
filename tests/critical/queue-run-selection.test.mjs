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

test("runSendQueue logs queue.run_item_failed_soft when processSendQueueItem returns ok=false", async () => {
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

  assert.equal(result.attempted_count, 1);
  assert.equal(result.claimed_count, 0);
  assert.equal(result.sent_count, 0);
  assert.equal(result.failed_count, 1);

  const failed_soft = warn_calls.find((c) => c.event === "queue.run_item_failed_soft");
  assert.ok(failed_soft, "queue.run_item_failed_soft warn was emitted");
  assert.equal(failed_soft.meta.queue_item_id, 2005);
  assert.equal(failed_soft.meta.reason, "missing_textgrid_number");

  const completed = completedLog(info_calls);
  assert.equal(completed.blocked_rows, 0);
  assert.equal(completed.failed_count, 1);
  assert.equal(completed.first_10_skipped_item_ids_with_reason.length, 1);
  assert.equal(completed.first_10_skipped_item_ids_with_reason[0].queue_item_id, 2005);
  assert.equal(completed.first_10_skipped_item_ids_with_reason[0].reason, "missing_textgrid_number");
});

test("runSendQueue fails soft on an unexpected queue item crash, marks the item failed, and continues", async () => {
  const { info_calls, warn_calls, deps } = makeStubs();
  const failed_updates = [];

  const crash_item = createPodioItem(2012, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T09:00:00.000Z"),
    "master-owner": appRefField(5001),
  });
  const ok_item = createPodioItem(2013, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T10:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([crash_item, ok_item]);
  deps.processSendQueueItem = async (queue_item_id) => {
    if (queue_item_id === 2012) {
      throw new Error("boom");
    }
    return { ok: true, sent: true };
  };
  deps.failQueueItem = async (queue_item_id, payload) => {
    failed_updates.push({ queue_item_id, payload });
  };

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.ok, true);
  assert.equal(result.partial, true);
  assert.equal(result.attempted_count, 2);
  assert.equal(result.claimed_count, 1);
  assert.equal(result.started_count, 2);
  assert.equal(result.processed_count, 2);
  assert.equal(result.sent_count, 1);
  assert.equal(result.failed_count, 1);
  assert.equal(result.blocked_count, 0);
  assert.equal(result.duplicate_locked_count, 0);
  assert.equal(result.first_failing_queue_item_id, 2012);
  assert.equal(result.first_failing_reason, "queue_processing_exception");
  assert.equal(result.first_failure_queue_item_id, 2012);
  assert.equal(result.first_failure_reason, "queue_processing_exception");
  assert.equal(failed_updates.length, 1);
  assert.equal(failed_updates[0].queue_item_id, 2012);
  assert.equal(failed_updates[0].payload.queue_status, "Failed");
  assert.equal(failed_updates[0].payload.failed_reason, "Network Error");

  const completed = completedLog(info_calls);
  assert.equal(completed.started_count, 2);
  assert.equal(completed.processed_count, 2);
  assert.equal(completed.claimed_count, 1);
  assert.equal(completed.failed_count, 1);
  assert.equal(completed.first_failing_queue_item_id, 2012);
  assert.equal(completed.first_failing_reason, "queue_processing_exception");

  const crash_warn = warn_calls.find((c) => c.event === "queue.run_item_crashed");
  assert.ok(crash_warn, "queue.run_item_crashed warn was emitted");
  assert.equal(crash_warn.meta.queue_item_id, 2012);
  assert.equal(crash_warn.meta.reason, "queue_processing_exception");
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

test("runSendQueue counts claim conflicts as duplicate_locked skips without failing the batch", async () => {
  const { info_calls, deps } = makeStubs();

  const due_item = createPodioItem(2020, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T08:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([due_item]);
  deps.processSendQueueItem = async () => ({
    ok: true,
    skipped: true,
    reason: "queue_item_claim_conflict",
    claim_conflict: true,
    claimed: false,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.ok, true);
  assert.equal(result.skipped_count, 1);
  assert.equal(result.duplicate_locked_count, 1);
  assert.equal(result.claimed_count, 0);

  const completed = completedLog(info_calls);
  assert.equal(completed.duplicate_locked_count, 1);
});

// ─── test 7: diagnostic fields appear in returned summary (not just logs) ───────

test("runSendQueue returns total_rows_loaded, due_rows, future_rows, first_10_excluded in summary object", async () => {
  const { deps } = makeStubs();

  const due_item = createPodioItem(3001, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T10:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  const future_item = createPodioItem(3002, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-05T10:00:00.000Z"), // future
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([due_item, future_item]);

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);

  assert.equal(result.total_rows_loaded, 2, "total_rows_loaded in returned summary");
  assert.equal(result.queued_rows_loaded, 2, "queued_rows_loaded in returned summary");
  assert.equal(result.due_rows, 1, "due_rows in returned summary");
  assert.equal(result.future_rows, 1, "future_rows in returned summary");
  assert.equal(result.outside_window_rows, 0, "outside_window_rows in returned summary");
  assert.deepEqual(result.first_10_candidate_item_ids, [3001], "first_10_candidate_item_ids in returned summary");
  assert.equal(result.first_10_excluded.length, 1, "first_10_excluded in returned summary");
  assert.equal(result.first_10_excluded[0].item_id, 3002);
  assert.equal(result.first_10_excluded[0].reason, "not_due_yet");
});

// ─── test 8: dry_run summary also includes diagnostic fields ─────────────────

test("runSendQueue dry_run=true returns diagnostic fields in summary without processing rows", async () => {
  const { deps } = makeStubs();

  const due_item = createPodioItem(3010, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-04T08:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  const future_item = createPodioItem(3011, {
    "queue-status": categoryField("Queued"),
    "scheduled-for-utc": dateField("2026-04-06T08:00:00.000Z"),
    "master-owner": appRefField(5001),
  });

  deps.fetchAllItems = makeQueue([due_item, future_item]);

  const result = await runSendQueue({ limit: 10, now: NOW, dry_run: true }, deps);

  assert.equal(result.dry_run, true, "dry_run flag set in summary");
  assert.equal(result.total_rows_loaded, 2, "total_rows_loaded in dry_run summary");
  assert.equal(result.due_rows, 1, "due_rows in dry_run summary");
  assert.equal(result.future_rows, 1, "future_rows in dry_run summary");
  assert.equal(result.outside_window_rows, 0, "outside_window_rows in dry_run summary");
  assert.deepEqual(result.first_10_candidate_item_ids, [3010], "candidate ids in dry_run summary");
  assert.equal(result.first_10_excluded.length, 1, "first_10_excluded in dry_run summary");
  assert.equal(result.first_10_excluded[0].item_id, 3011);
  assert.equal(result.first_10_excluded[0].reason, "not_due_yet");
  assert.equal(result.sent_count, 0, "no rows sent in dry_run");
  assert.equal(result.attempted_count, 1, "attempted_count reflects runnable rows in dry_run");
  assert.equal(result.claimed_count, 0, "dry_run never claims rows");
  assert.equal(result.processed_count, 1, "processed_count reflects runnable rows even in dry_run");
});
