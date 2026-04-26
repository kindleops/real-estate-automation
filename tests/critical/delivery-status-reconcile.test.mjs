/**
 * delivery-status-reconcile.test.mjs
 *
 * Regression tests for Supabase delivery_status normalization.
 *
 * Scenarios covered:
 * 1. applyDeliveredNormalization: returns correction for stale pending row
 *    (delivered_at set, failed_at null, delivery_status pending)
 * 2. applyDeliveredNormalization: returns null when already delivered
 * 3. applyDeliveredNormalization: returns null when failed_at is populated
 * 4. applyDeliveredNormalization: returns null when delivered_at is missing
 * 5. reconcileSupabaseDeliveryStatuses (Path 1):
 *    message_event with delivery_status=pending and delivered_at set
 *    → reconcile normalizes it to delivered
 * 6. reconcileSupabaseDeliveryStatuses (Path 2):
 *    send_queue.delivery_confirmed=confirmed with matching message_event pending
 *    → reconcile updates message_event to delivered
 * 7. reconcileSupabaseDeliveryStatuses: does NOT touch rows with failed_at set
 * 8. reconcileSupabaseDeliveryStatuses: dry_run=true counts without writing
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  applyDeliveredNormalization,
  reconcileSupabaseDeliveryStatuses,
} from "@/lib/domain/events/normalize-delivery-status.js";

// ---------------------------------------------------------------------------
// Unit tests — applyDeliveredNormalization
// ---------------------------------------------------------------------------

test("applyDeliveredNormalization: returns correction for stale pending row", () => {
  const row = {
    id: 1001,
    provider_message_sid: "SM_abc123",
    delivered_at: "2026-04-19T12:05:00.000Z",
    failed_at: null,
    delivery_status: "pending",
  };

  const correction = applyDeliveredNormalization(row);
  assert.ok(correction, "correction must not be null");
  assert.equal(correction.delivery_status, "delivered");
  assert.equal(correction.provider_delivery_status, "delivered");
  assert.equal(correction.is_final_failure, false);
  assert.equal(correction.failure_bucket, null);
  assert.equal(correction.failure_reason, null);
  assert.equal(correction.error_message, null);
});

test("applyDeliveredNormalization: returns correction for stale sent row", () => {
  const row = {
    id: 1002,
    delivered_at: "2026-04-19T12:05:00.000Z",
    failed_at: null,
    delivery_status: "sent",
  };

  const correction = applyDeliveredNormalization(row);
  assert.ok(correction, "correction must not be null for sent status");
  assert.equal(correction.delivery_status, "delivered");
});

test("applyDeliveredNormalization: returns null when already delivered", () => {
  const row = {
    id: 1003,
    delivered_at: "2026-04-19T12:05:00.000Z",
    failed_at: null,
    delivery_status: "delivered",
  };

  const correction = applyDeliveredNormalization(row);
  assert.equal(correction, null, "no correction needed when already delivered");
});

test("applyDeliveredNormalization: returns null when failed_at is populated", () => {
  const row = {
    id: 1004,
    delivered_at: null,
    failed_at: "2026-04-19T12:06:00.000Z",
    delivery_status: "failed",
  };

  const correction = applyDeliveredNormalization(row);
  assert.equal(correction, null, "must not correct rows with failed_at set");
});

test("applyDeliveredNormalization: returns null when delivered_at is null", () => {
  const row = {
    id: 1005,
    delivered_at: null,
    failed_at: null,
    delivery_status: "pending",
  };

  const correction = applyDeliveredNormalization(row);
  assert.equal(correction, null, "no correction when delivered_at is not populated");
});

// ---------------------------------------------------------------------------
// Integration-style tests — reconcileSupabaseDeliveryStatuses
// ---------------------------------------------------------------------------

/**
 * Build a fake Supabase client that returns specified rows for specified tables
 * and records any update calls.
 *
 * The chain is a thenable — every filter method returns `chain` itself so that
 * `await supabase.from(T).select(...).not(...).limit(N)` resolves correctly,
 * and `await supabase.from(T).update(payload).eq(...).is(...)` also resolves.
 */
function makeFakeSupabase({ message_events_rows = [], queue_rows = [] } = {}) {
  const updates = [];

  function makeChain(table, row_data) {
    let update_payload = null;
    let is_update = false;

    const chain = {
      select: () => chain,
      not: () => chain,
      is: () => chain,
      neq: () => chain,
      eq: () => chain,
      or: () => chain,
      limit: () => chain,
      update(payload) {
        is_update = true;
        update_payload = payload;
        return chain;
      },
      // Make chain a PromiseLike so `await chain` and `await chain.method()` both work
      then(onFulfilled, onRejected) {
        if (is_update && update_payload !== null) {
          updates.push({ table, payload: update_payload });
        }
        const value = is_update
          ? { data: null, error: null }
          : { data: row_data, error: null };
        return Promise.resolve(value).then(onFulfilled, onRejected);
      },
      catch(onRejected) {
        return Promise.resolve({ data: is_update ? null : row_data, error: null }).catch(onRejected);
      },
    };

    return chain;
  }

  const client = {
    from(table) {
      if (table === "message_events") return makeChain(table, message_events_rows);
      if (table === "send_queue") return makeChain(table, queue_rows);
      return makeChain(table, []);
    },
  };

  return { client, updates };
}

test("reconcileSupabaseDeliveryStatuses (Path 1): message_event with delivered_at and pending delivery_status is normalized to delivered", async () => {
  const stale_event = {
    id: 2001,
    provider_message_sid: "SM_stale_1",
    delivered_at: "2026-04-19T12:05:00.000Z",
    failed_at: null,
    delivery_status: "pending",
  };

  const { client, updates } = makeFakeSupabase({
    message_events_rows: [stale_event],
    queue_rows: [],
  });

  const result = await reconcileSupabaseDeliveryStatuses(
    { limit: 50, now: "2026-04-19T12:30:00.000Z" },
    { supabase: client }
  );

  assert.equal(result.ok, true, "reconcile must succeed");
  assert.equal(result.normalized_from_delivered_at, 1, "must count one normalized event from path 1");
  assert.equal(result.total_normalized >= 1, true, "total must be at least 1");

  const me_update = updates.find((u) => u.table === "message_events");
  assert.ok(me_update, "must have written an update to message_events");
  assert.equal(me_update.payload.delivery_status, "delivered");
  assert.equal(me_update.payload.provider_delivery_status, "delivered");
  assert.equal(me_update.payload.is_final_failure, false);
  assert.equal(me_update.payload.failure_bucket, null);
  assert.equal(me_update.payload.failure_reason, null);
  assert.equal(me_update.payload.error_message, null);
});

test("reconcileSupabaseDeliveryStatuses (Path 2): send_queue.delivery_confirmed=confirmed with matching pending message_event is normalized", async () => {
  const confirmed_queue_row = {
    id: 3001,
    provider_message_id: "SM_confirmed_1",
    delivered_at: "2026-04-19T11:00:00.000Z",
    delivery_confirmed: "confirmed",
  };

  // Path 1 returns empty so only path 2 fires for counts
  const { client, updates } = makeFakeSupabase({
    message_events_rows: [],  // path 1: no stale events
    queue_rows: [confirmed_queue_row],
  });

  const result = await reconcileSupabaseDeliveryStatuses(
    { limit: 50, now: "2026-04-19T12:30:00.000Z" },
    { supabase: client }
  );

  assert.equal(result.ok, true, "reconcile must succeed");
  assert.equal(result.normalized_from_queue_confirmed, 1, "must count one normalized event from path 2");

  const me_update = updates.find(
    (u) => u.table === "message_events"
  );
  assert.ok(me_update, "must have written an update to message_events");
  assert.equal(me_update.payload.delivery_status, "delivered");
  assert.equal(me_update.payload.provider_delivery_status, "delivered");
  assert.equal(me_update.payload.is_final_failure, false);
  assert.equal(me_update.payload.failure_bucket, null);
  assert.equal(me_update.payload.failure_reason, null);
});

test("reconcileSupabaseDeliveryStatuses: does NOT touch failed message_events rows", () => {
  // applyDeliveredNormalization must return null for rows with failed_at set
  const failed_row = {
    id: 4001,
    delivered_at: null,
    failed_at: "2026-04-19T12:10:00.000Z",
    delivery_status: "failed",
  };

  const correction = applyDeliveredNormalization(failed_row);
  assert.equal(correction, null, "failed rows must never be corrected by normalization helper");
});

test("reconcileSupabaseDeliveryStatuses: dry_run=true counts corrections without writing", async () => {
  const stale_event = {
    id: 5001,
    provider_message_sid: "SM_dry_1",
    delivered_at: "2026-04-19T12:05:00.000Z",
    failed_at: null,
    delivery_status: "sent",
  };

  const { client, updates } = makeFakeSupabase({
    message_events_rows: [stale_event],
    queue_rows: [],
  });

  const result = await reconcileSupabaseDeliveryStatuses(
    { limit: 50, now: "2026-04-19T12:30:00.000Z", dry_run: true },
    { supabase: client }
  );

  assert.equal(result.ok, true);
  assert.equal(result.dry_run, true);
  assert.equal(result.normalized_from_delivered_at, 1, "dry_run must count the correction");
  // No updates should have been written
  const me_writes = updates.filter((u) => u.table === "message_events");
  assert.equal(me_writes.length, 0, "dry_run must not write to message_events");
});

test("reconcileSupabaseDeliveryStatuses: returns errors array on query failure", async () => {
  const err = new Error("db_error");
  const error_client = {
    from() {
      const chain = {
        select: () => chain,
        not: () => chain,
        is: () => chain,
        neq: () => chain,
        eq: () => chain,
        or: () => chain,
        limit: () => chain,
        update: () => chain,
        then(onFulfilled, onRejected) {
          return Promise.resolve({ data: null, error: err }).then(onFulfilled, onRejected);
        },
        catch(onRejected) {
          return Promise.resolve({ data: null, error: err }).catch(onRejected);
        },
      };
      return chain;
    },
  };

  const result = await reconcileSupabaseDeliveryStatuses(
    { limit: 50 },
    { supabase: error_client }
  );

  assert.equal(result.ok, false, "ok must be false on query error");
  assert.ok(result.errors.length > 0, "must include at least one error message");
  assert.ok(result.errors.some((e) => e.includes("db_error")), "error message must reference db_error");
});
