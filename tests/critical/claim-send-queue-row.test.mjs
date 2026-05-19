import test from "node:test";
import assert from "node:assert/strict";

import { claimSendQueueRow } from "@/lib/supabase/sms-engine.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeClaimSupabase(existing_queue_status) {
  // Simulates a Supabase client whose .in("queue_status",...) filter
  // only returns a row if the status is in the provided list.
  // This validates the WHERE condition in claimSendQueueRow.
  const captured = { in_statuses: null };

  const row_returned = { id: "test-row-id", queue_status: "sending", is_locked: true, lock_token: "abc", locked_at: new Date().toISOString(), metadata: {}, to_phone_number: "+15005550006", from_phone_number: "+15005550001", message_body: "Hello", message_text: "Hello", seller_first_name: "Test", updated_at: new Date().toISOString() };

  const query = {
    update() { return query; },
    eq() { return query; },
    in(col, vals) {
      if (col === "queue_status") captured.in_statuses = vals;
      return query;
    },
    is() { return query; },
    select() { return query; },
    async maybeSingle() {
      // Return the row only if the existing status is in the accepted list
      const accepted = captured.in_statuses || [];
      const matched = accepted.includes(existing_queue_status);
      return { data: matched ? row_returned : null, error: null };
    },
  };

  return { supabase: { from: () => query }, captured };
}

function makeRow(queue_status) {
  return {
    id: "test-row-id",
    queue_status,
    is_locked: false,
    lock_token: null,
    locked_at: null,
    metadata: {},
    to_phone_number: "+15005550006",
    from_phone_number: "+15005550001",
    message_body: "Hello test",
    message_text: "Hello test",
    seller_first_name: "Test",
  };
}

// ─── regression: "scheduled" rows were not claimable before fix ───────────────

test("claimSendQueueRow succeeds for queue_status=scheduled (regression: was missing from WHERE)", async () => {
  const { supabase } = makeClaimSupabase("scheduled");

  const result = await claimSendQueueRow(makeRow("scheduled"), { supabase });

  assert.equal(result.ok, true, "scheduled row must be claimable");
  assert.equal(result.claimed, true);
  assert.equal(result.reason, "claimed");
  assert.ok(result.lock_token, "lock_token must be returned");
});

test("claimSendQueueRow returns queue_item_claim_conflict when WHERE does not match (simulates pre-fix behavior)", async () => {
  // Use a mock that only accepts "queued" — the pre-fix behavior
  const row_returned = { id: "test-row-id", queue_status: "sending", is_locked: true, lock_token: "abc", locked_at: new Date().toISOString(), metadata: {}, to_phone_number: "+15005550006", from_phone_number: "+15005550001", message_body: "Hello", message_text: "Hello", seller_first_name: "Test", updated_at: new Date().toISOString() };

  const query = {
    update() { return query; },
    eq() { return query; },
    in(col, vals) { return query; }, // absorb all .in() calls
    is() { return query; },
    select() { return query; },
    // Always returns null — simulates the pre-fix WHERE rejecting "scheduled"
    async maybeSingle() { return { data: null, error: null }; },
  };

  const supabase = { from: () => query };
  const result = await claimSendQueueRow(makeRow("scheduled"), { supabase });

  assert.equal(result.ok, false);
  assert.equal(result.claimed, false);
  assert.equal(result.reason, "queue_item_claim_conflict");
});

test("claimSendQueueRow succeeds for queue_status=queued (existing behavior preserved)", async () => {
  const { supabase } = makeClaimSupabase("queued");
  const result = await claimSendQueueRow(makeRow("queued"), { supabase });
  assert.equal(result.ok, true, "queued row must still be claimable");
  assert.equal(result.claimed, true);
});

test("claimSendQueueRow succeeds for queue_status=pending", async () => {
  const { supabase } = makeClaimSupabase("pending");
  const result = await claimSendQueueRow(makeRow("pending"), { supabase });
  assert.equal(result.ok, true, "pending row must be claimable");
});

test("claimSendQueueRow accepts all statuses loaded by loadRunnableSendQueueRows", async () => {
  const runnable_statuses = ["queued", "Queued", "scheduled", "pending", "approved", "ready"];
  for (const status of runnable_statuses) {
    const { supabase } = makeClaimSupabase(status);
    const result = await claimSendQueueRow(makeRow(status), { supabase });
    assert.equal(result.ok, true, `status="${status}" must be claimable`);
  }
});

test("claimSendQueueRow returns missing_queue_row_id when row has no id", async () => {
  const { supabase } = makeClaimSupabase("scheduled");
  const result = await claimSendQueueRow({ queue_status: "scheduled" }, { supabase });
  assert.equal(result.ok, false);
  assert.equal(result.reason, "missing_queue_row_id");
});
