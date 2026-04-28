import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";

const NOW = "2026-04-28T15:00:00.000Z";

function makeRow(id, overrides = {}) {
  return {
    id,
    queue_row_id: id,
    queue_status: "queued",
    scheduled_for: "2026-04-28T14:00:00.000Z",
    retry_count: 0,
    max_retries: 3,
    message_body: "Hey John, this is Chris. Do you still own 123 Main St?",
    message_text: "Hey John, this is Chris. Do you still own 123 Main St?",
    to_phone_number: "+17133781814",
    from_phone_number: "+12818458577",
    seller_first_name: "John",
    template_id: "200194",
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        master_owner_id: "mo_test",
        property_id: "prop_test",
      },
    },
    ...overrides,
  };
}

function makeHarness(initial_rows = [], overrides = {}) {
  const rows = new Map(initial_rows.map((row) => [String(row.id), { ...row, metadata: { ...(row.metadata || {}) } }]));
  const process_calls = [];
  const claim_calls = [];
  const pause_invalid_calls = [];

  const loadRows = async () => ({
    rows: [...rows.values()].filter((row) => row.queue_status === "queued"),
    raw_rows: [...rows.values()],
    skipped: [],
    now: NOW,
  });

  const deps = {
    getSystemFlag: async () => true,
    withRunLock: async ({ fn }) => fn(),
    info: () => {},
    warn: () => {},
    recordSystemAlert: async () => ({}),
    resolveSystemAlert: async () => ({}),
    loadRunnableSendQueueRows: loadRows,
    claimSendQueueRow: async (row, options = {}) => {
      const current = rows.get(String(row.id));
      if (!current || current.queue_status !== "queued") {
        return { claimed: false, reason: "queue_item_claim_conflict", row };
      }
      const lock_token = `lock-${row.id}`;
      Object.assign(current, {
        queue_status: "sending",
        is_locked: true,
        locked_at: options.now,
        lock_token,
        metadata: {
          ...(current.metadata || {}),
          processing_run_id: options.processing_run_id,
          run_started_at: options.run_started_at,
          claimed_at: options.now,
        },
      });
      claim_calls.push({ id: row.id, processing_run_id: options.processing_run_id });
      return { claimed: true, row: current, lock_token };
    },
    pauseInvalidQueueRow: async (row, reason) => {
      const current = rows.get(String(row.id));
      Object.assign(current, {
        queue_status: "paused_invalid_queue_row",
        is_locked: false,
        lock_token: null,
        metadata: {
          ...(current.metadata || {}),
          skip_reason: reason,
          final_queue_status: "paused_invalid_queue_row",
        },
      });
      pause_invalid_calls.push({ id: row.id, reason });
      return current;
    },
    pauseMaxRetriesQueueRow: async (row, reason) => {
      const current = rows.get(String(row.id));
      Object.assign(current, {
        queue_status: "paused_max_retries",
        is_locked: false,
        lock_token: null,
        metadata: {
          ...(current.metadata || {}),
          skip_reason: reason,
          final_queue_status: "paused_max_retries",
        },
      });
      return current;
    },
    failQueueItem: async (row, payload) => {
      const id = row.id || row.queue_row_id;
      const current = rows.get(String(id));
      Object.assign(current, {
        queue_status: payload.retry_count >= current.max_retries ? "failed" : "queued",
        retry_count: payload.retry_count,
        failed_reason: payload.failed_reason,
        is_locked: false,
        lock_token: null,
        metadata: {
          ...(current.metadata || {}),
          provider_error: payload.failed_reason,
          final_queue_status: payload.retry_count >= current.max_retries ? "failed" : "queued",
        },
      });
      return current;
    },
    finalizeClaimedSendQueueRows: async (claimed_rows) => {
      const finalized = [];
      for (const claimed of claimed_rows) {
        const id = String(claimed.queue_row_id || claimed.row?.id);
        const current = rows.get(id);
        if (!current || current.queue_status !== "sending") continue;
        const next_retry_count = Number(current.retry_count || 0) + 1;
        Object.assign(current, {
          queue_status: next_retry_count >= current.max_retries ? "failed" : "queued",
          retry_count: next_retry_count,
          is_locked: false,
          lock_token: null,
          metadata: {
            ...(current.metadata || {}),
            finalize_safety_net: true,
            final_queue_status: next_retry_count >= current.max_retries ? "failed" : "queued",
            finalization_error: claimed.reason || "finalize_safety_net",
          },
        });
        finalized.push(current);
      }
      return {
        ok: true,
        finalized_count: finalized.length,
        stuck_recycled_count: finalized.length,
        finalized,
        errors: [],
      };
    },
    processSendQueueItem: async (row) => {
      process_calls.push(row.id || row.queue_row_id || row);
      return { ok: true, sent: true, queue_status: "sent" };
    },
    ...overrides,
  };

  return {
    rows,
    deps,
    process_calls,
    claim_calls,
    pause_invalid_calls,
  };
}

test("outside contact window claimed row does not remain sending", async () => {
  const row = makeRow(9001);
  const harness = makeHarness([row], {
    processSendQueueItem: async (claimed_row) => ({
      ok: true,
      skipped: true,
      reason: "outside_contact_window",
      queue_status: "queued",
      queue_row_id: claimed_row.id,
    }),
  });

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9001").queue_status, "queued");
  assert.equal(result.finalize_safety_net_count, 1);
  assert.equal(result.results[0].final_queue_status, "queued");
});

test("missing seller name claimed row does not remain sending", async () => {
  const row = makeRow(9002, { seller_first_name: null });
  const harness = makeHarness([row], {
    processSendQueueItem: async (claimed_row) => {
      const current = harness.rows.get(String(claimed_row.id));
      current.queue_status = "paused_name_missing";
      current.is_locked = false;
      current.lock_token = null;
      current.metadata = {
        ...(current.metadata || {}),
        skip_reason: "missing_seller_first_name",
      };
      return {
        ok: false,
        skipped: true,
        reason: "missing_seller_first_name",
        queue_status: "paused_name_missing",
        queue_row_id: claimed_row.id,
      };
    },
  });

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9002").queue_status, "paused_name_missing");
  assert.equal(result.results[0].final_queue_status, "paused_name_missing");
  assert.equal(result.finalize_safety_net_count, 0);
});

test("malformed row with null selected_template_id and candidate_snapshot becomes paused_invalid_queue_row", async () => {
  const row = makeRow(9003, {
    template_id: null,
    metadata: {
      selected_template_id: null,
      candidate_snapshot: null,
    },
  });
  const harness = makeHarness([row]);

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9003").queue_status, "paused_invalid_queue_row");
  assert.equal(harness.pause_invalid_calls.length, 1);
  assert.equal(harness.pause_invalid_calls[0].reason, "missing_selected_template_id");
  assert.equal(harness.process_calls.length, 0);
  assert.equal(result.invalid_queue_row_count, 1);
  assert.equal(result.results[0].final_queue_status, "paused_invalid_queue_row");
});

test("provider exception claimed row does not remain sending", async () => {
  const row = makeRow(9004);
  const harness = makeHarness([row], {
    processSendQueueItem: async () => {
      throw new Error("provider_timeout");
    },
  });

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9004").queue_status, "queued");
  assert.equal(harness.rows.get("9004").retry_count, 1);
  assert.equal(result.failed_count, 1);
  assert.equal(result.results[0].final_queue_status, "queued");
});

test("batch of 25 claimed rows leaves zero rows in sending", async () => {
  const rows = Array.from({ length: 25 }, (_, index) => makeRow(9100 + index));
  const harness = makeHarness(rows, {
    processSendQueueItem: async (claimed_row) => {
      const id = Number(claimed_row.id);
      if (id < 9105) {
        const current = harness.rows.get(String(id));
        current.queue_status = "blocked";
        current.is_locked = false;
        current.lock_token = null;
        current.metadata = {
          ...(current.metadata || {}),
          skip_reason: "blocked_by_guard",
        };
        return {
          ok: false,
          reason: "blocked_by_guard",
          queue_status: "blocked",
          queue_row_id: id,
        };
      }

      return {
        ok: true,
        skipped: true,
        reason: "validation_skipped",
        queue_status: "queued",
        queue_row_id: id,
      };
    },
  });

  const result = await runSendQueue({ limit: 25, now: NOW }, harness.deps);
  const sending_rows = [...harness.rows.values()].filter((row) => row.queue_status === "sending");

  assert.equal(result.claimed_count, 25);
  assert.equal(result.blocked_count, 5);
  assert.equal(result.skipped_count, 20);
  assert.equal(result.finalize_safety_net_count, 20);
  assert.equal(result.stuck_recycled_count, 20);
  assert.equal(sending_rows.length, 0);
});
