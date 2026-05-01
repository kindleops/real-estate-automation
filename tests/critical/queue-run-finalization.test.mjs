import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { finalizeClaimedSendQueueRows } from "@/lib/supabase/sms-engine.js";

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
        seller_first_name: "John",
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
  const pause_name_missing_calls = [];

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
    pauseNameMissingQueueRow: async (row, reason) => {
      const current = rows.get(String(row.id));
      Object.assign(current, {
        queue_status: "paused_name_missing",
        is_locked: false,
        lock_token: null,
        metadata: {
          ...(current.metadata || {}),
          skip_reason: reason,
          final_queue_status: "paused_name_missing",
          paused_at: NOW,
        },
      });
      pause_name_missing_calls.push({ id: row.id, reason });
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
    pause_name_missing_calls,
  };
}

function makeSelectionSupabase(rows = []) {
  return {
    from() {
      const query = {
        select() {
          return query;
        },
        eq() {
          return query;
        },
        or() {
          return query;
        },
        not() {
          return query;
        },
        order() {
          return query;
        },
        limit() {
          return Promise.resolve({
            data: rows,
            error: null,
          });
        },
      };
      return query;
    },
  };
}

function makePreclaimHarness(initial_rows = [], overrides = {}) {
  const rows = new Map(initial_rows.map((row) => [String(row.id), { ...row, metadata: { ...(row.metadata || {}) } }]));
  const claimed_ids = [];
  const processed_ids = [];
  const paused_name_ids = [];
  const paused_invalid_ids = [];
  const paused_max_retry_ids = [];

  const deps = {
    getSystemFlag: async () => true,
    withRunLock: async ({ fn }) => fn(),
    info: () => {},
    warn: () => {},
    recordSystemAlert: async () => ({}),
    resolveSystemAlert: async () => ({}),
    supabase: makeSelectionSupabase([...rows.values()]),
    evaluateContactWindow: (row) => {
      if (row?.metadata?.window_state === "outside") {
        return {
          allowed: false,
          reason: "outside_contact_window",
          timezone: "America/Chicago",
          valid_window: true,
        };
      }
      return {
        allowed: true,
        reason: "inside_contact_window",
        timezone: "America/Chicago",
        valid_window: true,
      };
    },
    claimSendQueueRow: async (row, options = {}) => {
      const current = rows.get(String(row.id));
      if (!current || current.queue_status !== "queued") {
        return { claimed: false, reason: "queue_item_claim_conflict", row };
      }
      const lock_token = `lock-${row.id}`;
      Object.assign(current, {
        queue_status: "sending",
        is_locked: true,
        lock_token,
        locked_at: options.now,
        metadata: {
          ...(current.metadata || {}),
          processing_run_id: options.processing_run_id,
          run_started_at: options.run_started_at,
          claimed_at: options.now,
        },
      });
      claimed_ids.push(current.id);
      return {
        claimed: true,
        row: current,
        lock_token,
      };
    },
    pauseNameMissingQueueRow: async (row, payload_or_reason) => {
      const current = rows.get(String(row.id));
      const payload = typeof payload_or_reason === "string"
        ? {
            queue_status: "paused_name_missing",
            metadata: {
              ...(current.metadata || {}),
              skip_reason: payload_or_reason,
              final_queue_status: "paused_name_missing",
            },
          }
        : payload_or_reason;
      Object.assign(current, payload);
      paused_name_ids.push(current.id);
      return current;
    },
    pauseInvalidQueueRow: async (row, payload_or_reason) => {
      const current = rows.get(String(row.id));
      const payload = typeof payload_or_reason === "string"
        ? {
            queue_status: "paused_invalid_queue_row",
            metadata: {
              ...(current.metadata || {}),
              skip_reason: payload_or_reason,
              final_queue_status: "paused_invalid_queue_row",
            },
          }
        : payload_or_reason;
      Object.assign(current, payload);
      paused_invalid_ids.push(current.id);
      return current;
    },
    pauseMaxRetriesQueueRow: async (row, payload_or_reason) => {
      const current = rows.get(String(row.id));
      const payload = typeof payload_or_reason === "string"
        ? {
            queue_status: "paused_max_retries",
            metadata: {
              ...(current.metadata || {}),
              skip_reason: payload_or_reason,
              final_queue_status: "paused_max_retries",
            },
          }
        : payload_or_reason;
      Object.assign(current, payload);
      paused_max_retry_ids.push(current.id);
      return current;
    },
    processSendQueueItem: async (row) => {
      const current = rows.get(String(row.id));
      Object.assign(current, {
        queue_status: "sent",
        is_locked: false,
        lock_token: null,
        sent_at: NOW,
      });
      processed_ids.push(current.id);
      return {
        ok: true,
        sent: true,
        queue_status: "sent",
        final_queue_status: "sent",
        provider_message_id: `SM-${row.id}`,
        queue_row_id: row.id,
      };
    },
    finalizeClaimedSendQueueRows: async () => ({
      ok: true,
      finalized_count: 0,
      stuck_recycled_count: 0,
      finalized: [],
      errors: [],
    }),
    ...overrides,
  };

  return {
    rows,
    deps,
    claimed_ids,
    processed_ids,
    paused_name_ids,
    paused_invalid_ids,
    paused_max_retry_ids,
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

test("missing seller name is paused before claim and never becomes sending", async () => {
  const row = makeRow(9002, {
    seller_first_name: null,
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        master_owner_id: "mo_test",
        property_id: "prop_test",
      },
    },
  });
  const harness = makeHarness([row]);

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9002").queue_status, "paused_name_missing");
  assert.equal(harness.claim_calls.length, 0);
  assert.equal(harness.process_calls.length, 0);
  assert.equal(harness.pause_name_missing_calls.length, 1);
  assert.equal(result.results[0].final_queue_status, "paused_name_missing");
  assert.equal(result.claimed_count, 0);
  assert.equal(result.preclaim_paused_name_missing_count, 1);
  assert.equal(result.finalize_safety_net_count, 0);
});

test("candidate_snapshot.phone_first_name is used for preclaim eligibility when seller_first_name is missing", async () => {
  const row = makeRow(90021, {
    seller_first_name: null,
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        master_owner_id: "mo_test",
        property_id: "prop_test",
        phone_first_name: "Mia",
      },
    },
  });
  const harness = makePreclaimHarness([row]);

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(result.preclaim_paused_name_missing_count, 0);
  assert.equal(result.claimed_count, 1);
  assert.equal(result.sent_count, 1);
  assert.equal(harness.claimed_ids.length, 1);
  assert.deepEqual(harness.claimed_ids, [90021]);
  assert.equal(harness.processed_ids.length, 1);
  assert.equal(harness.rows.get("90021").queue_status, "sent");
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

test("manual inbox row without selected_template_id is runnable and not paused invalid", async () => {
  const row = makeRow(9010, {
    queue_key: "inbox:send_now:9010",
    template_id: null,
    seller_first_name: null,
    message_type: "manual_reply",
    use_case_template: "inbox_manual_send_now",
    metadata: {
      selected_template_id: null,
      candidate_snapshot: null,
    },
  });
  const harness = makeHarness([row]);

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.pause_invalid_calls.length, 0);
  assert.equal(harness.process_calls.length, 1);
  assert.equal(result.sent_count, 1);
});

test("manual inbox row with missing body still pauses invalid", async () => {
  const row = makeRow(9011, {
    queue_key: "inbox:send_now:9011",
    template_id: null,
    seller_first_name: null,
    message_body: "",
    message_text: "",
    message_type: "manual_reply",
    use_case_template: "inbox_manual_send_now",
    metadata: {
      selected_template_id: null,
      candidate_snapshot: null,
    },
  });
  const harness = makeHarness([row]);

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9011").queue_status, "paused_invalid_queue_row");
  assert.equal(harness.pause_invalid_calls[0].reason, "missing_message_body");
  assert.equal(harness.process_calls.length, 0);
  assert.equal(result.invalid_queue_row_count, 1);
});

test("manual inbox row with missing to/from still pauses invalid", async () => {
  const row = makeRow(9012, {
    queue_key: "inbox:send_now:9012",
    template_id: null,
    seller_first_name: null,
    to_phone_number: "",
    from_phone_number: "",
    message_type: "manual_reply",
    use_case_template: "inbox_manual_send_now",
    metadata: {
      selected_template_id: null,
      candidate_snapshot: null,
    },
  });
  const harness = makeHarness([row]);

  const result = await runSendQueue({ limit: 1, now: NOW }, harness.deps);

  assert.equal(harness.rows.get("9012").queue_status, "paused_invalid_queue_row");
  assert.equal(harness.pause_invalid_calls[0].reason, "missing_to_phone_number");
  assert.equal(harness.process_calls.length, 0);
  assert.equal(result.invalid_queue_row_count, 1);
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

test("preclaim selection skips outside-window rows and claims later eligible rows", async () => {
  const outside_rows = Array.from({ length: 30 }, (_, index) =>
    makeRow(9200 + index, {
      metadata: {
        selected_template_id: "200194",
        candidate_snapshot: {
          master_owner_id: "mo_test",
          property_id: "prop_test",
        },
        window_state: "outside",
      },
    })
  );
  const eligible_rows = Array.from({ length: 20 }, (_, index) => makeRow(9300 + index));
  const harness = makePreclaimHarness([...outside_rows, ...eligible_rows]);

  const result = await runSendQueue({ limit: 20, now: NOW }, harness.deps);

  assert.equal(result.preclaim_scanned_count, 50);
  assert.equal(result.preclaim_outside_window_excluded_count, 30);
  assert.equal(result.eligible_claim_count, 20);
  assert.equal(result.attempted_count, 20);
  assert.equal(result.claimed_count, 20);
  assert.deepEqual(harness.claimed_ids, eligible_rows.map((row) => row.id));
  assert.equal([...harness.rows.values()].filter((row) => row.metadata?.window_state === "outside" && row.queue_status === "queued").length, 30);
});

test("preclaim selection does not claim next_retry_pending rows", async () => {
  const retry_pending = makeRow(9401, {
    next_retry_at: "2026-04-28T16:00:00.000Z",
  });
  const eligible = makeRow(9402);
  const harness = makePreclaimHarness([retry_pending, eligible]);

  const result = await runSendQueue({ limit: 2, now: NOW }, harness.deps);

  assert.equal(result.preclaim_retry_pending_excluded_count, 1);
  assert.equal(result.eligible_claim_count, 1);
  assert.equal(result.claimed_count, 1);
  assert.deepEqual(harness.claimed_ids, [9402]);
  assert.equal(harness.rows.get("9401").queue_status, "queued");
});

test("outside-window rows do not increase claimed_count", async () => {
  const outside_rows = Array.from({ length: 3 }, (_, index) =>
    makeRow(9500 + index, {
      metadata: {
        selected_template_id: "200194",
        candidate_snapshot: {
          master_owner_id: "mo_test",
          property_id: "prop_test",
        },
        window_state: "outside",
      },
    })
  );
  const harness = makePreclaimHarness(outside_rows);

  const result = await runSendQueue({ limit: 3, now: NOW }, harness.deps);

  assert.equal(result.preclaim_outside_window_excluded_count, 3);
  assert.equal(result.eligible_claim_count, 0);
  assert.equal(result.attempted_count, 0);
  assert.equal(result.claimed_count, 0);
  assert.equal(harness.claimed_ids.length, 0);
  assert.equal([...harness.rows.values()].every((row) => row.queue_status === "queued"), true);
});

test("preclaim scans past paused and excluded rows until the eligible limit is reached", async () => {
  const missing_name_rows = Array.from({ length: 20 }, (_, index) =>
    makeRow(9600 + index, {
      seller_first_name: null,
      metadata: {
        selected_template_id: "200194",
        candidate_snapshot: {
          master_owner_id: "mo_test",
          property_id: "prop_test",
        },
      },
    })
  );
  const outside_rows = Array.from({ length: 20 }, (_, index) =>
    makeRow(9700 + index, {
      metadata: {
        selected_template_id: "200194",
        candidate_snapshot: {
          master_owner_id: "mo_test",
          property_id: "prop_test",
          seller_first_name: "John",
        },
        window_state: "outside",
      },
    })
  );
  const retry_pending_rows = Array.from({ length: 10 }, (_, index) =>
    makeRow(9800 + index, {
      next_retry_at: "2026-04-28T16:00:00.000Z",
    })
  );
  const eligible_rows = Array.from({ length: 50 }, (_, index) => makeRow(9900 + index));
  const harness = makePreclaimHarness([
    ...missing_name_rows,
    ...outside_rows,
    ...retry_pending_rows,
    ...eligible_rows,
  ]);

  const result = await runSendQueue({ limit: 25, now: NOW }, harness.deps);

  assert.equal(result.preclaim_scanned_count, 75);
  assert.equal(result.preclaim_paused_name_missing_count, 20);
  assert.equal(result.preclaim_outside_window_excluded_count, 20);
  assert.equal(result.preclaim_retry_pending_excluded_count, 10);
  assert.equal(result.eligible_claim_count, 25);
  assert.equal(result.claimed_count, 25);
  assert.deepEqual(harness.claimed_ids, eligible_rows.slice(0, 25).map((row) => row.id));
  assert.equal(new Set(harness.paused_name_ids).size, 20);
  assert.equal(missing_name_rows.every((row) => harness.rows.get(String(row.id)).queue_status === "paused_name_missing"), true);
  assert.equal(outside_rows.every((row) => harness.rows.get(String(row.id)).queue_status === "queued"), true);
  assert.equal(retry_pending_rows.every((row) => harness.rows.get(String(row.id)).queue_status === "queued"), true);
});

test("dry_run reports preclaim diagnostics without mutating row statuses", async () => {
  const missing_name = makeRow(10101, {
    seller_first_name: null,
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        master_owner_id: "mo_test",
        property_id: "prop_test",
      },
    },
  });
  const outside = makeRow(10102, {
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        master_owner_id: "mo_test",
        property_id: "prop_test",
        seller_first_name: "John",
      },
      window_state: "outside",
    },
  });
  const retry_pending = makeRow(10103, {
    next_retry_at: "2026-04-28T16:00:00.000Z",
  });
  const eligible = makeRow(10104);
  const harness = makePreclaimHarness([missing_name, outside, retry_pending, eligible]);

  const result = await runSendQueue({ limit: 1, dry_run: true, now: NOW }, harness.deps);

  assert.equal(result.preclaim_paused_name_missing_count, 1);
  assert.equal(result.preclaim_outside_window_excluded_count, 1);
  assert.equal(result.preclaim_retry_pending_excluded_count, 1);
  assert.equal(result.eligible_claim_count, 1);
  assert.equal(result.claimed_count, 0);
  assert.equal(result.attempted_count, 1);
  assert.equal(harness.paused_name_ids.length, 0);
  assert.equal([...harness.rows.values()].every((row) => row.queue_status === "queued"), true);
});

test("terminal paused rows are not safety-net recycled", async () => {
  let recycle_calls = 0;

  const result = await finalizeClaimedSendQueueRows(
    [{ row: { id: 11001, lock_token: "lock-11001" }, lock_token: "lock-11001" }],
    {
      processing_run_id: "run-current",
      loadQueueRowById: async () =>
        makeRow(11001, {
          queue_status: "paused_name_missing",
          lock_token: null,
          metadata: {
            selected_template_id: "200194",
            candidate_snapshot: {
              seller_first_name: "John",
            },
            processing_run_id: "run-current",
          },
        }),
      recycleClaimedSendingRow: async () => {
        recycle_calls += 1;
        return {};
      },
    }
  );

  assert.equal(result.finalized_count, 0);
  assert.equal(result.stuck_recycled_count, 0);
  assert.equal(recycle_calls, 0);
});

test("safety net does not recycle sending row when metadata.final_queue_status is paused_name_missing", async () => {
  let recycle_calls = 0;

  const result = await finalizeClaimedSendQueueRows(
    [{ row: { id: 11002, lock_token: "lock-11002" }, lock_token: "lock-11002" }],
    {
      processing_run_id: "run-current",
      loadQueueRowById: async () =>
        makeRow(11002, {
          queue_status: "sending",
          lock_token: "lock-11002",
          metadata: {
            selected_template_id: "200194",
            candidate_snapshot: {
              seller_first_name: "John",
            },
            processing_run_id: "run-current",
            final_queue_status: "paused_name_missing",
          },
        }),
      recycleClaimedSendingRow: async () => {
        recycle_calls += 1;
        return {};
      },
    }
  );

  assert.equal(result.finalized_count, 0);
  assert.equal(result.stuck_recycled_count, 0);
  assert.equal(recycle_calls, 0);
});

test("safety net recycles only still-sending rows from the current run", async () => {
  const loaded = new Map([
    [
      "12001",
      makeRow(12001, {
        queue_status: "sending",
        lock_token: "lock-12001",
        metadata: {
          selected_template_id: "200194",
          candidate_snapshot: {
            seller_first_name: "John",
          },
          processing_run_id: "run-current",
        },
      }),
    ],
    [
      "12002",
      makeRow(12002, {
        queue_status: "sending",
        lock_token: "lock-12002",
        metadata: {
          selected_template_id: "200194",
          candidate_snapshot: {
            seller_first_name: "John",
          },
          processing_run_id: "run-old",
        },
      }),
    ],
    [
      "12003",
      makeRow(12003, {
        queue_status: "queued",
        lock_token: null,
        metadata: {
          selected_template_id: "200194",
          candidate_snapshot: {
            seller_first_name: "John",
          },
          processing_run_id: "run-current",
        },
      }),
    ],
  ]);
  const recycled_ids = [];

  const result = await finalizeClaimedSendQueueRows(
    [
      { row: { id: 12001, lock_token: "lock-12001" }, lock_token: "lock-12001" },
      { row: { id: 12002, lock_token: "lock-12002" }, lock_token: "lock-12002" },
      { row: { id: 12003, lock_token: null }, lock_token: null },
    ],
    {
      processing_run_id: "run-current",
      loadQueueRowById: async (id) => loaded.get(String(id)),
      recycleClaimedSendingRow: async (row, _lock_token, payload) => {
        recycled_ids.push(row.id);
        return {
          ...row,
          ...payload,
        };
      },
    }
  );

  assert.deepEqual(recycled_ids, [12001]);
  assert.equal(result.finalized_count, 1);
  assert.equal(result.stuck_recycled_count, 1);
});
