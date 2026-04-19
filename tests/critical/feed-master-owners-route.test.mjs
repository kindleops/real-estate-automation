import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeFeederRequest,
  runFeederWithRollout,
} from "@/lib/domain/master-owners/feed-master-owners-request.js";
import {
  DEFAULT_FEEDER_BATCH_SIZE,
  DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
  DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
  DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
  DEFAULT_FEEDER_BUFFER_MIN_QUEUED,
  DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
  DEFAULT_FEEDER_SCAN_LIMIT,
  DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
} from "@/lib/config/rollout-controls.js";

function makeLogger() {
  const entries = [];
  return {
    entries,
    logger: {
      info: (event, meta) => entries.push({ level: "info", event, meta }),
      warn: (event, meta) => entries.push({ level: "warn", event, meta }),
      error: (event, meta) => entries.push({ level: "error", event, meta }),
    },
  };
}

function makeDeps({ executeRunResult } = {}) {
  const execute_calls = [];
  const { entries, logger } = makeLogger();

  return {
    entries,
    execute_calls,
    deps: {
      logger,
      resolveMutationDryRunImpl: () => ({
        effective_dry_run: true,
        reason: "requested_dry_run",
      }),
      executeRunImpl: async (options) => {
        execute_calls.push(options);
        return (
          executeRunResult || {
            ok: true,
            source: {
              view_id: 61752339,
              view_name: options.source_view_name,
            },
            queued_owner_ids: [],
          }
        );
      },
      withRunLockImpl: async () => {
        throw new Error("withRunLockImpl should not run when dry_run is forced");
      },
      recordSystemAlertImpl: async () => {},
      resolveSystemAlertImpl: async () => {},
      buildPodioCooldownSkipResultImpl: async () => null,
      buildPodioBackpressureSkipResultImpl: async () => null,
    },
  };
}

test("runFeederWithRollout defaults cron feeder source to Tier 1 ALL", async () => {
  const { entries, execute_calls, deps } = makeDeps();

  const result = await runFeederWithRollout({}, deps);

  assert.equal(execute_calls.length, 1);
  assert.equal(execute_calls[0].source_view_id, null);
  assert.equal(execute_calls[0].source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(result.rollout.requested_source_view_id, null);
  assert.equal(result.rollout.requested_source_view_name, null);
  assert.equal(result.rollout.effective_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(result.rollout.resolved_source_view_id, 61752339);
  assert.equal(result.rollout.resolved_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(result.rollout.source_view_fallback_occurred, false);
  assert.equal(result.rollout.source_view_fallback_reason, null);

  const scope = entries.find(
    (entry) => entry.event === "master_owner_feeder.source_view_scope_evaluated"
  )?.meta;
  assert.ok(scope, "source view scope log must be emitted");
  assert.equal(scope.requested_source_view_name, null);
  assert.equal(scope.resolved_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(scope.safe_scope_passed, true);
  assert.equal(scope.safe_scope_reason, "feeder_view_default_applied");
  assert.equal(scope.defaulted, true);

  const completed = entries.find(
    (entry) => entry.event === "master_owner_feeder.completed"
  )?.meta;
  assert.ok(completed, "completion log must be emitted");
  assert.equal(completed.effective_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(completed.resolved_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(completed.source_view_fallback_occurred, false);
  assert.equal(completed.scanned_count, 0);
});

test("runFeederWithRollout normalizes zero limit and scan_limit to defaults", async () => {
  const { execute_calls, deps } = makeDeps();

  await runFeederWithRollout(
    {
      limit: 0,
      scan_limit: 0,
      source_view_name: "SMS / TIER #1 / FILE #1",
    },
    deps
  );

  assert.equal(execute_calls.length, 1);
  assert.equal(execute_calls[0].limit, DEFAULT_FEEDER_BATCH_SIZE);
  assert.equal(execute_calls[0].scan_limit, DEFAULT_FEEDER_SCAN_LIMIT);
  assert.equal(execute_calls[0].source_view_name, "SMS / TIER #1 / FILE #1");
});

test("normalizeFeederRequest maps zero values to feeder defaults", () => {
  const normalized = normalizeFeederRequest({
    limit: 0,
    scan_limit: 0,
  });

  assert.equal(normalized.limit, DEFAULT_FEEDER_BATCH_SIZE);
  assert.equal(normalized.scan_limit, DEFAULT_FEEDER_SCAN_LIMIT);
});

test("runFeederWithRollout skips safely when Podio cooldown is active", async () => {
  const { execute_calls, deps } = makeDeps();
  let with_run_lock_called = false;

  const result = await runFeederWithRollout(
    {
      source_view_name: "SMS / TIER #1 / ALL",
    },
    {
      ...deps,
      buildPodioCooldownSkipResultImpl: async () => ({
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
        eligible_owner_count: 0,
        queued_count: 0,
        skip_reason_counts: [],
      }),
      withRunLockImpl: async () => {
        with_run_lock_called = true;
        throw new Error("withRunLockImpl should not run during Podio cooldown");
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "podio_rate_limit_cooldown_active");
  assert.equal(result.retry_after_seconds, 3600);
  assert.equal(execute_calls.length, 0);
  assert.equal(with_run_lock_called, false);
});

test("runFeederWithRollout skips live feeding when Podio backpressure is active", async () => {
  const { execute_calls, deps } = makeDeps();
  let with_run_lock_called = false;

  const result = await runFeederWithRollout(
    {
      source_view_name: "SMS / TIER #1 / ALL",
    },
    {
      ...deps,
      resolveMutationDryRunImpl: () => ({
        effective_dry_run: false,
        reason: "live_mode",
      }),
      buildPodioBackpressureSkipResultImpl: async () => ({
        ok: true,
        skipped: true,
        reason: "podio_rate_limit_low_remaining",
        podio_backpressure: {
          active: true,
          min_remaining: 150,
          observation: {
            path: "/item/app/30541680/filter/",
            operation: "filter_items",
            rate_limit_remaining: 42,
            rate_limit_limit: 1000,
            observed_at: "2026-04-08T19:15:25.000Z",
          },
        },
        scanned_count: 0,
        eligible_owner_count: 0,
        queued_count: 0,
        skip_reason_counts: [],
      }),
      withRunLockImpl: async () => {
        with_run_lock_called = true;
        throw new Error("withRunLockImpl should not run during Podio backpressure");
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "podio_rate_limit_low_remaining");
  assert.equal(execute_calls.length, 0);
  assert.equal(with_run_lock_called, false);
});

test("runFeederWithRollout tops up the queue buffer when future inventory is low", async () => {
  const { execute_calls, deps } = makeDeps();
  let with_run_lock_called = false;

  const result = await runFeederWithRollout(
    {
      source_view_name: "SMS / TIER #1 / ALL",
    },
    {
      ...deps,
      resolveMutationDryRunImpl: () => ({
        effective_dry_run: false,
        reason: "live_mode",
      }),
      getRolloutControlsImpl: () => ({
        feeder_default_batch: DEFAULT_FEEDER_BATCH_SIZE,
        feeder_default_scan_limit: DEFAULT_FEEDER_SCAN_LIMIT,
        feeder_buffer_min_queued: DEFAULT_FEEDER_BUFFER_MIN_QUEUED,
        feeder_buffer_critical_low: DEFAULT_FEEDER_BUFFER_CRITICAL_LOW,
        feeder_buffer_replenish_target: DEFAULT_FEEDER_BUFFER_REPLENISH_TARGET,
        feeder_buffer_healthy_target: DEFAULT_FEEDER_BUFFER_HEALTHY_TARGET,
        feeder_buffer_ideal_target: DEFAULT_FEEDER_BUFFER_IDEAL_TARGET,
        feeder_max_batch: 500,
        feeder_view_only_id: null,
        feeder_view_only_name: null,
        single_master_owner_id: null,
      }),
      capFeederBatchImpl: (value) => Math.min(Number(value || 0), 500),
      capFeederScanLimitImpl: (value) => Math.min(Number(value || 0), 1000),
      inspectQueueBufferImpl: async () => ({
        queued_inventory_count: 180,
        available_inventory_count: 180,
        future_inventory_count: 12,
        due_inventory_count: 6,
        queued_future_count: 12,
        queued_due_now_count: 6,
        sending_count: 0,
        failed_recent_count: 3,
        critical_low_threshold: 250,
        replenish_target: 750,
        healthy_target: 1500,
        ideal_target: 2000,
        desired_buffer_target: 2000,
        critical_low_threshold_breached: true,
        replenish_threshold_met: false,
        healthy_buffer_threshold_met: false,
        ideal_buffer_threshold_met: false,
        buffer_target: 2000,
        buffer_deficit: 1820,
        buffer_satisfied: false,
        snapshot_limit: 500,
      }),
      withRunLockImpl: async ({ fn }) => {
        with_run_lock_called = true;
        return fn();
      },
    }
  );

  assert.equal(with_run_lock_called, true);
  assert.equal(execute_calls.length, 1);
  assert.equal(execute_calls[0].limit, 500);
  assert.equal(execute_calls[0].scan_limit, 1000);
  assert.equal(result.queue_inventory.available_inventory_count, 180);
  assert.equal(result.queue_inventory.future_inventory_count, 12);
  assert.equal(result.queue_inventory.critical_low_threshold_breached, true);
  assert.equal(result.rollout.queue_inventory.buffer_deficit, 1820);
});

test("runFeederWithRollout skips live feeding when queued future inventory already satisfies the buffer", async () => {
  const { execute_calls, deps } = makeDeps();

  const result = await runFeederWithRollout(
    {
      source_view_name: "SMS / TIER #1 / ALL",
    },
    {
      ...deps,
      resolveMutationDryRunImpl: () => ({
        effective_dry_run: false,
        reason: "live_mode",
      }),
      inspectQueueBufferImpl: async () => ({
        queued_inventory_count: 140,
        future_inventory_count: 125,
        due_inventory_count: 15,
        buffer_target: 120,
        buffer_deficit: 0,
        buffer_satisfied: true,
        snapshot_limit: 120,
      }),
    }
  );

  assert.equal(execute_calls.length, 0);
  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "feeder_queue_buffer_satisfied");
  assert.equal(result.queue_inventory.future_inventory_count, 125);
});

// ---------------------------------------------------------------------------
// Query/body parsing — feeder diagnostics patch
// ---------------------------------------------------------------------------

test("normalizeFeederRequest parses string limit=1 and scan_limit=10 from query params", () => {
  const normalized = normalizeFeederRequest({
    limit: "1",
    scan_limit: "10",
  });

  assert.equal(normalized.limit, 1, "limit should parse string '1' to number 1");
  assert.equal(normalized.scan_limit, 10, "scan_limit should parse string '10' to number 10");
  assert.equal(normalized.limit_was_provided, true);
  assert.equal(normalized.scan_limit_was_provided, true);
});

test("normalizeFeederRequest query-param values override body defaults when merged before normalizing", () => {
  // Simulate what POST handler does: body is baseline, query params override
  const body = { limit: "500", scan_limit: "5000", dry_run: "false" };
  const query_overrides = { limit: "1", scan_limit: "10" };
  const merged = { ...body, ...query_overrides };

  const normalized = normalizeFeederRequest(merged);

  assert.equal(normalized.limit, 1, "query override limit=1 should win over body limit=500");
  assert.equal(normalized.scan_limit, 10, "query override scan_limit=10 should win over body scan_limit=5000");
  assert.equal(normalized.dry_run, false);
});

test("normalizeFeederRequest preserves dry_run=true from query string 'true'", () => {
  const normalized = normalizeFeederRequest({ limit: "1", dry_run: "true" });
  assert.equal(normalized.dry_run, true);
});

test("normalizeFeederRequest preserves dry_run=false from query string 'false'", () => {
  const normalized = normalizeFeederRequest({ limit: "1", dry_run: "false" });
  assert.equal(normalized.dry_run, false);
});

test("runFeederWithRollout passes explicit limit=1 from request to executeRunImpl", async () => {
  const { execute_calls, deps } = makeDeps();

  await runFeederWithRollout({ limit: 1, scan_limit: 10 }, deps);

  assert.equal(execute_calls.length, 1);
  assert.equal(execute_calls[0].limit, 1, "executeRun should receive limit=1");
  assert.equal(execute_calls[0].scan_limit, 10, "executeRun should receive scan_limit=10");
});

// ---------------------------------------------------------------------------
// Completion summary log — feeder diagnostics patch
// ---------------------------------------------------------------------------

test("runFeederWithRollout completion log includes loaded_count, inserted_count, duplicate_count", async () => {
  const { entries, deps } = makeDeps({
    executeRunResult: {
      ok: true,
      source: { view_id: null, view_name: "SMS / TIER #1 / ALL" },
      scanned_count: 8,
      raw_items_pulled: 12,
      eligible_owner_count: 5,
      queued_count: 3,
      duplicate_skip_count: 2,
      queue_create_duplicate_cancel_count: 1,
      skipped_count: 2,
      skip_reason_counts: [
        { reason: "already_queued", count: 2 },
      ],
      template_resolution_diagnostics: null,
      queue_create_attempt_count: 4,
      queue_create_success_count: 3,
      results: [],
      queued_owner_ids: [],
    },
  });

  await runFeederWithRollout({ limit: 1, scan_limit: 10 }, deps);

  const completed = entries.find((e) => e.event === "master_owner_feeder.completed")?.meta;
  assert.ok(completed, "master_owner_feeder.completed must be emitted");

  assert.equal(completed.loaded_count, 12, "loaded_count should equal raw_items_pulled");
  assert.equal(completed.eligible_count, 5);
  assert.equal(completed.inserted_count, 3, "inserted_count should equal queued_count");
  assert.equal(completed.duplicate_count, 3, "duplicate_count = duplicate_skip_count + queue_create_duplicate_cancel_count");
  assert.equal(completed.skipped_count, 2);
  assert.equal(completed.effective_limit, 1);
  assert.equal(completed.effective_scan_limit, 10);
});

test("runFeederWithRollout completion log includes first_10_skip_reasons", async () => {
  const skip_reason_counts = [
    { reason: "already_queued", count: 4 },
    { reason: "no_phone", count: 1 },
  ];

  const { entries, deps } = makeDeps({
    executeRunResult: {
      ok: true,
      source: { view_id: null, view_name: "SMS / TIER #1 / ALL" },
      scanned_count: 5,
      raw_items_pulled: 5,
      eligible_owner_count: 2,
      queued_count: 0,
      duplicate_skip_count: 4,
      queue_create_duplicate_cancel_count: 0,
      skipped_count: 5,
      skip_reason_counts,
      template_resolution_diagnostics: null,
      queue_create_attempt_count: 0,
      queue_create_success_count: 0,
      results: [],
      queued_owner_ids: [],
    },
  });

  await runFeederWithRollout({}, deps);

  const completed = entries.find((e) => e.event === "master_owner_feeder.completed")?.meta;
  assert.ok(completed, "master_owner_feeder.completed must be emitted");
  assert.deepEqual(
    completed.first_10_skip_reasons,
    skip_reason_counts,
    "first_10_skip_reasons should include the skip reason counts"
  );
});

test("runFeederWithRollout completion log includes supabase_insert_summary", async () => {
  const { entries, deps } = makeDeps({
    executeRunResult: {
      ok: true,
      source: { view_id: null, view_name: "SMS / TIER #1 / ALL" },
      scanned_count: 3,
      raw_items_pulled: 3,
      eligible_owner_count: 3,
      queued_count: 2,
      duplicate_skip_count: 0,
      queue_create_duplicate_cancel_count: 1,
      skipped_count: 1,
      skip_reason_counts: [],
      template_resolution_diagnostics: null,
      queue_create_attempt_count: 3,
      queue_create_success_count: 2,
      results: [],
      queued_owner_ids: [],
    },
  });

  await runFeederWithRollout({}, deps);

  const completed = entries.find((e) => e.event === "master_owner_feeder.completed")?.meta;
  assert.ok(completed, "master_owner_feeder.completed must be emitted");

  assert.deepEqual(completed.supabase_insert_summary, {
    attempted: 3,
    succeeded: 2,
    duplicate_canceled: 1,
  });
});

test("runFeederWithRollout completion log includes first_10_errors for failed results", async () => {
  const { entries, deps } = makeDeps({
    executeRunResult: {
      ok: true,
      source: { view_id: null, view_name: "SMS / TIER #1 / ALL" },
      scanned_count: 2,
      raw_items_pulled: 2,
      eligible_owner_count: 2,
      queued_count: 1,
      duplicate_skip_count: 0,
      queue_create_duplicate_cancel_count: 0,
      skipped_count: 0,
      skip_reason_counts: [],
      template_resolution_diagnostics: null,
      queue_create_attempt_count: 2,
      queue_create_success_count: 1,
      results: [
        { ok: true, skipped: false, plan: { master_owner_id: 100 } },
        { ok: false, skipped: false, reason: "template_not_found", plan: { master_owner_id: 200 } },
      ],
      queued_owner_ids: [100],
    },
  });

  await runFeederWithRollout({}, deps);

  const completed = entries.find((e) => e.event === "master_owner_feeder.completed")?.meta;
  assert.ok(completed, "master_owner_feeder.completed must be emitted");
  assert.equal(completed.first_10_errors.length, 1);
  assert.equal(completed.first_10_errors[0].reason, "template_not_found");
  assert.equal(completed.first_10_errors[0].master_owner_id, 200);
});
