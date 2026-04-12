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
  assert.equal(result.rollout.effective_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
  assert.equal(result.rollout.resolved_source_view_id, 61752339);
  assert.equal(result.rollout.resolved_source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);

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
