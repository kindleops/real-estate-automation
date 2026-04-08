import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeFeederRequest,
  runFeederWithRollout,
} from "@/lib/domain/master-owners/feed-master-owners-request.js";
import { DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME } from "@/lib/config/rollout-controls.js";

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
  assert.equal(execute_calls[0].limit, 25);
  assert.equal(execute_calls[0].scan_limit, 150);
  assert.equal(execute_calls[0].source_view_name, "SMS / TIER #1 / FILE #1");
});

test("normalizeFeederRequest maps zero values to feeder defaults", () => {
  const normalized = normalizeFeederRequest({
    limit: 0,
    scan_limit: 0,
  });

  assert.equal(normalized.limit, 25);
  assert.equal(normalized.scan_limit, 150);
});
