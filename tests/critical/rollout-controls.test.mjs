import test from "node:test";
import assert from "node:assert/strict";

import ENV from "@/lib/config/env.js";
import {
  capBuyerBlastRecipients,
  capQueueBatch,
  resolveFeederViewScope,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";

test("beta rollout mode forces mutation paths into dry run by default", () => {
  const result = resolveMutationDryRun({
    requested_dry_run: false,
  });

  assert.equal(result.mode, "beta");
  assert.equal(result.effective_dry_run, true);
  assert.equal(result.reason, "rollout_beta_mode_forced_dry_run");
});

test("scoped ids reject requests outside configured safe scope", () => {
  const result = resolveScopedId({
    requested_id: 456,
    safe_id: 123,
    resource: "contract",
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "contract_outside_safe_scope");
  assert.equal(result.effective_id, 123);
});

test("feeder view scope honors the configured rollout view in dry-run and live runs", () => {
  const original = {
    ROLLOUT_FEEDER_VIEW_ONLY_ID: ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID,
    ROLLOUT_FEEDER_VIEW_ONLY_NAME: ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME,
    FEEDER_SOURCE_VIEW_DEFAULT_ID: ENV.FEEDER_SOURCE_VIEW_DEFAULT_ID,
    FEEDER_SOURCE_VIEW_DEFAULT_NAME: ENV.FEEDER_SOURCE_VIEW_DEFAULT_NAME,
  };

  try {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = "123";
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = "Launch Sellers";

    const dry_run_result = resolveFeederViewScope();
    const live_result = resolveFeederViewScope();

    assert.equal(dry_run_result.ok, true);
    assert.equal(dry_run_result.enforced, true);
    assert.equal(dry_run_result.reason, "feeder_view_safe_scope_applied");
    assert.equal(dry_run_result.source_view_id, "123");
    assert.equal(dry_run_result.source_view_name, "Launch Sellers");

    assert.equal(live_result.ok, true);
    assert.equal(live_result.enforced, true);
    assert.equal(live_result.reason, "feeder_view_safe_scope_applied");
    assert.equal(live_result.source_view_id, "123");
    assert.equal(live_result.source_view_name, "Launch Sellers");
  } finally {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = original.ROLLOUT_FEEDER_VIEW_ONLY_ID;
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = original.ROLLOUT_FEEDER_VIEW_ONLY_NAME;
    ENV.FEEDER_SOURCE_VIEW_DEFAULT_ID = original.FEEDER_SOURCE_VIEW_DEFAULT_ID;
    ENV.FEEDER_SOURCE_VIEW_DEFAULT_NAME = original.FEEDER_SOURCE_VIEW_DEFAULT_NAME;
  }
});

test("feeder view scope applies the default ALL view unless an explicit override is provided", () => {
  const original = {
    ROLLOUT_FEEDER_VIEW_ONLY_ID: ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID,
    ROLLOUT_FEEDER_VIEW_ONLY_NAME: ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME,
    FEEDER_SOURCE_VIEW_DEFAULT_NAME: ENV.FEEDER_SOURCE_VIEW_DEFAULT_NAME,
  };

  try {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = "";
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = "";
    ENV.FEEDER_SOURCE_VIEW_DEFAULT_NAME = "SMS / TIER #1 / ALL";

    const default_result = resolveFeederViewScope();
    const override_result = resolveFeederViewScope({
      requested_view_name: "SMS / TIER #1 / FILE #1",
    });

    assert.equal(default_result.ok, true);
    assert.equal(default_result.source_view_name, "SMS / TIER #1 / ALL");
    assert.equal(default_result.reason, "default_feeder_view_applied");
    assert.equal(override_result.ok, true);
    assert.equal(override_result.source_view_name, "SMS / TIER #1 / FILE #1");
    assert.equal(override_result.reason, "no_view_scope_configured");
  } finally {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = original.ROLLOUT_FEEDER_VIEW_ONLY_ID;
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = original.ROLLOUT_FEEDER_VIEW_ONLY_NAME;
    ENV.FEEDER_SOURCE_VIEW_DEFAULT_NAME = original.FEEDER_SOURCE_VIEW_DEFAULT_NAME;
  }
});

test("rollout caps clamp batch sizes to safe configured ceilings", () => {
  assert.equal(capQueueBatch(999, 50), 50);
  assert.equal(capBuyerBlastRecipients(42, 5), 5);
});
