import test from "node:test";
import assert from "node:assert/strict";

import ENV from "@/lib/config/env.js";
import {
  DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
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

test("feeder view scope allows the Tier 1 ALL feeder view", () => {
  const result = resolveFeederViewScope({
    requested_view_name: DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
  });

  assert.equal(result.ok, true);
  assert.equal(result.enforced, false);
  assert.equal(result.safe_scope_passed, true);
  assert.equal(result.reason, "feeder_view_safe_scope_applied");
  assert.equal(result.source_view_id, null);
  assert.equal(result.source_view_name, DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME);
});

test("feeder view scope allows Tier 1 file feeder views", () => {
  const file_one = resolveFeederViewScope({
    requested_view_name: "SMS / TIER #1 / FILE #1",
  });
  const file_nine = resolveFeederViewScope({
    requested_view_name: "SMS / TIER #1 / FILE #9",
  });

  assert.equal(file_one.ok, true);
  assert.equal(file_one.safe_scope_passed, true);
  assert.equal(file_one.source_view_name, "SMS / TIER #1 / FILE #1");

  assert.equal(file_nine.ok, true);
  assert.equal(file_nine.safe_scope_passed, true);
  assert.equal(file_nine.source_view_name, "SMS / TIER #1 / FILE #9");
});

test("feeder view scope blocks unknown feeder views", () => {
  const result = resolveFeederViewScope({
    requested_view_name: "SMS / TIER #2 / ALL",
  });

  assert.equal(result.ok, false);
  assert.equal(result.safe_scope_passed, false);
  assert.equal(result.reason, "feeder_view_outside_safe_scope");
  assert.equal(result.source_view_id, null);
  assert.equal(result.source_view_name, null);
});

test("configured rollout feeder views remain allowed when explicitly requested", () => {
  const original = {
    ROLLOUT_FEEDER_VIEW_ONLY_ID: ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID,
    ROLLOUT_FEEDER_VIEW_ONLY_NAME: ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME,
  };

  try {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = "123";
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = "Launch Sellers";

    const result = resolveFeederViewScope({
      requested_view_name: "Launch Sellers",
    });

    assert.equal(result.ok, true);
    assert.equal(result.safe_scope_passed, true);
    assert.equal(result.source_view_id, "123");
    assert.equal(result.source_view_name, "Launch Sellers");
  } finally {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = original.ROLLOUT_FEEDER_VIEW_ONLY_ID;
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = original.ROLLOUT_FEEDER_VIEW_ONLY_NAME;
  }
});

test("feeder view scope hard-clamps null-request defaults to Tier 1 ALL", () => {
  const original = {
    ROLLOUT_FEEDER_VIEW_ONLY_ID: ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID,
    ROLLOUT_FEEDER_VIEW_ONLY_NAME: ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME,
  };

  try {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = "";
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = "SMS / TIER #1 / FILE #1";

    const default_result = resolveFeederViewScope();
    const override_result = resolveFeederViewScope({
      requested_view_name: "SMS / TIER #1 / FILE #1",
    });

    assert.equal(default_result.ok, true);
    assert.equal(default_result.source_view_name, "SMS / TIER #1 / ALL");
    assert.equal(default_result.reason, "feeder_view_default_applied");
    assert.equal(override_result.ok, true);
    assert.equal(override_result.source_view_name, "SMS / TIER #1 / FILE #1");
    assert.equal(override_result.reason, "feeder_view_safe_scope_applied");
  } finally {
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_ID = original.ROLLOUT_FEEDER_VIEW_ONLY_ID;
    ENV.ROLLOUT_FEEDER_VIEW_ONLY_NAME = original.ROLLOUT_FEEDER_VIEW_ONLY_NAME;
  }
});

test("rollout caps clamp batch sizes to safe configured ceilings", () => {
  assert.equal(capQueueBatch(999, 50), 50);
  assert.equal(capBuyerBlastRecipients(42, 5), 5);
});
