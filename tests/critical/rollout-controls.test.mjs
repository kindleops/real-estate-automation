import test from "node:test";
import assert from "node:assert/strict";

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

test("feeder view scope bypasses one-view lock during dry-run diagnostics", () => {
  const result = resolveFeederViewScope({
    requested_view_id: "987",
    requested_view_name: "SMS Hot Leads",
    dry_run: true,
  });

  assert.equal(result.ok, true);
  assert.equal(result.enforced, false);
  assert.equal(result.reason, "dry_run_view_scope_bypassed");
  assert.equal(result.source_view_id, "987");
});

test("rollout caps clamp batch sizes to safe configured ceilings", () => {
  assert.equal(capQueueBatch(999, 50), 50);
  assert.equal(capBuyerBlastRecipients(42, 5), 5);
});
