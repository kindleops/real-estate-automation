import test from "node:test";
import assert from "node:assert/strict";

import {
  applyAlertOperatorActionMeta,
  getEffectiveAlertOperatorState,
} from "@/lib/domain/alerts/system-alerts.js";

test("alert operator actions acknowledge and preserve operator history", () => {
  const next = applyAlertOperatorActionMeta(
    {
      status: "open",
      operator_history: [],
    },
    {
      action: "acknowledge",
      actor: "ops@example.test",
      note: "Investigating queue drain failure",
      timestamp: "2026-04-01T12:00:00.000Z",
    }
  );

  assert.equal(next.operator_state, "acknowledged");
  assert.equal(next.acknowledged_by, "ops@example.test");
  assert.equal(next.acknowledged_note, "Investigating queue drain failure");
  assert.equal(next.operator_history.length, 1);
  assert.equal(next.operator_history[0].action, "acknowledge");
  assert.equal(getEffectiveAlertOperatorState(next, "2026-04-01T12:30:00.000Z"), "acknowledged");
});

test("alert operator actions silence and unsilence without changing resolved status semantics", () => {
  const silenced = applyAlertOperatorActionMeta(
    {
      status: "open",
      operator_history: [],
    },
    {
      action: "silence",
      actor: "ops@example.test",
      note: "Maintenance window",
      silenced_until: "2026-04-01T14:00:00.000Z",
      timestamp: "2026-04-01T12:00:00.000Z",
    }
  );

  assert.equal(silenced.operator_state, "silenced");
  assert.equal(
    getEffectiveAlertOperatorState(silenced, "2026-04-01T13:00:00.000Z"),
    "silenced"
  );
  assert.equal(
    getEffectiveAlertOperatorState(silenced, "2026-04-01T15:00:00.000Z"),
    "open"
  );

  const unsilenced = applyAlertOperatorActionMeta(silenced, {
    action: "unsilence",
    actor: "ops@example.test",
    note: "Window complete",
    timestamp: "2026-04-01T13:30:00.000Z",
  });

  assert.equal(unsilenced.operator_state, "open");
  assert.equal(unsilenced.silenced_until, null);
  assert.equal(unsilenced.operator_history.length, 2);
  assert.equal(unsilenced.operator_history[1].action, "unsilence");
});
