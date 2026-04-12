import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  __resetExecutionBrainTestDeps,
  __setExecutionBrainTestDeps,
  updateBrainFromExecution,
} from "@/lib/domain/brain/update-brain-from-execution.js";
import {
  __resetTitleRoutingStatusTestDeps,
  __setTitleRoutingStatusTestDeps,
  updateTitleRoutingStatus,
} from "@/lib/domain/title/update-title-routing-status.js";
import {
  __resetClosingStatusTestDeps,
  __setClosingStatusTestDeps,
  updateClosingStatus,
} from "@/lib/domain/closings/update-closing-status.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  numberField,
  textField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetExecutionBrainTestDeps();
  __resetTitleRoutingStatusTestDeps();
  __resetClosingStatusTestDeps();
});

test("updateBrainFromExecution resolves the contract-linked brain and writes stage 8 contract fields", async () => {
  let patched = null;

  __setExecutionBrainTestDeps({
    getBrainItem: async () =>
      createPodioItem(701, {
        "conversation-stage": categoryField("Negotiation"),
        number: numberField(6),
        "ai-route": categoryField("Negotiation"),
        "current-seller-state": categoryField("Negotiating"),
        "status-ai-managed": categoryField("Active Negotiation"),
        "follow-up-trigger-state": categoryField("AI Running"),
        title: textField("Stage 6 negotiation with above-range ask."),
      }),
    applyBrainStateUpdate: async ({ brain_id, fields }) => {
      patched = { brain_id, fields };
      return { ok: true, updated_fields: fields };
    },
  });

  const result = await updateBrainFromExecution({
    source: "contract",
    contract_item: createPodioItem(801, {
      conversation: appRefField(701),
    }),
    normalized_status: "Sent",
    notes: "DocuSign webhook processed: Sent.",
    now: new Date("2026-04-11T12:00:00.000Z"),
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, true);
  assert.equal(result.milestone, "contract_sent");
  assert.equal(patched.brain_id, 701);
  assert.equal(patched.fields["conversation-stage"], "Contract Out");
  assert.equal(patched.fields.number, 8);
  assert.equal(patched.fields["ai-route"], "Contract Push");
  assert.equal(patched.fields["current-seller-state"], "Ready For Contract");
  assert.equal(patched.fields["status-ai-managed"], "Waiting on Seller");
  assert.equal(patched.fields["follow-up-trigger-state"], "Waiting");
  assert.deepEqual(patched.fields["last-contact-timestamp"], {
    start: "2026-04-11 12:00:00",
  });
});

test("updateBrainFromExecution preserves DNC terminal states against execution noise", async () => {
  let patched = false;

  __setExecutionBrainTestDeps({
    resolveBrain: async () =>
      createPodioItem(702, {
        "conversation-stage": categoryField("Closed / Dead Outcome"),
        number: numberField(10),
        "current-seller-state": categoryField("DNC"),
        "status-ai-managed": categoryField("DNC"),
      }),
    applyBrainStateUpdate: async () => {
      patched = true;
      return { ok: true };
    },
  });

  const result = await updateBrainFromExecution({
    source: "closing",
    closing_item: createPodioItem(901, {
      "master-owner": appRefField(3001),
    }),
    closing_status: "Completed",
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, false);
  assert.equal(result.reason, "protected_terminal_state");
  assert.equal(patched, false);
});

test("updateTitleRoutingStatus bridges title milestones into the Brain authority layer", async () => {
  let brainArgs = null;
  let payload = null;

  __setTitleRoutingStatusTestDeps({
    updateTitleRoutingItem: async (_item_id, nextPayload) => {
      payload = nextPayload;
      return { ok: true };
    },
    syncPipelineState: async () => ({ current_stage: "Title" }),
    updateBrainFromExecution: async (args) => {
      brainArgs = args;
      return { ok: true, updated: true };
    },
  });

  const result = await updateTitleRoutingStatus({
    title_routing_item: createPodioItem(1001, {
      "title-file-status": categoryField("Routed"),
    }),
    status: "Opened",
    notes: "Title opened the file.",
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, true);
  assert.equal(payload["title-file-status"], "Opened");
  assert.equal(brainArgs.source, "title");
  assert.equal(brainArgs.routing_status, "Opened");
  assert.equal(brainArgs.title_routing_item.item_id, 1001);
});

test("updateClosingStatus bridges closing milestones into the Brain authority layer", async () => {
  let brainArgs = null;
  let payload = null;

  __setClosingStatusTestDeps({
    updateClosingItem: async (_item_id, nextPayload) => {
      payload = nextPayload;
      return { ok: true };
    },
    syncContractStatus: async () => ({ ok: true, updated: true }),
    updateTitleRoutingStatus: async () => ({ ok: true, updated: true }),
    createDealRevenueFromClosedClosing: async () => ({
      ok: true,
      created: true,
      deal_revenue_item_id: 3001,
    }),
    syncPipelineState: async () => ({ current_stage: "Closing" }),
    updateBrainFromExecution: async (args) => {
      brainArgs = args;
      return { ok: true, updated: true };
    },
  });

  const result = await updateClosingStatus({
    closing_item: createPodioItem(2001, {
      "closing-status": categoryField("Scheduled"),
    }),
    status: "Completed",
    notes: "Closing finished successfully.",
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, true);
  assert.equal(payload["closing-status"], "Completed");
  assert.equal(payload["closed-successfully"], "Yes");
  assert.ok(payload["actual-closing-date"]?.start);
  assert.equal(brainArgs.source, "closing");
  assert.equal(brainArgs.closing_status, "Completed");
  assert.equal(brainArgs.closing_item.item_id, 2001);
});
