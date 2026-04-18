import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  acquireRunLock,
  __setRunLockTestDeps,
  __resetRunLockTestDeps,
} from "@/lib/domain/runs/run-locks.js";
import {
  beginIdempotentProcessing,
  __setIdempotencyLedgerTestDeps,
  __resetIdempotencyLedgerTestDeps,
} from "@/lib/domain/events/idempotency-ledger.js";
import {
  handleTextgridDeliveryWebhook,
  __setTextgridDeliveryTestDeps,
  __resetTextgridDeliveryTestDeps,
} from "@/lib/flows/handle-textgrid-delivery.js";
import {
  handleTextgridInboundWebhook,
  __setTextgridInboundTestDeps,
  __resetTextgridInboundTestDeps,
} from "@/lib/flows/handle-textgrid-inbound.js";
import { createInMemoryIdempotencyLedger, createPodioItem } from "../helpers/test-helpers.js";

afterEach(() => {
  __resetRunLockTestDeps();
  __resetIdempotencyLedgerTestDeps();
  __resetTextgridDeliveryTestDeps();
  __resetTextgridInboundTestDeps();
});

test("acquireRunLock uses runtime state storage instead of Message Events rows", async () => {
  let created_args = null;

  __setRunLockTestDeps({
    readRuntimeState: async () => null,
    createRuntimeStateIfAbsent: async (args) => {
      created_args = args;
      return { created: true, state: args.state };
    },
    writeRuntimeState: async () => {
      throw new Error("fresh run lock should not overwrite runtime state");
    },
    warn: () => {},
  });

  const result = await acquireRunLock({
    scope: "queue-run",
    owner: "test-runner",
  });

  assert.equal(result.ok, true);
  assert.equal(result.acquired, true);
  assert.equal(result.record_item_id, "run-locks:queue-run");
  assert.equal(created_args.namespace, "run-locks");
  assert.equal(created_args.key, "queue-run");
});

test("beginIdempotentProcessing stores claims in runtime state instead of Message Events", async () => {
  let created_args = null;

  __setIdempotencyLedgerTestDeps({
    readRuntimeState: async () => null,
    createRuntimeStateIfAbsent: async (args) => {
      created_args = args;
      return { created: true, state: args.state };
    },
    writeRuntimeState: async () => {
      throw new Error("fresh idempotency claim should not overwrite runtime state");
    },
  });

  const result = await beginIdempotentProcessing({
    scope: "textgrid_delivery",
    key: "delivery-abc-123",
    metadata: { provider: "textgrid" },
  });

  assert.equal(result.ok, true);
  assert.equal(result.duplicate, false);
  assert.equal(
    result.record_item_id,
    "idempotency:textgrid_delivery:delivery-abc-123"
  );
  assert.equal(created_args.namespace, "idempotency");
  assert.equal(created_args.key, "textgrid_delivery:delivery-abc-123");
});

test("delivery processing without outbound correlation does not create junk event rows", async () => {
  let update_called = 0;

  __setTextgridDeliveryTestDeps({
    findMessageEventItemsByProviderMessageId: async () => [],
    fetchAllItems: async () => [],
    getItem: async () => null,
    updateMessageEventStatus: async () => {
      update_called += 1;
      return { ok: true };
    },
    updateBrainAfterDelivery: async () => ({ ok: true }),
    info: () => {},
    warn: () => {},
  });

  const result = await handleTextgridDeliveryWebhook({
    message_id: "provider-missing",
    status: "delivered",
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "message_event_not_found");
  assert.equal(update_called, 0);
});

test("inbound webhook rehydrates the same seller event after late brain creation", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const logged_payloads = [];

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async () => ({
      found: true,
      ids: {
        brain_item_id: null,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
        assigned_agent_id: 61,
        market_id: 71,
      },
      items: {
        brain_item: null,
        phone_item: createPodioItem(51),
      },
      recent: {
        recent_events: [
          {
            direction: "Outbound",
            message_id: "outbound:queue-55",
            textgrid_number_item_id: 81,
          },
        ],
      },
      summary: {
        conversation_stage: "Ownership Confirmation",
        property_address: "123 Main St",
      },
    }),
    classify: async () => ({ language: "English", source: "test" }),
    resolveRoute: () => ({
      stage: "Ownership",
      use_case: "ownership_check",
      seller_profile: null,
    }),
    logInboundMessageEvent: async (payload) => {
      logged_payloads.push(payload);
      return { item_id: payload.record_item_id || 991 };
    },
    updateBrainAfterInbound: async () => ({ ok: true }),
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => ({ ok: true, created: false }),
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: false }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      handled: false,
      queued: true,
      brain_stage: "Offer Interest Confirmation",
      plan: {
        detected_intent: "Ownership Confirmed",
        selected_use_case: "consider_selling",
      },
    }),
    createBrain: async () => createPodioItem(777),
    maybeQueueUnderwritingFollowUp: async () => ({ ok: true, queued: false }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Ownership" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-late-brain-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I own it.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(logged_payloads.length, 2);
  assert.equal(logged_payloads[0].record_item_id, undefined);
  assert.equal(logged_payloads[0].conversation_item_id, null);
  assert.equal(logged_payloads[1].record_item_id, 991);
  assert.equal(logged_payloads[1].conversation_item_id, 777);
  assert.equal(logged_payloads[1].prior_message_id, "outbound:queue-55");
  assert.equal(logged_payloads[1].stage_after, "Offer Interest Confirmation");
});
