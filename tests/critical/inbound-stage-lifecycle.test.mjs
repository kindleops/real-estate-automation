import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  handleTextgridInboundWebhook,
  __setTextgridInboundTestDeps,
  __resetTextgridInboundTestDeps,
} from "@/lib/flows/handle-textgrid-inbound.js";
import {
  createInMemoryIdempotencyLedger,
  createPodioItem,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetTextgridInboundTestDeps();
});

function buildContext({ brain_item = null } = {}) {
  return {
    found: true,
    ids: {
      brain_item_id: brain_item?.item_id || null,
      master_owner_id: 21,
      prospect_id: 31,
      property_id: 41,
      phone_item_id: 51,
    },
    items: {
      brain_item,
      phone_item: createPodioItem(51),
      master_owner_item: createPodioItem(21),
      property_item: createPodioItem(41),
    },
    summary: {
      conversation_stage: "Ownership Confirmation",
      language_preference: "English",
    },
  };
}

function installInboundDeps({
  context = buildContext(),
  resolveRoute = () => ({
    stage: "Ownership",
    use_case: "ownership_check",
    seller_profile: null,
  }),
  maybeQueueSellerStageReply = async () => ({
    ok: true,
    handled: false,
    queued: false,
    reason: "seller_flow_not_handled",
    plan: {
      selected_use_case: null,
      detected_intent: null,
    },
  }),
  createBrain = async () => null,
  updateBrainAfterInbound = async () => ({ ok: true }),
  updateBrainStage = async () => ({ ok: true }),
  syncPipelineState = async () => ({ ok: true, reason: "pipeline_not_created" }),
} = {}) {
  const ledger = createInMemoryIdempotencyLedger();
  const load_context_calls = [];

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async (args) => {
      load_context_calls.push(args);
      return context;
    },
    createBrain,
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute,
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound,
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage,
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => ({ ok: true, created: false }),
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: false }),
    maybeQueueSellerStageReply,
    maybeQueueUnderwritingFollowUp: async () => ({ ok: true, queued: false }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState,
  });

  return {
    load_context_calls,
  };
}

test("inbound webhook passes create_brain_if_missing: true to loadContext", async () => {
  let create_brain_count = 0;
  let update_brain_count = 0;
  let sync_pipeline_args = null;

  const { load_context_calls } = installInboundDeps({
    createBrain: async () => {
      create_brain_count += 1;
      return createPodioItem(77);
    },
    updateBrainAfterInbound: async () => {
      update_brain_count += 1;
      return { ok: true };
    },
    syncPipelineState: async (args) => {
      sync_pipeline_args = args;
      return { ok: true, reason: "pipeline_not_created" };
    },
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-no-brain",
    from: "+15550000001",
    to: "+15550000002",
    body: "Who is this?",
    status: "received",
  });

  assert.equal(result.ok, true);
  // Brain creation is now delegated to loadContext via create_brain_if_missing: true.
  // The handler no longer gates brain creation to Stage 1 — loadContext creates
  // the brain eagerly when the phone record resolves to a master owner.
  assert.equal(load_context_calls[0]?.create_brain_if_missing, true);
  // The mock loadContext doesn't actually call createBrain, and the mock context
  // has no brain, so shouldCreateBrainForInbound controls the post-queue create.
  // For "Who is this?" the plan does not have "Ownership Confirmed" intent, so
  // the narrow shouldCreateBrainForInbound gate still returns false.
  assert.equal(create_brain_count, 0);
  assert.equal(sync_pipeline_args?.create_if_missing, false);
});

test("inbound webhook creates the brain only after Stage 1 owner confirmation and still defers pipeline creation", async () => {
  let created_brain_args = null;
  const update_brain_calls = [];
  const stage_update_calls = [];
  let sync_pipeline_args = null;

  const result = await (async () => {
    installInboundDeps({
      maybeQueueSellerStageReply: async () => ({
        ok: true,
        handled: true,
        queued: true,
        reason: "seller_flow_reply_queued",
        brain_stage: "Offer Interest Confirmation",
        plan: {
          selected_use_case: "consider_selling",
          selected_variant_group: "Stage 2 Consider Selling",
          detected_intent: "Ownership Confirmed",
        },
      }),
      createBrain: async (args) => {
        created_brain_args = args;
        return createPodioItem(77);
      },
      updateBrainAfterInbound: async (args) => {
        update_brain_calls.push(args);
        return { ok: true };
      },
      updateBrainStage: async (args) => {
        stage_update_calls.push(args);
        return { ok: true };
      },
      syncPipelineState: async (args) => {
        sync_pipeline_args = args;
        return { ok: true, reason: "pipeline_not_created" };
      },
    });

    return handleTextgridInboundWebhook({
      message_id: "sms-stage-1-yes",
      from: "+15550000001",
      to: "+15550000002",
      body: "Yes, I own it.",
      status: "received",
    });
  })();

  assert.equal(result.ok, true);
  assert.equal(created_brain_args?.master_owner_id, 21);
  assert.equal(created_brain_args?.prospect_id, 31);
  assert.equal(created_brain_args?.property_id, 41);
  assert.equal(created_brain_args?.phone_item_id, 51);
  assert.deepEqual(
    update_brain_calls.map((entry) => entry.brain_id),
    [77]
  );
  assert.deepEqual(stage_update_calls, [
    { brain_id: 77, stage: "Offer Interest Confirmation" },
  ]);
  assert.equal(sync_pipeline_args?.conversation_item_id, 77);
  assert.equal(sync_pipeline_args?.create_if_missing, false);
});

test("inbound webhook allows pipeline creation only after Stage 2 offer-interest confirmation", async () => {
  let create_brain_count = 0;
  const update_brain_calls = [];
  let sync_pipeline_args = null;

  const brain_item = createPodioItem(11);

  installInboundDeps({
    context: buildContext({ brain_item }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "consider_selling",
      seller_profile: null,
    }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      handled: true,
      queued: true,
      reason: "seller_flow_reply_queued",
      brain_stage: "Seller Price Discovery",
      plan: {
        selected_use_case: "asking_price",
        selected_variant_group: "Stage 3 Asking Price",
        detected_intent: "Open to Selling",
      },
    }),
    createBrain: async () => {
      create_brain_count += 1;
      return createPodioItem(88);
    },
    updateBrainAfterInbound: async (args) => {
      update_brain_calls.push(args);
      return { ok: true };
    },
    syncPipelineState: async (args) => {
      sync_pipeline_args = args;
      return { ok: true, pipeline_item_id: 61, current_stage: "Offer" };
    },
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-stage-2-yes",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I'd consider an offer.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(create_brain_count, 0);
  assert.deepEqual(
    update_brain_calls.map((entry) => entry.brain_id),
    [11]
  );
  assert.equal(sync_pipeline_args?.conversation_item_id, 11);
  assert.equal(sync_pipeline_args?.create_if_missing, true);
});
