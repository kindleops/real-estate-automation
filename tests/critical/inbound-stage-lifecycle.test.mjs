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
  logInboundMessageEvent = async () => {},
  postInboundSmsDiscordCard = async () => ({ ok: true, discord_message_id: "discord-msg-1" }),
  findInboundAutopilotQueue = async () => null,
  buildInboundAutopilotSchedule = (delay_seconds = 60) => {
    const scheduled_for = new Date(Date.now() + delay_seconds * 1000).toISOString();
    return {
      scheduled_for,
      scheduled_for_utc: scheduled_for,
      scheduled_for_local: scheduled_for,
    };
  },
  getSupabaseClient = () => null,
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
    logInboundMessageEvent,
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
    postInboundSmsDiscordCard,
    findInboundAutopilotQueue,
    buildInboundAutopilotSchedule,
    getSupabaseClient,
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

test("inbound webhook defaults to delayed autopilot and still posts Discord control card", async () => {
  const seller_reply_calls = [];
  const card_calls = [];
  const logged_events = [];

  installInboundDeps({
    maybeQueueSellerStageReply: async (args) => {
      seller_reply_calls.push(args);
      if (args.preview_only) {
        return {
          ok: true,
          handled: true,
          queued: false,
          preview_only: true,
          reason: "seller_flow_reply_preview_ready",
          brain_stage: "Ownership Confirmation",
          plan: {
            selected_use_case: "ownership_check",
            detected_intent: "Ownership Confirmed",
          },
          preview_result: {
            rendered_message_text: "Suggested review reply",
            template_id: "ownership-template-1",
            selected_template_source: "seller_flow",
          },
        };
      }

      return {
        ok: true,
        handled: true,
        queued: true,
        preview_only: false,
        reason: "seller_flow_reply_queued",
        brain_stage: "Ownership Confirmation",
        plan: {
          selected_use_case: "ownership_check",
          detected_intent: "Ownership Confirmed",
        },
        queue_item_id: "queue-1",
        queue_result: {
          rendered_message_text: "Suggested review reply",
          template_id: "ownership-template-1",
          selected_template_source: "seller_flow",
        },
      };
    },
    logInboundMessageEvent: async (args) => {
      logged_events.push(args);
      return { item_id: args.record_item_id || "msg-event-1" };
    },
    postInboundSmsDiscordCard: async (args) => {
      card_calls.push(args);
      return { ok: true, discord_message_id: "discord-msg-1", channel_id: "chan-1", channel_key: "inbound_replies", fallback: false };
    },
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-review-default-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I own it.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(seller_reply_calls.length, 2);
  assert.equal(seller_reply_calls[0].preview_only, true);
  assert.equal(seller_reply_calls[1].preview_only, false);
  assert.equal(result.seller_stage_reply?.queued, true);
  assert.equal(card_calls.length, 1);
  assert.equal(card_calls[0].autopilot_enabled, true);
  assert.equal(card_calls[0].suggested_reply_preview, "Suggested review reply");
  assert.equal(logged_events.at(-1)?.metadata?.autopilot_reply, true);
  assert.equal(logged_events.at(-1)?.metadata?.discord_review_status, "autopilot_pending");
  assert.equal(logged_events.at(-1)?.metadata?.discord_message_id, "discord-msg-1");
});

test("inbound webhook skips delayed queue and marks manual review when autopilot is disabled", async () => {
  const seller_reply_calls = [];
  const card_calls = [];
  const logged_events = [];

  installInboundDeps({
    maybeQueueSellerStageReply: async (args) => {
      seller_reply_calls.push(args);
      return {
        ok: true,
        handled: true,
        queued: false,
        preview_only: true,
        reason: "seller_flow_reply_preview_ready",
        brain_stage: "Offer Interest Confirmation",
        plan: {
          selected_use_case: "consider_selling",
          detected_intent: "Open to Selling",
        },
        preview_result: {
          rendered_message_text: "Autopilot reply",
          template_id: "template-auto-1",
          selected_template_source: "seller_flow",
        },
      };
    },
    logInboundMessageEvent: async (args) => {
      logged_events.push(args);
      return { item_id: args.record_item_id || "msg-event-2" };
    },
    postInboundSmsDiscordCard: async (args) => {
      card_calls.push(args);
      return { ok: true, discord_message_id: "discord-msg-2", channel_id: "chan-1", channel_key: "inbound_replies", fallback: false };
    },
  });

  const result = await handleTextgridInboundWebhook(
    {
      message_id: "sms-autopilot-on-1",
      from: "+15550000001",
      to: "+15550000002",
      body: "Yes, I'd consider an offer.",
      status: "received",
    },
    {
      auto_reply_enabled: true,
    }
  );

  assert.equal(result.ok, true);
  assert.equal(seller_reply_calls[0]?.preview_only, true);
  assert.equal(result.seller_stage_reply?.queued, false);
  assert.equal(card_calls.length, 1);
  assert.equal(card_calls[0].autopilot_enabled, false);
  assert.equal(logged_events.at(-1)?.metadata?.discord_review_status, "manual_review_required");
});

test("inbound webhook still posts Discord review card when classification degrades", async () => {
  const card_calls = [];

  installInboundDeps({
    resolveRoute: () => {
      throw new Error("route resolution failed");
    },
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      handled: true,
      queued: false,
      preview_only: true,
      reason: "seller_flow_reply_preview_ready",
      plan: {
        selected_use_case: null,
        detected_intent: null,
      },
      preview_result: {
        rendered_message_text: "Fallback suggested reply",
      },
    }),
    logInboundMessageEvent: async (args) => ({ item_id: args.record_item_id || "msg-event-3" }),
    postInboundSmsDiscordCard: async (args) => {
      card_calls.push(args);
      return { ok: true, discord_message_id: "discord-msg-3", channel_id: "chan-1", channel_key: "inbound_replies", fallback: false };
    },
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-degraded-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Tell me more.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(result.classification?.source, "inbound_review_fallback");
  assert.equal(card_calls.length, 1);
  assert.equal(card_calls[0].suggested_reply_preview, "");
  assert.match(card_calls[0].safety_state || "", /manual review/i);
});

test("discord post failure does not block delayed autopilot queueing", async () => {
  const logged_events = [];

  installInboundDeps({
    maybeQueueSellerStageReply: async (args) => {
      if (args.preview_only) {
        return {
          ok: true,
          handled: true,
          queued: false,
          preview_only: true,
          plan: { selected_use_case: "ownership_check", detected_intent: "Ownership Confirmed" },
          preview_result: {
            rendered_message_text: "Queued despite card failure",
            template_id: "tmpl-card-fail",
            selected_template_source: "seller_flow",
          },
        };
      }

      return {
        ok: true,
        handled: true,
        queued: true,
        queue_item_id: "queue-card-fail",
        plan: { selected_use_case: "ownership_check", detected_intent: "Ownership Confirmed" },
        queue_result: {
          rendered_message_text: "Queued despite card failure",
          template_id: "tmpl-card-fail",
          selected_template_source: "seller_flow",
        },
      };
    },
    logInboundMessageEvent: async (args) => {
      logged_events.push(args);
      return { item_id: args.record_item_id || "msg-event-card-fail" };
    },
    postInboundSmsDiscordCard: async () => {
      throw new Error("discord unavailable");
    },
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-card-fail-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I own it.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(result.seller_stage_reply?.queued, true);
  assert.equal(logged_events.at(-1)?.metadata?.autopilot_reply, true);
  assert.equal(logged_events.at(-1)?.metadata?.discord_card_error, "discord unavailable");
});

test("idempotent replay does not duplicate autopilot queue or Discord card", async () => {
  const seller_reply_calls = [];
  const card_calls = [];

  installInboundDeps({
    maybeQueueSellerStageReply: async (args) => {
      seller_reply_calls.push(args);
      if (args.preview_only) {
        return {
          ok: true,
          handled: true,
          queued: false,
          preview_only: true,
          plan: { selected_use_case: "ownership_check", detected_intent: "Ownership Confirmed" },
          preview_result: { rendered_message_text: "Replay-safe reply" },
        };
      }

      return {
        ok: true,
        handled: true,
        queued: true,
        queue_item_id: "queue-replay-safe",
        plan: { selected_use_case: "ownership_check", detected_intent: "Ownership Confirmed" },
        queue_result: { rendered_message_text: "Replay-safe reply" },
      };
    },
    logInboundMessageEvent: async (args) => ({ item_id: args.record_item_id || "msg-event-replay-1" }),
    postInboundSmsDiscordCard: async (args) => {
      card_calls.push(args);
      return { ok: true, discord_message_id: "discord-msg-replay-1" };
    },
  });

  const payload = {
    message_id: "sms-replay-safe-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I own it.",
    status: "received",
  };

  const first = await handleTextgridInboundWebhook(payload);
  const second = await handleTextgridInboundWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(second.ok, true);
  assert.equal(seller_reply_calls.length, 2);
  assert.equal(card_calls.length, 1);
});
