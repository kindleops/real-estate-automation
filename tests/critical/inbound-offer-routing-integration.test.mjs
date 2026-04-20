import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  createInMemoryIdempotencyLedger,
  createPodioItem,
  categoryField,
  numberField,
} from "../helpers/test-helpers.js";

process.env.PODIO_CLIENT_ID ||= "test";
process.env.PODIO_CLIENT_SECRET ||= "test";
process.env.PODIO_USERNAME ||= "test";
process.env.PODIO_PASSWORD ||= "test";
process.env.INTERNAL_API_SECRET ||= "test";
process.env.BUYER_WEBHOOK_SECRET ||= "test";
process.env.OPS_DASHBOARD_SECRET ||= "test";
process.env.APP_BASE_URL ||= "http://localhost:3000";

const {
  handleTextgridInboundWebhook,
  __setTextgridInboundTestDeps,
  __resetTextgridInboundTestDeps,
} = await import("@/lib/flows/handle-textgrid-inbound.js");

afterEach(() => {
  __resetTextgridInboundTestDeps();
});

function buildContext({ property_id = 41, units = 1 } = {}) {
  const property_item = property_id
    ? createPodioItem(property_id, {
        "property-type": categoryField(units >= 5 ? "Multifamily" : "Single Family"),
        "property-class": categoryField(units >= 5 ? "Multifamily" : "Residential"),
        "number-of-units": numberField(units),
      })
    : null;

  return {
    found: true,
    ids: {
      brain_item_id: 11,
      master_owner_id: 21,
      prospect_id: 31,
      property_id,
      phone_item_id: 51,
      market_id: 61,
      assigned_agent_id: 71,
    },
    items: {
      brain_item: createPodioItem(11),
      phone_item: createPodioItem(51),
      master_owner_item: createPodioItem(21),
      prospect_item: createPodioItem(31),
      property_item,
      agent_item: createPodioItem(71),
    },
    summary: {
      conversation_stage: "Ownership Confirmation",
      language_preference: "English",
      property_address: property_id ? "123 Main St" : null,
      property_type: units >= 5 ? "Multifamily" : "Single Family",
      deal_strategy: "cash",
    },
    recent: {
      recent_events: [],
    },
  };
}

function installDeps({
  context = buildContext(),
  classification = { source: "test", objection: null, compliance_flag: null },
  route = { stage: "Offer", use_case: "ownership_check", deal_strategy: "cash" },
  offerRouting = { ok: true, offer_route: "no_offer_signal", reason: "none", meta: {} },
} = {}) {
  const ledger = createInMemoryIdempotencyLedger();
  const calls = {
    maybeCreateOfferFromContext: [],
    maybeQueueSellerStageReply: [],
    routeInboundOffer: [],
    transferDealToUnderwriting: [],
  };

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async () => context,
    classify: async () => classification,
    resolveRoute: () => route,
    routeInboundOffer: async (args) => {
      calls.routeInboundOffer.push(args);
      return offerRouting;
    },
    logInboundMessageEvent: async () => ({ item_id: 901 }),
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    createBrain: async () => createPodioItem(88),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async (args) => {
      calls.maybeCreateOfferFromContext.push(args);
      return { ok: true, created: false, reason: "stubbed" };
    },
    transferDealToUnderwriting: async (args) => {
      calls.transferDealToUnderwriting.push(args);
      return { ok: true, underwriting_item_id: 777, diagnostics: {} };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: false }),
    maybeQueueSellerStageReply: async (args) => {
      calls.maybeQueueSellerStageReply.push(args);
      return {
        ok: true,
        handled: true,
        queued: true,
        reason: "seller_flow_reply_queued",
        plan: {
          selected_use_case: args.explicit_use_case || "ownership_check",
          detected_intent: "Offer Request",
        },
        brain_stage: args.explicit_use_case || "Ownership Confirmation",
      };
    },
    maybeQueueUnderwritingFollowUp: async () => ({ ok: true, queued: false, reason: "not_needed" }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ ok: true, reason: "pipeline_not_created" }),
    extractUnderwritingSignals: () => ({ has_underwriting_signal: false }),
    buildInboundConversationState: () => ({ follow_up_trigger_state: "AI Running", conversation_stage: "Offer" }),
    isNegativeReply: () => false,
    cancelPendingQueueItemsForOwner: async () => ({ canceled_count: 0, items_checked: 0 }),
  });

  return calls;
}

async function sendInbound(body = "how much would you pay") {
  return handleTextgridInboundWebhook({
    message_id: `msg-${Math.random().toString(36).slice(2)}`,
    from: "+15550000001",
    to: "+15550000002",
    body,
    status: "received",
  });
}

test("how much would you pay + SFH snapshot queues offer_reveal_cash and includes snapshot id", async () => {
  const calls = installDeps({
    offerRouting: {
      ok: true,
      offer_route: "sfh_cash_preview",
      reason: "active_cash_snapshot_found",
      meta: { snapshot_id: "snap-123", cash_offer: 215000 },
    },
  });

  const result = await sendInbound("how much would you pay");

  assert.equal(result.ok, true);
  assert.equal(result.offer_routing.offer_route, "sfh_cash_preview");
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
  assert.equal(calls.maybeQueueSellerStageReply.length, 1);
  assert.equal(calls.maybeQueueSellerStageReply[0].explicit_use_case, "offer_reveal_cash");
  assert.equal(calls.maybeQueueSellerStageReply[0].cash_offer_snapshot_id, "snap-123");
  assert.equal(calls.maybeQueueSellerStageReply[0].extra_queue_context.cash_offer_amount, 215000);
});

test("offer message does not create Podio Offer immediately", async () => {
  const calls = installDeps({
    offerRouting: {
      ok: true,
      offer_route: "sfh_cash_preview",
      reason: "active_cash_snapshot_found",
      meta: { snapshot_id: "snap-321", cash_offer: 199000 },
    },
  });

  const result = await sendInbound("what would you pay");

  assert.equal(result.ok, true);
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
  assert.equal(result.offer?.created, false);
  assert.match(result.offer?.reason || "", /deferred/);
});

test("sent offer path still defers Offer creation to the post-send sync hook path", async () => {
  const calls = installDeps({
    offerRouting: {
      ok: true,
      offer_route: "sfh_cash_preview",
      reason: "active_cash_snapshot_found",
      meta: { snapshot_id: "snap-sync", cash_offer: 207500 },
    },
  });

  await sendInbound("how much can you pay");

  const queueArgs = calls.maybeQueueSellerStageReply[0];
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
  assert.equal(queueArgs.cash_offer_snapshot_id, "snap-sync");
  assert.equal(queueArgs.explicit_use_case, "offer_reveal_cash");
});

test("8 units routes to underwriting and never queues cash offer", async () => {
  const calls = installDeps({
    context: buildContext({ units: 8 }),
    offerRouting: {
      ok: true,
      offer_route: "underwriting",
      reason: "multifamily_property_signal",
      meta: { underwriting_reason: "multifamily_property_signal" },
    },
  });

  const result = await sendInbound("it is 8 units");

  assert.equal(result.ok, true);
  assert.equal(result.diagnostics.underwriting_route_reason, "multifamily_property_signal");
  assert.equal(calls.transferDealToUnderwriting.length, 1);
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
  assert.notEqual(calls.maybeQueueSellerStageReply[0]?.explicit_use_case, "offer_reveal_cash");
});

test("seller finance routes to underwriting and never queues cash offer", async () => {
  const calls = installDeps({
    offerRouting: {
      ok: true,
      offer_route: "underwriting",
      reason: "creative-finance_signal",
      meta: { underwriting_reason: "creative-finance_signal" },
    },
  });

  const result = await sendInbound("I want seller finance");

  assert.equal(result.ok, true);
  assert.equal(calls.transferDealToUnderwriting.length, 1);
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
  assert.notEqual(calls.maybeQueueSellerStageReply[0]?.explicit_use_case, "offer_reveal_cash");
});

test("no snapshot + property known queues condition clarifier", async () => {
  const calls = installDeps({
    offerRouting: {
      ok: true,
      offer_route: "condition_clarifier",
      reason: "no_snapshot_property_id_present",
      meta: { property_id: 41 },
    },
  });

  const result = await sendInbound("what is your offer");

  assert.equal(result.ok, true);
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
  assert.equal(calls.maybeQueueSellerStageReply.length, 1);
  assert.equal(calls.maybeQueueSellerStageReply[0].explicit_use_case, "ask_condition_clarifier");
});

test("no snapshot + no property routes manual review with no auto-send", async () => {
  const calls = installDeps({
    context: buildContext({ property_id: null }),
    offerRouting: {
      ok: true,
      offer_route: "manual_review",
      reason: "no_snapshot_no_property_id",
      meta: {},
    },
  });

  const result = await sendInbound("how much would you pay");

  assert.equal(result.ok, true);
  assert.equal(calls.maybeQueueSellerStageReply.length, 0);
  assert.equal(result.seller_stage_reply.queued, false);
  assert.equal(result.seller_stage_reply.reason, "offer_manual_review_no_auto_send");
  assert.equal(calls.maybeCreateOfferFromContext.length, 0);
});

test("wrong number and stop still suppress and bypass offer route", async () => {
  const wrongNumberCalls = installDeps({
    route: { stage: "Ownership", use_case: "wrong_person" },
    classification: { source: "test", compliance_flag: null },
  });

  const wrongNumberResult = await sendInbound("wrong number");
  assert.equal(wrongNumberResult.ok, true);
  assert.equal(wrongNumberCalls.routeInboundOffer.length, 0);

  const stopCalls = installDeps({
    classification: { source: "test", compliance_flag: "stop_texting" },
    route: { stage: "Ownership", use_case: "stop_or_opt_out" },
  });

  const stopResult = await sendInbound("stop");
  assert.equal(stopResult.ok, true);
  assert.equal(stopCalls.routeInboundOffer.length, 0);
});