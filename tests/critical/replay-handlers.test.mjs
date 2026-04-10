import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  handleTextgridInboundWebhook,
  __setTextgridInboundTestDeps,
  __resetTextgridInboundTestDeps,
} from "@/lib/flows/handle-textgrid-inbound.js";
import {
  handleTextgridDeliveryWebhook,
  __setTextgridDeliveryTestDeps,
  __resetTextgridDeliveryTestDeps,
} from "@/lib/flows/handle-textgrid-delivery.js";
import {
  handleDocusignWebhook,
  __setDocusignWebhookTestDeps,
  __resetDocusignWebhookTestDeps,
} from "@/lib/domain/contracts/handle-docusign-webhook.js";
import {
  handleTitleResponseWebhook,
  __setTitleWebhookTestDeps,
  __resetTitleWebhookTestDeps,
} from "@/lib/domain/title/handle-title-response-webhook.js";
import {
  handleClosingResponseWebhook,
  __setClosingWebhookTestDeps,
  __resetClosingWebhookTestDeps,
} from "@/lib/domain/closings/handle-closing-response-webhook.js";
import {
  appRefField,
  categoryField,
  createInMemoryIdempotencyLedger,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetTextgridInboundTestDeps();
  __resetTextgridDeliveryTestDeps();
  __resetDocusignWebhookTestDeps();
  __resetTitleWebhookTestDeps();
  __resetClosingWebhookTestDeps();
});

test("inbound webhook ignores replay after first completion", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let logInboundCount = 0;
  const inboundLogPayloads = [];
  let updateBrainAfterInboundCount = 0;
  let createOfferCount = 0;

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
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "offer_follow_up",
      seller_profile: "motivated",
    }),
    logInboundMessageEvent: async (payload) => {
      logInboundCount += 1;
      inboundLogPayloads.push(payload);
    },
    updateBrainAfterInbound: async () => {
      updateBrainAfterInboundCount += 1;
    },
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => {
      createOfferCount += 1;
      return { ok: true, created: false };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: true }),
    maybeQueueUnderwritingFollowUp: async () => ({ ok: true, queued: false }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const payload = {
    message_id: "sms-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "I am interested.",
    status: "received",
  };

  const first = await handleTextgridInboundWebhook(payload);
  const second = await handleTextgridInboundWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.duplicate, undefined);
  assert.equal(second.ok, true);
  assert.equal(second.duplicate, true);
  assert.equal(logInboundCount, 1);
  assert.equal(inboundLogPayloads[0].conversation_item_id, 11);
  assert.equal(updateBrainAfterInboundCount, 1);
  assert.equal(createOfferCount, 1);
});

test("inbound webhook suppresses underwriting follow-up when seller-stage reply is handled", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const stage_updates = [];
  let underwriting_follow_up_count = 0;

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
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Ownership",
      use_case: "ownership_check",
      seller_profile: null,
    }),
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async (payload) => {
      stage_updates.push(payload);
      return { ok: true };
    },
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => ({ ok: true, created: false }),
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: true }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      queued: true,
      handled: true,
      reason: "seller_flow_reply_queued",
      brain_stage: "Offer",
      plan: {
        selected_use_case: "consider_selling",
      },
    }),
    maybeQueueUnderwritingFollowUp: async () => {
      underwriting_follow_up_count += 1;
      return { ok: true, queued: true };
    },
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-stage-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I own it.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(result.seller_stage_reply.queued, true);
  assert.equal(result.underwriting_follow_up.reason, "suppressed_by_seller_stage_reply");
  assert.equal(underwriting_follow_up_count, 0);
  assert.deepEqual(
    stage_updates.map((entry) => entry.stage),
    ["Ownership", "Offer"]
  );
});

test("inbound webhook does not run a second offer pass when the initial offer already exists", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const create_offer_calls = [];

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
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "offer_reveal",
      seller_profile: "motivated",
    }),
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async (payload) => {
      create_offer_calls.push(payload);
      return { ok: true, created: true, offer: { offer_item_id: 901 } };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({
      ok: true,
      extracted: true,
      strategy: { auto_offer_ready: true },
      signals: { underwriting_auto_offer_ready: true },
    }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      queued: false,
      handled: false,
    }),
    maybeQueueUnderwritingFollowUp: async () => ({
      ok: true,
      queued: true,
      offer_ready: true,
    }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-offer-single-pass",
    from: "+15550000001",
    to: "+15550000002",
    body: "Make me an offer.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(create_offer_calls.length, 1);
  assert.equal(create_offer_calls[0].respect_underwriting_gate, undefined);
});

test("inbound webhook runs a single ungated second offer pass only after underwriting is ready", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const create_offer_calls = [];

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
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "offer_reveal",
      seller_profile: "motivated",
    }),
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async (payload) => {
      create_offer_calls.push(payload);
      if (create_offer_calls.length === 1) {
        return { ok: true, created: false, reason: "offer_requires_underwriting_first" };
      }
      return { ok: true, created: true, offer: { offer_item_id: 902 } };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({
      ok: true,
      extracted: true,
      strategy: { auto_offer_ready: false },
      signals: { underwriting_auto_offer_ready: false },
    }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      queued: false,
      handled: false,
    }),
    maybeQueueUnderwritingFollowUp: async () => ({
      ok: true,
      queued: true,
      offer_ready: true,
    }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-offer-second-pass",
    from: "+15550000001",
    to: "+15550000002",
    body: "Make me an offer.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(create_offer_calls.length, 2);
  assert.equal(create_offer_calls[0].respect_underwriting_gate, undefined);
  assert.equal(create_offer_calls[1].respect_underwriting_gate, false);
});

test("delivery webhook ignores replay after exact queue correlation succeeds", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const queueUpdates = [];
  const deliveryLogPayloads = [];
  let deliveryEventCount = 0;
  let eventStatusUpdateCount = 0;
  let brainDeliveryUpdateCount = 0;

  const outboundEvent = createPodioItem(801, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("provider-1"),
    "processed-by": categoryField("Scheduled Campaign"),
    "source-app": categoryField("Send Queue"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async (item_id, payload) => {
      queueUpdates.push({ item_id, payload });
    },
    logDeliveryEvent: async (payload) => {
      deliveryEventCount += 1;
      deliveryLogPayloads.push(payload);
    },
    updateMessageEventStatus: async () => {
      eventStatusUpdateCount += 1;
    },
    findLatestBrainByProspectId: async () => createPodioItem(701),
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => {
      throw new Error("should_not_update_phone");
    },
    updateBrainAfterDelivery: async () => {
      brainDeliveryUpdateCount += 1;
    },
    mapTextgridFailureBucket: () => "Soft Bounce",
  });

  const payload = {
    message_id: "provider-1",
    status: "delivered",
    client_reference_id: "queue-123",
  };

  const first = await handleTextgridDeliveryWebhook(payload);
  const second = await handleTextgridDeliveryWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.correlation_mode, "client_reference");
  assert.equal(second.ok, true);
  assert.equal(second.duplicate, true);
  assert.equal(queueUpdates.length, 1);
  assert.equal(deliveryEventCount, 1);
  assert.equal(deliveryLogPayloads[0].conversation_item_id, 701);
  assert.equal(deliveryLogPayloads[0].processed_by, "Scheduled Campaign");
  assert.equal(deliveryLogPayloads[0].source_app, "Send Queue");
  assert.equal(eventStatusUpdateCount, 1);
  assert.equal(brainDeliveryUpdateCount, 1);
});

test("delivery webhook updates verification send events without queue correlation", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let deliveryEventCount = 0;
  let eventStatusUpdateCount = 0;

  const verificationEvent = createPodioItem(811, {
    "trigger-name": textField("verification-textgrid-send:run-1"),
    "message-id": textField("provider-verify-1"),
    "ai-output": textField(
      JSON.stringify({
        verification_run_id: "run-1",
        client_reference_id: "verify-textgrid-run-1",
        provider_message_id: "provider-verify-1",
      })
    ),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByMessageId: async () => [verificationEvent],
    getItem: async () => null,
    fetchAllItems: async () => [],
    updateItem: async () => {
      throw new Error("queue_should_not_be_updated");
    },
    logDeliveryEvent: async () => {
      deliveryEventCount += 1;
    },
    updateMessageEventStatus: async () => {
      eventStatusUpdateCount += 1;
    },
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    message_id: "provider-verify-1",
    status: "delivered",
  });

  assert.equal(result.ok, true);
  assert.equal(result.queue_item_count, 0);
  assert.equal(result.matched_event_count, 1);
  assert.equal(deliveryEventCount, 1);
  assert.equal(eventStatusUpdateCount, 1);
});

test("delivery webhook normalizes raw TextGrid sent callbacks and updates queue state", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const queueUpdates = [];

  const outboundEvent = createPodioItem(821, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("provider-sent-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-sent-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async (item_id, payload) => {
      queueUpdates.push({ item_id, payload });
    },
    logDeliveryEvent: async () => {},
    updateMessageEventStatus: async () => {},
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    SmsSid: "provider-sent-1",
    SmsStatus: "sent",
    From: "+15550000002",
    To: "+15550000001",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_state, "Sent");
  assert.equal(queueUpdates.length, 1);
  assert.equal(queueUpdates[0].item_id, 123);
  assert.equal(queueUpdates[0].payload["delivery-confirmed"], "⏳ Pending");
});

test("delivery webhook normalizes raw TextGrid delivered callbacks and confirms delivery", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const queueUpdates = [];

  const outboundEvent = createPodioItem(822, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("provider-delivered-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-delivered-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async (item_id, payload) => {
      queueUpdates.push({ item_id, payload });
    },
    logDeliveryEvent: async () => {},
    updateMessageEventStatus: async () => {},
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    MessageSid: "provider-delivered-1",
    MessageStatus: "delivered",
    From: "+15550000002",
    To: "+15550000001",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_state, "Delivered");
  assert.equal(queueUpdates.length, 1);
  assert.equal(queueUpdates[0].item_id, 123);
  assert.equal(queueUpdates[0].payload["delivery-confirmed"], "✅ Confirmed");
  assert.equal(queueUpdates[0].payload["queue-status"], "Delivered");
});

test("delivery webhook suppresses future outreach on hard-bounce destination failures", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const phoneUpdates = [];

  const outboundEvent = createPodioItem(823, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("provider-failed-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-failed-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async () => {},
    logDeliveryEvent: async () => {},
    updateMessageEventStatus: async () => {},
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async (item_id, payload) => {
      phoneUpdates.push({ item_id, payload });
    },
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Hard Bounce",
  });

  const result = await handleTextgridDeliveryWebhook({
    MessageSid: "provider-failed-1",
    MessageStatus: "undelivered",
    ErrorCode: "30003",
    ErrorMessage: "Destination unreachable",
    From: "+15550000002",
    To: "+15550000001",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_state, "Failed");
  assert.equal(phoneUpdates.length, 1);
  assert.equal(phoneUpdates[0].item_id, 401);
  assert.equal(phoneUpdates[0].payload["do-not-call"], "TRUE");
  assert.equal(phoneUpdates[0].payload["dnc-source"], "Carrier Flag");
  assert.ok(phoneUpdates[0].payload["opt-out-date"]?.start);
});

test("DocuSign webhook ignores replay after first contract mutation", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let contractUpdateCount = 0;

  __setDocusignWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findContractItems: async () => [createPodioItem(9001)],
    updateContractItem: async () => {
      contractUpdateCount += 1;
    },
    maybeCreateTitleRoutingFromSignedContract: async () => ({
      created: true,
      title_routing_item_id: 9101,
    }),
    maybeCreateClosingFromTitleRouting: async () => ({
      created: true,
      closing_item_id: 9201,
    }),
    maybeSendTitleIntro: async () => ({ sent: true }),
    syncPipelineState: async () => ({ current_stage: "Contract" }),
  });

  const payload = {
    event_id: "doc-event-1",
    envelope_id: "env-1",
    status: "completed",
  };

  const first = await handleDocusignWebhook(payload);
  const second = await handleDocusignWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.normalized_status, "Completed");
  assert.equal(second.duplicate, true);
  assert.equal(contractUpdateCount, 1);
});

test("title webhook ignores replay after first state update", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let titleStatusUpdateCount = 0;
  let closingStatusUpdateCount = 0;

  __setTitleWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    getTitleRoutingItem: async () => createPodioItem(1001),
    findTitleRoutingItems: async () => [],
    findClosingItems: async () => [],
    getClosingItem: async () => createPodioItem(2001),
    classifyTitleResponse: () => ({
      normalized_event: "title_opened",
      routing_status: "Opened",
      closing_status: "Scheduled",
      reason: "title_open_signal_detected",
      confidence: 0.9,
      sender_email: "title@example.com",
      subject: "Need estoppel",
    }),
    updateTitleRoutingStatus: async () => {
      titleStatusUpdateCount += 1;
      return { updated: true };
    },
    updateClosingStatus: async () => {
      closingStatusUpdateCount += 1;
      return { updated: true };
    },
  });

  const payload = {
    event_id: "title-event-1",
    title_routing_item_id: 1001,
    closing_item_id: 2001,
    subject: "Need estoppel",
    body: "Please send docs",
    event: "email_reply",
  };

  const first = await handleTitleResponseWebhook(payload);
  const second = await handleTitleResponseWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.updated, true);
  assert.equal(second.duplicate, true);
  assert.equal(titleStatusUpdateCount, 1);
  assert.equal(closingStatusUpdateCount, 1);
});

test("closing webhook ignores replay after first close/revenue path", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let markClosedCount = 0;
  let revenueCreateCount = 0;

  __setClosingWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    getClosingItem: async () =>
      createPodioItem(3001, {
        "closing-id": textField("CL-1"),
      }),
    findClosingItems: async () => [],
    maybeMarkClosed: async () => {
      markClosedCount += 1;
      return { updated: true };
    },
    createDealRevenueFromClosedClosing: async () => {
      revenueCreateCount += 1;
      return { created: true, deal_revenue_item_id: 4001 };
    },
    updateClosingStatus: async () => ({ updated: true }),
  });

  const payload = {
    event_id: "closing-event-1",
    closing_item_id: 3001,
    status: "funded",
    body: "Funds released today",
  };

  const first = await handleClosingResponseWebhook(payload);
  const second = await handleClosingResponseWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.updated, true);
  assert.equal(first.normalized_event, "funded");
  assert.equal(second.duplicate, true);
  assert.equal(markClosedCount, 1);
  assert.equal(revenueCreateCount, 1);
});
