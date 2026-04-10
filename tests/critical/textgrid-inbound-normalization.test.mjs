import test from "node:test";
import assert from "node:assert/strict";

import { normalizeTextgridInboundPayload } from "@/lib/webhooks/textgrid-inbound-normalize.js";
import { handleTextgridInboundWebhook, __setTextgridInboundTestDeps, __resetTextgridInboundTestDeps } from "@/lib/flows/handle-textgrid-inbound.js";
import { createInMemoryIdempotencyLedger, createPodioItem } from "../helpers/test-helpers.js";

test("inbound route normalizer maps Twilio/TextGrid webhook fields", () => {
  const payload = normalizeTextgridInboundPayload(
    {
      SmsMessageSid: "SM123",
      SmsSid: "SM999",
      From: "+15551230001",
      To: "+15559870002",
      Body: "Hello there",
      SmsStatus: "received",
    },
    new Headers()
  );

  assert.equal(payload.message_id, "SM123");
  assert.equal(payload.from, "+15551230001");
  assert.equal(payload.to, "+15559870002");
  assert.equal(payload.message, "Hello there");
  assert.equal(payload.status, "received");
  assert.ok(payload.received_at);
});

test("inbound handler accepts raw Twilio/TextGrid payload and logs inbound event", async (t) => {
  const ledger = createInMemoryIdempotencyLedger();
  let logged_payload = null;

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
    classify: async () => ({ language: "English", source: "test" }),
    resolveRoute: () => ({ stage: "Ownership", use_case: "ownership_check", seller_profile: null }),
    logInboundMessageEvent: async (payload) => {
      logged_payload = payload;
      return { item_id: 991 };
    },
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => ({ ok: true, created: false }),
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: false }),
    maybeQueueSellerStageReply: async () => ({ ok: true, handled: false, queued: false }),
    maybeQueueUnderwritingFollowUp: async () => ({ ok: true, queued: false }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Ownership" }),
  });

  t.after(() => {
    __resetTextgridInboundTestDeps();
  });

  const result = await handleTextgridInboundWebhook({
    SmsMessageSid: "SM123",
    From: "+15551230001",
    To: "+15559870002",
    Body: "Yes, I own it",
    SmsStatus: "received",
    http_received_at: "2026-04-08T00:00:00.000Z",
  });

  assert.equal(result.ok, true);
  assert.equal(result.message_id, "SM123");
  assert.equal(result.inbound_from, "+15551230001");
  assert.equal(result.body, "Yes, I own it");
  assert.equal(logged_payload.provider_message_id, "SM123");
  assert.equal(logged_payload.received_at, "2026-04-08T00:00:00.000Z");
  assert.equal(logged_payload.processed_by, "Manual Sender");
  assert.equal(logged_payload.source_app, "External API");
});
