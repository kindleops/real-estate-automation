import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  __resetBuyerWebhookTestDeps,
  __setBuyerWebhookTestDeps,
  handleBuyerResponseWebhook,
  maybeHandleBuyerTextgridInbound,
} from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { BUYER_MATCH_FIELDS } from "@/lib/podio/apps/buyer-match.js";
import {
  appRefField,
  categoryField,
  createInMemoryIdempotencyLedger,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetBuyerWebhookTestDeps();
});

test("buyer webhook updates interest once and ignores replay", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const updates = [];
  const createdEvents = [];
  const threadUpdates = [];
  let pipelineUpdates = 0;

  const buyerMatch = createPodioItem(701, {
    [BUYER_MATCH_FIELDS.match_status]: categoryField("Sent to Buyers"),
    [BUYER_MATCH_FIELDS.buyer_response_status]: categoryField("Sent"),
    [BUYER_MATCH_FIELDS.assignment_status]: categoryField("In Progress"),
    [BUYER_MATCH_FIELDS.property]: appRefField(801),
    [BUYER_MATCH_FIELDS.master_owner]: appRefField(901),
    [BUYER_MATCH_FIELDS.contract]: appRefField(1001),
  });

  const blastEvent = createPodioItem(601, {
    "message-id": textField("blast-msg-1"),
    "trigger-name": textField("buyer-blast:701:501"),
    "ai-output": textField(
      JSON.stringify({
        event_kind: "buyer_blast",
        buyer_match_item_id: 701,
        company_item_id: 501,
        recipient_email: "buyer@example.com",
        company_name: "Atlas Buyer Group",
      })
    ),
  });

  __setBuyerWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventByMessageId: async (messageId) =>
      messageId === "blast-msg-1" ? blastEvent : null,
    findMessageEvents: async () => ({ items: [] }),
    getBuyerMatchItem: async () => buyerMatch,
    getCompanyItem: async () => createPodioItem(501, {
      title: textField("Atlas Buyer Group"),
    }),
    updateBuyerMatchItem: async (itemId, payload) => {
      updates.push({ itemId, payload });
      return { ok: true };
    },
    createMessageEvent: async (payload) => {
      createdEvents.push(payload);
      return { ok: true, item_id: 9901 };
    },
    upsertBuyerDispositionThread: async (payload) => {
      threadUpdates.push(payload);
      return { ok: true, thread_item_id: 8801 };
    },
    syncPipelineState: async () => {
      pipelineUpdates += 1;
      return { ok: true, current_stage: "Buyer Match" };
    },
  });

  const payload = {
    event_id: "buyer-reply-1",
    in_reply_to: "blast-msg-1",
    from: "buyer@example.com",
    subject: "Interested",
    body: "We are interested and have proof of funds attached.",
    attachments_count: 1,
  };

  const first = await handleBuyerResponseWebhook(payload);
  const second = await handleBuyerResponseWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.updated, true);
  assert.equal(first.classification.normalized_response, "interested");
  assert.equal(second.ok, true);
  assert.equal(second.duplicate, true);
  assert.equal(updates.length, 1);
  assert.equal(createdEvents.length, 1);
  assert.equal(threadUpdates.length, 1);
  assert.equal(pipelineUpdates, 1);
  assert.equal(updates[0].itemId, 701);
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.buyer_response_status], "Interested");
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.match_status], "Buyers Interested");
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.buyer_proof_of_funds_received], "Yes");
  assert.equal(threadUpdates[0].interaction_status, "Interested");
  assert.equal(threadUpdates[0].channel, "email");
});

test("buyer webhook can choose buyer via sender-email fallback correlation", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const updates = [];
  const threadUpdates = [];

  const buyerMatch = createPodioItem(702, {
    [BUYER_MATCH_FIELDS.match_status]: categoryField("Sent to Buyers"),
    [BUYER_MATCH_FIELDS.buyer_response_status]: categoryField("Sent"),
    [BUYER_MATCH_FIELDS.assignment_status]: categoryField("In Progress"),
    [BUYER_MATCH_FIELDS.property]: appRefField(802),
    [BUYER_MATCH_FIELDS.master_owner]: appRefField(902),
    [BUYER_MATCH_FIELDS.contract]: appRefField(1002),
  });

  const blastEvent = createPodioItem(602, {
    "trigger-name": textField("buyer-blast:702:502"),
    "ai-output": textField(
      JSON.stringify({
        event_kind: "buyer_blast",
        buyer_match_item_id: 702,
        company_item_id: 502,
        recipient_email: "closer@example.com",
        company_name: "Closer Capital",
      })
    ),
  });

  __setBuyerWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventByMessageId: async () => null,
    findMessageEvents: async () => ({ items: [blastEvent] }),
    getBuyerMatchItem: async () => buyerMatch,
    getCompanyItem: async () => createPodioItem(502, {
      title: textField("Closer Capital"),
    }),
    updateBuyerMatchItem: async (itemId, payload) => {
      updates.push({ itemId, payload });
      return { ok: true };
    },
    createMessageEvent: async () => ({ ok: true, item_id: 9902 }),
    upsertBuyerDispositionThread: async (payload) => {
      threadUpdates.push(payload);
      return { ok: true, thread_item_id: 8802 };
    },
    syncPipelineState: async () => ({ ok: true, current_stage: "Buyer Match" }),
  });

  const result = await handleBuyerResponseWebhook({
    event_id: "buyer-reply-2",
    from: "closer@example.com",
    subject: "We will take it",
    body: "We will take it. Send assignment docs.",
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, true);
  assert.equal(result.classification.normalized_response, "chosen");
  assert.equal(result.correlation_mode, "sender_email_recent_blast");
  assert.equal(updates.length, 1);
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.buyer_response_status], "Selected");
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.match_status], "Buyers Chosen");
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.assignment_status], "Buyer Confirmed");
  assert.deepEqual(updates[0].payload[BUYER_MATCH_FIELDS.selected_buyer], [502]);
  assert.equal(updates[0].payload[BUYER_MATCH_FIELDS.dispo_outcome], "Buyer Secured");
  assert.equal(threadUpdates.length, 1);
  assert.equal(threadUpdates[0].company_item_id, "502");
  assert.equal(threadUpdates[0].interaction_status, "Selected");
});

test("buyer webhook can correlate an SMS response back to the buyer thread by phone", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const updates = [];
  const threadUpdates = [];

  const buyerMatch = createPodioItem(703, {
    [BUYER_MATCH_FIELDS.match_status]: categoryField("Sent to Buyers"),
    [BUYER_MATCH_FIELDS.buyer_response_status]: categoryField("Sent"),
    [BUYER_MATCH_FIELDS.assignment_status]: categoryField("In Progress"),
    [BUYER_MATCH_FIELDS.property]: appRefField(803),
    [BUYER_MATCH_FIELDS.master_owner]: appRefField(903),
    [BUYER_MATCH_FIELDS.contract]: appRefField(1003),
  });

  const blastEvent = createPodioItem(603, {
    "trigger-name": textField("buyer-blast:703:503"),
    "ai-output": textField(
      JSON.stringify({
        event_kind: "buyer_blast",
        buyer_match_item_id: 703,
        company_item_id: 503,
        recipient_email: "unused@example.com",
        recipient_phone: "5557771212",
        company_name: "SMS Capital",
      })
    ),
  });

  __setBuyerWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventByMessageId: async () => null,
    findMessageEvents: async () => ({ items: [blastEvent] }),
    getBuyerMatchItem: async () => buyerMatch,
    getCompanyItem: async () => createPodioItem(503, {
      title: textField("SMS Capital"),
    }),
    updateBuyerMatchItem: async (itemId, payload) => {
      updates.push({ itemId, payload });
      return { ok: true };
    },
    createMessageEvent: async () => ({ ok: true, item_id: 9903 }),
    upsertBuyerDispositionThread: async (payload) => {
      threadUpdates.push(payload);
      return { ok: true, thread_item_id: 8803 };
    },
    syncPipelineState: async () => ({ ok: true, current_stage: "Buyer Match" }),
  });

  const result = await handleBuyerResponseWebhook({
    event_id: "buyer-reply-sms-1",
    channel: "sms",
    from_phone: "5557771212",
    body: "Interested. Send me the assignment details.",
  });

  assert.equal(result.ok, true);
  assert.equal(result.updated, true);
  assert.equal(result.correlation_mode, "sender_phone_recent_blast");
  assert.equal(updates.length, 1);
  assert.equal(threadUpdates.length, 1);
  assert.equal(threadUpdates[0].channel, "sms");
  assert.equal(threadUpdates[0].recipient_phone, "5557771212");
});

test("textgrid inbound can intercept a matched buyer SMS before seller-side processing", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let updates = 0;

  const buyerMatch = createPodioItem(704, {
    [BUYER_MATCH_FIELDS.match_status]: categoryField("Sent to Buyers"),
    [BUYER_MATCH_FIELDS.buyer_response_status]: categoryField("Sent"),
    [BUYER_MATCH_FIELDS.assignment_status]: categoryField("In Progress"),
    [BUYER_MATCH_FIELDS.property]: appRefField(804),
    [BUYER_MATCH_FIELDS.master_owner]: appRefField(904),
    [BUYER_MATCH_FIELDS.contract]: appRefField(1004),
  });

  const blastEvent = createPodioItem(604, {
    "trigger-name": textField("buyer-blast:704:504"),
    "ai-output": textField(
      JSON.stringify({
        event_kind: "buyer_blast",
        buyer_match_item_id: 704,
        company_item_id: 504,
        recipient_phone: "5558881212",
        company_name: "SMS Intercept Capital",
        channel: "sms",
      })
    ),
  });

  __setBuyerWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventByMessageId: async () => null,
    findMessageEvents: async () => ({ items: [blastEvent] }),
    getBuyerMatchItem: async () => buyerMatch,
    getCompanyItem: async () => createPodioItem(504, {
      title: textField("SMS Intercept Capital"),
    }),
    updateBuyerMatchItem: async () => {
      updates += 1;
      return { ok: true };
    },
    createMessageEvent: async () => ({ ok: true, item_id: 9904 }),
    upsertBuyerDispositionThread: async () => ({ ok: true, thread_item_id: 8804 }),
    syncPipelineState: async () => ({ ok: true, current_stage: "Buyer Match" }),
  });

  const result = await maybeHandleBuyerTextgridInbound({
    id: "tg-buyer-sms-1",
    from: "+1 (555) 888-1212",
    to: "+1 (555) 222-3434",
    message: "Interested. Text me the assignment package.",
  });

  assert.equal(result.ok, true);
  assert.equal(result.matched, true);
  assert.equal(result.result.updated, true);
  assert.equal(result.result.correlation_mode, "sender_phone_recent_blast");
  assert.equal(updates, 1);
});
