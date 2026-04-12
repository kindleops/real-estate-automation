import assert from "node:assert/strict";
import test from "node:test";

import {
  __resetDocusignWebhookTestDeps,
  __setDocusignWebhookTestDeps,
  handleDocusignWebhook,
} from "@/lib/domain/contracts/handle-docusign-webhook.js";
import { CONTRACT_FIELDS } from "@/lib/podio/apps/contracts.js";
import {
  categoryField,
  createInMemoryIdempotencyLedger,
  createPodioItem,
} from "../helpers/test-helpers.js";

function buildContractItem({
  item_id = 9001,
  status = "Draft",
  envelope_id = "",
} = {}) {
  const fields = {
    [CONTRACT_FIELDS.contract_status]: categoryField(status),
  };

  if (envelope_id) {
    fields[CONTRACT_FIELDS.docusign_envelope_id] = { value: envelope_id };
  }

  return createPodioItem(item_id, fields);
}

function buildWebhookDeps({
  contract_item,
  updates,
  brain_calls,
  title_calls,
  closing_calls,
  buyer_match_calls,
} = {}) {
  const ledger = createInMemoryIdempotencyLedger();

  __setDocusignWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findContractItems: async () => [contract_item],
    updateContractItem: async (_item_id, payload) => {
      updates.push(payload);
    },
    maybeCreateTitleRoutingFromSignedContract: async (payload) => {
      title_calls.push(payload);
      return {
        ok: true,
        created: false,
        reason: "contract_not_signed",
      };
    },
    maybeCreateClosingFromTitleRouting: async (payload) => {
      closing_calls.push(payload);
      return {
        ok: true,
        created: false,
        reason: "missing_title_routing_item_id",
      };
    },
    createBuyerMatchFlow: async (payload) => {
      buyer_match_calls.push(payload);
      return {
        ok: true,
        created: false,
        reason: "buyer_match_skipped_for_test",
      };
    },
    maybeSendTitleIntro: async () => ({ sent: false, reason: "no_title_routing" }),
    syncPipelineState: async () => ({ current_stage: "Contract" }),
    updateBrainFromExecution: async (payload) => {
      brain_calls.push(payload);
      return { ok: true, updated: true, reason: "brain_updated" };
    },
  });
}

test.afterEach(() => {
  __resetDocusignWebhookTestDeps();
});

test("seller signed webhook maps to Seller Signed contract state and seller timestamp", async () => {
  const updates = [];
  const brain_calls = [];
  const title_calls = [];
  const closing_calls = [];
  const buyer_match_calls = [];

  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Sent" }),
    updates,
    brain_calls,
    title_calls,
    closing_calls,
    buyer_match_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-seller-signed",
    envelopeSummary: {
      envelopeId: "env-seller",
      status: "sent",
      sentDateTime: "2026-04-11T12:00:00.000Z",
      recipients: {
        signers: [
          {
            roleName: "Seller",
            status: "completed",
            completedDateTime: "2026-04-11T12:05:00.000Z",
          },
          {
            roleName: "Buyer",
            status: "sent",
          },
        ],
      },
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Seller Signed");
  assert.equal(result.contract_status, "Seller Signed");
  assert.equal(updates.length, 1);
  assert.equal(updates[0][CONTRACT_FIELDS.contract_status], "Seller Signed");
  assert.equal(
    updates[0][CONTRACT_FIELDS.seller_signed_timestamp].start,
    "2026-04-11T12:05:00.000Z"
  );
  assert.equal(
    updates[0][CONTRACT_FIELDS.contract_sent_timestamp].start,
    "2026-04-11T12:00:00.000Z"
  );
  assert.equal(brain_calls[0].normalized_status, "Seller Signed");
  assert.equal(title_calls.length, 1);
  assert.equal(closing_calls.length, 1);
  assert.equal(buyer_match_calls.length, 0);
});

test("buyer signed webhook maps to Buyer Signed contract state and buyer timestamp", async () => {
  const updates = [];
  const brain_calls = [];
  const title_calls = [];
  const closing_calls = [];
  const buyer_match_calls = [];

  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Viewed" }),
    updates,
    brain_calls,
    title_calls,
    closing_calls,
    buyer_match_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-buyer-signed",
    envelopeSummary: {
      envelopeId: "env-buyer",
      status: "sent",
      recipients: {
        signers: [
          {
            roleName: "Seller",
            status: "sent",
          },
          {
            roleName: "Buyer",
            status: "completed",
            completedDateTime: "2026-04-11T12:09:00.000Z",
          },
        ],
      },
    },
    recipient_status: "completed",
    status: "sent",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Buyer Signed");
  assert.equal(result.contract_status, "Buyer Signed");
  assert.equal(
    updates[0][CONTRACT_FIELDS.buyer_signed_timestamp].start,
    "2026-04-11T12:09:00.000Z"
  );
  assert.equal(brain_calls[0].normalized_status, "Buyer Signed");
  assert.equal(buyer_match_calls.length, 0);
});

test("viewed webhook maps to Viewed and stamps contract viewed timestamp", async () => {
  const updates = [];
  const brain_calls = [];
  const title_calls = [];
  const closing_calls = [];
  const buyer_match_calls = [];

  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Sent" }),
    updates,
    brain_calls,
    title_calls,
    closing_calls,
    buyer_match_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-viewed",
    envelopeSummary: {
      envelopeId: "env-viewed",
      status: "delivered",
      deliveredDateTime: "2026-04-11T12:07:00.000Z",
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Delivered");
  assert.equal(result.contract_status, "Viewed");
  assert.equal(
    updates[0][CONTRACT_FIELDS.contract_viewed_timestamp].start,
    "2026-04-11T12:07:00.000Z"
  );
  assert.equal(brain_calls[0].normalized_status, "Delivered");
  assert.equal(buyer_match_calls.length, 0);
});

test("completed webhook maps to Fully Executed and kicks execution bridge forward", async () => {
  const updates = [];
  const brain_calls = [];
  const title_calls = [];
  const closing_calls = [];
  const buyer_match_calls = [];

  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Viewed" }),
    updates,
    brain_calls,
    title_calls,
    closing_calls,
    buyer_match_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-completed",
    envelopeSummary: {
      envelopeId: "env-completed",
      status: "completed",
      completedDateTime: "2026-04-11T12:15:00.000Z",
      recipients: {
        signers: [
          {
            roleName: "Seller",
            status: "completed",
            completedDateTime: "2026-04-11T12:10:00.000Z",
          },
          {
            roleName: "Buyer",
            status: "completed",
            completedDateTime: "2026-04-11T12:12:00.000Z",
          },
        ],
      },
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Completed");
  assert.equal(result.contract_status, "Fully Executed");
  assert.equal(
    updates[0][CONTRACT_FIELDS.fully_executed_timestamp].start,
    "2026-04-11T12:15:00.000Z"
  );
  assert.equal(
    updates[0][CONTRACT_FIELDS.seller_signed_timestamp].start,
    "2026-04-11T12:10:00.000Z"
  );
  assert.equal(
    updates[0][CONTRACT_FIELDS.buyer_signed_timestamp].start,
    "2026-04-11T12:12:00.000Z"
  );
  assert.equal(title_calls.length, 1);
  assert.equal(closing_calls.length, 1);
  assert.equal(buyer_match_calls.length, 1);
  assert.equal(buyer_match_calls[0].contract_id, 9001);
  assert.equal(brain_calls[0].normalized_status, "Completed");
});

test("voided webhook maps to Cancelled without regressing terminal contract rows", async () => {
  const updates = [];
  const brain_calls = [];
  const title_calls = [];
  const closing_calls = [];
  const buyer_match_calls = [];

  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Fully Executed", envelope_id: "env-voided" }),
    updates,
    brain_calls,
    title_calls,
    closing_calls,
    buyer_match_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-voided",
    envelopeSummary: {
      envelopeId: "env-voided",
      status: "voided",
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Voided");
  assert.equal(result.contract_status, null);
  assert.equal(updates.length, 0);
  assert.equal(brain_calls[0].normalized_status, "Voided");
  assert.equal(buyer_match_calls.length, 0);
});
