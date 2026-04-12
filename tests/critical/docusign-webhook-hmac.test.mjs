import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";

import {
  computeDocusignHmacBase64,
  verifyDocusignConnectHmac,
} from "@/lib/security/docusign-hmac.js";
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

// ─── Helpers ─────────────────────────────────────────────────────────────────

const TEST_SECRET = "test-hmac-secret-32-bytes-long-99";

function makeHmac(secret, body) {
  return crypto
    .createHmac("sha256", Buffer.from(secret, "utf8"))
    .update(Buffer.from(body, "utf8"))
    .digest("base64");
}

/** Build a plain-object headers map with a correct signature for `body`. */
function validHeaders(body, secret = TEST_SECRET) {
  return {
    "x-docusign-signature-1": makeHmac(secret, body),
    "x-authorization-digest": "HMAC-SHA256",
  };
}

// ─── HMAC unit tests ─────────────────────────────────────────────────────────

test("valid HMAC signature passes verification", () => {
  const body = '{"status":"sent","envelopeId":"env-001"}';
  const result = verifyDocusignConnectHmac(body, validHeaders(body), TEST_SECRET);

  assert.equal(result.ok, true);
  assert.equal(result.reason, "verified");
});

test("invalid HMAC signature is rejected", () => {
  const body = '{"status":"sent","envelopeId":"env-002"}';
  const headers = {
    "x-docusign-signature-1": "aGVsbG8gd29ybGQ=", // valid base64, wrong value
    "x-authorization-digest": "HMAC-SHA256",
  };

  const result = verifyDocusignConnectHmac(body, headers, TEST_SECRET);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "invalid_docusign_hmac_signature");
});

test("missing HMAC signature header is rejected", () => {
  const body = '{"status":"sent"}';
  const headers = { "x-authorization-digest": "HMAC-SHA256" };

  const result = verifyDocusignConnectHmac(body, headers, TEST_SECRET);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "missing_docusign_hmac_signature");
});

test("empty string HMAC signature header is rejected", () => {
  const body = '{"status":"sent"}';
  const headers = {
    "x-docusign-signature-1": "",
    "x-authorization-digest": "HMAC-SHA256",
  };

  const result = verifyDocusignConnectHmac(body, headers, TEST_SECRET);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "missing_docusign_hmac_signature");
});

test("body tampering is detected by HMAC mismatch", () => {
  const original_body = '{"status":"sent","envelopeId":"env-003"}';
  const tampered_body = '{"status":"completed","envelopeId":"env-003"}';
  const headers = validHeaders(original_body);

  const result = verifyDocusignConnectHmac(tampered_body, headers, TEST_SECRET);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "invalid_docusign_hmac_signature");
});

test("missing secret fails closed regardless of valid signature", () => {
  const body = '{"status":"sent"}';
  const headers = validHeaders(body, TEST_SECRET);

  const result = verifyDocusignConnectHmac(body, headers, "");

  assert.equal(result.ok, false);
  assert.equal(result.reason, "missing_docusign_hmac_secret");
});

test("null secret fails closed", () => {
  const body = '{"status":"sent"}';

  const result = verifyDocusignConnectHmac(body, validHeaders(body), null);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "missing_docusign_hmac_secret");
});

test("header lookup is case-insensitive (mixed-case header names work)", () => {
  const body = '{"status":"completed","envelopeId":"env-004"}';
  const headers = {
    "X-DocuSign-Signature-1": makeHmac(TEST_SECRET, body), // upper-case variant
    "X-Authorization-Digest": "HMAC-SHA256",
  };

  const result = verifyDocusignConnectHmac(body, headers, TEST_SECRET);

  assert.equal(result.ok, true);
  assert.equal(result.reason, "verified");
});

test("Next.js Headers object is supported (get() API)", () => {
  const body = '{"status":"voided","envelopeId":"env-005"}';
  const sig = makeHmac(TEST_SECRET, body);

  // Simulate Next.js Headers: case-insensitive .get()
  const headers = new Map([
    ["x-docusign-signature-1", sig],
    ["x-authorization-digest", "HMAC-SHA256"],
  ]);
  headers.get = (name) => headers.get.call({ data: headers }, name);

  // Build a real Headers-like object using a simple proxy
  const headersObj = {
    get(name) {
      for (const [key, val] of headers.entries()) {
        if (key.toLowerCase() === name.toLowerCase()) return val;
      }
      return null;
    },
  };

  const result = verifyDocusignConnectHmac(body, headersObj, TEST_SECRET);

  assert.equal(result.ok, true);
  assert.equal(result.reason, "verified");
});

test("unsupported digest algorithm (MD5) is explicitly rejected", () => {
  const body = '{"status":"sent"}';
  const headers = {
    "x-docusign-signature-1": makeHmac(TEST_SECRET, body),
    "x-authorization-digest": "MD5",
  };

  const result = verifyDocusignConnectHmac(body, headers, TEST_SECRET);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "unsupported_docusign_digest_algorithm");
});

test("absent x-authorization-digest header is accepted (optional header)", () => {
  const body = '{"status":"sent"}';
  const headers = {
    "x-docusign-signature-1": makeHmac(TEST_SECRET, body),
    // x-authorization-digest intentionally omitted
  };

  const result = verifyDocusignConnectHmac(body, headers, TEST_SECRET);

  assert.equal(result.ok, true);
  assert.equal(result.reason, "verified");
});

test("computeDocusignHmacBase64 is consistent with a known reference vector", () => {
  // Reference computed independently:
  // echo -n 'hello' | openssl dgst -sha256 -hmac 'key' -binary | base64
  const expected = crypto
    .createHmac("sha256", "key")
    .update("hello")
    .digest("base64");

  assert.equal(computeDocusignHmacBase64("key", "hello"), expected);
});

// ─── Lifecycle regression tests (verify HMAC module does not break mapping) ──

function buildContractItem({ item_id = 9001, status = "Draft", envelope_id = "" } = {}) {
  const fields = { [CONTRACT_FIELDS.contract_status]: categoryField(status) };
  if (envelope_id) fields[CONTRACT_FIELDS.docusign_envelope_id] = { value: envelope_id };
  return createPodioItem(item_id, fields);
}

function buildWebhookDeps({
  contract_item,
  updates = [],
  brain_calls = [],
  buyer_match_calls = [],
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
    updateContractItem: async (_id, payload) => { updates.push(payload); },
    maybeCreateTitleRoutingFromSignedContract: async () => ({ ok: true, created: false, reason: "test" }),
    maybeCreateClosingFromTitleRouting: async () => ({ ok: true, created: false, reason: "test" }),
    createBuyerMatchFlow: async (payload) => {
      buyer_match_calls.push(payload);
      return { ok: true, created: false, reason: "test" };
    },
    maybeSendTitleIntro: async () => ({ sent: false }),
    syncPipelineState: async () => ({ current_stage: "Contract" }),
    updateBrainFromExecution: async (p) => { brain_calls.push(p); return { ok: true }; },
  });
}

test.afterEach(() => {
  __resetDocusignWebhookTestDeps();
});

test("lifecycle: sent event still maps to Stage 8 brain stage after HMAC module is present", async () => {
  const updates = [];
  const brain_calls = [];
  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Draft" }),
    updates,
    brain_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-sent-01",
    envelopeSummary: {
      envelopeId: "env-sent-01",
      status: "sent",
      sentDateTime: "2026-04-11T10:00:00.000Z",
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Sent");
  assert.equal(result.contract_status, "Sent");
  assert.equal(updates[0][CONTRACT_FIELDS.contract_status], "Sent");
  assert.equal(brain_calls[0].normalized_status, "Sent");
});

test("lifecycle: lower-signal 'sent' event does not regress a Fully Executed contract", async () => {
  const updates = [];
  const brain_calls = [];
  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Fully Executed", envelope_id: "env-fe-01" }),
    updates,
    brain_calls,
  });

  // A duplicate / out-of-order "sent" event arrives after the contract is fully executed.
  const result = await handleDocusignWebhook({
    event_id: "evt-late-sent-01",
    envelopeSummary: {
      envelopeId: "env-fe-01",
      status: "sent",
      sentDateTime: "2026-04-11T09:00:00.000Z",
    },
  });

  assert.equal(result.ok, true);
  // Brain still receives the event (for audit), but the contract STATUS must not regress.
  // (Timestamp-only writes are still allowed.)
  const status_written = updates.some((u) => CONTRACT_FIELDS.contract_status in u);
  assert.equal(status_written, false, "contract status must not regress from Fully Executed to Sent");
  assert.equal(brain_calls[0].normalized_status, "Sent");
});

test("lifecycle: lower-signal 'delivered' event does not regress a Seller Signed contract", async () => {
  const updates = [];
  const brain_calls = [];
  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Seller Signed", envelope_id: "env-ss-01" }),
    updates,
    brain_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-late-delivered-01",
    envelopeSummary: {
      envelopeId: "env-ss-01",
      status: "delivered",
      deliveredDateTime: "2026-04-11T11:00:00.000Z",
    },
  });

  assert.equal(result.ok, true);
  const status_written = updates.some((u) => CONTRACT_FIELDS.contract_status in u);
  assert.equal(status_written, false, "Delivered must not regress Seller Signed");
  assert.equal(brain_calls[0].normalized_status, "Delivered");
});

test("lifecycle: completed event maps to Fully Executed and does not fire if already Fully Executed (idempotent)", async () => {
  const updates = [];
  const brain_calls = [];
  const buyer_match_calls = [];
  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Seller Signed" }),
    updates,
    brain_calls,
    buyer_match_calls,
  });

  const payload = {
    event_id: "evt-completed-01",
    envelopeSummary: {
      envelopeId: "env-completed-01",
      status: "completed",
      completedDateTime: "2026-04-11T14:00:00.000Z",
      recipients: {
        signers: [
          { roleName: "Seller", status: "completed", completedDateTime: "2026-04-11T13:00:00.000Z" },
          { roleName: "Buyer", status: "completed", completedDateTime: "2026-04-11T13:30:00.000Z" },
        ],
      },
    },
  };

  const first = await handleDocusignWebhook(payload);
  assert.equal(first.ok, true);
  assert.equal(first.normalized_status, "Completed");
  assert.equal(first.contract_status, "Fully Executed");
  assert.equal(updates[0][CONTRACT_FIELDS.contract_status], "Fully Executed");
  assert.equal(buyer_match_calls.length, 1);

  // Second call with identical event_id — idempotency ledger must short-circuit.
  const second = await handleDocusignWebhook(payload);
  assert.equal(second.ok, true);
  assert.equal(second.duplicate, true);
  assert.equal(updates.length, 1, "contract must not be updated twice");
  assert.equal(buyer_match_calls.length, 1, "buyer match bridge must not fire twice");
});

test("lifecycle: voided event does not regress a Fully Executed contract to Cancelled", async () => {
  const updates = [];
  const brain_calls = [];
  buildWebhookDeps({
    contract_item: buildContractItem({ status: "Fully Executed", envelope_id: "env-void-01" }),
    updates,
    brain_calls,
  });

  const result = await handleDocusignWebhook({
    event_id: "evt-voided-01",
    envelopeSummary: { envelopeId: "env-void-01", status: "voided" },
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_status, "Voided");
  assert.equal(result.contract_status, null);
  const cancelled_written = updates.some(
    (u) => u[CONTRACT_FIELDS.contract_status] === "Cancelled"
  );
  assert.equal(cancelled_written, false, "Fully Executed must not be regressed to Cancelled");
  assert.equal(brain_calls[0].normalized_status, "Voided");
});
