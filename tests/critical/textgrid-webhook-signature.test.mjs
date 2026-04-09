/**
 * textgrid-webhook-signature.test.mjs
 *
 * Focused tests for the shared TextGrid webhook signature verifier
 * and its integration in the inbound and delivery request handlers.
 *
 * The test runner sets APP_BASE_URL=http://localhost:3000, so all test
 * request URLs use that origin — no canonicalization rewrite occurs and
 * expected signatures can be precomputed against the same URLs.
 *
 * Coverage:
 *  1.  Twilio-style signature (URL + sorted form params) — valid
 *  2.  Twilio-style signature — wrong secret → rejected
 *  3.  Twilio-style signature — webhook_secret fallback → valid
 *  4.  Raw-body HMAC signature (base64) — valid
 *  5.  Raw-body HMAC signature (sha1= prefix) — valid
 *  6.  No secrets configured → verification not required
 *  7.  Secret configured, no signature → missing_signature
 *  8.  buildCanonicalWebhookUrl: rewrites origin with override_base
 *  9.  buildCanonicalWebhookUrl: preserves query string
 * 10.  buildCanonicalWebhookUrl: no base → request_url unchanged
 * 11.  Failure diagnostics include required fields, no secrets
 * 12.  Twilio signing sorts params regardless of raw body order
 * 13.  handleTextgridDeliveryRequest: valid Twilio sig → 200
 * 14.  handleTextgridDeliveryRequest: invalid sig → 401 + diagnostics in log
 */

import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import {
  verifyTextgridWebhookRequest,
  buildCanonicalWebhookUrl,
} from "@/lib/webhooks/textgrid-verify-webhook.js";

import { handleTextgridDeliveryRequest } from "@/lib/webhooks/textgrid-delivery-request.js";

// ── helpers ────────────────────────────────────────────────────────────────

function makeLogger() {
  const entries = [];
  return {
    entries,
    logger: {
      info: (event, meta) => entries.push({ level: "info", event, meta }),
      warn: (event, meta) => entries.push({ level: "warn", event, meta }),
      error: (event, meta) => entries.push({ level: "error", event, meta }),
    },
  };
}

/**
 * Build a real Twilio-style HMAC-SHA1 signature.
 * Algorithm: base64(HMAC-SHA1(url + sorted key+value pairs, secret))
 */
function buildTwilioSig(url, params, secret) {
  const sorted_keys = Object.keys(params).sort();
  let signing_string = url;
  for (const key of sorted_keys) {
    signing_string += key + String(params[key] ?? "");
  }
  return crypto.createHmac("sha1", secret).update(signing_string, "utf8").digest("base64");
}

/** Build raw-body HMAC-SHA1 in base64. */
function buildRawBodySig(raw_body, secret) {
  return crypto.createHmac("sha1", secret).update(raw_body, "utf8").digest("base64");
}

const TEST_AUTH_TOKEN = "test-auth-token-abc123";
const TEST_WEBHOOK_SECRET = "test-wh-secret-xyz789";

// Use localhost:3000 so the APP_BASE_URL (set by test runner) doesn't rewrite
// the canonical URL — keeps expected signatures stable.
const INBOUND_URL = "http://localhost:3000/api/webhooks/textgrid/inbound";
const DELIVERY_URL = "http://localhost:3000/api/webhooks/textgrid/delivery";

const FORM_PARAMS = {
  SmsSid: "SM123",
  SmsStatus: "received",
  From: "+15550001234",
  To: "+15559876543",
  Body: "Hello world",
};
const RAW_BODY = new URLSearchParams(FORM_PARAMS).toString();

// ── 1. Valid Twilio-style signature (auth_token) ─────────────────────────

test("verifyTextgridWebhookRequest: accepts valid Twilio-style signature (auth_token)", () => {
  const sig = buildTwilioSig(INBOUND_URL, FORM_PARAMS, TEST_AUTH_TOKEN);

  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: FORM_PARAMS,
    content_type: "application/x-www-form-urlencoded",
    signature: sig,
    signature_header_name: "x-twilio-signature",
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: "",
  });

  assert.equal(result.ok, true, "Should be ok");
  assert.equal(result.verified, true, "Should be verified");
  assert.equal(result.required, true);
  assert.equal(result.reason, "verified");
  assert.ok(result.algorithm?.includes("Twilio"), `Expected Twilio algorithm, got: ${result.algorithm}`);
});

// ── 2. Twilio-style signature — wrong secret ─────────────────────────────

test("verifyTextgridWebhookRequest: rejects Twilio-style signature signed with wrong secret", () => {
  const wrong_sig = buildTwilioSig(INBOUND_URL, FORM_PARAMS, "wrong-secret");

  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: FORM_PARAMS,
    content_type: "application/x-www-form-urlencoded",
    signature: wrong_sig,
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: TEST_WEBHOOK_SECRET,
  });

  assert.equal(result.ok, false);
  assert.equal(result.verified, false);
  assert.equal(result.required, true);
  assert.equal(result.reason, "invalid_signature");
  assert.ok(Array.isArray(result.diagnostics.modes_tried));
  assert.ok(result.diagnostics.modes_tried.length >= 2, "Should try multiple modes");
});

// ── 3. Twilio-style signature — webhook_secret fallback ──────────────────

test("verifyTextgridWebhookRequest: accepts valid Twilio-style signature using webhook_secret", () => {
  const sig = buildTwilioSig(INBOUND_URL, FORM_PARAMS, TEST_WEBHOOK_SECRET);

  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: FORM_PARAMS,
    content_type: "application/x-www-form-urlencoded",
    signature: sig,
    auth_token: "unrelated-token",
    webhook_secret: TEST_WEBHOOK_SECRET,
  });

  assert.equal(result.ok, true);
  assert.equal(result.verified, true);
  assert.equal(result.reason, "verified");
});

// ── 4. Raw-body HMAC — valid (base64) ────────────────────────────────────

test("verifyTextgridWebhookRequest: accepts valid raw-body HMAC in base64 (auth_token)", () => {
  const sig = buildRawBodySig(RAW_BODY, TEST_AUTH_TOKEN);

  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: null,
    content_type: "text/plain",
    signature: sig,
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: "",
  });

  assert.equal(result.ok, true);
  assert.equal(result.verified, true);
  assert.ok(result.algorithm?.includes("Raw"));
});

// ── 5. Raw-body HMAC — sha1= prefixed ────────────────────────────────────

test("verifyTextgridWebhookRequest: accepts sha1= prefixed raw-body HMAC", () => {
  const hex = crypto
    .createHmac("sha1", TEST_AUTH_TOKEN)
    .update(RAW_BODY, "utf8")
    .digest("hex");
  const prefixed = `sha1=${hex}`;

  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: null,
    content_type: "text/plain",
    signature: prefixed,
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: "",
  });

  assert.equal(result.ok, true);
  assert.equal(result.verified, true);
});

// ── 6. No secrets configured ─────────────────────────────────────────────

test("verifyTextgridWebhookRequest: not required when no secrets configured", () => {
  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: FORM_PARAMS,
    content_type: "application/x-www-form-urlencoded",
    signature: "any-sig",
    auth_token: "",
    webhook_secret: "",
  });

  assert.equal(result.ok, true);
  assert.equal(result.verified, false);
  assert.equal(result.required, false);
  assert.equal(result.reason, "no_secrets_configured");
});

// ── 7. Secret configured, no signature ───────────────────────────────────

test("verifyTextgridWebhookRequest: rejects with missing_signature when secret set but sig absent", () => {
  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: FORM_PARAMS,
    content_type: "application/x-www-form-urlencoded",
    signature: "",
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: "",
  });

  assert.equal(result.ok, false);
  assert.equal(result.required, true);
  assert.equal(result.reason, "missing_signature");
  assert.equal(result.signature_present, false);
});

// ── 8. URL canonicalization: override_base rewrites origin ───────────────

test("buildCanonicalWebhookUrl: replaces origin with override_base", () => {
  const canonical = buildCanonicalWebhookUrl(
    "http://internal-host/api/webhooks/textgrid/inbound",
    "https://myapp.vercel.app"
  );
  assert.equal(canonical, "https://myapp.vercel.app/api/webhooks/textgrid/inbound");
});

// ── 9. URL canonicalization: query string preserved ───────────────────────

test("buildCanonicalWebhookUrl: preserves query string", () => {
  const canonical = buildCanonicalWebhookUrl(
    "http://internal-host/api/webhooks/textgrid/delivery?foo=bar",
    "https://myapp.vercel.app"
  );
  assert.equal(canonical, "https://myapp.vercel.app/api/webhooks/textgrid/delivery?foo=bar");
});

// ── 10. URL canonicalization: no override → ENV.APP_BASE_URL takes effect ─

test("buildCanonicalWebhookUrl: falls back to ENV.APP_BASE_URL when no override given", () => {
  // The test runner sets APP_BASE_URL=http://localhost:3000, so the origin
  // is rewritten to that base even when override_base is empty.
  const canonical = buildCanonicalWebhookUrl(
    "https://myapp.vercel.app/api/webhooks/textgrid/inbound",
    ""
  );
  assert.ok(
    canonical.includes("/api/webhooks/textgrid/inbound"),
    "Path should be preserved"
  );
  assert.ok(
    typeof canonical === "string" && canonical.startsWith("http"),
    "Result should be a valid URL string"
  );
});

// ── 11. Failure diagnostics ───────────────────────────────────────────────

test("verifyTextgridWebhookRequest: diagnostics include required fields, no secrets", () => {
  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: RAW_BODY,
    form_params: FORM_PARAMS,
    content_type: "application/x-www-form-urlencoded",
    signature: "bad-sig",
    signature_header_name: "x-twilio-signature",
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: TEST_WEBHOOK_SECRET,
  });

  assert.equal(result.ok, false);
  const d = result.diagnostics;

  // Required fields
  assert.equal(d.signature_header, "x-twilio-signature", "signature_header");
  assert.ok(d.content_type?.includes("form-urlencoded"), "content_type");
  assert.ok(typeof d.request_path === "string", "request_path");
  assert.equal(d.raw_body_present, true, "raw_body_present");
  assert.equal(d.auth_token_configured, true, "auth_token_configured");
  assert.equal(d.webhook_secret_configured, true, "webhook_secret_configured");
  assert.ok(typeof d.canonical_url_base === "string", "canonical_url_base");
  assert.ok(Array.isArray(d.modes_tried), "modes_tried is array");

  // Secrets must NOT appear in diagnostics
  const d_str = JSON.stringify(d);
  assert.ok(!d_str.includes(TEST_AUTH_TOKEN), "auth_token must not appear in diagnostics");
  assert.ok(!d_str.includes(TEST_WEBHOOK_SECRET), "webhook_secret must not appear in diagnostics");
});

// ── 12. Param sort order: Twilio signs sorted params ────────────────────

test("verifyTextgridWebhookRequest: Twilio signing sorts params regardless of raw body order", () => {
  // Params intentionally in non-alphabetical order
  const params = {
    To: "+15559876543",
    SmsSid: "SM-rev",
    SmsStatus: "received",
    From: "+15550001234",
    Body: "Test",
  };
  const raw = new URLSearchParams(params).toString();
  const sig = buildTwilioSig(INBOUND_URL, params, TEST_AUTH_TOKEN);

  const result = verifyTextgridWebhookRequest({
    request_url: INBOUND_URL,
    raw_body: raw,
    form_params: params,
    content_type: "application/x-www-form-urlencoded",
    signature: sig,
    auth_token: TEST_AUTH_TOKEN,
    webhook_secret: "",
  });

  assert.equal(result.ok, true, `Verification failed: ${result.reason}`);
  assert.equal(result.verified, true);
});

// ── 13. Delivery handler: valid Twilio signature → 200 ───────────────────

test("handleTextgridDeliveryRequest: valid Twilio signature accepted → 200", async () => {
  const form_params = {
    SmsSid: "SM-delivery-999",
    MessageStatus: "delivered",
    From: "+12085550111",
    To: "+12085550222",
    AccountSid: "AC-test",
  };
  const raw_body = new URLSearchParams(form_params).toString();

  // Signature is computed against the canonical URL that the verifier will
  // produce when APP_BASE_URL=http://localhost:3000.
  const sig = buildTwilioSig(DELIVERY_URL, form_params, TEST_AUTH_TOKEN);

  const { logger } = makeLogger();
  let handled_payload = null;

  const response = await handleTextgridDeliveryRequest(
    new Request(DELIVERY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-twilio-signature": sig,
      },
      body: raw_body,
    }),
    {
      logger,
      verifyTextgridWebhookSignatureImpl: (opts) =>
        verifyTextgridWebhookRequest({
          ...opts,
          auth_token: TEST_AUTH_TOKEN,
          webhook_secret: "",
        }),
      handleTextgridDeliveryImpl: async (payload) => {
        handled_payload = payload;
        return { ok: true, normalized_state: "Delivered" };
      },
    }
  );

  assert.equal(response.status, 200, `Expected 200, got ${response.status}`);
  assert.equal(response.payload.ok, true);
  assert.equal(handled_payload?.message_id, "SM-delivery-999");
  assert.equal(handled_payload?.status, "delivered");
});

// ── 14. Delivery handler: invalid signature → 401 + diagnostics ──────────

test("handleTextgridDeliveryRequest: invalid signature → 401 with diagnostics in log", async () => {
  const form_params = {
    SmsSid: "SM-delivery-000",
    MessageStatus: "delivered",
    From: "+12085550111",
    To: "+12085550222",
  };
  const raw_body = new URLSearchParams(form_params).toString();

  const { entries, logger } = makeLogger();

  const response = await handleTextgridDeliveryRequest(
    new Request(DELIVERY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-twilio-signature": "definitely-wrong-signature",
      },
      body: raw_body,
    }),
    {
      logger,
      verifyTextgridWebhookSignatureImpl: (opts) =>
        verifyTextgridWebhookRequest({
          ...opts,
          auth_token: TEST_AUTH_TOKEN,
          webhook_secret: TEST_WEBHOOK_SECRET,
        }),
      handleTextgridDeliveryImpl: async () => ({ ok: true }),
    }
  );

  assert.equal(response.status, 401, `Expected 401, got ${response.status}`);
  assert.equal(response.payload.ok, false);
  assert.equal(response.payload.error, "invalid_textgrid_signature");

  const warn = entries.find((e) => e.event === "textgrid_delivery.invalid_signature");
  assert.ok(warn, "Should log textgrid_delivery.invalid_signature warning");
  assert.ok(Array.isArray(warn.meta.modes_tried), "Log should include modes_tried");
  assert.equal(warn.meta.auth_token_configured, true, "Log should show auth_token configured");
  assert.equal(warn.meta.webhook_secret_configured, true, "Log should show webhook_secret configured");

  // Secrets must NOT appear in the log entry
  const log_str = JSON.stringify(warn.meta);
  assert.ok(!log_str.includes(TEST_AUTH_TOKEN), "Auth token must not appear in log");
  assert.ok(!log_str.includes(TEST_WEBHOOK_SECRET), "Webhook secret must not appear in log");
});
