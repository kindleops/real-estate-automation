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
 * 14.  handleTextgridDeliveryRequest: strict invalid sig → 401 + diagnostics in log
 * 15.  handleTextgridDeliveryRequest: observe invalid sig → 200 + continues
 * 16.  handleTextgridDeliveryRequest: off mode skips verification and continues
 * 17.  inbound route: strict invalid sig → 401
 * 18.  inbound route: observe invalid sig → 200 + continues
 * 19.  inbound route: off mode skips verification and continues
 */

import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import {
  verifyTextgridWebhookRequest,
  buildCanonicalWebhookUrl,
} from "@/lib/webhooks/textgrid-verify-webhook.js";

import {
  POST as postTextgridInbound,
  __resetTextgridInboundRouteTestDeps,
  __setTextgridInboundRouteTestDeps,
} from "@/app/api/webhooks/textgrid/inbound/route.js";
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

function setSignatureMode(t, mode) {
  const previous = process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE;

  if (mode === undefined || mode === null) {
    delete process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE;
  } else {
    process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE = mode;
  }

  t.after(() => {
    if (previous === undefined) {
      delete process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE;
    } else {
      process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE = previous;
    }
  });
}

function eventIndex(entries, event, predicate = null) {
  return entries.findIndex(
    (entry) => entry.event === event && (typeof predicate === "function" ? predicate(entry) : true)
  );
}

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
  assert.equal(d.signature_header_name, "x-twilio-signature", "signature_header_name");
  assert.equal(d.signature_header_present, true, "signature_header_present");
  assert.equal(d.signature_header_length, "bad-sig".length, "signature_header_length");
  assert.ok(d.content_type?.includes("form-urlencoded"), "content_type");
  assert.ok(typeof d.request_path === "string", "request_path");
  assert.equal(d.request_url, INBOUND_URL, "request_url");
  assert.equal(d.raw_body_present, true, "raw_body_present");
  assert.equal(d.raw_body_length, RAW_BODY.length, "raw_body_length");
  assert.equal(d.auth_token_configured, true, "auth_token_configured");
  assert.equal(d.webhook_secret_configured, true, "webhook_secret_configured");
  assert.deepEqual(
    d.parsed_form_param_keys,
    ["Body", "From", "SmsSid", "SmsStatus", "To"],
    "parsed_form_param_keys"
  );
  assert.ok(typeof d.canonical_url_base === "string", "canonical_url_base");
  assert.ok(Array.isArray(d.modes_tried), "modes_tried is array");
  assert.equal(d.failure_reason, "no_mode_produced_matching_digest", "failure_reason");

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

test("handleTextgridDeliveryRequest: strict mode invalid signature → 401 with diagnostics in log", async (t) => {
  setSignatureMode(t, "strict");

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
  assert.equal(response.payload.verification.signature_verification_mode, "strict");
  assert.equal(response.payload.verification.signature_bypassed, false);
  assert.equal(
    response.payload.verification.signature_failure_reason,
    "no_mode_produced_matching_digest"
  );

  const normalized = entries.find((entry) => entry.event === "textgrid_delivery.normalized");
  assert.ok(normalized, "strict mode should log normalized");
  const branch = entries.find((entry) => entry.event === "textgrid_delivery.signature_branch_selected");
  assert.ok(branch, "strict mode should log signature branch");
  assert.equal(branch.meta.signature_verification_mode, "strict");
  assert.equal(branch.meta.will_continue_after_signature_check, false);

  const warn = entries.find((e) => e.event === "textgrid_delivery.invalid_signature");
  assert.ok(warn, "Should log textgrid_delivery.invalid_signature warning");
  assert.ok(Array.isArray(warn.meta.modes_tried), "Log should include modes_tried");
  assert.equal(warn.meta.auth_token_configured, true, "Log should show auth_token configured");
  assert.equal(warn.meta.webhook_secret_configured, true, "Log should show webhook_secret configured");
  assert.equal(warn.meta.signature_verification_mode, "strict");
  assert.equal(warn.meta.signature_header_name, "x-twilio-signature");
  assert.equal(warn.meta.downstream_handler_invoked, false);
  assert.equal(warn.meta.podio_persistence_attempted, false);

  const responseLog = entries.find((entry) => entry.event === "textgrid_delivery.response_sent");
  assert.ok(responseLog, "strict mode should log response_sent");
  assert.equal(responseLog.meta.final_response_status, 401);
  assert.equal(responseLog.meta.downstream_handler_invoked, false);
  assert.equal(responseLog.meta.podio_persistence_attempted, false);
  assert.equal(
    entries.some((entry) => entry.event === "textgrid_delivery.handler_started"),
    false,
    "strict mode must not invoke delivery handler"
  );

  // Secrets must NOT appear in the log entry
  const log_str = JSON.stringify(warn.meta);
  assert.ok(!log_str.includes(TEST_AUTH_TOKEN), "Auth token must not appear in log");
  assert.ok(!log_str.includes(TEST_WEBHOOK_SECRET), "Webhook secret must not appear in log");
});

test("handleTextgridDeliveryRequest: observe mode accepts invalid signature and continues", async (t) => {
  setSignatureMode(t, "observe");

  const form_params = {
    SmsSid: "SM-delivery-observe-1",
    MessageStatus: "delivered",
    From: "+12085550111",
    To: "+12085550222",
  };
  const raw_body = new URLSearchParams(form_params).toString();
  const { entries, logger } = makeLogger();
  let handled_payload = null;

  const response = await handleTextgridDeliveryRequest(
    new Request(DELIVERY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "observe-mode-invalid-signature",
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
      handleTextgridDeliveryImpl: async (payload) => {
        handled_payload = payload;
        return { ok: true, normalized_state: "Delivered" };
      },
    }
  );

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(handled_payload?.message_id, "SM-delivery-observe-1");
  assert.equal(handled_payload?.signature_verification_mode, "observe");
  assert.equal(handled_payload?.signature_verified, false);
  assert.equal(handled_payload?.signature_bypassed, true);
  assert.equal(
    handled_payload?.signature_failure_reason,
    "no_mode_produced_matching_digest"
  );
  assert.equal(handled_payload?.signature_header_name, "x-textgrid-signature");
  assert.equal(handled_payload?.signature_unverified_observe_mode, true);
  assert.equal(
    response.payload.verification.signature_unverified_observe_mode,
    true
  );

  const warn = entries.find((entry) => entry.event === "textgrid_delivery.invalid_signature");
  assert.ok(warn, "observe mode should still log invalid signature");
  assert.equal(warn.meta.signature_verification_mode, "observe");
  assert.equal(warn.meta.signature_bypassed, true);
  assert.equal(warn.meta.signature_unverified_observe_mode, true);
  assert.equal(warn.meta.request_path, "/api/webhooks/textgrid/delivery");

  const normalized = entries.find((entry) => entry.event === "textgrid_delivery.normalized");
  assert.ok(normalized, "observe mode should log normalized");
  const branch = entries.find((entry) => entry.event === "textgrid_delivery.signature_branch_selected");
  assert.ok(branch, "observe mode should log signature branch");
  assert.equal(branch.meta.signature_verification_mode, "observe");
  assert.equal(branch.meta.will_continue_after_signature_check, true);

  const accepted = entries.find((entry) => entry.event === "textgrid_delivery.accepted");
  assert.ok(accepted, "observe mode should log accepted");
  assert.equal(accepted.meta.downstream_handler_invoked, false);
  assert.equal(accepted.meta.final_response_status, null);

  const started = entries.find((entry) => entry.event === "textgrid_delivery.handler_started");
  assert.ok(started, "observe mode should start delivery handler");
  assert.equal(started.meta.downstream_handler_invoked, true);
  assert.equal(started.meta.podio_persistence_attempted, true);

  const completed = entries.find((entry) => entry.event === "textgrid_delivery.handler_completed");
  assert.ok(completed, "observe mode should complete delivery handler");
  assert.equal(completed.meta.downstream_handler_invoked, true);
  assert.equal(completed.meta.podio_persistence_attempted, true);

  const responseLog = entries.find((entry) => entry.event === "textgrid_delivery.response_sent");
  assert.ok(responseLog, "observe mode should log response_sent");
  assert.equal(responseLog.meta.final_response_status, 200);
  assert.equal(responseLog.meta.downstream_handler_invoked, true);
  assert.equal(responseLog.meta.podio_persistence_attempted, true);

  const normalizedIndex = eventIndex(entries, "textgrid_delivery.normalized");
  const acceptedIndex = eventIndex(entries, "textgrid_delivery.accepted");
  const startedIndex = eventIndex(entries, "textgrid_delivery.handler_started");
  const responseIndex = eventIndex(entries, "textgrid_delivery.response_sent");
  assert.ok(normalizedIndex > -1 && acceptedIndex > normalizedIndex, "accepted should follow normalized");
  assert.ok(acceptedIndex > -1 && startedIndex > acceptedIndex, "handler_started should follow accepted");
  assert.ok(responseIndex > startedIndex, "response_sent should follow handler_started");
});

test("handleTextgridDeliveryRequest: off mode accepts request and skips verification", async (t) => {
  setSignatureMode(t, "off");

  const form_params = {
    SmsSid: "SM-delivery-off-1",
    MessageStatus: "sent",
    From: "+12085550111",
    To: "+12085550222",
  };
  const raw_body = new URLSearchParams(form_params).toString();
  const { entries, logger } = makeLogger();
  let handled_payload = null;

  const response = await handleTextgridDeliveryRequest(
    new Request(DELIVERY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "off-mode-signature",
      },
      body: raw_body,
    }),
    {
      logger,
      verifyTextgridWebhookSignatureImpl: () => {
        throw new Error("verify should not run in off mode");
      },
      handleTextgridDeliveryImpl: async (payload) => {
        handled_payload = payload;
        return { ok: true, normalized_state: "Sent" };
      },
    }
  );

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(handled_payload?.signature_verification_mode, "off");
  assert.equal(handled_payload?.signature_verified, false);
  assert.equal(handled_payload?.signature_bypassed, true);
  assert.equal(handled_payload?.signature_failure_reason, "signature_verification_disabled");
  assert.equal(handled_payload?.signature_header_name, "x-textgrid-signature");
  assert.equal(handled_payload?.signature_unverified_observe_mode, false);

  const warn = entries.find(
    (entry) => entry.event === "textgrid_delivery.signature_verification_disabled"
  );
  assert.ok(warn, "off mode should log disabled verification");
  assert.equal(warn.meta.signature_verification_disabled, true);
  assert.equal(warn.meta.signature_verification_mode, "off");
  assert.equal(warn.meta.signature_bypassed, true);

  const normalized = entries.find((entry) => entry.event === "textgrid_delivery.normalized");
  assert.ok(normalized, "off mode should log normalized");
  const branch = entries.find((entry) => entry.event === "textgrid_delivery.signature_branch_selected");
  assert.ok(branch, "off mode should log signature branch");
  assert.equal(branch.meta.signature_verification_mode, "off");
  assert.equal(branch.meta.will_continue_after_signature_check, true);

  const accepted = entries.find((entry) => entry.event === "textgrid_delivery.accepted");
  assert.ok(accepted, "off mode should log accepted");

  const started = entries.find((entry) => entry.event === "textgrid_delivery.handler_started");
  assert.ok(started, "off mode should start delivery handler");
  assert.equal(started.meta.downstream_handler_invoked, true);

  const completed = entries.find((entry) => entry.event === "textgrid_delivery.handler_completed");
  assert.ok(completed, "off mode should complete delivery handler");
  assert.equal(completed.meta.downstream_handler_invoked, true);

  const responseLog = entries.find((entry) => entry.event === "textgrid_delivery.response_sent");
  assert.ok(responseLog, "off mode should log response_sent");
  assert.equal(responseLog.meta.final_response_status, 200);
  assert.equal(responseLog.meta.downstream_handler_invoked, true);
});

test("textgrid inbound route: strict mode rejects invalid signature with 401", async (t) => {
  setSignatureMode(t, "strict");

  const { entries, logger } = makeLogger();
  let handled_payload = null;

  __setTextgridInboundRouteTestDeps({
    logger,
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async (payload) => {
      handled_payload = payload;
      return { ok: true };
    },
    verifyTextgridWebhookRequestImpl: (opts) =>
      verifyTextgridWebhookRequest({
        ...opts,
        auth_token: TEST_AUTH_TOKEN,
        webhook_secret: TEST_WEBHOOK_SECRET,
      }),
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
  });

  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "strict-mode-invalid-signature",
      },
      body: new URLSearchParams({
        SmsMessageSid: "SM-inbound-strict-1",
        From: "+15550001234",
        To: "+15559876543",
        Body: "Hello inbound strict",
        SmsStatus: "received",
      }),
    })
  );
  const payload = await response.json();

  assert.equal(response.status, 401);
  assert.equal(payload.ok, false);
  assert.equal(payload.error, "invalid_textgrid_signature");
  assert.equal(payload.verification.signature_verification_mode, "strict");
  assert.equal(payload.verification.signature_bypassed, false);
  assert.equal(handled_payload, null);

  const normalized = entries.find((entry) => entry.event === "textgrid_inbound.normalized");
  assert.ok(normalized, "strict mode should log normalized");
  const branch = entries.find((entry) => entry.event === "textgrid_inbound.signature_branch_selected");
  assert.ok(branch, "strict mode should log signature branch");
  assert.equal(branch.meta.signature_verification_mode, "strict");
  assert.equal(branch.meta.will_continue_after_signature_check, false);

  const warn = entries.find((entry) => entry.event === "textgrid_inbound.invalid_signature");
  assert.ok(warn, "strict mode should log invalid signature");
  assert.equal(warn.meta.signature_verification_mode, "strict");
  assert.equal(warn.meta.signature_header_name, "x-textgrid-signature");
  assert.equal(warn.meta.request_path, "/api/webhooks/textgrid/inbound");
  assert.equal(warn.meta.downstream_handler_invoked, false);
  assert.equal(warn.meta.podio_persistence_attempted, false);

  const responseLog = entries.find((entry) => entry.event === "textgrid_inbound.response_sent");
  assert.ok(responseLog, "strict mode should log response_sent");
  assert.equal(responseLog.meta.final_response_status, 401);
  assert.equal(responseLog.meta.downstream_handler_invoked, false);
  assert.equal(
    entries.some((entry) => entry.event === "textgrid_inbound.handler_started"),
    false,
    "strict mode must not invoke inbound handler"
  );
});

test("textgrid inbound route: fake minimal form payload in observe mode accepts invalid signature and continues", async (t) => {
  setSignatureMode(t, "observe");

  const { entries, logger } = makeLogger();
  let handled_payload = null;

  __setTextgridInboundRouteTestDeps({
    logger,
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async (payload) => {
      handled_payload = payload;
      return { ok: true, message_id: payload.message_id };
    },
    verifyTextgridWebhookRequestImpl: (opts) =>
      verifyTextgridWebhookRequest({
        ...opts,
        auth_token: TEST_AUTH_TOKEN,
        webhook_secret: TEST_WEBHOOK_SECRET,
      }),
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
  });

  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "observe-mode-invalid-signature",
      },
      body: new URLSearchParams({
        SmsMessageSid: "SM-inbound-observe-1",
        From: "+15550001234",
        To: "+15559876543",
        Body: "Hello inbound observe",
        SmsStatus: "received",
      }),
    })
  );
  const payload = await response.json();

  assert.equal(response.status, 200);
  assert.equal(payload.ok, true);
  assert.equal(handled_payload?.message_id, "SM-inbound-observe-1");
  assert.equal(handled_payload?.signature_verification_mode, "observe");
  assert.equal(handled_payload?.signature_verified, false);
  assert.equal(handled_payload?.signature_bypassed, true);
  assert.equal(
    handled_payload?.signature_failure_reason,
    "no_mode_produced_matching_digest"
  );
  assert.equal(handled_payload?.signature_header_name, "x-textgrid-signature");
  assert.equal(handled_payload?.signature_unverified_observe_mode, true);
  assert.equal(
    payload.verification.signature_unverified_observe_mode,
    true
  );

  const warn = entries.find((entry) => entry.event === "textgrid_inbound.invalid_signature");
  assert.ok(warn, "observe mode should log invalid signature");
  assert.equal(warn.meta.signature_verification_mode, "observe");
  assert.equal(warn.meta.signature_bypassed, true);
  assert.equal(warn.meta.signature_unverified_observe_mode, true);

  const normalized = entries.find((entry) => entry.event === "textgrid_inbound.normalized");
  assert.ok(normalized, "observe mode should log normalized");
  const checkpoint1 = entries.find((entry) => entry.event === "textgrid_inbound.pre_accept_checkpoint_1");
  const checkpoint2 = entries.find((entry) => entry.event === "textgrid_inbound.pre_accept_checkpoint_2");
  const checkpoint3 = entries.find((entry) => entry.event === "textgrid_inbound.pre_accept_checkpoint_3");
  assert.ok(checkpoint1, "observe mode should log pre_accept_checkpoint_1");
  assert.ok(checkpoint2, "observe mode should log pre_accept_checkpoint_2");
  assert.ok(checkpoint3, "observe mode should log pre_accept_checkpoint_3");
  const branch = entries.find((entry) => entry.event === "textgrid_inbound.signature_branch_selected");
  assert.ok(branch, "observe mode should log signature branch");
  assert.equal(branch.meta.signature_verification_mode, "observe");
  assert.equal(branch.meta.will_continue_after_signature_check, true);

  const accepted = entries.find((entry) => entry.event === "textgrid_inbound.accepted");
  assert.ok(accepted, "observe mode should log accepted");
  assert.equal(accepted.meta.downstream_handler_invoked, false);

  const mainHandlerStarted = entries.find(
    (entry) =>
      entry.event === "textgrid_inbound.handler_started" &&
      entry.meta.handler_name === "handleTextgridInbound"
  );
  assert.ok(mainHandlerStarted, "observe mode should invoke inbound handler");
  assert.equal(mainHandlerStarted.meta.downstream_handler_invoked, true);
  assert.equal(mainHandlerStarted.meta.podio_persistence_attempted, true);

  const mainHandlerCompleted = entries.find(
    (entry) =>
      entry.event === "textgrid_inbound.handler_completed" &&
      entry.meta.handler_name === "handleTextgridInbound"
  );
  assert.ok(mainHandlerCompleted, "observe mode should complete inbound handler");
  assert.equal(mainHandlerCompleted.meta.downstream_handler_invoked, true);
  assert.equal(mainHandlerCompleted.meta.podio_persistence_attempted, true);

  const responseLog = entries.find((entry) => entry.event === "textgrid_inbound.response_sent");
  assert.ok(responseLog, "observe mode should log response_sent");
  assert.equal(responseLog.meta.final_response_status, 200);
  assert.equal(responseLog.meta.downstream_handler_invoked, true);
  assert.equal(responseLog.meta.podio_persistence_attempted, true);

  const normalizedIndex = eventIndex(entries, "textgrid_inbound.normalized");
  const checkpoint1Index = eventIndex(entries, "textgrid_inbound.pre_accept_checkpoint_1");
  const branchIndex = eventIndex(entries, "textgrid_inbound.signature_branch_selected");
  const checkpoint2Index = eventIndex(entries, "textgrid_inbound.pre_accept_checkpoint_2");
  const checkpoint3Index = eventIndex(entries, "textgrid_inbound.pre_accept_checkpoint_3");
  const acceptedIndex = eventIndex(entries, "textgrid_inbound.accepted");
  const startedIndex = eventIndex(
    entries,
    "textgrid_inbound.handler_started",
    (entry) => entry.meta.handler_name === "handleTextgridInbound"
  );
  const responseIndex = eventIndex(entries, "textgrid_inbound.response_sent");
  assert.ok(checkpoint1Index > normalizedIndex, "checkpoint_1 should follow normalized");
  assert.ok(branchIndex > checkpoint1Index, "signature branch should follow checkpoint_1");
  assert.ok(checkpoint2Index > branchIndex, "checkpoint_2 should follow signature branch");
  assert.ok(checkpoint3Index > checkpoint2Index, "checkpoint_3 should follow checkpoint_2");
  assert.ok(normalizedIndex > -1 && acceptedIndex > normalizedIndex, "accepted should follow normalized");
  assert.ok(acceptedIndex > -1 && startedIndex > acceptedIndex, "handler_started should follow accepted");
  assert.ok(responseIndex > startedIndex, "response_sent should follow handler_started");
});

test("textgrid inbound route: off mode accepts request and skips verification", async (t) => {
  setSignatureMode(t, "off");

  const { entries, logger } = makeLogger();
  let handled_payload = null;

  __setTextgridInboundRouteTestDeps({
    logger,
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async (payload) => {
      handled_payload = payload;
      return { ok: true, message_id: payload.message_id };
    },
    verifyTextgridWebhookRequestImpl: () => {
      throw new Error("verify should not run in off mode");
    },
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
  });

  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "off-mode-signature",
      },
      body: new URLSearchParams({
        SmsMessageSid: "SM-inbound-off-1",
        From: "+15550001234",
        To: "+15559876543",
        Body: "Hello inbound off",
        SmsStatus: "received",
      }),
    })
  );
  const payload = await response.json();

  assert.equal(response.status, 200);
  assert.equal(payload.ok, true);
  assert.equal(handled_payload?.signature_verification_mode, "off");
  assert.equal(handled_payload?.signature_verified, false);
  assert.equal(handled_payload?.signature_bypassed, true);
  assert.equal(handled_payload?.signature_failure_reason, "signature_verification_disabled");
  assert.equal(handled_payload?.signature_header_name, "x-textgrid-signature");
  assert.equal(handled_payload?.signature_unverified_observe_mode, false);

  const warn = entries.find(
    (entry) => entry.event === "textgrid_inbound.signature_verification_disabled"
  );
  assert.ok(warn, "off mode should log disabled verification");
  assert.equal(warn.meta.signature_verification_disabled, true);
  assert.equal(warn.meta.signature_verification_mode, "off");
  assert.equal(warn.meta.signature_bypassed, true);

  const normalized = entries.find((entry) => entry.event === "textgrid_inbound.normalized");
  assert.ok(normalized, "off mode should log normalized");
  const branch = entries.find((entry) => entry.event === "textgrid_inbound.signature_branch_selected");
  assert.ok(branch, "off mode should log signature branch");
  assert.equal(branch.meta.signature_verification_mode, "off");
  assert.equal(branch.meta.will_continue_after_signature_check, true);

  const accepted = entries.find((entry) => entry.event === "textgrid_inbound.accepted");
  assert.ok(accepted, "off mode should log accepted");

  const mainHandlerStarted = entries.find(
    (entry) =>
      entry.event === "textgrid_inbound.handler_started" &&
      entry.meta.handler_name === "handleTextgridInbound"
  );
  assert.ok(mainHandlerStarted, "off mode should invoke inbound handler");
  assert.equal(mainHandlerStarted.meta.downstream_handler_invoked, true);

  const mainHandlerCompleted = entries.find(
    (entry) =>
      entry.event === "textgrid_inbound.handler_completed" &&
      entry.meta.handler_name === "handleTextgridInbound"
  );
  assert.ok(mainHandlerCompleted, "off mode should complete inbound handler");
  assert.equal(mainHandlerCompleted.meta.downstream_handler_invoked, true);

  const responseLog = entries.find((entry) => entry.event === "textgrid_inbound.response_sent");
  assert.ok(responseLog, "off mode should log response_sent");
  assert.equal(responseLog.meta.final_response_status, 200);
  assert.equal(responseLog.meta.downstream_handler_invoked, true);
});

test("textgrid inbound route: real-style form payload in observe mode reaches accepted and handler", async (t) => {
  setSignatureMode(t, "observe");

  const { entries, logger } = makeLogger();
  let handled_payload = null;

  __setTextgridInboundRouteTestDeps({
    logger,
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async (payload) => {
      handled_payload = payload;
      return { ok: true, message_id: payload.message_id };
    },
    verifyTextgridWebhookRequestImpl: (opts) =>
      verifyTextgridWebhookRequest({
        ...opts,
        auth_token: TEST_AUTH_TOKEN,
        webhook_secret: TEST_WEBHOOK_SECRET,
      }),
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
  });

  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "observe-mode-invalid-real-style",
      },
      body: new URLSearchParams({
        SmsMessageSid: "SM-inbound-real-1",
        MessageSid: "SM-inbound-real-1",
        AccountSid: "AC-real-1",
        ApiVersion: "2010-04-01",
        From: "+15551112222",
        To: "+15553334444",
        Body: "Real style inbound observe payload",
        SmsStatus: "received",
      }),
    })
  );
  const payload = await response.json();

  assert.equal(response.status, 200);
  assert.equal(payload.ok, true);
  assert.equal(handled_payload?.message_id, "SM-inbound-real-1");

  const normalizedIndex = eventIndex(entries, "textgrid_inbound.normalized");
  const branchIndex = eventIndex(entries, "textgrid_inbound.signature_branch_selected");
  const acceptedIndex = eventIndex(entries, "textgrid_inbound.accepted");
  const startedIndex = eventIndex(
    entries,
    "textgrid_inbound.handler_started",
    (entry) => entry.meta.handler_name === "handleTextgridInbound"
  );
  const responseIndex = eventIndex(entries, "textgrid_inbound.response_sent");

  assert.ok(normalizedIndex > -1, "real-style payload should log normalized");
  assert.ok(branchIndex > normalizedIndex, "real-style payload should log signature branch");
  assert.ok(acceptedIndex > branchIndex, "real-style payload should log accepted");
  assert.ok(startedIndex > acceptedIndex, "real-style payload should log handler_started");
  assert.ok(responseIndex > startedIndex, "real-style payload should log response_sent");
});

test("handleTextgridDeliveryRequest: failure before accepted emits failed_before_accept", async (t) => {
  setSignatureMode(t, "observe");

  const entries = [];
  const logger = {
    info: (event, meta) => entries.push({ level: "info", event, meta }),
    warn: (event, meta) => {
      entries.push({ level: "warn", event, meta });
      if (event === "textgrid_delivery.invalid_signature") {
        throw new Error("delivery_pre_accept_failure");
      }
    },
    error: (event, meta) => entries.push({ level: "error", event, meta }),
  };

  const response = await handleTextgridDeliveryRequest(
    new Request(DELIVERY_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "observe-mode-invalid-signature",
      },
      body: new URLSearchParams({
        SmsSid: "SM-delivery-fail-before-accept",
        MessageStatus: "delivered",
        From: "+12085550111",
        To: "+12085550222",
      }),
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

  assert.equal(response.status, 500);
  assert.equal(response.payload.error, "textgrid_delivery_failed");
  assert.equal(eventIndex(entries, "textgrid_delivery.accepted"), -1);

  const failed = entries.find((entry) => entry.event === "textgrid_delivery.failed_before_accept");
  assert.ok(failed, "should log failed_before_accept");
  assert.equal(failed.meta.error_message, "delivery_pre_accept_failure");
  assert.equal(failed.meta.signature_verification_mode, "observe");

  const responseLog = entries.find((entry) => entry.event === "textgrid_delivery.response_sent");
  assert.ok(responseLog, "should log response_sent on pre-accept failure");
  assert.equal(responseLog.meta.final_response_status, 500);

  const normalizedIndex = eventIndex(entries, "textgrid_delivery.normalized");
  const failedIndex = eventIndex(entries, "textgrid_delivery.failed_before_accept");
  assert.ok(normalizedIndex > -1 && failedIndex > normalizedIndex);
});

test("textgrid inbound route: failure before accepted emits failed_pre_accept", async (t) => {
  setSignatureMode(t, "observe");

  const { entries, logger } = makeLogger();

  __setTextgridInboundRouteTestDeps({
    logger,
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async () => ({ ok: true }),
    normalizeTextgridInboundPayloadImpl: () => {
      const payload = {
        provider: "textgrid",
        raw: {},
        message_id: "SM-inbound-fail-before-accept",
        from: "+15550001234",
        to: "+15559876543",
        status: "received",
        header_signature: "observe-mode-invalid-signature",
        header_signature_name: "x-textgrid-signature",
        header_event: "inbound",
      };

      Object.defineProperty(payload, "message", {
        enumerable: true,
        configurable: true,
        get() {
          throw new Error("inbound_pre_accept_failure");
        },
      });

      return payload;
    },
    verifyTextgridWebhookRequestImpl: (opts) =>
      verifyTextgridWebhookRequest({
        ...opts,
        auth_token: TEST_AUTH_TOKEN,
        webhook_secret: TEST_WEBHOOK_SECRET,
      }),
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
  });

  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "x-textgrid-signature": "observe-mode-invalid-signature",
      },
      body: new URLSearchParams({
        SmsMessageSid: "SM-inbound-fail-before-accept",
        From: "+15550001234",
        To: "+15559876543",
        Body: "Hello inbound pre accept fail",
        SmsStatus: "received",
      }),
    })
  );
  const payload = await response.json();

  assert.equal(response.status, 500);
  assert.equal(payload.error, "textgrid_inbound_failed");
  assert.equal(eventIndex(entries, "textgrid_inbound.accepted"), -1);

  const failed = entries.find((entry) => entry.event === "textgrid_inbound.failed_pre_accept");
  assert.ok(failed, "should log failed_pre_accept");
  assert.equal(failed.meta.error_message, "inbound_pre_accept_failure");
  assert.equal(failed.meta.signature_verification_mode, "observe");
  assert.deepEqual(failed.meta.parsed_body_keys, ["SmsMessageSid", "From", "To", "Body", "SmsStatus"]);

  const responseLog = entries.find((entry) => entry.event === "textgrid_inbound.response_sent");
  assert.ok(responseLog, "should log response_sent on pre-accept failure");
  assert.equal(responseLog.meta.final_response_status, 500);

  const normalizedIndex = eventIndex(entries, "textgrid_inbound.normalized");
  const checkpoint1Index = eventIndex(entries, "textgrid_inbound.pre_accept_checkpoint_1");
  const failedIndex = eventIndex(entries, "textgrid_inbound.failed_pre_accept");
  assert.ok(normalizedIndex > -1 && failedIndex > normalizedIndex);
  assert.ok(checkpoint1Index > normalizedIndex && failedIndex > checkpoint1Index);
});
