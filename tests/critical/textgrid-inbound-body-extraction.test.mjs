/**
 * textgrid-inbound-body-extraction.test.mjs
 *
 * Focused tests for TextGrid inbound SMS body extraction.
 *
 * Coverage:
 *  1. JSON payload with Body writes message_body
 *  2. JSON payload with body (lowercase) writes message_body
 *  3. JSON payload with MessageBody writes message_body
 *  4. JSON payload with nested payload.body writes message_body
 *  5. Form-urlencoded payload with Body writes message_body
 *  6. Missing body sets body_missing = true in metadata and does not crash
 *  7. webhook_log is written before logInboundMessageEvent when from is missing
 *  8. Production-realistic full Twilio/TextGrid form payload extracts body end-to-end
 *  9. Production-realistic form payload: logInboundMessageEvent writes correct message_body
 */

import test from "node:test";
import assert from "node:assert/strict";

import { normalizeTextgridInboundPayload } from "@/lib/webhooks/textgrid-inbound-normalize.js";
import { logInboundMessageEvent } from "@/lib/supabase/sms-engine.js";
import {
  POST as postTextgridInbound,
  __setTextgridInboundRouteTestDeps,
  __resetTextgridInboundRouteTestDeps,
} from "@/app/api/webhooks/textgrid/inbound/route.js";

const INBOUND_URL = "http://localhost:3000/api/webhooks/textgrid/inbound";

// ── 1. JSON Body (capital B) ────────────────────────────────────────────────

test("inbound JSON with Body writes message_body", () => {
  const payload = normalizeTextgridInboundPayload(
    { From: "+15550001111", To: "+15559990000", Body: "Yes I am interested", MessageSid: "SM001" },
    new Headers()
  );
  assert.equal(payload.message_body, "Yes I am interested");
  assert.equal(payload.message, "Yes I am interested");
  assert.equal(payload.body_source, "Body");
});

// ── 2. JSON body (lowercase) ────────────────────────────────────────────────

test("inbound JSON with body (lowercase) writes message_body", () => {
  const payload = normalizeTextgridInboundPayload(
    { From: "+15550001111", To: "+15559990000", body: "Call me back", MessageSid: "SM002" },
    new Headers()
  );
  assert.equal(payload.message_body, "Call me back");
  assert.equal(payload.body_source, "body");
});

// ── 3. JSON MessageBody ─────────────────────────────────────────────────────

test("inbound JSON with MessageBody writes message_body", () => {
  const payload = normalizeTextgridInboundPayload(
    { From: "+15550001111", To: "+15559990000", MessageBody: "Stop texting", MessageSid: "SM003" },
    new Headers()
  );
  assert.equal(payload.message_body, "Stop texting");
  assert.equal(payload.body_source, "MessageBody");
});

// ── 4. Nested payload.body ──────────────────────────────────────────────────

test("inbound JSON with nested payload.body writes message_body", () => {
  const payload = normalizeTextgridInboundPayload(
    {
      from: "+15550001111",
      to: "+15559990000",
      sid: "SM004",
      payload: { body: "Nested text here" },
    },
    new Headers()
  );
  assert.equal(payload.message_body, "Nested text here");
  assert.equal(payload.body_source, "payload.body");
});

// ── 5. Form-urlencoded Body → normalizer + sms-engine ─────────────────────
//   We test via the route using injected deps so we can verify the
//   logSupabaseInboundMessageEventImpl receives a non-null message_body.

test("inbound form-urlencoded Body writes message_body in message event", async (t) => {
  process.env.SUPABASE_URL = "https://fake.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "fake-service-role-key";

  let captured_event = null;
  let webhook_log_calls = 0;

  __setTextgridInboundRouteTestDeps({
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async () => ({ ok: true }),
    verifyTextgridWebhookRequestImpl: () => ({ ok: true, required: false }),
    writeWebhookLogImpl: async () => { webhook_log_calls++; },
    logSupabaseInboundMessageEventImpl: async (payload) => {
      captured_event = payload;
    },
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
    delete process.env.SUPABASE_URL;
    delete process.env.SUPABASE_SERVICE_ROLE_KEY;
  });

  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        From: "+15550001111",
        To: "+15559990000",
        Body: "Form encoded reply",
        MessageSid: "SM005",
        SmsStatus: "received",
      }),
    })
  );

  assert.equal(response.status, 200);
  assert.ok(captured_event, "logSupabaseInboundMessageEventImpl should have been called");
  assert.equal(captured_event?.message_body, "Form encoded reply");
  assert.equal(captured_event?.body_source, "Body");
});

// ── 6. Missing body → body_missing metadata, no crash ─────────────────────
//   Call logInboundMessageEvent directly so we can inspect the built event,
//   which is where body_missing metadata is stamped (not on the raw payload).

test("missing body sets body_missing in metadata and does not crash", async () => {
  let captured_event = null;

  await logInboundMessageEvent(
    { message_id: "SM006", from: "+15550001111", to: "+15559990000" },
    {
      logInboundMessageEvent: (event) => {
        captured_event = event;
        return event;
      },
      now: "2026-04-19T00:00:00.000Z",
    }
  );

  assert.ok(captured_event, "logInboundMessageEvent must call the injected callback");
  assert.equal(captured_event.message_body, null, "message_body should be null");
  assert.equal(captured_event.metadata.body_missing, true);
  assert.ok(
    Array.isArray(captured_event.metadata.available_payload_keys),
    "available_payload_keys should be an array"
  );
  assert.equal(captured_event.direction, "inbound");
  assert.equal(captured_event.event_type, "inbound_sms");
});

// ── 7. webhook_log is written before logInboundMessageEvent (ordering) ─────
//   Send a request with no From field. The route returns 400 (invalid payload)
//   but webhook_log must still be written; logInboundMessageEvent must NOT be called.

test("webhook_log writes before logInboundMessageEvent even when from is missing", async (t) => {
  process.env.SUPABASE_URL = "https://fake.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "fake-service-role-key";

  const call_order = [];

  __setTextgridInboundRouteTestDeps({
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async () => ({ ok: true }),
    verifyTextgridWebhookRequestImpl: () => ({ ok: true, required: false }),
    writeWebhookLogImpl: async () => { call_order.push("webhook_log"); },
    logSupabaseInboundMessageEventImpl: async () => { call_order.push("message_event"); },
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
    delete process.env.SUPABASE_URL;
    delete process.env.SUPABASE_SERVICE_ROLE_KEY;
  });

  // No From field → route returns 400 after webhook_log, before message_event
  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        To: "+15559990000",
        Body: "orphan inbound",
        MessageSid: "SM007",
      }),
    })
  );

  assert.equal(response.status, 400);
  assert.ok(call_order.includes("webhook_log"), "webhook_log must be written");
  assert.ok(!call_order.includes("message_event"), "message_event must NOT be written when from is missing");
  assert.equal(call_order[0], "webhook_log", "webhook_log must be first call");
});

// ── 8. Production-realistic full Twilio/TextGrid form payload ───────────────
//   Uses the exact field set TextGrid sends in production (Twilio-compatible).
//   Ensures the normalizer and end-to-end route both extract the body correctly.

test("production-realistic full Twilio/TextGrid form payload extracts body end-to-end", async (t) => {
  process.env.SUPABASE_URL = "https://fake.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "fake-service-role-key";

  let captured_event = null;

  __setTextgridInboundRouteTestDeps({
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    maybeHandleBuyerTextgridInboundImpl: async () => ({ ok: true, matched: false }),
    handleTextgridInboundImpl: async () => ({ ok: true }),
    verifyTextgridWebhookRequestImpl: () => ({ ok: true, required: false }),
    writeWebhookLogImpl: async () => {},
    logSupabaseInboundMessageEventImpl: async (payload) => {
      captured_event = payload;
    },
  });

  t.after(() => {
    __resetTextgridInboundRouteTestDeps();
    delete process.env.SUPABASE_URL;
    delete process.env.SUPABASE_SERVICE_ROLE_KEY;
  });

  // Exact Twilio/TextGrid production field set (form-urlencoded)
  const response = await postTextgridInbound(
    new Request(INBOUND_URL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        AccountSid: "ACtest123456789",
        ApiVersion: "2010-04-01",
        Body: "Yes I want to sell",
        From: "+15550001111",
        FromCity: "CHICAGO",
        FromCountry: "US",
        FromState: "IL",
        FromZip: "60601",
        MessageSid: "SM_PROD_001",
        NumMedia: "0",
        NumSegments: "1",
        SmsMessageSid: "SM_PROD_001",
        SmsSid: "SM_PROD_001",
        SmsStatus: "received",
        To: "+15559990000",
        ToCity: "NEW YORK",
        ToCountry: "US",
        ToState: "NY",
        ToZip: "10001",
      }),
    })
  );

  assert.equal(response.status, 200, "route should return 200 for valid production payload");
  assert.ok(captured_event, "logSupabaseInboundMessageEventImpl should have been called");
  assert.equal(
    captured_event?.message_body,
    "Yes I want to sell",
    "message_body must be extracted from Body field"
  );
  assert.equal(captured_event?.body_source, "Body", "body_source must be 'Body'");
  assert.equal(captured_event?.message, "Yes I want to sell", "message field must match");
  assert.ok(
    captured_event?.from?.includes("15550001111"),
    "from phone must be normalized from From field"
  );
});

// ── 9. logInboundMessageEvent with production-normalized payload ─────────────
//   Simulates what the route passes to logInboundMessageEvent after normalization:
//   payload has .message, .message_body, .body_source set by the normalizer.

test("logInboundMessageEvent correctly stores body from normalizer output", async () => {
  // Build the normalized payload exactly as the route does
  const normalizer_output = normalizeTextgridInboundPayload(
    {
      AccountSid: "ACtest123456789",
      ApiVersion: "2010-04-01",
      Body: "Interested in selling",
      From: "+15550001111",
      FromCity: "CHICAGO",
      FromCountry: "US",
      FromState: "IL",
      FromZip: "60601",
      MessageSid: "SM_PROD_002",
      NumMedia: "0",
      NumSegments: "1",
      SmsMessageSid: "SM_PROD_002",
      SmsSid: "SM_PROD_002",
      SmsStatus: "received",
      To: "+15559990000",
    },
    new Headers()
  );

  // Verify normalizer output first
  assert.equal(normalizer_output.message, "Interested in selling");
  assert.equal(normalizer_output.message_body, "Interested in selling");
  assert.equal(normalizer_output.body_source, "Body");

  // Now verify logInboundMessageEvent produces the correct event
  let built_event = null;
  await logInboundMessageEvent(normalizer_output, {
    logInboundMessageEvent: (event) => { built_event = event; return event; },
    now: "2026-04-19T00:00:00.000Z",
  });

  assert.ok(built_event, "logInboundMessageEvent must call the injected callback");
  assert.equal(built_event.message_body, "Interested in selling", "message_body stored correctly");
  assert.equal(built_event.character_count, "Interested in selling".length);
  assert.equal(built_event.metadata.body_source, "Body");
  assert.equal(built_event.metadata.body_missing, undefined, "body_missing must not be set");
  assert.equal(built_event.direction, "inbound");
  assert.equal(built_event.event_type, "inbound_sms");
});
