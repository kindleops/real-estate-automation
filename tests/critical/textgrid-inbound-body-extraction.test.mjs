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
