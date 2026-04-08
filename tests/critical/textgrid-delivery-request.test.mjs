import test from "node:test";
import assert from "node:assert/strict";

import { normalizeTextgridDeliveryPayload } from "@/lib/webhooks/textgrid-delivery-normalize.js";
import { handleTextgridDeliveryRequest } from "@/lib/webhooks/textgrid-delivery-request.js";

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

test("normalizeTextgridDeliveryPayload maps Twilio-style sent payload fields", () => {
  const normalized = normalizeTextgridDeliveryPayload(
    {
      SmsSid: "SM123",
      SmsStatus: "sent",
      MessageSid: "SM123",
      From: "+12085550111",
      To: "+12085550222",
      AccountSid: "AC123",
      ApiVersion: "2010-04-01",
      NumSegments: "1",
    },
    new Headers({
      "x-twilio-signature": "sig-123",
    })
  );

  assert.equal(normalized.message_id, "SM123");
  assert.equal(normalized.status, "sent");
  assert.equal(normalized.from, "+12085550111");
  assert.equal(normalized.to, "+12085550222");
  assert.equal(normalized.account_id, "AC123");
  assert.equal(normalized.api_version, "2010-04-01");
  assert.equal(normalized.segments, "1");
  assert.equal(normalized.header_signature, "sig-123");
});

test("normalizeTextgridDeliveryPayload accepts lowercase and underscored provider keys", () => {
  const normalized = normalizeTextgridDeliveryPayload({
    sms_sid: "SM-lower-1",
    message_status: "delivered",
    from_number: "+12085550111",
    to_number: "+12085550222",
    account_sid: "AC-lower",
    num_segments: "2",
  });

  assert.equal(normalized.message_id, "SM-lower-1");
  assert.equal(normalized.status, "delivered");
  assert.equal(normalized.from, "+12085550111");
  assert.equal(normalized.to, "+12085550222");
  assert.equal(normalized.account_id, "AC-lower");
  assert.equal(normalized.segments, "2");
});

test("handleTextgridDeliveryRequest accepts form-encoded TextGrid delivered callbacks and returns 200", async () => {
  const { logger } = makeLogger();
  let handled_payload = null;

  const response = await handleTextgridDeliveryRequest(
    new Request("http://localhost/api/webhooks/textgrid/delivery", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        SmsSid: "SM-delivered-1",
        MessageSid: "SM-delivered-1",
        MessageStatus: "delivered",
        From: "+12085550111",
        To: "+12085550222",
        AccountSid: "AC999",
      }),
    }),
    {
      logger,
      verifyTextgridWebhookSignatureImpl: () => ({
        ok: true,
        verified: false,
        required: false,
        reason: "webhook_secret_not_configured",
      }),
      handleTextgridDeliveryImpl: async (payload) => {
        handled_payload = payload;
        return {
          ok: true,
          normalized_state: "Delivered",
        };
      },
    }
  );

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(handled_payload.message_id, "SM-delivered-1");
  assert.equal(handled_payload.status, "delivered");
  assert.equal(handled_payload.from, "+12085550111");
  assert.equal(handled_payload.to, "+12085550222");
});

test("handleTextgridDeliveryRequest accepts form-encoded TextGrid sent callbacks and returns 200", async () => {
  const { logger } = makeLogger();
  let handled_payload = null;

  const response = await handleTextgridDeliveryRequest(
    new Request("http://localhost/api/webhooks/textgrid/delivery", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        SmsSid: "SM-sent-1",
        SmsStatus: "sent",
        From: "+12085550111",
        To: "+12085550222",
        AccountSid: "AC777",
      }),
    }),
    {
      logger,
      verifyTextgridWebhookSignatureImpl: () => ({
        ok: true,
        verified: false,
        required: false,
        reason: "webhook_secret_not_configured",
      }),
      handleTextgridDeliveryImpl: async (payload) => {
        handled_payload = payload;
        return {
          ok: true,
          normalized_state: "Sent",
        };
      },
    }
  );

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(handled_payload.message_id, "SM-sent-1");
  assert.equal(handled_payload.status, "sent");
  assert.equal(handled_payload.from, "+12085550111");
  assert.equal(handled_payload.to, "+12085550222");
});

test("handleTextgridDeliveryRequest reparses raw provider payloads even when content-type is text/plain", async () => {
  const { entries, logger } = makeLogger();
  let handled_payload = null;

  const response = await handleTextgridDeliveryRequest(
    new Request("http://localhost/api/webhooks/textgrid/delivery", {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
      },
      body:
        "SmsSid=SM-raw-1&SmsStatus=sent&From=%2B12085550111&To=%2B12085550222&ApiVersion=2010-04-01",
    }),
    {
      logger,
      verifyTextgridWebhookSignatureImpl: () => ({
        ok: true,
        verified: false,
        required: false,
        reason: "webhook_secret_not_configured",
      }),
      handleTextgridDeliveryImpl: async (payload) => {
        handled_payload = payload;
        return {
          ok: true,
          normalized_state: "Sent",
        };
      },
    }
  );

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(handled_payload.message_id, "SM-raw-1");
  assert.equal(handled_payload.status, "sent");
  assert.equal(handled_payload.api_version, "2010-04-01");
  assert.equal(
    entries.some((entry) => entry.event === "textgrid_delivery.invalid_payload"),
    false
  );
});
