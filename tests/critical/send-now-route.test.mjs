import test from "node:test";
import assert from "node:assert/strict";

import {
  handleSendNowRequestData,
  normalizeSendNowInput,
} from "@/lib/domain/outbound/send-now-request.js";

function makeLogger() {
  const entries = [];
  return {
    entries,
    logger: {
      info: (event, meta) => entries.push({ level: "info", event, meta }),
      error: (event, meta) => entries.push({ level: "error", event, meta }),
    },
  };
}

test("normalizeSendNowInput keeps missing touch_number as null", () => {
  const normalized = normalizeSendNowInput({
    phone: "12087034955",
  });

  assert.equal(normalized.touch_number, null);
  assert.equal(normalized.rendered_message_text, null);
});

test("send-now POST accepts custom message_text and forwards it as a real override", async () => {
  const calls = {
    queued: null,
    processed: null,
  };
  const { logger } = makeLogger();

  const response = await handleSendNowRequestData(
    new Request("http://localhost/api/internal/outbound/send-now", {
      method: "POST",
      body: JSON.stringify({
        phone: "12087034955",
        message_text: "Got it Jose, thanks. Would you be open to an offer on 5521 Laster Ln?",
      }),
      headers: {
        "Content-Type": "application/json",
      },
    }),
    "POST",
    {
      logger,
      queueOutboundMessageImpl: async (payload) => {
        calls.queued = payload;
        return {
          ok: true,
          queue_item_id: 7001,
        };
      },
      processSendQueueImpl: async (payload) => {
        calls.processed = payload;
        return {
          ok: true,
          sent: true,
        };
      },
    }
  );

  assert.equal(response.status, 200);
  assert.equal(calls.queued.phone, "12087034955");
  assert.equal(calls.queued.touch_number, null);
  assert.equal(
    calls.queued.rendered_message_text,
    "Got it Jose, thanks. Would you be open to an offer on 5521 Laster Ln?"
  );
  assert.deepEqual(calls.processed, { queue_item_id: 7001 });

  const payload = response.payload;
  assert.equal(payload.ok, true);
  assert.equal(payload.result.queued.queue_item_id, 7001);
});

test("send-now GET keeps missing touch_number null and accepts rendered_message_text alias", async () => {
  let queued_payload = null;

  const response = await handleSendNowRequestData(
    new Request(
      "http://localhost/api/internal/outbound/send-now?phone=12087034955&rendered_message_text=Manual%20reply"
    ),
    "GET",
    {
      logger: makeLogger().logger,
      queueOutboundMessageImpl: async (payload) => {
        queued_payload = payload;
        return {
          ok: true,
          queue_item_id: 7002,
        };
      },
      processSendQueueImpl: async () => ({
        ok: true,
        sent: true,
      }),
    }
  );

  assert.equal(response.status, 200);
  assert.equal(queued_payload.touch_number, null);
  assert.equal(queued_payload.rendered_message_text, "Manual reply");
});

test("send-now route returns a structured queue-build failure instead of a blind 500", async () => {
  const { entries, logger } = makeLogger();

  const response = await handleSendNowRequestData(
    new Request("http://localhost/api/internal/outbound/send-now", {
      method: "POST",
      body: JSON.stringify({
        phone: "12087034955",
        message_text: "Manual reply",
      }),
      headers: {
        "Content-Type": "application/json",
      },
    }),
    "POST",
    {
      logger,
      queueOutboundMessageImpl: async () => {
        throw new Error("queue_override_failed");
      },
    }
  );

  assert.equal(response.status, 400);
  const payload = response.payload;
  assert.equal(payload.ok, false);
  assert.equal(payload.result.queued.stage, "queue_build");
  assert.equal(payload.result.queued.reason, "queue_build_failed");
  assert.equal(payload.result.queued.message, "queue_override_failed");

  const completed_entry = entries.find((entry) => entry.event === "outbound_send_now.completed");
  assert.ok(completed_entry, "expected completion log entry");
  assert.equal(completed_entry.meta.phone, "12087034955");
  assert.equal(completed_entry.meta.message_override_present, true);
  assert.equal(completed_entry.meta.queued_stage, "queue_build");
  assert.equal(completed_entry.meta.queued_reason, "queue_build_failed");
});

test("send-now route returns a structured queue-processing failure when immediate send crashes", async () => {
  const response = await handleSendNowRequestData(
    new Request("http://localhost/api/internal/outbound/send-now", {
      method: "POST",
      body: JSON.stringify({
        phone: "12087034955",
        message_text: "Manual reply",
      }),
      headers: {
        "Content-Type": "application/json",
      },
    }),
    "POST",
    {
      logger: makeLogger().logger,
      queueOutboundMessageImpl: async () => ({
        ok: true,
        queue_item_id: 7003,
      }),
      processSendQueueImpl: async () => {
        throw new Error("send_stage_failed");
      },
    }
  );

  assert.equal(response.status, 400);
  assert.equal(response.payload.ok, false);
  assert.equal(response.payload.result.processed.stage, "queue_processing");
  assert.equal(response.payload.result.processed.reason, "queue_processing_failed");
  assert.equal(response.payload.result.processed.message, "send_stage_failed");
});
