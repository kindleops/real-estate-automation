import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { runDevSendTest } from "@/app/api/dev/send-test/route.js";
import {
  finalizeSendQueueSuccess,
  normalizeSendQueueRow,
  selectAvailableTextgridNumber,
  writeOutboundSuccessMessageEvent,
  writeWebhookLog,
} from "@/lib/supabase/sms-engine.js";

function makeSelectSupabase(rows = []) {
  return {
    from() {
      const query = {
        select() {
          return query;
        },
        in() {
          return query;
        },
        order() {
          return query;
        },
        limit() {
          return Promise.resolve({
            data: rows,
            error: null,
          });
        },
      };
      return query;
    },
  };
}

function makeNumbersSupabase(rows = []) {
  return {
    from() {
      const query = {
        select() {
          return query;
        },
        order() {
          return query;
        },
        limit() {
          return Promise.resolve({
            data: rows,
            error: null,
          });
        },
      };
      return query;
    },
  };
}

test("runSendQueue uses the Supabase candidate path, claims rows, and passes the claim token into processing", async () => {
  const processed = [];
  const info_calls = [];

  const row = normalizeSendQueueRow({
    id: 11,
    queue_key: "queue-11",
    queue_id: "queue-11",
    queue_status: "queued",
    scheduled_for: "2026-04-18T12:00:00.000Z",
    retry_count: 0,
    max_retries: 3,
    message_body: "Hello world",
    to_phone_number: "+16127433952",
    from_phone_number: "+16128060495",
  });

  const result = await runSendQueue(
    {
      limit: 10,
      now: "2026-04-18T15:00:00.000Z",
    },
    {
      supabase: makeSelectSupabase([row]),
      claimSendQueueRow: async (candidate) => ({
        ok: true,
        claimed: true,
        row: {
          ...candidate,
          queue_status: "sending",
          lock_token: "lock-11",
          is_locked: true,
        },
        lock_token: "lock-11",
      }),
      processSendQueueItem: async (candidate, deps) => {
        processed.push({
          candidate,
          lock_token: deps.claimedLockToken,
        });
        return {
          ok: true,
          sent: true,
          provider_message_id: "SM-11",
        };
      },
      withRunLock: async ({ fn }) => fn(),
      info: (event, meta) => info_calls.push({ event, meta }),
      warn: () => {},
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.sent_count, 1);
  assert.equal(result.claimed_count, 1);
  assert.equal(processed.length, 1);
  assert.equal(processed[0].candidate.id, 11);
  assert.equal(processed[0].lock_token, "lock-11");

  const candidates_loaded = info_calls.find(
    (entry) => entry.event === "queue.run_candidates_loaded"
  );
  assert.ok(candidates_loaded);
  assert.equal(candidates_loaded.meta.total_rows_loaded, 1);
  assert.equal(candidates_loaded.meta.runnable_count, 1);
});

test("finalizeSendQueueSuccess refuses to mark a Supabase queue row sent without a provider SID", async () => {
  await assert.rejects(
    () =>
      finalizeSendQueueSuccess(
        {
          id: 99,
          queue_key: "queue-99",
          queue_status: "sending",
          message_body: "Hello",
          to_phone_number: "+16127433952",
          from_phone_number: "+16128060495",
        },
        "lock-99",
        {
          status: "queued",
        },
        {
          updateSendQueueRowWithLock: async () => {
            throw new Error("should_not_run");
          },
        }
      ),
    /SEND FAILED - NO SID/
  );
});

test("selectAvailableTextgridNumber prefers the row linked on textgrid_number_id when from_phone_number is missing", async () => {
  const selection = await selectAvailableTextgridNumber(
    {
      id: 1,
      queue_key: "queue-1",
      queue_status: "queued",
      message_body: "Hello",
      to_phone_number: "+16127433952",
      textgrid_number_id: 22,
    },
    {
      supabase: makeNumbersSupabase([
        {
          id: 21,
          phone_number: "+16128060001",
          status: "active",
          messages_sent_today: 10,
          daily_limit: 100,
        },
        {
          id: 22,
          phone_number: "+16128060002",
          status: "active",
          messages_sent_today: 15,
          daily_limit: 100,
        },
      ]),
    }
  );

  assert.equal(selection.ok, true);
  assert.equal(selection.reason, "preferred_textgrid_number_selected");
  assert.equal(selection.selected.id, 22);
  assert.equal(selection.from_phone_number, "+16128060002");
});

test("writeOutboundSuccessMessageEvent builds the canonical outbound_send payload", async () => {
  let captured = null;

  await writeOutboundSuccessMessageEvent(
    {
      id: 7,
      queue_key: "queue-7",
      queue_status: "queued",
      message_body: "Hello there",
      to_phone_number: "+16127433952",
      from_phone_number: "+16128060495",
      master_owner_id: "mo-1",
      property_id: "prop-1",
    },
    {
      sid: "SM-7",
      status: "queued",
    },
    {
      now: "2026-04-18T18:00:00.000Z",
      latency_ms: 123,
      writeOutboundSuccessMessageEvent: async (payload) => {
        captured = payload;
        return payload;
      },
    }
  );

  assert.ok(captured);
  assert.equal(captured.message_event_key, "outbound_queue-7");
  assert.equal(captured.provider_message_sid, "SM-7");
  assert.equal(captured.event_type, "outbound_send");
  assert.equal(captured.delivery_status, "sent");
  assert.equal(captured.character_count, "Hello there".length);
  assert.equal(captured.metadata.source, "supabase_send_queue");
});

test("writeWebhookLog forwards a structured raw payload for TextGrid webhooks", async () => {
  let captured = null;

  await writeWebhookLog({
    event_type: "delivery",
    direction: "outbound",
    provider_message_sid: "SM-123",
    payload: {
      status: "delivered",
    },
    headers: {
      "x-textgrid-event": "delivery",
    },
    received_at: "2026-04-18T18:10:00.000Z",
    writeWebhookLog: async (payload) => {
      captured = payload;
      return payload;
    },
  });

  assert.ok(captured);
  assert.equal(captured.provider, "textgrid");
  assert.equal(captured.event_type, "delivery");
  assert.equal(captured.direction, "outbound");
  assert.equal(captured.provider_message_sid, "SM-123");
});

test("runDevSendTest inserts a canonical queued row and optionally runs the queue immediately", async () => {
  let inserted_payload = null;
  let run_called = false;

  const result = await runDevSendTest({
    request_url: "http://localhost/api/dev/send-test?run_now=true",
    insertSupabaseSendQueueRowImpl: async (payload) => {
      inserted_payload = payload;
      return {
        ok: true,
        item_id: 501,
        queue_id: payload.queue_id,
        queue_key: payload.queue_key,
        raw: payload,
      };
    },
    runSendQueueImpl: async () => {
      run_called = true;
      return {
        ok: true,
        sent_count: 1,
      };
    },
  });

  assert.equal(result.ok, true);
  assert.equal(run_called, true);
  assert.ok(inserted_payload);
  assert.equal(inserted_payload.queue_status, "queued");
  assert.equal(inserted_payload.send_priority, 10);
  assert.equal(inserted_payload.to_phone_number, "+16127433952");
  assert.equal(inserted_payload.from_phone_number, "+16128060495");
  assert.equal(inserted_payload.metadata.source, "dev_send_test");
});
