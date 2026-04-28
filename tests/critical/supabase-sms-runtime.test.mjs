import test from "node:test";
import assert from "node:assert/strict";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { processSendQueueItem } from "@/lib/domain/queue/process-send-queue.js";
import {
  GET as getDevSendTest,
  POST as postDevSendTest,
  handleDevSendTestRequest,
  runDevSendTest,
} from "@/app/api/dev/send-test/route.js";
import { GET as getDevEnvCheck } from "@/app/api/dev/env-check/route.js";
import { GET as getDevForceSend, handleDevForceSendRequest } from "@/app/api/dev/force-send/route.js";
import {
  finalizeSendQueueSuccess,
  loadRunnableSendQueueRows,
  normalizeSendQueueRow,
  selectAvailableTextgridNumber,
  shouldRunSendQueueRow,
  writeOutboundSuccessMessageEvent,
  writeWebhookLog,
} from "@/lib/supabase/sms-engine.js";

function makeSelectSupabase(rows = [], calls = null) {
  return {
    from() {
      const query = {
        select() {
          calls?.push({ fn: "select", args: [] });
          return query;
        },
        eq(...args) {
          calls?.push({ fn: "eq", args });
          return query;
        },
        or(...args) {
          calls?.push({ fn: "or", args });
          return query;
        },
        not(...args) {
          calls?.push({ fn: "not", args });
          return query;
        },
        order(...args) {
          calls?.push({ fn: "order", args });
          return query;
        },
        limit() {
          calls?.push({ fn: "limit", args: [] });
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

function buildSupabaseQueueRow(overrides = {}) {
  return normalizeSendQueueRow({
    id: "sq-test-uuid-1",
    queue_key: "queue-sq-test-uuid-1",
    queue_id: "queue-sq-test-uuid-1",
    queue_status: "sending",
    scheduled_for: "2026-04-18T12:00:00.000Z",
    retry_count: 0,
    max_retries: 3,
    lock_token: "lock-sq-test-uuid-1",
    is_locked: true,
    message_body: "Hello from Supabase",
    to_phone_number: "+16127433952",
    from_phone_number: "+16128060495",
    seller_first_name: "John",
    template_id: "200194",
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        seller_first_name: "John",
      },
    },
    ...overrides,
  });
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
    seller_first_name: "John",
    template_id: "200194",
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        seller_first_name: "John",
      },
    },
  });

  const result = await runSendQueue(
    {
      limit: 10,
      now: "2026-04-18T15:00:00.000Z",
    },
    {
      getSystemFlag: async () => true,
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

test("loadRunnableSendQueueRows queries canonical queue filters and ordering only", async () => {
  const calls = [];

  const result = await loadRunnableSendQueueRows(10, {
    now: "2026-04-18T15:00:00.000Z",
    supabase: makeSelectSupabase(
      [
        {
          id: 42,
          queue_key: "queue-42",
          queue_status: "queued",
          scheduled_for: "2026-04-18T14:00:00.000Z",
          send_priority: 9,
          message_body: "Hello world",
          to_phone_number: "+16127433952",
          from_phone_number: "+12818458577",
          seller_first_name: "John",
          template_id: "200194",
          metadata: {
            selected_template_id: "200194",
            candidate_snapshot: {
              seller_first_name: "John",
            },
          },
        },
      ],
      calls
    ),
  });

  assert.equal(result.rows.length, 1);
  assert.deepEqual(
    calls.filter((entry) => entry.fn === "eq"),
    [{ fn: "eq", args: ["queue_status", "queued"] }]
  );
  assert.ok(
    calls.some(
      (entry) =>
        entry.fn === "or" &&
        String(entry.args[0] || "").includes("scheduled_for.is.null") &&
        String(entry.args[0] || "").includes("scheduled_for.lte.2026-04-18T15:00:00.000Z")
    )
  );
  assert.ok(
    calls.some(
      (entry) =>
        entry.fn === "not" &&
        entry.args[0] === "is_locked" &&
        entry.args[1] === "is" &&
        entry.args[2] === "true"
    )
  );
  assert.deepEqual(
    calls.filter((entry) => entry.fn === "order").map((entry) => entry.args[0]),
    ["send_priority", "scheduled_for"]
  );
});

test("normalizeSendQueueRow ignores scaffold-only to_number and from_number aliases", () => {
  const normalized = normalizeSendQueueRow({
    id: 501,
    queue_key: "queue-501",
    queue_status: "queued",
    message_body: "Hello there",
    to_number: "+16127433952",
    from_number: "+16128060495",
  });

  assert.equal(normalized.to_phone_number, null);
  assert.equal(normalized.from_phone_number, null);
});

test("normalizeSendQueueRow mirrors the Supabase row id into legacy-compatible aliases", () => {
  const normalized = normalizeSendQueueRow({
    id: "sq-row-501",
    queue_key: "queue-sq-row-501",
    queue_status: "queued",
    message_body: "Hello there",
    to_phone_number: "+16127433952",
  });

  assert.equal(normalized.id, "sq-row-501");
  assert.equal(normalized.queue_row_id, "sq-row-501");
  assert.equal(normalized.queue_item_id, "sq-row-501");
  assert.equal(normalized.item_id, "sq-row-501");
});

test("shouldRunSendQueueRow does not use status as the queue status alias", () => {
  const decision = shouldRunSendQueueRow(
    {
      id: 77,
      queue_key: "queue-77",
      status: "queued",
      message_body: "Hello there",
      to_phone_number: "+16127433952",
    },
    "2026-04-18T15:00:00.000Z"
  );

  assert.equal(decision.ok, false);
  assert.equal(decision.reason, "queue_status_not_queued");
});

test("normalizeSendQueueRow does not use priority as the send priority alias", () => {
  const normalized = normalizeSendQueueRow({
    id: 88,
    queue_key: "queue-88",
    queue_status: "queued",
    priority: 99,
    message_body: "Hello there",
    to_phone_number: "+16127433952",
  });

  assert.equal(normalized.send_priority, 5);
});

test("normalizeSendQueueRow prefers message_body over message_text", () => {
  const normalized = normalizeSendQueueRow({
    id: 89,
    queue_key: "queue-89",
    queue_status: "queued",
    message_body: "Primary body",
    message_text: "Compatibility body",
    to_phone_number: "+16127433952",
  });

  assert.equal(normalized.message_body, "Primary body");
  assert.equal(normalized.message_text, "Compatibility body");
});

test("normalizeSendQueueRow prefers scheduled_for over scheduled_for_utc", () => {
  const normalized = normalizeSendQueueRow({
    id: 90,
    queue_key: "queue-90",
    queue_status: "queued",
    scheduled_for: "2026-04-18T13:00:00.000Z",
    scheduled_for_utc: "2026-04-18T14:00:00.000Z",
    message_body: "Hello there",
    to_phone_number: "+16127433952",
  });

  assert.equal(normalized.scheduled_for, "2026-04-18T13:00:00.000Z");
});

test("shouldRunSendQueueRow requires to_phone_number and ignores to_number", () => {
  const decision = shouldRunSendQueueRow(
    {
      id: 91,
      queue_key: "queue-91",
      queue_status: "queued",
      message_body: "Hello there",
      to_number: "+16127433952",
    },
    "2026-04-18T15:00:00.000Z"
  );

  assert.equal(decision.ok, false);
  assert.equal(decision.reason, "missing_to_phone_number");
});

test("selectAvailableTextgridNumber uses the queue row from_phone_number when present", async () => {
  const selection = await selectAvailableTextgridNumber(
    {
      id: 92,
      queue_key: "queue-92",
      queue_status: "queued",
      message_body: "Hello",
      to_phone_number: "+16127433952",
      from_phone_number: "+16128060495",
    },
    {
      supabase: {
        from() {
          throw new Error("should_not_query_textgrid_numbers");
        },
      },
    }
  );

  assert.equal(selection.ok, true);
  assert.equal(selection.reason, "queue_row_from_phone_number_present");
  assert.equal(selection.from_phone_number, "+16128060495");
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

test("runSendQueue uses Supabase string ids in candidate summaries and result payloads", async () => {
  const processed = [];

  const row = buildSupabaseQueueRow({
    id: "sq-run-uuid-1",
    queue_key: "queue-sq-run-uuid-1",
    queue_id: "queue-sq-run-uuid-1",
    queue_status: "queued",
    lock_token: null,
    is_locked: false,
  });

  const result = await runSendQueue(
    {
      limit: 10,
      now: "2026-04-18T15:00:00.000Z",
    },
    {
      getSystemFlag: async () => true,
      supabase: makeSelectSupabase([row]),
      claimSendQueueRow: async (candidate) => ({
        ok: true,
        claimed: true,
        row: {
          ...candidate,
          queue_status: "sending",
          lock_token: "lock-sq-run-uuid-1",
          is_locked: true,
        },
        lock_token: "lock-sq-run-uuid-1",
      }),
      processSendQueueItem: async (candidate) => {
        processed.push(candidate);
        return {
          ok: true,
          sent: true,
          queue_row_id: candidate.queue_row_id,
          queue_item_id: candidate.queue_item_id,
          provider_message_id: "SM-sq-run-uuid-1",
        };
      },
      withRunLock: async ({ fn }) => fn(),
      info: () => {},
      warn: () => {},
    }
  );

  assert.deepEqual(result.first_10_candidate_item_ids, ["sq-run-uuid-1"]);
  assert.equal(processed[0].id, "sq-run-uuid-1");
  assert.equal(processed[0].queue_row_id, "sq-run-uuid-1");
  assert.equal(result.results[0].queue_row_id, "sq-run-uuid-1");
  assert.equal(result.results[0].queue_item_id, "sq-run-uuid-1");
});

test("processSendQueueItem accepts a UUID string and does not fail with missing_queue_row_id", async () => {
  const loaded_ids = [];
  const row = buildSupabaseQueueRow({
    id: "sq-process-uuid-1",
    queue_key: "queue-sq-process-uuid-1",
    queue_id: "queue-sq-process-uuid-1",
    lock_token: "lock-sq-process-uuid-1",
  });

  const result = await processSendQueueItem("sq-process-uuid-1", {
    loadQueueRowById: async (id) => {
      loaded_ids.push(id);
      return row;
    },
    updateSendQueueRowWithLock: async (row_id, lock_token, payload) => ({
      ...row,
      ...payload,
      id: row_id,
      queue_row_id: row_id,
      queue_item_id: row_id,
      item_id: row_id,
      lock_token,
    }),
    sendTextgridSMS: async () => ({
      sid: "SM-sq-process-uuid-1",
      raw: { status: "queued" },
    }),
    writeOutboundSuccessMessageEvent: async () => ({ item_id: "evt-1" }),
  });

  assert.deepEqual(loaded_ids, ["sq-process-uuid-1"]);
  assert.equal(result.sent, true);
  assert.equal(result.queue_row_id, "sq-process-uuid-1");
  assert.notEqual(result.reason, "missing_queue_row_id");
});

test("processSendQueueItem accepts a normalized Supabase row object directly", async () => {
  const row = buildSupabaseQueueRow({
    id: "sq-process-uuid-2",
    queue_key: "queue-sq-process-uuid-2",
    queue_id: "queue-sq-process-uuid-2",
    lock_token: "lock-sq-process-uuid-2",
  });

  const result = await processSendQueueItem(row, {
    loadQueueRowById: async () => {
      throw new Error("should_not_load_queue_row_by_id");
    },
    updateSendQueueRowWithLock: async (row_id, lock_token, payload) => ({
      ...row,
      ...payload,
      id: row_id,
      queue_row_id: row_id,
      queue_item_id: row_id,
      item_id: row_id,
      lock_token,
    }),
    sendTextgridSMS: async () => ({
      sid: "SM-sq-process-uuid-2",
      raw: { status: "queued" },
    }),
    writeOutboundSuccessMessageEvent: async () => ({ item_id: "evt-2" }),
  });

  assert.equal(result.sent, true);
  assert.equal(result.queue_row_id, "sq-process-uuid-2");
  assert.equal(result.queue_item_id, "sq-process-uuid-2");
});

test("processSendQueueItem resolves seller_first_name from candidate_snapshot.phone_first_name", async () => {
  let captured_seller_first_name = null;

  const row = buildSupabaseQueueRow({
    id: "sq-process-uuid-3",
    queue_key: "queue-sq-process-uuid-3",
    queue_id: "queue-sq-process-uuid-3",
    seller_first_name: null,
    metadata: {
      selected_template_id: "200194",
      candidate_snapshot: {
        phone_first_name: "Mia",
      },
    },
  });

  const result = await processSendQueueItem(row, {
    selectAvailableTextgridNumber: async () => ({
      ok: true,
      from_phone_number: "+16128060495",
      selected: { id: "tn-1", phone_number: "+16128060495", market: "houston" },
    }),
    updateSendQueueRowWithLock: async (row_id, lock_token, payload) => ({
      ...row,
      ...payload,
      id: row_id,
      queue_row_id: row_id,
      queue_item_id: row_id,
      item_id: row_id,
      lock_token,
    }),
    sendTextgridSMS: async (payload) => {
      captured_seller_first_name = payload.seller_first_name;
      return {
        sid: "SM-sq-process-uuid-3",
        raw: { status: "queued" },
      };
    },
    writeOutboundSuccessMessageEvent: async () => ({ item_id: "evt-3" }),
  });

  assert.equal(result.sent, true);
  assert.equal(captured_seller_first_name, "Mia");
});

test("runDevSendTest inserts a canonical queued row and optionally runs the queue immediately", async () => {
  let inserted_payload = null;
  let fetched = null;
  const original_cron_secret = process.env.CRON_SECRET;
  process.env.CRON_SECRET = "cron-secret";

  try {
    const result = await runDevSendTest({
      request_url:
        "http://localhost/api/dev/send-test?run_now=true&to=%2B16127430000&from=%2B16128060000",
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
      fetchImpl: async (url, options) => {
        fetched = { url, options };
        return {
          ok: true,
          status: 200,
          text: async () =>
            JSON.stringify({
              ok: true,
              route: "internal/queue/run",
              result: {
                ok: true,
                sent_count: 1,
              },
            }),
        };
      },
    });

    assert.equal(result.ok, true);
    assert.ok(inserted_payload);
    assert.equal(inserted_payload.queue_status, "queued");
    assert.equal(inserted_payload.send_priority, 10);
    assert.equal(inserted_payload.to_phone_number, "+16127430000");
    assert.equal(inserted_payload.from_phone_number, "+16128060000");
    assert.equal(inserted_payload.message_body, "Test message from Supabase send_queue");
    assert.equal(inserted_payload.message_text, "Test message from Supabase send_queue");
    assert.equal(inserted_payload.metadata.source, "dev_send_test");

    assert.ok(fetched);
    assert.equal(fetched.url, "http://localhost/api/internal/queue/run");
    assert.equal(fetched.options.method, "GET");
    assert.equal(fetched.options.headers.Authorization, "Bearer cron-secret");
    assert.equal(fetched.options.headers["x-vercel-cron-secret"], "cron-secret");
    assert.equal(result.queue_run.ok, true);
    assert.equal(result.queue_run.result.sent_count, 1);
  } finally {
    if (original_cron_secret === undefined) {
      delete process.env.CRON_SECRET;
    } else {
      process.env.CRON_SECRET = original_cron_secret;
    }
  }
});

test("dev send-test route exports GET and POST handlers and request helper returns JSON response", async () => {
  assert.equal(typeof getDevSendTest, "function");
  assert.equal(typeof postDevSendTest, "function");

  const response = await handleDevSendTestRequest(
    {
      url: "http://localhost/api/dev/send-test?run_now=false",
    },
    {
      insertSupabaseSendQueueRowImpl: async (payload) => ({
        ok: true,
        item_id: 777,
        queue_id: payload.queue_id,
        queue_key: payload.queue_key,
        raw: payload,
      }),
      fetchImpl: async () => {
        throw new Error("should_not_fetch_when_run_now_false");
      },
    }
  );

  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.ok, true);
  assert.equal(body.inserted.item_id, 777);
  assert.equal(body.queue_run, null);
});

test("dev SMS routes return 404 in production without x-internal-api-secret", async () => {
  const original_node_env = process.env.NODE_ENV;
  const original_internal_api_secret = process.env.INTERNAL_API_SECRET;

  process.env.NODE_ENV = "production";
  process.env.INTERNAL_API_SECRET = "internal-secret";

  try {
    const denied_send_test = await getDevSendTest({
      url: "http://localhost/api/dev/send-test",
      headers: new Headers(),
    });
    const denied_force_send = await getDevForceSend({
      headers: new Headers(),
    });
    const denied_env_check = await getDevEnvCheck({
      headers: new Headers(),
    });

    assert.equal(denied_send_test.status, 404);
    assert.equal(denied_force_send.status, 404);
    assert.equal(denied_env_check.status, 404);
  } finally {
    if (original_node_env === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = original_node_env;
    }

    if (original_internal_api_secret === undefined) {
      delete process.env.INTERNAL_API_SECRET;
    } else {
      process.env.INTERNAL_API_SECRET = original_internal_api_secret;
    }
  }
});

test("dev force-send returns 404 in production before sendTextgridSMS", async () => {
  let send_calls = 0;
  let access_guard_calls = 0;
  const warnings = [];

  const response = await handleDevForceSendRequest(
    {
      headers: new Headers({
        "x-internal-api-secret": "internal-secret",
      }),
    },
    {
      env: {
        NODE_ENV: "development",
        VERCEL_ENV: "production",
      },
      logger: {
        warn: (event, meta) => warnings.push({ event, meta }),
        log: () => {},
        error: () => {},
      },
      requireDevRouteAccess: () => {
        access_guard_calls += 1;
        return null;
      },
      sendTextgridSMS: async () => {
        send_calls += 1;
        throw new Error("sendTextgridSMS must not run in production");
      },
    }
  );

  assert.equal(response.status, 404);
  assert.equal(access_guard_calls, 0);
  assert.equal(send_calls, 0);
  assert.equal(warnings[0]?.event, "dev_force_send_blocked_in_production");
});

test("dev SMS routes allow access in production with x-internal-api-secret", async () => {
  const original_node_env = process.env.NODE_ENV;
  const original_internal_api_secret = process.env.INTERNAL_API_SECRET;

  process.env.NODE_ENV = "production";
  process.env.INTERNAL_API_SECRET = "internal-secret";

  try {
    const response = await handleDevSendTestRequest(
      {
        url: "http://localhost/api/dev/send-test?run_now=false",
        headers: new Headers({
          "x-internal-api-secret": "internal-secret",
        }),
      },
      {
        insertSupabaseSendQueueRowImpl: async (payload) => ({
          ok: true,
          item_id: 778,
          queue_id: payload.queue_id,
          queue_key: payload.queue_key,
          raw: payload,
        }),
        fetchImpl: async () => {
          throw new Error("should_not_fetch_when_run_now_false");
        },
      }
    );

    const body = await response.json();
    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.equal(body.inserted.item_id, 778);
  } finally {
    if (original_node_env === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = original_node_env;
    }

    if (original_internal_api_secret === undefined) {
      delete process.env.INTERNAL_API_SECRET;
    } else {
      process.env.INTERNAL_API_SECRET = original_internal_api_secret;
    }
  }
});
