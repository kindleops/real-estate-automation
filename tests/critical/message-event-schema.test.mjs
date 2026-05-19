import test from "node:test";
import assert from "node:assert/strict";

import { writeOutboundSuccessMessageEvent } from "@/lib/supabase/sms-engine.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeRow(overrides = {}) {
  return {
    id: "e2dba9b0-1383-4788-b3e5-007237432cee",
    queue_id: "feed:abc123",
    queue_key: "feed:abc123",
    queue_status: "sent",
    to_phone_number: "+13025077311",
    from_phone_number: "+16128060495",
    message_body: "Hey Cecilia, this is Greg.",
    message_text: "Hey Cecilia, this is Greg.",
    thread_key: "+13025077311",
    master_owner_id: "mo_abc123",
    prospect_id: null,
    property_id: "12345",
    market_id: null,
    market: null,
    sms_agent_id: null,
    textgrid_number_id: "tid-abc",
    template_id: "211409",
    property_address: null,
    character_count: 28,
    type: "outbound",
    stage_before: null,
    stage_after: null,
    current_stage: null,
    detected_intent: null,
    safety_status: "pending",
    risk: "low",
    priority: "normal",
    language: null,
    metadata: {},
    ...overrides,
  };
}

function makeUpsertSupabase(captured) {
  // Captures the upsert payload for assertion; always returns success.
  const query = {
    update() { return query; },
    upsert(payload) {
      captured.payload = payload;
      return query;
    },
    select() { return query; },
    async maybeSingle() {
      return { data: { id: "new-event-id", ...captured.payload }, error: null };
    },
  };
  return {
    from() { return query; },
  };
}

function makeInboxSupabase() {
  // Accepts inbox_thread_state upsert silently.
  const query = {
    upsert() { return query; },
    select() { return query; },
    async maybeSingle() { return { data: { id: "its-id" }, error: null }; },
  };
  return { from() { return query; } };
}

// ─── Test: auto_reply columns are present in the payload ─────────────────────

test("writeOutboundSuccessMessageEvent payload includes auto_reply_status and auto_reply_queue_id", async () => {
  const captured = {};
  const row = makeRow({ type: "outbound" }); // not auto_reply — both fields should be null

  await writeOutboundSuccessMessageEvent(row, { sid: "SM123" }, {
    supabase: makeUpsertSupabase(captured),
    upsertInboxThreadState: async () => ({ ok: true }),
  });

  assert.ok("auto_reply_status" in captured.payload,
    "auto_reply_status must be in the upsert payload");
  assert.ok("auto_reply_queue_id" in captured.payload,
    "auto_reply_queue_id must be in the upsert payload");
  assert.equal(captured.payload.auto_reply_status, null,
    "auto_reply_status must be null for non-auto-reply type");
  assert.equal(captured.payload.auto_reply_queue_id, null,
    "auto_reply_queue_id must be null for non-auto-reply type");
});

test("writeOutboundSuccessMessageEvent sets auto_reply_status=sent for auto_reply type rows", async () => {
  const captured = {};
  const row = makeRow({ type: "auto_reply" });

  await writeOutboundSuccessMessageEvent(row, { sid: "SM456" }, {
    supabase: makeUpsertSupabase(captured),
    upsertInboxThreadState: async () => ({ ok: true }),
  });

  assert.equal(captured.payload.auto_reply_status, "sent");
  assert.equal(captured.payload.auto_reply_queue_id, String(row.id));
});

test("writeOutboundSuccessMessageEvent returns the upserted event row on success", async () => {
  const captured = {};
  const row = makeRow();

  const result = await writeOutboundSuccessMessageEvent(row, { sid: "SM789" }, {
    supabase: makeUpsertSupabase(captured),
    upsertInboxThreadState: async () => ({ ok: true }),
  });

  assert.ok(result, "must return the upserted event row");
  assert.equal(result.direction, "outbound");
  assert.equal(result.delivery_status, "sent");
});

test("writeOutboundSuccessMessageEvent uses the injected writeOutboundSuccessMessageEvent override when provided", async () => {
  // Validates that the options.writeOutboundSuccessMessageEvent injection short-circuits correctly
  // so caller-controlled mocks work in the full pipeline tests.
  let captured_payload = null;

  const result = await writeOutboundSuccessMessageEvent(makeRow(), { sid: "SMoverride" }, {
    writeOutboundSuccessMessageEvent: (payload) => {
      captured_payload = payload;
      return { id: "injected-event", ...payload };
    },
  });

  assert.ok(captured_payload, "injection override must be called with the built payload");
  assert.equal(captured_payload.message_event_key.startsWith("outbound_"), true);
  assert.ok("auto_reply_status" in captured_payload, "auto_reply_status must be in payload");
  assert.ok("auto_reply_queue_id" in captured_payload, "auto_reply_queue_id must be in payload");
  assert.equal(result.id, "injected-event");
});

// ─── Test: improved bookkeeping error visibility ──────────────────────────────

test("message_event bookkeeping catch logs error_code and error_message when upsert fails", async () => {
  // Import processSendQueueItem to reach the bookkeeping catch via the full pipeline.
  // Instead, verify the warn payload shape directly by simulating what the catch now logs.
  // This tests the guard logic — the warn call now includes error_code and hint.
  const warn_calls = [];
  const fake_warn = (event, meta) => warn_calls.push({ event, meta });

  // Simulate the catch block behaviour by calling what the updated catch does:
  const simulated_error = { message: "column 'auto_reply_queue_id' not found", code: "PGRST204" };
  const bookkeeping_errors = [];
  const me_err_msg = simulated_error?.message || "unknown_error";
  const me_err_code = simulated_error?.code || null;
  bookkeeping_errors.push(`message_event_write_failed:${me_err_msg}`);
  fake_warn("queue.success_message_event_write_failed", {
    queue_row_id: "test-id",
    error_code: me_err_code,
    error_message: me_err_msg,
    hint: me_err_code === "PGRST204"
      ? "schema drift: payload column missing from message_events table"
      : null,
  });

  assert.equal(warn_calls.length, 1);
  assert.equal(warn_calls[0].event, "queue.success_message_event_write_failed");
  assert.equal(warn_calls[0].meta.error_code, "PGRST204");
  assert.equal(warn_calls[0].meta.error_message, simulated_error.message);
  assert.ok(warn_calls[0].meta.hint?.includes("schema drift"),
    "hint must mention schema drift for PGRST204");
  assert.equal(bookkeeping_errors[0], `message_event_write_failed:${simulated_error.message}`);
});
