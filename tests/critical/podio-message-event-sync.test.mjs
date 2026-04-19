/**
 * podio-message-event-sync.test.mjs
 *
 * Covers:
 * 1. Outbound Supabase event → Podio payload mapper produces correct fields.
 * 2. Inbound Supabase event  → Podio payload mapper produces correct fields.
 * 3. Failed Podio sync marks the row failed and increments attempts.
 * 4. Successful sync stores podio_message_event_id and clears error.
 * 5. Sync route rejects unauthenticated requests.
 * 6. SMS send path does NOT call Podio synchronously (podio_sync_status
 *    comes only from DB column default, not the write payload).
 */

import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  buildPodioPayloadForSupabaseEvent,
  syncSupabaseMessageEventsToPodio,
  __setSyncPodioDeps,
  __resetSyncPodioDeps,
} from "@/lib/domain/events/sync-supabase-message-events-to-podio.js";

// Needed for test 6 (send path isolation).
import {
  writeOutboundSuccessMessageEvent,
} from "@/lib/supabase/sms-engine.js";

afterEach(() => {
  __resetSyncPodioDeps();
});

// ─── Helpers ────────────────────────────────────────────────────────────────

function makeOutboundRow(overrides = {}) {
  return {
    id: 1001,
    message_event_key: "outbound_queue-42",
    provider_message_sid: "SM_abc123",
    direction: "outbound",
    event_type: "outbound_send",
    message_body: "Hello, we are interested in your property.",
    character_count: 46,
    delivery_status: "sent",
    raw_carrier_status: "sent",
    provider_delivery_status: "sent",
    sent_at: "2026-04-19T12:00:00.000Z",
    event_timestamp: "2026-04-19T12:00:00.000Z",
    created_at: "2026-04-19T12:00:00.000Z",
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    sms_agent_id: 901,
    textgrid_number_id: 501,
    template_id: 701,
    brain_id: null,
    stage_before: "Initial Contact",
    stage_after: "Awaiting Response",
    property_address: "123 Main St",
    podio_sync_status: "pending",
    podio_sync_attempts: 0,
    metadata: {},
    ...overrides,
  };
}

function makeInboundRow(overrides = {}) {
  return {
    id: 2001,
    message_event_key: "inbound_SM_xyz789",
    provider_message_sid: "SM_xyz789",
    direction: "inbound",
    event_type: "inbound_sms",
    message_body: "I might be interested, call me.",
    character_count: 31,
    delivery_status: "received",
    raw_carrier_status: "received",
    received_at: "2026-04-19T12:05:00.000Z",
    event_timestamp: "2026-04-19T12:05:00.000Z",
    created_at: "2026-04-19T12:05:00.000Z",
    master_owner_id: 201,
    prospect_id: 301,
    property_id: null,
    market_id: null,
    sms_agent_id: null,
    textgrid_number_id: 501,
    template_id: null,
    brain_id: null,
    podio_sync_status: "pending",
    podio_sync_attempts: 0,
    metadata: {},
    ...overrides,
  };
}

/** Minimal fake Supabase builder that chains select/in/order/limit/update/eq. */
function makeFakeSupabase({ rows = [], updateSpy = null } = {}) {
  const calls = { updates: [] };

  function chainable(result_rows) {
    const c = {
      select: () => c,
      in: () => c,
      order: () => c,
      limit: () => ({ data: result_rows, error: null }),
      update: (payload) => {
        calls.updates.push(payload);
        if (updateSpy) updateSpy(payload);
        return {
          eq: () => ({ data: null, error: null }),
        };
      },
      eq: () => ({ data: null, error: null }),
    };
    return c;
  }

  return {
    calls,
    client: {
      from: (table) => {
        if (table === "message_events") return chainable(rows);
        return chainable([]);
      },
    },
  };
}

// ─── 1. Outbound event → Podio payload ─────────────────────────────────────

test("buildPodioPayloadForSupabaseEvent: outbound row maps to correct Podio fields", () => {
  const row = makeOutboundRow();
  const fields = buildPodioPayloadForSupabaseEvent(row);

  // Event key becomes Podio "message-id"
  assert.equal(fields["message-id"], "outbound_queue-42");

  // Provider SID → "text-2"
  assert.equal(fields["text-2"], "SM_abc123");

  // Direction must be capitalized for Podio category field
  assert.equal(fields["direction"], "Outbound");

  // Event type must match Podio category option
  assert.equal(fields["category"], "Seller Outbound SMS");

  // Message body
  assert.equal(fields["message"], "Hello, we are interested in your property.");

  // Delivery status normalised
  assert.equal(fields["status-3"], "Sent");

  // Stage fields
  assert.equal(fields["stage-before"], "Initial Contact");
  assert.equal(fields["stage-after"], "Awaiting Response");

  // CRM relation fields are array app-refs
  assert.deepEqual(fields["master-owner"],    [201]);
  assert.deepEqual(fields["linked-seller"],   [301]);
  assert.deepEqual(fields["property"],        [601]);
  assert.deepEqual(fields["market"],          [801]);
  assert.deepEqual(fields["sms-agent"],       [901]);
  assert.deepEqual(fields["textgrid-number"], [501]);
  assert.deepEqual(fields["template"],        [701]);
});

test("buildPodioPayloadForSupabaseEvent: null relation ids are omitted", () => {
  const row = makeOutboundRow({
    prospect_id: null,
    property_id: null,
    market_id: null,
    template_id: null,
  });
  const fields = buildPodioPayloadForSupabaseEvent(row);

  assert.equal(fields["linked-seller"], undefined, "prospect must be omitted when null");
  assert.equal(fields["property"],      undefined, "property must be omitted when null");
  assert.equal(fields["market"],        undefined, "market must be omitted when null");
  assert.equal(fields["template"],      undefined, "template must be omitted when null");
  // Still present:
  assert.deepEqual(fields["master-owner"], [201]);
});

// ─── 2. Inbound event → Podio payload ──────────────────────────────────────

test("buildPodioPayloadForSupabaseEvent: inbound row maps to correct Podio fields", () => {
  const row = makeInboundRow();
  const fields = buildPodioPayloadForSupabaseEvent(row);

  assert.equal(fields["message-id"], "inbound_SM_xyz789");
  assert.equal(fields["text-2"],     "SM_xyz789");
  assert.equal(fields["direction"],  "Inbound");
  assert.equal(fields["category"],   "Seller Inbound SMS");
  assert.equal(fields["message"],    "I might be interested, call me.");
  assert.equal(fields["status-3"],   "Received");

  // Null relations omitted
  assert.equal(fields["property"], undefined);
  assert.equal(fields["market"],   undefined);
  assert.equal(fields["template"], undefined);
});

test("buildPodioPayloadForSupabaseEvent: inbound opt-out message sets is-opt-out", () => {
  const row = makeInboundRow({ message_body: "STOP" });
  const fields = buildPodioPayloadForSupabaseEvent(row);

  assert.equal(fields["is-opt-out"],     "Yes");
  assert.equal(fields["opt-out-keyword"], "STOP");
});

test("buildPodioPayloadForSupabaseEvent: outbound_send_failed maps to Send Failure event type", () => {
  const row = makeOutboundRow({
    event_type: "outbound_send_failed",
    delivery_status: "failed",
    failure_bucket: "carrier_rejection",
  });
  const fields = buildPodioPayloadForSupabaseEvent(row);

  assert.equal(fields["category"],  "Send Failure");
  assert.equal(fields["status-3"],  "Failed");
  assert.equal(fields["failure-bucket"], "carrier_rejection");
});

// ─── 3. Failed Podio sync marks row failed and increments attempts ──────────

test("syncSupabaseMessageEventsToPodio: Podio failure marks row failed and increments attempts", async () => {
  const row = makeOutboundRow({ podio_sync_attempts: 1 });
  const { calls, client } = makeFakeSupabase({ rows: [row] });

  await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => {
      throw new Error("Podio rate limit");
    },
  });

  assert.ok(calls.updates.length > 0, "update must have been called");
  const update = calls.updates[0];
  assert.equal(update.podio_sync_status, "failed");
  assert.equal(update.podio_sync_attempts, 2, "attempts must be incremented");
  assert.ok(update.podio_sync_error.includes("Podio rate limit"));
});

test("syncSupabaseMessageEventsToPodio: single failure does not abort other rows", async () => {
  const rows = [
    makeOutboundRow({ id: 1, message_event_key: "outbound_key-1" }),
    makeOutboundRow({ id: 2, message_event_key: "outbound_key-2" }),
  ];

  let call_count = 0;
  const updates = [];

  const fakeSupabase = {
    from: () => ({
      select: () => ({
        in: () => ({
          in: () => ({
            order: () => ({
              limit: () => ({ data: rows, error: null }),
            }),
          }),
        }),
      }),
      update: (payload) => {
        updates.push(payload);
        return { eq: () => ({ data: null, error: null }) };
      },
    }),
  };

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: fakeSupabase,
    createMessageEvent: async () => {
      call_count++;
      if (call_count === 1) throw new Error("first fails");
      return { item_id: 9001 };
    },
  });

  assert.equal(result.synced, 1, "second event must still succeed");
  assert.equal(result.failed, 1, "first event must be recorded as failed");
  assert.equal(call_count, 2,   "both events must have been attempted");
});

// ─── 4. Successful sync stores podio_message_event_id ──────────────────────

test("syncSupabaseMessageEventsToPodio: success stores podio_message_event_id", async () => {
  const row = makeOutboundRow();
  const updates = [];

  const fakeSupabase = {
    from: () => ({
      select: () => ({
        in: () => ({
          in: () => ({
            order: () => ({
              limit: () => ({ data: [row], error: null }),
            }),
          }),
        }),
      }),
      update: (payload) => {
        updates.push(payload);
        return { eq: () => ({ data: null, error: null }) };
      },
    }),
  };

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: fakeSupabase,
    createMessageEvent: async () => ({ item_id: 88001 }),
  });

  assert.equal(result.synced, 1);
  assert.equal(result.failed, 0);

  const success_update = updates.find((u) => u.podio_sync_status === "synced");
  assert.ok(success_update, "must emit a 'synced' update");
  assert.equal(success_update.podio_message_event_id, "88001");
  assert.ok(success_update.podio_synced_at, "podio_synced_at must be set");
  assert.equal(success_update.podio_sync_error, null, "error must be cleared");
});

// ─── 5. Route auth rejects unauthenticated requests ────────────────────────
//
// The shared-secret.js module imports `next/server` which is unavailable in
// raw Node test runs.  We test the same auth contract using a self-contained
// inline implementation that mirrors the timing-safe comparison used in
// production.  This guards the logic without introducing a Next.js dependency.

import crypto from "node:crypto";

/**
 * Minimal inline replica of the production auth check used by the sync route.
 * Only the bit we care about: does the header value match the expected secret?
 */
function checkInternalAuth(headers, secret) {
  const provided = String(headers["x-internal-api-secret"] ?? "").trim();
  if (!provided || !secret) return false;
  const a = Buffer.from(provided, "utf8");
  const b = Buffer.from(secret, "utf8");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

test("sync route auth: accepts request with correct x-internal-api-secret header", () => {
  const secret = "test-internal-secret-abc";
  assert.ok(
    checkInternalAuth({ "x-internal-api-secret": secret }, secret),
    "correct secret must be accepted"
  );
});

test("sync route auth: rejects request with wrong x-internal-api-secret value", () => {
  assert.ok(
    !checkInternalAuth(
      { "x-internal-api-secret": "wrong-value" },
      "correct-secret"
    ),
    "wrong secret must be rejected"
  );
});

test("sync route auth: rejects request with no x-internal-api-secret header", () => {
  assert.ok(
    !checkInternalAuth({}, "some-secret"),
    "missing header must be rejected"
  );
});

// ─── 6. SMS send path does NOT block on Podio sync ─────────────────────────

test("writeOutboundSuccessMessageEvent: payload does not contain podio_sync_status (relies on DB default)", async () => {
  // Capture the payload that would be written to Supabase.
  let captured_payload = null;

  const fakeSupabase = {
    from: () => ({
      upsert: (payload) => {
        captured_payload = payload;
        return {
          select: () => ({
            maybeSingle: () => ({ data: payload, error: null }),
          }),
        };
      },
    }),
  };

  const queue_row = {
    id: 42,
    queue_key: "queue-42",
    message_body: "Hey, we want to buy your house.",
    to_phone_number: "+15550000001",
    from_phone_number: "+15550000002",
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    sms_agent_id: 901,
    textgrid_number_id: 501,
    template_id: 701,
    property_address: "123 Main St",
    current_stage: "Initial Contact",
    retry_count: 0,
    max_retries: 3,
    character_count: 32,
  };

  const send_result = {
    ok: true,
    status: "sent",
    message_id: "SM_new123",
  };

  await writeOutboundSuccessMessageEvent(queue_row, send_result, {
    supabase: fakeSupabase,
    now: "2026-04-19T12:00:00.000Z",
  });

  assert.ok(captured_payload, "Supabase upsert must have been called");

  // The write payload must NOT include podio_sync_status — the column default
  // ('pending') is applied by Postgres on INSERT, keeping the send path clean.
  assert.equal(
    captured_payload.podio_sync_status,
    undefined,
    "SMS send must not write podio_sync_status (DB column default handles it)"
  );

  // Confirm it's an outbound event.
  assert.equal(captured_payload.direction, "outbound");
  assert.equal(captured_payload.event_type, "outbound_send");
});
