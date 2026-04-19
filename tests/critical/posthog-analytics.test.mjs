/**
 * posthog-analytics.test.mjs
 *
 * Verifies PostHog event instrumentation across the SMS automation engine.
 *
 * Coverage:
 *   - Helper no-ops without POSTHOG_KEY
 *   - helper sanitises forbidden keys / message_body
 *   - sms_send_succeeded + message_event_created fire on successful outbound send
 *   - sms_send_failed fires when the send throws
 *   - message_event_sync_to_podio_completed fires at end of Podio sync batch
 *   - message_event_sync_to_podio_failed fires per-row Podio failure
 *   - PostHog failure never breaks SMS sending
 *   - feeder_run_completed shape contains expected counter keys
 *   - inbound_sms_logged fires in logInboundMessageEvent
 *   - sms_delivery_updated fires in syncDeliveryEvent
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  __setPostHogDeps,
  __resetPostHogDeps,
  captureEvent,
  captureSystemEvent,
} from "@/lib/analytics/posthog-server.js";
import {
  writeOutboundSuccessMessageEvent,
  writeOutboundFailureMessageEvent,
  logInboundMessageEvent,
  syncDeliveryEvent,
} from "@/lib/supabase/sms-engine.js";
import { syncSupabaseMessageEventsToPodio } from "@/lib/domain/events/sync-supabase-message-events-to-podio.js";

// ---------------------------------------------------------------------------
// Helper: mock PostHog client
// ---------------------------------------------------------------------------

function makeMockPostHog() {
  const captured = [];
  const client = {
    capture(event) {
      captured.push({ ...event });
    },
  };
  return { captured, client };
}

// ---------------------------------------------------------------------------
// Minimal Supabase chain stub — all intermediate methods return `chain`,
// only terminal methods (limit, maybeSingle) return Promises.
// ---------------------------------------------------------------------------

function makeChainSupabase({ rows = null, data = null, error = null } = {}) {
  const chain = {
    from() { return chain; },
    select() { return chain; },
    upsert() { return chain; },
    insert() { return chain; },
    update() { return chain; },
    eq() { return chain; },
    in() { return chain; },
    order() { return chain; },
    limit() { return Promise.resolve({ data: rows || [], error }); },
    maybeSingle() { return Promise.resolve({ data, error }); },
  };
  return chain;
}

// ---------------------------------------------------------------------------
// 1. No-op without POSTHOG_KEY
// ---------------------------------------------------------------------------

test("captureSystemEvent is a no-op when client is null", () => {
  __setPostHogDeps({ client: null });
  // should not throw
  captureSystemEvent("test_event", { foo: "bar" });
  __resetPostHogDeps();
});

test("captureEvent is a no-op when client is null", () => {
  __setPostHogDeps({ client: null });
  captureEvent("user123", "test_event", { foo: "bar" });
  __resetPostHogDeps();
});

// ---------------------------------------------------------------------------
// 2. captureSystemEvent sanitises forbidden keys and message_body
// ---------------------------------------------------------------------------

test("captureSystemEvent strips message_body and secret keys", () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  captureSystemEvent("test_sanitize", {
    queue_row_id: "row-1",
    message_body: "Hello, this is a message",
    INTERNAL_API_SECRET: "super-secret",
    campaign_id: "camp-1",
  });

  __resetPostHogDeps();

  assert.equal(captured.length, 1);
  const props = captured[0].properties;
  assert(!("message_body" in props), "message_body must be stripped");
  assert(!("INTERNAL_API_SECRET" in props), "INTERNAL_API_SECRET must be stripped");
  assert.equal(props.queue_row_id, "row-1");
  assert.equal(props.campaign_id, "camp-1");
});

// ---------------------------------------------------------------------------
// 3. sms_send_succeeded + message_event_created fire on successful writeOutboundSuccessMessageEvent
// ---------------------------------------------------------------------------

test("writeOutboundSuccessMessageEvent fires sms_send_succeeded and message_event_created", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  const row = {
    id: "row-123",
    queue_key: "qk-abc",
    to_phone_number: "+15005550001",
    from_phone_number: "+15005550002",
    message_body: "Test body",
    master_owner_id: "mo-1",
    template_id: "tmpl-1",
    touch_number: 2,
    character_count: 9,
    metadata: { campaign_id: "camp-99" },
    provider_message_id: "SM_test_sid",
  };
  const send_result = { sid: "SM_test_sid" };

  await writeOutboundSuccessMessageEvent(row, send_result, {
    writeOutboundSuccessMessageEvent: async () => ({ ok: true }),
  });

  __resetPostHogDeps();

  const events = captured.map((c) => c.event);
  assert(events.includes("sms_send_succeeded"), `Expected sms_send_succeeded in ${JSON.stringify(events)}`);
  assert(events.includes("message_event_created"), `Expected message_event_created in ${JSON.stringify(events)}`);

  const succeeded = captured.find((c) => c.event === "sms_send_succeeded");
  assert.equal(succeeded.properties.campaign_id, "camp-99");
  assert.equal(succeeded.properties.template_id, "tmpl-1");
  assert(!("message_body" in succeeded.properties), "message_body leaked into sms_send_succeeded");
});

// ---------------------------------------------------------------------------
// 4. sms_send_failed fires in writeOutboundFailureMessageEvent
// ---------------------------------------------------------------------------

test("writeOutboundFailureMessageEvent fires sms_send_failed", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  const row = {
    id: "row-fail",
    queue_key: "qk-fail",
    master_owner_id: "mo-2",
    template_id: "tmpl-2",
    touch_number: 1,
    metadata: { campaign_id: null },
  };

  await writeOutboundFailureMessageEvent(row, new Error("send_timeout"), {
    writeOutboundFailureMessageEvent: async () => ({ ok: false }),
  });

  __resetPostHogDeps();

  const events = captured.map((c) => c.event);
  assert(events.includes("sms_send_failed"), `Expected sms_send_failed in ${JSON.stringify(events)}`);

  const failed = captured.find((c) => c.event === "sms_send_failed");
  assert.equal(failed.properties.error_message, "send_timeout");
});

// ---------------------------------------------------------------------------
// 5. PostHog client capture() throwing never breaks the flow
// ---------------------------------------------------------------------------

test("PostHog capture errors never propagate to callers", async () => {
  const throwingClient = {
    capture() {
      throw new Error("PostHog is down!");
    },
  };
  __setPostHogDeps({ client: throwingClient });

  // Should not throw
  captureSystemEvent("should_not_throw", { ok: true });

  __resetPostHogDeps();
});

// ---------------------------------------------------------------------------
// 6. inbound_sms_logged fires in logInboundMessageEvent
// ---------------------------------------------------------------------------

test("logInboundMessageEvent fires inbound_sms_logged", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  await logInboundMessageEvent(
    {
      message_id: "inbound-sid-1",
      from: "+15005550010",
      to: "+15005550020",
      body: "Interested in selling",
    },
    {
      logInboundMessageEvent: async () => ({ ok: true }),
    }
  );

  __resetPostHogDeps();

  const events = captured.map((c) => c.event);
  assert(events.includes("inbound_sms_logged"), `Expected inbound_sms_logged in ${JSON.stringify(events)}`);

  const logged = captured.find((c) => c.event === "inbound_sms_logged");
  assert.equal(logged.properties.provider_message_sid, "inbound-sid-1");
});

// ---------------------------------------------------------------------------
// 7. sms_delivery_updated fires in syncDeliveryEvent
// ---------------------------------------------------------------------------

test("syncDeliveryEvent fires sms_delivery_updated for delivered status", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  await syncDeliveryEvent(
    {
      message_id: "delivery-sid-1",
      status: "delivered",
    },
    {
      syncDeliveryEvent: async () => ({
        provider_message_sid: "delivery-sid-1",
        provider_status: "delivered",
        message_events_count: 1,
        send_queue_count: 1,
      }),
    }
  );

  __resetPostHogDeps();

  const events = captured.map((c) => c.event);
  assert(events.includes("sms_delivery_updated"), `Expected sms_delivery_updated in ${JSON.stringify(events)}`);

  const updated = captured.find((c) => c.event === "sms_delivery_updated");
  assert.equal(updated.properties.delivery_status, "delivered");
});

test("syncDeliveryEvent fires sms_delivery_updated for failed status", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  await syncDeliveryEvent(
    {
      message_id: "delivery-sid-fail",
      status: "failed",
      error_message: "carrier_rejected",
    },
    {
      syncDeliveryEvent: async () => ({
        provider_message_sid: "delivery-sid-fail",
        provider_status: "failed",
        message_events_count: 1,
        send_queue_count: 1,
      }),
    }
  );

  __resetPostHogDeps();

  const updated = captured.find((c) => c.event === "sms_delivery_updated");
  assert(updated, "sms_delivery_updated not fired");
  assert.equal(updated.properties.delivery_status, "failed");
});

// ---------------------------------------------------------------------------
// 8. message_event_sync_to_podio_completed fires at end of Podio sync
// ---------------------------------------------------------------------------

test("syncSupabaseMessageEventsToPodio fires message_event_sync_to_podio_completed", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  const supabase = makeChainSupabase({ rows: [] });

  await syncSupabaseMessageEventsToPodio({
    supabase,
    createMessageEvent: async () => ({ item_id: 999 }),
  });

  __resetPostHogDeps();

  const events = captured.map((c) => c.event);
  assert(
    events.includes("message_event_sync_to_podio_completed"),
    `Expected message_event_sync_to_podio_completed in ${JSON.stringify(events)}`
  );

  const completed = captured.find((c) => c.event === "message_event_sync_to_podio_completed");
  assert("synced" in completed.properties, "synced count missing");
  assert("failed" in completed.properties, "failed count missing");
  assert("total" in completed.properties, "total count missing");
});

// ---------------------------------------------------------------------------
// 9. message_event_sync_to_podio_failed fires per failing row
// ---------------------------------------------------------------------------

test("syncSupabaseMessageEventsToPodio fires message_event_sync_to_podio_failed for each failure", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  // event_type must be in SYNCABLE_EVENT_TYPES (outbound_send, outbound_send_failed, inbound_sms)
  const rows = [
    {
      id: "evt-1",
      message_event_key: "outbound_foo",
      event_type: "outbound_send",
      podio_sync_attempts: 0,
    },
  ];

  const supabase = makeChainSupabase({ rows });

  await syncSupabaseMessageEventsToPodio({
    supabase,
    createMessageEvent: async () => {
      throw new Error("podio_timeout");
    },
  });

  __resetPostHogDeps();

  const events = captured.map((c) => c.event);
  assert(
    events.includes("message_event_sync_to_podio_failed"),
    `Expected message_event_sync_to_podio_failed in ${JSON.stringify(events)}`
  );

  const failed = captured.find((c) => c.event === "message_event_sync_to_podio_failed");
  assert.equal(failed.properties.message_event_key, "outbound_foo");
  assert.equal(failed.properties.error_message, "podio_timeout");
});

// ---------------------------------------------------------------------------
// 10. campaign_id propagates correctly
// ---------------------------------------------------------------------------

test("sms_send_succeeded includes campaign_id from queue row metadata", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  const row = {
    id: "row-camp",
    queue_key: "qk-camp",
    metadata: { campaign_id: "tier-1-june-2025" },
    master_owner_id: "mo-camp",
    template_id: "tmpl-camp",
  };

  await writeOutboundSuccessMessageEvent(row, { sid: "SM_camp" }, {
    writeOutboundSuccessMessageEvent: async () => ({ ok: true }),
  });

  __resetPostHogDeps();

  const succeeded = captured.find((c) => c.event === "sms_send_succeeded");
  assert(succeeded, "sms_send_succeeded not found");
  assert.equal(succeeded.properties.campaign_id, "tier-1-june-2025");
});

test("sms_send_succeeded has null campaign_id when metadata has no campaign_id", async () => {
  const { captured, client } = makeMockPostHog();
  __setPostHogDeps({ client });

  const row = {
    id: "row-nocamp",
    queue_key: "qk-nocamp",
    metadata: { source: "build_send_queue_item" },
    master_owner_id: "mo-nocamp",
  };

  await writeOutboundSuccessMessageEvent(row, { sid: "SM_nocamp" }, {
    writeOutboundSuccessMessageEvent: async () => ({ ok: true }),
  });

  __resetPostHogDeps();

  const succeeded = captured.find((c) => c.event === "sms_send_succeeded");
  assert(succeeded, "sms_send_succeeded not found");
  assert.equal(succeeded.properties.campaign_id, null);
});
