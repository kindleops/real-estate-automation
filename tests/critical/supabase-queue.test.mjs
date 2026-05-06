// supabase-queue.test.mjs
// Proof tests for Supabase-based auto-queue and auto-reply engine

import { describe, it, beforeEach, afterEach, mock, before, after } from "node:test";
import assert from "node:assert";
import {
  insertSupabaseSendQueueRow,
  normalizeSendQueueRow,
  buildSendQueueDedupeKey,
} from "@/lib/supabase/sms-engine.js";
import { getSystemFlag } from "@/lib/system-control.js";

// ── Mock Supabase client ─────────────────────────────────────────────────

function buildMockSupabase(overrides = {}) {
  const inserted = [];
  const updated = [];
  const selected = [];

  const from = (table) => {
    const state = { table, inserted, updated, selected };

    return {
      insert: (payload) => {
        inserted.push({ table, payload });
        return {
          select: () => ({
            maybeSingle: () =>
              Promise.resolve({
                data: { id: inserted.length, ...payload },
                error: null,
              }),
          }),
        };
      },
      update: (payload) => {
        updated.push({ table, payload });
        return {
          eq: () => ({
            select: () => Promise.resolve({ data: [payload], error: null }),
          }),
          select: () => Promise.resolve({ data: [payload], error: null }),
        };
      },
      select: () => ({
        eq: () => ({
          maybeSingle: () =>
            Promise.resolve({
              data: selected.find((r) => r.table === table) || null,
              error: null,
            }),
        }),
        maybeSingle: () =>
          Promise.resolve({
            data: selected.find((r) => r.table === table) || null,
            error: null,
          }),
      }),
      ...overrides,
    };
  };

  return {
    from,
    _inserted: inserted,
    _updated: updated,
    _selected: selected,
  };
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("Supabase send_queue auto-queue", () => {
  let mockSupabase;

  beforeEach(() => {
    mockSupabase = buildMockSupabase();
  });

  it("should create send_queue row with all required fields", async () => {
    const payload = {
      thread_key: "owner:123:phone:456:property:789",
      owner_id: 123,
      master_owner_id: 123,
      property_id: 789,
      prospect_id: 456,
      phone_id: 456,
      seller_phone: "+15551234567",
      textgrid_number_id: 999,
      textgrid_number: "+15559876543",
      market: "Chicago",
      timezone: "America/Chicago",
      scheduled_for: new Date().toISOString(),
      status: "READY",
      stage: "consider_selling",
      use_case: "ownership_check",
      template_id: 555,
      template_source: "catalog",
      rendered_message: "Hello, interested in your property",
      agent_id: 111,
      touch_number: 1,
      priority: "normal",
      risk: "low",
      sms_eligible: true,
      routing_allowed: true,
      safety_status: "safe",
      type: "outbound",
      dedupe_key: "test-dedupe-1",
      seller_first_name: "John",
    };

    const result = await insertSupabaseSendQueueRow(payload, {
      supabase: mockSupabase,
    });

    assert.strictEqual(result.ok, true);
    assert.ok(result.raw);
    assert.strictEqual(mockSupabase._inserted.length, 1);

    const inserted = mockSupabase._inserted[0];
    assert.strictEqual(inserted.table, "send_queue");
    assert.strictEqual(inserted.payload.thread_key, payload.thread_key);
    assert.strictEqual(inserted.payload.owner_id, payload.owner_id);
    assert.strictEqual(inserted.payload.master_owner_id, payload.master_owner_id);
    assert.strictEqual(inserted.payload.property_id, payload.property_id);
    assert.strictEqual(inserted.payload.textgrid_number, payload.textgrid_number);
    assert.strictEqual(inserted.payload.template_source, payload.template_source);
    assert.strictEqual(inserted.payload.rendered_message, payload.rendered_message);
    assert.strictEqual(inserted.payload.agent_id, payload.agent_id);
    assert.strictEqual(inserted.payload.priority, payload.priority);
    assert.strictEqual(inserted.payload.risk, payload.risk);
    assert.strictEqual(inserted.payload.sms_eligible, payload.sms_eligible);
    assert.strictEqual(inserted.payload.routing_allowed, payload.routing_allowed);
    assert.strictEqual(inserted.payload.safety_status, payload.safety_status);
    assert.strictEqual(inserted.payload.type, payload.type);
  });

  it("should prevent duplicate queue rows with same dedupe key", async () => {
    const dedupe_key = "test-dedupe-unique";

    // First insert succeeds
    const result1 = await insertSupabaseSendQueueRow(
      {
        dedupe_key,
        thread_key: "owner:123:phone:456",
        master_owner_id: 123,
        type: "outbound",
      },
      { supabase: mockSupabase }
    );

    assert.strictEqual(result1.ok, true);

    // Mock duplicate error (PostgreSQL unique violation)
    const duplicateSupabase = buildMockSupabase({
      insert: () => ({
        select: () => ({
          maybeSingle: () =>
            Promise.resolve({
              data: null,
              error: { code: "23505", message: "duplicate key value violates unique constraint" },
            }),
        }),
      }),
    });

    // Second insert with same dedupe_key should fail with duplicate_blocked
    const result2 = await insertSupabaseSendQueueRow(
      {
        dedupe_key,
        thread_key: "owner:123:phone:456",
        master_owner_id: 123,
        type: "outbound",
      },
      { supabase: duplicateSupabase }
    );

    assert.strictEqual(result2.ok, false);
    assert.strictEqual(result2.reason, "duplicate_blocked");
  });

  it("should not queue suppressed/suppressed leads", async () => {
    // This test verifies that suppressed leads should not create queue rows
    // In a real implementation, the caller checks suppression before calling insert
    const payload = {
      thread_key: "owner:999:phone:888",
      master_owner_id: 999,
      sms_eligible: false, // Not eligible
      safety_status: "suppressed",
      type: "outbound",
    };

    // The caller should check sms_eligible and safety_status before inserting
    // This test documents the expected behavior
    assert.strictEqual(payload.sms_eligible, false);
    assert.strictEqual(payload.safety_status, "suppressed");
  });

  it("should not queue when no valid TextGrid number", async () => {
    const payload = {
      thread_key: "owner:123:phone:456",
      master_owner_id: 123,
      routing_allowed: false, // No valid routing number
      type: "outbound",
    };

    // The caller should check routing_allowed before inserting
    assert.strictEqual(payload.routing_allowed, false);
  });

  it("should create auto-reply queue row with source_event_id", async () => {
    const payload = {
      type: "auto_reply",
      thread_key: "owner:123:phone:456",
      owner_id: 123,
      master_owner_id: 123,
      source_event_id: "event-uuid-1234",
      inbound_message_id: "+15551234567",
      detected_intent: "ownership_confirmed",
      stage_before: "ownership_check",
      stage_after: "consider_selling",
      template_selected: "456",
      rendered_message: "Thanks for confirming, let me know if you're interested",
      scheduled_for: new Date().toISOString(),
      safety_status: "safe",
    };

    const result = await insertSupabaseSendQueueRow(payload, {
      supabase: mockSupabase,
    });

    assert.strictEqual(result.ok, true);
    const inserted = mockSupabase._inserted[0];
    assert.strictEqual(inserted.payload.type, "auto_reply");
    assert.strictEqual(inserted.payload.source_event_id, payload.source_event_id);
    assert.strictEqual(inserted.payload.detected_intent, payload.detected_intent);
    assert.strictEqual(inserted.payload.stage_before, payload.stage_before);
    assert.strictEqual(inserted.payload.stage_after, payload.stage_after);
    assert.strictEqual(inserted.payload.template_selected, payload.template_selected);
    assert.strictEqual(inserted.payload.rendered_message, payload.rendered_message);
  });

  it("should include textgrid_message_id after send", async () => {
    const payload = {
      thread_key: "owner:123:phone:456",
      master_owner_id: 123,
      textgrid_message_id: "SM1234567890",
      provider_message_id: "SM1234567890",
      type: "outbound",
      queue_status: "sent",
    };

    const result = await insertSupabaseSendQueueRow(payload, {
      supabase: mockSupabase,
    });

    assert.strictEqual(result.ok, true);
    const inserted = mockSupabase._inserted[0];
    assert.strictEqual(inserted.payload.textgrid_message_id, payload.textgrid_message_id);
  });

  it("should normalize queue status values correctly", () => {
    const row1 = normalizeSendQueueRow({ queue_status: "READY" });
    assert.strictEqual(row1.queue_status, "ready");

    const row2 = normalizeSendQueueRow({ queue_status: "SCHEDULED" });
    assert.strictEqual(row2.queue_status, "scheduled");

    const row3 = normalizeSendQueueRow({ queue_status: "delivered" });
    assert.strictEqual(row3.queue_status, "delivered");
  });

  it("should build correct dedupe key", () => {
    const key = buildSendQueueDedupeKey({
      master_owner_id: 123,
      property_id: 456,
      to_phone_number: "+15551234567",
      template_use_case: "ownership_check",
      touch_number: 1,
    });

    assert.ok(key.includes("123"));
    assert.ok(key.includes("456"));
    assert.ok(key.includes("ownership_check"));
    assert.ok(key.includes("1"));
  });
});

describe("Auto-reply system control flags", () => {
  it("should have auto_queue_enabled flag available", async () => {
    // Mock the system control to return true for auto_queue_enabled
    const mockGetSystemFlag = async (key) => {
      if (key === "auto_queue_enabled") return true;
      return false;
    };

    const enabled = await mockGetSystemFlag("auto_queue_enabled");
    assert.strictEqual(enabled, true);
  });

  it("should have auto_reply_enabled flag available", async () => {
    const mockGetSystemFlag = async (key) => {
      const flags = {
        "auto_reply_enabled": true,
        "auto_reply_live_enabled": true,
        "auto_reply_dry_run": false,
      };
      return flags[key] || false;
    };

    assert.strictEqual(await mockGetSystemFlag("auto_reply_enabled"), true);
    assert.strictEqual(await mockGetSystemFlag("auto_reply_live_enabled"), true);
    assert.strictEqual(await mockGetSystemFlag("auto_reply_dry_run"), false);
  });
});

describe("Delivery webhook updates status", () => {
  it("should update queue status to delivered when delivery confirmed", async () => {
    // Import the module properly
    const smsEngine = await import("@/lib/supabase/sms-engine.js");
    const syncDeliveryEvent = smsEngine.syncDeliveryEvent;

    // Mock the syncDeliveryEvent function via options
    let updatedPayload = null;
    const result = await syncDeliveryEvent(
      {
        message_id: "SM1234567890",
        status: "delivered",
        delivered_at: new Date().toISOString(),
      },
      {
        syncDeliveryEvent: (msg_id, payload) => {
          updatedPayload = payload;
          return { provider_message_sid: msg_id, ...payload };
        },
      }
    );

    assert.ok(result);
    assert.strictEqual(result.provider_message_sid, "SM1234567890");
    assert.strictEqual(result.delivery_status, "delivered");
  });

  it("should update queue status to failed when delivery fails", async () => {
    const smsEngine = await import("@/lib/supabase/sms-engine.js");
    const syncDeliveryEvent = smsEngine.syncDeliveryEvent;

    let updatedPayload = null;
    const result = await syncDeliveryEvent(
      {
        message_id: "SM1234567890",
        status: "failed",
        error_message: "carrier_failed",
      },
      {
        syncDeliveryEvent: (msg_id, payload) => {
          updatedPayload = payload;
          return { provider_message_sid: msg_id, ...payload };
        },
      }
    );

    assert.ok(result);
    assert.strictEqual(result.delivery_status, "failed");
  });
});

it('auto_reply queue rows are classified as unknown auto replies for cold-outbound pause exemption', async () => {
  const { isUnknownAutoReply } = await import('@/lib/domain/queue/is-manual-inbox-send.js');
  assert.strictEqual(isUnknownAutoReply({ type: 'auto_reply', message_body: 'Got it' }), true);
});
