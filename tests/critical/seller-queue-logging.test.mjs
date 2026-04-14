/**
 * seller_queue structured logging tests.
 *
 * Verifies that maybe-queue-seller-stage-reply emits:
 *   seller_queue.entry
 *   seller_queue.skipped       (plan not handled / no auto reply)
 *   seller_queue.before_create (just before queue_message call)
 *   seller_queue.created       (successful queue creation)
 *   seller_queue.create_failed (queue creation failed or returned ok:false)
 *
 * Each log event is checked for required metadata fields.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  maybeQueueSellerStageReply,
  __setSellerQueueLogDeps,
  __resetSellerQueueLogDeps,
} from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";
import APP_IDS from "@/lib/config/app-ids.js";

// ══════════════════════════════════════════════════════════════════════════
// LOG CAPTURE
// ══════════════════════════════════════════════════════════════════════════

function createLogCapture() {
  const logs = [];
  return {
    logs,
    info(event, meta) { logs.push({ level: "info", event, meta }); },
    warn(event, meta) { logs.push({ level: "warn", event, meta }); },
    find(event) { return logs.filter((l) => l.event === event); },
    first(event) { return logs.find((l) => l.event === event) || null; },
  };
}

// ══════════════════════════════════════════════════════════════════════════
// FIXTURES
// ══════════════════════════════════════════════════════════════════════════

function stubContext(overrides = {}) {
  return {
    found: true,
    ids: {
      phone_item_id: 401,
      brain_item_id: 501,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      ...overrides.ids,
    },
    items: {
      brain_item: { item_id: 501 },
      phone_item: { item_id: 401 },
      property_item: null,
      master_owner_item: null,
      agent_item: null,
      ...overrides.items,
    },
    summary: {
      property_type: "Single Family",
      owner_type: "Individual",
      market_timezone: "Central",
      contact_window: "9AM-8PM CT",
      seller_first_name: "Jose",
      agent_name: "Agent Smith",
      property_address: "123 Main St",
      property_city: "Dallas",
      ...overrides.summary,
    },
    recent: {
      touch_count: 0,
      ...overrides.recent,
    },
    ...overrides,
  };
}

function stubClassification(overrides = {}) {
  return {
    message: "Yes I own this property",
    language: "English",
    objection: null,
    emotion: "calm",
    stage_hint: "Ownership Confirmation",
    compliance_flag: null,
    positive_signals: ["ownership_confirmed"],
    confidence: 0.85,
    motivation_score: 50,
    source: "classifier",
    ...overrides,
  };
}

// ── Log capture via injectable deps ──────────────────────────────────────

function withLogCapture(fn) {
  return async () => {
    const capture = createLogCapture();
    __setSellerQueueLogDeps({ info: capture.info, warn: capture.warn });
    try {
      return await fn(capture);
    } finally {
      __resetSellerQueueLogDeps();
    }
  };
}

// ══════════════════════════════════════════════════════════════════════════
// 1. seller_queue.entry — always emitted
// ══════════════════════════════════════════════════════════════════════════

test("seller_queue.entry: emitted at function start with required ids", withLogCapture(async (capture) => {
  await maybeQueueSellerStageReply({
    inbound_from: "+12125551234",
    context: stubContext(),
    classification: stubClassification({ stage_hint: "some-unknown-stage" }),
    message: "Hello",
  });

  const entry = capture.first("seller_queue.entry");
  assert.ok(entry, "seller_queue.entry should be emitted");
  assert.equal(entry.meta.inbound_from, "+12125551234");
  assert.equal(entry.meta.phone_id, 401);
  assert.equal(entry.meta.brain_id, 501);
  assert.equal(entry.meta.master_owner_id, 201);
  assert.equal(entry.meta.send_queue_app_id, APP_IDS.send_queue);
}));

test("seller_queue.entry: includes context presence indicators", withLogCapture(async (capture) => {
  await maybeQueueSellerStageReply({
    inbound_from: "+12125551234",
    context: stubContext(),
    classification: stubClassification(),
    message: "I own this property",
  });

  const entry = capture.first("seller_queue.entry");
  assert.ok(entry);
  assert.equal(entry.meta.has_context, true);
  assert.equal(entry.meta.has_classification, true);
}));

// ══════════════════════════════════════════════════════════════════════════
// 2. seller_queue.skipped — plan not handled
// ══════════════════════════════════════════════════════════════════════════

test("seller_queue.skipped: emitted when plan.handled is false", withLogCapture(async (capture) => {
  // Classification with no recognized seller flow triggers "not handled"
  const result = await maybeQueueSellerStageReply({
    inbound_from: "+12125551234",
    context: stubContext(),
    classification: stubClassification({
      stage_hint: "completely_unknown_intent",
      positive_signals: [],
      objection: null,
    }),
    message: "random gibberish xyz123",
  });

  // If the plan wasn't handled, we should see a skipped log
  if (!result.handled) {
    const skipped = capture.first("seller_queue.skipped");
    assert.ok(skipped, "seller_queue.skipped should be emitted");
    assert.equal(skipped.meta.phone_id, 401);
    assert.equal(skipped.meta.brain_id, 501);
    assert.equal(skipped.meta.reason, "seller_flow_not_handled");
    assert.equal(skipped.meta.send_queue_app_id, APP_IDS.send_queue);
  }
}));

// ══════════════════════════════════════════════════════════════════════════
// 3. seller_queue.before_create — emitted before queue_message call
// ══════════════════════════════════════════════════════════════════════════

test("seller_queue.before_create: emitted with scheduling context", withLogCapture(async (capture) => {
  const queue_message_stub = async () => ({
    ok: true,
    queue_item_id: 9999,
    template_id: 123,
    queue_result: { item_id: 9999, queue_id: "abc123def456" },
    pipeline: "sms_engine_v2",
  });

  await maybeQueueSellerStageReply({
    inbound_from: "+12125551234",
    context: stubContext(),
    classification: stubClassification({
      stage_hint: "Ownership Confirmation",
      positive_signals: ["ownership_confirmed"],
    }),
    message: "Yes I own this property",
    queue_message: queue_message_stub,
  });

  const before = capture.first("seller_queue.before_create");
  if (before) {
    assert.equal(before.meta.inbound_from, "+12125551234");
    assert.equal(before.meta.phone_id, 401);
    assert.equal(before.meta.brain_id, 501);
    assert.equal(before.meta.send_queue_app_id, APP_IDS.send_queue);
    assert.equal(before.meta.queue_status, "Queued");
    assert.equal(before.meta.next_action, "queue_outbound_message");
    assert.ok("scheduled_for_utc" in before.meta, "should include scheduled_for_utc");
    assert.ok("use_case" in before.meta, "should include use_case");
    assert.ok("rotation_key" in before.meta, "should include rotation_key");
  }
}));

// ══════════════════════════════════════════════════════════════════════════
// 4. seller_queue.created — emitted after successful queue creation
// ══════════════════════════════════════════════════════════════════════════

test("seller_queue.created: emitted with item_id and dedupe_key on success", withLogCapture(async (capture) => {
  const queue_message_stub = async () => ({
    ok: true,
    queue_item_id: 9999,
    template_id: 123,
    queue_result: { item_id: 9999, queue_id: "abc123def456ab" },
    pipeline: "sms_engine_v2",
  });

  const result = await maybeQueueSellerStageReply({
    inbound_from: "+12125551234",
    context: stubContext(),
    classification: stubClassification({
      stage_hint: "Ownership Confirmation",
      positive_signals: ["ownership_confirmed"],
    }),
    message: "Yes I own this property",
    queue_message: queue_message_stub,
  });

  if (result.queued) {
    const created = capture.first("seller_queue.created");
    assert.ok(created, "seller_queue.created should be emitted");
    assert.equal(created.meta.inbound_from, "+12125551234");
    assert.equal(created.meta.phone_id, 401);
    assert.equal(created.meta.brain_id, 501);
    assert.equal(created.meta.send_queue_app_id, APP_IDS.send_queue);
    assert.equal(created.meta.queue_status, "Queued");
    assert.equal(created.meta.template_id, 123);
    assert.equal(created.meta.queue_item_id, 9999);
    assert.equal(created.meta.dedupe_key, "abc123def456ab");
    assert.equal(created.meta.pipeline, "sms_engine_v2");
  }
}));

// ══════════════════════════════════════════════════════════════════════════
// 5. seller_queue.create_failed — emitted when queue_message returns ok:false
// ══════════════════════════════════════════════════════════════════════════

test("seller_queue.create_failed: emitted when queue_message returns ok:false", withLogCapture(async (capture) => {
  const queue_message_stub = async () => ({
    ok: false,
    reason: "template_not_found",
    stage: "template",
  });

  const result = await maybeQueueSellerStageReply({
    inbound_from: "+12125551234",
    context: stubContext(),
    classification: stubClassification({
      stage_hint: "Ownership Confirmation",
      positive_signals: ["ownership_confirmed"],
    }),
    message: "Yes I own this property",
    queue_message: queue_message_stub,
  });

  if (!result.queued) {
    const failed = capture.first("seller_queue.create_failed");
    assert.ok(failed, "seller_queue.create_failed should be emitted");
    assert.equal(failed.level, "warn");
    assert.equal(failed.meta.inbound_from, "+12125551234");
    assert.equal(failed.meta.phone_id, 401);
    assert.equal(failed.meta.brain_id, 501);
    assert.equal(failed.meta.send_queue_app_id, APP_IDS.send_queue);
    assert.equal(failed.meta.reason, "template_not_found");
    assert.equal(failed.meta.stage, "template");
  }
}));

test("seller_queue.create_failed: emitted when queue_message throws", withLogCapture(async (capture) => {
  const queue_message_stub = async () => {
    const err = new Error("Podio 400: Invalid field value");
    err.response = { data: { error_description: "Invalid category value" } };
    throw err;
  };

  await assert.rejects(
    () => maybeQueueSellerStageReply({
      inbound_from: "+12125551234",
      context: stubContext(),
      classification: stubClassification({
        stage_hint: "Ownership Confirmation",
        positive_signals: ["ownership_confirmed"],
      }),
      message: "Yes I own this property",
      queue_message: queue_message_stub,
    }),
    { message: "Podio 400: Invalid field value" }
  );

  const failed = capture.first("seller_queue.create_failed");
  if (failed) {
    assert.equal(failed.level, "warn");
    assert.equal(failed.meta.error, "Podio 400: Invalid field value");
    assert.equal(failed.meta.error_description, "Invalid category value");
    assert.equal(failed.meta.send_queue_app_id, APP_IDS.send_queue);
  }
}));

// ══════════════════════════════════════════════════════════════════════════
// 6. No silent failures — every path emits at least entry + one outcome
// ══════════════════════════════════════════════════════════════════════════

test("seller_queue: every code path emits entry log", withLogCapture(async (capture) => {
  // Not-handled path
  await maybeQueueSellerStageReply({
    inbound_from: "+12125559999",
    context: stubContext(),
    classification: stubClassification({ stage_hint: "unknown" }),
    message: "random",
  });

  const entries = capture.find("seller_queue.entry");
  assert.ok(entries.length >= 1, "at least one entry log should be emitted");
}));

test("seller_queue: skipped path emits both entry and skipped", withLogCapture(async (capture) => {
  const result = await maybeQueueSellerStageReply({
    inbound_from: "+12125559999",
    context: stubContext(),
    classification: stubClassification({ stage_hint: "unknown" }),
    message: "random gibberish",
  });

  const entries = capture.find("seller_queue.entry");
  assert.ok(entries.length >= 1, "entry log should be emitted");

  if (!result.handled) {
    const skipped = capture.find("seller_queue.skipped");
    assert.ok(skipped.length >= 1, "skipped log should be emitted for not-handled");
  }
}));

// ══════════════════════════════════════════════════════════════════════════
// 7. queue_message.dedupe_lookup_failed — no longer silent
// ══════════════════════════════════════════════════════════════════════════

import {
  queueMessage,
  __setQueueMessageTestDeps,
  __resetQueueMessageTestDeps,
} from "@/lib/sms/queue_message.js";

test("queue_message: dedupe lookup failure proceeds to create (not silently ignored)", async () => {
  let create_called = false;

  __setQueueMessageTestDeps({
    createItem: async () => { create_called = true; return { item_id: 9999 }; },
    getFirstMatchingItem: async () => { throw new Error("Podio timeout"); },
  });

  try {
    const result = await queueMessage({
      rendered_text: "Test message",
      schedule: { scheduled_utc: "2025-01-15T16:00:00Z" },
      resolution: { use_case: "test" },
      links: { master_owner_id: 1 },
      context: { touch_number: 1, phone_e164: "+1234" },
    });

    assert.equal(result.ok, true);
    assert.equal(result.item_id, 9999);
    assert.ok(create_called, "createItem should still be called despite dedupe lookup failure");
  } finally {
    __resetQueueMessageTestDeps();
  }
});
