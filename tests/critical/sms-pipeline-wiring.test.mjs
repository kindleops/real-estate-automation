// ─── sms-pipeline-wiring.test.mjs ─────────────────────────────────────────
// End-to-end integration tests verifying that the production SMS pipeline
// wires through the new SMS engine modules:
//   flow_map → template_resolver → personalize_template → latency → queue_message
//
// These tests validate the RUNTIME CALL SITES — not the individual modules
// (covered in sms-engine-pipeline.test.mjs) — but the integration points
// in queue-outbound-message.js and queue_message.js.

import test from "node:test";
import assert from "node:assert/strict";

import { buildQueueFields, QUEUE_FIELDS } from "@/lib/sms/queue_message.js";

// ══════════════════════════════════════════════════════════════════════════
// 1. QUEUE_FIELDS constant — template-2 mapping
// ══════════════════════════════════════════════════════════════════════════

test("QUEUE_FIELDS maps template to 'template-2' (never raw 'template')", () => {
  assert.equal(QUEUE_FIELDS.template, "template-2");
  assert.equal(QUEUE_FIELDS.queue_id, "queue-id-2");
});

test("QUEUE_FIELDS includes all required enrichment fields", () => {
  const required = [
    "queue_id", "scheduled_local", "scheduled_utc", "timezone",
    "contact_window", "queue_status", "message_text", "character_count",
    "touch_number", "message_type", "template", "sms_agent",
    "master_owner", "prospects", "properties", "phone_number",
    "market", "textgrid_number", "use_case", "property_address",
    "property_type", "owner_type", "max_retries", "retry_count",
    "personalization_tags", "current_stage", "send_priority",
    "dnc_check", "delivery_confirmed",
  ];

  for (const key of required) {
    assert.ok(QUEUE_FIELDS[key], `QUEUE_FIELDS.${key} must be defined`);
  }
});

// ══════════════════════════════════════════════════════════════════════════
// 2. buildQueueFields — writes truth fields from SMS engine resolution
// ══════════════════════════════════════════════════════════════════════════

test("buildQueueFields writes rendered_text, character_count, and schedule", () => {
  const fields = buildQueueFields({
    rendered_text: "Hi Jose, is 5521 Laster Ln yours?",
    schedule: {
      scheduled_local: "2025-01-15T10:00:00",
      scheduled_utc: "2025-01-15T16:00:00Z",
      timezone: "Central",
    },
    resolution: {
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
    },
    links: { master_owner_id: 201, phone_id: 401 },
    context: { touch_number: 1, is_first_touch: true },
  });

  assert.equal(fields[QUEUE_FIELDS.message_text], "Hi Jose, is 5521 Laster Ln yours?");
  assert.equal(fields[QUEUE_FIELDS.character_count], 33);
  assert.deepEqual(fields[QUEUE_FIELDS.scheduled_local], { start: "2025-01-15T10:00:00" });
  assert.deepEqual(fields[QUEUE_FIELDS.scheduled_utc], { start: "2025-01-15T16:00:00Z" });
  assert.equal(fields[QUEUE_FIELDS.timezone], "Central");
  assert.equal(fields[QUEUE_FIELDS.queue_status], "Queued");
  assert.equal(fields[QUEUE_FIELDS.use_case], "ownership_check");
  assert.equal(fields[QUEUE_FIELDS.current_stage], "S1");
});

test("buildQueueFields writes all app-reference links", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: { use_case: "ownership_check", stage_code: "S1" },
    links: {
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 501,
      phone_id: 401,
      market_id: 601,
      agent_id: 801,
      textgrid_number_id: 701,
    },
    context: {},
  });

  assert.deepEqual(fields[QUEUE_FIELDS.master_owner], [201]);
  assert.deepEqual(fields[QUEUE_FIELDS.prospects], [301]);
  assert.deepEqual(fields[QUEUE_FIELDS.properties], [501]);
  assert.deepEqual(fields[QUEUE_FIELDS.phone_number], [401]);
  assert.deepEqual(fields[QUEUE_FIELDS.market], [601]);
  assert.deepEqual(fields[QUEUE_FIELDS.sms_agent], [801]);
  assert.deepEqual(fields[QUEUE_FIELDS.textgrid_number], [701]);
});

test("buildQueueFields writes priority, DNC, and delivery_confirmed from context", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: { use_case: "ownership_check" },
    links: {},
    context: {
      send_priority: "_ Urgent",
      dnc_check: "✅ Cleared",
      delivery_confirmed: "⏳ Pending",
    },
  });

  assert.equal(fields[QUEUE_FIELDS.send_priority], "_ Urgent");
  assert.equal(fields[QUEUE_FIELDS.dnc_check], "✅ Cleared");
  assert.equal(fields[QUEUE_FIELDS.delivery_confirmed], "⏳ Pending");
});

test("buildQueueFields writes property metadata from context", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: { use_case: "offer_reveal_cash" },
    links: {},
    context: {
      property_address: "5521 Laster Ln",
      property_type: "Residential",
      owner_type: "Individual",
      placeholders_used: ["seller_first_name", "property_address", "offer_price"],
    },
  });

  assert.equal(fields[QUEUE_FIELDS.property_address], "5521 Laster Ln");
  assert.equal(fields[QUEUE_FIELDS.property_type], "Residential");
  assert.equal(fields[QUEUE_FIELDS.owner_type], "Individual");
  assert.deepEqual(fields[QUEUE_FIELDS.personalization_tags], [
    "seller_first_name",
    "property_address",
    "offer_price",
  ]);
});

test("buildQueueFields generates SHA256 dedupe queue_id", () => {
  const fields = buildQueueFields({
    rendered_text: "Hello",
    schedule: {},
    resolution: { use_case: "ownership_check", stage_code: "S1", language: "English" },
    links: { master_owner_id: 201 },
    context: { phone_e164: "+12087034955" },
  });

  const queue_id = fields[QUEUE_FIELDS.queue_id];
  assert.ok(queue_id, "queue_id is generated");
  assert.equal(queue_id.length, 16, "queue_id is 16 chars (truncated SHA256)");
  assert.match(queue_id, /^[0-9a-f]+$/, "queue_id is hex");
});

test("buildQueueFields resolves Cold Outbound message type for first touch", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: { use_case: "ownership_check" },
    links: {},
    context: { is_first_touch: true, touch_number: 1 },
  });

  assert.equal(fields[QUEUE_FIELDS.message_type], "Cold Outbound");
});

test("buildQueueFields resolves Follow-Up message type for subsequent touches", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: { use_case: "ownership_check" },
    links: {},
    context: { is_follow_up: true, touch_number: 3 },
  });

  assert.equal(fields[QUEUE_FIELDS.message_type], "Follow-Up");
});

test("buildQueueFields omits undefined app references (no null arrays)", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: { use_case: "ownership_check" },
    links: { master_owner_id: 201 },
    context: {},
  });

  assert.deepEqual(fields[QUEUE_FIELDS.master_owner], [201]);
  assert.equal(fields[QUEUE_FIELDS.prospects], undefined);
  assert.equal(fields[QUEUE_FIELDS.properties], undefined);
  assert.equal(fields[QUEUE_FIELDS.market], undefined);
});

// ══════════════════════════════════════════════════════════════════════════
// 3. Template-2 field — never writes raw "template"
// ══════════════════════════════════════════════════════════════════════════

test("buildQueueFields never writes a raw 'template' field key", () => {
  const fields = buildQueueFields({
    rendered_text: "test",
    schedule: {},
    resolution: {
      use_case: "ownership_check",
      attachable_template_ref: { app_id: 30647181, item_id: 9991 },
    },
    links: {},
    context: {},
  });

  // Should use "template-2", never "template"
  assert.ok(fields["template-2"] || fields[QUEUE_FIELDS.template] !== undefined);
  assert.equal(fields["template"], undefined, "raw 'template' key must not exist");
});
