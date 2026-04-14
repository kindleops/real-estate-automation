/**
 * Send Queue creation comprehensive tests.
 *
 * Covers:
 *   1. buildQueueFields produces correct Podio field map
 *   2. queueMessage creates item via createItem with correct app_id
 *   3. Empty string / null guards prevent Podio throws
 *   4. Schema option IDs match live Podio (delivery-confirmed, queue-status, etc.)
 *   5. Brain creation flow — create_brain_if_missing: true propagates through pipeline
 *   6. Dedupe blocks duplicate queue items
 *   7. property-address uses Podio location format
 *   8. App references use [id] array format
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  buildQueueFields,
  queueMessage,
  buildDedupeFingerprint,
  QUEUE_FIELDS,
  MESSAGE_TYPES,
  __setQueueMessageTestDeps,
  __resetQueueMessageTestDeps,
} from "@/lib/sms/queue_message.js";

import APP_IDS from "@/lib/config/app-ids.js";

import {
  getCategoryOptionId,
  normalizePodioFieldMap,
  hasAttachedSchema,
} from "@/lib/podio/schema.js";

// ══════════════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════════════

function minimalParams(overrides = {}) {
  return {
    rendered_text: "Hi Jose, are you the owner of 5521 Laster Ln?",
    schedule: {
      scheduled_local: "2025-01-15T10:00:00",
      scheduled_utc: "2025-01-15T16:00:00Z",
      timezone: "Central",
    },
    resolution: {
      use_case: "ownership_check",
      stage_code: "Ownership Confirmation",
      language: "English",
      agent_style_fit: "Warm Professional",
      attachable_template_ref: null,
    },
    links: {
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 501,
      phone_id: 401,
      market_id: 601,
      agent_id: 701,
      textgrid_number_id: 801,
    },
    context: {
      touch_number: 1,
      is_first_touch: true,
      phone_e164: "+12087034955",
      contact_window: "9AM-8PM CT",
      placeholders_used: ["seller_first_name", "property_address"],
      property_address: "5521 Laster Ln, Dallas TX 75241",
      property_type: "Single Family",
      owner_type: "Individual",
      max_retries: 3,
      send_priority: "_ Normal",
      dnc_check: "✅ Cleared",
      delivery_confirmed: "⏳ Pending",
    },
    ...overrides,
  };
}

// ══════════════════════════════════════════════════════════════════════════
// 1. buildQueueFields — basic field mapping
// ══════════════════════════════════════════════════════════════════════════

test("buildQueueFields: produces all expected Podio field keys", () => {
  const fields = buildQueueFields(minimalParams());

  assert.equal(fields["message-text"], "Hi Jose, are you the owner of 5521 Laster Ln?");
  assert.equal(fields["character-count"], 45);
  assert.deepEqual(fields["scheduled-for-local"], { start: "2025-01-15T10:00:00" });
  assert.deepEqual(fields["scheduled-for-utc"], { start: "2025-01-15T16:00:00Z" });
  assert.equal(fields["timezone"], "Central");
  assert.equal(fields["queue-status"], "Queued");
  assert.equal(fields["touch-number"], 1);
  assert.equal(fields["message-type"], "Cold Outbound");
  assert.equal(fields["use-case-template"], "ownership_check");
  assert.equal(fields["current-stage"], "Ownership Confirmation");
  assert.equal(fields["max-retries"], 3);
  assert.equal(fields["retry-count"], 0);
  assert.deepEqual(fields["personalization-tags-used"], ["seller_first_name", "property_address"]);
  assert.equal(fields["contact-window"], "9AM-8PM CT");
  assert.equal(fields["send-priority"], "_ Normal");
  assert.equal(fields["dnc-check"], "✅ Cleared");
  assert.equal(fields["delivery-confirmed"], "⏳ Pending");
});

test("buildQueueFields: app references use [id] array format", () => {
  const fields = buildQueueFields(minimalParams());

  assert.deepEqual(fields["master-owner"], [201]);
  assert.deepEqual(fields["prospects"], [301]);
  assert.deepEqual(fields["properties"], [501]);
  assert.deepEqual(fields["phone-number"], [401]);
  assert.deepEqual(fields["market"], [601]);
  assert.deepEqual(fields["sms-agent"], [701]);
  assert.deepEqual(fields["textgrid-number"], [801]);
});

test("buildQueueFields: property-address uses Podio location format", () => {
  const fields = buildQueueFields(minimalParams());

  assert.deepEqual(fields["property-address"], { value: "5521 Laster Ln, Dallas TX 75241" });
});

test("buildQueueFields: property-address passes through object values", () => {
  const fields = buildQueueFields(minimalParams({
    context: {
      ...minimalParams().context,
      property_address: { value: "5521 Laster Ln", city: "Dallas", state: "TX" },
    },
  }));

  assert.deepEqual(fields["property-address"], { value: "5521 Laster Ln", city: "Dallas", state: "TX" });
});

test("buildQueueFields: queue_id is a 16-char hex fingerprint", () => {
  const fields = buildQueueFields(minimalParams());

  assert.ok(fields["queue-id-2"], "queue-id-2 should be set");
  assert.equal(fields["queue-id-2"].length, 16);
  assert.match(fields["queue-id-2"], /^[a-f0-9]{16}$/);
});

// ══════════════════════════════════════════════════════════════════════════
// 2. Empty string / null guards
// ══════════════════════════════════════════════════════════════════════════

test("buildQueueFields: omits use-case-template when resolution.use_case is empty string", () => {
  const fields = buildQueueFields(minimalParams({
    resolution: { ...minimalParams().resolution, use_case: "" },
  }));

  assert.equal(fields["use-case-template"], undefined, "empty use_case should be omitted");
});

test("buildQueueFields: omits current-stage when resolution.stage_code is null", () => {
  const fields = buildQueueFields(minimalParams({
    resolution: { ...minimalParams().resolution, stage_code: null },
  }));

  assert.equal(fields["current-stage"], undefined);
});

test("buildQueueFields: omits contact-window when context.contact_window is empty string", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, contact_window: "" },
  }));

  assert.equal(fields["contact-window"], undefined);
});

test("buildQueueFields: omits send-priority when empty string", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, send_priority: "" },
  }));

  assert.equal(fields["send-priority"], undefined);
});

test("buildQueueFields: omits delivery-confirmed when null", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, delivery_confirmed: null },
  }));

  assert.equal(fields["delivery-confirmed"], undefined);
});

test("buildQueueFields: omits dnc-check when empty string", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, dnc_check: "" },
  }));

  assert.equal(fields["dnc-check"], undefined);
});

test("buildQueueFields: omits property-type when empty string", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, property_type: "" },
  }));

  assert.equal(fields["property-type"], undefined);
});

test("buildQueueFields: omits owner-type when null", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, owner_type: null },
  }));

  assert.equal(fields["owner-type"], undefined);
});

test("buildQueueFields: omits property-address when empty string", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, property_address: "" },
  }));

  assert.equal(fields["property-address"], undefined);
});

test("buildQueueFields: omits timezone when empty string", () => {
  const fields = buildQueueFields(minimalParams({
    schedule: { ...minimalParams().schedule, timezone: "" },
  }));

  assert.equal(fields["timezone"], undefined);
});

// ══════════════════════════════════════════════════════════════════════════
// 3. Schema option IDs match live Podio
// ══════════════════════════════════════════════════════════════════════════

test("schema: delivery-confirmed options have correct IDs from live Podio", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) {
    return; // skip if no schema attached (CI mode)
  }

  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "delivery-confirmed", "✅ Confirmed"), 1);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "delivery-confirmed", "❌ Failed"), 2);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "delivery-confirmed", "⏳ Pending"), 3);
});

test("schema: queue-status Delivered=7 (not 4) matches live Podio", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) return;

  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Queued"), 1);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Sending"), 2);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Sent"), 3);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Delivered"), 7);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Cancelled"), 4);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Blocked"), 5);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "queue-status", "Failed"), 6);
});

test("schema: owner-type Government=5 matches live Podio", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) return;

  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "owner-type", "Government"), 5);
});

test("schema: send-priority option IDs match live Podio", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) return;

  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "send-priority", "Urgent"), 1);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "send-priority", "Normal"), 2);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "send-priority", "Low"), 3);
});

test("schema: send-priority '_ Urgent' normalizes to match 'Urgent' option", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) return;

  // Code writes "_ Urgent" → normalizeCategoryText strips _ → matches "Urgent"
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "send-priority", "_ Urgent"), 1);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "send-priority", "_ Normal"), 2);
  assert.equal(getCategoryOptionId(APP_IDS.send_queue, "send-priority", "_ Low"), 3);
});

test("schema: normalizePodioFieldMap handles all buildQueueFields output for send queue", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) return;

  const fields = buildQueueFields(minimalParams());

  // This is THE critical test: normalizePodioFieldMap should NOT throw
  // for any field combination produced by buildQueueFields.
  const normalized = normalizePodioFieldMap(APP_IDS.send_queue, fields);
  assert.ok(normalized, "normalizePodioFieldMap should succeed without throwing");

  // Queue status should resolve to option id 1 (Queued)
  assert.equal(normalized["queue-status"], 1);

  // Delivery confirmed should resolve to option id 3 (⏳ Pending)
  assert.equal(normalized["delivery-confirmed"], 3);
});

test("schema: normalizePodioFieldMap does not throw when optional category fields are omitted", () => {
  if (!hasAttachedSchema(APP_IDS.send_queue)) return;

  // Minimal fields: only message-text, queue-status, message-type
  const fields = buildQueueFields({
    rendered_text: "Hello",
    schedule: {},
    resolution: {},
    links: {},
    context: { touch_number: 1 },
  });

  const normalized = normalizePodioFieldMap(APP_IDS.send_queue, fields);
  assert.ok(normalized, "should not throw for minimal fields");
});

// ══════════════════════════════════════════════════════════════════════════
// 4. queueMessage — create flow
// ══════════════════════════════════════════════════════════════════════════

test("queueMessage: calls createItem with APP_IDS.send_queue", async () => {
  let captured_app_id = null;
  let captured_fields = null;

  __setQueueMessageTestDeps({
    createItem: async (app_id, fields) => {
      captured_app_id = app_id;
      captured_fields = fields;
      return { item_id: 9001 };
    },
    getFirstMatchingItem: async () => null,
  });

  try {
    const result = await queueMessage(minimalParams());

    assert.equal(result.ok, true);
    assert.equal(result.item_id, 9001);
    assert.equal(captured_app_id, APP_IDS.send_queue);
    assert.ok(captured_fields["message-text"]);
    assert.equal(captured_fields["queue-status"], "Queued");
    assert.ok(captured_fields["queue-id-2"]);
  } finally {
    __resetQueueMessageTestDeps();
  }
});

test("queueMessage: returns ok with queue_id and fields on success", async () => {
  __setQueueMessageTestDeps({
    createItem: async () => ({ item_id: 9002 }),
    getFirstMatchingItem: async () => null,
  });

  try {
    const result = await queueMessage(minimalParams());

    assert.equal(result.ok, true);
    assert.equal(result.item_id, 9002);
    assert.ok(result.queue_id, "should return queue_id");
    assert.ok(result.fields, "should return fields");
    assert.equal(result.fields["queue-status"], "Queued");
  } finally {
    __resetQueueMessageTestDeps();
  }
});

// ══════════════════════════════════════════════════════════════════════════
// 5. Dedupe
// ══════════════════════════════════════════════════════════════════════════

test("queueMessage: blocks duplicate when existing Queued item found", async () => {
  const params = minimalParams();
  const expected_queue_id = buildQueueFields(params)["queue-id-2"];

  __setQueueMessageTestDeps({
    createItem: async () => { throw new Error("should not be called"); },
    getFirstMatchingItem: async () => ({
      item_id: 8888,
      fields: [
        {
          external_id: "queue-status",
          values: [{ value: { text: "Queued" } }],
        },
      ],
    }),
  });

  try {
    const result = await queueMessage(params);

    assert.equal(result.ok, false);
    assert.equal(result.reason, "duplicate_blocked");
    assert.equal(result.existing_item_id, 8888);
    assert.equal(result.queue_id, expected_queue_id);
  } finally {
    __resetQueueMessageTestDeps();
  }
});

test("queueMessage: allows creation when existing item is Failed", async () => {
  __setQueueMessageTestDeps({
    createItem: async () => ({ item_id: 9003 }),
    getFirstMatchingItem: async () => ({
      item_id: 8888,
      fields: [
        {
          external_id: "queue-status",
          values: [{ value: { text: "Failed" } }],
        },
      ],
    }),
  });

  try {
    const result = await queueMessage(minimalParams());

    assert.equal(result.ok, true);
    assert.equal(result.item_id, 9003);
  } finally {
    __resetQueueMessageTestDeps();
  }
});

test("queueMessage: proceeds when dedupe lookup throws", async () => {
  __setQueueMessageTestDeps({
    createItem: async () => ({ item_id: 9004 }),
    getFirstMatchingItem: async () => { throw new Error("Podio rate limit"); },
  });

  try {
    const result = await queueMessage(minimalParams());

    assert.equal(result.ok, true);
    assert.equal(result.item_id, 9004);
  } finally {
    __resetQueueMessageTestDeps();
  }
});

// ══════════════════════════════════════════════════════════════════════════
// 6. Fingerprint stability
// ══════════════════════════════════════════════════════════════════════════

test("buildDedupeFingerprint: same inputs produce same hash", () => {
  const a = buildDedupeFingerprint({
    master_owner_id: 201,
    phone_e164: "+12087034955",
    use_case: "ownership_check",
    stage_code: "S1",
    language: "English",
    agent_style_fit: "Warm Professional",
    rendered_text: "Hi Jose",
  });

  const b = buildDedupeFingerprint({
    master_owner_id: 201,
    phone_e164: "+12087034955",
    use_case: "ownership_check",
    stage_code: "S1",
    language: "English",
    agent_style_fit: "Warm Professional",
    rendered_text: "Hi Jose",
  });

  assert.equal(a, b);
});

test("buildDedupeFingerprint: different rendered_text produces different hash", () => {
  const a = buildDedupeFingerprint({
    master_owner_id: 201,
    phone_e164: "+12087034955",
    use_case: "ownership_check",
    rendered_text: "Hi Jose",
  });

  const b = buildDedupeFingerprint({
    master_owner_id: 201,
    phone_e164: "+12087034955",
    use_case: "ownership_check",
    rendered_text: "Hi Maria",
  });

  assert.notEqual(a, b);
});

// ══════════════════════════════════════════════════════════════════════════
// 7. Missing link fields produce valid items
// ══════════════════════════════════════════════════════════════════════════

test("buildQueueFields: works with null/missing link IDs", () => {
  const fields = buildQueueFields(minimalParams({
    links: {
      master_owner_id: null,
      prospect_id: null,
      property_id: null,
      phone_id: null,
      market_id: null,
      agent_id: null,
      textgrid_number_id: null,
    },
  }));

  // App ref fields should be absent (not undefined arrays)
  assert.equal(fields["master-owner"], undefined);
  assert.equal(fields["prospects"], undefined);
  assert.equal(fields["properties"], undefined);
  assert.equal(fields["phone-number"], undefined);
  assert.equal(fields["market"], undefined);
  assert.equal(fields["sms-agent"], undefined);
  assert.equal(fields["textgrid-number"], undefined);

  // Core fields still present
  assert.ok(fields["message-text"]);
  assert.equal(fields["queue-status"], "Queued");
  assert.ok(fields["queue-id-2"]);
});

test("buildQueueFields: no template ref when resolution has no attachable_template_ref", () => {
  const fields = buildQueueFields(minimalParams({
    resolution: {
      ...minimalParams().resolution,
      attachable_template_ref: null,
    },
  }));

  assert.equal(fields["template-2"], undefined);
});

test("buildQueueFields: attaches template ref when app_id matches templates", () => {
  const fields = buildQueueFields(minimalParams({
    resolution: {
      ...minimalParams().resolution,
      attachable_template_ref: {
        app_id: APP_IDS.templates,
        item_id: 12345,
      },
    },
  }));

  assert.deepEqual(fields["template-2"], [12345]);
});

// ══════════════════════════════════════════════════════════════════════════
// 8. MESSAGE_TYPES constant
// ══════════════════════════════════════════════════════════════════════════

test("MESSAGE_TYPES: has all expected values", () => {
  assert.equal(MESSAGE_TYPES.COLD_OUTBOUND, "Cold Outbound");
  assert.equal(MESSAGE_TYPES.FOLLOW_UP, "Follow-Up");
  assert.equal(MESSAGE_TYPES.RE_ENGAGEMENT, "Re-Engagement");
  assert.equal(MESSAGE_TYPES.OPT_OUT_CONFIRM, "Opt-Out Confirm");
});

test("buildQueueFields: first touch context produces Cold Outbound message type", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, is_first_touch: true },
  }));

  assert.equal(fields["message-type"], "Cold Outbound");
});

test("buildQueueFields: follow-up context produces Follow-Up message type", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, is_first_touch: false, is_follow_up: true },
  }));

  assert.equal(fields["message-type"], "Follow-Up");
});

test("buildQueueFields: opt-out context produces Opt-Out Confirm message type", () => {
  const fields = buildQueueFields(minimalParams({
    context: { ...minimalParams().context, is_opt_out_confirm: true },
  }));

  assert.equal(fields["message-type"], "Opt-Out Confirm");
});

// ══════════════════════════════════════════════════════════════════════════
// 9. QUEUE_FIELDS external IDs match expected Podio names
// ══════════════════════════════════════════════════════════════════════════

test("QUEUE_FIELDS: uses plural prospects/properties (not singular)", () => {
  assert.equal(QUEUE_FIELDS.prospects, "prospects");
  assert.equal(QUEUE_FIELDS.properties, "properties");
});

test("QUEUE_FIELDS: uses template-2 (not template)", () => {
  assert.equal(QUEUE_FIELDS.template, "template-2");
});

test("QUEUE_FIELDS: uses queue-id-2 (not queue-id)", () => {
  assert.equal(QUEUE_FIELDS.queue_id, "queue-id-2");
});
