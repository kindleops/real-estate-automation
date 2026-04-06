/**
 * Contact-window field safety tests for buildSendQueueItem.
 *
 * The Send Queue app (30680653) has `contact-window` as a category field.
 * The attached schema may be stale — it currently only has one live option
 * ("9AM-8PM CT", id=1).  Source contact windows from the Master Owners app
 * (e.g. "8AM-9AM CT", "12PM-2PM CT") are valid seller preference data but
 * often have no matching option ID in the schema.
 *
 * When normalizeCategoryValue cannot find an option ID it falls through to
 * shouldAllowRawCategoryCompatibility, which returns the raw text string.
 * Podio rejects raw text for category fields with a 400 error.
 *
 * The fix: resolveContactWindowField() checks for a valid option ID before
 * including the field.  If none exists the field is omitted and a warning is
 * logged — queue creation succeeds, scheduling is unaffected (times are already
 * encoded in scheduled_for_local/utc), and the queue runner allows sending
 * when contact-window is absent.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { buildSendQueueItem, resolveContactWindowField } from "@/lib/domain/queue/build-send-queue-item.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

// ── helpers ────────────────────────────────────────────────────────────────────

function createActivePhoneItem(item_id = 401) {
  return createPodioItem(item_id, {
    "phone-activity-status": categoryField("Active for 12 months or longer"),
    "phone-hidden": textField("9188102617"),
    "canonical-e164": textField("+19188102617"),
    "linked-master-owner": appRefField(201),
    "linked-contact": appRefField(301),
  });
}

function makeBaseContext(overrides = {}) {
  return {
    found: true,
    items: {
      phone_item: createActivePhoneItem(),
      brain_item: null,
      master_owner_item: createPodioItem(201),
      property_item: null,
      agent_item: null,
      market_item: null,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: null,
      market_id: null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: {
      total_messages_sent: 0,
      ...overrides.summary,
    },
    ...overrides,
  };
}

// ── resolveContactWindowField unit tests ───────────────────────────────────────

test("resolveContactWindowField: recognized schema option returns field_value and omitted=false", () => {
  // "9AM-8PM CT" is the one option present in the attached schema (id=1).
  const result = resolveContactWindowField("9AM-8PM CT");

  assert.equal(result.omitted, false, "should not be omitted for a known schema option");
  assert.equal(result.field_value, "9AM-8PM CT", "field_value should be the raw string");
  assert.equal(result.category_option_id, 1, "should resolve to option id 1");
  assert.equal(result.reason, null);
});

test("resolveContactWindowField: valid time-range format without schema option passes through via compat bypass", () => {
  // "8AM-9AM CT" is a valid source contact window from Master Owners.  It has no
  // matching option in the Send Queue schema (only "9AM-8PM CT" is an option),
  // but schema.js shouldAllowRawCategoryCompatibility / isValidSendQueueContactWindow
  // accepts the raw string so Podio does not return a 400.  We therefore INCLUDE it.
  const result = resolveContactWindowField("8AM-9AM CT");

  assert.equal(result.omitted, false, "valid time-range format must not be omitted");
  assert.equal(result.field_value, "8AM-9AM CT");
  assert.equal(result.category_option_id, null);
  assert.equal(result.reason, null);
});

test("resolveContactWindowField: another real-world seller window without schema option passes through via compat bypass", () => {
  const result = resolveContactWindowField("12PM-2PM CT");

  assert.equal(result.omitted, false);
  assert.equal(result.field_value, "12PM-2PM CT");
  assert.equal(result.reason, null);
});

test("resolveContactWindowField: empty / null / undefined contact window is omitted with 'empty' reason", () => {
  for (const value of ["", null, undefined]) {
    const result = resolveContactWindowField(value);
    assert.equal(result.omitted, true, `should be omitted for: ${JSON.stringify(value)}`);
    assert.equal(result.reason, "empty");
  }
});

// ── buildSendQueueItem integration tests ──────────────────────────────────────

test("buildSendQueueItem writes contact-window when value matches time-range format (compat bypass)", async () => {
  let captured_fields = null;

  const result = await buildSendQueueItem({
    context: makeBaseContext({
      summary: { total_messages_sent: 0, contact_window: "8AM-9AM CT" },
    }),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "8AM-9AM CT",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 1001 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok, "queue creation must succeed");
  assert.equal(result.contact_window_written, true, "contact_window_written must be true");
  assert.equal(result.contact_window_omit_reason, null, "no omit reason for a valid time-range");

  // The contact-window field IS in the Podio payload — schema.js compat bypass
  // allows valid time-range strings as raw text without an option ID.
  assert.equal(
    "contact-window" in (captured_fields || {}),
    true,
    "contact-window must be present in the Podio creation payload"
  );

  // Core fields must still be present
  assert.ok(captured_fields?.["scheduled-for-local"], "scheduled-for-local must be set");
  assert.ok(captured_fields?.["phone-number"], "phone-number must be set");
});

test("buildSendQueueItem writes contact-window when the value matches a known schema option", async () => {
  let captured_fields = null;

  const result = await buildSendQueueItem({
    context: makeBaseContext({
      summary: { total_messages_sent: 0, contact_window: "9AM-8PM CT" },
    }),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "9AM-8PM CT",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 1002 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok, "queue creation must succeed");
  assert.equal(result.contact_window_written, true, "contact_window_written must be true");
  assert.equal(result.contact_window_omit_reason, null);
  // The mock create_item captures the raw fields before normalizePodioFieldMap
  // converts the text to an option ID, so we assert the raw matched string.
  assert.equal(
    captured_fields?.["contact-window"],
    "9AM-8PM CT",
    "contact-window must be present in the payload as the matched raw string"
  );
});

test("buildSendQueueItem writes contact-window for 12PM-2PM CT (compat bypass, no schema option)", async () => {
  const result = await buildSendQueueItem({
    context: makeBaseContext({
      summary: { total_messages_sent: 0, contact_window: "12PM-2PM CT" },
    }),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 12:00:00",
    contact_window: "12PM-2PM CT",
    create_item: async () => ({ item_id: 1003 }),
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.contact_window_written, true, "valid time-range must be written");
  // No warning — the compat bypass handles it gracefully
  const has_contact_window_warning = result.warnings.some(
    (w) => typeof w === "string" && w.includes("contact-window field omitted")
  );
  assert.equal(has_contact_window_warning, false, "no omit warning for a valid time-range window");
});

test("buildSendQueueItem writing contact-window does not affect scheduling timestamps", async () => {
  // The scheduled_for_local and scheduled_for_utc fields encode the real
  // send time — they must be present regardless of contact-window outcome.
  let captured_fields = null;

  await buildSendQueueItem({
    context: makeBaseContext({
      summary: { total_messages_sent: 0, contact_window: "8AM-9AM CT" },
    }),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 08:30:00",
    scheduled_for_utc: "2026-04-04 13:30:00",
    contact_window: "8AM-9AM CT",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 1004 };
    },
    update_item: async () => {},
  });

  assert.ok(captured_fields?.["scheduled-for-local"]?.start, "scheduled-for-local must be set");
  assert.ok(captured_fields?.["scheduled-for-utc"]?.start, "scheduled-for-utc must be set");
  // contact-window IS written now (compat bypass allows valid time-range strings)
  assert.equal("contact-window" in captured_fields, true, "contact-window must be present");
  assert.equal(captured_fields["contact-window"], "8AM-9AM CT");
});
