/**
 * Send Queue payload builder tests — message-text normalization and new enrichment fields.
 *
 * Part 1: message-text truncation fix
 *   - Multiline rendered SMS is flattened to a single space-separated line
 *   - character-count is based on the normalized single-line value
 *   - CRLF, LF, and CR variants are all handled
 *
 * Part 2+3: New enrichment fields
 *   - property-address written from a real Property item (omitted when no real property_id)
 *   - property-type safely omitted when schema has no matching option
 *   - category safely omitted when schema has no matching option
 *   - use-case-template safely omitted when schema has no matching option
 *   - resolveQueueCategoryField unit behaviour
 *
 * Part 4: normalizeForQueueText unit behaviour
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  buildSendQueueItem,
  resolveQueueCategoryField,
  normalizeForQueueText,
  _matchCategoryOption,
} from "@/lib/domain/queue/build-send-queue-item.js";

import {
  appRefField,
  categoryField,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

// ── helpers ────────────────────────────────────────────────────────────────────

function makeActivePhoneItem(item_id = 401) {
  return createPodioItem(item_id, {
    "phone-activity-status": categoryField("Active for 12 months or longer"),
    "phone-hidden": textField("9188102617"),
    "canonical-e164": textField("+19188102617"),
    "linked-master-owner": appRefField(201),
    "linked-contact": appRefField(301),
  });
}

function makeBaseContext({ property_item = null, market_item = null } = {}) {
  return {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item: createPodioItem(201),
      property_item,
      agent_item: null,
      market_item,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: property_item?.item_id ?? null,
      market_id: market_item?.item_id ?? null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: { total_messages_sent: 0 },
  };
}

async function buildAndCapture(overrides = {}) {
  let captured_fields = null;
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9000 };
    },
    update_item: async () => {},
    ...overrides,
  });
  return { result, captured_fields };
}

// ── Part 4: normalizeForQueueText unit tests ───────────────────────────────────

test("normalizeForQueueText: LF newlines replaced by single space", () => {
  assert.equal(normalizeForQueueText("Hello\nWorld"), "Hello World");
});

test("normalizeForQueueText: CRLF newlines replaced by single space", () => {
  assert.equal(normalizeForQueueText("Hello\r\nWorld"), "Hello World");
});

test("normalizeForQueueText: CR-only replaced by single space", () => {
  assert.equal(normalizeForQueueText("Hello\rWorld"), "Hello World");
});

test("normalizeForQueueText: mixed newlines collapsed and trimmed", () => {
  assert.equal(
    normalizeForQueueText("  Line one\r\nLine two\nLine three  "),
    "Line one Line two Line three"
  );
});

test("normalizeForQueueText: repeated whitespace collapsed to single space", () => {
  assert.equal(normalizeForQueueText("Hello   World"), "Hello World");
});

test("normalizeForQueueText: empty / null / undefined returns empty string", () => {
  assert.equal(normalizeForQueueText(""), "");
  assert.equal(normalizeForQueueText(null), "");
  assert.equal(normalizeForQueueText(undefined), "");
});

test("normalizeForQueueText: single-line text is unchanged (apart from trim)", () => {
  assert.equal(normalizeForQueueText("  Hello World  "), "Hello World");
});

// ── Part 1: message-text and character-count use normalized value ──────────────

test("Part 1 — message-text written as single-line when rendered SMS has LF newlines", async () => {
  const multiline = "Hi Ryan,\nWe noticed your property at 123 Main St.\nInterested in selling?";
  const { captured_fields } = await buildAndCapture({ rendered_message_text: multiline });

  const written = captured_fields?.["message-text"];
  assert.ok(written, "message-text must be present");
  assert.ok(!written.includes("\n"), "message-text must contain no LF characters");
  assert.ok(!written.includes("\r"), "message-text must contain no CR characters");
  assert.equal(written, "Hi Ryan, We noticed your property at 123 Main St. Interested in selling?");
});

test("Part 1 — message-text written as single-line when rendered SMS has CRLF newlines", async () => {
  const multiline = "Hello\r\nLine two\r\nLine three";
  const { captured_fields } = await buildAndCapture({ rendered_message_text: multiline });

  assert.equal(captured_fields?.["message-text"], "Hello Line two Line three");
});

test("Part 1 — character-count reflects the normalized single-line length", async () => {
  const multiline = "Hello\nWorld";
  const expected_text = "Hello World";
  const { captured_fields } = await buildAndCapture({ rendered_message_text: multiline });

  assert.equal(captured_fields?.["character-count"], expected_text.length);
});

test("Part 1 — character-count does NOT count newline characters from the original multiline text", async () => {
  // 3-line message: 5 + 1(LF) + 5 + 1(LF) + 5 = 17 chars raw; normalized = 15 chars + 2 spaces = 17 chars
  // but without newlines the space-separated version is shorter than the raw with newlines
  const line1 = "AAAAA";
  const line2 = "BBBBB";
  const line3 = "CCCCC";
  const multiline = `${line1}\n${line2}\n${line3}`;
  const normalized = `${line1} ${line2} ${line3}`;
  const { captured_fields } = await buildAndCapture({ rendered_message_text: multiline });

  assert.equal(captured_fields?.["character-count"], normalized.length, "character-count must equal normalized length");
  assert.equal(captured_fields?.["message-text"], normalized);
});

test("Part 1 — single-line message-text is written unchanged (no regression)", async () => {
  const single = "Hi there, interested in selling your property?";
  const { captured_fields } = await buildAndCapture({ rendered_message_text: single });

  assert.equal(captured_fields?.["message-text"], single);
  assert.equal(captured_fields?.["character-count"], single.length);
});

test("Part 1 — explicit personalization tags are persisted exactly when provided", async () => {
  const { captured_fields } = await buildAndCapture({
    rendered_message_text: "Hi there",
    personalization_tags_used: ["{{seller_first_name}}", "{{property_city}}"],
  });

  assert.deepEqual(captured_fields?.["personalization-tags-used"], [
    "{{seller_first_name}}",
    "{{property_city}}",
  ]);
});

// ── Part 2: property-address ──────────────────────────────────────────────────

test("Part 2 — property-address written when real property_id and address available", async () => {
  const real_property = createPodioItem(5001, {
    "property-address": textField("123 Main St"),
  });
  let captured_fields = null;

  await buildSendQueueItem({
    context: makeBaseContext({ property_item: real_property }),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9001 };
    },
    update_item: async () => {},
  });

  assert.equal(captured_fields?.["property-address"], "123 Main St");
});

test("Part 2 — property-address omitted when no real property_id (context.ids.property_id is null)", async () => {
  let captured_fields = null;

  await buildSendQueueItem({
    context: makeBaseContext({ property_item: null }),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9002 };
    },
    update_item: async () => {},
  });

  assert.equal(
    "property-address" in (captured_fields || {}),
    false,
    "property-address must be absent when no real property item"
  );
});

test("Part 2 — result.property_address_written is true when address was included", async () => {
  const real_property = createPodioItem(5002, {
    "property-address": textField("456 Oak Ave"),
  });

  const result = await buildSendQueueItem({
    context: makeBaseContext({ property_item: real_property }),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async () => ({ item_id: 9003 }),
    update_item: async () => {},
  });

  assert.equal(result.property_address_written, true);
});

test("Part 2 — result.property_address_written is false when no real property", async () => {
  const result = await buildSendQueueItem({
    context: makeBaseContext({ property_item: null }),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async () => ({ item_id: 9004 }),
    update_item: async () => {},
  });

  assert.equal(result.property_address_written, false);
});

// ── Part 3: resolveQueueCategoryField unit tests ───────────────────────────────

test("resolveQueueCategoryField: empty value returns omitted=true with reason 'empty'", () => {
  for (const value of ["", null, undefined]) {
    const result = resolveQueueCategoryField("property-type", value);
    assert.equal(result.omitted, true, `should be omitted for: ${JSON.stringify(value)}`);
    assert.equal(result.reason, "empty");
    assert.equal(result.field_value, undefined);
    assert.equal(result.category_option_id, null);
  }
});

test("resolveQueueCategoryField: value not in real options returns no_matching_category_option_in_schema", () => {
  // The supplement now has real Send Queue property-type options (Single Family, Multi-Family, etc.).
  // "Residential" is the property-class value — it doesn't match any Send Queue property-type option.
  const result = resolveQueueCategoryField("property-type", "Residential");
  assert.equal(result.omitted, true);
  assert.equal(result.reason, "no_matching_category_option_in_schema");
  assert.equal(result.field_value, undefined);
});

test("resolveQueueCategoryField: category field (removed from Send Queue) returns stale_empty_schema_options", () => {
  // Send Queue no longer has a "category" field — was removed from supplement
  // because it never existed in real Podio.
  const result = resolveQueueCategoryField("category", "First Touch");
  assert.equal(result.omitted, true);
  assert.equal(result.reason, "stale_empty_schema_options");
});

test("resolveQueueCategoryField: use-case-template with unrecognised slug returns no_matching_category_option_in_schema", () => {
  // "first_touch_sfr" is not in the supplement's use-case-template options list.
  const result = resolveQueueCategoryField("use-case-template", "first_touch_sfr");
  assert.equal(result.omitted, true);
  assert.equal(result.reason, "no_matching_category_option_in_schema");
});

// ── Part 3: new category fields omitted safely in payload ─────────────────────

test("Part 3 — property-type absent from payload when schema has no matching option", async () => {
  let captured_fields = null;

  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    property_type: "Residential",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9005 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok, "queue creation must succeed");
  assert.equal(result.property_type_written, false);
  assert.equal(
    "property-type" in (captured_fields || {}),
    false,
    "property-type must be absent when schema has no option for the value"
  );
});

test("Part 3 — category absent from payload when schema has no matching option", async () => {
  let captured_fields = null;

  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    secondary_category: "Follow-Up",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9006 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal("category" in (captured_fields || {}), false);
});

test("Part 3 — use-case-template absent from payload when schema has no matching option", async () => {
  let captured_fields = null;

  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    use_case_template: "first_touch_sfr",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9007 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.use_case_template_written, false);
  assert.equal("use-case-template" in (captured_fields || {}), false);
});

test("Part 3 — queue creation succeeds even when all three new category fields are omitted", async () => {
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hello, interested in selling?",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    property_type: "Residential",
    secondary_category: "First Touch",
    use_case_template: "sfr_cold_outbound_v1",
    create_item: async () => ({ item_id: 9008 }),
    update_item: async () => {},
  });

  assert.ok(result.ok, "queue creation must succeed when all new fields are omitted");
  assert.equal(result.property_type_written, false);
  assert.equal(result.use_case_template_written, false);
});

test("Part 3 — null values for new params do not cause payload failures", async () => {
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    property_type: null,
    secondary_category: null,
    use_case_template: null,
    create_item: async () => ({ item_id: 9009 }),
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.property_type_written, false);
  assert.equal(result.use_case_template_written, false);
});

// ── Core fields unaffected by new params ──────────────────────────────────────

// ── Part 5: _matchCategoryOption — category matching logic unit tests ─────────
// These tests exercise the matching logic directly with in-memory options, so
// they are independent of the schema supplement state.  They confirm behaviour
// once real options are populated in the supplement.

const FAKE_SEND_QUEUE_OPTIONS = [
  { id: 1, text: "Residential" },
  { id: 2, text: "Probate / Trust" },
  { id: 3, text: "Landlord / Multifamily" },
  { id: 4, text: "Corporate / Institutional" },
];

test("_matchCategoryOption: exact label match returns correct option id", () => {
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Residential"), 1);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Probate / Trust"), 2);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Landlord / Multifamily"), 3);
});

test("_matchCategoryOption: case-insensitive normalized match", () => {
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "residential"), 1);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "RESIDENTIAL"), 1);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "probate / trust"), 2);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "PROBATE / TRUST"), 2);
});

test("_matchCategoryOption: punctuation-stripped normalized match", () => {
  // normalizeCategoryLabel strips non-alphanumeric chars — 'Probate / Trust' → 'probate trust'
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Probate Trust"), 2);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Landlord Multifamily"), 3);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Corporate Institutional"), 4);
});

test("_matchCategoryOption: returns null when value not in options (no_matching case)", () => {
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "Commercial"), null);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "ownership_check"), null);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "first_touch_sfr"), null);
});

test("_matchCategoryOption: returns null for empty options list (stale schema)", () => {
  assert.equal(_matchCategoryOption([], "Residential"), null);
  assert.equal(_matchCategoryOption([], "anything"), null);
});

test("_matchCategoryOption: returns null for empty / null / undefined value", () => {
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, ""), null);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, null), null);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, undefined), null);
});

test("_matchCategoryOption: numeric id match when value is an option id", () => {
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, 1), 1);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, "2"), 2);
  assert.equal(_matchCategoryOption(FAKE_SEND_QUEUE_OPTIONS, 99), null);
});

// ── Core fields unaffected by new params ──────────────────────────────────────

test("Core fields (phone-number, scheduled-for-local) remain present when new params are passed", async () => {
  let captured_fields = null;
  const real_property = createPodioItem(5003, {
    "property-address": textField("789 Elm St"),
  });

  await buildSendQueueItem({
    context: makeBaseContext({ property_item: real_property }),
    rendered_message_text: "Hello",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    property_type: "Residential",
    secondary_category: "First Touch",
    use_case_template: "sfr_first_touch",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9010 };
    },
    update_item: async () => {},
  });

  assert.ok(captured_fields?.["phone-number"], "phone-number must still be present");
  assert.ok(captured_fields?.["scheduled-for-local"], "scheduled-for-local must still be present");
  assert.ok(captured_fields?.["message-text"], "message-text must still be present");
  // property-address should be written since real property_id exists
  assert.equal(captured_fields?.["property-address"], "789 Elm St");
});

// ── Part 6: queue-id-2 and queue-sequence field mapping ───────────────────────

test("Part 6 — composite queue ID is written to queue-id-2 (text field)", async () => {
  const composite_id = "mo:201:401:1";
  const { captured_fields } = await buildAndCapture({ queue_id: composite_id });

  assert.equal(
    captured_fields?.["queue-id-2"],
    composite_id,
    "composite queue ID must be written to queue-id-2"
  );
});

test("Part 6 — composite queue ID is NOT written to the old numeric queue-id field", async () => {
  const composite_id = "mo:201:401:1";
  const { captured_fields } = await buildAndCapture({ queue_id: composite_id });

  assert.equal(
    "queue-id" in (captured_fields || {}),
    false,
    "queue-id (old numeric field) must not appear in the creation payload"
  );
});

test("Part 6 — queue-sequence receives a numeric value after item creation", async () => {
  let update_fields = null;
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hello",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, _fields) => ({ item_id: 9200 }),
    update_item: async (_item_id, fields) => {
      update_fields = fields;
    },
  });

  assert.ok(result.ok, "queue creation must succeed");
  assert.ok(update_fields, "update_item must have been called");
  assert.equal(typeof update_fields?.["queue-sequence"], "number", "queue-sequence must be a number");
  assert.equal(update_fields?.["queue-sequence"], 9200, "queue-sequence must equal the created item_id");
  assert.equal(result.queue_sequence, 9200, "result.queue_sequence must be returned");
});

test("Part 6 — no composite string is written into any numeric Podio field", async () => {
  const composite_id = "mo:201:401:3";
  const { captured_fields } = await buildAndCapture({ queue_id: composite_id });

  for (const [key, value] of Object.entries(captured_fields || {})) {
    if (typeof value === "string" && value.startsWith("mo:")) {
      // The only field that should receive the composite string is queue-id-2
      assert.equal(
        key,
        "queue-id-2",
        `composite string must only appear in queue-id-2, found in: ${key}`
      );
    }
  }
  // queue-id-2 must be text (confirmed by the schema type: "text")
  assert.equal(typeof captured_fields?.["queue-id-2"], "string");
});

test("Part 6 — queue-id-2 is absent from payload when no composite id is provided", async () => {
  const { captured_fields } = await buildAndCapture({ queue_id: null });

  assert.equal(
    "queue-id-2" in (captured_fields || {}),
    false,
    "queue-id-2 must not appear when no queue_id is provided"
  );
});

test("Part 6 — result.queue_id returns the composite string when provided", async () => {
  const composite_id = "mo:201:401:2";
  const { result } = await buildAndCapture({ queue_id: composite_id });

  assert.equal(result.queue_id, composite_id);
});

test("Part 6 — result.queue_id is null when no composite id provided", async () => {
  const { result } = await buildAndCapture({ queue_id: null });

  assert.equal(result.queue_id, null);
});

// ── Part 7: live field-quality fixes ─────────────────────────────────────────
// These tests verify the 5 critical field-quality issues reported in live queue rows.
//
// Issue 1: message-text stored only "Hi" — template text contained HTML markup
//          from Podio's rich-text editor.  normalizeForQueueText now strips HTML
//          before persisting so the full plain-text body is written.
//
// Issue 2: property-type blank — caller supplied property-class ("Residential")
//          instead of property-type ("Single Family").  buildSendQueueItem now
//          reads property-type directly from the property item.
//
// Issue 3: category blank — secondary_category was always null.  buildSendQueueItem
//          now reads owner-type from the master owner item and maps it.
//
// Issue 3b: owner-type blank — Send Queue did not persist the dedicated owner
//           type field. It now reads owner-type from the linked property item.
//
// Issue 4: contact-window blank — resolveContactWindowField omitted any value
//          without a schema option ID.  Values matching CONTACT_WINDOW_PATTERN are
//          now passed through to schema.js's compat bypass.
//
// Issue 5: property-address ugly format — handled by formatPropertyAddress()
//          reading structured sub-fields (street_address, city, state, postal_code).

// ── Part 7.1: HTML stripping in normalizeForQueueText ────────────────────────

test("Part 7 — normalizeForQueueText strips <p> HTML tags and preserves full body", () => {
  const html_message =
    "<p>Hi Ryan,</p><p>We have a cash offer ready for your property at 5139 W 11th St.</p><p>Interested in a quick close?</p>";
  const result = normalizeForQueueText(html_message);
  assert.equal(
    result,
    "Hi Ryan, We have a cash offer ready for your property at 5139 W 11th St. Interested in a quick close?"
  );
});

test("Part 7 — normalizeForQueueText converts <br> to space", () => {
  const html = "Line one<br>Line two<br/>Line three";
  const result = normalizeForQueueText(html);
  assert.equal(result, "Line one Line two Line three");
});

test("Part 7 — normalizeForQueueText decodes HTML entities", () => {
  const html = "Seller&#39;s name is Smith &amp; Sons";
  assert.equal(normalizeForQueueText(html), "Seller's name is Smith & Sons");
});

test("Part 7 — message-text stores full plain-text body when template contains HTML", async () => {
  const html_template =
    "<p>Hi there,</p><p>We are interested in purchasing your property at 5139 W 11th St, Tulsa OK.</p><p>Can we make you an offer?</p>";
  const expected =
    "Hi there, We are interested in purchasing your property at 5139 W 11th St, Tulsa OK. Can we make you an offer?";

  const { captured_fields } = await buildAndCapture({ rendered_message_text: html_template });

  assert.equal(captured_fields?.["message-text"], expected, "must store full body, not just 'Hi'");
  assert.equal(captured_fields?.["character-count"], expected.length);
});

test("Part 7 — character-count reflects stripped plain-text length (not HTML)", async () => {
  const html = "<p>Hi</p>";
  // HTML is 9 chars, plain text is "Hi" = 2 chars
  const { captured_fields } = await buildAndCapture({ rendered_message_text: html });
  assert.equal(captured_fields?.["message-text"], "Hi");
  assert.equal(captured_fields?.["character-count"], 2);
  assert.notEqual(captured_fields?.["character-count"], 9, "must not count HTML markup chars");
});

// ── Part 7.2: property-type reads directly from the property item ─────────────

test("Part 7 — property-type written as 'Single Family' when property item has that value", async () => {
  const property_item = createPodioItem(5100, {
    "property-address": textField("5139 W 11th St"),
    "property-type": categoryField("Single Family"),
  });
  const context = {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item: createPodioItem(201),
      property_item,
      agent_item: null,
      market_item: null,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: property_item.item_id,
      market_id: null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: { total_messages_sent: 0 },
  };

  let captured_fields = null;
  const result = await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    property_type: "Residential", // caller passes wrong source; should be overridden
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9200 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.property_type_written, true, "property-type must be written");
  assert.equal(
    captured_fields?.["property-type"],
    "Single Family",
    "property-type must come from property item, not caller-supplied property_type"
  );
});

test("Part 7 — property-type written as 'Multi-Family' from property item", async () => {
  const property_item = createPodioItem(5101, {
    "property-type": categoryField("Multi-Family"),
  });
  const context = {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item: createPodioItem(201),
      property_item,
      agent_item: null,
      market_item: null,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: property_item.item_id,
      market_id: null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: { total_messages_sent: 0 },
  };

  let captured_fields = null;
  await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    property_type: "Residential",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9201 };
    },
    update_item: async () => {},
  });

  assert.equal(captured_fields?.["property-type"], "Multi-Family");
});

test("Part 7 — owner-type written from linked property owner-type-2", async () => {
  const property_item = createPodioItem(5102, {
    "owner-type-2": categoryField("Corporate"),
  });
  const master_owner_item = createPodioItem(201, {
    "owner-type": categoryField("INDIVIDUAL | ABSENTEE"),
  });
  const context = {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item,
      property_item,
      agent_item: null,
      market_item: null,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: property_item.item_id,
      market_id: null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: { total_messages_sent: 0 },
  };

  let captured_fields = null;
  const result = await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 92015 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.owner_type_written, true, "owner-type must be written");
  assert.equal(
    captured_fields?.["owner-type"],
    "Corporate",
    "owner-type must come from linked property owner-type-2"
  );
});

// ── Part 7.3: category field removed — Send Queue has no "category" field ────

test("Part 7 — category field is never written (Send Queue has no category field)", async () => {
  const master_owner_item = createPodioItem(201, {
    "owner-type": categoryField("LLC/CORP | ABSENTEE"),
  });
  const context = {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item,
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
    summary: { total_messages_sent: 0 },
  };

  let captured_fields = null;
  await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9202 };
    },
    update_item: async () => {},
  });

  assert.equal("category" in (captured_fields || {}), false, "category field must not be written — Send Queue has no such field");
});

// ── Part 7.4: contact-window omit path when schema has no matching option ─────

test("Part 7 — contact-window '8AM-9PM Local' is written via compat layer", async () => {
  let captured_fields = null;
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "8AM-9PM Local",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9210 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.contact_window_written, true, "contact-window must be written via compat layer");
  assert.equal(captured_fields?.["contact-window"], "8AM-9PM Local");
});

test("Part 7 — contact-window '9AM-8PM CT' is written (matches pattern and schema option)", async () => {
  let captured_fields = null;
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "9AM-8PM CT",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9211 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.contact_window_written, true);
  assert.equal(captured_fields?.["contact-window"], "9AM-8PM CT");
});

test("Part 7 — contact-window '7AM-8PM ET' is written via compat layer", async () => {
  let captured_fields = null;
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "7AM-8PM ET",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9212 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.contact_window_written, true, "7AM-8PM ET must be written via compat layer");
  assert.equal(captured_fields?.["contact-window"], "7AM-8PM ET");
});

test("Part 7 — contact-window with invalid format is omitted", async () => {
  let captured_fields = null;
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "whenever",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9213 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(result.contact_window_written, false, "invalid format must be omitted");
  assert.equal("contact-window" in (captured_fields || {}), false);
});

// ── Part 7.5: property-address structured location format ─────────────────────

test("Part 7 — property-address formatted as 'Street, City, State ZIP' from structured location", async () => {
  // Simulate a Podio location field value with structured sub-fields.
  // Podio exposes street_address, city, state, postal_code alongside .value.
  const property_item = createPodioItem(5200, {
    "property-address": {
      street_address: "5139 W 11th St",
      city: "Tulsa",
      state: "OK",
      postal_code: "74127",
      value: "Tulsa 74127 OK 5139 W 11th St", // ugly geocoded string
    },
  });
  const context = {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item: createPodioItem(201),
      property_item,
      agent_item: null,
      market_item: null,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: property_item.item_id,
      market_id: null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: { total_messages_sent: 0 },
  };

  let captured_fields = null;
  await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9220 };
    },
    update_item: async () => {},
  });

  assert.equal(
    captured_fields?.["property-address"],
    "5139 W 11th St, Tulsa OK 74127",
    "must use structured sub-fields, not the ugly geocoded string"
  );
  assert.notEqual(
    captured_fields?.["property-address"],
    "Tulsa 74127 OK 5139 W 11th St",
    "must NOT use the ugly geocoded string"
  );
});
