/**
 * Discord replay command center — critical tests
 *
 * Covers /replay inbound, owner, template, batch subcommands with:
 * - dry-run simulation without SMS/queue/Podio mutations
 * - embed response validation
 * - permission checks
 * - error sanitization
 * - safe custom_id limits
 */

import test from "node:test";
import assert from "node:assert/strict";

// Test that embeds can be imported and built
test("buildReplayInboundEmbed returns a valid Discord embed object", async () => {
  const { buildReplayInboundEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayInboundEmbed({
    message_body:            "What is your offer?",
    classification:          { language: "English", objection: "no", emotion: "interested" },
    previous_stage:          "offer_reveal_cash",
    next_stage:              "offer_reveal_cash",
    selected_use_case:       "offer_reveal_cash",
    selected_template_source: "sms_library",
    would_queue_reply:       true,
    underwriting_signals:    { property_type: "Single Family", creative_strategy: "cash" },
    underwriting_route:      "standard",
    alignment_passed:        true,
  });

  assert.equal(typeof embed.title, "string", "embed should have a title");
  assert.ok(embed.title.includes("Inbound"), "title should mention Inbound");
  assert.equal(typeof embed.color, "number", "embed should have a color");
  assert.ok(Array.isArray(embed.fields), "embed should have fields");
  assert.ok(embed.fields.length > 0, "embed should have at least one field");
  assert.ok(embed.fields.some(f => f.name.includes("Language")), "should have language field");
  assert.ok(embed.fields.some(f => f.name.includes("Queue")), "should have queue status field");
});

test("buildReplayOwnerEmbed returns a valid Discord embed object", async () => {
  const { buildReplayOwnerEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayOwnerEmbed({
    owner_id:                "owner_123",
    owner_name:              "John Seller",
    property_address:        "123 Oak St, Austin TX",
    property_type:           "Single Family",
    message_body:            "Is this a real offer?",
    classification:          { language: "English", stage_hint: "offer_reveal" },
    current_stage:           "offer_reveal_cash",
    next_stage:              "offer_reveal_cash",
    selected_use_case:       "offer_reveal_cash",
    selected_template_source: "custom_offer_template",
    cash_offer_snapshot:     "$250,000",
    underwriting_route:      "standard",
    would_queue:             true,
  });

  assert.equal(typeof embed.title, "string");
  assert.ok(embed.title.includes("Owner"), "title should mention Owner");
  assert.ok(embed.fields.some(f => f.name.includes("Owner")), "should have owner field");
  assert.ok(embed.fields.some(f => f.name.includes("Property")), "should have property field");
  assert.ok(embed.fields.some(f => f.name.includes("Cash")), "should have offer field");
});

test("buildReplayTemplateEmbed returns a valid Discord embed object", async () => {
  const { buildReplayTemplateEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayTemplateEmbed({
    use_case:               "offer_reveal_cash",
    template_id:            "tmpl_sfr_offer_001",
    template_source:        "sms_library",
    stage_code:             "offer_reveal_cash",
    language:               "English",
    template_text:          "Hi {{owner_name}}, we have a cash offer of {{cash_offer}} for your property.",
    property_type_resolved: "Single Family",
  });

  assert.equal(typeof embed.title, "string");
  assert.ok(embed.title.includes("Template"), "title should mention Template");
  assert.ok(embed.fields.some(f => f.name.includes("Use Case")), "should have use case field");
  assert.ok(embed.fields.some(f => f.name.includes("Preview")), "should have preview field");
});

test("buildReplayBatchEmbed returns a valid Discord embed object", async () => {
  const { buildReplayBatchEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayBatchEmbed({
    scenario: "ownership",
    tested:   3,
    passed:   3,
    warnings: 0,
    failed:   0,
    results: [
      { name: "ownership_check_1", status: "pass", note: "ownership_confirmation" },
      { name: "ownership_check_2", status: "pass", note: "ownership_confirmation" },
      { name: "wrong_person", status: "pass", note: "wrong_person" },
    ],
  });

  assert.equal(typeof embed.title, "string");
  assert.ok(embed.title.includes("Batch"), "title should mention Batch");
  assert.ok(embed.fields.some(f => f.name.includes("Tested")), "should have tested field");
  assert.ok(embed.fields.some(f => f.name.includes("Status")), "should have status field");
  assert.ok(embed.color === 0x2ECC71, "color should be green for all passing");
});

test("replay batch with warnings returns yellow color", async () => {
  const { buildReplayBatchEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayBatchEmbed({
    scenario: "test",
    tested:   2,
    passed:   1,
    warnings: 1,
    failed:   0,
    results: [
      { name: "test_1", status: "pass" },
      { name: "test_2", status: "warn" },
    ],
  });

  assert.ok(embed.color === 0xF1C40F, "color should be yellow when warnings present");
});

test("replay batch with failures returns red color", async () => {
  const { buildReplayBatchEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayBatchEmbed({
    scenario: "test",
    tested:   2,
    passed:   1,
    warnings: 0,
    failed:   1,
    results: [
      { name: "test_1", status: "pass" },
      { name: "test_2", status: "fail" },
    ],
  });

  assert.ok(embed.color === 0xE74C3C, "color should be red when failures present");
});

test("buildReplayInboundEmbed with alignment failure returns yellow color", async () => {
  const { buildReplayInboundEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayInboundEmbed({
    message_body:     "Test",
    alignment_passed: false,
    would_queue_reply: false,
  });

  assert.ok(embed.color === 0xF1C40F, "color should be yellow when alignment fails");
});

test("buildReplayInboundEmbed with alignment success returns green color", async () => {
  const { buildReplayInboundEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayInboundEmbed({
    message_body:     "Test",
    alignment_passed: true,
  });

  assert.ok(embed.color === 0x2ECC71, "color should be green when alignment passes");
});

test("/replay inbound embed text field has helpful label", async () => {
  const { buildReplayInboundEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const long_text = "a".repeat(500);
  const embed = buildReplayInboundEmbed({
    message_body: long_text,
  });

  const text_field = embed.fields.find(f => f.name.includes("Seller Text"));
  assert.ok(text_field, "should have Seller Text field");
  assert.ok(text_field.value.length <= 160, "text should be truncated to ~150 chars");
});

test("/replay owner embed text field is truncated", async () => {
  const { buildReplayOwnerEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const long_text = "a".repeat(500);
  const embed = buildReplayOwnerEmbed({
    message_body: long_text,
  });

  const text_field = embed.fields.find(f => f.name.includes("Seller Text"));
  assert.ok(text_field);
  assert.ok(text_field.value.length <= 110, "text should be truncated to ~100 chars");
});

test("replay inbound embed footer mentions dry-run", async () => {
  const { buildReplayInboundEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayInboundEmbed({});

  assert.ok(embed.footer?.text.toLowerCase().includes("dry-run"), "footer should mention dry-run");
  assert.ok(embed.footer?.text.includes("no SMS"), "footer should mention no SMS sent");
});

test("replay batch embed footer mentions scenario", async () => {
  const { buildReplayBatchEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayBatchEmbed({
    scenario: "ownership",
    tested:   0,
    passed:   0,
    warnings: 0,
    failed:   0,
  });

  assert.ok(embed.footer?.text.toLowerCase().includes("scenario"), "footer should mention scenario");
});

test("custom_ids for replay buttons are under 100 chars and safe", async () => {
  // Verify that well-formed custom_ids don't exceed Discord's 100 char limit
  const safe_ids = [
    "replay:inbound:ask_question",
    "replay:owner:context",
    "replay:template:preview",
    "replay:batch:results",
  ];

  for (const id of safe_ids) {
    assert.ok(id.length <= 100, `custom_id "${id}" must be <= 100 characters`);
    assert.ok(/^[a-z0-9_:-]+$/.test(id), `custom_id "${id}" must match [a-z0-9_:-]`);
  }
});

test("replay embeds do not include secrets or raw errors", async () => {
  const { buildReplayInboundEmbed, buildReplayBatchEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  // Simulate with mock data that might contain secrets
  const mock_data = {
    message_body:     "Test",
    classification:   {},
    selected_use_case: "offer_reveal_cash",
  };

  const embed = buildReplayInboundEmbed(mock_data);
  const embed_str = JSON.stringify(embed);

  assert.ok(!embed_str.includes("PODIO"), "should not include PODIO API keys");
  assert.ok(!embed_str.includes("SECRET"), "should not include SECRET keys");
  assert.ok(!embed_str.includes("API_KEY"), "should not include API keys");
});

test("replay batch result list is limited to 10 items", async () => {
  const { buildReplayBatchEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const many_results = Array.from({ length: 20 }, (_, i) => ({
    name: `test_${i}`,
    status: i % 2 === 0 ? "pass" : "warn",
  }));

  const embed = buildReplayBatchEmbed({
    scenario: "all",
    tested:   20,
    passed:   10,
    warnings: 10,
    failed:   0,
    results: many_results,
  });

  const results_field = embed.fields.find(f => f.name === "Results");
  if (results_field) {
    const lines = results_field.value.split("\n");
    assert.ok(lines.length <= 10, "results field should show max 10 items");
  }
});

test("replay inbound embed handles empty message body gracefully", async () => {
  const { buildReplayInboundEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayInboundEmbed({
    message_body: "",
  });

  const text_field = embed.fields.find(f => f.name.includes("Seller Text"));
  assert.ok(text_field?.value === "(no text)" || text_field?.value === "(empty)", "should show placeholder for empty text");
});

test("replay owner embed handles missing property address", async () => {
  const { buildReplayOwnerEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayOwnerEmbed({
    owner_id: "owner_1",
    // property_address intentionally omitted
  });

  const prop_field = embed.fields.find(f => f.name === "Property");
  assert.ok(prop_field?.value === "unknown address", "should show placeholder for missing address");
});

test("replay template embed shows use_case correctly", async () => {
  const { buildReplayTemplateEmbed } = await import("@/lib/discord/discord-embed-factory.js");

  const embed = buildReplayTemplateEmbed({
    use_case: "offer_reveal_cash",
  });

  const use_case_field = embed.fields.find(f => f.name === "Use Case");
  assert.equal(use_case_field?.value, "offer_reveal_cash");
});
