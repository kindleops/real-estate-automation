// ─── sms-engine-pipeline.test.mjs ─────────────────────────────────────────
// Comprehensive tests for the SMS engine pipeline:
//   1. CSV loader (template_catalog)
//   2. Language aliasing (language_aliases)
//   3. Template resolution (template_resolver)
//   4. Personalization (personalize_template)
//   5. Flow mapping (flow_map)
//   6. Latency / scheduling (latency)
//   7. Queue truthfulness (queue_message)

import test from "node:test";
import assert from "node:assert/strict";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEST_CSV = resolve(__dirname, "../helpers/test-templates.csv");

// ══════════════════════════════════════════════════════════════════════════
// 1. CSV LOADER
// ══════════════════════════════════════════════════════════════════════════

import {
  loadCatalog,
  reloadCatalog,
  __resetCatalog,
  catalogSize,
  allRows,
  parseCSV,
  REQUIRED_COLUMNS,
} from "@/lib/sms/template_catalog.js";

test("csv loader: ignores junk columns and typo columns", (t) => {
  __resetCatalog();
  const catalog = loadCatalog(TEST_CSV);
  const first = catalog.rows[0];
  // Junk columns "-" and "-0.1" should not appear
  assert.equal(first["-"], undefined);
  assert.equal(first["-0.1"], undefined);
  // The typo "Agent Style FIt" should be ignored
  assert.equal(first["Agent Style FIt"], undefined);
  t.after(() => __resetCatalog());
});

test("csv loader: loads correct row count (active + inactive)", (t) => {
  __resetCatalog();
  const catalog = loadCatalog(TEST_CSV);
  // Test CSV has 20 data rows
  assert.equal(catalog.rows.length, 20);
  t.after(() => __resetCatalog());
});

test("csv loader: validates required columns", () => {
  assert.throws(
    () => parseCSV("Foo,Bar\n1,2"),
    /missing required columns/i
  );
});

test("csv loader: correctly parses booleans", (t) => {
  __resetCatalog();
  const catalog = loadCatalog(TEST_CSV);
  const t001 = catalog.rows.find((r) => r.template_id === "T001");
  assert.equal(t001.is_first_touch, true, "T001 should be first touch");
  assert.equal(t001.is_follow_up, false, "T001 should not be follow-up");

  const t005 = catalog.rows.find((r) => r.template_id === "T005");
  assert.equal(t005.is_first_touch, false, "T005 should not be first touch");
  assert.equal(t005.is_follow_up, true, "T005 should be follow-up");
  t.after(() => __resetCatalog());
});

test("csv loader: rejects blank template text rows", () => {
  const csv = `Template ID,Active?,Use Case,Language,Template Text
T1,Yes,test,English,Hello
T2,Yes,test,English,`;
  const rows = parseCSV(csv);
  assert.equal(rows.length, 1, "blank template text should be filtered");
});

test("csv loader: index by_use_case returns only active by default", (t) => {
  __resetCatalog();
  const catalog = loadCatalog(TEST_CSV);
  const key = "ownership_check";
  const all = catalog.indexes.by_use_case.get(key) || [];
  const active_only = all.filter((r) => r.active);
  // T010 is inactive, should not be in active-only set
  assert.ok(active_only.every((r) => r.active), "all results should be active");
  assert.ok(!active_only.find((r) => r.template_id === "T010"), "inactive T010 should not appear");
  // But the full set should include T010
  assert.ok(all.find((r) => r.template_id === "T010"), "full index should include T010");
  t.after(() => __resetCatalog());
});

test("csv loader: index by_use_case_language filters correctly", (t) => {
  __resetCatalog();
  const catalog = loadCatalog(TEST_CSV);
  const key = "ownership_check|spanish";
  const results = catalog.indexes.by_use_case_language.get(key) || [];
  assert.equal(results.length, 1);
  assert.equal(results[0].template_id, "T002");
  t.after(() => __resetCatalog());
});

test("csv loader: index by_template_id returns exact row", (t) => {
  __resetCatalog();
  const catalog = loadCatalog(TEST_CSV);
  const row = catalog.indexes.by_template_id.get("T014");
  assert.ok(row);
  assert.equal(row.use_case, "offer_reveal_cash");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 2. LANGUAGE ALIASING
// ══════════════════════════════════════════════════════════════════════════

import {
  normalizeLanguage,
  isUnsupportedTemplateLanguage,
  resolveLanguage,
} from "@/lib/sms/language_aliases.js";

test("language: Hindi normalizes to Asian Indian (Hindi or Other)", () => {
  assert.equal(normalizeLanguage("Hindi"), "Asian Indian (Hindi or Other)");
});

test("language: Indian (Hindi or Other) normalizes correctly", () => {
  assert.equal(normalizeLanguage("Indian (Hindi or Other)"), "Asian Indian (Hindi or Other)");
});

test("language: classifier + brain + template values normalize to same key", () => {
  const from_classifier = normalizeLanguage("Hindi");
  const from_brain = normalizeLanguage("Asian Indian (Hindi or Other)");
  const from_template = normalizeLanguage("Asian Indian (Hindi or Other)");
  assert.equal(from_classifier, from_brain);
  assert.equal(from_brain, from_template);
  assert.equal(from_classifier, "Asian Indian (Hindi or Other)");
});

test("language: direct languages pass through", () => {
  assert.equal(normalizeLanguage("English"), "English");
  assert.equal(normalizeLanguage("Spanish"), "Spanish");
  assert.equal(normalizeLanguage("Mandarin"), "Mandarin");
  assert.equal(normalizeLanguage("Vietnamese"), "Vietnamese");
});

test("language: ISO codes normalize", () => {
  assert.equal(normalizeLanguage("es"), "Spanish");
  assert.equal(normalizeLanguage("zh"), "Mandarin");
  assert.equal(normalizeLanguage("hi"), "Asian Indian (Hindi or Other)");
});

test("language: unsupported template languages detected", () => {
  assert.equal(isUnsupportedTemplateLanguage("Thai"), true);
  assert.equal(isUnsupportedTemplateLanguage("Farsi"), true);
  assert.equal(isUnsupportedTemplateLanguage("Pashto"), true);
  assert.equal(isUnsupportedTemplateLanguage("English"), false);
});

test("language: resolveLanguage returns unsupported flag", () => {
  const result = resolveLanguage("Thai");
  assert.equal(result.unsupported, true);
  assert.equal(result.canonical, "Thai");

  const english = resolveLanguage("English");
  assert.equal(english.unsupported, false);
  assert.equal(english.canonical, "English");
});

// ══════════════════════════════════════════════════════════════════════════
// 3. TEMPLATE RESOLUTION
// ══════════════════════════════════════════════════════════════════════════

import {
  resolveTemplate,
  scoreTemplate,
  stableHash,
  deterministicPick,
} from "@/lib/sms/template_resolver.js";

test("resolver: exact match returns correct row", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);
  const result = resolveTemplate({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Warm Professional",
    property_type_scope: "Residential",
    deal_strategy: "Cash",
    is_first_touch: true,
    is_follow_up: false,
    csv_path: TEST_CSV,
  });
  assert.ok(result.resolved, "should resolve");
  assert.equal(result.template_id, "T001");
  assert.equal(result.source, "csv_catalog");
  assert.ok(result.resolution_path.includes("exact_match"));
  t.after(() => __resetCatalog());
});

test("resolver: no first-touch / follow-up cross contamination", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);

  // Query for follow-up → should NOT return first-touch T001
  const result = resolveTemplate({
    use_case: "ownership_check",
    language: "English",
    is_first_touch: false,
    is_follow_up: true,
    csv_path: TEST_CSV,
  });
  // Should not return T001/T003 (first touch only)
  if (result.resolved) {
    assert.notEqual(result.template_id, "T001");
    assert.notEqual(result.template_id, "T003");
  }
  t.after(() => __resetCatalog());
});

test("resolver: probate scope routes correctly", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);
  const result = resolveTemplate({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Warm Professional",
    property_type_scope: "Probate / Trust",
    is_first_touch: true,
    is_follow_up: false,
    csv_path: TEST_CSV,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T011");
  t.after(() => __resetCatalog());
});

test("resolver: corporate scope routes correctly", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);
  const result = resolveTemplate({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Warm Professional",
    property_type_scope: "Corporate / Institutional",
    is_first_touch: true,
    is_follow_up: false,
    csv_path: TEST_CSV,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T012");
  t.after(() => __resetCatalog());
});

test("resolver: unsupported language falls back to English truthfully", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);
  const result = resolveTemplate({
    use_case: "ownership_check",
    language: "Thai",
    is_first_touch: true,
    is_follow_up: false,
    csv_path: TEST_CSV,
  });
  // Should find an English template and signal unsupported language
  assert.ok(result.resolved);
  assert.equal(result.language, "English");
  assert.ok(result.fallback_reason?.includes("unsupported"), "should signal unsupported language in fallback_reason");
  t.after(() => __resetCatalog());
});

test("resolver: agent style fallback from Investor Direct to Warm Professional", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);
  // Ask for consider_selling with Investor Direct — only Warm Professional exists
  // The scoring engine finds T004 on primary pass (use_case match alone is sufficient)
  // but the style bonus is not awarded
  const result = resolveTemplate({
    use_case: "consider_selling",
    language: "English",
    agent_style_fit: "Investor Direct",
    is_first_touch: false,
    is_follow_up: false,
    csv_path: TEST_CSV,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T004");
  // The style doesn't match, but the template was found (best available)
  assert.equal(result.agent_style_fit, "Warm Professional");
  t.after(() => __resetCatalog());
});

test("resolver: deterministic pick is stable with same seed", () => {
  const candidates = [{ id: "A" }, { id: "B" }, { id: "C" }];
  const seed = ["owner_123", "+15551234567", "ownership_check", "S1", "English", "Warm Professional"];
  const pick1 = deterministicPick(candidates, seed);
  const pick2 = deterministicPick(candidates, seed);
  assert.deepEqual(pick1, pick2, "same seed should return same pick");
});

test("resolver: stableHash is deterministic", () => {
  const h1 = stableHash(["a", "b", "c"]);
  const h2 = stableHash(["a", "b", "c"]);
  assert.equal(h1, h2);
});

test("resolver: no_use_case returns unresolved", (t) => {
  __resetCatalog();
  loadCatalog(TEST_CSV);
  const result = resolveTemplate({ csv_path: TEST_CSV });
  assert.equal(result.resolved, false);
  assert.equal(result.fallback_reason, "no_use_case");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 4. PERSONALIZATION
// ══════════════════════════════════════════════════════════════════════════

import {
  personalizeTemplate,
  detectPlaceholders,
  countSegments,
  formatCurrency,
  formatDate,
} from "@/lib/sms/personalize_template.js";

test("personalization: placeholders fill correctly", () => {
  const result = personalizeTemplate(
    "Hi {{seller_first_name}} this is {{agent_name}} about {{property_address}}.",
    {
      seller_first_name: "John",
      agent_name: "Sarah",
      property_address: "123 Main St",
    }
  );
  assert.ok(result.ok);
  assert.equal(result.text, "Hi John this is Sarah about 123 Main St.");
  assert.deepEqual(result.placeholders_used.sort(), ["agent_name", "property_address", "seller_first_name"]);
});

test("personalization: unresolved placeholders block send", () => {
  const result = personalizeTemplate(
    "Hi {{seller_first_name}} about {{property_address}} for {{offer_price}}.",
    {
      seller_first_name: "John",
      property_address: "123 Main St",
      // offer_price is missing
    }
  );
  assert.equal(result.ok, false);
  assert.ok(result.missing.includes("offer_price"));
  assert.ok(result.reason.includes("offer_price"));
});

test("personalization: currency formatting is clean", () => {
  assert.equal(formatCurrency(150000), "$150,000");
  assert.equal(formatCurrency(99999.50), "$99,999.50");
  assert.equal(formatCurrency("250000"), "$250,000");
});

test("personalization: date formatting is clean", () => {
  // Use explicit UTC time to avoid timezone-shifting the date
  const result = formatDate(new Date(2026, 4, 15)); // May 15 2026 in local time
  assert.match(result, /^5\/15\/2026$/);
});

test("personalization: no placeholders returns text as-is", () => {
  const result = personalizeTemplate("Hello world no placeholders here.");
  assert.ok(result.ok);
  assert.equal(result.text, "Hello world no placeholders here.");
  assert.deepEqual(result.placeholders_used, []);
});

test("personalization: empty template fails cleanly", () => {
  const result = personalizeTemplate("");
  assert.equal(result.ok, false);
  assert.equal(result.reason, "empty_template");
});

test("personalization: smart punctuation normalized for SMS", () => {
  const result = personalizeTemplate("\u201CHello\u201D \u2014 it\u2019s me");
  assert.ok(result.ok);
  assert.equal(result.text, '"Hello" - it\'s me');
});

test("personalization: detectPlaceholders finds all unique placeholders", () => {
  const placeholders = detectPlaceholders("{{seller_first_name}} and {{agent_name}} and {{seller_first_name}}");
  assert.deepEqual(placeholders, ["seller_first_name", "agent_name"]);
});

test("personalization: countSegments returns correct count", () => {
  assert.equal(countSegments("Hello"), 1);
  assert.equal(countSegments("A".repeat(160)), 1);
  assert.equal(countSegments("A".repeat(161)), 2);
  assert.equal(countSegments("A".repeat(306)), 2);
  assert.equal(countSegments("A".repeat(307)), 3);
});

// ══════════════════════════════════════════════════════════════════════════
// 5. FLOW MAPPING
// ══════════════════════════════════════════════════════════════════════════

import {
  mapNextAction,
  ACTIONS,
  resolveCompliance,
  resolveDelayProfile,
  OBJECTION_ROUTES,
} from "@/lib/sms/flow_map.js";

test("flow: compliance stop blocks all outbound", () => {
  const result = mapNextAction({
    classify_result: { compliance_flag: "stop_texting" },
    brain_state: {},
  });
  assert.equal(result.action, ACTIONS.STOP);
  assert.equal(result.cancel_queued, true);
});

test("flow: wrong_number routes to wrong_person by default", () => {
  const result = mapNextAction({
    classify_result: { objection: "wrong_number" },
    brain_state: {},
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
  assert.equal(result.use_case, "wrong_person");
});

test("flow: wrong_number routes to wrong_number_knows_owner when message references owner", () => {
  const result = mapNextAction({
    classify_result: { objection: "wrong_number", notes: "I think the owner lives next door" },
    brain_state: {},
  });
  assert.equal(result.use_case, "wrong_number_knows_owner");
});

test("flow: who_is_this routes correctly", () => {
  const result = mapNextAction({
    classify_result: { objection: "who_is_this" },
    brain_state: {},
  });
  assert.equal(result.use_case, "who_is_this");
});

test("flow: send_offer_first routes to offer if underwriting ready", () => {
  const result = mapNextAction({
    classify_result: { objection: "send_offer_first" },
    brain_state: {},
    property_context: { underwriting_ready: true },
  });
  assert.equal(result.use_case, "offer_reveal_cash");
});

test("flow: send_offer_first routes to condition request if underwriting not ready", () => {
  const result = mapNextAction({
    classify_result: { objection: "send_offer_first" },
    brain_state: {},
    property_context: { underwriting_ready: false, needs_condition_info: true },
  });
  assert.equal(result.use_case, "condition_question_set");
});

test("flow: need_time routes to not_ready by default", () => {
  const result = mapNextAction({
    classify_result: { objection: "need_time" },
    brain_state: {},
  });
  assert.equal(result.use_case, "not_ready");
});

test("flow: need_time routes to text_me_later_specific when seller asks for later", () => {
  const result = mapNextAction({
    classify_result: {
      objection: "need_time",
      positive_signals: ["text_me_later"],
    },
    brain_state: {},
  });
  assert.equal(result.use_case, "text_me_later_specific");
});

test("flow: wants_proof_of_funds routes to proof_of_funds", () => {
  const result = mapNextAction({
    classify_result: { objection: "wants_proof_of_funds" },
    brain_state: {},
  });
  assert.equal(result.use_case, "proof_of_funds");
});

test("flow: stage progression from ownership to consider_selling", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: ["confirms_ownership"] },
    brain_state: { conversation_stage: "Ownership" },
  });
  assert.equal(result.use_case, "consider_selling");
});

test("flow: delay profile resolves hot for motivated emotion", () => {
  assert.equal(resolveDelayProfile({ emotion: "motivated" }), "hot");
});

test("flow: delay profile resolves cold for skeptical emotion", () => {
  assert.equal(resolveDelayProfile({ emotion: "skeptical" }), "cold");
});

test("flow: delay profile resolves neutral by default", () => {
  assert.equal(resolveDelayProfile({}), "neutral");
});

// ══════════════════════════════════════════════════════════════════════════
// 6. LATENCY / SCHEDULING
// ══════════════════════════════════════════════════════════════════════════

import {
  computeScheduledSend,
  extractAgentLatency,
  parseContactWindow,
  seededRandom,
  randomInRange,
  DEFAULT_LATENCY,
} from "@/lib/sms/latency.js";

test("latency: uses assigned agent ranges", () => {
  const agent = {
    "latency-hot-min": 10,
    "latency-hot-max": 20,
  };
  const result = computeScheduledSend({
    now_utc: new Date("2026-04-13T15:00:00Z"),
    timezone: "Eastern",
    assigned_agent: agent,
    delay_profile: "hot",
    seeded_key: ["test", "123"],
  });
  assert.ok(result.latency_seconds >= 10);
  assert.ok(result.latency_seconds <= 20 + 1); // +1 for rounding
  assert.ok(result.delay_source.includes("hot_latency"));
});

test("latency: deterministic randomness stable with same seed", () => {
  const r1 = seededRandom(["a", "b", "c"]);
  const r2 = seededRandom(["a", "b", "c"]);
  assert.equal(r1, r2);
  assert.ok(r1 >= 0 && r1 <= 1);
});

test("latency: different seeds produce different values", () => {
  const r1 = seededRandom(["a", "b", "c"]);
  const r2 = seededRandom(["x", "y", "z"]);
  assert.notEqual(r1, r2);
});

test("latency: respects contact window - does not schedule outside hours", () => {
  // 11 PM Eastern = outside default 9AM-8PM window
  const result = computeScheduledSend({
    now_utc: new Date("2026-04-13T03:00:00Z"), // 3 AM UTC = 11 PM Eastern
    timezone: "Eastern",
    assigned_agent: null,
    delay_profile: "hot",
    contact_window: "9AM-8PM",
    seeded_key: ["window_test"],
  });
  // Should roll forward to next day's window
  assert.ok(result.latency_seconds > 60, "should delay significantly to next window");
});

test("latency: stage day delays apply correctly", () => {
  const agent = {
    "number-6": 2, // Stage 2 Delay = 2 days
  };
  const result = computeScheduledSend({
    now_utc: new Date("2026-04-13T15:00:00Z"),
    timezone: "Eastern",
    assigned_agent: agent,
    message_kind: "follow_up",
    stage_code: "S2F",
    seeded_key: ["stage_delay_test"],
  });
  // 2 days = 172800 seconds, plus up to 3600 jitter
  assert.ok(result.latency_seconds >= 172800);
  assert.ok(result.latency_seconds <= 176400);
  assert.ok(result.delay_source.includes("stage_delay"));
});

test("latency: extractAgentLatency falls back to defaults", () => {
  const latency = extractAgentLatency(null);
  assert.equal(latency.hot_min, DEFAULT_LATENCY.hot_min);
  assert.equal(latency.cold_max, DEFAULT_LATENCY.cold_max);
});

test("latency: parseContactWindow handles AM/PM format", () => {
  const window = parseContactWindow("9AM-8PM");
  assert.equal(window.start_hour, 9);
  assert.equal(window.end_hour, 20);
});

// ══════════════════════════════════════════════════════════════════════════
// 7. QUEUE TRUTHFULNESS
// ══════════════════════════════════════════════════════════════════════════

import {
  buildQueueFields,
  buildDedupeFingerprint,
  queueMessage,
  __setQueueMessageTestDeps,
  __resetQueueMessageTestDeps,
  QUEUE_FIELDS,
  MESSAGE_TYPES,
} from "@/lib/sms/queue_message.js";

test("queue: no fake template attachment when field app ref is incompatible", () => {
  const fields = buildQueueFields({
    rendered_text: "Hello world",
    schedule: { scheduled_utc: "2026-04-13T15:00:00Z", scheduled_local: "2026-04-13T11:00:00", timezone: "America/New_York" },
    resolution: {
      use_case: "ownership_check",
      attachable_template_ref: { app_id: 99999999, item_id: 12345 }, // wrong app
    },
    links: { master_owner_id: 1 },
    context: {},
  });
  // Should NOT have template field set (wrong app)
  assert.equal(fields[QUEUE_FIELDS.template], undefined);
});

test("queue: rendered message text always equals queued message text", () => {
  const rendered = "Hi John about 123 Main St";
  const fields = buildQueueFields({
    rendered_text: rendered,
    schedule: {},
    resolution: {},
    links: {},
    context: {},
  });
  assert.equal(fields[QUEUE_FIELDS.message_text], rendered);
});

test("queue: dedupe blocks duplicate queue rows", async (t) => {
  let createCalled = false;

  __setQueueMessageTestDeps({
    createItem: async () => {
      createCalled = true;
      return { item_id: 999 };
    },
    getFirstMatchingItem: async () => ({
      item_id: 888,
      fields: [{ external_id: "queue-status", values: [{ value: { text: "Queued" } }] }],
    }),
  });

  t.after(() => __resetQueueMessageTestDeps());

  const result = await queueMessage({
    rendered_text: "Hello",
    schedule: {},
    resolution: { use_case: "ownership_check" },
    links: { master_owner_id: 1 },
    context: { phone_e164: "+15551234567" },
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "duplicate_blocked");
  assert.equal(createCalled, false, "should not create when duplicate exists");
});

test("queue: creates row when no duplicate exists", async (t) => {
  let created_fields = null;

  __setQueueMessageTestDeps({
    createItem: async (app_id, fields) => {
      created_fields = fields;
      return { item_id: 1001 };
    },
    getFirstMatchingItem: async () => null,
  });

  t.after(() => __resetQueueMessageTestDeps());

  const result = await queueMessage({
    rendered_text: "Hello world",
    schedule: { scheduled_utc: "2026-04-13T15:00:00Z", scheduled_local: "2026-04-13T11:00:00", timezone: "America/New_York" },
    resolution: { use_case: "ownership_check", stage_code: "S1" },
    links: { master_owner_id: 100, prospect_id: 200 },
    context: { phone_e164: "+15551234567", touch_number: 1 },
  });

  assert.ok(result.ok);
  assert.equal(result.item_id, 1001);
  assert.equal(created_fields[QUEUE_FIELDS.message_text], "Hello world");
  assert.equal(created_fields[QUEUE_FIELDS.queue_status], "Queued");
  assert.equal(created_fields[QUEUE_FIELDS.use_case], "ownership_check");
  assert.deepEqual(created_fields[QUEUE_FIELDS.master_owner], [100]);
  assert.deepEqual(created_fields[QUEUE_FIELDS.prospects], [200]);
});

test("queue: dedupe fingerprint is deterministic", () => {
  const fp1 = buildDedupeFingerprint({ master_owner_id: 1, phone_e164: "+15551234567", use_case: "test" });
  const fp2 = buildDedupeFingerprint({ master_owner_id: 1, phone_e164: "+15551234567", use_case: "test" });
  assert.equal(fp1, fp2);
});

test("queue: dedupe fingerprint changes with different inputs", () => {
  const fp1 = buildDedupeFingerprint({ master_owner_id: 1, use_case: "test" });
  const fp2 = buildDedupeFingerprint({ master_owner_id: 2, use_case: "test" });
  assert.notEqual(fp1, fp2);
});

test("queue: message type resolves correctly", () => {
  const cold = buildQueueFields({
    rendered_text: "hi",
    schedule: {},
    resolution: {},
    links: {},
    context: { is_first_touch: true },
  });
  assert.equal(cold[QUEUE_FIELDS.message_type], MESSAGE_TYPES.COLD_OUTBOUND);

  const followup = buildQueueFields({
    rendered_text: "hi",
    schedule: {},
    resolution: {},
    links: {},
    context: { is_follow_up: true },
  });
  assert.equal(followup[QUEUE_FIELDS.message_type], MESSAGE_TYPES.FOLLOW_UP);
});

// ══════════════════════════════════════════════════════════════════════════
// SUPPLEMENTAL — AGENT STYLE NORMALIZATION
// ══════════════════════════════════════════════════════════════════════════

import { normalizeAgentStyleFit, VALID_STYLES } from "@/lib/sms/agent_style.js";

test("agent style: No-Nonsense Closer maps to Investor Direct", () => {
  assert.equal(normalizeAgentStyleFit({ agent_archetype: "No-Nonsense Closer" }), "Investor Direct");
});

test("agent style: Empathetic maps to Warm Professional", () => {
  assert.equal(normalizeAgentStyleFit({ agent_archetype: "Empathetic" }), "Warm Professional");
});

test("agent style: Market-Local maps to Buyer / Local Buyer", () => {
  assert.equal(normalizeAgentStyleFit({ agent_archetype: "Market-Local" }), "Buyer / Local Buyer");
});

test("agent style: default is Warm Professional", () => {
  assert.equal(normalizeAgentStyleFit({}), "Warm Professional");
});

test("agent style: explicit valid style passes through", () => {
  assert.equal(normalizeAgentStyleFit({ agent_style: "Investor Direct" }), "Investor Direct");
  assert.equal(normalizeAgentStyleFit({ agent_style: "Neutral" }), "Neutral");
});

// ══════════════════════════════════════════════════════════════════════════
// SUPPLEMENTAL — PROPERTY SCOPE
// ══════════════════════════════════════════════════════════════════════════

import { resolvePropertyTypeScope } from "@/lib/sms/property_scope.js";

test("property scope: probate resolves correctly", () => {
  assert.equal(resolvePropertyTypeScope({ is_probate: true }), "Probate / Trust");
  assert.equal(resolvePropertyTypeScope({ owner_type: "Trust / Estate" }), "Probate / Trust");
});

test("property scope: corporate resolves correctly", () => {
  assert.equal(resolvePropertyTypeScope({ is_corporate: true }), "Corporate / Institutional");
  assert.equal(resolvePropertyTypeScope({ owner_type: "Corporate" }), "Corporate / Institutional");
});

test("property scope: unit count routing", () => {
  assert.equal(resolvePropertyTypeScope({ unit_count: 2 }), "Duplex");
  assert.equal(resolvePropertyTypeScope({ unit_count: 3 }), "Triplex");
  assert.equal(resolvePropertyTypeScope({ unit_count: 4 }), "Fourplex");
  assert.equal(resolvePropertyTypeScope({ unit_count: 10 }), "5+ Units");
});

test("property scope: default is Residential", () => {
  assert.equal(resolvePropertyTypeScope({}), "Residential");
});

// ══════════════════════════════════════════════════════════════════════════
// SUPPLEMENTAL — DEAL STRATEGY
// ══════════════════════════════════════════════════════════════════════════

import { resolveDealStrategy } from "@/lib/sms/deal_strategy.js";

test("deal strategy: default is Cash", () => {
  assert.equal(resolveDealStrategy({}), "Cash");
});

test("deal strategy: multifamily underwriting detected", () => {
  assert.equal(resolveDealStrategy({ is_multifamily_underwriting: true }), "Multifamily Underwrite");
  assert.equal(resolveDealStrategy({ stage_code: "MF1" }), "Multifamily Underwrite");
});

test("deal strategy: negotiation from objections", () => {
  assert.equal(resolveDealStrategy({ objection: "need_more_money" }), "Negotiation");
  assert.equal(resolveDealStrategy({ is_negotiation: true }), "Negotiation");
});

test("deal strategy: creative signals route correctly", () => {
  assert.equal(resolveDealStrategy({ seller_wants_lease_option: true }), "Lease Option");
  assert.equal(resolveDealStrategy({ seller_wants_subject_to: true }), "Subject To");
  assert.equal(resolveDealStrategy({ seller_wants_novation: true }), "Novation");
});

// ══════════════════════════════════════════════════════════════════════════
// 8b. STAGE NORMALIZATION — Podio text values → flow_map short codes
// ══════════════════════════════════════════════════════════════════════════

test("flow: Podio stage 'Ownership Confirmation' resolves to ownership_check", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Ownership Confirmation" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
  assert.equal(result.use_case, "ownership_check");
});

test("flow: Podio stage 'Offer Interest Confirmation' resolves to consider_selling stage", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Offer Interest Confirmation" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
  assert.equal(result.use_case, "consider_selling_follow_up");
});

test("flow: Podio stage 'Seller Price Discovery' resolves to asking_price stage", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Seller Price Discovery" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
  assert.equal(result.use_case, "asking_price_follow_up");
});

test("flow: Podio stage 'Condition / Timeline Discovery' resolves to condition probe", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Condition / Timeline Discovery" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
  assert.equal(result.use_case, "condition_question_set");
});

test("flow: Podio stage 'Offer Positioning' resolves to offer stage", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Offer Positioning" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
  assert.equal(result.use_case, "justify_price");
});

test("flow: Podio stage 'Contract Out' resolves to contract stage", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Contract Out" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
});

test("flow: Podio stage 'Signed / Closing' resolves to close stage", () => {
  const result = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "Signed / Closing" },
  });
  assert.equal(result.action, ACTIONS.QUEUE_REPLY);
});

test("flow: existing short codes still work after normalization", () => {
  const r1 = mapNextAction({
    classify_result: { positive_signals: ["confirms_ownership"] },
    brain_state: { conversation_stage: "ownership" },
  });
  assert.equal(r1.use_case, "consider_selling");

  const r2 = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "s1" },
  });
  assert.equal(r2.use_case, "ownership_check");

  const r3 = mapNextAction({
    classify_result: { positive_signals: [] },
    brain_state: { conversation_stage: "consider_selling" },
  });
  assert.equal(r3.use_case, "consider_selling_follow_up");
});
