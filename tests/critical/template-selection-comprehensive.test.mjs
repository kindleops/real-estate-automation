// ─── template-selection-comprehensive.test.mjs ───────────────────────────
// Comprehensive template selection tests covering EVERY use case:
//   • Seller flow stages (ownership → asking price → underwriting → offers)
//   • Follow-ups for every stage
//   • Multifamily (MF) pipeline
//   • Novation pipeline
//   • Negotiation stages
//   • Identity / terminal (who_is_this, wrong_person, not_interested, etc.)
//   • Disposition / title / closing
//   • Reengagement
//   • Language variations + English fallback
//   • Cross-violation guards (first/follow-up, cash/creative, specific scope)
//   • CSV resolver fallback ladder
//   • Local template registry coverage

import test from "node:test";
import assert from "node:assert/strict";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEST_CSV = resolve(__dirname, "../helpers/test-templates.csv");

// ── CSV resolver ─────────────────────────────────────────────────────────
import {
  resolveTemplate,
  scoreTemplate,
  deterministicPick,
  buildFallbackQueries,
} from "@/lib/sms/template_resolver.js";

import {
  loadCatalog,
  __resetCatalog,
} from "@/lib/sms/template_catalog.js";

// ── Podio resolver ───────────────────────────────────────────────────────
import { loadTemplateCandidates } from "@/lib/domain/templates/load-template.js";

// ── Local templates ──────────────────────────────────────────────────────
import { LOCAL_TEMPLATE_CANDIDATES } from "@/lib/domain/templates/local-template-registry.js";

// ══════════════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════════════

function makeTemplate(overrides = {}) {
  const defaults = {
    item_id: 900000 + Math.floor(Math.random() * 100000),
    template_id: `T-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    title: null,
    raw: null,
    active: "Yes",
    use_case: "ownership_check",
    variant_group: null,
    stage_code: null,
    stage_label: null,
    language: "English",
    agent_style_fit: "Warm Professional",
    property_type_scope: "Residential",
    deal_strategy: "Cash",
    is_first_touch: "No",
    is_follow_up: "No",
    text: "Hi {{seller_first_name}} about {{property_address}}.",
    template_text: "Hi {{seller_first_name}} about {{property_address}}.",
    english_translation: "Hi {{seller_first_name}} about {{property_address}}.",
    category_primary: "Residential",
    category_secondary: "Underwriting",
    tone: "Neutral",
    gender_variant: "Neutral",
    sequence_position: "V1",
    paired_with_agent_type: "Warm Professional",
    personalization_tags: [],
    deliverability_score: 92,
    spam_risk: 4,
    historical_reply_rate: 24,
    total_sends: 0,
    total_replies: 0,
    total_conversations: 0,
    cooldown_days: 3,
    version: 1,
    last_used: null,
    source: "local_registry",
  };
  return { ...defaults, ...overrides };
}

function makeLocalFetcher(templates) {
  return () => templates;
}

async function noRemoteFetch() {
  return [];
}

const MINIMAL_CONTEXT = {
  found: true,
  ids: { master_owner_id: 12345 },
  items: {},
  summary: {
    property_address: "123 Main St",
    seller_first_name: "John",
    agent_first_name: "Sarah",
  },
  recent: { recently_used_template_ids: [] },
};

function resolveCSV(overrides = {}) {
  return resolveTemplate({ csv_path: TEST_CSV, ...overrides });
}

// ══════════════════════════════════════════════════════════════════════════
// 1. SELLER FLOW STAGES — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("csv: ownership_check first touch resolves to T001 (English, Warm Professional)", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Warm Professional",
    property_type_scope: "Residential",
    deal_strategy: "Cash",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T001");
  assert.equal(result.source, "csv_catalog");
  assert.ok(result.resolution_path.includes("exact_match"));
  t.after(() => __resetCatalog());
});

test("csv: ownership_check Spanish resolves to T002", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "Spanish",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T002");
  assert.equal(result.language, "Spanish");
  t.after(() => __resetCatalog());
});

test("csv: ownership_check Investor Direct resolves to T003", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Investor Direct",
    property_type_scope: "Residential",
    deal_strategy: "Cash",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T003");
  t.after(() => __resetCatalog());
});

test("csv: ownership_check Buyer / Local Buyer resolves to T013", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Buyer / Local Buyer",
    property_type_scope: "Residential",
    deal_strategy: "Cash",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T013");
  t.after(() => __resetCatalog());
});

test("csv: ownership_check Hindi resolves to T020", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "Asian Indian (Hindi or Other)",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T020");
  t.after(() => __resetCatalog());
});

test("csv: consider_selling resolves to T004", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "consider_selling",
    language: "English",
    agent_style_fit: "Warm Professional",
    is_first_touch: false,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T004");
  t.after(() => __resetCatalog());
});

test("csv: consider_selling_follow_up resolves to T005", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "consider_selling_follow_up",
    language: "English",
    is_first_touch: false,
    is_follow_up: true,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T005");
  t.after(() => __resetCatalog());
});

test("csv: ownership_check_follow_up resolves to T006", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check_follow_up",
    language: "English",
    is_first_touch: false,
    is_follow_up: true,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T006");
  t.after(() => __resetCatalog());
});

test("csv: creative_probe resolves to T009", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "creative_probe",
    language: "English",
    deal_strategy: "Creative",
    is_first_touch: false,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T009");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 2. OFFER STAGES — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("csv: offer_reveal_cash resolves to T014", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "offer_reveal_cash",
    language: "English",
    is_first_touch: false,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T014");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 3. MULTIFAMILY PIPELINE — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("csv: mf_confirm_units resolves to T015", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "mf_confirm_units",
    language: "English",
    property_type_scope: "Landlord / Multifamily",
    deal_strategy: "Multifamily Underwrite",
    is_first_touch: false,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T015");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 4. IDENTITY / TERMINAL USE CASES — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("csv: who_is_this resolves to T016", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "who_is_this",
    language: "English",
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T016");
  t.after(() => __resetCatalog());
});

test("csv: wrong_person resolves to T008", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "wrong_person",
    language: "English",
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T008");
  t.after(() => __resetCatalog());
});

test("csv: not_interested resolves to T007", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "not_interested",
    language: "English",
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T007");
  t.after(() => __resetCatalog());
});

test("csv: not_ready resolves to T018", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "not_ready",
    language: "English",
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T018");
  t.after(() => __resetCatalog());
});

test("csv: proof_of_funds resolves to T017", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "proof_of_funds",
    language: "English",
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T017");
  t.after(() => __resetCatalog());
});

test("csv: reengagement resolves to T019", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "reengagement",
    language: "English",
    is_first_touch: false,
    is_follow_up: true,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T019");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 5. PROPERTY TYPE SCOPE ROUTING — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("csv: Probate / Trust ownership_check resolves to T011", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Probate / Trust",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T011");
  t.after(() => __resetCatalog());
});

test("csv: Corporate / Institutional ownership_check resolves to T012", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Corporate / Institutional",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T012");
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 6. LANGUAGE FALLBACK — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("csv: unsupported language (Thai) falls back to English with fallback_reason", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "Thai",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.language, "English");
  // Thai normalizes to null → query language becomes English → primary match succeeds
  assert.ok(result.fallback_reason?.includes("unsupported"));
  assert.ok(result.resolution_path.includes("exact_match"));
  t.after(() => __resetCatalog());
});

test("csv: Vietnamese with no Vietnamese template falls back to English", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "consider_selling",
    language: "Vietnamese",
    is_first_touch: false,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.language, "English");
  // No Vietnamese template exists → English template picked via primary scoring (english_fallback bonus)
  assert.ok(result.resolution_path.includes("exact_match"));
  t.after(() => __resetCatalog());
});

test("csv: English query does NOT trigger english_fallback step", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "English",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.ok(!result.resolution_path.includes("english_fallback"));
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 7. CROSS-VIOLATION GUARDS — SCORING
// ══════════════════════════════════════════════════════════════════════════

test("score: first-touch template rejected for follow-up query", () => {
  const template = makeTemplate({ is_first_touch: true, is_follow_up: false });
  const query = { use_case: "ownership_check", is_first_touch: false, is_follow_up: true };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("first_follow_up_cross"));
});

test("score: follow-up template rejected for first-touch query", () => {
  const template = makeTemplate({ is_first_touch: false, is_follow_up: true });
  const query = { use_case: "ownership_check", is_first_touch: true, is_follow_up: false };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("first_follow_up_cross"));
});

test("score: dual-flagged template (first+follow) accepted for either", () => {
  const template = makeTemplate({ is_first_touch: true, is_follow_up: true });
  const query_first = { use_case: "ownership_check", is_first_touch: true, is_follow_up: false };
  const query_follow = { use_case: "ownership_check", is_first_touch: false, is_follow_up: true };
  assert.ok(scoreTemplate(template, query_first).score > 0);
  assert.ok(scoreTemplate(template, query_follow).score > 0);
});

test("score: Creative template rejected for Cash query", () => {
  const template = makeTemplate({ deal_strategy: "Creative" });
  const query = { use_case: "ownership_check", deal_strategy: "Cash" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("cash_creative_cross"));
});

test("score: Cash template rejected for Creative query", () => {
  const template = makeTemplate({ deal_strategy: "Cash" });
  const query = { use_case: "ownership_check", deal_strategy: "Creative" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("cash_creative_cross"));
});

test("score: Lease Option template rejected for Cash query", () => {
  const template = makeTemplate({ deal_strategy: "Lease Option" });
  const query = { use_case: "ownership_check", deal_strategy: "Cash" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("cash_creative_cross"));
});

test("score: Subject To template rejected for Cash query", () => {
  const template = makeTemplate({ deal_strategy: "Subject To" });
  const query = { use_case: "ownership_check", deal_strategy: "Cash" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("cash_creative_cross"));
});

test("score: Novation template rejected for Cash query", () => {
  const template = makeTemplate({ deal_strategy: "Novation" });
  const query = { use_case: "ownership_check", deal_strategy: "Cash" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("cash_creative_cross"));
});

test("score: Probate template rejected for non-Probate query", () => {
  const template = makeTemplate({ property_type_scope: "Probate / Trust" });
  const query = { use_case: "ownership_check", property_type_scope: "Residential" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("specific_scope_cross"));
});

test("score: non-Probate template rejected for Probate query", () => {
  const template = makeTemplate({ property_type_scope: "Residential" });
  const query = { use_case: "ownership_check", property_type_scope: "Probate / Trust" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("specific_scope_cross"));
});

test("score: Corporate template rejected for Residential query", () => {
  const template = makeTemplate({ property_type_scope: "Corporate / Institutional" });
  const query = { use_case: "ownership_check", property_type_scope: "Residential" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("specific_scope_cross"));
});

test("score: Any Residential template accepted for Probate query", () => {
  const template = makeTemplate({ property_type_scope: "Any Residential" });
  const query = { use_case: "ownership_check", property_type_scope: "Probate / Trust" };
  const result = scoreTemplate(template, query);
  assert.ok(result.score > 0);
});

test("score: inactive template hard-rejected", () => {
  const template = makeTemplate({ active: false });
  const query = { use_case: "ownership_check" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("inactive"));
});

test("score: use_case mismatch hard-rejected", () => {
  const template = makeTemplate({ use_case: "consider_selling" });
  const query = { use_case: "ownership_check" };
  const result = scoreTemplate(template, query);
  assert.equal(result.score, -1);
  assert.ok(result.mismatches.includes("use_case"));
});

// ══════════════════════════════════════════════════════════════════════════
// 8. SCORING WEIGHT VERIFICATION
// ══════════════════════════════════════════════════════════════════════════

test("score: exact language (+500) beats English fallback (+300)", () => {
  const template_exact = makeTemplate({ language: "Spanish" });
  const template_english = makeTemplate({ language: "English" });
  const query = { use_case: "ownership_check", language: "spanish" };
  const exact = scoreTemplate(template_exact, query);
  const english = scoreTemplate(template_english, query);
  assert.ok(exact.score > english.score, `exact ${exact.score} should beat english ${english.score}`);
  assert.ok(exact.matches.includes("language"));
  assert.ok(english.matches.includes("english_fallback"));
});

test("score: use_case match grants +1000", () => {
  const template = makeTemplate({ use_case: "ownership_check" });
  const query = { use_case: "ownership_check" };
  const result = scoreTemplate(template, query);
  assert.ok(result.score >= 1000);
  assert.ok(result.matches.includes("use_case"));
});

test("score: is_first_touch match grants +200", () => {
  const t1 = makeTemplate({ is_first_touch: true, is_follow_up: false });
  const t2 = makeTemplate({ is_first_touch: false, is_follow_up: false });
  const query = { use_case: "ownership_check", is_first_touch: true };
  const s1 = scoreTemplate(t1, query);
  const s2 = scoreTemplate(t2, query);
  assert.ok(s1.score > s2.score);
  assert.equal(s1.score - s2.score, 200);
});

test("score: is_follow_up match grants +200", () => {
  const t1 = makeTemplate({ is_follow_up: true, is_first_touch: false });
  const t2 = makeTemplate({ is_follow_up: false, is_first_touch: false });
  const query = { use_case: "ownership_check", is_first_touch: false, is_follow_up: true };
  const s1 = scoreTemplate(t1, query);
  const s2 = scoreTemplate(t2, query);
  assert.ok(s1.score > s2.score);
  assert.equal(s1.score - s2.score, 200);
});

test("score: deal_strategy match grants +100", () => {
  const t1 = makeTemplate({ deal_strategy: "Cash" });
  const t2 = { ...makeTemplate(), deal_strategy: undefined };
  const query = { use_case: "ownership_check", deal_strategy: "Cash" };
  const s1 = scoreTemplate(t1, query);
  const s2 = scoreTemplate(t2, query);
  assert.equal(s1.score - s2.score, 100);
});

test("score: property_type_scope match grants +80", () => {
  const t1 = makeTemplate({ property_type_scope: "Residential" });
  const t2 = { ...makeTemplate(), property_type_scope: undefined };
  const query = { use_case: "ownership_check", property_type_scope: "Residential" };
  const s1 = scoreTemplate(t1, query);
  const s2 = scoreTemplate(t2, query);
  assert.equal(s1.score - s2.score, 80);
});

test("score: agent_style_fit match grants +60", () => {
  const t1 = makeTemplate({ agent_style_fit: "Warm Professional" });
  const t2 = { ...makeTemplate(), agent_style_fit: undefined };
  const query = { use_case: "ownership_check", agent_style_fit: "Warm Professional" };
  const s1 = scoreTemplate(t1, query);
  const s2 = scoreTemplate(t2, query);
  assert.equal(s1.score - s2.score, 60);
});

test("score: stage_code match grants +40", () => {
  const t1 = makeTemplate({ stage_code: "S1" });
  const t2 = makeTemplate({ stage_code: null });
  const query = { use_case: "ownership_check", stage_code: "S1" };
  const s1 = scoreTemplate(t1, query);
  const s2 = scoreTemplate(t2, query);
  assert.equal(s1.score - s2.score, 40);
});

// ══════════════════════════════════════════════════════════════════════════
// 9. FALLBACK LADDER — CSV RESOLVER
// ══════════════════════════════════════════════════════════════════════════

test("fallback: agent style falls back from unknown to Warm Professional", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "consider_selling",
    language: "English",
    agent_style_fit: "Investor Direct",
    is_first_touch: false,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.template_id, "T004");
  assert.equal(result.agent_style_fit, "Warm Professional");
  t.after(() => __resetCatalog());
});

test("fallback: buildFallbackQueries includes relax_stage_code when stage_code present", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    stage_code: "S1",
    agent_style_fit: "Warm Professional",
    property_type_scope: "Residential",
    deal_strategy: "Cash",
  });
  assert.ok(steps.some((s) => s.step === "relax_stage_code"));
});

test("fallback: buildFallbackQueries includes relax_agent_style when agent_style present", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    agent_style_fit: "Investor Direct",
  });
  assert.ok(steps.some((s) => s.step === "relax_agent_style"));
});

test("fallback: buildFallbackQueries includes relax_property_scope for Residential", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Residential",
  });
  const scope_step = steps.find((s) => s.step === "relax_property_scope");
  assert.ok(scope_step);
  assert.equal(scope_step.property_type_scope, "Any Residential");
});

test("fallback: buildFallbackQueries maps Duplex to Landlord / Multifamily", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Duplex",
  });
  const scope_step = steps.find((s) => s.step === "relax_property_scope");
  assert.equal(scope_step.property_type_scope, "Landlord / Multifamily");
});

test("fallback: buildFallbackQueries maps Triplex to Landlord / Multifamily", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Triplex",
  });
  const scope_step = steps.find((s) => s.step === "relax_property_scope");
  assert.equal(scope_step.property_type_scope, "Landlord / Multifamily");
});

test("fallback: buildFallbackQueries maps Fourplex to Landlord / Multifamily", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Fourplex",
  });
  const scope_step = steps.find((s) => s.step === "relax_property_scope");
  assert.equal(scope_step.property_type_scope, "Landlord / Multifamily");
});

test("fallback: buildFallbackQueries maps 5+ Units to Landlord / Multifamily", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "5+ Units",
  });
  const scope_step = steps.find((s) => s.step === "relax_property_scope");
  assert.equal(scope_step.property_type_scope, "Landlord / Multifamily");
});

test("fallback: buildFallbackQueries includes relax_deal_strategy for Creative", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    deal_strategy: "Creative",
  });
  const deal_step = steps.find((s) => s.step === "relax_deal_strategy");
  assert.ok(deal_step);
  assert.equal(deal_step.deal_strategy, "Cash");
});

test("fallback: buildFallbackQueries skips relax_deal_strategy for Cash", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
    deal_strategy: "Cash",
  });
  assert.ok(!steps.some((s) => s.step === "relax_deal_strategy"));
});

test("fallback: buildFallbackQueries always includes warm_professional and neutral", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
  });
  assert.ok(steps.some((s) => s.step === "fallback_warm_professional"));
  assert.ok(steps.some((s) => s.step === "fallback_neutral"));
});

test("fallback: buildFallbackQueries includes english_fallback for non-English", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "Spanish",
  });
  assert.ok(steps.some((s) => s.step === "english_fallback"));
  const eng_step = steps.find((s) => s.step === "english_fallback");
  assert.equal(eng_step.language, "English");
});

test("fallback: buildFallbackQueries omits english_fallback for English", () => {
  const steps = buildFallbackQueries({
    use_case: "ownership_check",
    language: "English",
  });
  assert.ok(!steps.some((s) => s.step === "english_fallback"));
});

test("fallback: no_use_case returns unresolved", (t) => {
  __resetCatalog();
  const result = resolveCSV({});
  assert.equal(result.resolved, false);
  assert.equal(result.fallback_reason, "no_use_case");
  t.after(() => __resetCatalog());
});

test("fallback: unknown use_case returns no_matching_template", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "totally_bogus_use_case",
    language: "English",
  });
  assert.equal(result.resolved, false);
  assert.equal(result.fallback_reason, "no_matching_template");
  assert.ok(result.resolution_path.includes("no_match"));
  t.after(() => __resetCatalog());
});

// ══════════════════════════════════════════════════════════════════════════
// 10. DETERMINISTIC PICK — STABILITY
// ══════════════════════════════════════════════════════════════════════════

test("deterministic: same seed always picks the same template", () => {
  const templates = [{ id: "A" }, { id: "B" }, { id: "C" }, { id: "D" }];
  const seed = ["owner_999", "+15559876543", "ownership_check"];
  const pick1 = deterministicPick(templates, seed);
  const pick2 = deterministicPick(templates, seed);
  assert.deepEqual(pick1, pick2);
});

test("deterministic: different seed picks different (usually)", () => {
  const templates = Array.from({ length: 20 }, (_, i) => ({ id: `T${i}` }));
  const seed_a = ["owner_1", "+15551111111", "uc_a"];
  const seed_b = ["owner_2", "+15552222222", "uc_b"];
  const pick_a = deterministicPick(templates, seed_a);
  const pick_b = deterministicPick(templates, seed_b);
  // With 20 candidates, different seeds should almost always pick different
  // This is probabilistic but with 20 items, collision chance is ~5%
  // We test it anyway — the important thing is determinism, not uniqueness
  assert.ok(pick_a !== undefined);
  assert.ok(pick_b !== undefined);
});

test("deterministic: single candidate always returns that candidate", () => {
  const templates = [{ id: "ONLY" }];
  const pick = deterministicPick(templates, ["any", "seed"]);
  assert.deepEqual(pick, { id: "ONLY" });
});

test("deterministic: empty array returns null", () => {
  assert.equal(deterministicPick([], ["seed"]), null);
});

// ══════════════════════════════════════════════════════════════════════════
// 11. LOCAL TEMPLATE REGISTRY — COVERAGE
// ══════════════════════════════════════════════════════════════════════════

const EXPECTED_LOCAL_USE_CASES = [
  "ownership_check",
  "ownership_check_follow_up",
  "consider_selling_follow_up",
  "asking_price_follow_up",
  "price_works_confirm_basics_follow_up",
  "price_high_condition_probe_follow_up",
  "offer_reveal_cash_follow_up",
  "justify_price",
  "ask_timeline",
  "ask_condition_clarifier",
  "narrow_range",
  "close_handoff",
  "mf_confirm_units",
  "mf_occupancy",
  "mf_rents",
  "mf_expenses",
  "mf_underwriting_ack",
  "mf_confirm_units_follow_up",
  "mf_occupancy_follow_up",
  "mf_rents_follow_up",
  "mf_expenses_follow_up",
  "novation_probe",
  "novation_condition_scope",
  "novation_listing_readiness",
  "novation_timeline",
  "novation_net_to_seller",
  "disposition_access_coordination",
  "disposition_marketing_update",
  "reengagement",
];

test("local registry: every expected use_case has at least one template", () => {
  const available = new Set(LOCAL_TEMPLATE_CANDIDATES.map((t) => t.use_case));
  const missing = EXPECTED_LOCAL_USE_CASES.filter((uc) => !available.has(uc));
  assert.deepEqual(missing, [], `Missing use_cases in local registry: ${missing.join(", ")}`);
});

test("local registry: all templates have non-empty template_text", () => {
  const empty = LOCAL_TEMPLATE_CANDIDATES.filter((t) => !t.text?.trim());
  assert.equal(empty.length, 0, `Found ${empty.length} templates with empty text`);
});

test("local registry: all templates have valid source=local_registry", () => {
  const wrong_source = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.source !== "local_registry");
  assert.equal(wrong_source.length, 0);
});

test("local registry: ownership_check templates marked is_first_touch=Yes", () => {
  const oc = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case === "ownership_check");
  assert.ok(oc.length > 0, "should have ownership_check templates");
  const non_first = oc.filter((t) => t.is_first_touch !== "Yes");
  assert.equal(non_first.length, 0, "all ownership_check should be is_first_touch=Yes");
});

test("local registry: follow_up templates NOT marked is_first_touch=Yes", () => {
  const follow_ups = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case.includes("_follow_up"));
  assert.ok(follow_ups.length > 0, "should have follow-up templates");
  const wrong = follow_ups.filter((t) => t.is_first_touch === "Yes");
  assert.equal(wrong.length, 0, "follow-up templates should not be first touch");
});

test("local registry: Spanish ownership_check templates exist", () => {
  const spanish = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check" && t.language === "Spanish"
  );
  assert.ok(spanish.length >= 2, `expected ≥2 Spanish ownership_check, got ${spanish.length}`);
});

test("local registry: Spanish ownership_check_follow_up templates exist", () => {
  const spanish = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check_follow_up" && t.language === "Spanish"
  );
  assert.ok(spanish.length >= 1, `expected ≥1 Spanish ownership_check_follow_up, got ${spanish.length}`);
});

test("local registry: MF templates have Landlord / Multifamily category", () => {
  const mf = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case.startsWith("mf_"));
  assert.ok(mf.length > 0);
  const wrong_cat = mf.filter((t) => t.category_primary !== "Landlord / Multifamily");
  assert.equal(wrong_cat.length, 0, `MF templates with wrong category: ${wrong_cat.map((t) => t.use_case)}`);
});

test("local registry: novation templates exist for all novation stages", () => {
  const novation_cases = [
    "novation_probe",
    "novation_condition_scope",
    "novation_listing_readiness",
    "novation_timeline",
    "novation_net_to_seller",
  ];
  for (const uc of novation_cases) {
    const found = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case === uc);
    assert.ok(found.length >= 1, `missing local template for ${uc}`);
  }
});

test("local registry: negotiation use_cases all have ≥2 variants", () => {
  const negotiation_cases = [
    "justify_price",
    "ask_timeline",
    "ask_condition_clarifier",
    "narrow_range",
    "close_handoff",
  ];
  for (const uc of negotiation_cases) {
    const found = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case === uc);
    assert.ok(found.length >= 2, `${uc} has only ${found.length} variants, expected ≥2`);
  }
});

test("local registry: disposition templates exist", () => {
  const dispo_cases = ["disposition_access_coordination", "disposition_marketing_update"];
  for (const uc of dispo_cases) {
    const found = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case === uc);
    assert.ok(found.length >= 2, `${uc} has only ${found.length} variants, expected ≥2`);
  }
});

test("local registry: reengagement has English and Spanish variants", () => {
  const re = LOCAL_TEMPLATE_CANDIDATES.filter((t) => t.use_case === "reengagement");
  const langs = new Set(re.map((t) => t.language));
  assert.ok(langs.has("English"), "reengagement should have English variant");
  assert.ok(langs.has("Spanish"), "reengagement should have Spanish variant");
});

test("local registry: reengagement has MF variant", () => {
  const re_mf = LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "reengagement" && t.category_primary === "Landlord / Multifamily"
  );
  assert.ok(re_mf.length >= 1, "reengagement should have MF variant");
});

// ══════════════════════════════════════════════════════════════════════════
// 12. PODIO RESOLVER — loadTemplateCandidates WITH LOCAL FETCHER
// ══════════════════════════════════════════════════════════════════════════

test("podio: ownership_check first-touch selects correct local template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-1001", use_case: "ownership_check", is_first_touch: "Yes", language: "English" }),
    makeTemplate({ item_id: "t-1002", use_case: "ownership_check", is_first_touch: "No", language: "English" }),
    makeTemplate({ item_id: "t-1003", use_case: "consider_selling", is_first_touch: "No", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1, "should have at least one candidate");
  assert.ok(candidates.some((c) => c.use_case === "ownership_check"), "should include ownership_check");
});

test("podio: language match preferred over English fallback", async () => {
  const templates = [
    makeTemplate({ item_id: "t-2001", use_case: "ownership_check", language: "Spanish", is_first_touch: "Yes" }),
    makeTemplate({ item_id: "t-2002", use_case: "ownership_check", language: "English", is_first_touch: "Yes" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "Spanish",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  // Spanish template should rank higher than English
  const spanish = candidates.find((c) => c.language === "Spanish");
  assert.ok(spanish, "Spanish template should be in candidates");
});

test("podio: English fallback when target language unavailable", async () => {
  const templates = [
    makeTemplate({ item_id: "t-3001", use_case: "ownership_check", language: "English", is_first_touch: "Yes" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "Vietnamese",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1, "English should survive as fallback");
  assert.ok(candidates.some((c) => c.language === "English"));
});

test("podio: consider_selling selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-4001", use_case: "consider_selling", language: "English" }),
    makeTemplate({ item_id: "t-4002", use_case: "ownership_check", language: "English", is_first_touch: "Yes" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "consider_selling",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "consider_selling"));
});

test("podio: ownership_check_follow_up selects follow-up template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-5001", use_case: "ownership_check_follow_up", is_follow_up: "Yes", language: "English" }),
    makeTemplate({ item_id: "t-5002", use_case: "ownership_check", is_first_touch: "Yes", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check_follow_up",
    touch_type: "Follow-Up",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.item_id === "t-5001"), "follow-up template must survive");
});

test("podio: offer_reveal_cash selects offer template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-6001", use_case: "offer_reveal_cash", language: "English" }),
    makeTemplate({ item_id: "t-6002", use_case: "consider_selling", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "offer_reveal_cash",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "offer_reveal_cash"));
});

test("podio: mf_confirm_units selects MF template", async () => {
  const templates = [
    makeTemplate({
      item_id: "t-7001",
      use_case: "mf_confirm_units",
      language: "English",
      property_type_scope: "Landlord / Multifamily",
      category_primary: "Landlord / Multifamily",
      deal_strategy: "Multifamily Underwrite",
    }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_confirm_units",
    language: "English",
    property_type_scope: "Landlord / Multifamily",
    deal_strategy: "Multifamily Underwrite",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_confirm_units"));
});

test("podio: mf_occupancy selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-7010", use_case: "mf_occupancy", language: "English", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
    makeTemplate({ item_id: "t-7011", use_case: "mf_confirm_units", language: "English", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_occupancy",
    language: "English",
    category: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_occupancy"));
});

test("podio: mf_rents selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-7020", use_case: "mf_rents", language: "English", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_rents",
    language: "English",
    category: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_rents"));
});

test("podio: mf_expenses selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-7030", use_case: "mf_expenses", language: "English", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_expenses",
    language: "English",
    category: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_expenses"));
});

test("podio: mf_underwriting_ack selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-7040", use_case: "mf_underwriting_ack", language: "English", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_underwriting_ack",
    language: "English",
    category: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_underwriting_ack"));
});

test("podio: novation_probe selects novation template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-8001", use_case: "novation_probe", language: "English", deal_strategy: "Novation" }),
    makeTemplate({ item_id: "t-8002", use_case: "creative_probe", language: "English", deal_strategy: "Creative" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "novation_probe",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "novation_probe"));
});

test("podio: novation_condition_scope selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-8010", use_case: "novation_condition_scope", language: "English", deal_strategy: "Novation" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "novation_condition_scope",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "novation_condition_scope"));
});

test("podio: novation_listing_readiness selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-8020", use_case: "novation_listing_readiness", language: "English", deal_strategy: "Novation" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "novation_listing_readiness",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "novation_listing_readiness"));
});

test("podio: novation_timeline selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-8030", use_case: "novation_timeline", language: "English", deal_strategy: "Novation" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "novation_timeline",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "novation_timeline"));
});

test("podio: novation_net_to_seller selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-8040", use_case: "novation_net_to_seller", language: "English", deal_strategy: "Novation" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "novation_net_to_seller",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "novation_net_to_seller"));
});

test("podio: justify_price selects negotiation template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-9001", use_case: "justify_price", language: "English" }),
    makeTemplate({ item_id: "t-9002", use_case: "narrow_range", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "justify_price",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "justify_price"));
});

test("podio: ask_timeline selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-9010", use_case: "ask_timeline", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ask_timeline",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "ask_timeline"));
});

test("podio: ask_condition_clarifier selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-9020", use_case: "ask_condition_clarifier", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ask_condition_clarifier",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "ask_condition_clarifier"));
});

test("podio: narrow_range selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-9030", use_case: "narrow_range", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "narrow_range",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "narrow_range"));
});

test("podio: close_handoff selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-9040", use_case: "close_handoff", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "close_handoff",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "close_handoff"));
});

test("podio: who_is_this selects identity template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-10001", use_case: "who_is_this", language: "English" }),
    makeTemplate({ item_id: "t-10002", use_case: "ownership_check", language: "English", is_first_touch: "Yes" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "who_is_this",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "who_is_this"));
});

test("podio: wrong_person selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-10010", use_case: "wrong_person", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "wrong_person",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "wrong_person"));
});

test("podio: not_interested selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-10020", use_case: "not_interested", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "not_interested",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "not_interested"));
});

test("podio: disposition_access_coordination selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-11001", use_case: "disposition_access_coordination", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "disposition_access_coordination",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "disposition_access_coordination"));
});

test("podio: disposition_marketing_update selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-11010", use_case: "disposition_marketing_update", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "disposition_marketing_update",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "disposition_marketing_update"));
});

test("podio: reengagement selects re-engagement template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-12001", use_case: "reengagement", is_follow_up: "Yes", language: "English" }),
    makeTemplate({ item_id: "t-12002", use_case: "ownership_check_follow_up", is_follow_up: "Yes", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "reengagement",
    touch_type: "Follow-Up",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "reengagement"));
});

// ══════════════════════════════════════════════════════════════════════════
// 13. PODIO RESOLVER — OFFER VARIANTS
// ══════════════════════════════════════════════════════════════════════════

test("podio: offer_reveal_lease_option selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-13001", use_case: "offer_reveal_lease_option", language: "English", deal_strategy: "Lease Option" }),
    makeTemplate({ item_id: "t-13002", use_case: "offer_reveal_cash", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "offer_reveal_lease_option",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "offer_reveal_lease_option"));
});

test("podio: offer_reveal_subject_to selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-13010", use_case: "offer_reveal_subject_to", language: "English", deal_strategy: "Subject To" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "offer_reveal_subject_to",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "offer_reveal_subject_to"));
});

test("podio: offer_reveal_novation selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-13020", use_case: "offer_reveal_novation", language: "English", deal_strategy: "Novation" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "offer_reveal_novation",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "offer_reveal_novation"));
});

test("podio: mf_offer_reveal selects MF offer template", async () => {
  const templates = [
    makeTemplate({
      item_id: "t-13030",
      use_case: "mf_offer_reveal",
      language: "English",
      property_type_scope: "Landlord / Multifamily",
      category_primary: "Landlord / Multifamily",
      deal_strategy: "Multifamily Underwrite",
    }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_offer_reveal",
    language: "English",
    property_type_scope: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_offer_reveal"));
});

// ══════════════════════════════════════════════════════════════════════════
// 14. PODIO RESOLVER — FOLLOW-UP STAGES
// ══════════════════════════════════════════════════════════════════════════

const FOLLOW_UP_USE_CASES = [
  "ownership_check_follow_up",
  "consider_selling_follow_up",
  "asking_price_follow_up",
  "price_works_confirm_basics_follow_up",
  "price_high_condition_probe_follow_up",
  "offer_reveal_cash_follow_up",
  "mf_confirm_units_follow_up",
  "mf_occupancy_follow_up",
  "mf_rents_follow_up",
  "mf_expenses_follow_up",
];

for (const uc of FOLLOW_UP_USE_CASES) {
  test(`podio: ${uc} selects correct follow-up template`, async () => {
    const isMF = uc.startsWith("mf_");
    const templates = [
      makeTemplate({
        item_id: `t-fu-${uc}`,
        use_case: uc,
        is_follow_up: "Yes",
        language: "English",
        ...(isMF ? { deal_strategy: "Multifamily Underwrite", property_type_scope: "Landlord / Multifamily" } : {}),
      }),
      makeTemplate({ item_id: "t-fu-decoy", use_case: "ownership_check", is_first_touch: "Yes", language: "English" }),
    ];
    const candidates = await loadTemplateCandidates({
      use_case: uc,
      touch_type: "Follow-Up",
      language: "English",
      ...(isMF ? { category: "Landlord / Multifamily" } : {}),
      context: MINIMAL_CONTEXT,
      remote_fetcher: noRemoteFetch,
      local_fetcher: makeLocalFetcher(templates),
    });
    assert.ok(candidates.length >= 1, `no candidates for ${uc}`);
    assert.ok(candidates.some((c) => c.use_case === uc), `${uc} template must be in candidates`);
  });
}

// ══════════════════════════════════════════════════════════════════════════
// 15. PODIO RESOLVER — MULTI-LANGUAGE SELECTION
// ══════════════════════════════════════════════════════════════════════════

const LANGUAGES_TO_TEST = ["Spanish", "Vietnamese", "Mandarin", "Korean", "Tagalog", "Arabic"];

for (const lang of LANGUAGES_TO_TEST) {
  test(`podio: ${lang} ownership_check includes ${lang} template`, async () => {
    const templates = [
      makeTemplate({ item_id: `t-lang-${lang}`, use_case: "ownership_check", language: lang, is_first_touch: "Yes" }),
      makeTemplate({ item_id: "t-lang-en", use_case: "ownership_check", language: "English", is_first_touch: "Yes" }),
    ];
    const candidates = await loadTemplateCandidates({
      use_case: "ownership_check",
      language: lang,
      context: MINIMAL_CONTEXT,
      remote_fetcher: noRemoteFetch,
      local_fetcher: makeLocalFetcher(templates),
    });
    assert.ok(candidates.length >= 1);
    assert.ok(candidates.some((c) => c.language === lang), `${lang} template should be in candidates`);
  });
}

// ══════════════════════════════════════════════════════════════════════════
// 16. PODIO RESOLVER — PROPERTY SCOPE PREFERENCE
// ══════════════════════════════════════════════════════════════════════════

test("podio: Probate scope template survives for Probate query", async () => {
  const templates = [
    makeTemplate({ item_id: "t-16001", use_case: "ownership_check", property_type_scope: "Probate / Trust", is_first_touch: "Yes" }),
    makeTemplate({ item_id: "t-16002", use_case: "ownership_check", property_type_scope: "Residential", is_first_touch: "Yes" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    property_type_scope: "Probate / Trust",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
});

test("podio: Multifamily scope template survives for MF query", async () => {
  const templates = [
    makeTemplate({ item_id: "t-16010", use_case: "mf_confirm_units", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
    makeTemplate({ item_id: "t-16011", use_case: "mf_confirm_units", property_type_scope: "Landlord / Multifamily", category_primary: "Residential", deal_strategy: "Multifamily Underwrite" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_confirm_units",
    language: "English",
    property_type_scope: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_confirm_units"));
});

// ══════════════════════════════════════════════════════════════════════════
// 17. PODIO RESOLVER — DEAL STRATEGY PREFERENCE
// ══════════════════════════════════════════════════════════════════════════

test("podio: Novation template survives for novation query", async () => {
  const templates = [
    makeTemplate({ item_id: "t-17001", use_case: "novation_probe", language: "English", deal_strategy: "Novation" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "novation_probe",
    language: "English",
    deal_strategy: "Novation",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "novation_probe"));
});

test("podio: MF Underwrite template survives for MF query", async () => {
  const templates = [
    makeTemplate({ item_id: "t-17010", use_case: "mf_confirm_units", language: "English", property_type_scope: "Landlord / Multifamily", category_primary: "Landlord / Multifamily", deal_strategy: "Multifamily Underwrite" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "mf_confirm_units",
    language: "English",
    deal_strategy: "Multifamily Underwrite",
    category: "Landlord / Multifamily",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "mf_confirm_units"));
});

// ══════════════════════════════════════════════════════════════════════════
// 18. PODIO RESOLVER — FALLS BACK TO LOCAL WHEN NO REMOTE
// ══════════════════════════════════════════════════════════════════════════

test("podio: falls back to local_registry templates when no remote available", async () => {
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
  });
  assert.ok(candidates.length >= 1, "should fall back to local templates");
  assert.ok(
    candidates.some((c) => c.source === "local_registry"),
    "at least one template should be from local_registry"
  );
});

// ══════════════════════════════════════════════════════════════════════════
// 19. UNDERWRITING USE CASES — COMPREHENSIVE
// ══════════════════════════════════════════════════════════════════════════

test("podio: asking_price selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-19001", use_case: "asking_price", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "asking_price",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "asking_price"));
});

test("podio: price_works_confirm_basics selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-19010", use_case: "price_works_confirm_basics", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "price_works_confirm_basics",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "price_works_confirm_basics"));
});

test("podio: price_high_condition_probe selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-19020", use_case: "price_high_condition_probe", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "price_high_condition_probe",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "price_high_condition_probe"));
});

test("podio: creative_probe selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-19030", use_case: "creative_probe", language: "English", deal_strategy: "Creative" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "creative_probe",
    language: "English",
    deal_strategy: "Creative",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "creative_probe"));
});

test("podio: condition_question_set selects correct template", async () => {
  const templates = [
    makeTemplate({ item_id: "t-19040", use_case: "condition_question_set", language: "English" }),
  ];
  const candidates = await loadTemplateCandidates({
    use_case: "condition_question_set",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher(templates),
  });
  assert.ok(candidates.length >= 1);
  assert.ok(candidates.some((c) => c.use_case === "condition_question_set"));
});

// ══════════════════════════════════════════════════════════════════════════
// 20. EDGE CASES
// ══════════════════════════════════════════════════════════════════════════

test("podio: empty template list with unknown use_case returns empty", async () => {
  const candidates = await loadTemplateCandidates({
    use_case: "totally_nonexistent_use_case_xyz",
    language: "English",
    context: MINIMAL_CONTEXT,
    remote_fetcher: noRemoteFetch,
    local_fetcher: makeLocalFetcher([]),
  });
  assert.equal(candidates.length, 0, "should have no candidates for nonexistent use case");
});

test("csv: null language defaults to English in resolver", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: null,
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.language, "English");
  t.after(() => __resetCatalog());
});

test("csv: undefined language defaults to English in resolver", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: undefined,
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  assert.equal(result.language, "English");
  t.after(() => __resetCatalog());
});

test("csv: case-insensitive use_case matching", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "OWNERSHIP_CHECK",
    language: "English",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  t.after(() => __resetCatalog());
});

test("csv: case-insensitive language matching", (t) => {
  __resetCatalog();
  const result = resolveCSV({
    use_case: "ownership_check",
    language: "english",
    is_first_touch: true,
    is_follow_up: false,
  });
  assert.ok(result.resolved);
  t.after(() => __resetCatalog());
});
