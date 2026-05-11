// ─── template-automation.proof.mjs ────────────────────────────────────────
import { rankTemplateCandidates } from "../../src/lib/automation/templateSelector.js";
import assert from "node:assert";

async function runProof() {
  console.log("🚀 Running Template Automation Proof...");

  const mockTemplates = [
    {
      id: "tpl-1",
      template_id: "840001",
      use_case: "consider_selling",
      language: "English",
      agent_persona: "Warm Professional",
      template_body: "Hi {{seller_first_name}}, are you interested in selling {{property_address}}?",
      sample_size: 100,
      positive_rate_pct: 0.25,
      opt_out_rate_pct: 0.02
    },
    {
      id: "tpl-2",
      template_id: "840002",
      use_case: "consider_selling",
      language: "Spanish",
      agent_persona: "Warm Professional",
      template_body: "Hola {{seller_first_name}}, ¿le interesa vender {{property_address}}?",
      sample_size: 50,
      positive_rate_pct: 0.30,
      opt_out_rate_pct: 0.01
    },
    {
      id: "tpl-3",
      template_id: "840003",
      use_case: "consider_selling",
      language: "English",
      agent_persona: "Investor Direct",
      template_body: "I want to buy {{property_address}}. You selling?",
      sample_size: 10, // low sample size, no KPI boost
      positive_rate_pct: 0.50,
      opt_out_rate_pct: 0.10
    }
  ];

  // Test Case 1: English Warm Professional
  console.log("\nCase 1: English Warm Professional");
  const ranked1 = rankTemplateCandidates(mockTemplates, {
    language: "English",
    agent_style_fit: "Warm Professional",
    intent: "ownership_confirmed"
  });

  assert.strictEqual(ranked1[0].id, "tpl-1", "Tpl-1 should be first for English Warm Prof");
  assert.ok(ranked1[0].matches.includes("kpi_weighted"), "Should have KPI weight boost");
  console.log("✅ Case 1 Passed");

  // Test Case 2: Spanish Preference
  console.log("\nCase 2: Spanish Preference");
  const ranked2 = rankTemplateCandidates(mockTemplates, {
    language: "Spanish",
    agent_style_fit: "Warm Professional"
  });

  assert.strictEqual(ranked2[0].id, "tpl-2", "Tpl-2 should be first for Spanish");
  console.log("✅ Case 2 Passed");

  // Test Case 3: Investor Direct (Low Sample)
  console.log("\nCase 3: Investor Direct (Low Sample)");
  const ranked3 = rankTemplateCandidates(mockTemplates, {
    language: "English",
    agent_style_fit: "Investor Direct"
  });

  assert.strictEqual(ranked3[0].id, "tpl-3", "Tpl-3 should be first due to persona match despite low sample");
  console.log("✅ Case 3 Passed");

  console.log("\n✨ Template Automation Proof Completed Successfully!");
}

runProof().catch(err => {
  console.error("❌ Proof Failed:", err);
  process.exit(1);
});
