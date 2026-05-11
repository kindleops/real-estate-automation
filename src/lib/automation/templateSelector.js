// ─── templateSelector.js ──────────────────────────────────────────────────
import { supabase } from "@/lib/supabase/client.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import { getIntentRoute, ACTIONS } from "./intentMap.js";
import crypto from "node:crypto";

// ══════════════════════════════════════════════════════════════════════════
// UTILITIES
// ══════════════════════════════════════════════════════════════════════════

function stableHash(parts = []) {
  const input = parts.map((p) => String(p ?? "")).join("|");
  const hash = crypto.createHash("sha256").update(input, "utf8").digest();
  return hash.readUInt32BE(0) / 0xffffffff; // 0..1
}

function deterministicPick(candidates, seed_parts) {
  if (!candidates.length) return null;
  if (candidates.length === 1) return candidates[0];
  const r = stableHash(seed_parts);
  return candidates[Math.floor(r * candidates.length)];
}

// ══════════════════════════════════════════════════════════════════════════
// RANKING ENGINE
// ══════════════════════════════════════════════════════════════════════════

/**
 * Score and rank template candidates based on context and KPI performance.
 */
export function rankTemplateCandidates(candidates, context) {
  const {
    intent,
    language = "English",
    agent_style_fit,
    property_type_scope,
    deal_strategy,
    touch_number = 1,
  } = context;

  return candidates.map(tpl => {
    let score = 0;
    const matches = [];

    // 1. Intent / Use Case Match (Required)
    // We assume candidates already filtered by use_case/stage from the route

    // 2. Language Match (High Weight)
    if (tpl.language === language) {
      score += 1000;
      matches.push("language");
    } else if (tpl.language === "English") {
      score += 500; // Accept English fallback
      matches.push("language_fallback");
    }

    // 3. Agent Style Match
    if (agent_style_fit && tpl.agent_persona === agent_style_fit) {
      score += 200;
      matches.push("agent_persona");
    }

    // 4. Property Type Scope Match
    if (property_type_scope && tpl.property_type_scope === property_type_scope) {
      score += 100;
      matches.push("property_scope");
    }

    // 5. Deal Strategy Match
    if (deal_strategy && tpl.deal_strategy === deal_strategy) {
      score += 50;
      matches.push("deal_strategy");
    }

    // 6. KPI Performance Weighting
    // Boost based on success rate if we have enough samples
    const sample_size = tpl.sample_size || 0;
    if (sample_size >= 20) {
      const success_boost = (tpl.positive_rate_pct || 0) * 10;
      const opt_out_penalty = (tpl.opt_out_rate_pct || 0) * 20;
      score += (success_boost - opt_out_penalty);
      matches.push("kpi_weighted");
    }

    return { ...tpl, score, matches };
  }).sort((a, b) => b.score - a.score);
}

// ══════════════════════════════════════════════════════════════════════════
// SELECTION
// ══════════════════════════════════════════════════════════════════════════

/**
 * Select the best template for the given context.
 */
export async function selectNextTemplate(context) {
  const { primary_intent, seller_state, confidence } = context.classification;
  const route = getIntentRoute(primary_intent);

  if (route.action !== ACTIONS.QUEUE_REPLY) {
    return {
      ok: false,
      action: route.action,
      reason: route.reason || "no_reply_needed",
      template: null,
    };
  }

  // Fetch candidates from DB with KPI join
  const { data: candidates, error } = await supabase
    .from("sms_templates")
    .select(`
      *,
      kpis:template_performance_kpis_v(*)
    `)
    .eq("use_case", route.use_case)
    .eq("is_active", true);

  if (error || !candidates?.length) {
    return {
      ok: false,
      action: ACTIONS.ESCALATE,
      reason: "no_templates_found",
      template: null,
    };
  }

  // Flatten KPI data
  const flattened = candidates.map(c => ({
    ...c,
    ...(c.kpis?.[0] || {})
  }));

  const ranked = rankTemplateCandidates(flattened, context);
  const best = ranked[0];

  // Seed for deterministic pick if scores are tied
  const seed_parts = [
    context.thread_key,
    route.use_case,
    context.language
  ];

  const winners = ranked.filter(r => r.score === best.score);
  const winner = deterministicPick(winners, seed_parts);

  return {
    ok: true,
    action: ACTIONS.QUEUE_REPLY,
    template: winner,
    use_case: route.use_case,
    stage_code: winner.stage_code || route.stage,
  };
}

// ══════════════════════════════════════════════════════════════════════════
// VALIDATION & RENDERING
// ══════════════════════════════════════════════════════════════════════════

/**
 * Ensure template is safe for the detected intent.
 */
export function validateTemplateForIntent(template, context) {
  const { primary_intent } = context.classification;
  const route = getIntentRoute(primary_intent);

  if (!template) return { ok: false, reason: "missing_template" };
  if (template.use_case !== route.use_case) return { ok: false, reason: "use_case_mismatch" };

  // Personalization safety check (no empty greetings, etc)
  const required = ["seller_first_name", "property_address"];
  for (const field of required) {
    if (template.template_body.includes(`{{${field}}}`) && !context.variables[field]) {
      // If missing name, we can fallback to "there", but address is often critical
      if (field === "property_address") return { ok: false, reason: "missing_critical_variable: property_address" };
    }
  }

  return { ok: true };
}

/**
 * Render template safely with fallbacks.
 */
export function renderSafeTemplate(template, variables) {
  const vars = { ...variables };

  // Fallbacks
  if (!vars.seller_first_name) vars.seller_first_name = "there";

  const render = personalizeTemplate(template.template_body, vars);

  if (!render.ok) return render;

  // Personalization Safety Gates
  const text = render.text;
  if (text.includes("{{") || text.includes("undefined") || text.includes("null")) {
    return { ok: false, reason: "unresolved_tokens_detected", text: null };
  }

  const bad_greetings = ["Hi ,", "Hey ,", "Hello ,"];
  if (bad_greetings.some(g => text.startsWith(g))) {
    return { ok: false, reason: "blank_greeting_detected", text: null };
  }

  return render;
}

export default {
  selectNextTemplate,
  rankTemplateCandidates,
  validateTemplateForIntent,
  renderSafeTemplate,
};
