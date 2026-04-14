// ─── template_resolver.js ─────────────────────────────────────────────────
// Given conversation state + classify result + property context + agent context,
// choose the single best template row from the CSV catalog.
// Applies deterministic fallback ladder and returns a resolution result.

import crypto from "node:crypto";
import { loadCatalog } from "@/lib/sms/template_catalog.js";
import { normalizeLanguage, isUnsupportedTemplateLanguage } from "@/lib/sms/language_aliases.js";
import { normalizeAgentStyleFit } from "@/lib/sms/agent_style.js";
import { resolvePropertyTypeScope } from "@/lib/sms/property_scope.js";
import { resolveDealStrategy } from "@/lib/sms/deal_strategy.js";

// ══════════════════════════════════════════════════════════════════════════
// SEEDED DETERMINISTIC RANDOM
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
// CROSS-VIOLATION GUARDS
// ══════════════════════════════════════════════════════════════════════════

const CREATIVE_STRATEGIES = new Set(["Creative", "Lease Option", "Subject To", "Novation"]);

function wouldCrossFirstFollowUp(template, query) {
  if (query.is_first_touch && template.is_follow_up && !template.is_first_touch) return true;
  if (query.is_follow_up && template.is_first_touch && !template.is_follow_up) return true;
  return false;
}

function wouldCrossCashCreative(template, query) {
  const q_creative = CREATIVE_STRATEGIES.has(query.deal_strategy);
  const t_creative = CREATIVE_STRATEGIES.has(template.deal_strategy);
  if (!q_creative && t_creative) return true;
  if (q_creative && template.deal_strategy === "Cash") return true;
  return false;
}

function wouldCrossSpecificScope(template, query) {
  const specific = new Set(["Probate / Trust", "Corporate / Institutional"]);
  if (specific.has(query.property_type_scope) && !specific.has(template.property_type_scope) && template.property_type_scope !== "Any Residential") return true;
  if (specific.has(template.property_type_scope) && !specific.has(query.property_type_scope)) return true;
  return false;
}

// ══════════════════════════════════════════════════════════════════════════
// SCORING
// ══════════════════════════════════════════════════════════════════════════

function lc(val) {
  return String(val ?? "").toLowerCase().trim();
}

function scoreTemplate(template, query) {
  let score = 0;
  const matches = [];
  const mismatches = [];

  // Hard filters — must be active
  if (!template.active) return { score: -1, matches, mismatches: ["inactive"] };

  // Hard violation guards
  if (wouldCrossFirstFollowUp(template, query)) return { score: -1, matches, mismatches: ["first_follow_up_cross"] };
  if (wouldCrossCashCreative(template, query)) return { score: -1, matches, mismatches: ["cash_creative_cross"] };
  if (wouldCrossSpecificScope(template, query)) return { score: -1, matches, mismatches: ["specific_scope_cross"] };

  // Use case (highest weight)
  if (lc(template.use_case) === lc(query.use_case)) {
    score += 1000;
    matches.push("use_case");
  } else {
    mismatches.push("use_case");
    return { score: -1, matches, mismatches };
  }

  // Language (required match for primary pass)
  if (lc(template.language) === lc(query.language)) {
    score += 500;
    matches.push("language");
  } else {
    mismatches.push("language");
  }

  // First touch / follow-up flags
  if (query.is_first_touch !== undefined) {
    if (template.is_first_touch === query.is_first_touch) {
      score += 200;
      matches.push("is_first_touch");
    } else {
      mismatches.push("is_first_touch");
    }
  }

  if (query.is_follow_up !== undefined) {
    if (template.is_follow_up === query.is_follow_up) {
      score += 200;
      matches.push("is_follow_up");
    } else {
      mismatches.push("is_follow_up");
    }
  }

  // Deal strategy
  if (query.deal_strategy && lc(template.deal_strategy) === lc(query.deal_strategy)) {
    score += 100;
    matches.push("deal_strategy");
  } else if (query.deal_strategy && template.deal_strategy) {
    mismatches.push("deal_strategy");
  }

  // Property type scope
  if (query.property_type_scope && lc(template.property_type_scope) === lc(query.property_type_scope)) {
    score += 80;
    matches.push("property_type_scope");
  } else if (query.property_type_scope && template.property_type_scope) {
    mismatches.push("property_type_scope");
  }

  // Agent style fit
  if (query.agent_style_fit && lc(template.agent_style_fit) === lc(query.agent_style_fit)) {
    score += 60;
    matches.push("agent_style_fit");
  } else if (query.agent_style_fit && template.agent_style_fit) {
    mismatches.push("agent_style_fit");
  }

  // Stage code
  if (query.stage_code && lc(template.stage_code) === lc(query.stage_code)) {
    score += 40;
    matches.push("stage_code");
  } else if (query.stage_code && template.stage_code) {
    mismatches.push("stage_code");
  }

  return { score, matches, mismatches };
}

// ══════════════════════════════════════════════════════════════════════════
// FALLBACK LADDER
// ══════════════════════════════════════════════════════════════════════════

const BROADER_SCOPE_FALLBACKS = Object.freeze({
  "Residential": "Any Residential",
  "Duplex": "Landlord / Multifamily",
  "Triplex": "Landlord / Multifamily",
  "Fourplex": "Landlord / Multifamily",
  "5+ Units": "Landlord / Multifamily",
});

function buildFallbackQueries(original_query) {
  const steps = [];

  // A: exact everything (already tried in primary scoring)

  // B: relax stage_code
  if (original_query.stage_code) {
    steps.push({ ...original_query, stage_code: null, step: "relax_stage_code" });
  }

  // C: relax agent_style_fit
  if (original_query.agent_style_fit) {
    steps.push({ ...original_query, stage_code: null, agent_style_fit: null, step: "relax_agent_style" });
  }

  // D: relax property_type_scope to broader
  if (original_query.property_type_scope) {
    const broader = BROADER_SCOPE_FALLBACKS[original_query.property_type_scope] || "Any Residential";
    steps.push({
      ...original_query,
      stage_code: null,
      agent_style_fit: null,
      property_type_scope: broader,
      step: "relax_property_scope",
    });
  }

  // E: relax deal_strategy (only if use_case is still logically valid)
  if (original_query.deal_strategy && original_query.deal_strategy !== "Cash") {
    steps.push({
      ...original_query,
      stage_code: null,
      agent_style_fit: null,
      property_type_scope: null,
      deal_strategy: "Cash",
      step: "relax_deal_strategy",
    });
  }

  // F: same language + use_case + first/follow flags + Warm Professional or Neutral
  steps.push({
    use_case: original_query.use_case,
    language: original_query.language,
    is_first_touch: original_query.is_first_touch,
    is_follow_up: original_query.is_follow_up,
    agent_style_fit: "Warm Professional",
    stage_code: null,
    property_type_scope: null,
    deal_strategy: null,
    step: "fallback_warm_professional",
  });

  steps.push({
    use_case: original_query.use_case,
    language: original_query.language,
    is_first_touch: original_query.is_first_touch,
    is_follow_up: original_query.is_follow_up,
    agent_style_fit: "Neutral",
    stage_code: null,
    property_type_scope: null,
    deal_strategy: null,
    step: "fallback_neutral",
  });

  // G: English equivalent (only if original was non-English)
  if (lc(original_query.language) !== "english") {
    steps.push({
      use_case: original_query.use_case,
      language: "English",
      is_first_touch: original_query.is_first_touch,
      is_follow_up: original_query.is_follow_up,
      agent_style_fit: null,
      stage_code: null,
      property_type_scope: null,
      deal_strategy: null,
      step: "english_fallback",
    });
  }

  return steps;
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN RESOLVER
// ══════════════════════════════════════════════════════════════════════════

/**
 * @param {object} context
 * @param {string} context.use_case - Required outbound use case
 * @param {string} [context.stage_code]
 * @param {string} [context.language] - Canonical language
 * @param {string} [context.agent_style_fit] - One of: Investor Direct, Warm Professional, Neutral, Buyer / Local Buyer
 * @param {string} [context.property_type_scope]
 * @param {string} [context.deal_strategy]
 * @param {boolean} [context.is_first_touch]
 * @param {boolean} [context.is_follow_up]
 * @param {string} [context.master_owner_id] - For deterministic seed
 * @param {string} [context.phone_e164] - For deterministic seed
 * @param {string} [context.csv_path] - Override CSV path for testing
 * @returns {object} Resolution result
 */
export function resolveTemplate(context = {}) {
  const catalog = loadCatalog(context.csv_path);
  const resolution_path = [];

  const language = normalizeLanguage(context.language) || "English";
  const unsupported = isUnsupportedTemplateLanguage(context.language);

  const query = {
    use_case: context.use_case || null,
    stage_code: context.stage_code || null,
    language,
    agent_style_fit: context.agent_style_fit || null,
    property_type_scope: context.property_type_scope || null,
    deal_strategy: context.deal_strategy || null,
    is_first_touch: context.is_first_touch ?? undefined,
    is_follow_up: context.is_follow_up ?? undefined,
  };

  if (!query.use_case) {
    return {
      template_id: null,
      template_text: null,
      english_translation: null,
      language: null,
      use_case: null,
      stage_code: null,
      agent_style_fit: null,
      property_type_scope: null,
      deal_strategy: null,
      source: "csv_catalog",
      resolution_path: ["no_use_case"],
      attachable_template_ref: null,
      resolved: false,
      fallback_reason: "no_use_case",
    };
  }

  const seed_parts = [
    context.master_owner_id,
    context.phone_e164,
    query.use_case,
    query.stage_code,
    query.language,
    query.agent_style_fit,
  ];

  // Primary pass: score all rows against exact query
  const scored = catalog.rows
    .map((row) => ({ row, ...scoreTemplate(row, query) }))
    .filter((s) => s.score > 0)
    .sort((a, b) => b.score - a.score);

  if (scored.length > 0) {
    const best_score = scored[0].score;
    const tied = scored.filter((s) => s.score === best_score);
    const winner = deterministicPick(
      tied.map((s) => s.row),
      seed_parts
    );
    resolution_path.push("exact_match");
    return buildResult(winner, resolution_path, unsupported);
  }

  // Fallback ladder
  const fallback_queries = buildFallbackQueries(query);
  for (const fq of fallback_queries) {
    const fallback_scored = catalog.rows
      .map((row) => ({ row, ...scoreTemplate(row, fq) }))
      .filter((s) => s.score > 0)
      .sort((a, b) => b.score - a.score);

    if (fallback_scored.length > 0) {
      const best_score = fallback_scored[0].score;
      const tied = fallback_scored.filter((s) => s.score === best_score);
      const winner = deterministicPick(
        tied.map((s) => s.row),
        seed_parts
      );
      resolution_path.push(fq.step);
      if (fq.step === "english_fallback" && unsupported) {
        resolution_path.push("unsupported_template_language");
      }
      return buildResult(winner, resolution_path, unsupported);
    }
  }

  resolution_path.push("no_match");
  return {
    template_id: null,
    template_text: null,
    english_translation: null,
    language: query.language,
    use_case: query.use_case,
    stage_code: query.stage_code,
    agent_style_fit: query.agent_style_fit,
    property_type_scope: query.property_type_scope,
    deal_strategy: query.deal_strategy,
    source: "csv_catalog",
    resolution_path,
    attachable_template_ref: null,
    resolved: false,
    fallback_reason: "no_matching_template",
  };
}

function buildResult(template, resolution_path, unsupported_language = false) {
  return {
    template_id: template.template_id,
    template_text: template.template_text,
    english_translation: template.english_translation || null,
    language: template.language,
    use_case: template.use_case,
    stage_code: template.stage_code || null,
    agent_style_fit: template.agent_style_fit || null,
    property_type_scope: template.property_type_scope || null,
    deal_strategy: template.deal_strategy || null,
    source: "csv_catalog",
    resolution_path,
    attachable_template_ref: null, // populated externally when Podio ref is valid
    resolved: true,
    fallback_reason: unsupported_language ? "unsupported_template_language_english_fallback" : null,
  };
}

export { stableHash, deterministicPick, scoreTemplate, buildFallbackQueries };

export default { resolveTemplate };
