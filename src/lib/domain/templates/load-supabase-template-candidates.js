/**
 * load-supabase-template-candidates.js
 *
 * sms_templates (Supabase) is the first-class runtime template source for the
 * SMS automation engine.  Podio Templates is used for KPI / metrics / mirroring
 * only.  The local registry is an emergency fallback for dev / test environments.
 *
 * Exports
 *   normalizeSupabaseTemplateRow     – converts an sms_templates DB row into
 *     the template candidate shape expected by evaluateTemplateCandidate in
 *     load-template.js
 *   fetchSupabaseTemplateCandidates  – queries sms_templates and returns
 *     normalised candidates ready for scoring; fails open (returns []) when
 *     Supabase is unavailable
 */

import { hasSupabaseConfig, supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { normalizeSelectorText } from "@/lib/domain/templates/template-selector.js";

function clean(value) {
  return String(value ?? "").trim();
}

function stripHtmlForEmptyCheck(value) {
  return clean(value)
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// ---------------------------------------------------------------------------
// Stage 1 signal derivation helpers
// ---------------------------------------------------------------------------

/**
 * Derive the is_first_touch string value ("Yes" / "No" / null) from a
 * sms_templates row.  The boolean column may be unset; we also accept implicit
 * Stage 1 signals from use_case, stage_code, and stage_label.
 */
function deriveIsFirstTouch(row = {}) {
  if (row.is_first_touch === true) return "Yes";
  if (row.is_first_touch === false) return "No";

  // Derive from additional Stage 1 signals when boolean field is NULL.
  if (normalizeSelectorText(row.use_case) === normalizeSelectorText("ownership_check")) {
    return "Yes";
  }
  if (normalizeSelectorText(row.stage_code) === "s1") {
    return "Yes";
  }
  const sl = normalizeSelectorText(row.stage_label || "");
  if (sl.includes("stage 1") && !sl.includes("follow")) {
    return "Yes";
  }

  return null;
}

/**
 * Derive a variant_group string from a sms_templates row so that
 * normalizeTemplateTouchType and isStage1Template can read it.
 */
function deriveVariantGroup(row = {}) {
  const stage_label = clean(row.stage_label);
  if (stage_label) return stage_label;

  if (normalizeSelectorText(row.stage_code) === "s1") {
    return "Stage 1 — Ownership Confirmation";
  }
  return null;
}

// ---------------------------------------------------------------------------
// Public: row normaliser
// ---------------------------------------------------------------------------

/**
 * Normalise a sms_templates database row into the template candidate shape
 * expected by evaluateTemplateCandidate in load-template.js.
 *
 * Key mappings:
 *   template_body  → text       (evaluated for emptiness by evaluateTemplateCandidate)
 *   is_active      → active     "Yes" / "No"
 *   is_first_touch → derived    "Yes" / "No" / null (also inferred from use_case / stage_code)
 *   stage_label    → variant_group  (read by normalizeTemplateTouchType / isStage1Template)
 *   source         = "supabase" (accepted by the feeder live-queue source guard)
 *
 * @param {object} row
 * @returns {object|null}
 */
export function normalizeSupabaseTemplateRow(row = {}) {
  if (!row) return null;

  const id             = clean(row.id);
  const template_id    = clean(row.template_id) || clean(row.podio_template_id) || id;
  const is_first_touch = deriveIsFirstTouch(row);
  const variant_group  = deriveVariantGroup(row);

  return {
    // Source tracking -------------------------------------------------------
    source:                    "supabase",
    template_resolution_source: "supabase_sms_templates",

    // IDs -------------------------------------------------------------------
    id,
    item_id:           template_id || id,
    template_id:       clean(row.template_id)       || null,
    podio_template_id: clean(row.podio_template_id) || null,

    // Use-case fields (read by normalizeTemplateSelectorUseCase) ------------
    use_case:              clean(row.use_case) || null,
    selector_use_case:     clean(row.use_case) || null,
    use_case_label:        clean(row.use_case) || null,
    canonical_routing_slug: null,

    // Touch-type fields (read by normalizeTemplateTouchType) ----------------
    is_first_touch,
    is_follow_up: row.is_follow_up === true
      ? "Yes"
      : row.is_follow_up === false
        ? "No"
        : null,

    // Stage / variant fields ------------------------------------------------
    variant_group,
    stage_code:  clean(row.stage_code)  || null,
    stage_label: clean(row.stage_label) || null,

    // Template body — mapped to "text" for evaluateTemplateCandidate --------
    text:          clean(row.template_body) || "",
    template_text: clean(row.template_body) || "",
    template_body: clean(row.template_body) || "",

    // Status ----------------------------------------------------------------
    active: row.is_active ? "Yes" : "No",

    // Selector fields -------------------------------------------------------
    language:            clean(row.language)            || "English",
    property_type_scope: clean(row.property_type_scope) || null,
    deal_strategy:       clean(row.deal_strategy)       || null,
    agent_persona:       clean(row.agent_persona)       || null,
    tone:                clean(row.agent_persona)       || null,

    // Metadata --------------------------------------------------------------
    english_translation: clean(row.english_translation) || null,
    template_name:       clean(row.template_name)       || null,
    metadata:            row.metadata  || {},
    variables:           row.variables || [],

    // Performance hints (no Podio-sourced engagement metrics available) -----
    spam_risk:               null,
    deliverability_score:    null,
    historical_reply_rate:   null,
    total_sends:             row.usage_count ?? 0,
    total_replies:           null,
    total_conversations:     null,
  };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Return true when the selector is requesting a Stage 1 / first-touch /
 * ownership-check template.
 */
function isStage1SelectorRequest(selector_input = {}) {
  const uc = normalizeSelectorText(selector_input?.use_case || "");
  const tt = normalizeSelectorText(selector_input?.touch_type || "");
  return (
    uc === normalizeSelectorText("ownership_check") ||
    tt === normalizeSelectorText("First Touch")
  );
}

// ---------------------------------------------------------------------------
// Public: Supabase fetcher
// ---------------------------------------------------------------------------

/**
 * Fetch active template candidates from the sms_templates Supabase table.
 *
 * For Stage 1 / first-touch requests the query uses an OR-filter across the
 * three canonical Stage 1 signals so that any matching template is returned
 * regardless of which signal column was populated:
 *
 *   use_case = 'ownership_check'
 *   OR  stage_code = 'S1'
 *   OR  is_first_touch = true
 *
 * For other selectors the query matches by use_case exactly.
 *
 * Rows with an empty / HTML-only template_body are pre-filtered out before
 * being returned so the caller's evaluateTemplateCandidate sees clean data.
 *
 * Returns [] (fail-open) when:
 *   - Supabase is not configured and no client is injected
 *   - The Supabase query throws
 *
 * @param {object}      selector_input              - normalised selector
 * @param {object}      [opts]
 * @param {object|null} [opts.supabase_client]      - injectable client for testing
 * @returns {Promise<object[]>}  normalised template candidates
 */
export async function fetchSupabaseTemplateCandidates(
  selector_input = {},
  { supabase_client = null } = {}
) {
  if (!supabase_client && !hasSupabaseConfig()) return [];

  const client   = supabase_client || defaultSupabase;
  const use_case = clean(selector_input?.use_case) || null;
  const is_stage1 = isStage1SelectorRequest(selector_input);

  try {
    let query = client
      .from("sms_templates")
      .select("*")
      .eq("is_active", true);

    if (is_stage1) {
      query = query.or(
        "use_case.eq.ownership_check,stage_code.eq.S1,is_first_touch.eq.true"
      );
    } else if (use_case) {
      query = query.eq("use_case", use_case);
    }

    const { data, error } = await query;
    if (error) throw error;

    return (Array.isArray(data) ? data : [])
      .map(normalizeSupabaseTemplateRow)
      .filter(Boolean)
      .filter((row) => Boolean(stripHtmlForEmptyCheck(row.text)));
  } catch {
    // Fail open — let the Podio / local-registry path handle it.
    return [];
  }
}
