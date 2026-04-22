import { NextResponse } from "next/server.js";

import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import { routeSellerConversation } from "@/lib/domain/seller-flow/route-seller-conversation.js";
import { maybeQueueSellerStageReply } from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import { SELLER_FLOW_STAGES } from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { warn as logWarn } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// ── Stage → expected use_case groups ──────────────────────────────────────────

const STAGE_EXPECTED_USE_CASES = Object.freeze({
  [SELLER_FLOW_STAGES.ownership_check]:   ["ownership_confirmation", "ownership_check", "who_is_this"],
  [SELLER_FLOW_STAGES.consider_selling]:  ["consider_selling"],
  [SELLER_FLOW_STAGES.asking_price]:      ["asking_price", "price_check"],
  [SELLER_FLOW_STAGES.offer_reveal_cash]: ["offer_reveal_cash", "make_offer", "offer_reveal"],
  [SELLER_FLOW_STAGES.mf_confirm_units]:  ["mf_confirm_units", "multifamily_underwriting", "underwriting_multifamily"],
  [SELLER_FLOW_STAGES.stop_or_opt_out]:   ["stop_or_opt_out"],
  [SELLER_FLOW_STAGES.wrong_person]:      ["wrong_person"],
  [SELLER_FLOW_STAGES.who_is_this]:       ["who_is_this", "identity_clarification"],
  [SELLER_FLOW_STAGES.not_interested]:    ["not_interested", "soft_no"],
  [SELLER_FLOW_STAGES.reengagement]:      ["reengagement"],
});

// ── Helpers ───────────────────────────────────────────────────────────────────

function clean(value) {
  return String(value ?? "").trim();
}

function sanitizeErrorMessage(err) {
  const raw = clean(err?.message || "unexpected_error");
  const secrets = [
    process.env.INTERNAL_API_SECRET,
    process.env.CRON_SECRET,
    process.env.DISCORD_BOT_TOKEN,
  ].filter(Boolean);

  const contains_secret = secrets.some((s) => raw.includes(String(s)));
  if (!raw || contains_secret) return "unexpected_error";
  return raw.slice(0, 200);
}

function isTemplateCsvMissingError(err) {
  const msg = clean(err?.message).toLowerCase();
  return msg.includes("enoent") && msg.includes("lifecycle-sms-template-pack.csv");
}

const defaultReplayDeps = {
  classify,
  extractUnderwritingSignals,
  routeSellerConversation,
  maybeQueueSellerStageReply,
  loadTemplate,
  personalizeTemplate,
  warn: logWarn,
};

let replayDeps = { ...defaultReplayDeps };

export function __setReplayInboundTestDeps(overrides = {}) {
  replayDeps = { ...replayDeps, ...overrides };
}

export function __resetReplayInboundTestDeps() {
  replayDeps = { ...defaultReplayDeps };
}

/**
 * Build a minimal synthetic context that mimics what handle-textgrid-inbound
 * would load from Podio/Supabase. Keeps the replay endpoint stateless and
 * safe in dry-run mode.
 */
function buildSyntheticContext({
  master_owner_id = null,
  property_id = null,
  phone_item_id = null,
  prior_stage = null,
  prior_language = "English",
  property_type = null,
  deal_strategy = null,
  prior_use_case = null,
} = {}) {
  const ids = {
    brain_item_id:    null,
    master_owner_id:  master_owner_id ? String(master_owner_id) : null,
    prospect_id:      null,
    property_id:      property_id     ? String(property_id)     : null,
    phone_item_id:    phone_item_id   ? String(phone_item_id)   : null,
  };

  const recent_events = prior_use_case
    ? [
        {
          direction:        "outbound",
          message_body:     "",
          sent_at:          new Date(Date.now() - 3_600_000).toISOString(),
          metadata: {
            selected_use_case: prior_use_case,
          },
        },
      ]
    : [];

  return {
    found: true,
    ids,
    items: {
      brain_item:        null,
      phone_item:        null,
      master_owner_item: null,
      property_item:     null,
      agent_item:        null,
    },
    summary: {
      conversation_stage:   prior_stage || null,
      language_preference:  prior_language || "English",
      property_type:        property_type || null,
      deal_strategy:        deal_strategy || null,
      market_timezone:      "Central",
      contact_window:       "12AM-11:59PM CT",
      total_messages_sent:  0,
    },
    recent: {
      recent_events,
      touch_count: 0,
    },
  };
}

/**
 * Builds a personalization context from available inputs so that
 * personalizeTemplate() can substitute placeholders where possible.
 */
function buildPersonalizeContext({ from_phone_number, to_phone_number, context, underwriting }) {
  return {
    phone_e164:      clean(from_phone_number),
    to_phone_e164:   clean(to_phone_number),
    property_type:   underwriting?.property_type || context?.summary?.property_type || null,
    deal_strategy:   underwriting?.creative_strategy || context?.summary?.deal_strategy || null,
    first_name:      null,
    owner_name:      null,
    market_name:     null,
    asking_price:    underwriting?.asking_price ?? null,
  };
}

/**
 * Run alignment assertions on the routing plan and resolved template.
 * Returns an array of { ok, assertion, detail } records.
 */
function runAlignmentAssertions({ plan, template_resolved, underwriting }) {
  const assertions = [];

  const next_stage     = clean(plan?.next_expected_stage);
  const use_case       = clean(template_resolved?.use_case || plan?.selected_use_case);
  const stage_code     = clean(template_resolved?.stage_code);

  // S-code alignment: the resolved template's stage_code should match next_expected_stage
  if (next_stage && stage_code) {
    const ok = stage_code === next_stage || stage_code === use_case;
    assertions.push({
      ok,
      assertion: "stage_code_matches_next_stage",
      detail: `next_expected_stage=${next_stage}, template.stage_code=${stage_code}`,
    });
  }

  // Use-case alignment: template use_case should match expected bucket for next_stage
  if (next_stage && STAGE_EXPECTED_USE_CASES[next_stage]) {
    const allowed = STAGE_EXPECTED_USE_CASES[next_stage];
    const ok = allowed.includes(use_case) || allowed.includes(stage_code);
    assertions.push({
      ok,
      assertion: "use_case_in_expected_bucket",
      detail: `use_case=${use_case}, allowed=${allowed.join("|")}`,
    });
  }

  // Multifamily guard: MF properties must NOT get single-family cash offer templates
  const is_multifamily = /multi.*family|apartment|duplex|triplex|quad|units/i.test(
    underwriting?.property_type || ""
  );
  if (is_multifamily) {
    const is_sfh_offer = /offer_reveal_cash|make_offer/.test(use_case);
    assertions.push({
      ok: !is_sfh_offer,
      assertion: "multifamily_no_sfh_cash_offer",
      detail: `property_type=${underwriting?.property_type}, use_case=${use_case}`,
    });
  }

  // Compliance: stop / opt-out should never queue a reply
  const is_compliance = plan?.detected_intent === "opt_out" ||
    plan?.selected_use_case === "stop_or_opt_out";
  if (is_compliance) {
    assertions.push({
      ok: plan?.should_queue_reply === false,
      assertion: "compliance_no_auto_reply",
      detail: `should_queue_reply=${plan?.should_queue_reply}`,
    });
  }

  // No HTML in rendered text
  const rendered = clean(template_resolved?.template_text);
  if (rendered) {
    const has_html = /<[a-z][\s\S]*>/i.test(rendered);
    assertions.push({
      ok: !has_html,
      assertion: "template_text_no_html_tags",
      detail: has_html ? "HTML tags detected in template_text" : "clean",
    });
  }

  return assertions;
}

// ── POST handler ─────────────────────────────────────────────────────────────

export async function POST(request) {
  const auth = requireSharedSecretAuth(request, null, {
    env_name:     "INTERNAL_API_SECRET",
    header_names: ["x-internal-api-secret"],
  });
  if (!auth.authorized) return auth.response;

  let body;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  const {
    from_phone_number    = null,
    to_phone_number      = null,
    from_number          = null,
    to_number            = null,
    from                 = null,
    to                   = null,
    message_body         = body?.body ?? body?.message ?? null,
    owner_id             = null,
    master_owner_id      = owner_id,
    property_id          = null,
    phone_item_id        = null,
    prior_stage          = null,
    prior_language       = "English",
    prior_use_case       = null,
    property_type        = null,
    deal_strategy        = null,
    maybe_offer          = null,
    existing_offer       = null,
  } = body ?? {};

  // Replay endpoint is always dry-run to guarantee no side effects.
  const dry_run = true;

  const normalized_from_number = clean(from_number ?? from_phone_number ?? from) || null;
  const normalized_to_number = clean(to_number ?? to_phone_number ?? to) || null;
  const normalized_message_body = clean(message_body);

  if (!normalized_message_body || typeof normalized_message_body !== "string") {
    return NextResponse.json(
      { ok: false, error: "missing_required_field", field: "message_body" },
      { status: 400 }
    );
  }

  const errors   = [];
  const pipeline = {};

  try {
    // ── 1. Classify ──────────────────────────────────────────────────────
    const classification = await replayDeps.classify(normalized_message_body, null);
    pipeline.classify = { ok: true, result: classification };

    // ── 2. Build synthetic context ────────────────────────────────────────
    const context = buildSyntheticContext({
      master_owner_id,
      property_id,
      phone_item_id,
      prior_stage,
      prior_language,
      property_type,
      deal_strategy,
      prior_use_case,
    });

    // ── 3. Extract underwriting signals ────────────────────────────────────
    const underwriting = replayDeps.extractUnderwritingSignals({
      message:        normalized_message_body,
      classification,
      context,
    });
    pipeline.underwriting = { ok: true, result: underwriting };

    // ── 4. Route the conversation ─────────────────────────────────────────
    const plan = replayDeps.routeSellerConversation({
      context,
      classification,
      message: normalized_message_body,
      previous_outbound_use_case: prior_use_case,
      maybe_offer,
      existing_offer,
    });
    pipeline.route = { ok: true, result: plan };

    // ── 5. Dry-run queue capture ──────────────────────────────────────────
    let captured_queue_payload = null;
    const dry_run_capture = async (payload) => {
      captured_queue_payload = payload;
      return {
        ok:          true,
        queued:      false,
        dry_run:     true,
        reason:      "dry_run_capture",
        queue_item:  null,
        payload,
      };
    };

    let queue_result = null;
    if (plan?.handled) {
      queue_result = await maybeQueueSellerStageReply({
        inbound_from:               normalized_from_number,
        context,
        classification,
        message:                    normalized_message_body,
        previous_outbound_use_case: prior_use_case,
        maybe_offer,
        existing_offer,
        queue_message:              dry_run ? dry_run_capture : undefined,
      });
      pipeline.queue = { ok: true, result: queue_result };
    }

    // ── 6. Resolve template ───────────────────────────────────────────────
    let template_resolved = {
      resolved: false,
      source: null,
      template_id: null,
      stage_code: null,
      use_case: null,
      language: null,
      template_text: null,
    };

    try {
      const selected_template = await replayDeps.loadTemplate({
        use_case:            plan?.template_lookup_use_case || plan?.selected_use_case,
        language:            plan?.detected_language || classification?.language || "English",
        touch_type:          "Follow-Up",
        property_type_scope: underwriting?.property_type || property_type || null,
        deal_strategy:       underwriting?.creative_strategy || deal_strategy || null,
        context,
      });

      if (selected_template?.text) {
        template_resolved = {
          resolved: true,
          source:
            selected_template?.template_resolution_source ||
            selected_template?.selected_template_source ||
            selected_template?.source ||
            null,
          template_id: selected_template?.template_id || selected_template?.item_id || null,
          stage_code: selected_template?.stage_code || plan?.next_expected_stage || null,
          use_case:
            selected_template?.selector_use_case ||
            selected_template?.use_case ||
            plan?.template_lookup_use_case ||
            plan?.selected_use_case ||
            null,
          language:
            selected_template?.language ||
            plan?.detected_language ||
            classification?.language ||
            "English",
          template_text: selected_template.text,
        };
      }
      pipeline.template_resolve = { ok: true, result: template_resolved };
    } catch (template_err) {
      if (isTemplateCsvMissingError(template_err)) {
        replayDeps.warn("replay.template_csv_missing", {
          reason: "csv_template_catalog_not_found",
        });
      }
      pipeline.template_resolve = {
        ok: false,
        error: sanitizeErrorMessage(template_err),
      };
    }

    // ── 7. Personalize template ───────────────────────────────────────────
    let personalized = null;
    if (template_resolved?.resolved && template_resolved?.template_text) {
      const personalize_context = buildPersonalizeContext({
        from_phone_number: normalized_from_number,
        to_phone_number: normalized_to_number,
        context,
        underwriting,
      });
      personalized = replayDeps.personalizeTemplate(template_resolved.template_text, personalize_context);
    }
    pipeline.personalize = personalized
      ? { ok: true, result: personalized }
      : { ok: false, reason: "no_template_resolved" };

    // ── 8. Alignment assertions ───────────────────────────────────────────
    const assertions = runAlignmentAssertions({ plan, template_resolved, underwriting });
    const assertions_passed = assertions.every((a) => a.ok);

    // ── 9. Build response ─────────────────────────────────────────────────
    const response = {
      ok:                       true,
      dry_run,
      message_body:             normalized_message_body,
      from_number:              normalized_from_number,
      to_number:                normalized_to_number,
      inbound_message_body:     normalized_message_body,
      classification: {
        language:         classification?.language,
        objection:        classification?.objection,
        emotion:          classification?.emotion,
        stage_hint:       classification?.stage_hint,
        compliance_flag:  classification?.compliance_flag,
        positive_signals: classification?.positive_signals,
        confidence:       classification?.confidence,
      },
      previous_stage:           prior_stage,
      next_stage:               plan?.next_expected_stage,
      route: {
        previous_stage:          prior_stage,
        next_stage:              plan?.next_expected_stage,
        detected_intent:         plan?.detected_intent || null,
        selected_use_case:       plan?.selected_use_case || null,
        template_lookup_use_case: plan?.template_lookup_use_case || null,
      },
      stage_transition_reason:  plan?.reasoning_summary || null,
      detected_intent:          plan?.detected_intent || null,
      selected_use_case:        plan?.selected_use_case,
      template_lookup_use_case: plan?.template_lookup_use_case || null,
      selected_template_source:     template_resolved?.source || null,
      selected_template_id:         template_resolved?.template_id || null,
      selected_template_stage_code: template_resolved?.stage_code || null,
      selected_template_use_case:   template_resolved?.use_case || null,
      selected_template_language:   template_resolved?.language || null,
      rendered_message_text:        personalized?.ok ? personalized.text : null,
      personalization_ok:           personalized?.ok ?? false,
      personalization_missing:      personalized?.ok === false ? personalized?.missing : null,
      would_queue_reply:            plan?.should_queue_reply ?? false,
      suppression_reason:           plan?.handled === false ? "seller_flow_not_handled" : null,
      underwriting_signals: {
        property_type:         underwriting?.property_type || null,
        asking_price:          underwriting?.asking_price ?? null,
        unit_count:            underwriting?.unit_count ?? null,
        creative_strategy:     underwriting?.creative_strategy || null,
        creative_terms_interest: underwriting?.creative_terms_interest ?? false,
        occupancy_status:      underwriting?.occupancy_status || null,
      },
      underwriting_route: underwriting?.property_type
        ? /multi.*family|apartment|duplex|triplex|quad|\d+\s*unit/i.test(underwriting.property_type)
          ? "multifamily_underwriting"
          : "standard"
        : null,
      offer_route: plan?.selected_use_case?.includes("offer_reveal") || plan?.selected_use_case?.includes("make_offer")
        ? plan?.selected_use_case
        : null,
      captured_queue_payload:   dry_run ? captured_queue_payload : undefined,
      safety: {
        sms_sent: false,
        queue_created: false,
        podio_mutated: false,
      },
      alignment_assertions:     assertions,
      alignment_passed:         assertions_passed,
      errors:                   errors.length > 0 ? errors : null,
      _pipeline:                process.env.NODE_ENV !== "production" ? pipeline : undefined,
    };

    return NextResponse.json(response, { status: assertions_passed ? 200 : 200 });

  } catch (err) {
    errors.push({
      step:    "pipeline",
      message: sanitizeErrorMessage(err),
    });

    return NextResponse.json(
      {
        ok:     false,
        dry_run,
        error:  "pipeline_error",
        detail: sanitizeErrorMessage(err),
        errors,
      },
      { status: 500 }
    );
  }
}
