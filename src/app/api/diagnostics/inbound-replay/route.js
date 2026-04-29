import { NextResponse } from "next/server";
import { handleTextgridInboundWebhook } from "@/lib/flows/handle-textgrid-inbound.js";
import { supabase } from "@/lib/supabase/client.js";
import {
  normalizeSellerInboundIntent,
  resolveNextSellerStage,
  resolveAutoReplyUseCase,
  shouldSuppressSellerAutoReply,
  resolveSellerAutoReplyPlan,
} from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";
import {
  resolveSafetyTier,
  SELLER_FLOW_SAFETY_TIERS,
  SELLER_FLOW_SAFETY_POLICY,
} from "@/lib/domain/seller-flow/seller-flow-safety-policy.js";

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (value === null || value === undefined) return fallback;
  const s = String(value).toLowerCase().trim();
  if (["true", "1", "yes", "on"].includes(s)) return true;
  if (["false", "0", "no", "off"].includes(s)) return false;
  return fallback;
}

function clean(v) { return String(v ?? "").trim(); }

function verifyAuth(request) {
  const secret = process.env.INTERNAL_API_SECRET;
  if (!secret) return true; // No secret configured = dev mode
  const auth = request.headers.get("x-api-secret") || request.headers.get("authorization")?.replace("Bearer ", "");
  return auth === secret;
}

/**
 * Lightweight classification-only diagnostic.
 * Does NOT run the full inbound handler — just the planner functions.
 */
function classifyOnly({ body, from, current_stage, auto_reply_enabled = true }) {
  const input = {
    message_body: body,
    current_stage: current_stage || null,
    auto_reply_enabled,
    conversation_context: { found: Boolean(current_stage) },
  };

  const intent = normalizeSellerInboundIntent(input);
  const next_stage = resolveNextSellerStage(input);
  const selected_use_case = resolveAutoReplyUseCase(input);
  const suppression = shouldSuppressSellerAutoReply(input);

  const plan_stub = {
    current_stage: current_stage || null,
    inbound_intent: intent,
    should_queue_reply: !suppression.suppress && !!selected_use_case,
  };

  const safety_tier = resolveSafetyTier(plan_stub, auto_reply_enabled);

  // Find matching policy entry
  const policy_match =
    SELLER_FLOW_SAFETY_POLICY[current_stage]?.[intent] ||
    SELLER_FLOW_SAFETY_POLICY.global[intent] ||
    null;

  return {
    matched: true,
    matched_outbound_queue_row: null,
    classification: null,
    detected_intent: intent,
    current_stage: current_stage || null,
    next_stage,
    selected_use_case,
    selected_template: null,
    rendered_reply_preview: "",
    would_queue_reply: plan_stub.should_queue_reply,
    suppression_reason: suppression.reason,
    auto_send_eligible: safety_tier === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND,
    brain_created: false,
    brain_id: null,
    discord_notification_count_expectation: 1,
    safety_tier,
    policy_match: policy_match
      ? {
          next_stage: policy_match.next_stage,
          template: policy_match.template,
          safety: policy_match.safety,
        }
      : null,
    routing_consistent: policy_match ? policy_match.next_stage === next_stage : true,
    mode: "classify_only",
  };
}

/**
 * Full diagnostic replay through the actual inbound handler.
 */
function extractDiagnostics(result) {
  return {
    matched: result.ok && !result.unknown_router,
    matched_outbound_queue_row: result.context?.recent?.outbound_match || null,
    classification: result.classification || null,
    detected_intent: result.seller_stage_reply?.plan?.inbound_intent || null,
    current_stage: result.seller_stage_reply?.plan?.current_stage || null,
    next_stage: result.seller_stage_reply?.plan?.next_stage || null,
    selected_use_case: result.seller_stage_reply?.plan?.selected_use_case || null,
    selected_template: result.seller_stage_reply?.queue_result?.template_id || null,
    rendered_reply_preview: result.seller_stage_reply?.queue_result?.rendered_message_text || "",
    would_queue_reply: result.seller_stage_reply?.plan?.should_queue_reply || false,
    suppression_reason: result.seller_stage_reply?.plan?.suppression_reason || null,
    auto_send_eligible: result.seller_stage_reply?.plan?.auto_send_eligible || false,
    brain_created: result.context?.created_brain_during_load || false,
    brain_id: result.context?.ids?.brain_item_id || null,
    discord_notification_count_expectation: 1,
    safety_tier: result.seller_stage_reply?.plan?.safety_tier || "review",
    mode: "full_replay",
  };
}

/**
 * GET /api/diagnostics/inbound-replay
 *
 * Single message diagnostic.
 * Query params:
 *   ?message_id=SM-xxx    — replay a real message from DB
 *   ?from=+1xxx&body=text — synthetic test
 *   ?mode=classify_only   — skip full handler, just run planner
 *   ?current_stage=xxx    — set current stage context for classify_only
 *   ?dry_run=true         — (default: true) prevent DB writes in full replay
 */
export async function GET(request) {
  if (!verifyAuth(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  const message_id = searchParams.get("message_id");
  const from = searchParams.get("from");
  const body = searchParams.get("body");
  const mode = searchParams.get("mode") || "full_replay";
  const current_stage = searchParams.get("current_stage");
  const dry_run = asBoolean(searchParams.get("dry_run"), true);

  if (!message_id && (!from || !body)) {
    return NextResponse.json(
      { error: "Provide message_id OR from + body" },
      { status: 400 }
    );
  }

  // Fast path: classify_only skips the full handler
  if (mode === "classify_only" && body) {
    const diagnostics = classifyOnly({
      body,
      from: from || "+10000000000",
      current_stage,
      auto_reply_enabled: true,
    });
    return NextResponse.json({ ok: true, diagnostics });
  }

  let payload = {};

  if (message_id) {
    const { data: event, error } = await supabase
      .from("message_events")
      .select("*")
      .eq("provider_message_id", message_id)
      .maybeSingle();

    if (error || !event) {
      return NextResponse.json(
        { error: "Message not found in DB", detail: error?.message },
        { status: 404 }
      );
    }

    payload = {
      message_id: event.provider_message_id,
      from: event.inbound_from,
      to: event.inbound_to,
      body: event.message_body,
      status: "received",
    };
  } else {
    payload = {
      message_id: `diag-${Date.now()}`,
      from,
      to: searchParams.get("to") || "+14693131600",
      body,
      status: "received",
    };
  }

  try {
    const result = await handleTextgridInboundWebhook(payload, {
      dry_run,
      auto_reply_enabled: true,
      auto_post_discord_card: false,
      inbound_user_initiated: true,
    });

    return NextResponse.json({ ok: true, diagnostics: extractDiagnostics(result) });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: "Diagnostic run failed", detail: error.message, stack: error.stack },
      { status: 500 }
    );
  }
}

/**
 * POST /api/diagnostics/inbound-replay
 *
 * Batch replay: test multiple inbound examples in one request.
 *
 * Body:
 * {
 *   "mode": "classify_only" | "full_replay",
 *   "examples": [
 *     { "body": "yes I own it", "from": "+15550001234", "current_stage": "ownership_check" },
 *     { "body": "stop texting me", "from": "+15550005678" },
 *     { "message_id": "SM-real-id-from-db" }
 *   ]
 * }
 */
export async function POST(request) {
  if (!verifyAuth(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  let input;
  try {
    input = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const { examples = [], mode = "classify_only" } = input;

  if (!Array.isArray(examples) || examples.length === 0) {
    return NextResponse.json(
      { error: "Provide an 'examples' array with at least one entry" },
      { status: 400 }
    );
  }

  if (examples.length > 50) {
    return NextResponse.json(
      { error: "Maximum 50 examples per batch" },
      { status: 400 }
    );
  }

  const results = [];

  for (const example of examples) {
    const ex_body = clean(example.body);
    const ex_from = clean(example.from) || "+10000000000";
    const ex_current_stage = clean(example.current_stage) || null;
    const ex_message_id = clean(example.message_id) || null;

    try {
      if (mode === "classify_only" && ex_body) {
        results.push({
          input: { body: ex_body, from: ex_from, current_stage: ex_current_stage },
          diagnostics: classifyOnly({
            body: ex_body,
            from: ex_from,
            current_stage: ex_current_stage,
            auto_reply_enabled: true,
          }),
        });
        continue;
      }

      // Full replay mode
      let payload;

      if (ex_message_id) {
        const { data: event, error } = await supabase
          .from("message_events")
          .select("*")
          .eq("provider_message_id", ex_message_id)
          .maybeSingle();

        if (error || !event) {
          results.push({
            input: { message_id: ex_message_id },
            error: "Message not found",
            detail: error?.message,
          });
          continue;
        }

        payload = {
          message_id: event.provider_message_id,
          from: event.inbound_from,
          to: event.inbound_to,
          body: event.message_body,
          status: "received",
        };
      } else if (ex_body) {
        payload = {
          message_id: `diag-batch-${Date.now()}-${results.length}`,
          from: ex_from,
          to: clean(example.to) || "+14693131600",
          body: ex_body,
          status: "received",
        };
      } else {
        results.push({
          input: example,
          error: "Each example needs body or message_id",
        });
        continue;
      }

      const result = await handleTextgridInboundWebhook(payload, {
        dry_run: true,
        auto_reply_enabled: true,
        auto_post_discord_card: false,
        inbound_user_initiated: true,
      });

      results.push({
        input: { body: payload.body, from: payload.from },
        diagnostics: extractDiagnostics(result),
      });
    } catch (error) {
      results.push({
        input: example,
        error: error.message,
      });
    }
  }

  // Summary stats
  const summary = {
    total: results.length,
    matched: results.filter((r) => r.diagnostics?.matched).length,
    unmatched: results.filter((r) => r.diagnostics && !r.diagnostics.matched).length,
    errors: results.filter((r) => r.error).length,
    by_safety_tier: {
      auto_send: results.filter((r) => r.diagnostics?.safety_tier === "auto_send").length,
      review: results.filter((r) => r.diagnostics?.safety_tier === "review").length,
      suppress: results.filter((r) => r.diagnostics?.safety_tier === "suppress").length,
    },
    by_intent: {},
  };

  for (const r of results) {
    const intent = r.diagnostics?.detected_intent || "error";
    summary.by_intent[intent] = (summary.by_intent[intent] || 0) + 1;
  }

  return NextResponse.json({ ok: true, summary, results });
}
