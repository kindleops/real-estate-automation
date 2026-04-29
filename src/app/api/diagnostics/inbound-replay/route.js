import { NextResponse } from "next/server";
import { supabase } from "@/lib/supabase/client.js";
import { loadContext } from "@/lib/domain/context/load-context.js";
import { loadContextWithFallback } from "@/lib/domain/context/load-context-with-fallback.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { personalizeTemplate } from "@/lib/sms/personalize_template.js";
import {
  resolveSellerAutoReplyPlan,
} from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";
import {
  resolveDeterministicStageTransition,
} from "@/lib/domain/seller-flow/deterministic-stage-map.js";
import { normalizeInboundReplayExampleBody } from "@/lib/diagnostics/normalize-inbound-replay-example-body.js";
import {
  buildVerificationDiagnostics as buildVerificationDiagnosticsCore,
  loadReplayPayload as loadReplayPayloadCore,
  normalizeInboundReplayMode,
} from "@/lib/diagnostics/inbound-replay-verifier.js";

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (value === null || value === undefined) return fallback;
  const s = String(value).toLowerCase().trim();
  if (["true", "1", "yes", "on"].includes(s)) return true;
  if (["false", "0", "no", "off"].includes(s)) return false;
  return fallback;
}

function clean(v) { return String(v ?? "").trim(); }

const defaultDeps = {
  supabase,
  loadContext,
  loadContextWithFallback,
  classify,
  resolveRoute,
  resolveSellerAutoReplyPlan,
  resolveDeterministicStageTransition,
  loadTemplate,
  personalizeTemplate,
};

let runtimeDeps = { ...defaultDeps };

export function __setInboundReplayTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetInboundReplayTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function normalizeMode(raw_mode = null) {
  return normalizeInboundReplayMode(raw_mode);
}

/**
 * Test hook: normalize inbound message body aliases for diagnostics/inbound-replay.
 * Internal normalization only; does not alter queueing/SMS behavior.
 */
export function __normalizeInboundReplayExampleBody(example = null) {
  return normalizeInboundReplayExampleBody(example);
}

function verifyAuth(request) {
  const secret = process.env.INTERNAL_API_SECRET;
  if (!secret) return true; // No secret configured = dev mode
  const auth = request.headers.get("x-api-secret") || request.headers.get("authorization")?.replace("Bearer ", "");
  return auth === secret;
}

async function buildVerificationDiagnostics({
  body,
  from,
  to,
  current_stage,
  auto_reply_enabled = true,
  mode = "deterministic_replay",
} = {}) {
  return buildVerificationDiagnosticsCore({
    body,
    from,
    to,
    current_stage,
    auto_reply_enabled,
    mode,
    deps: runtimeDeps,
  });
}

async function loadReplayPayload({ message_id, from, body, to }) {
  return loadReplayPayloadCore({ message_id, from, body, to, deps: runtimeDeps });
}

/**
 * GET /api/diagnostics/inbound-replay
 *
 * Single message diagnostic.
 * Query params:
 *   ?message_id=SM-xxx    — replay a real message from DB
 *   ?from=+1xxx&body=text — synthetic test
 *   ?mode=classify_only   — skip template preview, planner only
 *   ?mode=deterministic_replay — planner + no-write template preview (default)
 *   ?mode=full_replay     — blocked in production-safe verifier
 *   ?current_stage=xxx    — set current stage context for classify_only
 */
export async function GET(request) {
  if (!verifyAuth(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  const message_id = searchParams.get("message_id");
  const from = searchParams.get("from");
  const body = searchParams.get("body");
  const mode = normalizeMode(searchParams.get("mode"));
  const current_stage = searchParams.get("current_stage");
  const auto_reply_enabled = asBoolean(searchParams.get("auto_reply_enabled"), true);

  if (mode === "full_replay") {
    return NextResponse.json(
      {
        error: "full_replay_disabled",
        detail:
          "Production-safe verifier blocks full inbound handler replay to prevent writes and side effects.",
      },
      { status: 400 }
    );
  }

  if (!message_id && (!from || !body)) {
    return NextResponse.json(
      { error: "Provide message_id OR from + body" },
      { status: 400 }
    );
  }

  const payload_result = await loadReplayPayload({
    message_id,
    from,
    body,
    to: searchParams.get("to"),
  });

  if (!payload_result.ok) {
    return NextResponse.json(
      { error: payload_result.error, detail: payload_result.detail },
      { status: payload_result.status || 404 }
    );
  }

  try {
    const diagnostics = await buildVerificationDiagnostics({
      body: payload_result.payload.body,
      from: payload_result.payload.from,
      to: payload_result.payload.to,
      current_stage,
      auto_reply_enabled,
      mode,
    });

    return NextResponse.json({
      ok: true,
      replay_input: payload_result.payload,
      diagnostics,
    });
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
 *   "mode": "classify_only" | "deterministic_replay",
 *   "auto_reply_enabled": true,
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

  const { examples = [], mode: input_mode = "deterministic_replay" } = input;
  const mode = normalizeMode(input_mode);
  const auto_reply_enabled = asBoolean(input?.auto_reply_enabled, true);

  if (mode === "full_replay") {
    return NextResponse.json(
      {
        error: "full_replay_disabled",
        detail:
          "Production-safe verifier blocks full inbound handler replay to prevent writes and side effects.",
      },
      { status: 400 }
    );
  }

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
    const ex_body = normalizeInboundReplayExampleBody(example);
    const ex_from = clean(example.from) || "+10000000000";
    const ex_current_stage = clean(example.current_stage) || null;
    const ex_message_id = clean(example.message_id) || null;

    try {
      if (!ex_message_id && !ex_body) {
        results.push({
          input: example,
          error: "Each example needs body or message_id",
        });
        continue;
      }

      const payload_result = await loadReplayPayload({
        message_id: ex_message_id,
        from: ex_from,
        body: ex_body,
        to: clean(example.to) || "+14693131600",
      });

      if (!payload_result.ok) {
        results.push({
          input: { message_id: ex_message_id },
          error: payload_result.error || "Message not found",
          detail: payload_result.detail,
        });
        continue;
      }

      const diagnostics = await buildVerificationDiagnostics({
        body: payload_result.payload.body,
        from: payload_result.payload.from,
        to: payload_result.payload.to,
        current_stage: ex_current_stage,
        auto_reply_enabled,
        mode,
      });

      results.push({
        input: { body: payload_result.payload.body, from: payload_result.payload.from },
        diagnostics,
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
