import { runSupabaseCandidateFeeder } from "@/lib/domain/outbound/supabase-candidate-feeder.js";
import { child } from "@/lib/logging/logger.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";

const logger = child({ module: "domain.outbound.feed_candidates_request" });

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

function asPositiveInteger(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

export function normalizeFeedCandidatesInput(input = {}) {
  return {
    limit: asPositiveInteger(input.limit, 25),
    scan_limit: asPositiveInteger(input.scan_limit, 500),
    candidate_source: clean(input.candidate_source) || null,
    market: clean(input.market) || null,
    state: clean(input.state) || null,
    routing_safe_only: asBoolean(input.routing_safe_only, true),
    within_contact_window_now: asBoolean(input.within_contact_window_now, true),
    dry_run: asBoolean(input.dry_run, false),
    template_use_case: clean(input.template_use_case) || "ownership_check",
    touch_number: asPositiveInteger(input.touch_number, 1),
    campaign_session_id: clean(input.campaign_session_id) || null,
    debug_templates: asBoolean(input.debug_templates, false),
  };
}

function mergeBodyAndQuery(request, method, body = {}) {
  const merged = { ...(body || {}) };
  const search_params = new URL(request.url).searchParams;

  for (const key of [
    "limit",
    "scan_limit",
    "candidate_source",
    "market",
    "state",
    "routing_safe_only",
    "within_contact_window_now",
    "dry_run",
    "template_use_case",
    "touch_number",
    "campaign_session_id",
    "debug_templates",
  ]) {
    const value = search_params.get(key);
    if (value !== null) merged[key] = value;
  }

  if (method === "GET") {
    return merged;
  }

  return merged;
}

export async function handleFeedCandidatesRequest(request, method = "GET", options = {}) {
  const route = clean(options.route) || "internal/outbound/feed-candidates";
  const route_logger = options.logger || logger;
  const json_response = options.jsonResponse || ((payload, init = {}) => Response.json(payload, init));

  try {
    const auth = requireCronAuth(request, route_logger);
    if (!auth.authorized) return auth.response;

    const body = method === "POST" ? await request.json().catch(() => ({})) : {};
    const normalized = normalizeFeedCandidatesInput(mergeBodyAndQuery(request, method, body));

    const diagnostics = await runSupabaseCandidateFeeder(normalized, options.deps || {});

    return json_response(
      {
        ok: diagnostics.ok !== false,
        route,
        loaded_count: diagnostics.scanned_count,
        eligible_count: diagnostics.eligible_count,
        inserted_count: diagnostics.queued_count,
        ...diagnostics,
      },
      { status: diagnostics.ok === false ? 400 : 200 }
    );
  } catch (error) {
    captureRouteException(error, {
      route,
      subsystem: "outbound_feeder",
      context: { method },
    });

    return json_response(
      {
        ok: false,
        route,
        error: "feed_candidates_failed",
        message: error?.message || "feed_candidates_failed",
      },
      { status: 500 }
    );
  }
}
