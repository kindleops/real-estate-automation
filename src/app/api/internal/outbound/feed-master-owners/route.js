import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import {
  buildPodioCooldownSkipResult,
  isPodioRateLimitError,
  serializePodioError,
} from "@/lib/providers/podio.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import {
  normalizeFeederRequest,
  runFeederWithRollout,
} from "@/lib/domain/master-owners/feed-master-owners-request.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";
import { captureSystemEvent } from "@/lib/analytics/posthog-server.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

const logger = child({
  module: "api.internal.outbound.feed_master_owners",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value, fallback = null) {
  const normalized = clean(value);
  if (!normalized) return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

function buildFeederSummary(result = {}) {
  const results = Array.isArray(result?.results) ? result.results : [];

  return {
    loaded_count:
      Number(result?.raw_items_pulled ?? result?.raw_scanned_count ?? result?.scanned_count) ||
      0,
    eligible_count: Number(result?.eligible_owner_count) || 0,
    inserted_count: Number(result?.queued_count) || 0,
    duplicate_count:
      (Number(result?.duplicate_skip_count) || 0) +
      (Number(result?.queue_create_duplicate_cancel_count) || 0),
    skipped_count: Number(result?.skipped_count) || 0,
    error_count: results.filter((entry) => entry?.ok === false && !entry?.skipped).length,
  };
}

/**
 * Determine the human-readable reason feeder inserted zero rows.
 * Returns null when rows were inserted.
 */
function buildZeroInsertReason(result = {}, summary = {}) {
  if (summary.inserted_count > 0) return null;
  if (result?.skipped) return result.reason || "feeder_run_skipped";
  if (summary.loaded_count === 0) return "no_source_items";
  if (summary.eligible_count === 0) {
    // Check skip_reason_counts for clues
    const reasons = Array.isArray(result?.skip_reason_counts) ? result.skip_reason_counts : [];
    const reason_keys = reasons.map((r) => String(r?.reason || r || ""));
    if (reason_keys.some((r) => r.includes("dnc") || r.includes("opt_out"))) return "all_dnc";
    if (reason_keys.some((r) => r.includes("phone"))) return "all_missing_phone";
    if (reason_keys.some((r) => r.includes("window"))) return "outside_contact_window";
    if (reason_keys.some((r) => r.includes("from") || r.includes("number"))) return "all_missing_from_number";
    return "no_eligible_items";
  }
  if (summary.duplicate_count > 0 && summary.error_count === 0) return "all_duplicates";
  if (summary.error_count > 0 && summary.duplicate_count === 0) {
    const failures = Array.isArray(result?.results) ? result.results : [];
    const fail_reasons = failures.filter((r) => r?.ok === false && !r?.skipped).map((r) => String(r?.reason || ""));
    if (fail_reasons.some((r) => r.includes("template"))) return "all_template_resolution_failed";
    if (fail_reasons.some((r) => r.includes("supabase") || r.includes("insert"))) return "all_supabase_insert_failed";
    return "all_errors";
  }
  return "unknown_no_insert_reason";
}

export async function GET(request) {
  let feeder_stage = "auth";
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    feeder_stage = "request_parse";
    const { searchParams } = new URL(request.url);
    const options = normalizeFeederRequest({
      limit: searchParams.get("limit"),
      scan_limit: searchParams.get("scan_limit"),
      dry_run: searchParams.get("dry_run"),
      test_mode: searchParams.get("test_mode"),
      seller_id: searchParams.get("seller_id"),
      master_owner_id: searchParams.get("master_owner_id"),
      source_view_id: searchParams.get("source_view_id"),
      source_view_name: searchParams.get("source_view_name"),
    });

    logger.info("master_owner_feeder.requested", {
      method: "GET",
      limit: options.limit,
      scan_limit: options.scan_limit,
      dry_run: options.dry_run,
      test_mode: options.test_mode,
      seller_id: options.seller_id || null,
      master_owner_id: asNumber(options.master_owner_id, null),
      source_view_id: options.source_view_id || null,
      source_view_name: options.source_view_name || null,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    feeder_stage = "execute";
    const result = await runFeederWithRollout(options, { logger });
    feeder_stage = "log_result";

    if (result?.skipped) {
      captureSystemEvent("feeder_run_skipped", {
        method: "GET",
        reason: result.reason || null,
        dry_run: result.dry_run ?? false,
        retry_after_seconds: result.retry_after_seconds ?? null,
      });
    } else {
      const summary = buildFeederSummary(result);
      captureSystemEvent("feeder_run_completed", {
        method: "GET",
        ok: result?.ok !== false,
        dry_run: result?.dry_run ?? false,
        loaded_count: summary.loaded_count,
        eligible_count: summary.eligible_count,
        inserted_count: summary.inserted_count,
        duplicate_count: summary.duplicate_count,
        skipped_count: summary.skipped_count,
        error_count: summary.error_count,
      });
    }

    const get_summary = buildFeederSummary(result);
    const get_effective_limit = result?.rollout?.effective_limit ?? options.limit ?? null;
    const get_effective_scan_limit = result?.rollout?.effective_scan_limit ?? options.scan_limit ?? null;
    const get_effective_dry_run = result?.dry_run ?? options.dry_run ?? false;
    const get_zero_insert_reason = buildZeroInsertReason(result, get_summary);

    logger.info("master_owner_feeder.route_completed", {
      method: "GET",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      effective_limit: get_effective_limit,
      effective_scan_limit: get_effective_scan_limit,
      effective_dry_run: get_effective_dry_run,
      loaded_count: get_summary.loaded_count,
      eligible_count: get_summary.eligible_count,
      inserted_count: get_summary.inserted_count,
      duplicate_count: get_summary.duplicate_count,
      skipped_count: get_summary.skipped_count,
      error_count: get_summary.error_count,
      zero_insert_reason: get_zero_insert_reason,
      first_10_skip_reasons: (result?.skip_reason_counts ?? []).slice(0, 10),
      first_10_errors: (Array.isArray(result?.results) ? result.results : [])
        .filter((r) => r?.ok === false && !r?.skipped)
        .slice(0, 10)
        .map((r) => ({ reason: r?.reason || "unknown", master_owner_id: r?.plan?.master_owner_id ?? r?.owner?.item_id ?? null })),
      template_resolution_summary: result?.template_resolution_diagnostics ?? null,
      supabase_insert_summary: {
        attempted: result?.queue_create_attempt_count ?? null,
        succeeded: result?.queue_create_success_count ?? null,
        duplicate_canceled: result?.queue_create_duplicate_cancel_count ?? null,
      },
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        effective_limit: get_effective_limit,
        effective_scan_limit: get_effective_scan_limit,
        effective_dry_run: get_effective_dry_run,
        zero_insert_reason: get_zero_insert_reason,
        result: {
          ...result,
          ...get_summary,
        },
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("master_owner_feeder.failed", {
      method: "GET",
      feeder_stage,
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await buildPodioCooldownSkipResult({
        scanned_count: 0,
        raw_items_pulled: 0,
        eligible_owner_count: 0,
        queued_count: 0,
        skipped_count: 0,
        skip_reason_counts: [],
        template_resolution_diagnostics: null,
        results: [],
      });

      return NextResponse.json(
        {
          ok: true,
          route: "internal/outbound/feed-master-owners",
          result,
        },
        { status: 200 }
      );
    }

    captureRouteException(error, {
      route: "internal/outbound/feed-master-owners",
      subsystem: "feeder",
      context: { method: "GET" },
    });

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_failed",
        message: diagnostics.message,
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  let feeder_stage = "auth";
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    feeder_stage = "request_parse";
    const body = await request.json().catch(() => ({}));

    // Query params override body values to allow precise manual testing:
    //   POST /feed-master-owners?limit=1&scan_limit=10&dry_run=false
    const { searchParams } = new URL(request.url);
    const merged_input = { ...body };
    for (const key of [
      "limit", "scan_limit", "dry_run", "test_mode",
      "seller_id", "master_owner_id", "source_view_id", "source_view_name",
    ]) {
      const val = searchParams.get(key);
      if (val !== null) merged_input[key] = val;
    }

    const options = normalizeFeederRequest(merged_input);

    logger.info("master_owner_feeder.requested", {
      method: "POST",
      limit: options.limit,
      scan_limit: options.scan_limit,
      dry_run: options.dry_run,
      test_mode: options.test_mode,
      seller_id: options.seller_id || null,
      master_owner_id: asNumber(options.master_owner_id, null),
      source_view_id: options.source_view_id || null,
      source_view_name: options.source_view_name || null,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    feeder_stage = "execute";
    const result = await runFeederWithRollout(options, { logger });
    feeder_stage = "log_result";

    if (result?.skipped) {
      captureSystemEvent("feeder_run_skipped", {
        method: "POST",
        reason: result.reason || null,
        dry_run: result.dry_run ?? false,
        retry_after_seconds: result.retry_after_seconds ?? null,
      });
    } else {
      const summary = buildFeederSummary(result);
      captureSystemEvent("feeder_run_completed", {
        method: "POST",
        ok: result?.ok !== false,
        dry_run: result?.dry_run ?? false,
        loaded_count: summary.loaded_count,
        eligible_count: summary.eligible_count,
        inserted_count: summary.inserted_count,
        duplicate_count: summary.duplicate_count,
        skipped_count: summary.skipped_count,
        error_count: summary.error_count,
      });
    }

    const post_summary = buildFeederSummary(result);
    const post_effective_limit = result?.rollout?.effective_limit ?? options.limit ?? null;
    const post_effective_scan_limit = result?.rollout?.effective_scan_limit ?? options.scan_limit ?? null;
    const post_effective_dry_run = result?.dry_run ?? options.dry_run ?? false;
    const post_zero_insert_reason = buildZeroInsertReason(result, post_summary);

    logger.info("master_owner_feeder.route_completed", {
      method: "POST",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      effective_limit: post_effective_limit,
      effective_scan_limit: post_effective_scan_limit,
      effective_dry_run: post_effective_dry_run,
      loaded_count: post_summary.loaded_count,
      eligible_count: post_summary.eligible_count,
      inserted_count: post_summary.inserted_count,
      duplicate_count: post_summary.duplicate_count,
      skipped_count: post_summary.skipped_count,
      error_count: post_summary.error_count,
      zero_insert_reason: post_zero_insert_reason,
      first_10_skip_reasons: (result?.skip_reason_counts ?? []).slice(0, 10),
      first_10_errors: (Array.isArray(result?.results) ? result.results : [])
        .filter((r) => r?.ok === false && !r?.skipped)
        .slice(0, 10)
        .map((r) => ({ reason: r?.reason || "unknown", master_owner_id: r?.plan?.master_owner_id ?? r?.owner?.item_id ?? null })),
      template_resolution_summary: result?.template_resolution_diagnostics ?? null,
      supabase_insert_summary: {
        attempted: result?.queue_create_attempt_count ?? null,
        succeeded: result?.queue_create_success_count ?? null,
        duplicate_canceled: result?.queue_create_duplicate_cancel_count ?? null,
      },
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        effective_limit: post_effective_limit,
        effective_scan_limit: post_effective_scan_limit,
        effective_dry_run: post_effective_dry_run,
        zero_insert_reason: post_zero_insert_reason,
        result: {
          ...result,
          ...post_summary,
        },
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("master_owner_feeder.failed", {
      method: "POST",
      feeder_stage,
      error: diagnostics,
    });

    if (isPodioRateLimitError(error)) {
      const result = await buildPodioCooldownSkipResult({
        scanned_count: 0,
        raw_items_pulled: 0,
        eligible_owner_count: 0,
        queued_count: 0,
        skipped_count: 0,
        skip_reason_counts: [],
        template_resolution_diagnostics: null,
        results: [],
      });

      return NextResponse.json(
        {
          ok: true,
          route: "internal/outbound/feed-master-owners",
          result,
        },
        { status: 200 }
      );
    }

    captureRouteException(error, {
      route: "internal/outbound/feed-master-owners",
      subsystem: "feeder",
      context: { method: "POST" },
    });

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_failed",
        message: diagnostics.message,
      },
      { status: 500 }
    );
  }
}
