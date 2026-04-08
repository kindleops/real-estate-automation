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

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

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

export async function GET(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

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

    const result = await runFeederWithRollout(options, { logger });

    logger.info("master_owner_feeder.route_completed", {
      method: "GET",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      queued_count: result?.queued_count ?? 0,
      scanned_count: result?.scanned_count ?? 0,
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("master_owner_feeder.failed", {
      method: "GET",
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
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const options = normalizeFeederRequest(body);

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

    const result = await runFeederWithRollout(options, { logger });

    logger.info("master_owner_feeder.route_completed", {
      method: "POST",
      ok: result?.ok !== false,
      skipped: result?.skipped || false,
      reason: result?.reason || null,
      queued_count: result?.queued_count ?? 0,
      scanned_count: result?.scanned_count ?? 0,
      retry_after_seconds: result?.retry_after_seconds ?? null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    const diagnostics = serializePodioError(error);

    logger.error("master_owner_feeder.failed", {
      method: "POST",
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
