import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { getPodioRetryAfterSeconds, isPodioRateLimitError } from "@/lib/providers/podio.js";
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
  if (result?.reason === "master_owner_feeder_rate_limited") return 429;
  return result?.ok === false ? 400 : 200;
}

function buildRateLimitResponse(error) {
  const retry_after_seconds = getPodioRetryAfterSeconds(error, null);
  const retry_after_at =
    Number.isFinite(retry_after_seconds) && retry_after_seconds > 0
      ? new Date(Date.now() + retry_after_seconds * 1000).toISOString()
      : null;

  return NextResponse.json(
    {
      ok: false,
      error: "master_owner_feeder_rate_limited",
      retry_after_seconds,
      retry_after_at,
    },
    { status: 429 }
  );
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

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("master_owner_feeder.failed", { error });

    if (isPodioRateLimitError(error)) {
      return buildRateLimitResponse(error);
    }

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_failed",
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

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/feed-master-owners",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("master_owner_feeder.failed", { error });

    if (isPodioRateLimitError(error)) {
      return buildRateLimitResponse(error);
    }

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_failed",
      },
      { status: 500 }
    );
  }
}
