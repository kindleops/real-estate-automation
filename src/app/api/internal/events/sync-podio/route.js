import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import {
  buildPodioCooldownSkipResult,
  isPodioRateLimitError,
  serializePodioError,
} from "@/lib/providers/podio.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { syncSupabaseMessageEventsToPodio } from "@/lib/domain/events/sync-supabase-message-events-to-podio.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

const logger = child({
  module: "api.internal.events.sync-podio",
});

/**
 * Accepts auth via either:
 *   - x-internal-api-secret header (INTERNAL_API_SECRET)
 *   - Authorization: Bearer <CRON_SECRET>  (standard Vercel cron token)
 *
 * Both are checked so operations teams can call this route manually AND
 * Vercel cron can invoke it on schedule.
 */
function requireAuth(request) {
  // Prefer cron-style Bearer check first (covers Vercel scheduler).
  const cron = requireCronAuth(request, logger);
  if (cron.authorized) return cron;

  // Fallback: internal API secret header.
  const internal = requireSharedSecretAuth(request, logger, {
    env_name: "INTERNAL_API_SECRET",
    header_names: ["x-internal-api-secret"],
  });
  return internal;
}

async function handle(request) {
  try {
    const auth = requireAuth(request);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);

    // For POST requests also read limit from JSON body as fallback.
    let body_limit = null;
    if (request.method === "POST") {
      try {
        const body = await request.clone().json();
        if (body?.limit != null) body_limit = Number(body.limit);
      } catch (_) {
        // not JSON or empty body — ignore
      }
    }

    // searchParams.get("limit") returns null when absent.
    // Number(null) === 0, which is falsy for our purposes, so we must check > 0.
    const raw_limit = searchParams.get("limit") != null
      ? Number(searchParams.get("limit"))
      : body_limit;
    const limit = Math.min(
      Number.isFinite(raw_limit) && raw_limit > 0 ? raw_limit : 50,
      100
    );

    logger.info("podio_sync.requested", {
      method: request.method,
      limit,
      authenticated: auth.auth.authenticated,
    });

    const result = await syncSupabaseMessageEventsToPodio({ limit });

    logger.info("podio_sync.completed", {
      loaded_count:             result.loaded_count,
      synced_count:             result.synced_count,
      failed_count:             result.failed_count,
      skipped_count:            result.skipped_count,
      total:                    result.total,
      first_10_event_keys:      result.first_10_event_keys,
      first_10_failed_errors:   result.first_10_failed_errors,
      first_10_skipped_reasons: result.first_10_skipped_reasons,
      method: request.method,
    });

    await notifyDiscordOps({
      event_type: result?.failed_count > 0 ? "podio_sync_failed" : "podio_sync_success",
      severity: result?.failed_count > 0 ? "warning" : "success",
      domain: "podio",
      title: result?.failed_count > 0 ? "Podio Sync Completed With Failures" : "Podio Sync Completed",
      summary: `loaded=${result.loaded_count || 0}, synced=${result.synced_count || 0}, failed=${result.failed_count || 0}`,
      fields: [
        { name: "Loaded", value: String(result.loaded_count || 0), inline: true },
        { name: "Synced", value: String(result.synced_count || 0), inline: true },
        { name: "Failed", value: String(result.failed_count || 0), inline: true },
      ],
      metadata: { method: request.method, limit },
      should_alert_critical: (result?.failed_count || 0) > 0,
    });

    return NextResponse.json({ ok: true, ...result }, { status: 200 });
  } catch (err) {
    if (isPodioRateLimitError(err)) {
      logger.warn("podio_sync.rate_limited", {
        error: serializePodioError(err),
      });
      return NextResponse.json(buildPodioCooldownSkipResult(err), {
        status: 429,
      });
    }

    logger.error("podio_sync.error", { error: serializePodioError(err) });

    captureRouteException(err, {
      route: "internal/events/sync-podio",
      subsystem: "podio_sync",
    });

    await notifyDiscordOps({
      event_type: "podio_sync_failed",
      severity: "critical",
      domain: "podio",
      title: "Podio Sync Failed",
      summary: err?.message || "podio_sync_failed",
      metadata: { error: serializePodioError(err) },
      should_alert_critical: true,
    });

    return NextResponse.json(
      { ok: false, error: err.message },
      { status: 500 }
    );
  }
}

export async function GET(request) {
  return handle(request);
}

export async function POST(request) {
  return handle(request);
}
