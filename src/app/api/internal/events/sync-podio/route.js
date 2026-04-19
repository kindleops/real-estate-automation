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
    const limit = Math.min(
      Number.isFinite(Number(searchParams.get("limit")))
        ? Number(searchParams.get("limit"))
        : 50,
      200
    );

    logger.info("podio_sync.requested", {
      method: request.method,
      limit,
      authenticated: auth.auth.authenticated,
    });

    const result = await syncSupabaseMessageEventsToPodio({ limit });

    logger.info("podio_sync.completed", {
      ...result,
      method: request.method,
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
