import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { forceReleaseStaleLock } from "@/lib/domain/runs/run-locks.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.runs.release-lock",
});

function clean(value) {
  return String(value ?? "").trim();
}

async function parseScope(request) {
  const method = request.method?.toUpperCase();

  // GET: scope comes from query string
  if (method === "GET") {
    const { searchParams } = new URL(request.url);
    return clean(searchParams.get("scope"));
  }

  // POST: scope comes from JSON body
  try {
    const body = await request.json();
    return clean(body?.scope);
  } catch {
    return null;
  }
}

async function handle(request) {
  const auth = requireSharedSecretAuth(request, logger, {
    env_name: "INTERNAL_API_SECRET",
    header_names: ["x-internal-api-secret"],
  });
  if (!auth.authorized) return auth.response;

  const scope = await parseScope(request);

  if (!scope) {
    logger.warn("run_lock.release.missing_scope", { method: request.method });
    return NextResponse.json(
      { ok: false, error: "missing_scope", released: false },
      { status: 400 }
    );
  }

  logger.info("RUN LOCK RELEASE STARTED", {
    scope,
    method: request.method,
    authenticated: auth.auth.authenticated,
  });

  const result = await forceReleaseStaleLock({
    scope,
    reason: "manual_release_via_api",
  });

  if (result.released) {
    logger.info("RUN LOCK RELEASED", {
      scope: result.scope,
      record_item_id: result.record_item_id,
      was_active: result.was_active,
      previous_owner: result.previous_owner,
      previous_expires_at: result.previous_expires_at,
    });
  } else {
    logger.info("RUN LOCK RELEASE SKIPPED", {
      scope: result.scope,
      reason: result.reason,
    });
  }

  return NextResponse.json(
    {
      ok: result.ok,
      released: result.released,
      reason: result.reason,
      scope: result.scope,
      record_item_id: result.record_item_id,
      was_active: result.was_active ?? null,
      previous_owner: result.previous_owner ?? null,
      previous_expires_at: result.previous_expires_at ?? null,
    },
    { status: result.ok ? 200 : 500 }
  );
}

export async function GET(request) {
  return handle(request);
}

export async function POST(request) {
  return handle(request);
}
