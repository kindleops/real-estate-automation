import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { processEmailQueue } from "@/lib/email/process-email-queue.js";
import { buildDisabledResponse, getSystemFlag } from "@/lib/system-control.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.internal.email.queue.run" });

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["1", "true", "yes"].includes(normalized)) return true;
  if (["0", "false", "no"].includes(normalized)) return false;
  return fallback;
}

function asLimit(value, fallback = 25) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(Math.trunc(parsed), 200);
}

async function runFromPayload(payload = {}) {
  const result = await processEmailQueue({
    limit: asLimit(payload.limit, 25),
    dry_run: asBoolean(payload.dry_run, false),
  });

  return {
    ok: result?.ok !== false,
    route: "internal/email/queue/run",
    result,
  };
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const email_enabled = await getSystemFlag("email_enabled");
    if (!email_enabled) {
      return NextResponse.json(buildDisabledResponse("email_enabled", "email-queue-run-route"), {
        status: 423,
      });
    }

    const { searchParams } = new URL(request.url);
    const response = await runFromPayload({
      limit: searchParams.get("limit"),
      dry_run: searchParams.get("dry_run"),
    });

    return NextResponse.json(response, { status: response.ok ? 200 : 400 });
  } catch (error) {
    logger.error("email.queue_run.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json({ ok: false, error: "email_queue_run_failed" }, { status: 500 });
  }
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const email_enabled = await getSystemFlag("email_enabled");
    if (!email_enabled) {
      return NextResponse.json(buildDisabledResponse("email_enabled", "email-queue-run-route"), {
        status: 423,
      });
    }

    const body = await request.json().catch(() => ({}));
    const response = await runFromPayload(body || {});

    return NextResponse.json(response, { status: response.ok ? 200 : 400 });
  } catch (error) {
    logger.error("email.queue_run.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json({ ok: false, error: "email_queue_run_failed" }, { status: 500 });
  }
}
