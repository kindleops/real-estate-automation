import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { getVerificationReadiness } from "@/lib/verification/readiness.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.verification.readiness",
});

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = String(value ?? "").trim().toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const perform_live = asBoolean(searchParams.get("perform_live"), false);
    const result = await getVerificationReadiness({ perform_live });

    return NextResponse.json(result, {
      status: result.ok ? 200 : 400,
    });
  } catch (error) {
    logger.error("verification.readiness_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "verification_readiness_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const perform_live = asBoolean(body?.perform_live, false);
    const result = await getVerificationReadiness({ perform_live });

    return NextResponse.json(result, {
      status: result.ok ? 200 : 400,
    });
  } catch (error) {
    logger.error("verification.readiness_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "verification_readiness_failed",
      },
      { status: 500 }
    );
  }
}
