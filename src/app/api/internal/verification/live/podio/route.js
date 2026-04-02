import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { runLivePodioRoundtripVerification } from "@/lib/verification/live-podio.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.verification.live.podio",
});

function asBoolean(value, fallback = true) {
  if (typeof value === "boolean") return value;
  const normalized = String(value ?? "").trim().toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const result = await runLivePodioRoundtripVerification({
      note: body?.note,
      delete_after: asBoolean(body?.delete_after, true),
      confirm_live: asBoolean(body?.confirm_live, false),
    });

    return NextResponse.json(result, {
      status: result.ok ? 200 : 400,
    });
  } catch (error) {
    logger.error("verification.live_podio_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "verification_live_podio_failed",
      },
      { status: 500 }
    );
  }
}
