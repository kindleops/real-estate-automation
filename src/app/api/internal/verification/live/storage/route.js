import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import {
  getLiveStorageVerificationStatus,
  runLiveStorageVerification,
} from "@/lib/verification/live-storage.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.verification.live.storage",
});

function asBoolean(value, fallback = false) {
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
    const action = String(body?.action || "").trim().toLowerCase() || "status";

    const result =
      action === "verify"
        ? await runLiveStorageVerification({
            note: body?.note,
            confirm_live: asBoolean(body?.confirm_live, false),
            verify_signed_url: asBoolean(body?.verify_signed_url, true),
          })
        : getLiveStorageVerificationStatus();

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        action,
        result,
      },
      {
        status: result?.ok === false ? 400 : 200,
      }
    );
  } catch (error) {
    logger.error("verification.live_storage_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "verification_live_storage_failed",
      },
      { status: 500 }
    );
  }
}
