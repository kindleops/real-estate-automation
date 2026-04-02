import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import {
  getLiveTextgridVerificationStatus,
  runLiveTextgridSendVerification,
} from "@/lib/verification/live-textgrid.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.verification.live.textgrid",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
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
    const action = clean(body?.action).toLowerCase() || "status";

    const result =
      action === "send"
        ? await runLiveTextgridSendVerification({
            to: body?.to,
            from: body?.from,
            body: body?.body,
            note: body?.note,
            confirm_live: asBoolean(body?.confirm_live, false),
          })
        : await getLiveTextgridVerificationStatus({
            run_id: body?.run_id,
            provider_message_id: body?.provider_message_id,
          });

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
    logger.error("verification.live_textgrid_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "verification_live_textgrid_failed",
      },
      { status: 500 }
    );
  }
}
