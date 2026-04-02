import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { runLiveDocusignVerification } from "@/lib/verification/live-docusign.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.verification.live.docusign",
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
    const result = await runLiveDocusignVerification({
      action: body?.action,
      envelope_id: body?.envelope_id,
      subject: body?.subject,
      template_id: body?.template_id,
      documents: body?.documents,
      signers: body?.signers,
      email_blurb: body?.email_blurb,
      metadata: body?.metadata,
      dry_run: asBoolean(body?.dry_run, true),
      fetch_status_after_send: asBoolean(body?.fetch_status_after_send, true),
      confirm_live: asBoolean(body?.confirm_live, false),
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        result,
      },
      {
        status: result?.ok === false ? 400 : 200,
      }
    );
  } catch (error) {
    logger.error("verification.live_docusign_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "verification_live_docusign_failed",
      },
      { status: 500 }
    );
  }
}
