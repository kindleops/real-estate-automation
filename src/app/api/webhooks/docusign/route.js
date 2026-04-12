import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleDocusignWebhook } from "@/lib/domain/contracts/handle-docusign-webhook.js";
import { verifyDocusignConnectHmac } from "@/lib/security/docusign-hmac.js";
import { ENV } from "@/lib/config/env.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.docusign",
});

function clean(value) {
  return String(value ?? "").trim();
}

function isProductionRuntime() {
  return (
    String(process.env.VERCEL_ENV ?? "").toLowerCase() === "production" ||
    String(process.env.NODE_ENV ?? "").toLowerCase() === "production"
  );
}

/**
 * Returns true when HMAC verification may be skipped.
 * Only permitted in non-production Node environments when the caller has
 * explicitly set DOCUSIGN_WEBHOOK_SKIP_HMAC=1.  Off by default.
 */
function isHmacBypassAllowed() {
  if (isProductionRuntime()) return false;
  return clean(process.env.DOCUSIGN_WEBHOOK_SKIP_HMAC) === "1";
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/docusign",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    // Read raw body first — must happen before any body-consuming operation.
    const rawBody = await request.text().catch(() => "");

    const hmac_bypass = isHmacBypassAllowed();

    if (!hmac_bypass) {
      const secret = clean(ENV.DOCUSIGN_WEBHOOK_SECRET);
      const hmac = verifyDocusignConnectHmac(rawBody, request.headers, secret);

      if (!hmac.ok) {
        logger.warn("docusign_webhook.hmac_rejected", {
          reason: hmac.reason,
          // Do not log the secret or the signature value.
        });

        return NextResponse.json(
          { ok: false, error: "unauthorized", reason: hmac.reason },
          { status: 401 }
        );
      }
    } else {
      logger.warn("docusign_webhook.hmac_bypass_active", {
        note: "DOCUSIGN_WEBHOOK_SKIP_HMAC=1 — HMAC verification skipped (dev only)",
      });
    }

    let payload = {};
    try {
      payload = rawBody ? JSON.parse(rawBody) : {};
    } catch {
      payload = {};
    }

    const result = await handleDocusignWebhook(payload);

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "webhooks/docusign",
        result,
      },
      { status: result?.ok === false ? 400 : 200 }
    );
  } catch (error) {
    logger.error("docusign_webhook.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "docusign_webhook_failed",
      },
      { status: 500 }
    );
  }
}
