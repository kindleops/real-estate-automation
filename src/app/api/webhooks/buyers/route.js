import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleBuyerResponseWebhook } from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.buyers",
});

function clean(value) {
  return String(value ?? "").trim();
}

async function parseRequestBody(request) {
  const contentType = clean(request.headers.get("content-type")).toLowerCase();

  if (contentType.includes("application/json")) {
    return request.json().catch(() => ({}));
  }

  if (
    contentType.includes("application/x-www-form-urlencoded") ||
    contentType.includes("multipart/form-data")
  ) {
    const form = await request.formData().catch(() => null);
    return form ? Object.fromEntries(form.entries()) : {};
  }

  const text = await request.text().catch(() => "");
  return { raw_text: text };
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/buyers",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "BUYER_WEBHOOK_SECRET",
      header_names: ["x-buyer-webhook-secret"],
    });
    if (!auth.authorized) return auth.response;

    const payload = await parseRequestBody(request);
    const result = await handleBuyerResponseWebhook(payload);

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "webhooks/buyers",
        result,
      },
      { status: result?.ok === false ? 400 : 200 }
    );
  } catch (error) {
    logger.error("buyers_webhook.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "buyers_webhook_failed",
      },
      { status: 500 }
    );
  }
}
