import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleClosingResponseWebhook } from "@/lib/domain/closings/handle-closing-response-webhook.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.closings",
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
    route: "webhooks/closings",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "CLOSINGS_WEBHOOK_SECRET",
      header_names: ["x-closings-webhook-secret"],
    });
    if (!auth.authorized) return auth.response;

    const payload = await parseRequestBody(request);
    const result = await handleClosingResponseWebhook(payload);

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "webhooks/closings",
        result,
      },
      { status: result?.ok === false ? 400 : 200 }
    );
  } catch (error) {
    logger.error("closings_webhook.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "closings_webhook_failed",
      },
      { status: 500 }
    );
  }
}
