import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { handleTextgridInboundWebhook } from "@/lib/flows/handle-textgrid-inbound.js";
import { handleTextgridDeliveryWebhook } from "@/lib/flows/handle-textgrid-delivery.js";
import { handleDocusignWebhook } from "@/lib/domain/contracts/handle-docusign-webhook.js";
import { handleTitleResponseWebhook } from "@/lib/domain/title/handle-title-response-webhook.js";
import { handleClosingResponseWebhook } from "@/lib/domain/closings/handle-closing-response-webhook.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.verification.replay",
});

const FLOW_HANDLERS = {
  textgrid_inbound: handleTextgridInboundWebhook,
  textgrid_delivery: handleTextgridDeliveryWebhook,
  docusign: handleDocusignWebhook,
  title: handleTitleResponseWebhook,
  closings: handleClosingResponseWebhook,
};

function clean(value) {
  return String(value ?? "").trim();
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asAttempts(value, fallback = 2) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(10, Math.trunc(parsed)));
}

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const flow = clean(body?.flow).toLowerCase();
    const attempts = asAttempts(body?.attempts, 2);
    const payload = asObject(body?.payload);
    const handler = FLOW_HANDLERS[flow];

    if (!handler) {
      return NextResponse.json(
        {
          ok: false,
          error: "unsupported_verification_flow",
          supported_flows: Object.keys(FLOW_HANDLERS),
        },
        { status: 400 }
      );
    }

    logger.info("verification.replay_requested", {
      flow,
      attempts,
    });

    const results = [];

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        const result = await handler(payload);
        results.push({
          attempt,
          ok: result?.ok !== false,
          duplicate: Boolean(result?.duplicate),
          result,
        });
      } catch (error) {
        results.push({
          attempt,
          ok: false,
          duplicate: false,
          error: error?.message || "verification_replay_failed",
        });
      }
    }

    const response = {
      ok: results.every((entry) => entry.ok),
      flow,
      attempts,
      results,
    };

    return NextResponse.json(response, {
      status: statusForResult(response),
    });
  } catch (error) {
    logger.error("verification.replay_failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "verification_replay_failed",
      },
      { status: 500 }
    );
  }
}
