import { NextResponse } from "next/server";

import { maybeHandleBuyerTextgridInbound } from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { child } from "@/lib/logging/logger.js";
import { handleTextgridInbound } from "@/lib/flows/handle-textgrid-inbound.js";
import { verifyTextgridWebhookRequest } from "@/lib/webhooks/textgrid-verify-webhook.js";
import { normalizeTextgridInboundPayload } from "@/lib/webhooks/textgrid-inbound-normalize.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.textgrid.inbound",
});

function clean(value) {
  return String(value ?? "").trim();
}

async function parseRequestBody(request) {
  const contentType = clean(request.headers.get("content-type")).toLowerCase();

  if (contentType.includes("application/json")) {
    return await request.json().catch(() => ({}));
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

export const __normalizeInboundPayloadForTest = normalizeTextgridInboundPayload;

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/textgrid/inbound",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    const raw_body = await request.clone().text().catch(() => "");
    const content_type = clean(request.headers.get("content-type"));
    const body = await parseRequestBody(request);

    // form_params is the decoded key/value object when the body is form-encoded.
    // The Twilio signing algorithm needs these (sorted) to reproduce the digest.
    const is_form_encoded = content_type.toLowerCase().includes("application/x-www-form-urlencoded");
    const form_params = is_form_encoded && body && !body.raw_text ? body : null;

    const payload = normalizeTextgridInboundPayload(body, request.headers);
    const verification = verifyTextgridWebhookRequest({
      request_url: request.url,
      raw_body,
      form_params,
      content_type,
      signature: payload.header_signature,
      signature_header_name: payload.header_signature_name,
    });

    if (!payload.from || !payload.message) {
      logger.warn("textgrid_inbound.invalid_payload", {
        payload,
      });

      return NextResponse.json(
        {
          ok: false,
          error: "invalid_textgrid_inbound_payload",
        },
        { status: 400 }
      );
    }

    if (verification.required && !verification.ok) {
      logger.warn("textgrid_inbound.invalid_signature", {
        message_id: payload.message_id || null,
        from: payload.from || null,
        to: payload.to || null,
        reason: verification.reason,
        ...verification.diagnostics,
      });

      return NextResponse.json(
        {
          ok: false,
          error: "invalid_textgrid_signature",
          verification,
        },
        { status: 401 }
      );
    }

    payload.webhook_verification = verification;

    const buyer_result = await maybeHandleBuyerTextgridInbound(payload);
    if (buyer_result?.matched) {
      logger.info("textgrid_inbound.routed_to_buyer_disposition", {
        message_id: payload.message_id || null,
        from: payload.from || null,
        buyer_match_item_id: buyer_result?.result?.buyer_match_item_id || null,
        company_item_id: buyer_result?.result?.company_item_id || null,
        correlation_mode: buyer_result?.correlation_mode || null,
      });

      return NextResponse.json(
        {
          ok: buyer_result?.result?.ok !== false,
          route: "webhooks/textgrid/inbound",
          verification,
          buyer_disposition: true,
          result: buyer_result.result,
        },
        { status: buyer_result?.result?.ok === false ? 400 : 200 }
      );
    }

    logger.info("textgrid_inbound.received", {
      message_id: payload.message_id || null,
      from: payload.from,
      to: payload.to || null,
      status: payload.status || null,
      event: payload.header_event || null,
      verified: verification.verified,
      signature_required: verification.required,
    });

    const result = await handleTextgridInbound(payload);

    return NextResponse.json({
      ok: result?.ok !== false,
      route: "webhooks/textgrid/inbound",
      verification,
      result,
    });
  } catch (error) {
    logger.error("textgrid_inbound.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "textgrid_inbound_failed",
      },
      { status: 500 }
    );
  }
}
