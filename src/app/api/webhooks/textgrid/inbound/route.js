import { NextResponse } from "next/server";

import { maybeHandleBuyerTextgridInbound } from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { child } from "@/lib/logging/logger.js";
import { handleTextgridInbound } from "@/lib/flows/handle-textgrid-inbound.js";
import { verifyTextgridWebhookSignature } from "@/lib/providers/textgrid.js";

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

function normalizeInboundPayload(body = {}, headers) {
  return {
    provider: "textgrid",
    raw: body,

    message_id: clean(
      body?.message_id ||
      body?.messageId ||
      body?.id ||
      body?.sms_id
    ),

    from: clean(
      body?.from ||
      body?.from_number ||
      body?.fromNumber ||
      body?.sender ||
      body?.phone
    ),

    to: clean(
      body?.to ||
      body?.to_number ||
      body?.toNumber ||
      body?.recipient
    ),

    message: clean(
      body?.message ||
      body?.body ||
      body?.text ||
      body?.content
    ),

    direction: clean(body?.direction || "inbound"),
    received_at: clean(
      body?.received_at ||
      body?.timestamp ||
      body?.created_at
    ),
    conversation_id: clean(body?.conversation_id || body?.conversationId),
    account_id: clean(body?.account_id || body?.accountId),
    status: clean(body?.status || "received"),

    header_signature: clean(
      headers.get("x-textgrid-signature") ||
      headers.get("x-signature") ||
      ""
    ),
    header_event: clean(
      headers.get("x-textgrid-event") ||
      headers.get("x-event-type") ||
      "inbound"
    ),
    http_received_at: new Date().toISOString(),
  };
}

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
    const body = await parseRequestBody(request);
    const payload = normalizeInboundPayload(body, request.headers);
    const verification = verifyTextgridWebhookSignature({
      raw_body,
      signature: payload.header_signature,
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
