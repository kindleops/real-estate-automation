import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleTextgridDelivery } from "@/lib/flows/handle-textgrid-delivery.js";
import { verifyTextgridWebhookSignature } from "@/lib/providers/textgrid.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.textgrid.delivery",
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

function normalizeDeliveryPayload(body = {}, headers) {
  return {
    provider: "textgrid",
    raw: body,

    message_id: clean(
      body?.message_id ||
      body?.messageId ||
      body?.id ||
      body?.sms_id
    ),

    status: clean(
      body?.status ||
      body?.message_status ||
      body?.delivery_status
    ),

    error_code: clean(
      body?.error_code ||
      body?.errorCode ||
      body?.code
    ),

    error_message: clean(
      body?.error_message ||
      body?.errorMessage ||
      body?.reason
    ),

    delivered_at: clean(
      body?.delivered_at ||
      body?.timestamp ||
      body?.updated_at
    ),

    client_reference_id: clean(
      body?.client_reference_id ||
      body?.clientReferenceId ||
      body?.external_id ||
      body?.externalId
    ),

    from: clean(
      body?.from ||
      body?.from_number ||
      body?.fromNumber ||
      body?.sender
    ),

    to: clean(
      body?.to ||
      body?.to_number ||
      body?.toNumber ||
      body?.recipient
    ),

    account_id: clean(body?.account_id || body?.accountId),
    conversation_id: clean(body?.conversation_id || body?.conversationId),

    header_signature: clean(
      headers.get("x-textgrid-signature") ||
      headers.get("x-signature") ||
      ""
    ),
    header_event: clean(
      headers.get("x-textgrid-event") ||
      headers.get("x-event-type") ||
      "delivery"
    ),
    http_received_at: new Date().toISOString(),
  };
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/textgrid/delivery",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    const raw_body = await request.clone().text().catch(() => "");
    const body = await parseRequestBody(request);
    const payload = normalizeDeliveryPayload(body, request.headers);
    const verification = verifyTextgridWebhookSignature({
      raw_body,
      signature: payload.header_signature,
    });

    if (!payload.message_id && !payload.status) {
      logger.warn("textgrid_delivery.invalid_payload", {
        payload,
      });

      return NextResponse.json(
        {
          ok: false,
          error: "invalid_textgrid_delivery_payload",
        },
        { status: 400 }
      );
    }

    if (verification.required && !verification.ok) {
      logger.warn("textgrid_delivery.invalid_signature", {
        message_id: payload.message_id || null,
        status: payload.status || null,
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

    logger.info("textgrid_delivery.received", {
      message_id: payload.message_id || null,
      status: payload.status || null,
      error_code: payload.error_code || null,
      event: payload.header_event || null,
      verified: verification.verified,
      signature_required: verification.required,
    });

    const result = await handleTextgridDelivery(payload);

    return NextResponse.json({
      ok: result?.ok !== false,
      route: "webhooks/textgrid/delivery",
      verification,
      result,
    });
  } catch (error) {
    logger.error("textgrid_delivery.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "textgrid_delivery_failed",
      },
      { status: 500 }
    );
  }
}
