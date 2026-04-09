import { NextResponse } from "next/server.js";

import { maybeHandleBuyerTextgridInbound } from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { child } from "@/lib/logging/logger.js";
import { handleTextgridInbound } from "@/lib/flows/handle-textgrid-inbound.js";
import {
  buildTextgridWebhookBypassResult,
  buildTextgridWebhookVerificationMeta,
  getTextgridWebhookSignatureMode,
  verifyTextgridWebhookRequest,
} from "@/lib/webhooks/textgrid-verify-webhook.js";
import { normalizeTextgridInboundPayload } from "@/lib/webhooks/textgrid-inbound-normalize.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.textgrid.inbound",
});

const defaultDeps = {
  logger,
  maybeHandleBuyerTextgridInboundImpl: maybeHandleBuyerTextgridInbound,
  handleTextgridInboundImpl: handleTextgridInbound,
  verifyTextgridWebhookRequestImpl: verifyTextgridWebhookRequest,
  normalizeTextgridInboundPayloadImpl: normalizeTextgridInboundPayload,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function buildSignatureLogMeta(payload, webhook_verification) {
  return {
    message_id: payload.message_id || null,
    from: payload.from || null,
    to: payload.to || null,
    status: payload.status || null,
    signature_verification_mode: webhook_verification?.signature_verification_mode || null,
    signature_verified: Boolean(webhook_verification?.signature_verified),
    signature_bypassed: Boolean(webhook_verification?.signature_bypassed),
    signature_failure_reason: webhook_verification?.signature_failure_reason || null,
    signature_header_name: webhook_verification?.signature_header_name || null,
    signature_unverified_observe_mode: Boolean(
      webhook_verification?.signature_unverified_observe_mode
    ),
    ...webhook_verification?.diagnostics,
  };
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

export function __setTextgridInboundRouteTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetTextgridInboundRouteTestDeps() {
  runtimeDeps = { ...defaultDeps };
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
    const content_type = clean(request.headers.get("content-type"));
    const body = await parseRequestBody(request);

    // form_params is the decoded key/value object when the body is form-encoded.
    // The Twilio signing algorithm needs these (sorted) to reproduce the digest.
    const is_form_encoded = content_type.toLowerCase().includes("application/x-www-form-urlencoded");
    const form_params = is_form_encoded && body && !body.raw_text ? body : null;

    const payload = runtimeDeps.normalizeTextgridInboundPayloadImpl(body, request.headers);
    const signature_verification_mode = getTextgridWebhookSignatureMode();
    const verification =
      signature_verification_mode === "off"
        ? buildTextgridWebhookBypassResult({
            request_url: request.url,
            raw_body,
            form_params,
            content_type,
            signature: payload.header_signature,
            signature_header_name: payload.header_signature_name,
          })
        : runtimeDeps.verifyTextgridWebhookRequestImpl({
            request_url: request.url,
            raw_body,
            form_params,
            content_type,
            signature: payload.header_signature,
            signature_header_name: payload.header_signature_name,
          });
    const signature_meta = buildTextgridWebhookVerificationMeta({
      verification,
      mode: signature_verification_mode,
      signature_header_name: payload.header_signature_name,
    });
    const webhook_verification = {
      ...verification,
      ...signature_meta,
    };

    if (!payload.from || !payload.message) {
      runtimeDeps.logger.warn("textgrid_inbound.invalid_payload", {
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
      runtimeDeps.logger.warn(
        "textgrid_inbound.invalid_signature",
        buildSignatureLogMeta(payload, webhook_verification)
      );

      if (signature_verification_mode === "strict") {
        return NextResponse.json(
          {
            ok: false,
            error: "invalid_textgrid_signature",
            verification: webhook_verification,
          },
          { status: 401 }
        );
      }
    }

    if (signature_verification_mode === "off") {
      runtimeDeps.logger.warn("textgrid_inbound.signature_verification_disabled", {
        signature_verification_disabled: true,
        ...buildSignatureLogMeta(payload, webhook_verification),
      });
    }

    payload.webhook_verification = webhook_verification;
    Object.assign(payload, signature_meta);

    const buyer_result = await runtimeDeps.maybeHandleBuyerTextgridInboundImpl(payload);
    if (buyer_result?.matched) {
      runtimeDeps.logger.info("textgrid_inbound.routed_to_buyer_disposition", {
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
          verification: webhook_verification,
          buyer_disposition: true,
          result: buyer_result.result,
        },
        { status: buyer_result?.result?.ok === false ? 400 : 200 }
      );
    }

    runtimeDeps.logger.info("textgrid_inbound.received", {
      message_id: payload.message_id || null,
      from: payload.from,
      to: payload.to || null,
      status: payload.status || null,
      event: payload.header_event || null,
      verified: verification.verified,
      signature_required: verification.required,
      signature_verification_mode,
      signature_bypassed: signature_meta.signature_bypassed,
    });

    const result = await runtimeDeps.handleTextgridInboundImpl(payload);

    return NextResponse.json({
      ok: result?.ok !== false,
      route: "webhooks/textgrid/inbound",
      verification: webhook_verification,
      result,
    });
  } catch (error) {
    runtimeDeps.logger.error("textgrid_inbound.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "textgrid_inbound_failed",
      },
      { status: 500 }
    );
  }
}
