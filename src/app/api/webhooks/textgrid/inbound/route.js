import { NextResponse } from "next/server.js";

import { maybeHandleBuyerTextgridInbound } from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { child } from "@/lib/logging/logger.js";
import { handleTextgridInbound } from "@/lib/flows/handle-textgrid-inbound.js";
import {
  buildTextgridWebhookBypassResult,
  buildTextgridWebhookLogMeta,
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
  let log_payload = null;
  let log_webhook_verification = null;
  let accepted_logged = false;
  let downstream_handler_invoked = false;
  let podio_persistence_attempted = false;

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
    log_payload = payload;
    log_webhook_verification = webhook_verification;

    runtimeDeps.logger.info(
      "textgrid_inbound.normalized",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        extra: {
          event: payload.header_event || null,
          parsed_body_keys: Object.keys(body || {}),
        },
      })
    );

    runtimeDeps.logger.info(
      "textgrid_inbound.signature_branch_selected",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        extra: {
          signature_invalid: Boolean(verification.required && !verification.ok),
          will_continue_after_signature_check: !(
            verification.required &&
            !verification.ok &&
            signature_verification_mode === "strict"
          ),
        },
      })
    );

    if (!payload.from || !payload.message) {
      runtimeDeps.logger.warn("textgrid_inbound.invalid_payload", {
        payload,
      });

      runtimeDeps.logger.info(
        "textgrid_inbound.response_sent",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          final_response_status: 400,
          extra: {
            response_error: "invalid_textgrid_inbound_payload",
          },
        })
      );

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
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
        })
      );

      if (signature_verification_mode === "strict") {
        runtimeDeps.logger.info(
          "textgrid_inbound.response_sent",
          buildTextgridWebhookLogMeta({
            payload,
            webhook_verification,
            final_response_status: 401,
            extra: {
              response_error: "invalid_textgrid_signature",
            },
          })
        );

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
        ...buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
        }),
      });
    }

    payload.webhook_verification = webhook_verification;
    Object.assign(payload, signature_meta);

    runtimeDeps.logger.info(
      "textgrid_inbound.accepted",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
      })
    );
    accepted_logged = true;

    downstream_handler_invoked = true;
    runtimeDeps.logger.info(
      "textgrid_inbound.handler_started",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        extra: {
          handler_name: "maybeHandleBuyerTextgridInbound",
        },
      })
    );

    const buyer_result = await runtimeDeps.maybeHandleBuyerTextgridInboundImpl(payload);

    runtimeDeps.logger.info(
      "textgrid_inbound.handler_completed",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: Boolean(buyer_result?.matched),
        extra: {
          handler_name: "maybeHandleBuyerTextgridInbound",
          buyer_disposition_matched: Boolean(buyer_result?.matched),
        },
      })
    );

    if (buyer_result?.matched) {
      podio_persistence_attempted = true;
      runtimeDeps.logger.info("textgrid_inbound.routed_to_buyer_disposition", {
        message_id: payload.message_id || null,
        from: payload.from || null,
        buyer_match_item_id: buyer_result?.result?.buyer_match_item_id || null,
        company_item_id: buyer_result?.result?.company_item_id || null,
        correlation_mode: buyer_result?.correlation_mode || null,
      });

      const response_status = buyer_result?.result?.ok === false ? 400 : 200;
      runtimeDeps.logger.info(
        "textgrid_inbound.response_sent",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          downstream_handler_invoked: true,
          podio_persistence_attempted: true,
          final_response_status: response_status,
          extra: {
            buyer_disposition: true,
          },
        })
      );

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

    downstream_handler_invoked = true;
    podio_persistence_attempted = true;
    runtimeDeps.logger.info(
      "textgrid_inbound.handler_started",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        extra: {
          handler_name: "handleTextgridInbound",
        },
      })
    );

    const result = await runtimeDeps.handleTextgridInboundImpl(payload);

    runtimeDeps.logger.info(
      "textgrid_inbound.handler_completed",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        extra: {
          handler_name: "handleTextgridInbound",
          handler_ok: result?.ok !== false,
        },
      })
    );

    runtimeDeps.logger.info(
      "textgrid_inbound.response_sent",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        final_response_status: 200,
      })
    );

    return NextResponse.json({
      ok: result?.ok !== false,
      route: "webhooks/textgrid/inbound",
      verification: webhook_verification,
      result,
    });
  } catch (error) {
    const error_meta = {
      error_message: error?.message || "Unknown error",
      error_stack: error?.stack || null,
    };

    if (!accepted_logged) {
      runtimeDeps.logger.error(
        "textgrid_inbound.failed_before_accept",
        buildTextgridWebhookLogMeta({
          payload: log_payload,
          webhook_verification: log_webhook_verification,
          downstream_handler_invoked,
          podio_persistence_attempted,
          final_response_status: 500,
          extra: error_meta,
        })
      );
    }

    runtimeDeps.logger.error("textgrid_inbound.failed", {
      ...buildTextgridWebhookLogMeta({
        payload: log_payload,
        webhook_verification: log_webhook_verification,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
      }),
      ...error_meta,
    });

    runtimeDeps.logger.info(
      "textgrid_inbound.response_sent",
      buildTextgridWebhookLogMeta({
        payload: log_payload,
        webhook_verification: log_webhook_verification,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
        extra: {
          response_error: "textgrid_inbound_failed",
        },
      })
    );

    return NextResponse.json(
      {
        ok: false,
        error: "textgrid_inbound_failed",
      },
      { status: 500 }
    );
  }
}
