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

function serializeForConsole(value) {
  try {
    return JSON.stringify(value, (_key, val) => {
      if (val instanceof Error) {
        return {
          name: val.name,
          message: val.message,
          stack: val.stack,
        };
      }
      return val;
    });
  } catch {
    return JSON.stringify({ serialization_error: true });
  }
}

function emitConsoleError(event, meta = {}) {
  console.error(
    serializeForConsole({
      timestamp: new Date().toISOString(),
      level: "ERROR",
      event,
      meta,
    })
  );
}

function safeRouteLog(level, event, meta = {}) {
  try {
    const logFn = runtimeDeps?.logger?.[level];
    if (typeof logFn === "function") {
      logFn(event, meta);
    }
  } catch (log_error) {
    emitConsoleError(`${event}.logger_failed`, {
      log_error_message: log_error?.message || "unknown_logger_error",
      log_error_stack: log_error?.stack || null,
      original_event: event,
      original_level: level,
    });
  }
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
  let parsed_body_keys = [];

  try {
    const raw_body = await request.clone().text().catch(() => "");
    const content_type = clean(request.headers.get("content-type"));
    const body = await parseRequestBody(request);

    // form_params is the decoded key/value object when the body is form-encoded.
    // The Twilio signing algorithm needs these (sorted) to reproduce the digest.
    const is_form_encoded = content_type.toLowerCase().includes("application/x-www-form-urlencoded");
    const form_params = is_form_encoded && body && !body.raw_text ? body : null;
    parsed_body_keys = Object.keys(body || {});

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

    safeRouteLog(
      "info",
      "textgrid_inbound.normalized",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        extra: {
          event: payload.header_event || null,
          parsed_body_keys,
        },
      })
    );

    try {
      safeRouteLog(
        "info",
        "textgrid_inbound.pre_accept_checkpoint_1",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          extra: {
            parsed_body_keys,
            checkpoint_target: "signature_branch_selected",
          },
        })
      );

      safeRouteLog(
        "info",
        "textgrid_inbound.signature_branch_selected",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          extra: {
            parsed_body_keys,
            signature_invalid: Boolean(verification.required && !verification.ok),
            will_continue_after_signature_check: !(
              verification.required &&
              !verification.ok &&
              signature_verification_mode === "strict"
            ),
          },
        })
      );

      safeRouteLog(
        "info",
        "textgrid_inbound.pre_accept_checkpoint_2",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          extra: {
            parsed_body_keys,
            checkpoint_target: "validation_and_signature_guards",
          },
        })
      );

      if (!payload.from || !payload.message) {
        safeRouteLog("warn", "textgrid_inbound.invalid_payload", {
          payload,
        });

        safeRouteLog(
          "info",
          "textgrid_inbound.response_sent",
          buildTextgridWebhookLogMeta({
            payload,
            webhook_verification,
            final_response_status: 400,
            extra: {
              parsed_body_keys,
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
        safeRouteLog(
          "warn",
          "textgrid_inbound.invalid_signature",
          buildTextgridWebhookLogMeta({
            payload,
            webhook_verification,
            extra: {
              parsed_body_keys,
            },
          })
        );

        if (signature_verification_mode === "strict") {
          safeRouteLog(
            "info",
            "textgrid_inbound.response_sent",
            buildTextgridWebhookLogMeta({
              payload,
              webhook_verification,
              final_response_status: 401,
              extra: {
                parsed_body_keys,
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
        safeRouteLog("warn", "textgrid_inbound.signature_verification_disabled", {
          signature_verification_disabled: true,
          ...buildTextgridWebhookLogMeta({
            payload,
            webhook_verification,
            extra: {
              parsed_body_keys,
            },
          }),
        });
      }

      safeRouteLog(
        "info",
        "textgrid_inbound.pre_accept_checkpoint_3",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          extra: {
            parsed_body_keys,
            checkpoint_target: "accepted_and_payload_mutation",
          },
        })
      );

      payload.webhook_verification = webhook_verification;
      Object.assign(payload, signature_meta);

      safeRouteLog(
        "info",
        "textgrid_inbound.accepted",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          extra: {
            parsed_body_keys,
          },
        })
      );
      accepted_logged = true;
    } catch (error) {
      const error_meta = buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
        extra: {
          error_message: error?.message || "Unknown error",
          error_stack: error?.stack || null,
          parsed_body_keys,
        },
      });

      safeRouteLog("error", "textgrid_inbound.failed_pre_accept", error_meta);
      safeRouteLog("error", "textgrid_inbound.failed", error_meta);
      emitConsoleError("textgrid_inbound.failed_pre_accept", error_meta);
      emitConsoleError("textgrid_inbound.failed", error_meta);

      const response_meta = buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
        extra: {
          parsed_body_keys,
          response_error: "textgrid_inbound_failed",
        },
      });
      safeRouteLog("info", "textgrid_inbound.response_sent", response_meta);
      emitConsoleError("textgrid_inbound.response_sent", response_meta);

      return NextResponse.json(
        {
          ok: false,
          error: "textgrid_inbound_failed",
        },
        { status: 500 }
      );
    }

    downstream_handler_invoked = true;
    safeRouteLog(
      "info",
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

    safeRouteLog(
      "info",
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
      safeRouteLog("info", "textgrid_inbound.routed_to_buyer_disposition", {
        message_id: payload.message_id || null,
        from: payload.from || null,
        buyer_match_item_id: buyer_result?.result?.buyer_match_item_id || null,
        company_item_id: buyer_result?.result?.company_item_id || null,
        correlation_mode: buyer_result?.correlation_mode || null,
      });

      const response_status = buyer_result?.result?.ok === false ? 400 : 200;
      safeRouteLog(
        "info",
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
    safeRouteLog(
      "info",
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

    safeRouteLog(
      "info",
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

    safeRouteLog(
      "info",
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
    const failure_meta = buildTextgridWebhookLogMeta({
      payload: log_payload,
      webhook_verification: log_webhook_verification,
      downstream_handler_invoked,
      podio_persistence_attempted,
      final_response_status: 500,
      extra: {
        error_message: error?.message || "Unknown error",
        error_stack: error?.stack || null,
        parsed_body_keys,
        accepted_logged,
      },
    });

    safeRouteLog("error", "textgrid_inbound.failed", failure_meta);
    emitConsoleError("textgrid_inbound.failed", failure_meta);

    if (!accepted_logged) {
      safeRouteLog("error", "textgrid_inbound.failed_pre_accept", failure_meta);
      emitConsoleError("textgrid_inbound.failed_pre_accept", failure_meta);
    }

    const response_meta = buildTextgridWebhookLogMeta({
      payload: log_payload,
      webhook_verification: log_webhook_verification,
      downstream_handler_invoked,
      podio_persistence_attempted,
      final_response_status: 500,
      extra: {
        parsed_body_keys,
        response_error: "textgrid_inbound_failed",
      },
    });
    safeRouteLog("info", "textgrid_inbound.response_sent", response_meta);
    emitConsoleError("textgrid_inbound.response_sent", response_meta);

    return NextResponse.json(
      {
        ok: false,
        error: "textgrid_inbound_failed",
      },
      { status: 500 }
    );
  }
}
