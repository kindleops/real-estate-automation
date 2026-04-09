import { handleTextgridDelivery } from "@/lib/flows/handle-textgrid-delivery.js";
import { child } from "@/lib/logging/logger.js";
import {
  buildTextgridWebhookBypassResult,
  buildTextgridWebhookLogMeta,
  buildTextgridWebhookVerificationMeta,
  getTextgridWebhookSignatureMode,
  verifyTextgridWebhookRequest,
} from "@/lib/webhooks/textgrid-verify-webhook.js";
import { normalizeTextgridDeliveryPayload } from "@/lib/webhooks/textgrid-delivery-normalize.js";

const defaultLogger = child({
  module: "webhooks.textgrid.delivery_request",
});

function clean(value) {
  return String(value ?? "").trim();
}

export async function parseTextgridDeliveryRequestBody(request) {
  const contentType = clean(request?.headers?.get("content-type")).toLowerCase();

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

function parseLooseTextBody(raw_body = "") {
  const trimmed = clean(raw_body);
  if (!trimmed) return {};

  try {
    const parsed = JSON.parse(trimmed);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed;
    }
  } catch {
    // Fall through to URLSearchParams parsing.
  }

  const params = new URLSearchParams(trimmed);
  const entries = Object.fromEntries(params.entries());
  if (Object.keys(entries).length > 0) {
    return entries;
  }

  return { raw_text: raw_body };
}

export async function handleTextgridDeliveryRequest(request, deps = {}) {
  const {
    logger = defaultLogger,
    handleTextgridDeliveryImpl = handleTextgridDelivery,
    verifyTextgridWebhookSignatureImpl = verifyTextgridWebhookRequest,
  } = deps;

  let log_payload = null;
  let log_webhook_verification = null;
  let accepted_logged = false;
  let downstream_handler_invoked = false;
  let podio_persistence_attempted = false;

  try {
    const raw_body = await request.clone().text().catch(() => "");
    const content_type = clean(request?.headers?.get("content-type"));
    let body = await parseTextgridDeliveryRequestBody(request);
    if (!Object.keys(body || {}).length || body?.raw_text) {
      const reparsed = parseLooseTextBody(raw_body);
      if (Object.keys(reparsed || {}).length) {
        body = reparsed;
      }
    }

    // form_params: the decoded key/value pairs needed by the Twilio signing algorithm.
    const is_form_encoded = content_type.toLowerCase().includes("application/x-www-form-urlencoded");
    const form_params = is_form_encoded && body && !body.raw_text ? body : null;

    const payload = normalizeTextgridDeliveryPayload(body, request.headers);
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
        : verifyTextgridWebhookSignatureImpl({
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

    logger.info(
      "textgrid_delivery.normalized",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        extra: {
          event: payload.header_event || null,
          parsed_body_keys: Object.keys(body || {}),
        },
      })
    );

    logger.info(
      "textgrid_delivery.signature_branch_selected",
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

    if (!payload.message_id && !payload.status) {
      logger.warn("textgrid_delivery.invalid_payload", {
        payload,
        raw_body_preview: clean(raw_body).slice(0, 500) || null,
        parsed_keys: Object.keys(body || {}),
      });

      logger.info(
        "textgrid_delivery.response_sent",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          final_response_status: 400,
          extra: {
            response_error: "invalid_textgrid_delivery_payload",
          },
        })
      );

      return {
        status: 400,
        payload: {
          ok: false,
          error: "invalid_textgrid_delivery_payload",
        },
      };
    }

    if (verification.required && !verification.ok) {
      logger.warn(
        "textgrid_delivery.invalid_signature",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
        })
      );

      if (signature_verification_mode === "strict") {
        logger.info(
          "textgrid_delivery.response_sent",
          buildTextgridWebhookLogMeta({
            payload,
            webhook_verification,
            final_response_status: 401,
            extra: {
              response_error: "invalid_textgrid_signature",
            },
          })
        );

        return {
          status: 401,
          payload: {
            ok: false,
            error: "invalid_textgrid_signature",
            verification: webhook_verification,
          },
        };
      }
    }

    if (signature_verification_mode === "off") {
      logger.warn("textgrid_delivery.signature_verification_disabled", {
        signature_verification_disabled: true,
        ...buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
        }),
      });
    }

    payload.webhook_verification = webhook_verification;
    Object.assign(payload, signature_meta);

    logger.info(
      "textgrid_delivery.accepted",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
      })
    );
    accepted_logged = true;

    downstream_handler_invoked = true;
    podio_persistence_attempted = true;
    logger.info(
      "textgrid_delivery.handler_started",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        extra: {
          handler_name: "handleTextgridDelivery",
        },
      })
    );

    const result = await handleTextgridDeliveryImpl(payload);

    logger.info(
      "textgrid_delivery.handler_completed",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        extra: {
          handler_name: "handleTextgridDelivery",
          handler_ok: result?.ok !== false,
        },
      })
    );

    logger.info(
      "textgrid_delivery.response_sent",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        final_response_status: 200,
      })
    );

    return {
      status: 200,
      payload: {
        ok: result?.ok !== false,
        route: "webhooks/textgrid/delivery",
        verification: webhook_verification,
        result,
      },
    };
  } catch (error) {
    const error_meta = {
      error_message: error?.message || "Unknown error",
      error_stack: error?.stack || null,
    };

    if (!accepted_logged) {
      logger.error(
        "textgrid_delivery.failed_before_accept",
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

    logger.error("textgrid_delivery.failed", {
      ...buildTextgridWebhookLogMeta({
        payload: log_payload,
        webhook_verification: log_webhook_verification,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
      }),
      ...error_meta,
    });

    logger.info(
      "textgrid_delivery.response_sent",
      buildTextgridWebhookLogMeta({
        payload: log_payload,
        webhook_verification: log_webhook_verification,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
        extra: {
          response_error: "textgrid_delivery_failed",
        },
      })
    );

    return {
      status: 500,
      payload: {
        ok: false,
        error: "textgrid_delivery_failed",
      },
    };
  }
}

export default handleTextgridDeliveryRequest;
