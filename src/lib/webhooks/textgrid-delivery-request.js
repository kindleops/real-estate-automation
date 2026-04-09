import { handleTextgridDelivery } from "@/lib/flows/handle-textgrid-delivery.js";
import { child } from "@/lib/logging/logger.js";
import {
  buildTextgridWebhookBypassResult,
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

    if (!payload.message_id && !payload.status) {
      logger.warn("textgrid_delivery.invalid_payload", {
        payload,
        raw_body_preview: clean(raw_body).slice(0, 500) || null,
        parsed_keys: Object.keys(body || {}),
      });

      return {
        status: 400,
        payload: {
          ok: false,
          error: "invalid_textgrid_delivery_payload",
        },
      };
    }

    if (verification.required && !verification.ok) {
      logger.warn("textgrid_delivery.invalid_signature", buildSignatureLogMeta(payload, webhook_verification));

      if (signature_verification_mode === "strict") {
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
        ...buildSignatureLogMeta(payload, webhook_verification),
      });
    }

    payload.webhook_verification = webhook_verification;
    Object.assign(payload, signature_meta);

    logger.info("textgrid_delivery.received", {
      message_id: payload.message_id || null,
      status: payload.status || null,
      error_code: payload.error_code || null,
      event: payload.header_event || null,
      verified: verification.verified,
      signature_required: verification.required,
      signature_verification_mode,
      signature_bypassed: signature_meta.signature_bypassed,
      from: payload.from || null,
      to: payload.to || null,
    });

    const result = await handleTextgridDeliveryImpl(payload);

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
    logger.error("textgrid_delivery.failed", {
      error: {
        message: error?.message || "Unknown error",
        stack: error?.stack || null,
      },
    });

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
