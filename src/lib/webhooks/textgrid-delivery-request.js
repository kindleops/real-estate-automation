import { handleTextgridDelivery } from "@/lib/flows/handle-textgrid-delivery.js";
import { child } from "@/lib/logging/logger.js";
import { verifyTextgridWebhookSignature } from "@/lib/providers/textgrid.js";
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
    verifyTextgridWebhookSignatureImpl = verifyTextgridWebhookSignature,
  } = deps;

  try {
    const raw_body = await request.clone().text().catch(() => "");
    let body = await parseTextgridDeliveryRequestBody(request);
    if (!Object.keys(body || {}).length || body?.raw_text) {
      const reparsed = parseLooseTextBody(raw_body);
      if (Object.keys(reparsed || {}).length) {
        body = reparsed;
      }
    }
    const payload = normalizeTextgridDeliveryPayload(body, request.headers);
    const verification = verifyTextgridWebhookSignatureImpl({
      raw_body,
      signature: payload.header_signature,
    });

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
      logger.warn("textgrid_delivery.invalid_signature", {
        message_id: payload.message_id || null,
        status: payload.status || null,
        reason: verification.reason,
      });

      return {
        status: 401,
        payload: {
          ok: false,
          error: "invalid_textgrid_signature",
          verification,
        },
      };
    }

    payload.webhook_verification = verification;

    logger.info("textgrid_delivery.received", {
      message_id: payload.message_id || null,
      status: payload.status || null,
      error_code: payload.error_code || null,
      event: payload.header_event || null,
      verified: verification.verified,
      signature_required: verification.required,
      from: payload.from || null,
      to: payload.to || null,
    });

    const result = await handleTextgridDeliveryImpl(payload);

    return {
      status: 200,
      payload: {
        ok: result?.ok !== false,
        route: "webhooks/textgrid/delivery",
        verification,
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
