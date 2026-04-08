import { processSendQueue } from "@/lib/domain/queue/process-send-queue.js";
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";

function clean(value) {
  return String(value ?? "").trim();
}

function asNullablePositiveInteger(value, fallback = null) {
  const normalized = clean(value);
  if (!normalized) return fallback;

  const n = Number(normalized);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function previewMessage(value, max = 160) {
  const normalized = clean(value);
  if (!normalized) return null;
  return normalized.length > max ? `${normalized.slice(0, max)}...` : normalized;
}

function statusForResult(result) {
  return result?.queued?.ok === false || result?.processed?.ok === false ? 400 : 200;
}

export function serializeRouteError(error) {
  return {
    name: error?.name || "Error",
    message: error?.message || "Unknown error",
    stack: error?.stack || null,
  };
}

export function normalizeSendNowInput(input = {}) {
  const rendered_message_text =
    clean(input?.rendered_message_text) || clean(input?.message_text) || null;

  return {
    phone: clean(input?.phone),
    use_case: clean(input?.use_case) || null,
    language: clean(input?.language) || null,
    touch_number: asNullablePositiveInteger(input?.touch_number, null),
    rendered_message_text,
    message_override_present: Boolean(rendered_message_text),
    message_override_preview: previewMessage(rendered_message_text),
  };
}

export async function buildAndSendNow(
  {
    phone,
    use_case = null,
    language = null,
    touch_number = null,
    rendered_message_text = null,
  },
  deps = {}
) {
  const {
    queueOutboundMessageImpl = queueOutboundMessage,
    processSendQueueImpl = processSendQueue,
  } = deps;
  let queued;

  try {
    queued = await queueOutboundMessageImpl({
      phone,
      use_case,
      language,
      touch_number,
      rendered_message_text,
    });
  } catch (error) {
    return {
      queued: {
        ok: false,
        stage: "queue_build",
        reason: "queue_build_failed",
        message: error?.message || "queue_build_failed",
      },
      processed: null,
    };
  }

  const queue_item_id =
    queued?.queue_item_id ||
    queued?.item_id ||
    queued?.result?.queue_item_id ||
    null;

  if (!queued?.ok) {
    return {
      queued,
      processed: null,
    };
  }

  if (!queue_item_id) {
    return {
      queued,
      processed: {
        ok: false,
        sent: false,
        reason: "missing_queue_item_id_after_queue",
      },
    };
  }

  let processed;

  try {
    processed = await processSendQueueImpl({
      queue_item_id,
    });
  } catch (error) {
    processed = {
      ok: false,
      sent: false,
      stage: "queue_processing",
      reason: "queue_processing_failed",
      message: error?.message || "queue_processing_failed",
      queue_item_id,
    };
  }

  return {
    queued,
    processed,
  };
}

export async function handleSendNowRequestData(request, method = "GET", deps = {}) {
  const {
    logger,
    queueOutboundMessageImpl = queueOutboundMessage,
    processSendQueueImpl = processSendQueue,
  } = deps;
  let request_meta = {
    method,
    phone: null,
    use_case: null,
    language: null,
    touch_number: null,
    message_override_present: false,
    message_override_preview: null,
  };

  try {
    let normalized_input = null;

    if (method === "GET") {
      const { searchParams } = new URL(request.url);
      normalized_input = normalizeSendNowInput({
        phone: searchParams.get("phone"),
        use_case: searchParams.get("use_case"),
        language: searchParams.get("language"),
        touch_number: searchParams.get("touch_number"),
        message_text: searchParams.get("message_text"),
        rendered_message_text: searchParams.get("rendered_message_text"),
      });
    } else {
      const body = await request.json().catch(() => ({}));
      normalized_input = normalizeSendNowInput(body);
    }

    request_meta = {
      method,
      phone: normalized_input.phone,
      use_case: normalized_input.use_case,
      language: normalized_input.language,
      touch_number: normalized_input.touch_number,
      message_override_present: normalized_input.message_override_present,
      message_override_preview: normalized_input.message_override_preview,
    };

    logger?.info?.("outbound_send_now.requested", request_meta);

    const result = await buildAndSendNow(normalized_input, {
      queueOutboundMessageImpl,
      processSendQueueImpl,
    });

    logger?.info?.("outbound_send_now.completed", {
      ...request_meta,
      ok: result?.queued?.ok === true && result?.processed?.ok === true,
      queued_stage: result?.queued?.stage || null,
      queued_reason: result?.queued?.reason || null,
      processed_stage: result?.processed?.stage || null,
      processed_reason: result?.processed?.reason || null,
      queue_item_id:
        result?.queued?.queue_item_id ||
        result?.processed?.queue_item_id ||
        null,
    });

    return {
      status: statusForResult(result),
      payload: {
        ok: result?.queued?.ok === true && result?.processed?.ok === true,
        route: "internal/outbound/send-now",
        result,
      },
    };
  } catch (error) {
    const diagnostics = serializeRouteError(error);

    logger?.error?.("outbound_send_now.failed", {
      ...request_meta,
      error: diagnostics,
    });

    return {
      status: 500,
      payload: {
        ok: false,
        error: "outbound_send_now_failed",
        message: diagnostics.message,
      },
    };
  }
}
