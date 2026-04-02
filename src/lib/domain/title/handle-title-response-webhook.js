// ─── handle-title-response-webhook.js ────────────────────────────────────
import {
  TITLE_ROUTING_FIELDS,
  getTitleRoutingItem,
  findTitleRoutingItems,
} from "@/lib/podio/apps/title-routing.js";
import {
  CLOSING_FIELDS,
  findClosingItems,
  getClosingItem,
} from "@/lib/podio/apps/closings.js";
import { classifyTitleResponse } from "@/lib/domain/title/classify-title-response.js";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { updateTitleRoutingStatus } from "@/lib/domain/title/update-title-routing-status.js";
import { updateClosingStatus } from "@/lib/domain/closings/update-closing-status.js";
import {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { info, warn } from "@/lib/logging/logger.js";

function clean(value) {
  return String(value ?? "").trim();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

const defaultDeps = {
  getTitleRoutingItem,
  findTitleRoutingItems,
  findClosingItems,
  getClosingItem,
  classifyTitleResponse,
  updateTitleRoutingStatus,
  updateClosingStatus,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setTitleWebhookTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetTitleWebhookTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function extractWebhookPayload(payload = {}) {
  const event_id =
    payload.event_id ||
    payload.eventId ||
    payload.message_id ||
    payload.messageId ||
    payload.thread_id ||
    payload.threadId ||
    payload.email_message_id ||
    payload.emailMessageId ||
    null;

  const title_routing_item_id =
    payload.title_routing_item_id ||
    payload.titleRoutingItemId ||
    payload.title_routing_id ||
    payload.titleRoutingId ||
    null;

  const closing_item_id =
    payload.closing_item_id ||
    payload.closingItemId ||
    payload.closing_id ||
    payload.closingId ||
    null;

  const subject =
    payload.subject ||
    payload.email_subject ||
    payload.emailSubject ||
    payload.data?.subject ||
    "";

  const body =
    payload.body ||
    payload.message ||
    payload.text ||
    payload.content ||
    payload.data?.body ||
    "";

  const event =
    payload.event ||
    payload.event_type ||
    payload.eventType ||
    payload.status ||
    payload.data?.event ||
    "";

  const sender_email =
    payload.from ||
    payload.sender ||
    payload.sender_email ||
    payload.senderEmail ||
    payload.data?.from ||
    "";

  return {
    raw: payload,
    event_id: clean(event_id) || null,
    title_routing_item_id: clean(title_routing_item_id) || null,
    closing_item_id: clean(closing_item_id) || null,
    subject: clean(subject),
    body: clean(body),
    event: clean(event),
    sender_email: clean(sender_email),
  };
}

function buildTitleWebhookIdempotencyKey(extracted = {}) {
  return (
    clean(extracted.event_id) ||
    runtimeDeps.hashIdempotencyPayload({
      provider: "title",
      title_routing_item_id: clean(extracted.title_routing_item_id) || null,
      closing_item_id: clean(extracted.closing_item_id) || null,
      event: clean(extracted.event) || null,
      subject: clean(extracted.subject) || null,
      body: clean(extracted.body) || null,
      sender_email: clean(extracted.sender_email) || null,
      raw: extracted.raw || null,
    })
  );
}

async function findLatestTitleRoutingByClosingId(closing_item_id) {
  if (!closing_item_id) return null;

  const matches = await runtimeDeps.findTitleRoutingItems(
    { [TITLE_ROUTING_FIELDS.closing]: closing_item_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

async function findLatestClosingByTitleRoutingId(title_routing_item_id) {
  if (!title_routing_item_id) return null;

  const matches = await runtimeDeps.findClosingItems(
    { [CLOSING_FIELDS.title_routing]: title_routing_item_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

export async function handleTitleResponseWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);
  const idempotency_key = buildTitleWebhookIdempotencyKey(extracted);

  runtimeDeps.info("title.response_received", {
    event_id: extracted.event_id,
    title_routing_item_id: extracted.title_routing_item_id,
    closing_item_id: extracted.closing_item_id,
    event: extracted.event,
    sender_email: extracted.sender_email,
  });

  const idempotency = await runtimeDeps.beginIdempotentProcessing({
    scope: "title_webhook",
    key: idempotency_key,
    summary: `Processed title event ${idempotency_key}`,
    metadata: {
      event_id: extracted.event_id,
      title_routing_item_id: extracted.title_routing_item_id,
      closing_item_id: extracted.closing_item_id,
      event: extracted.event,
    },
  });

  if (!idempotency.ok) {
    return {
      ok: false,
      reason: idempotency.reason,
      event_id: extracted.event_id,
      idempotency_key,
    };
  }

  if (idempotency.duplicate) {
    runtimeDeps.info("title.response_duplicate_ignored", {
      event_id: extracted.event_id,
      title_routing_item_id: extracted.title_routing_item_id,
      reason: idempotency.reason,
      idempotency_key,
    });

    return {
      ok: true,
      duplicate: true,
      updated: false,
      reason: idempotency.reason,
      event_id: extracted.event_id,
      idempotency_key,
    };
  }

  try {
    let title_routing_item = null;
    let closing_item = null;

    if (extracted.title_routing_item_id) {
      title_routing_item = await runtimeDeps.getTitleRoutingItem(
        extracted.title_routing_item_id
      );
    }

    if (!title_routing_item && extracted.closing_item_id) {
      title_routing_item = await findLatestTitleRoutingByClosingId(
        extracted.closing_item_id
      );
    }

    if (!title_routing_item?.item_id) {
      runtimeDeps.warn("title.response_title_routing_not_found", {
        title_routing_item_id: extracted.title_routing_item_id,
        closing_item_id: extracted.closing_item_id,
        event: extracted.event,
      });

      const result = {
        ok: false,
        reason: "title_routing_not_found",
        extracted,
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "title_webhook",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          event_id: extracted.event_id,
          title_routing_item_id: extracted.title_routing_item_id,
          closing_item_id: extracted.closing_item_id,
        },
      });

      await recordSystemAlert({
        subsystem: "title_webhook",
        code: "title_routing_not_found",
        severity: "high",
        retryable: true,
        summary: "Title webhook could not find a matching title routing record.",
        dedupe_key: `title-webhook:${clean(extracted.title_routing_item_id) || clean(extracted.closing_item_id) || idempotency_key}`,
        metadata: {
          event_id: extracted.event_id,
          closing_item_id: extracted.closing_item_id,
        },
      });

      return result;
    }

    if (extracted.closing_item_id) {
      closing_item = await runtimeDeps.getClosingItem(extracted.closing_item_id);
    }

    if (!closing_item && extracted.closing_item_id) {
      const closing_matches = await runtimeDeps.findClosingItems(
        { [CLOSING_FIELDS.closing_id]: extracted.closing_item_id },
        10,
        0
      );
      closing_item = sortNewestFirst(closing_matches)[0] || null;
    }

    if (!closing_item) {
      closing_item = await findLatestClosingByTitleRoutingId(title_routing_item.item_id);
    }

    const classified = runtimeDeps.classifyTitleResponse({
      event: extracted.event,
      subject: extracted.subject,
      body: extracted.body,
      sender_email: extracted.sender_email,
    });

    if (!classified.routing_status && !classified.closing_status) {
      runtimeDeps.info("title.response_unclassified", {
        title_routing_item_id: title_routing_item.item_id,
        closing_item_id: closing_item?.item_id || null,
        subject: extracted.subject,
        event: extracted.event,
        confidence: classified.confidence,
        reason: classified.reason,
      });

      const result = {
        ok: true,
        updated: false,
        reason: classified.reason,
        title_routing_item_id: title_routing_item.item_id,
        closing_item_id: closing_item?.item_id || null,
        normalized_event: classified.normalized_event,
        classification: classified,
        idempotency_key,
      };

      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "title_webhook",
        key: idempotency_key,
        summary: `Title webhook completed ${idempotency_key}`,
        metadata: {
          event_id: extracted.event_id,
          title_routing_item_id: title_routing_item.item_id,
          closing_item_id: closing_item?.item_id || null,
          normalized_event: classified.normalized_event,
          result_reason: classified.reason,
        },
      });

      return result;
    }

    const note_parts = [
      `Title response processed: ${classified.normalized_event}.`,
      classified.sender_email ? `From: ${classified.sender_email}.` : "",
      classified.subject ? `Subject: ${classified.subject}.` : "",
    ].filter(Boolean);

    const title_routing_update = await runtimeDeps.updateTitleRoutingStatus({
      title_routing_item_id: title_routing_item.item_id,
      title_routing_item,
      status: classified.routing_status,
      notes: note_parts.join(" "),
    });

    let closing_update = null;

    if (closing_item?.item_id && classified.closing_status) {
      closing_update = await runtimeDeps.updateClosingStatus({
        closing_item_id: closing_item.item_id,
        closing_item,
        status: classified.closing_status,
        notes: note_parts.join(" "),
      });
    }

    runtimeDeps.info("title.response_processed", {
      title_routing_item_id: title_routing_item.item_id,
      closing_item_id: closing_item?.item_id || null,
      normalized_event: classified.normalized_event,
      routing_status: classified.routing_status,
      closing_status: classified.closing_status,
      confidence: classified.confidence,
      title_routing_updated: Boolean(title_routing_update?.updated),
      closing_updated: Boolean(closing_update?.updated),
    });

    const result = {
      ok: true,
      updated: true,
      reason: classified.reason,
      normalized_event: classified.normalized_event,
      title_routing_item_id: title_routing_item.item_id,
      closing_item_id: closing_item?.item_id || null,
      classification: classified,
      title_routing_update,
      closing_update,
      extracted,
      idempotency_key,
    };

    await runtimeDeps.completeIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "title_webhook",
      key: idempotency_key,
      summary: `Title webhook completed ${idempotency_key}`,
      metadata: {
        event_id: extracted.event_id,
        title_routing_item_id: title_routing_item.item_id,
        closing_item_id: closing_item?.item_id || null,
        normalized_event: classified.normalized_event,
        routing_status: classified.routing_status,
        closing_status: classified.closing_status,
      },
    });

    return result;
  } catch (error) {
    await runtimeDeps.failIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "title_webhook",
      key: idempotency_key,
      error,
      metadata: {
        event_id: extracted.event_id,
        title_routing_item_id: extracted.title_routing_item_id,
        closing_item_id: extracted.closing_item_id,
      },
    });

    await recordSystemAlert({
      subsystem: "title_webhook",
      code: "handler_failed",
      severity: "high",
      retryable: true,
      summary: `Title webhook handler failed: ${clean(error?.message) || "unknown_error"}`,
      dedupe_key: `title-webhook:${clean(extracted.title_routing_item_id) || clean(extracted.closing_item_id) || idempotency_key}`,
      metadata: {
        event_id: extracted.event_id,
        title_routing_item_id: extracted.title_routing_item_id,
        closing_item_id: extracted.closing_item_id,
      },
    });

    throw error;
  }
}

export default handleTitleResponseWebhook;
