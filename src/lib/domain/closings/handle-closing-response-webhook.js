// ─── handle-closing-response-webhook.js ──────────────────────────────────
import {
  CLOSING_FIELDS,
  getClosingItem,
  findClosingItems,
} from "@/lib/podio/apps/closings.js";
import { maybeMarkClosed } from "@/lib/domain/closings/maybe-mark-closed.js";
import { createDealRevenueFromClosedClosing } from "@/lib/domain/revenue/create-deal-revenue-from-closed-closing.js";
import { updateClosingStatus } from "@/lib/domain/closings/update-closing-status.js";
import {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { info, warn } from "@/lib/logging/logger.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

const defaultDeps = {
  getClosingItem,
  findClosingItems,
  maybeMarkClosed,
  createDealRevenueFromClosedClosing,
  updateClosingStatus,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setClosingWebhookTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetClosingWebhookTestDeps() {
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

  const closing_item_id =
    payload.closing_item_id ||
    payload.closingItemId ||
    payload.closing_id ||
    payload.closingId ||
    null;

  const closing_record_id =
    payload.closing_record_id ||
    payload.closingRecordId ||
    null;

  const title_routing_item_id =
    payload.title_routing_item_id ||
    payload.titleRoutingItemId ||
    payload.title_routing_id ||
    payload.titleRoutingId ||
    null;

  const event =
    payload.event ||
    payload.event_type ||
    payload.eventType ||
    payload.status ||
    payload.data?.event ||
    "";

  const status =
    payload.status ||
    payload.closing_status ||
    payload.closingStatus ||
    payload.data?.status ||
    "";

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
    closing_item_id: clean(closing_item_id) || null,
    closing_record_id: clean(closing_record_id) || null,
    title_routing_item_id: clean(title_routing_item_id) || null,
    event: clean(event),
    status: clean(status),
    subject: clean(subject),
    body: clean(body),
    sender_email: clean(sender_email),
  };
}

function buildClosingWebhookIdempotencyKey(extracted = {}) {
  return (
    clean(extracted.event_id) ||
    runtimeDeps.hashIdempotencyPayload({
      provider: "closings",
      closing_item_id: clean(extracted.closing_item_id) || null,
      closing_record_id: clean(extracted.closing_record_id) || null,
      title_routing_item_id: clean(extracted.title_routing_item_id) || null,
      event: clean(extracted.event) || null,
      status: clean(extracted.status) || null,
      subject: clean(extracted.subject) || null,
      body: clean(extracted.body) || null,
      sender_email: clean(extracted.sender_email) || null,
      raw: extracted.raw || null,
    })
  );
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

async function findLatestClosingByClosingId(closing_id) {
  if (!closing_id) return null;

  const matches = await runtimeDeps.findClosingItems(
    { [CLOSING_FIELDS.closing_id]: closing_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

function normalizeClosingEvent({ event = "", status = "", subject = "", body = "" } = {}) {
  const combined = `${clean(event)} ${clean(status)} ${clean(subject)} ${clean(body)}`;
  const text = lower(combined);

  if (
    includesAny(text, [
      "funded",
      "funding complete",
      "wire sent",
      "wired",
      "disbursed",
      "disbursement complete",
      "funds released",
      "funds sent",
    ])
  ) {
    return {
      normalized_event: "funded",
      closing_status: "Completed",
      reason: "funded_signal_detected",
      should_mark_closed: true,
    };
  }

  if (
    includesAny(text, [
      "recorded",
      "recording confirmed",
      "deed recorded",
      "document recorded",
    ])
  ) {
    return {
      normalized_event: "recorded",
      closing_status: "Completed",
      reason: "recorded_signal_detected",
      should_mark_closed: true,
    };
  }

  if (
    includesAny(text, [
      "closed",
      "closing complete",
      "deal complete",
      "file complete",
      "completed closing",
    ])
  ) {
    return {
      normalized_event: "closed",
      closing_status: "Completed",
      reason: "closed_signal_detected",
      should_mark_closed: true,
    };
  }

  if (
    includesAny(text, [
      "clear to close",
      "ctc",
      "cleared to close",
      "ready to close",
    ])
  ) {
    return {
      normalized_event: "clear_to_close",
      closing_status: "Clear to Close",
      reason: "clear_to_close_signal_detected",
      should_mark_closed: false,
    };
  }

  if (
    includesAny(text, [
      "scheduled to close",
      "closing scheduled",
      "close scheduled",
      "set to close",
      "closing date",
    ])
  ) {
    return {
      normalized_event: "scheduled",
      closing_status: "Scheduled",
      reason: "scheduled_close_signal_detected",
      should_mark_closed: false,
    };
  }

  if (
    includesAny(text, [
      "cancelled",
      "canceled",
      "terminated",
      "fell through",
      "won't close",
      "will not close",
      "deal cancelled",
      "deal canceled",
    ])
  ) {
    return {
      normalized_event: "cancelled",
      closing_status: "Cancelled",
      reason: "cancelled_signal_detected",
      should_mark_closed: false,
    };
  }

  if (
    includesAny(text, [
      "awaiting docs",
      "need docs",
      "need documents",
      "missing docs",
      "requesting docs",
    ])
  ) {
    return {
      normalized_event: "awaiting_docs",
      closing_status: "Pending Docs",
      reason: "pending_docs_signal_detected",
      should_mark_closed: false,
    };
  }

  return {
    normalized_event: "unclassified",
    closing_status: null,
    reason: "unclassified_closing_response",
    should_mark_closed: false,
  };
}

export async function handleClosingResponseWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);
  const idempotency_key = buildClosingWebhookIdempotencyKey(extracted);

  runtimeDeps.info("closing.response_received", {
    event_id: extracted.event_id,
    closing_item_id: extracted.closing_item_id,
    closing_record_id: extracted.closing_record_id,
    title_routing_item_id: extracted.title_routing_item_id,
    event: extracted.event,
    status: extracted.status,
    sender_email: extracted.sender_email,
  });

  const idempotency = await runtimeDeps.beginIdempotentProcessing({
    scope: "closing_webhook",
    key: idempotency_key,
    summary: `Processed closing event ${idempotency_key}`,
    metadata: {
      event_id: extracted.event_id,
      closing_item_id: extracted.closing_item_id,
      closing_record_id: extracted.closing_record_id,
      title_routing_item_id: extracted.title_routing_item_id,
      event: extracted.event,
      status: extracted.status,
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
    runtimeDeps.info("closing.response_duplicate_ignored", {
      event_id: extracted.event_id,
      closing_item_id: extracted.closing_item_id,
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
    let closing_item = null;

    if (extracted.closing_item_id) {
      closing_item = await runtimeDeps.getClosingItem(extracted.closing_item_id);
    }

    if (!closing_item && extracted.closing_record_id) {
      closing_item = await findLatestClosingByClosingId(extracted.closing_record_id);
    }

    if (!closing_item && extracted.title_routing_item_id) {
      closing_item = await findLatestClosingByTitleRoutingId(
        extracted.title_routing_item_id
      );
    }

    if (!closing_item?.item_id) {
      runtimeDeps.warn("closing.response_not_found", {
        closing_item_id: extracted.closing_item_id,
        closing_record_id: extracted.closing_record_id,
        title_routing_item_id: extracted.title_routing_item_id,
        event: extracted.event,
        status: extracted.status,
      });

      const result = {
        ok: false,
        reason: "closing_not_found",
        extracted,
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "closing_webhook",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          event_id: extracted.event_id,
          closing_item_id: extracted.closing_item_id,
          closing_record_id: extracted.closing_record_id,
        },
      });

      await recordSystemAlert({
        subsystem: "closing_webhook",
        code: "closing_not_found",
        severity: "high",
        retryable: true,
        summary: "Closing webhook could not find a matching closing record.",
        dedupe_key: `closing-webhook:${clean(extracted.closing_item_id) || clean(extracted.closing_record_id) || idempotency_key}`,
        metadata: {
          event_id: extracted.event_id,
          title_routing_item_id: extracted.title_routing_item_id,
        },
      });

      return result;
    }

    const normalized = normalizeClosingEvent({
      event: extracted.event,
      status: extracted.status,
      subject: extracted.subject,
      body: extracted.body,
    });

    if (normalized.normalized_event === "unclassified") {
      runtimeDeps.info("closing.response_unclassified", {
        closing_item_id: closing_item.item_id,
        event: extracted.event,
        status: extracted.status,
        subject: extracted.subject,
      });

      const result = {
        ok: true,
        updated: false,
        reason: normalized.reason,
        closing_item_id: closing_item.item_id,
        normalized_event: normalized.normalized_event,
        extracted,
        idempotency_key,
      };

      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "closing_webhook",
        key: idempotency_key,
        summary: `Closing webhook completed ${idempotency_key}`,
        metadata: {
          event_id: extracted.event_id,
          closing_item_id: closing_item.item_id,
          normalized_event: normalized.normalized_event,
          result_reason: normalized.reason,
        },
      });

      return result;
    }

    const note = [
      `Closing response processed: ${normalized.normalized_event}.`,
      extracted.sender_email ? `From: ${extracted.sender_email}.` : "",
      extracted.subject ? `Subject: ${extracted.subject}.` : "",
    ].filter(Boolean).join(" ");

    let closing_update = null;
    let close_result = null;
    let revenue = null;

    if (normalized.should_mark_closed) {
      close_result = await runtimeDeps.maybeMarkClosed({
        closing_item_id: closing_item.item_id,
        closing_item,
        event: extracted.event,
        status: extracted.status,
        subject: extracted.subject,
        body: extracted.body,
        notes: note,
      });

      const refreshed_closing_item = await runtimeDeps.getClosingItem(
        closing_item.item_id
      );

      revenue = await runtimeDeps.createDealRevenueFromClosedClosing({
        closing_item_id: closing_item.item_id,
        closing_item: refreshed_closing_item,
        notes: `Created from closing response webhook: ${normalized.normalized_event}`,
      });
    } else if (normalized.closing_status) {
      closing_update = await runtimeDeps.updateClosingStatus({
        closing_item_id: closing_item.item_id,
        closing_item,
        status: normalized.closing_status,
        notes: note,
      });
    }

    runtimeDeps.info("closing.response_processed", {
      closing_item_id: closing_item.item_id,
      normalized_event: normalized.normalized_event,
      closing_status: normalized.closing_status,
      closing_updated: Boolean(closing_update?.updated),
      closing_marked_closed: Boolean(close_result?.updated),
      revenue_created: Boolean(revenue?.created),
      deal_revenue_item_id: revenue?.deal_revenue_item_id || null,
    });

    const result = {
      ok: true,
      updated: true,
      reason: normalized.reason,
      closing_item_id: closing_item.item_id,
      normalized_event: normalized.normalized_event,
      closing_status: normalized.closing_status,
      closing_update,
      close_result,
      revenue,
      extracted,
      idempotency_key,
    };

    await runtimeDeps.completeIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "closing_webhook",
      key: idempotency_key,
      summary: `Closing webhook completed ${idempotency_key}`,
      metadata: {
        event_id: extracted.event_id,
        closing_item_id: closing_item.item_id,
        normalized_event: normalized.normalized_event,
        closing_status: normalized.closing_status,
        revenue_item_id: revenue?.deal_revenue_item_id || null,
      },
    });

    return result;
  } catch (error) {
    await runtimeDeps.failIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "closing_webhook",
      key: idempotency_key,
      error,
      metadata: {
        event_id: extracted.event_id,
        closing_item_id: extracted.closing_item_id,
        closing_record_id: extracted.closing_record_id,
      },
    });

    await recordSystemAlert({
      subsystem: "closing_webhook",
      code: "handler_failed",
      severity: "high",
      retryable: true,
      summary: `Closing webhook handler failed: ${clean(error?.message) || "unknown_error"}`,
      dedupe_key: `closing-webhook:${clean(extracted.closing_item_id) || clean(extracted.closing_record_id) || idempotency_key}`,
      metadata: {
        event_id: extracted.event_id,
        closing_item_id: extracted.closing_item_id,
        closing_record_id: extracted.closing_record_id,
      },
    });

    throw error;
  }
}

export default handleClosingResponseWebhook;
