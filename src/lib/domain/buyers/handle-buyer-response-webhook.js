import {
  BUYER_MATCH_FIELDS,
  getBuyerMatchItem,
  updateBuyerMatchItem,
} from "@/lib/podio/apps/buyer-match.js";
import { getCompanyItem } from "@/lib/podio/apps/companies.js";
import {
  findMessageEventByMessageId,
  findMessageEvents,
  createMessageEvent,
} from "@/lib/podio/apps/message-events.js";
import { upsertBuyerDispositionThread } from "@/lib/domain/buyers/buyer-threads.js";
import { classifyBuyerResponse } from "@/lib/domain/buyers/classify-buyer-response.js";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { normalizeInboundTextgridPhone } from "@/lib/providers/textgrid.js";
import {
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";
import { info, warn } from "@/lib/logging/logger.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function addDaysIso(days = 0) {
  const next = new Date();
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
  return next.toISOString();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function normalizePhoneToken(value = "") {
  const normalized = normalizeInboundTextgridPhone(value);
  if (normalized) return normalized;

  const digits = clean(value).replace(/\D/g, "");
  return digits.length >= 10 ? digits.slice(-10) : clean(value);
}

function parseJson(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function toItems(result) {
  if (Array.isArray(result)) return result;
  if (Array.isArray(result?.items)) return result.items;
  return [];
}

function sortNewestFirst(items = []) {
  return [...items].sort((left, right) => Number(right?.item_id || 0) - Number(left?.item_id || 0));
}

function appendNotes(...values) {
  return values
    .map((value) => clean(value))
    .filter(Boolean)
    .join("\n");
}

function extractWebhookPayload(payload = {}) {
  const attachments = safeArray(
    payload.attachments ||
      payload.files ||
      payload.data?.attachments ||
      []
  );
  const raw_sender =
    clean(payload.from || payload.sender || payload.sender_email || payload.senderEmail || payload.data?.from) ||
    null;
  const explicit_sender_email =
    clean(payload.sender_email || payload.senderEmail || payload.data?.from_email) || null;

  return {
    raw: payload,
    event_id:
      clean(
        payload.event_id ||
          payload.eventId ||
          payload.message_id ||
          payload.messageId ||
          payload.email_message_id ||
          payload.emailMessageId ||
          payload.thread_id ||
          payload.threadId
      ) || null,
    message_id:
      clean(payload.message_id || payload.messageId || payload.email_message_id || payload.emailMessageId) || null,
    inbound_message_id:
      clean(payload.inbound_message_id || payload.inboundMessageId) || null,
    in_reply_to:
      clean(payload.in_reply_to || payload.inReplyTo || payload.reply_to_message_id || payload.replyToMessageId) || null,
    original_message_id:
      clean(payload.original_message_id || payload.originalMessageId) || null,
    thread_id: clean(payload.thread_id || payload.threadId) || null,
    buyer_match_item_id:
      clean(payload.buyer_match_item_id || payload.buyerMatchItemId || payload.buyer_match_id || payload.buyerMatchId) || null,
    company_item_id:
      clean(payload.company_item_id || payload.companyItemId || payload.buyer_company_item_id || payload.buyerCompanyItemId) || null,
    event: clean(payload.event || payload.event_type || payload.eventType || payload.status || ""),
    status: clean(payload.status || payload.delivery_status || payload.deliveryStatus || ""),
    subject: clean(payload.subject || payload.email_subject || payload.emailSubject || payload.data?.subject || ""),
    body: clean(payload.body || payload.message || payload.text || payload.content || payload.data?.body || ""),
    sender_email:
      explicit_sender_email ||
      (raw_sender && raw_sender.includes("@") ? raw_sender : null),
    sender_phone:
      clean(
        payload.from_phone ||
          payload.fromPhone ||
          payload.sender_phone ||
          payload.senderPhone ||
          payload.phone ||
          payload.msisdn ||
          payload.data?.from_phone
      ) || null,
    channel:
      clean(payload.channel || payload.message_channel || payload.messageChannel) ||
      (clean(
        payload.from_phone ||
          payload.fromPhone ||
          payload.sender_phone ||
          payload.senderPhone ||
          payload.phone ||
          payload.msisdn
      )
        ? "sms"
        : "email"),
    attachments_count:
      Number(payload.attachments_count || payload.attachmentsCount || attachments.length || 0) || 0,
  };
}

function buildBuyerWebhookIdempotencyKey(extracted = {}) {
  return (
    clean(extracted.event_id) ||
    clean(extracted.message_id) ||
    runtimeDeps.hashIdempotencyPayload({
      provider: "buyers",
      in_reply_to: extracted.in_reply_to,
      original_message_id: extracted.original_message_id,
      sender_email: extracted.sender_email,
      sender_phone: extracted.sender_phone,
      subject: extracted.subject,
      body: extracted.body,
      event: extracted.event,
      status: extracted.status,
    })
  );
}

function parseBuyerBlastMeta(event = null) {
  const meta = parseJson(getTextValue(event, "ai-output", ""));
  const trigger_name = clean(getTextValue(event, "trigger-name", ""));
  const match = trigger_name.match(/^buyer-blast:(\d+):(\d+)$/);
  const normalized_recipient_phone = normalizePhoneToken(meta?.recipient_phone);

  return {
    ...meta,
    buyer_match_item_id:
      clean(meta?.buyer_match_item_id) ||
      clean(match?.[1]) ||
      null,
    company_item_id:
      clean(meta?.company_item_id) ||
      clean(match?.[2]) ||
      null,
    recipient_email:
      clean(meta?.recipient_email) || null,
    recipient_phone:
      normalized_recipient_phone || clean(meta?.recipient_phone) || null,
    provider_message_id:
      clean(meta?.provider_message_id) || clean(getTextValue(event, "message-id", "")) || null,
  };
}

function buildBuyerResponseTriggerName(buyer_match_item_id = null, company_item_id = null) {
  return `buyer-response:${clean(buyer_match_item_id)}:${clean(company_item_id)}`;
}

const MATCH_STATUS_RANK = Object.freeze({
  "not started": 0,
  matching: 1,
  "buyers selected": 2,
  "sent to buyers": 3,
  "buyers interested": 4,
  "buyers chosen": 5,
  assigned: 6,
  closed: 7,
  dead: 8,
});

const RESPONSE_STATUS_RANK = Object.freeze({
  "not sent": 0,
  sent: 1,
  opened: 2,
  "needs more info": 3,
  interested: 4,
  passed: 2,
  "offer submitted": 5,
  selected: 6,
});

function chooseDominantStatus(current = "", next = "", rank_map = {}) {
  const current_rank = rank_map[lower(current)] ?? -1;
  const next_rank = rank_map[lower(next)] ?? -1;
  return next_rank > current_rank ? next : current;
}

async function resolveBuyerBlastEvent(extracted = {}) {
  const exact_message_ids = [
    extracted.in_reply_to,
    extracted.original_message_id,
    extracted.buyer_match_item_id,
  ].filter(Boolean);

  for (const message_id of exact_message_ids) {
    const direct = await runtimeDeps.findMessageEventByMessageId(message_id);
    if (!direct?.item_id) continue;

    const meta = parseBuyerBlastMeta(direct);
    if (meta?.buyer_match_item_id) {
      return {
        matched_event: direct,
        meta,
        correlation_mode: "exact_message_id",
      };
    }
  }

  const recent_events = sortNewestFirst(
    toItems(
      await runtimeDeps.findMessageEvents(
        { "source-app": "Buyer Disposition" },
        150,
        0
      )
    )
  );

  const sender_email = lower(extracted.sender_email);
  const sender_phone = normalizePhoneToken(extracted.sender_phone);
  const buyer_match_filter = clean(extracted.buyer_match_item_id);

  const matched_event = recent_events.find((event) => {
    const meta = parseBuyerBlastMeta(event);
    if (!meta?.buyer_match_item_id) return false;
    if (buyer_match_filter && clean(meta.buyer_match_item_id) !== buyer_match_filter) return false;
    if (sender_email && lower(meta.recipient_email) === sender_email) return true;
    if (sender_phone && normalizePhoneToken(meta.recipient_phone) === sender_phone) return true;
    if (sender_email || sender_phone) return false;
    return true;
  });

  if (!matched_event?.item_id) {
    return {
      matched_event: null,
      meta: null,
      correlation_mode: "not_found",
    };
  }

  return {
    matched_event,
    meta: parseBuyerBlastMeta(matched_event),
    correlation_mode: sender_phone && !sender_email
      ? "sender_phone_recent_blast"
      : "sender_email_recent_blast",
  };
}

async function logBuyerResponseEvent({
  extracted = {},
  buyer_match_item = null,
  company_item = null,
  classification = null,
  matched_event = null,
} = {}) {
  return runtimeDeps.createMessageEvent({
    "message-id":
      clean(extracted.message_id || extracted.event_id || extracted.inbound_message_id) ||
      `buyer-response:${clean(buyer_match_item?.item_id)}:${nowIso()}`,
    "timestamp": { start: nowIso() },
    "direction": "Inbound",
    "source-app": "Buyer Disposition",
    "processed-by": "Buyer Response Webhook",
    "trigger-name": buildBuyerResponseTriggerName(
      buyer_match_item?.item_id || null,
      company_item?.item_id || null
    ),
    "message": extracted.body || extracted.subject || extracted.event || "Buyer response received",
    "status-3": classification?.buyer_response_status || "Inbound",
    "status-2": classification?.normalized_response || "received",
    "property": getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.property, null)
      ? [getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.property, null)]
      : undefined,
    "master-owner": getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.master_owner, null)
      ? [getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.master_owner, null)]
      : undefined,
    "ai-output": JSON.stringify({
      version: 1,
      event_kind: "buyer_response",
      buyer_match_item_id: buyer_match_item?.item_id || null,
      company_item_id: company_item?.item_id || null,
      company_name: clean(company_item?.title) || null,
      sender_email: extracted.sender_email || null,
      classification,
      matched_blast_event_item_id: matched_event?.item_id || null,
      matched_blast_message_id: clean(getTextValue(matched_event, "message-id", "")) || null,
    }),
  });
}

function buildBuyerMatchUpdatePayload({
  buyer_match_item = null,
  classification = null,
  company_item = null,
  extracted = {},
} = {}) {
  const current_match_status = clean(
    getCategoryValue(buyer_match_item, BUYER_MATCH_FIELDS.match_status, "")
  );
  const current_response_status = clean(
    getCategoryValue(buyer_match_item, BUYER_MATCH_FIELDS.buyer_response_status, "")
  );
  const current_assignment_status = clean(
    getCategoryValue(buyer_match_item, BUYER_MATCH_FIELDS.assignment_status, "")
  );
  const current_dispo_outcome = clean(
    getCategoryValue(buyer_match_item, BUYER_MATCH_FIELDS.dispo_outcome, "")
  );

  const next_match_status =
    classification?.match_status
      ? chooseDominantStatus(current_match_status, classification.match_status, MATCH_STATUS_RANK)
      : current_match_status;
  const next_response_status =
    classification?.buyer_response_status
      ? chooseDominantStatus(current_response_status, classification.buyer_response_status, RESPONSE_STATUS_RANK)
      : current_response_status;
  const next_assignment_status =
    classification?.assignment_status
      ? chooseDominantStatus(current_assignment_status, classification.assignment_status, {
          "not started": 0,
          "in progress": 1,
          "buyer confirmed": 2,
          assigned: 3,
          closed: 4,
          cancelled: 5,
        })
      : current_assignment_status;

  const note = `[${nowIso()}] Buyer response from ${
    clean(company_item?.title) || extracted.sender_email || "buyer"
  }: ${classification?.normalized_response || "received"}${clean(extracted.subject) ? ` | ${clean(extracted.subject)}` : ""}${clean(extracted.body) ? ` | ${clean(extracted.body).slice(0, 280)}` : ""}`;

  const payload = {
    [BUYER_MATCH_FIELDS.buyer_response_status]: next_response_status || undefined,
    [BUYER_MATCH_FIELDS.match_status]: next_match_status || undefined,
    [BUYER_MATCH_FIELDS.assignment_status]: next_assignment_status || undefined,
    [BUYER_MATCH_FIELDS.automation_status]:
      ["Selected", "Passed"].includes(classification?.buyer_response_status)
        ? "Running"
        : "Waiting",
    [BUYER_MATCH_FIELDS.next_buyer_follow_up]: {
      start:
        classification?.normalized_response === "chosen"
          ? nowIso()
          : classification?.normalized_response === "passed"
            ? addDaysIso(1)
            : nowIso(),
    },
    [BUYER_MATCH_FIELDS.buyer_notes]: appendNotes(
      getTextValue(buyer_match_item, BUYER_MATCH_FIELDS.buyer_notes, ""),
      note
    ),
    [BUYER_MATCH_FIELDS.internal_notes]: appendNotes(
      getTextValue(buyer_match_item, BUYER_MATCH_FIELDS.internal_notes, ""),
      note
    ),
  };

  if (classification?.proof_of_funds_received) {
    payload[BUYER_MATCH_FIELDS.buyer_proof_of_funds_received] = "Yes";
  }

  if (classification?.normalized_response === "chosen" && company_item?.item_id) {
    payload[BUYER_MATCH_FIELDS.selected_buyer] = [company_item.item_id];
    payload[BUYER_MATCH_FIELDS.buyer_assigned_date] = { start: nowIso() };
    payload[BUYER_MATCH_FIELDS.dispo_outcome] =
      current_dispo_outcome || classification.dispo_outcome || "Buyer Secured";
  }

  if (
    classification?.normalized_response === "passed" &&
    !lower(current_dispo_outcome) &&
    !["buyers interested", "buyers chosen", "assigned", "closed"].includes(lower(current_match_status))
  ) {
    delete payload[BUYER_MATCH_FIELDS.dispo_outcome];
  }

  return payload;
}

const defaultDeps = {
  getBuyerMatchItem,
  updateBuyerMatchItem,
  getCompanyItem,
  findMessageEventByMessageId,
  findMessageEvents,
  createMessageEvent,
  classifyBuyerResponse,
  upsertBuyerDispositionThread,
  syncPipelineState,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setBuyerWebhookTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetBuyerWebhookTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function maybeHandleBuyerTextgridInbound(payload = {}) {
  const extracted = extractWebhookPayload({
    ...payload,
    channel: "sms",
    from_phone:
      payload?.from_phone ||
      payload?.fromPhone ||
      payload?.from ||
      payload?.sender ||
      payload?.msisdn ||
      null,
    body:
      payload?.body ||
      payload?.message ||
      payload?.text ||
      payload?.content ||
      "",
    message_id:
      payload?.message_id ||
      payload?.messageId ||
      payload?.id ||
      null,
    event_id:
      payload?.event_id ||
      payload?.eventId ||
      payload?.message_id ||
      payload?.messageId ||
      payload?.id ||
      null,
  });

  const sender_phone = normalizePhoneToken(extracted.sender_phone);
  if (!sender_phone && !clean(extracted.buyer_match_item_id) && !clean(extracted.company_item_id)) {
    return {
      ok: true,
      matched: false,
      reason: "buyer_sms_missing_phone",
    };
  }

  const resolution = await resolveBuyerBlastEvent({
    ...extracted,
    sender_phone,
  });

  if (!clean(resolution?.meta?.buyer_match_item_id)) {
    return {
      ok: true,
      matched: false,
      reason: "buyer_sms_context_not_found",
      correlation_mode: resolution?.correlation_mode || "not_found",
    };
  }

  const result = await handleBuyerResponseWebhook({
    ...payload,
    channel: "sms",
    from_phone: sender_phone,
    buyer_match_item_id:
      clean(extracted.buyer_match_item_id) ||
      clean(resolution?.meta?.buyer_match_item_id) ||
      null,
    company_item_id:
      clean(extracted.company_item_id) ||
      clean(resolution?.meta?.company_item_id) ||
      null,
    body: extracted.body,
    message_id: extracted.message_id,
    event_id: extracted.event_id || extracted.message_id,
  });

  return {
    ok: result?.ok !== false,
    matched: true,
    correlation_mode: resolution?.correlation_mode || null,
    result,
  };
}

export async function handleBuyerResponseWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);
  extracted.sender_phone = normalizePhoneToken(extracted.sender_phone);
  const idempotency_key = buildBuyerWebhookIdempotencyKey(extracted);

  runtimeDeps.info("buyer.response_received", {
    event_id: extracted.event_id,
    sender_email: extracted.sender_email,
    buyer_match_item_id: extracted.buyer_match_item_id,
  });

  const idempotency = await runtimeDeps.beginIdempotentProcessing({
    scope: "buyer_webhook",
    key: idempotency_key,
    summary: `Processed buyer response ${idempotency_key}`,
    metadata: {
      event_id: extracted.event_id,
      sender_email: extracted.sender_email,
      buyer_match_item_id: extracted.buyer_match_item_id,
    },
  });

  if (!idempotency.ok) {
    return {
      ok: false,
      reason: idempotency.reason,
      idempotency_key,
    };
  }

  if (idempotency.duplicate) {
    return {
      ok: true,
      duplicate: true,
      updated: false,
      reason: idempotency.reason,
      idempotency_key,
    };
  }

  try {
    const resolution = await resolveBuyerBlastEvent(extracted);
    const resolved_buyer_match_item_id =
      clean(extracted.buyer_match_item_id) ||
      clean(resolution?.meta?.buyer_match_item_id) ||
      null;
    const resolved_company_item_id =
      clean(extracted.company_item_id) ||
      clean(resolution?.meta?.company_item_id) ||
      null;

    if (!resolved_buyer_match_item_id) {
      const result = {
        ok: false,
        reason: "buyer_blast_context_not_found",
        correlation_mode: resolution?.correlation_mode || "not_found",
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "buyer_webhook",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          sender_email: extracted.sender_email,
          correlation_mode: result.correlation_mode,
        },
      });

      return result;
    }

    const buyer_match_item = await runtimeDeps.getBuyerMatchItem(resolved_buyer_match_item_id);
    if (!buyer_match_item?.item_id) {
      const result = {
        ok: false,
        reason: "buyer_match_not_found",
        buyer_match_item_id: resolved_buyer_match_item_id,
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "buyer_webhook",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          buyer_match_item_id: resolved_buyer_match_item_id,
        },
      });

      return result;
    }

    const company_item = resolved_company_item_id
      ? await runtimeDeps.getCompanyItem(resolved_company_item_id)
      : null;
    const classification = runtimeDeps.classifyBuyerResponse({
      event: extracted.event,
      status: extracted.status,
      subject: extracted.subject,
      body: extracted.body,
      attachments_count: extracted.attachments_count,
    });

    const response_event = await logBuyerResponseEvent({
      extracted,
      buyer_match_item,
      company_item,
      classification,
      matched_event: resolution?.matched_event || null,
    });

    try {
      await runtimeDeps.upsertBuyerDispositionThread({
        buyer_match_item,
        company_item,
        company_item_id: resolved_company_item_id,
        company_name:
          clean(company_item?.title) ||
          clean(resolution?.meta?.company_name) ||
          extracted.sender_email ||
          extracted.sender_phone ||
          "Partner",
        recipient_email:
          clean(extracted.sender_email) ||
          clean(resolution?.meta?.recipient_email) ||
          null,
        recipient_phone:
          clean(extracted.sender_phone) ||
          clean(resolution?.meta?.recipient_phone) ||
          null,
        channel: clean(extracted.channel) || "email",
        direction: "Inbound",
        interaction_kind: "response_received",
        interaction_status:
          clean(classification?.buyer_response_status) ||
          clean(classification?.normalized_response) ||
          "Inbound",
        subject: extracted.subject,
        message: extracted.body,
        provider_message_id:
          clean(extracted.message_id || extracted.event_id || extracted.inbound_message_id) || null,
        related_event_item_id: response_event?.item_id || null,
        classification,
        metadata: {
          correlation_mode: resolution?.correlation_mode || null,
        },
        timestamp: nowIso(),
      });
    } catch (thread_error) {
      runtimeDeps.warn("buyer.thread_sync_failed", {
        error: thread_error,
        buyer_match_item_id: buyer_match_item.item_id,
        company_item_id: resolved_company_item_id,
      });
    }

    if (!classification?.ok) {
      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "buyer_webhook",
        key: idempotency_key,
        summary: `Buyer response ${idempotency_key} received but not classified`,
        metadata: {
          buyer_match_item_id: buyer_match_item.item_id,
          company_item_id: company_item?.item_id || null,
          classification,
        },
      });

      return {
        ok: true,
        updated: false,
        reason: classification?.reason || "buyer_response_unclassified",
        buyer_match_item_id: buyer_match_item.item_id,
        company_item_id: company_item?.item_id || null,
        classification,
        correlation_mode: resolution?.correlation_mode || null,
      };
    }

    const payload = buildBuyerMatchUpdatePayload({
      buyer_match_item,
      classification,
      company_item,
      extracted,
    });

    await runtimeDeps.updateBuyerMatchItem(buyer_match_item.item_id, payload);

    const pipeline = await runtimeDeps.syncPipelineState({
      property_id: getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.property, null),
      master_owner_id: getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.master_owner, null),
      contract_item_id: getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.contract, null),
      closing_item_id: getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.closing, null),
      buyer_match_item_id: buyer_match_item.item_id,
      market_id: getFirstAppReferenceId(buyer_match_item, BUYER_MATCH_FIELDS.market, null),
      notes: `Buyer response ${classification.normalized_response} received from ${
        clean(company_item?.title) || extracted.sender_email || "buyer"
      }.`,
    });

    await runtimeDeps.completeIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "buyer_webhook",
      key: idempotency_key,
      summary: `Buyer response ${idempotency_key} processed`,
      metadata: {
        buyer_match_item_id: buyer_match_item.item_id,
        company_item_id: company_item?.item_id || null,
        classification,
      },
    });

    return {
      ok: true,
      updated: true,
      reason: "buyer_response_processed",
      buyer_match_item_id: buyer_match_item.item_id,
      company_item_id: company_item?.item_id || null,
      classification,
      correlation_mode: resolution?.correlation_mode || null,
      pipeline,
    };
  } catch (error) {
    await runtimeDeps.failIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "buyer_webhook",
      key: idempotency_key,
      error,
      metadata: {
        sender_email: extracted.sender_email,
      },
    });

    runtimeDeps.warn("buyer.response_failed", {
      error,
      sender_email: extracted.sender_email,
    });

    await recordSystemAlert({
      subsystem: "buyer_responses",
      code: "webhook_failed",
      severity: "high",
      retryable: true,
      summary: `Buyer response ingestion failed: ${clean(error?.message) || "unknown_error"}`,
      dedupe_key: `buyer-response:${clean(extracted.sender_email) || "unknown"}`,
      metadata: {
        sender_email: extracted.sender_email,
      },
    });

    return {
      ok: false,
      reason: clean(error?.message) || "buyer_response_failed",
      idempotency_key,
    };
  }
}

export default handleBuyerResponseWebhook;
