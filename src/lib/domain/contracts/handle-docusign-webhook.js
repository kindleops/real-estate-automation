// ─── handle-docusign-webhook.js ──────────────────────────────────────────
import {
  CONTRACT_FIELDS,
  findContractItems,
  updateContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { maybeCreateTitleRoutingFromSignedContract } from "@/lib/domain/title/maybe-create-title-routing-from-signed-contract.js";
import { maybeCreateClosingFromTitleRouting } from "@/lib/domain/closings/maybe-create-closing-from-title-routing.js";
import { maybeSendTitleIntro } from "@/lib/domain/title/maybe-send-title-intro.js";
import { createBuyerMatchFlow } from "@/lib/flows/create-buyer-match-flow.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { updateBrainFromExecution } from "@/lib/domain/brain/update-brain-from-execution.js";
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

const defaultDeps = {
  findContractItems,
  updateContractItem,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  maybeCreateTitleRoutingFromSignedContract,
  maybeCreateClosingFromTitleRouting,
  maybeSendTitleIntro,
  createBuyerMatchFlow,
  syncPipelineState,
  updateBrainFromExecution,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setDocusignWebhookTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetDocusignWebhookTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (first?.value?.item_id) return first.value.item_id;
  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.start) return first.start;

  return null;
}

function getEnvelopeSummary(payload = {}) {
  return (
    payload.envelopeSummary ||
    payload.data?.envelopeSummary ||
    payload.data?.envelope_summary ||
    payload.envelope ||
    payload.data ||
    payload
  );
}

function normalizeRecipientRole(value = "") {
  const normalized = lower(value);
  const seller_alias = lower(process.env.DOCUSIGN_SELLER_ROLE_NAME || "Seller");
  const buyer_alias = lower(process.env.DOCUSIGN_BUYER_ROLE_NAME || "Buyer");

  if (["seller", "seller signer", seller_alias].includes(normalized)) {
    return "seller";
  }
  if (["buyer", "buyer signer", buyer_alias].includes(normalized)) {
    return "buyer";
  }
  if (["internal_cc", "internal cc", "cc", "carbon copy"].includes(normalized)) {
    return "internal_cc";
  }

  return normalized || "seller";
}

function normalizeRecipientStatus(value = "") {
  const normalized = lower(value);

  if (!normalized) return null;
  if (["cancelled", "canceled", "voided"].includes(normalized)) return "Voided";
  if (normalized === "declined") return "Declined";
  if (normalized === "completed" || normalized === "signed") return "Completed";
  if (["delivered", "viewed"].includes(normalized)) return "Delivered";
  if (normalized === "sent") return "Sent";
  if (normalized === "created") return "Created";

  return clean(value) || null;
}

function pickFirstTimestamp(...values) {
  for (const value of values) {
    const normalized = clean(value);
    if (normalized) return normalized;
  }
  return null;
}

function extractWebhookRecipients(payload = {}) {
  const summary = getEnvelopeSummary(payload);
  const recipients =
    summary?.recipients ||
    payload.recipients ||
    payload.data?.recipients ||
    {};

  return [
    ...((Array.isArray(recipients?.signers) ? recipients.signers : []).map((recipient) => ({
      role: normalizeRecipientRole(recipient?.roleName || recipient?.role || recipient?.recipientRole),
      status: normalizeRecipientStatus(recipient?.status),
      completed_at: pickFirstTimestamp(
        recipient?.completedDateTime,
        recipient?.completed_at,
        recipient?.statusChangedDateTime
      ),
      delivered_at: pickFirstTimestamp(
        recipient?.deliveredDateTime,
        recipient?.delivered_at,
        recipient?.viewedDateTime,
        recipient?.viewed_at
      ),
      sent_at: pickFirstTimestamp(
        recipient?.sentDateTime,
        recipient?.sent_at,
        recipient?.statusChangedDateTime
      ),
      email: clean(recipient?.email) || null,
      name: clean(recipient?.name) || null,
    })) || []),
    ...((Array.isArray(recipients?.carbonCopies) ? recipients.carbonCopies : []).map((recipient) => ({
      role: "internal_cc",
      status: normalizeRecipientStatus(recipient?.status),
      completed_at: null,
      delivered_at: pickFirstTimestamp(
        recipient?.deliveredDateTime,
        recipient?.delivered_at,
        recipient?.viewedDateTime,
        recipient?.viewed_at
      ),
      sent_at: pickFirstTimestamp(
        recipient?.sentDateTime,
        recipient?.sent_at,
        recipient?.statusChangedDateTime
      ),
      email: clean(recipient?.email) || null,
      name: clean(recipient?.name) || null,
    })) || []),
  ];
}

function extractWebhookPayload(payload = {}) {
  const summary = getEnvelopeSummary(payload);
  const recipients = extractWebhookRecipients(payload);
  const event_id =
    payload.event_id ||
    payload.eventId ||
    payload.data?.event_id ||
    payload.data?.eventId ||
    summary?.eventId ||
    null;

  const envelope_id =
    payload.envelope_id ||
    payload.envelopeId ||
    payload.data?.envelopeId ||
    summary?.envelopeId ||
    null;

  const status =
    payload.status ||
    payload.envelope_status ||
    payload.event ||
    payload.event_type ||
    payload.data?.status ||
    summary?.status ||
    null;

  const recipient_status =
    payload.recipient_status ||
    payload.recipientStatus ||
    payload.data?.recipientStatus ||
    null;

  return {
    raw: payload,
    event_id: clean(event_id) || null,
    envelope_id: clean(envelope_id) || null,
    status: clean(status) || null,
    recipient_status: clean(recipient_status) || null,
    recipients,
    sent_at: pickFirstTimestamp(
      summary?.sentDateTime,
      summary?.sent_at,
      payload?.sentDateTime,
      payload?.sent_at
    ),
    viewed_at: pickFirstTimestamp(
      summary?.deliveredDateTime,
      summary?.viewedDateTime,
      recipients.find((recipient) => recipient.status === "Delivered")?.delivered_at,
      payload?.deliveredDateTime,
      payload?.viewedDateTime
    ),
    seller_signed_at: pickFirstTimestamp(
      recipients.find(
        (recipient) => recipient.role === "seller" && recipient.status === "Completed"
      )?.completed_at
    ),
    buyer_signed_at: pickFirstTimestamp(
      recipients.find(
        (recipient) => recipient.role === "buyer" && recipient.status === "Completed"
      )?.completed_at
    ),
    completed_at: pickFirstTimestamp(
      summary?.completedDateTime,
      summary?.completed_at,
      payload?.completedDateTime,
      payload?.completed_at,
      summary?.statusChangedDateTime
    ),
  };
}

function buildDocusignIdempotencyKey(extracted = {}) {
  return (
    clean(extracted.event_id) ||
    runtimeDeps.hashIdempotencyPayload({
      provider: "docusign",
      envelope_id: clean(extracted.envelope_id) || null,
      status: clean(extracted.status) || null,
      recipient_status: clean(extracted.recipient_status) || null,
      raw: extracted.raw || null,
    })
  );
}

function normalizeDocusignStatus({
  status = "",
  recipient_status = "",
  recipients = [],
} = {}) {
  const normalized_status = lower(status);
  const normalized_recipient_status = lower(recipient_status);
  const seller_signed = recipients.some(
    (recipient) => recipient.role === "seller" && recipient.status === "Completed"
  );
  const buyer_signed = recipients.some(
    (recipient) => recipient.role === "buyer" && recipient.status === "Completed"
  );
  const any_delivered = recipients.some((recipient) => recipient.status === "Delivered");
  const any_sent = recipients.some((recipient) => recipient.status === "Sent");

  if (["cancelled", "canceled", "voided"].includes(normalized_status)) return "Voided";
  if (normalized_status === "declined") return "Declined";
  if (normalized_status === "completed") return "Completed";
  if (seller_signed && buyer_signed) return "Completed";
  if (seller_signed) return "Seller Signed";
  if (buyer_signed) return "Buyer Signed";
  if (["delivered", "viewed"].includes(normalized_status) || any_delivered) return "Delivered";
  if (normalized_status === "sent" || any_sent) return "Sent";
  if (normalized_status === "created") return "Created";

  if (normalized_recipient_status === "completed") return "Completed";
  if (normalized_recipient_status === "declined") return "Declined";
  if (["delivered", "viewed"].includes(normalized_recipient_status)) return "Delivered";
  if (normalized_recipient_status === "sent") return "Sent";

  return clean(status) || clean(recipient_status) || "Unknown";
}

function mapContractStatusFromDocusign(normalized_status = "") {
  const status = clean(normalized_status).toLowerCase();

  if (status === "completed") return "Fully Executed";
  if (status === "seller signed") return "Seller Signed";
  if (status === "buyer signed") return "Buyer Signed";
  if (status === "declined" || status === "voided") return "Cancelled";
  if (status === "delivered") return "Viewed";
  if (status === "sent") return "Sent";
  if (status === "created") return "Draft";

  return null;
}

const CONTRACT_STATUS_ORDER = Object.freeze({
  Draft: 0,
  Sent: 1,
  Viewed: 2,
  "Seller Signed": 3,
  "Buyer Signed": 3,
  "Fully Executed": 4,
  "Sent To Title": 5,
  Opened: 6,
  "Clear To Close": 7,
  Closed: 8,
  Cancelled: 8,
});

function shouldAdvanceContractStatus(current_status = null, next_status = null) {
  const current = clean(current_status) || null;
  const next = clean(next_status) || null;

  if (!next) return false;
  if (!current) return true;
  if (current === next) return false;
  if (["Closed", "Cancelled"].includes(current)) return false;
  if (
    next === "Cancelled" &&
    ["Fully Executed", "Sent To Title", "Opened", "Clear To Close", "Closed"].includes(current)
  ) {
    return false;
  }
  if (
    ["Sent To Title", "Opened", "Clear To Close", "Closed"].includes(current) &&
    ["Draft", "Sent", "Viewed", "Seller Signed", "Buyer Signed", "Fully Executed"].includes(next)
  ) {
    return false;
  }

  const current_rank = CONTRACT_STATUS_ORDER[current] ?? -1;
  const next_rank = CONTRACT_STATUS_ORDER[next] ?? -1;

  if (current_rank < next_rank) return true;
  if (
    current_rank === next_rank &&
    ["Seller Signed", "Buyer Signed"].includes(current) &&
    ["Seller Signed", "Buyer Signed"].includes(next)
  ) {
    return true;
  }

  return false;
}

async function findLatestContractByEnvelopeId(envelope_id) {
  if (!envelope_id) return null;

  const matches = await runtimeDeps.findContractItems(
    { [CONTRACT_FIELDS.docusign_envelope_id]: envelope_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

function buildContractUpdatePayload({
  contract_item = null,
  envelope_id = null,
  normalized_status = null,
  extracted = {},
} = {}) {
  const contract_status = mapContractStatusFromDocusign(normalized_status);
  const payload = {};
  const current_status = clean(
    getFieldValue(contract_item, CONTRACT_FIELDS.contract_status)
  );

  if (shouldAdvanceContractStatus(current_status, contract_status)) {
    payload[CONTRACT_FIELDS.contract_status] = contract_status;
  }

  if (
    clean(envelope_id) &&
    !clean(getFieldValue(contract_item, CONTRACT_FIELDS.docusign_envelope_id))
  ) {
    payload[CONTRACT_FIELDS.docusign_envelope_id] = clean(envelope_id);
  }

  if (
    clean(extracted.sent_at) &&
    !clean(getFieldValue(contract_item, CONTRACT_FIELDS.contract_sent_timestamp))
  ) {
    payload[CONTRACT_FIELDS.contract_sent_timestamp] = { start: extracted.sent_at };
  }

  if (
    clean(extracted.viewed_at) &&
    !clean(getFieldValue(contract_item, CONTRACT_FIELDS.contract_viewed_timestamp))
  ) {
    payload[CONTRACT_FIELDS.contract_viewed_timestamp] = { start: extracted.viewed_at };
  }

  if (
    clean(extracted.seller_signed_at) &&
    !clean(getFieldValue(contract_item, CONTRACT_FIELDS.seller_signed_timestamp))
  ) {
    payload[CONTRACT_FIELDS.seller_signed_timestamp] = {
      start: extracted.seller_signed_at,
    };
  }

  if (
    clean(extracted.buyer_signed_at) &&
    !clean(getFieldValue(contract_item, CONTRACT_FIELDS.buyer_signed_timestamp))
  ) {
    payload[CONTRACT_FIELDS.buyer_signed_timestamp] = {
      start: extracted.buyer_signed_at,
    };
  }

  if (
    clean(extracted.completed_at) &&
    !clean(getFieldValue(contract_item, CONTRACT_FIELDS.fully_executed_timestamp))
  ) {
    payload[CONTRACT_FIELDS.fully_executed_timestamp] = {
      start: extracted.completed_at,
    };
  }

  return payload;
}

export async function handleDocusignWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);
  const normalized_status = normalizeDocusignStatus({
    status: extracted.status,
    recipient_status: extracted.recipient_status,
    recipients: extracted.recipients,
  });
  const idempotency_key = buildDocusignIdempotencyKey(extracted);

  runtimeDeps.info("docusign.webhook_received", {
    event_id: extracted.event_id,
    envelope_id: extracted.envelope_id,
    status: extracted.status,
    recipient_status: extracted.recipient_status,
    normalized_status,
  });

  const idempotency = await runtimeDeps.beginIdempotentProcessing({
    scope: "docusign_webhook",
    key: idempotency_key,
    summary: `Processed DocuSign event ${idempotency_key}`,
    metadata: {
      event_id: extracted.event_id,
      envelope_id: extracted.envelope_id,
      normalized_status,
    },
  });

  if (!idempotency.ok) {
    return {
      ok: false,
      reason: idempotency.reason,
      envelope_id: extracted.envelope_id,
      event_id: extracted.event_id,
      idempotency_key,
    };
  }

  if (idempotency.duplicate) {
    runtimeDeps.info("docusign.webhook_duplicate_ignored", {
      event_id: extracted.event_id,
      envelope_id: extracted.envelope_id,
      normalized_status,
      reason: idempotency.reason,
      idempotency_key,
    });

    return {
      ok: true,
      duplicate: true,
      updated: false,
      reason: idempotency.reason,
      envelope_id: extracted.envelope_id,
      event_id: extracted.event_id,
      normalized_status,
      idempotency_key,
    };
  }

  try {
    if (!extracted.envelope_id) {
      runtimeDeps.warn("docusign.webhook_missing_envelope_id", {
        status: extracted.status,
        recipient_status: extracted.recipient_status,
      });

      const result = {
        ok: false,
        reason: "missing_envelope_id",
      };

      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "docusign_webhook",
        key: idempotency_key,
        summary: `DocuSign webhook ignored: ${result.reason}`,
        metadata: {
          event_id: extracted.event_id,
          normalized_status,
          result_reason: result.reason,
        },
      });

      return result;
    }

    const contract_item = await findLatestContractByEnvelopeId(extracted.envelope_id);

    if (!contract_item?.item_id) {
      runtimeDeps.warn("docusign.webhook_contract_not_found", {
        envelope_id: extracted.envelope_id,
        normalized_status,
      });

      const result = {
        ok: false,
        reason: "contract_not_found",
        envelope_id: extracted.envelope_id,
        normalized_status,
      };

      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "docusign_webhook",
        key: idempotency_key,
        error: result.reason,
        metadata: {
          event_id: extracted.event_id,
          envelope_id: extracted.envelope_id,
          normalized_status,
        },
      });

      await recordSystemAlert({
        subsystem: "docusign_webhook",
        code: "contract_not_found",
        severity: "high",
        retryable: true,
        summary: `DocuSign webhook could not find contract for envelope ${clean(extracted.envelope_id) || "unknown"}.`,
        dedupe_key: `docusign-webhook:${clean(extracted.envelope_id) || "unknown"}`,
        metadata: {
          normalized_status,
          event_id: extracted.event_id,
        },
      });

      return result;
    }

    const update_payload = buildContractUpdatePayload({
      contract_item,
      envelope_id: extracted.envelope_id,
      normalized_status,
      extracted,
    });

    if (Object.keys(update_payload).length) {
      await runtimeDeps.updateContractItem(contract_item.item_id, update_payload);
    }

    const title_routing = await runtimeDeps.maybeCreateTitleRoutingFromSignedContract({
      contract_item,
      contract_item_id: contract_item.item_id,
      contract_status: update_payload[CONTRACT_FIELDS.contract_status] || null,
      docusign_status: normalized_status,
      webhook_result: {
        normalized_status,
        envelope_id: extracted.envelope_id,
      },
      source: "DocuSign Webhook",
    });

    const resolved_title_routing_item_id =
      title_routing?.title_routing_item_id ||
      title_routing?.result?.title_routing_item_id ||
      null;

    const resolved_title_routing_item =
      title_routing?.existing_title_routing ||
      title_routing?.result?.raw ||
      null;

    const closing = await runtimeDeps.maybeCreateClosingFromTitleRouting({
      title_routing_item_id: resolved_title_routing_item_id,
      title_routing_item: resolved_title_routing_item,
      title_routing_result: title_routing,
      contract_item_id: contract_item.item_id,
      source: "DocuSign Webhook",
    });

    const resolved_closing_item_id =
      closing?.closing_item_id ||
      closing?.result?.closing_item_id ||
      null;
    const buyer_match =
      normalized_status === "Completed"
        ? await runtimeDeps.createBuyerMatchFlow({
            contract_id: contract_item.item_id,
            closing_id: resolved_closing_item_id,
          })
        : null;
    const resolved_buyer_match_item_id =
      buyer_match?.buyer_match_item_id || null;

    const title_intro = await runtimeDeps.maybeSendTitleIntro({
      title_routing_item_id: resolved_title_routing_item_id,
      closing_item_id: resolved_closing_item_id,
      contract_item_id: contract_item.item_id,
      dry_run: false,
    });
    const pipeline = await runtimeDeps.syncPipelineState({
      contract_item_id: contract_item.item_id,
      title_routing_item_id: resolved_title_routing_item_id,
      closing_item_id: resolved_closing_item_id,
      buyer_match_item_id: resolved_buyer_match_item_id,
      notes: `DocuSign webhook processed: ${normalized_status}.`,
    });
    const brain_update = await runtimeDeps.updateBrainFromExecution({
      source: "contract",
      contract_item,
      normalized_status,
      contract_status: update_payload[CONTRACT_FIELDS.contract_status] || null,
      notes: `DocuSign webhook processed: ${normalized_status}.`,
    });

    runtimeDeps.info("docusign.webhook_processed", {
      contract_item_id: contract_item.item_id,
      envelope_id: extracted.envelope_id,
      normalized_status,
      contract_status: update_payload[CONTRACT_FIELDS.contract_status] || null,
      title_routing_created: Boolean(title_routing?.created),
      title_routing_item_id: resolved_title_routing_item_id,
      closing_created: Boolean(closing?.created),
      closing_item_id: resolved_closing_item_id,
      buyer_match_item_id: resolved_buyer_match_item_id,
      title_intro_sent: Boolean(title_intro?.sent),
      title_intro_reason: title_intro?.reason || null,
      title_company_email: title_intro?.title_company_email || null,
      pipeline_stage: pipeline?.current_stage || null,
      brain_updated: Boolean(brain_update?.updated),
      brain_reason: brain_update?.reason || null,
    });

    const result = {
      ok: true,
      reason: "docusign_webhook_processed",
      contract_item_id: contract_item.item_id,
      envelope_id: extracted.envelope_id,
      event_id: extracted.event_id,
      normalized_status,
      contract_status: update_payload[CONTRACT_FIELDS.contract_status] || null,
      update_payload,
      title_routing,
      closing,
      buyer_match,
      title_intro,
      pipeline,
      brain_update,
      idempotency_key,
    };

    await runtimeDeps.completeIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "docusign_webhook",
      key: idempotency_key,
      summary: `DocuSign webhook completed ${idempotency_key}`,
      metadata: {
        event_id: extracted.event_id,
        envelope_id: extracted.envelope_id,
        contract_item_id: contract_item.item_id,
        normalized_status,
        title_routing_item_id: resolved_title_routing_item_id,
        closing_item_id: resolved_closing_item_id,
        buyer_match_item_id: resolved_buyer_match_item_id,
      },
    });

    return result;
  } catch (error) {
    await runtimeDeps.failIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "docusign_webhook",
      key: idempotency_key,
      error,
      metadata: {
        event_id: extracted.event_id,
        envelope_id: extracted.envelope_id,
        normalized_status,
      },
    });

    await recordSystemAlert({
      subsystem: "docusign_webhook",
      code: "handler_failed",
      severity: "high",
      retryable: true,
      summary: `DocuSign webhook handler failed: ${clean(error?.message) || "unknown_error"}`,
      dedupe_key: `docusign-webhook:${clean(extracted.envelope_id) || idempotency_key}`,
      metadata: {
        envelope_id: extracted.envelope_id,
        event_id: extracted.event_id,
        normalized_status,
      },
    });

    throw error;
  }
}

export default handleDocusignWebhook;
