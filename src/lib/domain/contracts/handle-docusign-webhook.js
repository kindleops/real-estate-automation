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
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { info, warn } from "@/lib/logging/logger.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
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
  syncPipelineState,
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

function extractWebhookPayload(payload = {}) {
  const event_id =
    payload.event_id ||
    payload.eventId ||
    payload.data?.event_id ||
    payload.data?.eventId ||
    payload.envelopeSummary?.eventId ||
    null;

  const envelope_id =
    payload.envelope_id ||
    payload.envelopeId ||
    payload.data?.envelopeId ||
    payload.envelopeSummary?.envelopeId ||
    null;

  const status =
    payload.status ||
    payload.envelope_status ||
    payload.event ||
    payload.event_type ||
    payload.data?.status ||
    payload.envelopeSummary?.status ||
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

function normalizeDocusignStatus(status = "", recipient_status = "") {
  const normalized_status = clean(status).toLowerCase();
  const normalized_recipient_status = clean(recipient_status).toLowerCase();

  if (normalized_status === "completed") return "Completed";
  if (normalized_status === "declined") return "Declined";
  if (normalized_status === "voided") return "Voided";
  if (normalized_status === "delivered") return "Delivered";
  if (normalized_status === "sent") return "Sent";
  if (normalized_status === "created") return "Created";

  if (normalized_recipient_status === "completed") return "Completed";
  if (normalized_recipient_status === "declined") return "Declined";
  if (normalized_recipient_status === "delivered") return "Delivered";
  if (normalized_recipient_status === "sent") return "Sent";

  return clean(status) || clean(recipient_status) || "Unknown";
}

function mapContractStatusFromDocusign(normalized_status = "") {
  const status = clean(normalized_status).toLowerCase();

  if (status === "completed") return "Fully Executed";
  if (status === "declined" || status === "voided") return "Cancelled";
  if (status === "delivered") return "Viewed";
  if (status === "sent") return "Sent";
  if (status === "created") return "Draft";

  return null;
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

function buildContractUpdatePayload(normalized_status = null) {
  const contract_status = mapContractStatusFromDocusign(normalized_status);
  const timestamp = nowIso();
  const payload = {};

  if (contract_status) {
    payload[CONTRACT_FIELDS.contract_status] = contract_status;
  }

  if (clean(normalized_status).toLowerCase() === "sent") {
    payload[CONTRACT_FIELDS.contract_sent_timestamp] = { start: timestamp };
  }

  if (clean(normalized_status).toLowerCase() === "delivered") {
    payload[CONTRACT_FIELDS.contract_viewed_timestamp] = { start: timestamp };
  }

  if (clean(normalized_status).toLowerCase() === "completed") {
    payload[CONTRACT_FIELDS.fully_executed_timestamp] = { start: timestamp };
  }

  return payload;
}

export async function handleDocusignWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);
  const normalized_status = normalizeDocusignStatus(
    extracted.status,
    extracted.recipient_status
  );
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

    const update_payload = buildContractUpdatePayload(normalized_status);
    await runtimeDeps.updateContractItem(contract_item.item_id, update_payload);

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
      title_intro_sent: Boolean(title_intro?.sent),
      title_intro_reason: title_intro?.reason || null,
      title_company_email: title_intro?.title_company_email || null,
      pipeline_stage: pipeline?.current_stage || null,
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
      title_intro,
      pipeline,
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
