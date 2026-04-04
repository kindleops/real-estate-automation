// ─── handle-textgrid-inbound.js ──────────────────────────────────────────
import { loadContext } from "@/lib/domain/context/load-context.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { normalizeInboundTextgridPhone } from "@/lib/providers/textgrid.js";
import { logInboundMessageEvent } from "@/lib/domain/events/log-inbound-message-event.js";
import { updateBrainAfterInbound } from "@/lib/domain/brain/update-brain-after-inbound.js";
import { updateBrainStage } from "@/lib/domain/brain/update-brain-stage.js";
import { updateBrainLanguage } from "@/lib/domain/brain/update-brain-language.js";
import { updateBrainSellerProfile } from "@/lib/domain/brain/update-brain-seller-profile.js";
import { maybeCreateOfferFromContext } from "@/lib/domain/offers/maybe-create-offer-from-context.js";
import { maybeProgressOfferStatus } from "@/lib/domain/offers/maybe-progress-offer-status.js";
import { maybeUpsertUnderwritingFromInbound } from "@/lib/domain/underwriting/maybe-upsert-underwriting-from-inbound.js";
import { maybeQueueUnderwritingFollowUp } from "@/lib/domain/underwriting/maybe-queue-underwriting-follow-up.js";
import { maybeCreateContractFromAcceptedOffer } from "@/lib/domain/contracts/maybe-create-contract-from-accepted-offer.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { maybeQueueSellerStageReply } from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";
import {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { findLatestOpenOffer } from "@/lib/podio/apps/offers.js";
import { info, warn } from "@/lib/logging/logger.js";

const defaultDeps = {
  loadContext,
  classify,
  resolveRoute,
  normalizeInboundTextgridPhone,
  logInboundMessageEvent,
  updateBrainAfterInbound,
  updateBrainStage,
  updateBrainLanguage,
  updateBrainSellerProfile,
  maybeCreateOfferFromContext,
  maybeProgressOfferStatus,
  maybeUpsertUnderwritingFromInbound,
  maybeQueueUnderwritingFollowUp,
  maybeCreateContractFromAcceptedOffer,
  syncPipelineState,
  maybeQueueSellerStageReply,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  findLatestOpenOffer,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setTextgridInboundTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetTextgridInboundTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function clean(value) {
  return String(value ?? "").trim();
}

function extractWebhookPayload(payload = {}) {
  const message_id =
    payload.id ||
    payload.message_id ||
    payload.messageId ||
    null;

  const from =
    payload.from ||
    payload.sender ||
    payload.msisdn ||
    payload.contact?.phone ||
    null;

  const to =
    payload.to ||
    payload.recipient ||
    payload.phone_number ||
    null;

  const body =
    payload.body ||
    payload.message ||
    payload.text ||
    payload.content ||
    "";

  const status =
    payload.status ||
    payload.event_type ||
    payload.event ||
    "received";

  return {
    raw: payload,
    message_id,
    from,
    to,
    body: String(body || "").trim(),
    status,
  };
}

function buildInboundIdempotencyKey(extracted = {}) {
  return (
    clean(extracted.message_id) ||
    runtimeDeps.hashIdempotencyPayload({
      provider: "textgrid",
      from: clean(extracted.from),
      to: clean(extracted.to),
      body: clean(extracted.body),
      status: clean(extracted.status),
    })
  );
}

export async function handleTextgridInboundWebhook(payload = {}) {
  const extracted = extractWebhookPayload(payload);

  const inbound_from = runtimeDeps.normalizeInboundTextgridPhone(extracted.from);
  const inbound_to = runtimeDeps.normalizeInboundTextgridPhone(extracted.to);
  const message_body = extracted.body;

  runtimeDeps.info("textgrid.inbound_received", {
    message_id: extracted.message_id,
    inbound_from,
    inbound_to,
    status: extracted.status,
  });

  if (!inbound_from) {
    runtimeDeps.warn("textgrid.inbound_missing_from", {
      message_id: extracted.message_id,
    });

    return {
      ok: false,
      reason: "missing_inbound_from",
    };
  }

  if (!message_body) {
    runtimeDeps.warn("textgrid.inbound_empty_body", {
      message_id: extracted.message_id,
      inbound_from,
    });

    return {
      ok: false,
      reason: "empty_inbound_body",
    };
  }

  const idempotency_key = buildInboundIdempotencyKey(extracted);
  const idempotency = await runtimeDeps.beginIdempotentProcessing({
    scope: "textgrid_inbound",
    key: idempotency_key,
    summary: `Processed inbound SMS ${idempotency_key}`,
    metadata: {
      provider: "textgrid",
      provider_message_id: clean(extracted.message_id) || null,
      inbound_from,
      inbound_to,
    },
  });

  if (!idempotency.ok) {
    return {
      ok: false,
      reason: idempotency.reason,
      message_id: extracted.message_id,
      idempotency_key,
    };
  }

  if (idempotency.duplicate) {
    runtimeDeps.info("textgrid.inbound_duplicate_ignored", {
      message_id: extracted.message_id,
      inbound_from,
      reason: idempotency.reason,
      idempotency_key,
    });

    return {
      ok: true,
      duplicate: true,
      updated: false,
      reason: idempotency.reason,
      message_id: extracted.message_id,
      inbound_from,
      inbound_to,
      idempotency_key,
    };
  }

  try {
    const context = await runtimeDeps.loadContext({
      inbound_from,
      create_brain_if_missing: true,
    });

    if (!context?.found) {
      runtimeDeps.warn("textgrid.inbound_context_not_found", {
        message_id: extracted.message_id,
        inbound_from,
        reason: context?.reason || "unknown",
      });

      const result = {
        ok: false,
        reason: context?.reason || "context_not_found",
        inbound_from,
        context,
      };

      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_inbound",
        key: idempotency_key,
        summary: `Inbound SMS ignored: ${result.reason}`,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          inbound_from,
          inbound_to,
          result_reason: result.reason,
        },
      });

      return result;
    }

    const brain_item = context.items?.brain_item || null;
    const brain_id = context.ids?.brain_item_id || null;
    const master_owner_id = context.ids?.master_owner_id || null;
    const prospect_id = context.ids?.prospect_id || null;
    const property_id = context.ids?.property_id || null;
    const phone_item_id = context.ids?.phone_item_id || null;

    const classification = await runtimeDeps.classify(message_body, brain_item);

    const route = runtimeDeps.resolveRoute({
      classification,
      brain_item,
      phone_item: context.items?.phone_item || null,
      message: message_body,
    });

    const inbound_number_item_id = null;

    await runtimeDeps.logInboundMessageEvent({
      brain_item,
      conversation_item_id: brain_id,
      master_owner_id,
      prospect_id,
      property_id,
      phone_item_id,
      inbound_number_item_id,
      message_body,
      provider_message_id: extracted.message_id,
      raw_carrier_status: extracted.status || "received",
      processed_by: "Inbound Webhook",
      source_app: "TextGrid",
      trigger_name: "textgrid-inbound",
    });

    await runtimeDeps.updateBrainAfterInbound({
      brain_id,
      message_body,
    });

    await Promise.all([
      route?.stage
        ? runtimeDeps.updateBrainStage({
            brain_id,
            stage: route.stage,
          })
        : Promise.resolve({ ok: false, reason: "missing_stage" }),

      classification?.language
        ? runtimeDeps.updateBrainLanguage({
            brain_id,
            language: classification.language,
          })
        : Promise.resolve({ ok: false, reason: "missing_language" }),

      route?.seller_profile
        ? runtimeDeps.updateBrainSellerProfile({
            brain_id,
            seller_profile: route.seller_profile,
          })
        : Promise.resolve({ ok: false, reason: "missing_seller_profile" }),
    ]);

    const existing_offer = await runtimeDeps.findLatestOpenOffer({
      prospect_id,
      master_owner_id,
      property_id,
    });

    const maybe_offer_progress = existing_offer
      ? await runtimeDeps.maybeProgressOfferStatus({
          offer_item_id: existing_offer.item_id,
          message: message_body,
          classification,
          notes: message_body,
        })
      : {
          ok: true,
          updated: false,
          reason: "no_existing_open_offer",
        };

    const maybe_offer =
      maybe_offer_progress?.updated
        ? {
            ok: true,
            created: false,
            reason: "existing_offer_progressed",
            existing_offer_item_id: existing_offer?.item_id || null,
            progress: maybe_offer_progress,
          }
        : await runtimeDeps.maybeCreateOfferFromContext({
            context,
            classification,
            route,
            message: message_body,
            notes: message_body,
            created_by: "Inbound Offer Engine",
          });

    const active_offer_item_id =
      maybe_offer?.offer?.offer_item_id ||
      maybe_offer?.existing_offer_item_id ||
      existing_offer?.item_id ||
      null;

    const underwriting = await runtimeDeps.maybeUpsertUnderwritingFromInbound({
      context,
      classification,
      route,
      message: message_body,
      offer_item_id: active_offer_item_id,
      source_channel: "SMS",
      notes: message_body,
    });

    const seller_stage_reply = await runtimeDeps.maybeQueueSellerStageReply({
      inbound_from,
      context,
      classification,
      message: message_body,
      maybe_offer,
      existing_offer,
    });

    if (seller_stage_reply?.brain_stage) {
      await runtimeDeps.updateBrainStage({
        brain_id,
        stage: seller_stage_reply.brain_stage,
      });
    }

    const underwriting_follow_up = seller_stage_reply?.handled
      ? {
          ok: true,
          queued: false,
          reason: "suppressed_by_seller_stage_reply",
        }
      : await runtimeDeps.maybeQueueUnderwritingFollowUp({
          inbound_from,
          underwriting,
          classification,
          route,
          context,
        });

    const contract = await runtimeDeps.maybeCreateContractFromAcceptedOffer({
      offer_item: existing_offer || null,
      offer_item_id: active_offer_item_id,
      offer_progress: maybe_offer_progress,
      context,
      route,
      underwriting,
      notes: message_body,
      source_message: message_body,
      auto_send: false,
      dry_run: false,
    });
    const pipeline = await runtimeDeps.syncPipelineState({
      property_id,
      master_owner_id,
      prospect_id,
      conversation_item_id: brain_id,
      offer_item_id: active_offer_item_id,
      contract_item_id: contract?.contract_item_id || null,
      notes: `Inbound SMS processed${route?.stage ? ` at stage ${route.stage}` : ""}.`,
    });

    runtimeDeps.info("textgrid.inbound_processed", {
      message_id: extracted.message_id,
      inbound_from,
      brain_id,
      master_owner_id,
      prospect_id,
      property_id,
      classification_source: classification?.source || null,
      route_stage: route?.stage || null,
      route_use_case: route?.use_case || null,
      existing_offer_item_id: existing_offer?.item_id || null,
      offer_progressed: Boolean(maybe_offer_progress?.updated),
      offer_created: Boolean(maybe_offer?.created),
      offer_item_id: active_offer_item_id,
      underwriting_extracted: Boolean(underwriting?.extracted),
      underwriting_created: Boolean(underwriting?.created),
      underwriting_updated: Boolean(underwriting?.updated),
      underwriting_item_id: underwriting?.underwriting_item_id || null,
      seller_stage_reply_queued: Boolean(seller_stage_reply?.queued),
      seller_stage_reply_reason: seller_stage_reply?.reason || null,
      seller_stage_use_case: seller_stage_reply?.plan?.selected_use_case || null,
      underwriting_follow_up_queued: Boolean(underwriting_follow_up?.queued),
      underwriting_follow_up_reason: underwriting_follow_up?.reason || null,
      contract_created: Boolean(contract?.created),
      contract_sent: Boolean(contract?.sent),
      contract_item_id: contract?.contract_item_id || null,
      contract_reason: contract?.reason || null,
      pipeline_item_id: pipeline?.pipeline_item_id || null,
      pipeline_stage: pipeline?.current_stage || null,
    });

    const result = {
      ok: true,
      message_id: extracted.message_id,
      inbound_from,
      inbound_to,
      body: message_body,
      context,
      classification,
      route,
      existing_offer,
      offer_progress: maybe_offer_progress,
      offer: maybe_offer,
      underwriting,
      seller_stage_reply,
      underwriting_follow_up,
      contract,
      pipeline,
      idempotency_key,
    };

    await runtimeDeps.completeIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "textgrid_inbound",
      key: idempotency_key,
      summary: `Inbound SMS completed ${idempotency_key}`,
      metadata: {
        provider_message_id: clean(extracted.message_id) || null,
        inbound_from,
        inbound_to,
        brain_id,
        offer_item_id: active_offer_item_id,
        contract_item_id: contract?.contract_item_id || null,
        pipeline_item_id: pipeline?.pipeline_item_id || null,
        result_reason: "textgrid_inbound_processed",
      },
    });

    return result;
  } catch (error) {
    await runtimeDeps.failIdempotentProcessing({
      record_item_id: idempotency.record_item_id,
      scope: "textgrid_inbound",
      key: idempotency_key,
      error,
      metadata: {
        provider_message_id: clean(extracted.message_id) || null,
        inbound_from,
        inbound_to,
      },
    });

    throw error;
  }
}

export const handleTextgridInbound = handleTextgridInboundWebhook;

export default handleTextgridInboundWebhook;
