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
import { updateMasterOwnerAfterInbound } from "@/lib/domain/master-owners/update-master-owner-after-inbound.js";
import { isNegativeReply } from "@/lib/domain/classification/is-negative-reply.js";
import { cancelPendingQueueItemsForOwner } from "@/lib/domain/queue/cancel-pending-queue-items.js";
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
  updateMasterOwnerAfterInbound,
  isNegativeReply,
  cancelPendingQueueItemsForOwner,
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
    payload.SmsMessageSid ||
    payload.SmsSid ||
    payload.MessageSid ||
    null;

  const from =
    payload.from ||
    payload.sender ||
    payload.msisdn ||
    payload.contact?.phone ||
    payload.From ||
    null;

  const to =
    payload.to ||
    payload.recipient ||
    payload.phone_number ||
    payload.To ||
    null;

  const body =
    payload.body ||
    payload.message ||
    payload.text ||
    payload.content ||
    payload.Body ||
    "";

  const status =
    payload.status ||
    payload.SmsStatus ||
    payload.event_type ||
    payload.event ||
    "received";

  const received_at =
    payload.received_at ||
    payload.http_received_at ||
    payload.timestamp ||
    payload.created_at ||
    null;

  return {
    raw: payload,
    message_id,
    from,
    to,
    body: String(body || "").trim(),
    status,
    received_at,
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

// Logger guards — prevent any logger throw from escaping handler segments.
function safeInfo(event, meta = {}) {
  try { runtimeDeps.info(event, meta); } catch {}
}
function safeWarn(event, meta = {}) {
  try { runtimeDeps.warn(event, meta); } catch {}
}

export async function handleTextgridInboundWebhook(payload = {}, opts = {}) {
  const { inbound_debug_stage = null } = opts;

  // ── Earliest possible gate ────────────────────────────────────────────────
  // If this returns 500 the handler is not being invoked at all; the throw
  // is in the route between before_handler and the handleTextgridInboundImpl
  // call (likely safeRouteLog or maybeHandleBuyerTextgridInboundImpl).
  if (inbound_debug_stage === "handler_entry") {
    return { ok: true, stage: "handler_entry" };
  }

  // ── SEGMENT: handler_entry ────────────────────────────────────────────────
  let extracted, inbound_from, inbound_to, message_body;
  try {
    extracted = extractWebhookPayload(payload);

    if (inbound_debug_stage === "after_extract") {
      return { ok: true, stage: "after_extract", message_id: extracted?.message_id || null };
    }

    inbound_from = runtimeDeps.normalizeInboundTextgridPhone(extracted.from);

    if (inbound_debug_stage === "after_normalize_from") {
      return { ok: true, stage: "after_normalize_from", inbound_from };
    }

    inbound_to = runtimeDeps.normalizeInboundTextgridPhone(extracted.to);

    if (inbound_debug_stage === "after_normalize_to") {
      return { ok: true, stage: "after_normalize_to", inbound_from, inbound_to };
    }

    message_body = extracted.body;

    safeInfo("textgrid.inbound_received", {
      message_id: extracted.message_id,
      inbound_from,
      inbound_to,
      status: extracted.status,
    });

    if (inbound_debug_stage === "after_inbound_received_log") {
      return { ok: true, stage: "after_inbound_received_log", inbound_from, inbound_to };
    }
  } catch (err) {
    return { ok: false, error: "textgrid_inbound_failed_handler_entry", error_message: err?.message || "unknown" };
  }

  if (!inbound_from) {
    safeWarn("textgrid.inbound_missing_from", { message_id: extracted.message_id });
    return { ok: false, reason: "missing_inbound_from" };
  }

  if (!message_body) {
    safeWarn("textgrid.inbound_empty_body", { message_id: extracted.message_id, inbound_from });
    return { ok: false, reason: "empty_inbound_body" };
  }

  // ── SEGMENT: message_event_lookup ────────────────────────────────────────
  // beginIdempotentProcessing checks the ledger for prior processing of this
  // message ID — this is the "lookup" before we commit to processing.
  let idempotency_key, idempotency;
  try {
    idempotency_key = buildInboundIdempotencyKey(extracted);
    idempotency = await runtimeDeps.beginIdempotentProcessing({
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
  } catch (err) {
    return { ok: false, error: "textgrid_inbound_failed_message_event_lookup", error_message: err?.message || "unknown" };
  }

  if (!idempotency.ok) {
    return {
      ok: false,
      reason: idempotency.reason,
      message_id: extracted.message_id,
      idempotency_key,
    };
  }

  if (idempotency.duplicate) {
    safeInfo("textgrid.inbound_duplicate_ignored", {
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

  if (inbound_debug_stage === "after_message_event_lookup") {
    return { ok: true, stage: "after_message_event_lookup", idempotency_key };
  }

  // From here the idempotency record exists; the outer catch calls
  // failIdempotentProcessing if anything escapes all inner catches.
  try {
    // ── SEGMENT: brain_lookup ───────────────────────────────────────────────
    let context;
    try {
      context = await runtimeDeps.loadContext({
        inbound_from,
        create_brain_if_missing: true,
      });
    } catch (err) {
      return { ok: false, error: "textgrid_inbound_failed_brain_lookup", error_message: err?.message || "unknown" };
    }

    if (!context?.found) {
      safeWarn("textgrid.inbound_context_not_found", {
        message_id: extracted.message_id,
        inbound_from,
        reason: context?.reason || "unknown",
      });

      const not_found_result = {
        ok: false,
        reason: context?.reason || "context_not_found",
        inbound_from,
        context,
      };

      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_inbound",
        key: idempotency_key,
        summary: `Inbound SMS ignored: ${not_found_result.reason}`,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          inbound_from,
          inbound_to,
          result_reason: not_found_result.reason,
        },
      });

      return not_found_result;
    }

    const brain_item = context.items?.brain_item || null;
    const brain_id = context.ids?.brain_item_id || null;
    const master_owner_id = context.ids?.master_owner_id || null;
    const prospect_id = context.ids?.prospect_id || null;
    const property_id = context.ids?.property_id || null;
    const phone_item_id = context.ids?.phone_item_id || null;

    if (inbound_debug_stage === "after_brain_lookup") {
      return { ok: true, stage: "after_brain_lookup", brain_id, master_owner_id };
    }

    // ── SEGMENT: phone_resolution ────────────────────────────────────────
    // Phone identity is resolved from context — gate here confirms phone_item_id
    // is available before downstream steps that depend on it.
    if (inbound_debug_stage === "after_phone_resolution") {
      return { ok: true, stage: "after_phone_resolution", phone_item_id, inbound_from };
    }

    // ── SEGMENT: message_event_create ─────────────────────────────────────
    // Log the inbound message event to Podio before classification so the
    // record exists regardless of routing outcome.
    const inbound_number_item_id = null;
    try {
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
        received_at: extracted.received_at || payload?.http_received_at || new Date().toISOString(),
        processed_by: "Inbound Webhook",
        source_app: "TextGrid",
        trigger_name: "textgrid-inbound",
      });
    } catch (err) {
      return { ok: false, error: "textgrid_inbound_failed_message_event_create", error_message: err?.message || "unknown" };
    }

    if (inbound_debug_stage === "after_message_event_create") {
      return { ok: true, stage: "after_message_event_create" };
    }

    // ── SEGMENT: conversation_resolution ─────────────────────────────────
    // Classify the message body, handle negative-reply cancellations, and
    // resolve the routing decision.
    let classification, inbound_is_negative, queue_cancellation, route;
    try {
      classification = await runtimeDeps.classify(message_body, brain_item);

      inbound_is_negative = runtimeDeps.isNegativeReply(message_body);
      queue_cancellation = null;

      if (inbound_is_negative && (master_owner_id || phone_item_id)) {
        queue_cancellation = await runtimeDeps.cancelPendingQueueItemsForOwner({
          master_owner_id,
          phone_item_id,
          reason: "inbound_negative_reply",
        });

        safeInfo("textgrid.inbound_negative_reply_queue_canceled", {
          message_id: extracted.message_id,
          inbound_from,
          master_owner_id,
          phone_item_id,
          canceled_count: queue_cancellation?.canceled_count ?? 0,
          items_checked: queue_cancellation?.items_checked ?? 0,
        });
      }

      route = runtimeDeps.resolveRoute({
        classification,
        brain_item,
        phone_item: context.items?.phone_item || null,
        message: message_body,
      });
    } catch (err) {
      return { ok: false, error: "textgrid_inbound_failed_conversation_resolution", error_message: err?.message || "unknown" };
    }

    if (inbound_debug_stage === "after_conversation_resolution") {
      return { ok: true, stage: "after_conversation_resolution", route_stage: route?.stage || null, classification_source: classification?.source || null };
    }

    // ── SEGMENT: prospect_resolution ──────────────────────────────────────
    // Write brain activity, master-owner timestamps, and stage/language/profile
    // updates in parallel.
    try {
      await runtimeDeps.updateBrainAfterInbound({ brain_id, message_body });

      await runtimeDeps.updateMasterOwnerAfterInbound({
        master_owner_id,
        received_at: new Date().toISOString(),
      });

      await Promise.all([
        route?.stage
          ? runtimeDeps.updateBrainStage({ brain_id, stage: route.stage })
          : Promise.resolve({ ok: false, reason: "missing_stage" }),

        classification?.language
          ? runtimeDeps.updateBrainLanguage({ brain_id, language: classification.language })
          : Promise.resolve({ ok: false, reason: "missing_language" }),

        route?.seller_profile
          ? runtimeDeps.updateBrainSellerProfile({ brain_id, seller_profile: route.seller_profile })
          : Promise.resolve({ ok: false, reason: "missing_seller_profile" }),
      ]);
    } catch (err) {
      return { ok: false, error: "textgrid_inbound_failed_prospect_resolution", error_message: err?.message || "unknown" };
    }

    if (inbound_debug_stage === "after_prospect_resolution") {
      return { ok: true, stage: "after_prospect_resolution", brain_id, master_owner_id };
    }

    // ── SEGMENT: market_resolution ────────────────────────────────────────
    // Fetch the latest open offer to determine offer-progression vs. creation.
    let existing_offer;
    try {
      existing_offer = await runtimeDeps.findLatestOpenOffer({
        prospect_id,
        master_owner_id,
        property_id,
      });
    } catch (err) {
      return { ok: false, error: "textgrid_inbound_failed_market_resolution", error_message: err?.message || "unknown" };
    }

    if (inbound_debug_stage === "after_market_resolution") {
      return { ok: true, stage: "after_market_resolution", existing_offer_item_id: existing_offer?.item_id || null };
    }

    // ── SEGMENT: podio_write ──────────────────────────────────────────────
    // All offer, underwriting, contract, and pipeline writes happen here.
    let maybe_offer_progress, initial_offer, underwriting, seller_stage_reply,
        underwriting_follow_up, maybe_offer, active_offer_item_id, contract, pipeline;

    try {
      maybe_offer_progress = existing_offer
        ? await runtimeDeps.maybeProgressOfferStatus({
            offer_item_id: existing_offer.item_id,
            message: message_body,
            classification,
            notes: message_body,
          })
        : { ok: true, updated: false, reason: "no_existing_open_offer" };

      initial_offer = maybe_offer_progress?.updated
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

      underwriting = await runtimeDeps.maybeUpsertUnderwritingFromInbound({
        context,
        classification,
        route,
        message: message_body,
        offer_item_id:
          initial_offer?.offer?.offer_item_id ||
          initial_offer?.existing_offer_item_id ||
          existing_offer?.item_id ||
          null,
        source_channel: "SMS",
        notes: message_body,
      });

      seller_stage_reply = await runtimeDeps.maybeQueueSellerStageReply({
        inbound_from,
        context,
        classification,
        message: message_body,
        maybe_offer: initial_offer,
        existing_offer,
      });

      if (seller_stage_reply?.brain_stage) {
        await runtimeDeps.updateBrainStage({ brain_id, stage: seller_stage_reply.brain_stage });
      }

      underwriting_follow_up = seller_stage_reply?.handled
        ? { ok: true, queued: false, reason: "suppressed_by_seller_stage_reply" }
        : await runtimeDeps.maybeQueueUnderwritingFollowUp({
            inbound_from,
            underwriting,
            classification,
            route,
            context,
            message: message_body,
          });

      const underwriting_offer_ready =
        underwriting?.strategy?.auto_offer_ready === true ||
        underwriting?.signals?.underwriting_auto_offer_ready === true ||
        underwriting_follow_up?.offer_ready === true;

      maybe_offer =
        initial_offer?.created || initial_offer?.existing_offer_item_id || !underwriting_offer_ready
          ? initial_offer
          : await runtimeDeps.maybeCreateOfferFromContext({
              context,
              classification,
              route,
              message: message_body,
              notes: message_body,
              created_by: "Underwriting Offer Engine",
              respect_underwriting_gate: false,
            });

      active_offer_item_id =
        maybe_offer?.offer?.offer_item_id ||
        maybe_offer?.existing_offer_item_id ||
        initial_offer?.offer?.offer_item_id ||
        initial_offer?.existing_offer_item_id ||
        existing_offer?.item_id ||
        null;

      contract = await runtimeDeps.maybeCreateContractFromAcceptedOffer({
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

      pipeline = await runtimeDeps.syncPipelineState({
        property_id,
        master_owner_id,
        prospect_id,
        conversation_item_id: brain_id,
        offer_item_id: active_offer_item_id,
        contract_item_id: contract?.contract_item_id || null,
        notes: `Inbound SMS processed${route?.stage ? ` at stage ${route.stage}` : ""}.`,
      });
    } catch (err) {
      return { ok: false, error: "textgrid_inbound_failed_podio_write", error_message: err?.message || "unknown" };
    }

    if (inbound_debug_stage === "after_podio_write") {
      return { ok: true, stage: "after_podio_write", pipeline_item_id: pipeline?.pipeline_item_id || null };
    }

    safeInfo("textgrid.inbound_processed", {
      message_id: extracted.message_id,
      inbound_from,
      brain_id,
      master_owner_id,
      prospect_id,
      property_id,
      inbound_is_negative,
      queue_canceled_count: queue_cancellation?.canceled_count ?? null,
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
      inbound_is_negative,
      queue_cancellation,
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

    if (inbound_debug_stage === "handler_exit") {
      return { ok: true, stage: "handler_exit", message_id: extracted.message_id };
    }

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
