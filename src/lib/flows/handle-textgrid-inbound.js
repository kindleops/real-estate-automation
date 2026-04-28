// ─── handle-textgrid-inbound.js ──────────────────────────────────────────
import { loadContext } from "@/lib/domain/context/load-context.js";
import { loadContextWithFallback } from "@/lib/domain/context/load-context-with-fallback.js";
import { createBrain } from "@/lib/domain/context/resolve-brain.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { normalizeInboundTextgridPhone } from "@/lib/providers/textgrid.js";
import { getPodioRetryAfterSeconds, isPodioRateLimitError } from "@/lib/providers/podio.js";
import { logInboundMessageEvent } from "@/lib/domain/events/log-inbound-message-event.js";
import { updateBrainAfterInbound } from "@/lib/domain/brain/update-brain-after-inbound.js";
import { updateBrainStage } from "@/lib/domain/brain/update-brain-stage.js";
import { maybeCreateOfferFromContext } from "@/lib/domain/offers/maybe-create-offer-from-context.js";
import { maybeProgressOfferStatus } from "@/lib/domain/offers/maybe-progress-offer-status.js";
import { routeInboundOffer } from "@/lib/domain/offers/route-inbound-offer.js";
import { maybeUpsertUnderwritingFromInbound } from "@/lib/domain/underwriting/maybe-upsert-underwriting-from-inbound.js";
import { maybeQueueUnderwritingFollowUp } from "@/lib/domain/underwriting/maybe-queue-underwriting-follow-up.js";
import { transferDealToUnderwriting } from "@/lib/domain/underwriting/transfer-to-underwriting.js";
import { maybeCreateContractFromAcceptedOffer } from "@/lib/domain/contracts/maybe-create-contract-from-accepted-offer.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { maybeQueueSellerStageReply } from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";
import {
  normalizeSellerFlowUseCase,
  SELLER_FLOW_STAGES,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";
import { updateMasterOwnerAfterInbound } from "@/lib/domain/master-owners/update-master-owner-after-inbound.js";
import { isNegativeReply } from "@/lib/domain/classification/is-negative-reply.js";
import { cancelPendingQueueItemsForOwner } from "@/lib/domain/queue/cancel-pending-queue-items.js";
import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import { buildInboundConversationState } from "@/lib/domain/communications-engine/state-machine.js";
import {
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
} from "@/lib/domain/events/idempotency-ledger.js";
import { findLatestOpenOffer } from "@/lib/podio/apps/offers.js";
import { handleUnknownInboundRouter } from "@/lib/domain/inbound/unknown-inbound-router.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";
import { postInboundSmsDiscordCard } from "@/lib/discord/inbound-sms-card.js";
import {
  buildInboundAutopilotSchedule,
  findInboundAutopilotQueue,
  updateInboundAutopilotQueue,
} from "@/lib/discord/inbound-autopilot-queue.js";
import { getDefaultSupabaseClient } from "@/lib/supabase/default-client.js";
import { info, warn } from "@/lib/logging/logger.js";

const defaultDeps = {
  loadContext,
  loadContextWithFallback,
  createBrain,
  classify,
  resolveRoute,
  normalizeInboundTextgridPhone,
  logInboundMessageEvent,
  updateBrainAfterInbound,
  updateBrainStage,
  maybeCreateOfferFromContext,
  maybeProgressOfferStatus,
  routeInboundOffer,
  maybeUpsertUnderwritingFromInbound,
  maybeQueueUnderwritingFollowUp,
  transferDealToUnderwriting,
  maybeCreateContractFromAcceptedOffer,
  syncPipelineState,
  maybeQueueSellerStageReply,
  updateMasterOwnerAfterInbound,
  isNegativeReply,
  cancelPendingQueueItemsForOwner,
  extractUnderwritingSignals,
  buildInboundConversationState,
  beginIdempotentProcessing,
  completeIdempotentProcessing,
  failIdempotentProcessing,
  hashIdempotencyPayload,
  findLatestOpenOffer,
  handleUnknownInboundRouter,
  notifyDiscordOps,
  postInboundSmsDiscordCard,
  buildInboundAutopilotSchedule,
  findInboundAutopilotQueue,
  updateInboundAutopilotQueue,
  getSupabaseClient: getDefaultSupabaseClient,
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

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function asPositiveInt(value, fallback = 0) {
  const numeric = Number.parseInt(clean(value), 10);
  return Number.isFinite(numeric) && numeric >= 0 ? numeric : fallback;
}

function previewText(value = "", max = 180) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, max);
}

function buildInboundContextMatchMetadata(context = {}) {
  const match =
    context?.fallback_match_data ||
    context?.recent?.outbound_pair_match ||
    context?.match ||
    null;

  if (!match || typeof match !== "object") return {};

  return {
    fallback_pair_match: Boolean(context?.fallback_pair_match),
    fallback_match_source: context?.fallback_match_source || null,
    fallback_match_id: context?.fallback_match_id || null,
    matched_queue_id:
      match.matched_queue_id ||
      match.queue_row_id ||
      context?.fallback_match_id ||
      null,
    matched_queue_status: match.matched_queue_status || null,
    matched_sent_at: match.matched_sent_at || null,
    matched_source: match.matched_source || null,
    skipped_newer_orphan_count: Number(match.skipped_newer_orphan_count || 0),
    match_strategy: match.match_strategy || null,
    context_verified: Boolean(match.context_verified),
    conversation_brain_id: context?.ids?.conversation_brain_id || null,
    textgrid_number_id: context?.ids?.textgrid_number_id || null,
  };
}

function buildAutopilotStatusText({
  autopilot_enabled = false,
  autopilot_delay_seconds = 60,
  outbound_queue_id = null,
  suggested_reply_ready = false,
} = {}) {
  if (autopilot_enabled && outbound_queue_id) {
    return `Autopilot reply scheduled in ${autopilot_delay_seconds}s`;
  }

  if (!suggested_reply_ready) {
    return "Manual review required — no safe reply generated";
  }

  return autopilot_enabled
    ? `Autopilot enabled — scheduling unavailable`
    : "Manual review required";
}

function buildDiscordReviewMetadata({
  autopilot_enabled = false,
  autopilot_delay_seconds = 0,
  suggested_reply_preview = "",
  selected_template_id = null,
  selected_template_source = null,
  outbound_queue_id = null,
  discord_review_status = null,
  discord_card_error = null,
  post_result = {},
  existing_metadata = {},
  context_incomplete = false,
} = {}) {
  const available_actions = context_incomplete
    ? ["sr:m", "sr:wn", "sr:oo"]
    : ["sr:a", "sr:m", "sr:c", "sr:ni", "sr:wn", "sr:oo", "context:open_record"];

  return {
    ...existing_metadata,
    inbound_discord_review_required: !autopilot_enabled,
    inbound_autopilot_enabled: Boolean(autopilot_enabled),
    inbound_autopilot_post_discord_card: existing_metadata?.inbound_autopilot_post_discord_card ?? true,
    autopilot_reply: Boolean(autopilot_enabled && clean(outbound_queue_id)),
    autopilot_override_window_seconds: Number(autopilot_delay_seconds || 0),
    suggested_reply_ready: Boolean(clean(suggested_reply_preview)),
    suggested_reply_preview: clean(suggested_reply_preview) || null,
    selected_template_id: clean(selected_template_id) || null,
    selected_template_source: clean(selected_template_source) || null,
    outbound_queue_id: clean(outbound_queue_id) || existing_metadata?.outbound_queue_id || null,
    discord_card_posted_at: post_result?.ok && !post_result?.skipped ? new Date().toISOString() : existing_metadata?.discord_card_posted_at || null,
    discord_channel_id: post_result?.channel_id || existing_metadata?.discord_channel_id || null,
    discord_message_id: post_result?.discord_message_id || existing_metadata?.discord_message_id || null,
    discord_card_error: clean(discord_card_error) || existing_metadata?.discord_card_error || null,
    discord_review_status:
      clean(discord_review_status) ||
      existing_metadata?.discord_review_status ||
      (autopilot_enabled && clean(outbound_queue_id) ? "autopilot_pending" : "manual_review_required"),
    discord_available_actions: available_actions,
  };
}

async function postInboundDiscordReviewCard({
  runtimeDeps,
  message_event_id = null,
  inbound_from = "",
  message_body = "",
  context = null,
  classification = null,
  route = null,
  seller_stage_reply = null,
  inbound_autopilot_enabled = false,
  inbound_autopilot_delay_seconds = 60,
  outbound_queue_id = null,
  context_incomplete = false,
  existing_metadata = {},
} = {}) {
  if (typeof runtimeDeps.postInboundSmsDiscordCard !== "function") {
    return { ok: false, skipped: true, reason: "discord_card_poster_unavailable" };
  }

  const preview_source =
    seller_stage_reply?.preview_result?.rendered_message_text ||
    seller_stage_reply?.queue_result?.rendered_message_text ||
    seller_stage_reply?.queue_result?.rendered_message_preview ||
    "";
  const selected_template_id =
    seller_stage_reply?.preview_result?.template_id ||
    seller_stage_reply?.queue_result?.template_id ||
    seller_stage_reply?.queue_result?.selected_template_id ||
    null;
  const selected_template_source =
    seller_stage_reply?.preview_result?.selected_template_source ||
    seller_stage_reply?.queue_result?.selected_template_source ||
    seller_stage_reply?.queue_result?.selected_template_resolution_source ||
    null;

  return runtimeDeps.postInboundSmsDiscordCard({
    message_event_id,
    inbound_from,
    seller_name: context?.summary?.seller_first_name || context?.summary?.owner_name || null,
    property_address: context?.summary?.property_address || null,
    market: context?.summary?.market || context?.summary?.market_name || null,
    current_stage: seller_stage_reply?.brain_stage || route?.stage || context?.summary?.conversation_stage || null,
    classification_intent:
      seller_stage_reply?.plan?.selected_use_case ||
      route?.use_case ||
      classification?.objection ||
      classification?.source ||
      null,
    language:
      seller_stage_reply?.plan?.detected_language ||
      classification?.language ||
      context?.summary?.language_preference ||
      null,
    inbound_message_body: message_body,
    suggested_reply_preview: previewText(preview_source, 300),
    selected_template_id,
    selected_template_source,
    confidence: classification?.confidence ?? null,
    classification_result:
      clean(classification?.source) ||
      clean(classification?.objection) ||
      clean(seller_stage_reply?.plan?.detected_intent) ||
      "unknown",
    safety_state:
      context_incomplete
        ? "Manual review required — context incomplete"
        : buildAutopilotStatusText({
            autopilot_enabled: inbound_autopilot_enabled,
            autopilot_delay_seconds: inbound_autopilot_delay_seconds,
            outbound_queue_id,
            suggested_reply_ready: Boolean(clean(preview_source)),
          }),
    autopilot_enabled: Boolean(inbound_autopilot_enabled),
    autopilot_status: buildAutopilotStatusText({
      autopilot_enabled: inbound_autopilot_enabled,
      autopilot_delay_seconds: inbound_autopilot_delay_seconds,
      outbound_queue_id,
      suggested_reply_ready: Boolean(clean(preview_source)),
    }),
    outbound_queue_id,
    context_incomplete,
    existing_metadata,
  });
}

function formatOfferCurrency(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return `$${n.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function buildInboundStepFailure(error, err) {
  const podio_rate_limit = isPodioRateLimitError(err);
  return {
    ok: false,
    error,
    error_message: err?.message || "unknown",
    retryable: podio_rate_limit,
    podio_rate_limit,
    retry_after_seconds: podio_rate_limit ? getPodioRetryAfterSeconds(err, null) : null,
    retry_after_at: podio_rate_limit
      ? clean(
          err?.retry_after_at ||
            err?.cooldown_until ||
            err?.response?.data?.retry_after_at ||
            err?.response?.data?.cooldown_until
        ) || null
      : null,
  };
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

const PRE_PIPELINE_USE_CASES = new Set([
  SELLER_FLOW_STAGES.OWNERSHIP_CHECK,
  SELLER_FLOW_STAGES.OWNERSHIP_CHECK_FOLLOW_UP,
  SELLER_FLOW_STAGES.CONSIDER_SELLING,
  SELLER_FLOW_STAGES.CONSIDER_SELLING_FOLLOW_UP,
  SELLER_FLOW_STAGES.WRONG_PERSON,
  SELLER_FLOW_STAGES.WHO_IS_THIS,
  SELLER_FLOW_STAGES.HOW_GOT_NUMBER,
  SELLER_FLOW_STAGES.NOT_INTERESTED,
  SELLER_FLOW_STAGES.STOP_OR_OPT_OUT,
  SELLER_FLOW_STAGES.REENGAGEMENT,
]);

function shouldCreateBrainForInbound({
  brain_id = null,
  seller_stage_reply = null,
} = {}) {
  if (brain_id) return false;

  const plan = seller_stage_reply?.plan || null;
  const selected_use_case = normalizeSellerFlowUseCase(
    plan?.selected_use_case,
    plan?.selected_variant_group
  );

  return (
    plan?.detected_intent === "Ownership Confirmed" &&
    selected_use_case === SELLER_FLOW_STAGES.CONSIDER_SELLING
  );
}

function shouldCreatePipelineForInbound({
  seller_stage_reply = null,
  route = null,
  active_offer_item_id = null,
  contract_item_id = null,
} = {}) {
  if (active_offer_item_id || contract_item_id) return true;

  const seller_stage_use_case = normalizeSellerFlowUseCase(
    seller_stage_reply?.plan?.selected_use_case,
    seller_stage_reply?.plan?.selected_variant_group
  );

  if (seller_stage_use_case === SELLER_FLOW_STAGES.ASKING_PRICE) {
    return true;
  }

  const routed_use_case = normalizeSellerFlowUseCase(
    route?.use_case,
    route?.variant_group
  );

  if (!routed_use_case) return false;

  return !PRE_PIPELINE_USE_CASES.has(routed_use_case);
}

function shouldBypassInboundOfferRouting({ classification = null, route = null } = {}) {
  if (classification?.compliance_flag === "stop_texting") return true;

  const routed_use_case = normalizeSellerFlowUseCase(
    route?.use_case,
    route?.variant_group
  );

  return [
    SELLER_FLOW_STAGES.WRONG_PERSON,
    SELLER_FLOW_STAGES.STOP_OR_OPT_OUT,
  ].includes(routed_use_case);
}

export async function handleTextgridInboundWebhook(payload = {}, opts = {}) {
  const {
    inbound_debug_stage = null,
    dry_run = false,
    auto_reply_enabled = null,
    auto_post_discord_card = null,
    auto_reply_delay_seconds = null,
    inbound_user_initiated = true,
  } = opts;

  const inbound_autopilot_enabled = asBoolean(
    auto_reply_enabled,
    asBoolean(process.env.INBOUND_AUTOPILOT_ENABLED, true)
  );
  const inbound_autopilot_post_discord_card = asBoolean(
    auto_post_discord_card,
    asBoolean(process.env.INBOUND_AUTOPILOT_POST_DISCORD_CARD, true)
  );
  const inbound_autopilot_delay_seconds = asPositiveInt(
    auto_reply_delay_seconds,
    asPositiveInt(process.env.INBOUND_AUTOPILOT_DELAY_SECONDS, 60)
  );

  if (inbound_debug_stage === "handler_entry") {
    return { ok: true, stage: "handler_entry" };
  }

  let extracted, inbound_from, inbound_to, message_body;
  try {
    extracted = extractWebhookPayload(payload);
    if (inbound_debug_stage === "after_extract") {
      return { ok: true, stage: "after_extract" };
    }

    inbound_from = runtimeDeps.normalizeInboundTextgridPhone(extracted.from);
    if (inbound_debug_stage === "after_normalize_from") {
      return { ok: true, stage: "after_normalize_from" };
    }

    inbound_to = runtimeDeps.normalizeInboundTextgridPhone(extracted.to);
    if (inbound_debug_stage === "after_normalize_to") {
      return { ok: true, stage: "after_normalize_to" };
    }

    message_body = extracted.body;

    try {
      runtimeDeps.info("textgrid.inbound_received", {
        message_id: extracted.message_id,
        inbound_from,
        inbound_to,
        body_preview: String(message_body || "").slice(0, 120),
      });
    } catch {}

    if (inbound_debug_stage === "after_inbound_received_log") {
      return { ok: true, stage: "after_inbound_received_log" };
    }
  } catch (error) {
    return {
      ok: false,
      error: "textgrid_inbound_failed_handler_entry",
      detail: error?.message || "unknown_handler_entry_error",
    };
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
    return buildInboundStepFailure("textgrid_inbound_failed_message_event_lookup", err);
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
  let message_event_enriched = false;

  async function failStepAndReturn(stepError, err) {
    try {
      await runtimeDeps.failIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_inbound",
        key: idempotency_key,
        error: err,
        skip_content_fields: message_event_enriched,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          inbound_from,
          inbound_to,
        },
      });
    } catch (_) { /* best-effort */ }
    return buildInboundStepFailure(stepError, err);
  }

  try {
    // ── SEGMENT: brain_lookup ───────────────────────────────────────────────
    let context;
    try {
      const fallback_overridden =
        runtimeDeps.loadContextWithFallback !== defaultDeps.loadContextWithFallback;

      if (fallback_overridden) {
        context = await runtimeDeps.loadContextWithFallback({
          inbound_from,
          inbound_to,
          create_brain_if_missing: true,
        });
      } else {
        context = await runtimeDeps.loadContext({
          inbound_from,
          create_brain_if_missing: true,
        });

        if (
          !context?.found &&
          clean(context?.reason || "phone_not_found").toLowerCase() === "phone_not_found"
        ) {
          context = await runtimeDeps.loadContextWithFallback({
            inbound_from,
            inbound_to,
            create_brain_if_missing: true,
            primary_context: context,
            loadContextImpl: runtimeDeps.loadContext,
          });
        }
      }
    } catch (err) {
      return failStepAndReturn("textgrid_inbound_failed_brain_lookup", err);
    }

    if (!context?.found) {
      safeWarn("textgrid.inbound_context_not_found", {
        message_id: extracted.message_id,
        inbound_from,
        reason: context?.reason || "unknown",
      });

      let unknown_result;
      try {
        unknown_result = await runtimeDeps.handleUnknownInboundRouter({
          message_id: extracted.message_id,
          inbound_from,
          inbound_to,
          message_body,
          dry_run: Boolean(dry_run),
          auto_reply_enabled: inbound_autopilot_enabled,
          inbound_autopilot_enabled,
          inbound_user_initiated: Boolean(inbound_user_initiated),
        });
      } catch (err) {
        return failStepAndReturn("textgrid_inbound_failed_unknown_router", err);
      }

      let fallback_message_event_id = null;
      try {
        const fallback_event = await runtimeDeps.logInboundMessageEvent({
          brain_item: null,
          conversation_item_id: null,
          master_owner_id: null,
          prospect_id: null,
          property_id: null,
          market_id: null,
          phone_item_id: null,
          inbound_number_item_id: null,
          sms_agent_id: null,
          property_address: null,
          message_body,
          provider_message_id: extracted.message_id,
          raw_carrier_status: extracted.status || "received",
          received_at: extracted.received_at || payload?.http_received_at || new Date().toISOString(),
          processed_by: "Manual Sender",
          source_app: "External API",
          trigger_name: "textgrid-inbound",
          inbound_from,
          inbound_to,
          metadata: {
            inbound_discord_review_required: true,
            inbound_autopilot_enabled,
            suggested_reply_ready: false,
            discord_review_status: "pending",
          },
        });
        fallback_message_event_id = fallback_event?.item_id || null;
      } catch {}

      await postInboundDiscordReviewCard({
        runtimeDeps,
        message_event_id: fallback_message_event_id,
        inbound_from,
        message_body,
        context: null,
        classification: null,
        route: null,
        seller_stage_reply: null,
        inbound_autopilot_enabled: false,
        context_incomplete: true,
        existing_metadata: {},
      }).catch(() => {});

      await runtimeDeps.completeIdempotentProcessing({
        record_item_id: idempotency.record_item_id,
        scope: "textgrid_inbound",
        key: idempotency_key,
        summary: `Inbound SMS handled by unknown router: ${unknown_result?.unknown_router?.bucket || "unknown"}`,
        metadata: {
          provider_message_id: clean(extracted.message_id) || null,
          inbound_from,
          inbound_to,
          result_reason: context?.reason || "context_not_found",
          unknown_inbound: true,
          unknown_bucket: unknown_result?.unknown_router?.bucket || null,
          auto_reply_queued: Boolean(unknown_result?.unknown_router?.auto_reply_queued),
          suppression_applied: Boolean(unknown_result?.unknown_router?.suppression_applied),
          dry_run: Boolean(dry_run),
        },
      });

      return unknown_result;
    }

    let brain_item = context.items?.brain_item || null;
    const fallback_conversation_brain_id = asPositiveInt(context.ids?.conversation_brain_id, null) || null;
    let brain_id = context.ids?.brain_item_id || fallback_conversation_brain_id;
    const master_owner_id = context.ids?.master_owner_id || null;
    const prospect_id = context.ids?.prospect_id || null;
    const property_id = context.ids?.property_id || null;
    const phone_item_id = context.ids?.phone_item_id || null;
    const market_id = context.ids?.market_id || null;
    const sms_agent_id = context.ids?.assigned_agent_id || null;
    const property_address = context.summary?.property_address || null;
    const latest_outbound_event =
      context.recent?.recent_events?.find(
        (event) => clean(event?.direction).toLowerCase() === "outbound"
      ) || null;
    const inbound_number_item_id =
      latest_outbound_event?.textgrid_number_item_id ||
      context.ids?.textgrid_number_id ||
      null;
    const prior_message_id = latest_outbound_event?.message_id || null;
    const response_to_message_id = prior_message_id;
    const stage_before = context.summary?.conversation_stage || null;
    const inbound_context_match_metadata = buildInboundContextMatchMetadata(context);

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
    // Create the canonical seller Message Events row early, then rehydrate
    // that same row later if the Brain / stage context becomes richer during
    // the rest of the inbound pipeline.
    let inbound_message_event_id = null;
    try {
      const inbound_event = await runtimeDeps.logInboundMessageEvent({
        brain_item,
        conversation_item_id: brain_id,
        master_owner_id,
        prospect_id,
        property_id,
        market_id,
        phone_item_id,
        inbound_number_item_id,
        sms_agent_id,
        property_address,
        message_body,
        provider_message_id: extracted.message_id,
        raw_carrier_status: extracted.status || "received",
        received_at: extracted.received_at || payload?.http_received_at || new Date().toISOString(),
        processed_by: "Manual Sender",
        source_app: "External API",
        trigger_name: "textgrid-inbound",
        inbound_from,
        inbound_to,
        prior_message_id,
        response_to_message_id,
        stage_before,
        metadata: inbound_context_match_metadata,
      });
      inbound_message_event_id = inbound_event?.item_id || null;
      message_event_enriched = true;
    } catch (err) {
      return failStepAndReturn("textgrid_inbound_failed_message_event_create", err);
    }

    if (inbound_debug_stage === "after_message_event_create") {
      return { ok: true, stage: "after_message_event_create" };
    }

    // ── SEGMENT: conversation_resolution ─────────────────────────────────
    // Classify the message body, handle negative-reply cancellations, and
    // resolve the routing decision.
    let classification, inbound_is_negative, queue_cancellation, route, signals,
      deterministic_state, offer_routing;
    try {
      classification = await runtimeDeps.classify(message_body, brain_item);
      signals = runtimeDeps.extractUnderwritingSignals({
        message: message_body,
        classification,
        route: null,
        context,
      });

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

      signals = runtimeDeps.extractUnderwritingSignals({
        message: message_body,
        classification,
        route,
        context,
      });
      deterministic_state = runtimeDeps.buildInboundConversationState({
        context,
        classification,
        route,
        message: message_body,
        signals,
      });

      offer_routing = shouldBypassInboundOfferRouting({ classification, route })
        ? {
            ok: true,
            offer_route: "bypassed_existing_suppression",
            reason: "existing_compliance_or_wrong_number_route",
            meta: { bypassed: true },
          }
        : await runtimeDeps.routeInboundOffer({
            seller_message: message_body,
            message: message_body,
            classification,
            context,
            route,
            property: context.items?.property_item || null,
            owner: context.items?.master_owner_item || null,
            deal_strategy: route?.deal_strategy || context.summary?.deal_strategy || null,
          });
    } catch (err) {
      classification = {
        language: context?.summary?.language_preference || "English",
        source: "inbound_review_fallback",
        confidence: 0,
        notes: err?.message || "conversation_resolution_failed",
      };
      route = {
        stage: context?.summary?.conversation_stage || "unknown",
        use_case: null,
      };
      signals = {};
      deterministic_state = null;
      offer_routing = {
        ok: true,
        offer_route: "manual_review",
        reason: err?.message || "conversation_resolution_failed",
      };
      safeWarn("textgrid.inbound_conversation_resolution_degraded", {
        message_id: extracted.message_id,
        inbound_from,
        error: err?.message || "unknown",
      });
    }

    if (inbound_debug_stage === "after_conversation_resolution") {
      return { ok: true, stage: "after_conversation_resolution", route_stage: route?.stage || null, classification_source: classification?.source || null };
    }

    // ── SEGMENT: prospect_resolution ──────────────────────────────────────
    // Write brain activity, master-owner timestamps, and stage/language/profile
    // updates in parallel.
    try {
      await runtimeDeps.updateMasterOwnerAfterInbound({
        master_owner_id,
        received_at: new Date().toISOString(),
      });

      if (brain_id) {
        await runtimeDeps.updateBrainAfterInbound({
          brain_id,
          message_body,
          follow_up_trigger_state:
            deterministic_state?.follow_up_trigger_state || "AI Running",
          deterministic_state,
          extra_fields: {
            "master-owner": master_owner_id || undefined,
            prospect: prospect_id || undefined,
            ...(property_id ? { properties: [property_id] } : {}),
            ...(sms_agent_id ? { "sms-agent": sms_agent_id } : {}),
          },
        });
      }
    } catch (err) {
      return failStepAndReturn("textgrid_inbound_failed_prospect_resolution", err);
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
      return failStepAndReturn("textgrid_inbound_failed_market_resolution", err);
    }

    if (inbound_debug_stage === "after_market_resolution") {
      return { ok: true, stage: "after_market_resolution", existing_offer_item_id: existing_offer?.item_id || null };
    }

    // ── SEGMENT: podio_write ──────────────────────────────────────────────
    // All offer, underwriting, contract, and pipeline writes happen here.
    let maybe_offer_progress, initial_offer, underwriting, seller_stage_preview,
      seller_stage_reply, underwriting_follow_up, maybe_offer, active_offer_item_id,
      contract, pipeline, underwriting_transfer, autopilot_queue_row = null;

    try {
      const offer_route = offer_routing?.offer_route || null;
      const defer_immediate_offer_create = [
        "underwriting",
        "sfh_cash_preview",
        "condition_clarifier",
        "manual_review",
      ].includes(offer_route);

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
        : defer_immediate_offer_create
          ? {
              ok: true,
              created: false,
              reason: `offer_route_${offer_route}_deferred`,
            }
        : await runtimeDeps.maybeCreateOfferFromContext({
            context,
            classification,
            route,
            message: message_body,
            notes: message_body,
            created_by: "Inbound Offer Engine",
          });

      underwriting_transfer = offer_route === "underwriting"
        ? await runtimeDeps.transferDealToUnderwriting({
            owner: context.items?.master_owner_item || null,
            property: context.items?.property_item || null,
            prospect: context.items?.prospect_item || null,
            phone: context.items?.phone_item || null,
            sellerMessage: message_body,
            routeReason: offer_routing?.reason || offer_routing?.meta?.underwriting_reason || "offer_route_underwriting",
            dealStrategy: route?.deal_strategy || context.summary?.deal_strategy || null,
            sourceMessageEventId: inbound_message_event_id,
          })
        : null;

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

      if (offer_route === "sfh_cash_preview") {
        const cash_offer = offer_routing?.meta?.cash_offer ?? null;
        const snapshot_id = offer_routing?.meta?.snapshot_id ?? null;
        seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
          inbound_from,
          context,
          classification,
          message: message_body,
          maybe_offer: initial_offer,
          existing_offer,
          explicit_use_case: "offer_reveal_cash",
          explicit_template_lookup_use_case: "offer_reveal_cash",
          force_queue_reply: true,
          extra_queue_context: {
            offer_route,
            cash_offer_amount: cash_offer,
            cash_offer_snapshot_id: snapshot_id,
          },
          extra_template_render_overrides: {
            offer_price: formatOfferCurrency(cash_offer),
            smart_cash_offer_display: formatOfferCurrency(cash_offer),
          },
          cash_offer_snapshot_id: snapshot_id,
          preview_only: true,
        });
      } else if (offer_route === "condition_clarifier") {
        seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
          inbound_from,
          context,
          classification,
          message: message_body,
          maybe_offer: initial_offer,
          existing_offer,
          explicit_use_case: "ask_condition_clarifier",
          explicit_template_lookup_use_case: "ask_condition_clarifier",
          force_queue_reply: true,
          extra_queue_context: {
            offer_route,
            condition_clarifier_reason: offer_routing?.reason || null,
          },
          preview_only: true,
        });
      } else if (offer_route === "manual_review") {
        seller_stage_preview = {
          ok: true,
          queued: false,
          handled: true,
          reason: "offer_manual_review_no_auto_send",
          plan: {
            selected_use_case: null,
            detected_intent: null,
          },
          brain_stage: null,
        };

        safeWarn("textgrid.inbound_offer_manual_review", {
          message_id: extracted.message_id,
          inbound_from,
          master_owner_id,
          property_id,
          offer_route_reason: offer_routing?.reason || null,
        });
      } else {
        seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
          inbound_from,
          context,
          classification,
          message: message_body,
          maybe_offer: initial_offer,
          existing_offer,
          preview_only: true,
        });
      }

      seller_stage_reply = seller_stage_preview;

      if (shouldCreateBrainForInbound({ brain_id, seller_stage_reply: seller_stage_preview })) {
        brain_item = await runtimeDeps.createBrain({
          master_owner_id,
          prospect_id,
          property_id,
          phone_item_id,
        });
        brain_id = brain_item?.item_id || null;

        if (brain_id) {
          context.items = {
            ...(context.items || {}),
            brain_item,
          };
          context.ids = {
            ...(context.ids || {}),
            brain_item_id: brain_id,
          };
          context.summary = {
            ...(context.summary || {}),
            brain_item_id: brain_id,
          };

          await runtimeDeps.updateBrainAfterInbound({
            brain_id,
            message_body,
            follow_up_trigger_state:
              deterministic_state?.follow_up_trigger_state || "AI Running",
            deterministic_state,
            extra_fields: {
              "master-owner": master_owner_id || undefined,
              prospect: prospect_id || undefined,
              ...(property_id ? { properties: [property_id] } : {}),
              ...(sms_agent_id ? { "sms-agent": sms_agent_id } : {}),
            },
          });
        }
      }

      if (seller_stage_preview?.brain_stage && brain_id) {
        await runtimeDeps.updateBrainStage({ brain_id, stage: seller_stage_preview.brain_stage });
      }

      underwriting_follow_up = !inbound_autopilot_enabled
        ? { ok: true, queued: false, reason: "manual_review_required" }
        : seller_stage_preview?.handled
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
        defer_immediate_offer_create ||
        initial_offer?.created ||
        initial_offer?.existing_offer_item_id ||
        !underwriting_offer_ready
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

      const suggested_reply_preview =
        seller_stage_preview?.preview_result?.rendered_message_text ||
        seller_stage_preview?.queue_result?.rendered_message_text ||
        "";
      const autopilot_ready = Boolean(
        inbound_autopilot_enabled &&
        seller_stage_preview?.ok &&
        seller_stage_preview?.handled &&
        clean(suggested_reply_preview)
      );

      if (autopilot_ready && inbound_message_event_id) {
        const existing_autopilot_queue = await runtimeDeps.findInboundAutopilotQueue({
          message_event_id: inbound_message_event_id,
          supabase: runtimeDeps.getSupabaseClient?.() || null,
          includeStatuses: ["queued", "sending"],
        }).catch(() => null);

        if (existing_autopilot_queue?.id) {
          autopilot_queue_row = existing_autopilot_queue;
        } else {
          const autopilot_schedule = runtimeDeps.buildInboundAutopilotSchedule(
            inbound_autopilot_delay_seconds,
            new Date().toISOString()
          );

          const queue_extra_context = {
            ...(seller_stage_preview?.queue_result?.queue_context || {}),
            inbound_message_event_id: inbound_message_event_id,
            autopilot_reply: true,
            autopilot_override_window_seconds: inbound_autopilot_delay_seconds,
            discord_review_status: "autopilot_pending",
            action_type: "autopilot_inbound_reply",
          };

          let queued_autopilot_reply = null;
          if (offer_route === "sfh_cash_preview") {
            const cash_offer = offer_routing?.meta?.cash_offer ?? null;
            const snapshot_id = offer_routing?.meta?.snapshot_id ?? null;
            queued_autopilot_reply = await runtimeDeps.maybeQueueSellerStageReply({
              inbound_from,
              context,
              classification,
              message: message_body,
              maybe_offer: initial_offer,
              existing_offer,
              explicit_use_case: "offer_reveal_cash",
              explicit_template_lookup_use_case: "offer_reveal_cash",
              force_queue_reply: true,
              extra_queue_context: {
                offer_route,
                cash_offer_amount: cash_offer,
                cash_offer_snapshot_id: snapshot_id,
                ...queue_extra_context,
              },
              extra_template_render_overrides: {
                offer_price: formatOfferCurrency(cash_offer),
                smart_cash_offer_display: formatOfferCurrency(cash_offer),
              },
              cash_offer_snapshot_id: snapshot_id,
              preview_only: false,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              send_priority_override: "_ Urgent",
            });
          } else if (offer_route === "condition_clarifier") {
            queued_autopilot_reply = await runtimeDeps.maybeQueueSellerStageReply({
              inbound_from,
              context,
              classification,
              message: message_body,
              maybe_offer: initial_offer,
              existing_offer,
              explicit_use_case: "ask_condition_clarifier",
              explicit_template_lookup_use_case: "ask_condition_clarifier",
              force_queue_reply: true,
              extra_queue_context: {
                offer_route,
                condition_clarifier_reason: offer_routing?.reason || null,
                ...queue_extra_context,
              },
              preview_only: false,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              send_priority_override: "_ Urgent",
            });
          } else {
            queued_autopilot_reply = await runtimeDeps.maybeQueueSellerStageReply({
              inbound_from,
              context,
              classification,
              message: message_body,
              maybe_offer: initial_offer,
              existing_offer,
              extra_queue_context: queue_extra_context,
              preview_only: false,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              send_priority_override: "_ Urgent",
            });
          }

          if (queued_autopilot_reply?.ok && queued_autopilot_reply?.queue_item_id) {
            autopilot_queue_row = {
              id: queued_autopilot_reply.queue_item_id,
              queue_status: "queued",
              scheduled_for: autopilot_schedule.scheduled_for,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              metadata: {
                inbound_message_event_id: inbound_message_event_id,
                autopilot_reply: true,
                autopilot_override_window_seconds: inbound_autopilot_delay_seconds,
                discord_review_status: "autopilot_pending",
              },
            };
            seller_stage_reply = {
              ...queued_autopilot_reply,
              preview_result:
                seller_stage_preview?.preview_result ||
                seller_stage_preview?.queue_result ||
                null,
            };
          }
        }
      }

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
        create_if_missing: shouldCreatePipelineForInbound({
          seller_stage_reply,
          route,
          active_offer_item_id,
          contract_item_id: contract?.contract_item_id || null,
        }),
        property_id,
        master_owner_id,
        prospect_id,
        conversation_item_id: brain_id,
        offer_item_id: active_offer_item_id,
        contract_item_id: contract?.contract_item_id || null,
        notes: `Inbound SMS processed${route?.stage ? ` at stage ${route.stage}` : ""}.`,
      });

      if (inbound_message_event_id) {
        const suggested_reply_preview =
          seller_stage_reply?.preview_result?.rendered_message_text ||
          seller_stage_reply?.queue_result?.rendered_message_text ||
          seller_stage_preview?.preview_result?.rendered_message_text ||
          seller_stage_preview?.queue_result?.rendered_message_text ||
          "";
        const selected_template_id =
          seller_stage_reply?.preview_result?.template_id ||
          seller_stage_reply?.queue_result?.template_id ||
          seller_stage_preview?.preview_result?.template_id ||
          seller_stage_preview?.queue_result?.template_id ||
          null;
        const selected_template_source =
          seller_stage_reply?.preview_result?.selected_template_source ||
          seller_stage_reply?.queue_result?.selected_template_source ||
          seller_stage_reply?.queue_result?.selected_template_resolution_source ||
          seller_stage_preview?.preview_result?.selected_template_source ||
          seller_stage_preview?.queue_result?.selected_template_source ||
          seller_stage_preview?.queue_result?.selected_template_resolution_source ||
          null;
        const outbound_queue_id = autopilot_queue_row?.id || seller_stage_reply?.queue_item_id || null;
        const context_incomplete = Boolean(
          !context?.summary?.property_address || !context?.ids?.master_owner_id || !context?.ids?.property_id
        );
        const discord_review_status = outbound_queue_id && inbound_autopilot_enabled
          ? "autopilot_pending"
          : clean(suggested_reply_preview)
            ? "manual_review_required"
            : "manual_review_required";

        let discord_card = {
          ok: true,
          skipped: !inbound_autopilot_post_discord_card,
          reason: inbound_autopilot_post_discord_card ? null : "discord_card_disabled",
        };

        if (inbound_autopilot_post_discord_card) {
          discord_card = await postInboundDiscordReviewCard({
            runtimeDeps,
            message_event_id: inbound_message_event_id,
            inbound_from,
            message_body,
            context,
            classification,
            route,
            seller_stage_reply,
            inbound_autopilot_enabled: Boolean(inbound_autopilot_enabled && outbound_queue_id),
            inbound_autopilot_delay_seconds,
            outbound_queue_id,
            context_incomplete,
            existing_metadata: {},
          }).catch((error) => ({
            ok: false,
            reason: "discord_card_post_failed",
            error: error?.message || "discord_card_post_failed",
          }));
        }

        const discord_card_error = !discord_card?.ok
          ? clean(discord_card?.error || discord_card?.reason || "discord_card_post_failed")
          : null;

        if (discord_card_error) {
          safeWarn("textgrid.inbound_discord_card_failed", {
            message_id: extracted.message_id,
            inbound_from,
            message_event_id: inbound_message_event_id,
            discord_card_error,
          });
        }

        await runtimeDeps.logInboundMessageEvent({
          record_item_id: inbound_message_event_id,
          brain_item,
          conversation_item_id: brain_id,
          master_owner_id,
          prospect_id,
          property_id,
          market_id,
          phone_item_id,
          inbound_number_item_id,
          sms_agent_id,
          property_address,
          message_body,
          provider_message_id: extracted.message_id,
          raw_carrier_status: extracted.status || "received",
          received_at:
            extracted.received_at ||
            payload?.http_received_at ||
            new Date().toISOString(),
          processed_by: "Manual Sender",
          source_app: "External API",
          trigger_name: "textgrid-inbound",
          inbound_from,
          inbound_to,
          prior_message_id,
          response_to_message_id,
          stage_before,
          stage_after:
            seller_stage_reply?.brain_stage ||
            deterministic_state?.conversation_stage ||
            route?.stage ||
            null,
          is_opt_out:
            seller_stage_reply?.plan?.selected_use_case === SELLER_FLOW_STAGES.STOP_OR_OPT_OUT ||
            inbound_is_negative,
          metadata: {
            ...inbound_context_match_metadata,
            classification_source: classification?.source || null,
            classification_result:
              classification?.objection ||
              classification?.source ||
              seller_stage_reply?.plan?.detected_intent ||
              null,
            route_stage: route?.stage || null,
            route_use_case: route?.use_case || null,
            seller_stage_use_case:
              seller_stage_reply?.plan?.selected_use_case || null,
            ...buildDiscordReviewMetadata({
              autopilot_enabled: Boolean(inbound_autopilot_enabled && outbound_queue_id),
              autopilot_delay_seconds: inbound_autopilot_delay_seconds,
              suggested_reply_preview,
              selected_template_id,
              selected_template_source,
              outbound_queue_id,
              discord_review_status,
              discord_card_error,
              post_result: discord_card,
              existing_metadata: {},
              context_incomplete,
            }),
            offer_route: offer_routing?.offer_route || null,
            offer_route_reason: offer_routing?.reason || null,
            underwriting_route_reason:
              offer_routing?.offer_route === "underwriting"
                ? offer_routing?.reason || null
                : null,
          },
        });
      }
    } catch (err) {
      return failStepAndReturn("textgrid_inbound_failed_podio_write", err);
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
      offer_route: offer_routing?.offer_route || null,
      offer_route_reason: offer_routing?.reason || null,
      underwriting_route_reason:
        offer_routing?.offer_route === "underwriting"
          ? offer_routing?.reason || null
          : null,
    });

    await runtimeDeps.notifyDiscordOps({
      event_type: inbound_is_negative ? "inbound_not_lead" : "inbound_known_reply",
      severity: inbound_is_negative ? "warning" : "info",
      domain: "inbound",
      title: inbound_is_negative ? "Inbound Reply (Not Lead)" : "Inbound Reply (Known Contact)",
      summary: `from=${inbound_from} stage=${route?.stage || "unknown"}`,
      fields: [
        { name: "Route Stage", value: route?.stage || "unknown", inline: true },
        { name: "Use Case", value: route?.use_case || "unknown", inline: true },
        { name: "Offer Created", value: String(Boolean(maybe_offer?.created)), inline: true },
      ],
      metadata: {
        message_id: extracted.message_id,
        master_owner_id,
        prospect_id,
        property_id,
      },
    });

    if (Boolean(maybe_offer?.created) || Boolean(maybe_offer_progress?.updated) || Boolean(contract?.created)) {
      await runtimeDeps.notifyDiscordOps({
        event_type: "inbound_hot_lead",
        severity: "hot",
        domain: "deal_flow",
        title: "Inbound Hot Lead Signal",
        summary: `Inbound advanced deal flow (offer=${Boolean(maybe_offer?.created)}, progress=${Boolean(maybe_offer_progress?.updated)}, contract=${Boolean(contract?.created)})`,
        fields: [
          { name: "From", value: inbound_from, inline: true },
          { name: "Stage", value: route?.stage || "unknown", inline: true },
          { name: "Property", value: property_address || "n/a", inline: false },
        ],
        metadata: {
          master_owner_id,
          prospect_id,
          property_id,
        },
      });
    }

    if (seller_stage_reply?.plan?.selected_use_case === SELLER_FLOW_STAGES.WRONG_PERSON) {
      await runtimeDeps.notifyDiscordOps({
        event_type: "wrong_number",
        severity: "warning",
        domain: "inbound",
        title: "Wrong Number Reply",
        summary: `Known contact indicated wrong person: ${inbound_from}`,
        metadata: {
          message_id: extracted.message_id,
          route_use_case: seller_stage_reply?.plan?.selected_use_case,
        },
      });
    }

    if (seller_stage_reply?.plan?.selected_use_case === SELLER_FLOW_STAGES.STOP_OR_OPT_OUT) {
      await runtimeDeps.notifyDiscordOps({
        event_type: "opt_out",
        severity: "warning",
        domain: "inbound",
        title: "Inbound Opt-Out",
        summary: `Opt-out detected from ${inbound_from}`,
        metadata: {
          message_id: extracted.message_id,
          route_use_case: seller_stage_reply?.plan?.selected_use_case,
        },
      });
    }

    safeInfo("textgrid.inbound_ops_notified", {
      message_id: extracted.message_id,
      inbound_from,
      route_stage: route?.stage || null,
      inbound_is_negative,
      offer_created: Boolean(maybe_offer?.created),
      offer_progressed: Boolean(maybe_offer_progress?.updated),
      contract_created: Boolean(contract?.created),
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
      offer_routing,
      diagnostics: {
        offer_route: offer_routing?.offer_route || null,
        offer_route_reason: offer_routing?.reason || null,
        underwriting_route_reason:
          offer_routing?.offer_route === "underwriting"
            ? offer_routing?.reason || null
            : null,
      },
      underwriting,
      underwriting_transfer,
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
      skip_content_fields: message_event_enriched,
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
      skip_content_fields: message_event_enriched,
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
