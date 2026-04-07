// ─── process-send-queue.js ───────────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  getItem,
  getTextValue,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getPhoneValue,
  getNumberValue,
  getFirstMatchingItem,
  updateItem,
  createMessageEvent,
  PodioError,
  normalizeLanguage,
  isRevisionLimitExceeded,
} from "@/lib/providers/podio.js";

import {
  sendTextgridSMS,
  getTextgridSendEndpoint,
  mapTextgridFailureBucket,
  normalizePhone,
} from "@/lib/providers/textgrid.js";

import { validateActivePhone } from "@/lib/domain/compliance/validate-active-phone.js";
import { shouldSuppressOutreach } from "@/lib/domain/compliance/should-suppress-outreach.js";
import { validateSendQueueItem } from "@/lib/domain/queue/validate-send-queue-item.js";
import { logOutboundMessageEvent } from "@/lib/domain/events/log-outbound-message-event.js";
import {
  buildQueueClientReferenceId,
  buildQueueMessageEventMetadata,
  buildQueueSendFailedTriggerName,
  serializeMessageEventMetadata,
} from "@/lib/domain/events/message-event-metadata.js";
import { updateBrainAfterSend } from "@/lib/domain/brain/update-brain-after-send.js";
import { updateMasterOwnerAfterSend } from "@/lib/domain/master-owners/update-master-owner-after-send.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { renderTemplate } from "@/lib/domain/templates/render-template.js";
import { loadRecentTemplates } from "@/lib/domain/context/load-recent-templates.js";
import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";
import { findPropertyItems } from "@/lib/podio/apps/properties.js";
import { normalizeTemplateItem } from "@/lib/podio/apps/templates.js";
import { TEXTGRID_NUMBER_FIELDS } from "@/lib/podio/apps/textgrid-numbers.js";
import { normalizeForQueueText } from "@/lib/domain/queue/build-send-queue-item.js";
import {
  deriveCanonicalSellerFlowFromTemplate,
  inferCanonicalUseCaseFromOutboundText,
  canonicalStageForUseCase,
  normalizeSellerFlowTone,
} from "@/lib/domain/seller-flow/canonical-seller-flow.js";

import { info, warn } from "@/lib/logging/logger.js";

const QUEUE_FIELDS = {
  queue_status: "queue-status",
  scheduled_for_local: "scheduled-for-local",
  scheduled_for_utc: "scheduled-for-utc",
  timezone: "timezone",
  send_priority: "send-priority",
  retry_count: "retry-count",
  max_retries: "max-retries",

  master_owner: "master-owner",
  prospects: "prospects",
  properties: "properties",
  phone_number: "phone-number",
  market: "market",
  sms_agent: "sms-agent",
  textgrid_number: "textgrid-number",
  template: "template",

  message_type: "message-type",
  message_text: "message-text",
  personalization_tags_used: "personalization-tags-used",
  character_count: "character-count",

  sent_at: "sent-at",
  delivered_at: "delivered-at",
  failed_reason: "failed-reason",
  delivery_confirmed: "delivery-confirmed",
};

const EVENT_FIELDS = {
  message_id: "message-id",
  timestamp: "timestamp",
  direction: "direction",
  message_variant: "message-variant",
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
  conversation: "conversation",
  market: "market",
  ai_route: "ai-route",
  processed_by: "processed-by",
  source_app: "source-app",
  trigger_name: "trigger-name",
  message: "message",
  template_selected: "template-selected",
  character_count: "character-count",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  latency_ms: "latency-ms",
  failure_bucket: "failure-bucket",
  is_final_failure: "is-final-failure",
  ai_output: "ai-output",
};

function nowIso() {
  return new Date().toISOString();
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function asArrayAppRef(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? [parsed] : undefined;
}

function isPositiveCategory(value) {
  const raw = lower(value);
  return [
    "yes",
    "true",
    "active",
    "enabled",
    "available",
    "on",
    "_ active",
    "_ warming up",
  ].includes(raw);
}

function isNegativeCategory(value) {
  const raw = lower(value);
  return [
    "no",
    "false",
    "inactive",
    "disabled",
    "retired",
    "blocked",
    "off",
    "_ paused",
    "_ flagged",
    "⚫ retired",
  ].includes(raw);
}

function shouldRetryQueueUpdateWithoutTemplate(error) {
  if (!(error instanceof PodioError)) return false;
  // Revision-limit errors must NOT be retried — the item cannot be written to at all.
  if (isRevisionLimitExceeded(error)) return false;

  const message = clean(error?.message).toLowerCase();
  return (
    error.status === 400 &&
    (
      message.includes("template") ||
      message.includes("referenced") ||
      message.includes("item") ||
      message.includes("value")
    )
  );
}

function logRevisionLimitPhase(queue_item_id, operation, phase, err) {
  if (!isRevisionLimitExceeded(err)) return;
  warn("queue.process_revision_limit_at_phase", {
    queue_item_id,
    operation,
    phase,
    failure_bucket: "revision_limit_exceeded",
    skipped: true,
    message: err?.message || null,
  });
}

function getTouchNumber(queue_item) {
  const touch_number = Number(getNumberValue(queue_item, "touch-number", 1) || 1);
  return Number.isFinite(touch_number) && touch_number > 0 ? touch_number : 1;
}

function deriveOwnerTouchCountFromQueue(queue_item) {
  return Math.max(0, getTouchNumber(queue_item) - 1);
}

const FOLLOW_UP_CONTACT_STATUSES = new Set([
  "contacted",
  "engaged",
  "offer sent",
  "negotiating",
]);

const FOLLOW_UP_CONTACT_STATUS_2 = new Set([
  "sent",
  "received",
  "follow-up scheduled",
]);

function deriveOwnerStageHint(owner_item) {
  const contact_status = lower(getCategoryValue(owner_item, "contact-status", null));
  const contact_status_2 = lower(getCategoryValue(owner_item, "contact-status-2", null));
  const has_offer = Boolean(getFirstAppReferenceId(owner_item, "offer", null));

  if (has_offer || ["offer sent", "negotiating"].includes(contact_status)) {
    return "Offer";
  }

  if (
    FOLLOW_UP_CONTACT_STATUSES.has(contact_status) ||
    FOLLOW_UP_CONTACT_STATUS_2.has(contact_status_2)
  ) {
    return "Follow-Up";
  }

  return "Ownership";
}

function deriveOwnerEmotion(owner_item) {
  const urgency = Number(getNumberValue(owner_item, "urgency-score", 0) || 0);
  const financial_pressure = Number(
    getNumberValue(owner_item, "financial-pressure-score", 0) || 0
  );
  const tax_delinquent = Number(
    getNumberValue(owner_item, "portfolio-tax-delinquent-count", 0) || 0
  );
  const lien_count = Number(getNumberValue(owner_item, "portfolio-lien-count", 0) || 0);

  if (
    urgency >= 80 ||
    financial_pressure >= 80 ||
    tax_delinquent > 0 ||
    lien_count > 0
  ) {
    return "motivated";
  }

  return "curious";
}

function deriveTemplatePrimaryCategory(property_item, owner_item, fallback = "Residential") {
  const property_class = getCategoryValue(property_item, "property-class", null);
  if (property_class) return property_class;

  const majority = clean(getCategoryValue(owner_item, "property-type-majority", null)).toUpperCase();
  if (majority === "VACANT LAND") return "Vacant";
  return fallback || "Residential";
}

function deriveTemplateSecondaryCategory(property_item, owner_item, fallback = null) {
  return fallback;
}

function deriveSequencePosition(stage, owner_touch_count) {
  const touch_number = Math.max(1, Number(owner_touch_count || 0) + 1);

  if (stage === "Offer") {
    if (touch_number <= 1) return "V1";
    if (touch_number === 2) return "V2";
    return "V3";
  }

  if (touch_number <= 1) return "1st Touch";
  if (touch_number === 2) return "2nd Touch";
  if (touch_number === 3) return "3rd Touch";
  if (touch_number === 4) return "4th Touch";
  return "Final";
}

async function safeGetItem(item_id) {
  if (!item_id) return null;
  try {
    return await getItem(item_id);
  } catch (error) {
    warn("queue.deferred_context_item_failed", {
      item_id,
      message: error?.message || "Unknown item fetch error",
    });
    return null;
  }
}

function buildDeferredQueueContext({
  phone_item,
  brain_item,
  master_owner_item,
  property_item,
  market_item,
  agent_item,
  touch_count,
}) {
  const recent_templates = loadRecentTemplates({
    brain_item,
    limit: 10,
  });

  return {
    found: true,
    ids: {
      brain_item_id: brain_item?.item_id ?? null,
      phone_item_id: phone_item?.item_id ?? null,
      master_owner_id: master_owner_item?.item_id ?? null,
      prospect_id: getFirstAppReferenceId(phone_item, "linked-contact", null),
      property_id: property_item?.item_id ?? null,
      market_id: market_item?.item_id ?? null,
      assigned_agent_id: agent_item?.item_id ?? null,
    },
    items: {
      phone_item,
      brain_item,
      master_owner_item,
      prospect_item: null,
      property_item,
      market_item,
      agent_item,
    },
    summary: deriveContextSummary({
      phone_item,
      brain_item,
      master_owner_item,
      property_item,
      agent_item,
      market_item,
      touch_count,
    }),
    recent: {
      touch_count,
      last_template_id: recent_templates.last_template_id,
      recently_used_template_ids: recent_templates.recent_template_ids,
    },
  };
}

async function resolveDeferredQueueMessage(queue_item, { queue_item_id, phone_item, brain_item } = {}) {
  const existing_message_text = clean(getTextValue(queue_item, QUEUE_FIELDS.message_text, ""));
  const existing_template_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.template, null);

  if (existing_message_text) {
    return {
      ok: true,
      deferred: false,
      message_text: existing_message_text,
      template_id: existing_template_id,
      queue_item,
    };
  }

  const master_owner_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.master_owner, null);
  if (!master_owner_id) {
    return {
      ok: false,
      reason: "empty_message_body",
      details: {
        deferred_resolution_supported: false,
      },
    };
  }

  const touch_count = deriveOwnerTouchCountFromQueue(queue_item);
  const property_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.properties, null);
  const market_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.market, null);
  const sms_agent_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.sms_agent, null);

  info("queue.message_resolution_started", {
    queue_item_id,
    master_owner_id,
    touch_count,
  });

  const started_at = Date.now();

  const [master_owner_item, queued_property_item, queued_market_item, agent_item] = await Promise.all([
    safeGetItem(master_owner_id),
    safeGetItem(property_id),
    safeGetItem(market_id),
    safeGetItem(sms_agent_id),
  ]);
  let property_item =
    queued_property_item ||
    // Also check the brain item's properties relation before falling back to a lookup
    (brain_item ? await safeGetItem(getFirstAppReferenceId(brain_item, "properties", null)) : null);

  if (!property_item?.item_id && master_owner_id) {
    try {
      const response = await findPropertyItems({ "linked-master-owner": master_owner_id }, 1, 0);
      property_item = response?.items?.[0] ?? response?.[0] ?? null;
    } catch (_error) {
      property_item = null;
    }
  }

  if (!property_item?.item_id) {
    warn("queue.deferred_message_property_not_found", {
      queue_item_id,
      master_owner_id,
      queued_property_id: property_id || null,
      message: "No property item resolved for deferred message — property_address may be missing",
    });
  }

  const derived_market_id =
    queued_market_item?.item_id ??
    getFirstAppReferenceId(property_item, "market-2", null) ??
    getFirstAppReferenceId(property_item, "market", null) ??
    null;
  const market_item =
    queued_market_item?.item_id && String(queued_market_item.item_id) === String(derived_market_id || "")
      ? queued_market_item
      : await safeGetItem(derived_market_id);

  const context = buildDeferredQueueContext({
    phone_item,
    brain_item,
    master_owner_item,
    property_item,
    market_item,
    agent_item,
    touch_count,
  });

  const stage_hint = master_owner_item
    ? deriveOwnerStageHint(master_owner_item)
    : context.summary?.conversation_stage || "Ownership";
  const language = normalizeLanguage(
    getCategoryValue(master_owner_item, "language-primary", null) ||
      context.summary?.language_preference ||
      "English"
  );
  const classification = {
    message: "",
    language,
    objection: null,
    emotion: master_owner_item ? deriveOwnerEmotion(master_owner_item) : "curious",
    stage_hint,
    compliance_flag: null,
    positive_signals: [],
    confidence: 1,
    motivation_score:
      getNumberValue(master_owner_item, "urgency-score", null) ??
      getNumberValue(master_owner_item, "financial-pressure-score", null) ??
      null,
    source: "queue_processing",
    notes: "deferred_template_resolution",
    phone_activity_status: context.summary?.phone_activity_status || "Unknown",
  };

  const route = resolveRoute({
    classification,
    brain_item,
    phone_item,
    message: "",
  });

  const primary_category = deriveTemplatePrimaryCategory(
    property_item,
    master_owner_item,
    route?.primary_category || "Residential"
  );
  const secondary_category = deriveTemplateSecondaryCategory(
    property_item,
    master_owner_item,
    route?.secondary_category || null
  );
  const sequence_position = deriveSequencePosition(route?.stage || stage_hint, touch_count);
  const message_variant_seed = clean(
    getCategoryValue(master_owner_item, "message-variant-seed", null)
  );
  const rotation_key = [
    master_owner_id || "no-owner",
    phone_item?.item_id || "no-phone",
    property_item?.item_id || "no-property",
    route?.use_case || "no-use-case",
    route?.lifecycle_stage || route?.stage || stage_hint || "no-stage",
    message_variant_seed || "no-seed",
  ].join(":");

  const selected_template = await loadTemplate({
    category: primary_category,
    secondary_category,
    use_case: route?.use_case || "ownership_check",
    variant_group: route?.variant_group || "Stage 1 — Ownership Confirmation",
    tone: route?.tone || "Warm",
    gender_variant: "Neutral",
    language,
    sequence_position,
    paired_with_agent_type:
      route?.template_filters?.paired_with_agent_type || route?.persona || "Warm Professional",
    recently_used_template_ids: context?.recent?.recently_used_template_ids || [],
    rotation_key,
    fallback_agent_type:
      route?.template_filters?.fallback_agent_type || "Warm Professional",
  });

  if (!selected_template?.item_id) {
    return {
      ok: false,
      reason: "template_not_found",
      details: {
        route: {
          stage: route?.stage || stage_hint,
          lifecycle_stage: route?.lifecycle_stage || null,
          use_case: route?.use_case || "ownership_check",
          variant_group: route?.variant_group || "Stage 1 — Ownership Confirmation",
          language,
          tone: route?.tone || "Warm",
          sequence_position,
          category: primary_category,
          secondary_category,
        },
      },
    };
  }

  const render_result = renderTemplate({
    template_text: selected_template.text,
    context,
    overrides: {
      language,
      conversation_stage: route?.stage || stage_hint,
      lifecycle_stage: route?.lifecycle_stage || null,
      ai_route: route?.brain_ai_route || context.summary?.brain_ai_route || null,
    },
  });

  if (render_result?.missing_required_placeholders?.length || render_result?.invalid_placeholders?.length) {
    warn("queue.deferred_message_placeholder_validation_failed", {
      queue_item_id,
      template_id: selected_template.item_id,
      property_item_id: property_item?.item_id || null,
      missing_required_placeholders: render_result.missing_required_placeholders || [],
      invalid_placeholders: render_result.invalid_placeholders || [],
    });
    return {
      ok: false,
      reason: "template_placeholder_validation_failed",
      details: {
        template_id: selected_template.item_id,
        missing_required_placeholders: render_result.missing_required_placeholders || [],
        invalid_placeholders: render_result.invalid_placeholders || [],
      },
    };
  }

  // Use normalizeForQueueText (not clean) to collapse embedded newlines into
  // spaces before writing to the Send Queue message-text field.  That field is
  // a single-line Podio text field: passing a multiline string causes Podio to
  // store only the first line, truncating the message to "Hi" / "Hola".
  const rendered_message_text = normalizeForQueueText(render_result?.rendered_text || "");
  if (!rendered_message_text) {
    return {
      ok: false,
      reason: "rendered_message_empty",
      details: {
        template_id: selected_template.item_id,
      },
    };
  }

  const queue_resolution_payload = {
    [QUEUE_FIELDS.template]: [selected_template.item_id],
    [QUEUE_FIELDS.message_text]: rendered_message_text,
    [QUEUE_FIELDS.character_count]: rendered_message_text.length,
    ...(render_result.used_placeholders?.length
      ? { [QUEUE_FIELDS.personalization_tags_used]: render_result.used_placeholders }
      : {}),
  };

  try {
    await updateItem(queue_item_id, queue_resolution_payload);
  } catch (error) {
    logRevisionLimitPhase(queue_item_id, "resolve_deferred_message", "template_write", error);
    if (!shouldRetryQueueUpdateWithoutTemplate(error)) {
      throw error;
    }

    delete queue_resolution_payload[QUEUE_FIELDS.template];
    try {
      await updateItem(queue_item_id, queue_resolution_payload);
    } catch (retryError) {
      logRevisionLimitPhase(queue_item_id, "resolve_deferred_message", "template_write_retry", retryError);
      throw retryError;
    }
  }

  const refreshed_queue_item = await getItem(queue_item_id);

  info("queue.message_resolution_completed", {
    queue_item_id,
    template_id: selected_template.item_id,
    duration_ms: Date.now() - started_at,
    character_count: rendered_message_text.length,
  });

  return {
    ok: true,
    deferred: true,
    template_id: selected_template.item_id,
    template_item: selected_template.raw || null,
    canonical_flow: deriveCanonicalSellerFlowFromTemplate(selected_template),
    message_text: rendered_message_text,
    queue_item: refreshed_queue_item || queue_item,
    route,
  };
}

async function resolveCanonicalSellerFlowForQueue({
  template_id = null,
  template_item = null,
  message_body = "",
} = {}) {
  let candidate_template = template_item || null;

  if (!candidate_template && template_id) {
    candidate_template = await safeGetItem(template_id);
  }

  if (candidate_template) {
    const normalized_template =
      candidate_template.raw && candidate_template.text
        ? candidate_template
        : normalizeTemplateItem(candidate_template);
    const derived = deriveCanonicalSellerFlowFromTemplate(normalized_template);
    if (derived) return derived;

    if (
      clean(normalized_template?.use_case) ||
      clean(normalized_template?.variant_group)
    ) {
      return {
        selected_use_case: clean(normalized_template?.use_case) || null,
        template_use_case: clean(normalized_template?.use_case) || null,
        next_expected_stage: canonicalStageForUseCase(
          clean(normalized_template?.use_case) || null
        ),
        selected_variant_group: clean(normalized_template?.variant_group) || null,
        selected_tone: normalizeSellerFlowTone(normalized_template?.tone),
      };
    }
  }

  const inferred_use_case = inferCanonicalUseCaseFromOutboundText(message_body);
  if (!inferred_use_case) return null;

  return {
    selected_use_case: inferred_use_case,
    template_use_case: null,
    next_expected_stage: canonicalStageForUseCase(inferred_use_case),
    selected_variant_group: null,
    selected_tone: null,
  };
}

function extractItemRevision(item = null) {
  const revision =
    item?.current_revision?.revision ??
    item?.revision ??
    null;

  return Number.isFinite(Number(revision)) ? Number(revision) : null;
}

function mapFailureReasonToQueueCategory(send_result, fallback_bucket = null) {
  const bucket = fallback_bucket || mapTextgridFailureBucket(send_result) || "Other";

  if (bucket === "DNC") return "Opt-Out";
  if (bucket === "Hard Bounce") return "Invalid Number";
  if (bucket === "Soft Bounce") return "Network Error";
  if (bucket === "Spam") return "Carrier Block";

  const msg = String(send_result?.error_message || "").toLowerCase();
  if (msg.includes("daily") && msg.includes("limit")) return "Daily Limit Hit";

  return "Network Error";
}

async function resolveBrainForQueue({ master_owner_id, prospect_id }) {
  if (master_owner_id) {
    const hit = await getFirstMatchingItem(
      APP_IDS.ai_conversation_brain,
      { "master-owner": master_owner_id },
      { sort_desc: true }
    );
    if (hit) return hit;
  }

  if (prospect_id) {
    const hit = await getFirstMatchingItem(
      APP_IDS.ai_conversation_brain,
      { prospect: prospect_id },
      { sort_desc: true }
    );
    if (hit) return hit;
  }

  return null;
}

async function resolveQueuePropertyAndMarket({
  property_id = null,
  market_id = null,
  master_owner_id = null,
  brain_item = null,
} = {}) {
  const queued_property_item = await safeGetItem(property_id);
  let property_item =
    queued_property_item ||
    (await safeGetItem(getFirstAppReferenceId(brain_item, "properties", null)));

  if (!property_item?.item_id && master_owner_id) {
    try {
      const response = await findPropertyItems({ "linked-master-owner": master_owner_id }, 1, 0);
      property_item = response?.items?.[0] ?? response?.[0] ?? null;
    } catch (_error) {
      property_item = null;
    }
  }

  const queued_market_item = await safeGetItem(market_id);
  const derived_market_id =
    queued_market_item?.item_id ??
    getFirstAppReferenceId(property_item, "market-2", null) ??
    getFirstAppReferenceId(property_item, "market", null) ??
    null;

  const market_item =
    queued_market_item?.item_id && String(queued_market_item.item_id) === String(derived_market_id || "")
      ? queued_market_item
      : await safeGetItem(derived_market_id);

  return {
    property_item,
    market_item,
    property_id: property_item?.item_id ?? property_id ?? null,
    market_id: market_item?.item_id ?? derived_market_id ?? market_id ?? null,
  };
}

export function validateQueuedOutboundNumberItem(
  outbound_number_item = null,
  now = new Date()
) {
  const status = getCategoryValue(
    outbound_number_item,
    TEXTGRID_NUMBER_FIELDS.status,
    null
  );
  const hard_pause = getCategoryValue(
    outbound_number_item,
    TEXTGRID_NUMBER_FIELDS.hard_pause,
    null
  );
  const pause_until = getDateValue(
    outbound_number_item,
    TEXTGRID_NUMBER_FIELDS.pause_until,
    null
  );
  const normalized_from = normalizePhone(
    getPhoneValue(outbound_number_item, "phone-number", "") ||
      getTextValue(outbound_number_item, TEXTGRID_NUMBER_FIELDS.title, "")
  );

  if (!outbound_number_item?.item_id) {
    return {
      ok: false,
      reason: "outbound_number_item_missing",
      normalized_from,
      status,
      hard_pause,
      pause_until,
    };
  }

  if (!normalized_from) {
    return {
      ok: false,
      reason: "outbound_number_phone_invalid",
      normalized_from,
      status,
      hard_pause,
      pause_until,
    };
  }

  if (status && isNegativeCategory(status)) {
    return {
      ok: false,
      reason: `outbound_number_inactive:${lower(status)}`,
      normalized_from,
      status,
      hard_pause,
      pause_until,
    };
  }

  if (hard_pause && isPositiveCategory(hard_pause)) {
    return {
      ok: false,
      reason: "outbound_number_hard_paused",
      normalized_from,
      status,
      hard_pause,
      pause_until,
    };
  }

  if (pause_until) {
    const pause_until_ts = new Date(pause_until).getTime();
    if (!Number.isNaN(pause_until_ts) && pause_until_ts > now.getTime()) {
      return {
        ok: false,
        reason: "outbound_number_paused_until",
        normalized_from,
        status,
        hard_pause,
        pause_until,
      };
    }
  }

  return {
    ok: true,
    reason: null,
    normalized_from,
    status,
    hard_pause,
    pause_until,
  };
}

export function buildFailedOutboundMessageEventFields({
  brain_item = null,
  conversation_item_id = null,
  queue_item_id,
  master_owner_id,
  prospect_id,
  property_id,
  market_id,
  phone_item_id,
  outbound_number_item_id,
  template_id,
  message_body,
  message_variant = null,
  latency_ms = null,
  selected_use_case = null,
  template_use_case = null,
  next_expected_stage = null,
  selected_variant_group = null,
  selected_tone = null,
  send_result,
  retry_count,
  max_retries,
  client_reference_id,
}) {
  const ai_route = getCategoryValue(brain_item, "ai-route", null);
  const resolved_message_id = send_result.message_id || client_reference_id || null;
  const missing_relation_warnings = [];

  if (phone_item_id && !asArrayAppRef(phone_item_id)) {
    missing_relation_warnings.push("phone_relation_invalid");
  }
  if (property_id && !asArrayAppRef(property_id)) {
    missing_relation_warnings.push("property_relation_invalid");
  }
  if (template_id && !asArrayAppRef(template_id)) {
    missing_relation_warnings.push("template_relation_invalid");
  }
  if (conversation_item_id && !asArrayAppRef(conversation_item_id)) {
    missing_relation_warnings.push("conversation_relation_invalid");
  }

  if (missing_relation_warnings.length) {
    warn("events.failed_outbound_relation_payload_incomplete", {
      queue_item_id,
      master_owner_id,
      prospect_id,
      property_id,
      market_id,
      phone_item_id,
      outbound_number_item_id,
      conversation_item_id,
      template_id,
      warnings: missing_relation_warnings,
    });
  }

  return {
    [EVENT_FIELDS.message_id]: resolved_message_id,
    [EVENT_FIELDS.direction]: "Outbound",
    [EVENT_FIELDS.timestamp]: { start: nowIso() },
    [EVENT_FIELDS.message]: message_body,
    [EVENT_FIELDS.character_count]: String(message_body || "").length,
    [EVENT_FIELDS.delivery_status]: "Failed",
    [EVENT_FIELDS.raw_carrier_status]: String(send_result.error_status || send_result.status || ""),
    [EVENT_FIELDS.failure_bucket]: mapTextgridFailureBucket(send_result) || "Other",
    [EVENT_FIELDS.is_final_failure]: retry_count + 1 >= max_retries ? "Yes" : "No",
    [EVENT_FIELDS.processed_by]: "Scheduled Campaign",
    [EVENT_FIELDS.source_app]: "Send Queue",
    [EVENT_FIELDS.trigger_name]:
      queue_item_id ? buildQueueSendFailedTriggerName(queue_item_id) : "queue-send-failed",
    [EVENT_FIELDS.ai_output]: serializeMessageEventMetadata(
      buildQueueMessageEventMetadata({
        queue_item_id,
        client_reference_id,
        provider_message_id: send_result.message_id,
        event_kind: "outbound_send_failed",
        message_variant,
        master_owner_id,
        prospect_id,
        property_id,
        market_id,
        phone_item_id,
        outbound_number_item_id,
        conversation_item_id,
        template_id,
        selected_use_case: clean(selected_use_case) || null,
        template_use_case: clean(template_use_case) || null,
        next_expected_stage: clean(next_expected_stage) || null,
        selected_variant_group: clean(selected_variant_group) || null,
        selected_tone: clean(selected_tone) || null,
      })
    ),
    ...(message_variant !== null && message_variant !== undefined
      ? { [EVENT_FIELDS.message_variant]: Number(message_variant) || 0 }
      : {}),
    ...(asArrayAppRef(master_owner_id)
      ? { [EVENT_FIELDS.master_owner]: asArrayAppRef(master_owner_id) }
      : {}),
    ...(asArrayAppRef(prospect_id)
      ? { [EVENT_FIELDS.prospect]: asArrayAppRef(prospect_id) }
      : {}),
    ...(asArrayAppRef(property_id)
      ? { [EVENT_FIELDS.property]: asArrayAppRef(property_id) }
      : {}),
    ...(asArrayAppRef(market_id)
      ? { [EVENT_FIELDS.market]: asArrayAppRef(market_id) }
      : {}),
    ...(asArrayAppRef(phone_item_id)
      ? { [EVENT_FIELDS.phone_number]: asArrayAppRef(phone_item_id) }
      : {}),
    ...(asArrayAppRef(outbound_number_item_id)
      ? { [EVENT_FIELDS.textgrid_number]: asArrayAppRef(outbound_number_item_id) }
      : {}),
    ...(asArrayAppRef(conversation_item_id)
      ? { [EVENT_FIELDS.conversation]: asArrayAppRef(conversation_item_id) }
      : {}),
    ...(asArrayAppRef(template_id)
      ? { [EVENT_FIELDS.template_selected]: asArrayAppRef(template_id) }
      : {}),
    ...(latency_ms !== null && latency_ms !== undefined
      ? { [EVENT_FIELDS.latency_ms]: Number(latency_ms) || 0 }
      : {}),
    ...(ai_route ? { [EVENT_FIELDS.ai_route]: ai_route } : {}),
  };
}

export async function logFailedOutboundMessageEvent(payload = {}) {
  return createMessageEvent(buildFailedOutboundMessageEventFields(payload));
}

async function failQueueItem(
  queue_item_id,
  {
    queue_status = "Failed",
    failed_reason = "Network Error",
    retry_count = null,
    delivery_confirmed = "❌ Failed",
    _phase = "status_update",
  } = {}
) {
  const payload = {
    [QUEUE_FIELDS.queue_status]: queue_status,
    [QUEUE_FIELDS.failed_reason]: failed_reason,
    [QUEUE_FIELDS.delivery_confirmed]: delivery_confirmed,
  };

  if (typeof retry_count === "number") {
    payload[QUEUE_FIELDS.retry_count] = retry_count;
  }

  try {
    await updateItem(queue_item_id, payload);
  } catch (error) {
    logRevisionLimitPhase(queue_item_id, "fail_queue_item", _phase, error);
    throw error;
  }
}

async function claimQueueItemForSending(
  queue_item_id,
  queue_item,
  retry_count,
  extra_fields = {}
) {
  const revision = extractItemRevision(queue_item);
  const payload = {
    ...extra_fields,
    [QUEUE_FIELDS.queue_status]: "Sending",
    [QUEUE_FIELDS.retry_count]: retry_count + 1,
    [QUEUE_FIELDS.delivery_confirmed]: "⏳ Pending",
  };

  if (revision === null) {
    try {
      await updateItem(queue_item_id, payload);
    } catch (error) {
      logRevisionLimitPhase(queue_item_id, "claim_for_sending", "status_transition", error);
      throw error;
    }

    return {
      ok: true,
      optimistic: false,
      revision: null,
    };
  }

  try {
    await updateItem(queue_item_id, payload, revision);

    return {
      ok: true,
      optimistic: true,
      revision,
    };
  } catch (error) {
    if (error instanceof PodioError && error.status === 409) {
      return {
        ok: false,
        skipped: true,
        reason: "queue_item_claim_conflict",
      };
    }

    logRevisionLimitPhase(queue_item_id, "claim_for_sending", "optimistic_lock", error);
    throw error;
  }
}

export async function finalizeSuccessfulQueueSend({
  queue_item_id,
  phone_item,
  phone_item_id,
  brain_id = null,
  brain_item = null,
  conversation_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  market_id = null,
  outbound_number_item_id = null,
  template_id = null,
  message_body = "",
  message_variant = null,
  latency_ms = null,
  selected_use_case = null,
  template_use_case = null,
  next_expected_stage = null,
  selected_variant_group = null,
  selected_tone = null,
  send_result = {},
  current_total_messages_sent = 0,
  client_reference_id = null,
  now = nowIso(),
} = {}, deps = {}) {
  const update = deps.updateItem || updateItem;
  const logOutbound = deps.logOutboundMessageEvent || logOutboundMessageEvent;
  const updateBrain = deps.updateBrainAfterSend || updateBrainAfterSend;
  const updateMasterOwner =
    deps.updateMasterOwnerAfterSend || updateMasterOwnerAfterSend;

  const bookkeeping_errors = [];

  try {
    await update(queue_item_id, {
      [QUEUE_FIELDS.queue_status]: "Sent",
      [QUEUE_FIELDS.sent_at]: { start: now },
      [QUEUE_FIELDS.delivery_confirmed]: "⏳ Pending",
    });
  } catch (error) {
    bookkeeping_errors.push(
      `queue_sent_update_failed:${error?.message || "unknown_error"}`
    );
  }

  try {
    await logOutbound({
      brain_item,
      conversation_item_id: conversation_item_id || brain_id,
      master_owner_id,
      prospect_id,
      property_id,
      market_id,
      phone_item_id,
      outbound_number_item_id,
      message_body,
      provider_message_id: send_result.message_id,
      queue_item_id,
      client_reference_id,
      template_id,
      message_variant,
      latency_ms,
      send_result,
      ...(selected_use_case ? { selected_use_case } : {}),
      ...(template_use_case ? { template_use_case } : {}),
      ...(next_expected_stage ? { next_expected_stage } : {}),
      ...(selected_variant_group ? { selected_variant_group } : {}),
      ...(selected_tone ? { selected_tone } : {}),
    });
  } catch (error) {
    bookkeeping_errors.push(
      `outbound_event_log_failed:${error?.message || "unknown_error"}`
    );
  }

  try {
    await updateBrain({
      brain_id,
      phone_item_id,
      message_body,
      template_id,
      current_total_messages_sent,
    });
  } catch (error) {
    bookkeeping_errors.push(
      `brain_update_after_send_failed:${error?.message || "unknown_error"}`
    );
  }

  try {
    if (master_owner_id) {
      await updateMasterOwner({
        master_owner_id,
        sent_at: now,
        selected_use_case,
      });
    }
  } catch (error) {
    bookkeeping_errors.push(
      `master_owner_update_after_send_failed:${error?.message || "unknown_error"}`
    );
  }

  return {
    ok: bookkeeping_errors.length === 0,
    sent: true,
    partial: bookkeeping_errors.length > 0,
    queue_item_id,
    provider_message_id: send_result.message_id,
    to: send_result.to,
    from: send_result.from,
    brain_id,
    bookkeeping_errors,
    phone_item_id: phone_item?.item_id || phone_item_id || null,
  };
}

export async function processSendQueueItem(queue_item_id) {
  info("queue.process_started", { queue_item_id });

  let queue_item = await getItem(queue_item_id);
  if (!queue_item) {
    throw new Error(`Queue item not found: ${queue_item_id}`);
  }

  const initial_phone_item_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.phone_number, null);
  const initial_master_owner_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.master_owner, null);
  const initial_prospect_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.prospects, null);

  const [initial_phone_item, initial_brain_item] = await Promise.all([
    initial_phone_item_id ? safeGetItem(initial_phone_item_id) : Promise.resolve(null),
    resolveBrainForQueue({
      master_owner_id: initial_master_owner_id,
      prospect_id: initial_prospect_id,
    }),
  ]);

  const message_resolution = await resolveDeferredQueueMessage(queue_item, {
    queue_item_id,
    phone_item: initial_phone_item,
    brain_item: initial_brain_item,
  });

  if (!message_resolution.ok) {
    warn("queue.process_message_resolution_failed", {
      queue_item_id,
      reason: message_resolution.reason,
      details: message_resolution.details || null,
    });

    await failQueueItem(queue_item_id, {
      queue_status: "Blocked",
      failed_reason: "Network Error",
      _phase: "message_resolution_failed",
    });

    return {
      ok: false,
      reason: message_resolution.reason,
      details: message_resolution.details || null,
    };
  }

  if (message_resolution.queue_item?.item_id) {
    queue_item = message_resolution.queue_item;
  }

  const queue_validation = validateSendQueueItem(queue_item);

  if (!queue_validation.ok) {
    if (queue_validation.skipped) {
      info("queue.process_skipped_terminal_status", {
        queue_item_id,
        queue_status: queue_validation.queue_status,
      });

      return {
        ok: true,
        skipped: true,
        reason: queue_validation.reason,
      };
    }

    if (queue_validation.reason === "missing_phone_item") {
      await failQueueItem(queue_item_id, {
        failed_reason: "Invalid Number",
        retry_count: (queue_validation.retry_count ?? 0) + 1,
      });
    } else if (queue_validation.reason === "missing_textgrid_number") {
      await failQueueItem(queue_item_id, {
        failed_reason: "Network Error",
        retry_count: (queue_validation.retry_count ?? 0) + 1,
      });
    } else if (queue_validation.reason === "empty_message_body") {
      await failQueueItem(queue_item_id, {
        failed_reason: "Network Error",
        retry_count: (queue_validation.retry_count ?? 0) + 1,
      });
    } else if (queue_validation.reason === "junk_message_body") {
      // One-word / too-short body — truncation artifact from multiline template stored
      // in a single-line Podio field.  Block permanently; do NOT advance owner state.
      warn("queue.process_blocked_junk_message_body", {
        queue_item_id,
        word_count: queue_validation.word_count,
        message_body: queue_validation.message_body,
      });
      await failQueueItem(queue_item_id, {
        queue_status: "Blocked",
        failed_reason: "Content Filter",
      });
    } else if (queue_validation.reason === "max_retries_exceeded") {
      await failQueueItem(queue_item_id, {
        failed_reason: "Network Error",
      });
    }

    return {
      ok: false,
      reason: queue_validation.reason,
    };
  }

  const phone_item_id = queue_validation.phone_item_id;
  const outbound_number_item_id = queue_validation.textgrid_number_item_id;
  const message_body = queue_validation.message_text;
  const retry_count = queue_validation.retry_count;
  const max_retries = queue_validation.max_retries;

  const template_id =
    message_resolution.template_id ||
    getFirstAppReferenceId(queue_item, QUEUE_FIELDS.template, null);
  const master_owner_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.master_owner, null);
  const prospect_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.prospects, null);
  const property_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.properties, null);
  const market_id = getFirstAppReferenceId(queue_item, QUEUE_FIELDS.market, null);
  const message_variant = getTouchNumber(queue_item);
  const canonical_flow =
    message_resolution.canonical_flow ||
    (await resolveCanonicalSellerFlowForQueue({
      template_id,
      template_item: message_resolution.template_item || null,
      message_body,
    }));

  const [phone_item, outbound_number_item, brain_item] = await Promise.all([
    initial_phone_item && String(initial_phone_item?.item_id || "") === String(phone_item_id)
      ? Promise.resolve(initial_phone_item)
      : safeGetItem(phone_item_id),
    safeGetItem(outbound_number_item_id),
    initial_brain_item
      ? Promise.resolve(initial_brain_item)
      : resolveBrainForQueue({ master_owner_id, prospect_id }),
  ]);

  const brain_id = brain_item?.item_id ?? null;
  const { property_id: resolved_property_id, market_id: resolved_market_id } =
    await resolveQueuePropertyAndMarket({
      property_id,
      market_id,
      master_owner_id,
      brain_item,
    });

  const phone_validation = validateActivePhone(phone_item);

  if (!phone_validation.ok) {
    warn("queue.process_blocked_phone_not_active", {
      queue_item_id,
      phone_item_id,
      activity_status: phone_validation.activity_status,
      reason: phone_validation.reason,
    });

    await failQueueItem(queue_item_id, {
      queue_status: "Blocked",
      failed_reason: "Invalid Number",
      retry_count: retry_count + 1,
    });

    return {
      ok: false,
      reason: phone_validation.reason,
    };
  }

  const suppression = shouldSuppressOutreach({
    phone_item,
    brain_item,
  });

  if (suppression.suppress) {
    warn("queue.process_suppressed", {
      queue_item_id,
      phone_item_id,
      reason: suppression.reason,
      details: suppression.details,
    });

    await failQueueItem(queue_item_id, {
      queue_status: "Blocked",
      failed_reason:
        suppression.reason === "phone_post_contact_suppression" ||
        suppression.reason === "classification_stop_texting"
          ? "Opt-Out"
          : "Network Error",
    });

    return {
      ok: false,
      suppressed: true,
      reason: suppression.reason,
      details: suppression.details,
    };
  }

  const to_number =
    getTextValue(phone_item, "canonical-e164", "") ||
    normalizePhone(getTextValue(phone_item, "phone-hidden", ""));
  const outbound_number_validation = validateQueuedOutboundNumberItem(
    outbound_number_item
  );
  const from_number = outbound_number_validation.normalized_from || "";
  const client_reference_id = buildQueueClientReferenceId(queue_item_id);

  info("queue.process_send_preflight", {
    queue_item_id,
    phone_item_id,
    outbound_number_item_id,
    property_id: resolved_property_id,
    template_id,
    normalized_to: clean(to_number) || null,
    normalized_from: clean(from_number) || null,
    send_endpoint: getTextgridSendEndpoint(),
    outbound_number_found: Boolean(outbound_number_item?.item_id),
    outbound_number_valid: outbound_number_validation.ok,
    outbound_number_status: outbound_number_validation.status || null,
    outbound_number_hard_pause: outbound_number_validation.hard_pause || null,
    outbound_number_pause_until: outbound_number_validation.pause_until || null,
  });

  if (!to_number || !outbound_number_validation.ok) {
    const preflight_reason = !to_number
      ? "destination_number_invalid"
      : outbound_number_validation.reason;
    const preflight_message = !to_number
      ? `Invalid destination number for phone item ${phone_item_id}`
      : `Invalid sending number for TextGrid item ${outbound_number_item_id}: ${outbound_number_validation.reason}`;

    await failQueueItem(queue_item_id, {
      queue_status: "Blocked",
      failed_reason: "Invalid Number",
      retry_count: retry_count + 1,
    });

    try {
      await logFailedOutboundMessageEvent({
        brain_item,
        conversation_item_id: brain_id,
        queue_item_id,
        master_owner_id,
        prospect_id,
        property_id: resolved_property_id,
        market_id: resolved_market_id,
        phone_item_id,
        outbound_number_item_id,
        template_id,
        message_body,
        message_variant,
        latency_ms: 0,
        selected_use_case: canonical_flow?.selected_use_case || null,
        template_use_case: canonical_flow?.template_use_case || null,
        next_expected_stage: canonical_flow?.next_expected_stage || null,
        selected_variant_group: canonical_flow?.selected_variant_group || null,
        selected_tone: canonical_flow?.selected_tone || null,
        send_result: {
          ok: false,
          provider: "textgrid",
          message_id: null,
          status: "failed",
          error_status: "preflight_invalid_number",
          error_message: preflight_message,
          to: to_number || null,
          from: from_number || null,
          endpoint: getTextgridSendEndpoint(),
        },
        retry_count,
        max_retries,
        client_reference_id,
      });
    } catch (error) {
      warn("queue.process_preflight_failed_event_log_failed", {
        queue_item_id,
        reason: preflight_reason,
        message: error?.message || "unknown_error",
      });
    }

    return {
      ok: false,
      reason: preflight_reason,
    };
  }

  const queue_context_updates = {
    ...(!property_id && resolved_property_id
      ? { [QUEUE_FIELDS.properties]: [resolved_property_id] }
      : {}),
    ...(!market_id && resolved_market_id
      ? { [QUEUE_FIELDS.market]: [resolved_market_id] }
      : {}),
  };

  const claim = await claimQueueItemForSending(
    queue_item_id,
    queue_item,
    retry_count,
    queue_context_updates
  );

  if (!claim.ok) {
    info("queue.process_skipped_claim_conflict", {
      queue_item_id,
      reason: claim.reason,
    });

    return {
      ok: true,
      skipped: true,
      reason: claim.reason,
    };
  }

  const send_started_at = Date.now();
  const send_result = await sendTextgridSMS({
    to: to_number,
    from: from_number,
    body: message_body,
    message_type: "sms",
    client_reference_id,
  });
  const latency_ms = Date.now() - send_started_at;

  if (!send_result.ok) {
    const failed_reason = mapFailureReasonToQueueCategory(send_result);
    const bookkeeping_errors = [];

    warn("queue.process_send_failed", {
      queue_item_id,
      phone_item_id,
      outbound_number_item_id,
      error_status: send_result.error_status,
      error_message: send_result.error_message,
      failed_reason,
      normalized_to: send_result.to || to_number || null,
      normalized_from: send_result.from || from_number || null,
      send_endpoint: send_result.endpoint || getTextgridSendEndpoint(),
      outbound_number_valid: outbound_number_validation.ok,
      outbound_number_status: outbound_number_validation.status || null,
    });

    try {
      await failQueueItem(queue_item_id, {
        failed_reason,
      });
    } catch (error) {
      bookkeeping_errors.push(
        `queue_fail_update_failed:${error?.message || "unknown_error"}`
      );
    }

    try {
      await logFailedOutboundMessageEvent({
        brain_item,
        conversation_item_id: brain_id,
        queue_item_id,
        master_owner_id,
        prospect_id,
        property_id: resolved_property_id,
        market_id: resolved_market_id,
        phone_item_id,
        outbound_number_item_id,
        template_id,
        message_body,
        message_variant,
        latency_ms,
        selected_use_case: canonical_flow?.selected_use_case || null,
        template_use_case: canonical_flow?.template_use_case || null,
        next_expected_stage: canonical_flow?.next_expected_stage || null,
        selected_variant_group: canonical_flow?.selected_variant_group || null,
        selected_tone: canonical_flow?.selected_tone || null,
        send_result,
        retry_count,
        max_retries,
        client_reference_id,
      });
    } catch (error) {
      bookkeeping_errors.push(
        `failed_event_log_failed:${error?.message || "unknown_error"}`
      );
    }

    return {
      ok: false,
      sent: false,
      reason: send_result.error_message || "textgrid_send_failed",
      failed_reason,
      bookkeeping_errors,
    };
  }

  const current_total_messages_sent = Number(
    getNumberValue(phone_item, "total-messages-sent", 0) || 0
  );

  const bookkeeping_result = await finalizeSuccessfulQueueSend({
    queue_item_id,
    phone_item,
    phone_item_id,
    brain_id,
    brain_item,
    conversation_item_id: brain_id,
    master_owner_id,
    prospect_id,
    property_id: resolved_property_id,
    market_id: resolved_market_id,
    outbound_number_item_id,
    template_id,
    message_body,
    message_variant,
    latency_ms,
    selected_use_case: canonical_flow?.selected_use_case || null,
    template_use_case: canonical_flow?.template_use_case || null,
    next_expected_stage: canonical_flow?.next_expected_stage || null,
    selected_variant_group: canonical_flow?.selected_variant_group || null,
    selected_tone: canonical_flow?.selected_tone || null,
    send_result,
    current_total_messages_sent,
    client_reference_id,
  });

  info("queue.process_completed", {
    queue_item_id,
    phone_item_id,
    outbound_number_item_id,
    provider_message_id: send_result.message_id,
    brain_id,
    optimistic_claim: claim.optimistic,
    bookkeeping_error_count: bookkeeping_result.bookkeeping_errors.length,
  });

  return {
    ...bookkeeping_result,
  };
}

export async function processSendQueue(input = {}) {
  const queue_item_id =
    typeof input === "number"
      ? input
      : input?.queue_item_id ?? null;

  if (!queue_item_id) {
    return {
      ok: false,
      reason: "missing_queue_item_id",
    };
  }

  return processSendQueueItem(queue_item_id);
}

export default processSendQueueItem;
