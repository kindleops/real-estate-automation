// ─── load-context.js ─────────────────────────────────────────────────────
import { getCategoryValue, getFirstAppReferenceId, getItem, getNumberValue, normalizeUsPhone10, getTextValue } from "@/lib/providers/podio.js";
import { findPhoneRecord } from "@/lib/podio/apps/phone-numbers.js";

import { child, info, warn } from "@/lib/logging/logger.js";
import { resolveBrain, createBrain } from "@/lib/domain/context/resolve-brain.js";
import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";
import { derivePhoneDisqualification } from "@/lib/domain/context/phone-disqualification.js";
import { loadRecentEvents } from "@/lib/domain/context/load-recent-events.js";
import { loadRecentTemplates } from "@/lib/domain/context/load-recent-templates.js";
import { findPropertyItems } from "@/lib/podio/apps/properties.js";

const CONTEXT_LOAD_TIMEOUT_MS = 30_000;

function withTimeout(promise, ms, label) {
  let timer;

  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`[context] timeout: ${label} exceeded ${ms}ms`));
    }, ms);
  });

  return Promise.race([promise, timeout]).finally(() => {
    clearTimeout(timer);
  });
}

async function safeGetItem(item_id, label) {
  if (!item_id) return null;

  try {
    return await getItem(item_id);
  } catch (err) {
    warn("context.related_item_load_failed", {
      label,
      item_id,
      message: err?.message,
    });
    return null;
  }
}

function deriveAssignedAgentId(brain_item) {
  return (
    getFirstAppReferenceId(brain_item, "sms-agent", null) ??
    getFirstAppReferenceId(brain_item, "ai-agent-assigned", null) ??
    null
  );
}

export async function loadContext({
  inbound_from,
  create_brain_if_missing = true,
} = {}) {
  const normalized_phone = normalizeUsPhone10(inbound_from);

  if (!normalized_phone) {
    throw new Error("loadContext: inbound_from missing or invalid");
  }

  info("context.load_started", {
    inbound_from: normalized_phone,
    create_brain_if_missing,
  });

  return withTimeout(
    _loadContextInner({
      normalized_phone,
      create_brain_if_missing,
    }),
    CONTEXT_LOAD_TIMEOUT_MS,
    "loadContext"
  );
}

async function _loadContextInner({
  normalized_phone,
  create_brain_if_missing,
}) {
  const phone_item = await findPhoneRecord(normalized_phone);

  if (!phone_item) {
    warn("context.phone_not_found", {
      inbound_from: normalized_phone,
    });

    return {
      found: false,
      reason: "phone_not_found",
      inbound_from: normalized_phone,
    };
  }

  const phone_item_id = phone_item.item_id;
  const master_owner_id = getFirstAppReferenceId(phone_item, "linked-master-owner", null);
  const owner_id = getFirstAppReferenceId(phone_item, "linked-owner", null);
  const prospect_id = getFirstAppReferenceId(phone_item, "linked-contact", null);
  let property_id = getFirstAppReferenceId(phone_item, "primary-property", null);

  if (!property_id && master_owner_id) {
    try {
      const response = await findPropertyItems({ "master-owner": master_owner_id }, 1, 0);
      property_id = response?.items?.[0]?.item_id ?? response?.[0]?.item_id ?? null;
    } catch (_error) {
      property_id = null;
    }
  }

  const log = child({
    inbound_from: normalized_phone,
    phone_item_id,
    master_owner_id,
    owner_id,
    prospect_id,
    property_id,
  });

  const disqualification = derivePhoneDisqualification(phone_item);

  if (disqualification) {
    log.warn("context.phone_disqualified", {
      reason: disqualification,
      dnc: getCategoryValue(phone_item, "do-not-call", "FALSE"),
      activity_status: getCategoryValue(phone_item, "phone-activity-status", "Unknown"),
    });

    return {
      found: false,
      reason: disqualification,
      inbound_from: normalized_phone,
      phone_item_id,
      phone_item,
    };
  }

  log.info("context.phone_resolved", {
    activity_status: getCategoryValue(phone_item, "phone-activity-status", "Unknown"),
    engagement_tier: getCategoryValue(phone_item, "engagement-tier", null),
    do_not_call: getCategoryValue(phone_item, "do-not-call", "FALSE"),
    dnc_source: getCategoryValue(phone_item, "dnc-source", null),
  });

  let brain_item = await resolveBrain({
    phone_item_id,
    prospect_id,
    master_owner_id,
  });

  if (!brain_item && create_brain_if_missing) {
    brain_item = await createBrain({
      master_owner_id,
      prospect_id,
      property_id,
      phone_item_id,
      logger: log,
    });
  }

  const brain_item_id = brain_item?.item_id ?? null;
  const assigned_agent_id = deriveAssignedAgentId(brain_item);

  log.info("context.brain_resolved", {
    brain_item_id,
    assigned_agent_id,
  });

  const [
    master_owner_item,
    owner_item,
    prospect_item,
    property_item,
    agent_item,
  ] = await Promise.all([
    safeGetItem(master_owner_id, "master_owner"),
    safeGetItem(owner_id, "owner"),
    safeGetItem(prospect_id, "prospect"),
    safeGetItem(property_id, "property"),
    safeGetItem(assigned_agent_id, "agent"),
  ]);

  const market_id =
    getFirstAppReferenceId(property_item, "market-2", null) ??
    getFirstAppReferenceId(property_item, "market", null) ??
    null;

  const market_item = await safeGetItem(market_id, "market");

  const recent_template_context = loadRecentTemplates({
    brain_item,
    limit: 10,
  });

  const last_template_id = recent_template_context.last_template_id;
  const recently_used_ids = recent_template_context.recent_template_ids;

  const touch_count = Math.max(
    0,
    getNumberValue(phone_item, "total-messages-sent", 0) || 0
  );

  const recent_events_result = await loadRecentEvents({
    phone_item_id,
    master_owner_id,
    prospect_id,
    limit: 10,
  });

  const context_summary = deriveContextSummary({
    phone_item,
    brain_item,
    master_owner_item,
    owner_item,
    prospect_item,
    property_item,
    agent_item,
    market_item,
    touch_count,
  });

  log.info("context.load_complete", {
    brain_item_id,
    touch_count,
    conversation_stage: context_summary.conversation_stage,
    language_preference: context_summary.language_preference,
    contact_window: context_summary.contact_window,
    recent_event_count: recent_events_result.count,
  });

  return {
    found: true,
    inbound_from: normalized_phone,

    ids: {
      phone_item_id,
      brain_item_id,
      master_owner_id,
      owner_id,
      prospect_id,
      property_id,
      assigned_agent_id,
      market_id,
    },

    items: {
      phone_item,
      brain_item,
      master_owner_item,
      owner_item,
      prospect_item,
      property_item,
      agent_item,
      market_item,
    },

    flags: {
      do_not_call: getCategoryValue(phone_item, "do-not-call", "FALSE"),
      dnc_source: getCategoryValue(phone_item, "dnc-source", null),
      engagement_tier: getCategoryValue(phone_item, "engagement-tier", null),
      phone_activity_status: getCategoryValue(phone_item, "phone-activity-status", "Unknown"),
      follow_up_trigger_state: getCategoryValue(brain_item, "follow-up-trigger-state", null),
      status_ai_managed: getCategoryValue(brain_item, "status-ai-managed", null),
    },

    recent: {
      recently_used_template_ids: recently_used_ids,
      touch_count,
      last_template_id,
      last_inbound_message: getTextValue(brain_item, "last-inbound-message", ""),
      last_outbound_message: getTextValue(brain_item, "last-outbound-message", ""),
      recent_events: recent_events_result.events,
    },

    summary: context_summary,
  };
}

export default loadContext;
