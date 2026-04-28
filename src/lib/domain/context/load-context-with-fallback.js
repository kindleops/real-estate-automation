// ─── load-context-with-fallback.js ────────────────────────────────────────
// Wrapper around loadContext that adds fallback to recent outbound pair.
// When phone lookup fails, tries to find recent outbound send_queue/message_event pair.

import loadContext from "@/lib/domain/context/load-context.js";
import { findRecentOutboundContextPair } from "@/lib/domain/context/find-recent-outbound-pair.js";
import { info, warn } from "@/lib/logging/logger.js";

function clean(value) {
  return String(value ?? "").trim();
}

/**
 * Load context with fallback to recent outbound pair.
 *
 * Primary path: Phone lookup via Podio + brain resolution
 * Fallback path: Recent outbound message to/from pair in Supabase
 *
 * Response includes diagnostics:
 * - lookup_sources_tried: ["phone"] or ["phone", "fallback_outbound_pair"]
 * - fallback_match_source: null | "recent_outbound_send_queue" | "recent_outbound_message_event"
 * - fallback_pair_match: false | true
 * - fallback_match_data: null | { queue_row_id, event_id, ... }
 *
 * @param {Object} opts
 * @param {string} opts.inbound_from - Seller's phone number (normalized E164)
 * @param {string} opts.inbound_to - Our TextGrid number (normalized E164)
 * @param {boolean} opts.create_brain_if_missing - Default true
 * @returns {Promise<{
 *   found: boolean,
 *   reason?: string,
 *   lookup_sources_tried: string[],
 *   fallback_pair_match: boolean,
 *   fallback_match_source?: string,
 *   fallback_match_data?: object,
 *   ...context fields
 * }>}
 */
export async function loadContextWithFallback({
  inbound_from,
  inbound_to,
  create_brain_if_missing = true,
  primary_context = null,
  loadContextImpl = loadContext,
  findRecentOutboundContextPairImpl = findRecentOutboundContextPair,
} = {}) {
  const lookup_sources_tried = ["phone"];
  let fallback_pair_match = false;
  let fallback_match_source = null;
  let fallback_match_data = null;

  // Step 1: Try primary phone lookup
  let context = primary_context;
  if (!context) {
    context = await loadContextImpl({
      inbound_from,
      create_brain_if_missing,
    });
  }

  if (context?.found) {
    // Phone found, return immediately with diagnostics
    return {
      ...context,
      lookup_sources_tried,
      fallback_pair_match: false,
      fallback_match_source: null,
      fallback_match_id: null,
      fallback_match_data: null,
    };
  }

  const primary_reason = clean(context?.reason || "phone_not_found").toLowerCase();
  if (primary_reason !== "phone_not_found") {
    return {
      ...context,
      lookup_sources_tried,
      fallback_pair_match: false,
      fallback_match_source: null,
      fallback_match_id: null,
      fallback_match_data: null,
    };
  }

  // Step 2: Phone not found, try fallback
  info("context.load_attempting_fallback", {
    inbound_from,
    inbound_to,
    primary_reason: context?.reason || "phone_not_found",
  });

  lookup_sources_tried.push("fallback_outbound_pair");

  const fallback_result = await findRecentOutboundContextPairImpl(
    inbound_from,
    inbound_to
  );

  if (!fallback_result?.found) {
    // Fallback also failed, return original not-found with diagnostics
    info("context.load_fallback_failed", {
      inbound_from,
      fallback_reason: fallback_result?.reason || "unknown",
    });

    return {
      ...context,
      lookup_sources_tried,
      fallback_pair_match: false,
      fallback_match_source: null,
      fallback_match_id: null,
      fallback_match_data: null,
    };
  }

  // Fallback succeeded! Build context from outbound pair
  info("context.load_fallback_succeeded", {
    inbound_from,
    fallback_source: fallback_result.source,
    master_owner_id: fallback_result.context?.ids?.master_owner_id,
    prospect_id: fallback_result.context?.ids?.prospect_id,
  });

  fallback_pair_match = true;
  fallback_match_source = fallback_result.source;
  const fallback_match = fallback_result.context?.match || {};
  fallback_match_data = {
    queue_row_id: fallback_result.context?.queue_row_id || null,
    event_id: fallback_result.context?.event_id || null,
    matched_queue_id:
      fallback_match.matched_queue_id ||
      fallback_result.context?.queue_row_id ||
      null,
    matched_queue_status: fallback_match.matched_queue_status || null,
    matched_sent_at: fallback_match.matched_sent_at || null,
    matched_source: fallback_match.matched_source || null,
    skipped_newer_orphan_count: fallback_match.skipped_newer_orphan_count || 0,
    match_strategy: fallback_match.match_strategy || null,
    context_verified: Boolean(fallback_match.context_verified),
  };

  // Extract the match ID (whichever one exists from the pair)
  const fallback_match_id = fallback_result.context?.queue_row_id || fallback_result.context?.event_id || null;
  const fallback_ids = fallback_result.context?.ids || {};
  const conversation_brain_id = fallback_ids.conversation_brain_id || null;
  const brain_item_id = /^\d+$/.test(clean(conversation_brain_id)) ? conversation_brain_id : null;

  // Build a synthetic context from the fallback data
  const fallback_context = {
    found: true,
    inbound_from,
    lookup_sources_tried,
    fallback_pair_match: true,
    fallback_match_source,
    fallback_match_id,
    fallback_match_data,

    reason: null,

    // IDs extracted from the outbound record
    ids: {
      phone_item_id: null, // Not available from outbound pair
      brain_item_id,
      conversation_brain_id,
      master_owner_id: fallback_ids.master_owner_id || null,
      owner_id: null,
      prospect_id: fallback_ids.prospect_id || null,
      property_id: fallback_ids.property_id || null,
      template_id: fallback_ids.template_id || null,
      textgrid_number_id: fallback_ids.textgrid_number_id || null,
      assigned_agent_id: null,
      market_id: null,
    },

    // Items -- null since we don't have these from outbound
    items: {
      phone_item: null,
      brain_item: null,
      master_owner_item: null,
      owner_item: null,
      prospect_item: null,
      property_item: null,
      agent_item: null,
      market_item: null,
    },

    // Flags -- use defaults
    flags: {
      do_not_call: "FALSE",
      dnc_source: null,
      engagement_tier: null,
      phone_activity_status: "Unknown",
      follow_up_trigger_state: null,
      status_ai_managed: null,
    },

    // Recent events from the outbound record
    recent: {
      recently_used_template_ids: fallback_result.context?.ids?.template_id
        ? [fallback_result.context.ids.template_id]
        : [],
      touch_count: 1, // At least the message we're replying to
      last_template_id: fallback_result.context?.ids?.template_id || null,
      last_inbound_message: "",
      last_outbound_message: fallback_result.context?.recent?.last_outbound_message || "",
      recent_events: [],
      outbound_pair_match: fallback_match_data,
    },

    summary: {
      conversation_stage: "unknown",
      language_preference: "unknown",
      contact_window: "unknown",
      property_address: null,
    },
  };

  return fallback_context;
}

export default loadContextWithFallback;
