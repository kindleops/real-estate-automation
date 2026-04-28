// ─── find-recent-outbound-pair.js ─────────────────────────────────────────
// Fallback context resolution for inbound SMS when phone lookup fails.
// Looks for recent outbound send_queue or message_events matching From/To pair.

import { supabase } from "@/lib/supabase/client.js";
import { normalizeInboundTextgridPhone } from "@/lib/providers/textgrid.js";
import { warn } from "@/lib/logging/logger.js";

const SEND_QUEUE_PAIR_LIMIT = 50;

const DEPRIORITIZED_QUEUE_STATUSES = new Set([
  "blocked",
  "paused_name_missing",
  "paused_global_lock",
  "paused_duplicate",
  "failed",
  "cancelled",
  "canceled",
]);

const DEPRIORITIZED_SOURCES = new Set(["inbox", "leadcommand_inbox"]);

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function hasValue(value) {
  return clean(value) !== "";
}

function asTimestamp(value) {
  const parsed = Date.parse(clean(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function rowTimestamp(row = {}) {
  return Math.max(asTimestamp(row.sent_at), asTimestamp(row.created_at));
}

function metadata(row = {}) {
  return row?.metadata && typeof row.metadata === "object" ? row.metadata : {};
}

function pickFirst(...values) {
  for (const value of values) {
    if (hasValue(value)) return value;
  }
  return null;
}

function resolveQueueSource(row = {}) {
  const meta = metadata(row);
  return clean(
    pickFirst(
      row.source,
      row.queue_source,
      row.source_app,
      meta.source,
      meta.queue_source,
      meta.source_app
    )
  );
}

function resolveConversationBrainId(row = {}) {
  const meta = metadata(row);
  return pickFirst(
    row.conversation_brain_id,
    row.brain_id,
    row.conversation_item_id,
    meta.conversation_brain_id,
    meta.brain_id,
    meta.conversation_item_id
  );
}

function resolveTextgridNumberId(row = {}) {
  const meta = metadata(row);
  return pickFirst(
    row.textgrid_number_id,
    row.textgrid_number_item_id,
    row.outbound_number_item_id,
    meta.textgrid_number_id,
    meta.textgrid_number_item_id,
    meta.selected_textgrid_number_id
  );
}

function hasUsableContext(row = {}) {
  return hasValue(row.master_owner_id) && hasValue(row.property_id);
}

function isDeprioritizedSource(row = {}) {
  return DEPRIORITIZED_SOURCES.has(lower(resolveQueueSource(row)));
}

function isValidSentContextualQueueRow(row = {}) {
  return lower(row.queue_status) === "sent" && hasValue(row.sent_at) && hasUsableContext(row);
}

function isSkippedContextCandidate(row = {}) {
  const status = lower(row.queue_status);
  return (
    DEPRIORITIZED_QUEUE_STATUSES.has(status) ||
    status !== "sent" ||
    !hasValue(row.sent_at) ||
    !hasUsableContext(row) ||
    isDeprioritizedSource(row)
  );
}

function sortLatestFirst(rows = []) {
  return [...rows].sort((a, b) => {
    const timestamp_delta = rowTimestamp(b) - rowTimestamp(a);
    if (timestamp_delta !== 0) return timestamp_delta;
    return clean(b.id).localeCompare(clean(a.id));
  });
}

function selectSendQueuePairMatch(rows = []) {
  const ordered = sortLatestFirst(Array.isArray(rows) ? rows : []);
  if (!ordered.length) return null;

  const preferred =
    ordered.find((row) => isValidSentContextualQueueRow(row) && !isDeprioritizedSource(row)) ||
    ordered.find((row) => isValidSentContextualQueueRow(row));

  if (preferred) {
    const selected_index = ordered.indexOf(preferred);
    const skipped_newer_orphan_count = ordered
      .slice(0, Math.max(selected_index, 0))
      .filter(isSkippedContextCandidate)
      .length;

    return {
      row: preferred,
      context_verified: true,
      match_strategy: "valid_sent_contextual_outbound",
      skipped_newer_orphan_count,
    };
  }

  return {
    row: ordered[0],
    context_verified: false,
    match_strategy: "fallback_latest_pair_match",
    skipped_newer_orphan_count: 0,
  };
}

function buildSendQueueMatchContext(row = {}, match = {}) {
  const matched_source = resolveQueueSource(row) || null;
  const matched_sent_at = clean(row.sent_at) || null;
  const matched_queue_status = clean(row.queue_status) || null;
  const matched_queue_id = row.id || null;
  const conversation_brain_id = resolveConversationBrainId(row);
  const textgrid_number_id = resolveTextgridNumberId(row);

  return {
    ids: {
      master_owner_id: row.master_owner_id || null,
      prospect_id: row.prospect_id || null,
      property_id: row.property_id || null,
      template_id: row.template_id || null,
      textgrid_number_id: textgrid_number_id || null,
      conversation_brain_id: conversation_brain_id || null,
    },
    recent: {
      last_outbound_message: clean(row.message_body || row.message_text) || null,
      last_outbound_at: row.sent_at || row.created_at || null,
    },
    queue_row_id: matched_queue_id,
    match: {
      matched_queue_id,
      matched_queue_status,
      matched_sent_at,
      matched_source,
      skipped_newer_orphan_count: match.skipped_newer_orphan_count || 0,
      match_strategy: match.match_strategy || "fallback_latest_pair_match",
      context_verified: Boolean(match.context_verified),
    },
  };
}

function buildMessageEventMatchContext(row = {}) {
  const conversation_brain_id = resolveConversationBrainId(row);
  const textgrid_number_id = resolveTextgridNumberId(row);

  return {
    ids: {
      master_owner_id: row.master_owner_id || null,
      prospect_id: row.prospect_id || null,
      property_id: row.property_id || null,
      template_id: row.template_id || null,
      textgrid_number_id: textgrid_number_id || null,
      conversation_brain_id: conversation_brain_id || null,
    },
    recent: {
      last_outbound_message: clean(row.message_body) || null,
      last_outbound_at: row.sent_at || row.created_at || null,
    },
    event_id: row.id,
    match: {
      matched_queue_id: null,
      matched_queue_status: null,
      matched_sent_at: row.sent_at || null,
      matched_source: resolveQueueSource(row) || null,
      skipped_newer_orphan_count: 0,
      match_strategy: "fallback_latest_pair_match",
      context_verified: false,
    },
  };
}

/**
 * Try to find recent outbound context by matching inbound From/To pair.
 *
 * When an inbound SMS arrives from a phone not in our personal phones table,
 * we can look for a recent outbound message to that number from our TextGrid
 * number. If found, we extract master_owner_id, prospect_id, property_id, etc
 * from the outbound record.
 *
 * @param {string} inbound_from - Normalized inbound From number (e.g., +16128072000)
 * @param {string} inbound_to - Normalized inbound To number (e.g., +16128060495)
 * @param {object} [opts]
 * @returns {Promise<{
 *   found: boolean,
 *   source?: string,
 *   reason?: string,
 *   context?: {
 *     ids: {
 *       master_owner_id: string|null,
 *       prospect_id: string|null,
 *       property_id: string|null,
 *       template_id: string|null,
 *       textgrid_number_id: string|null,
 *       conversation_brain_id: string|null,
 *     },
 *     recent: {
 *       last_outbound_message?: string,
 *       last_outbound_at?: string,
 *     }
 *   }
 * }>}
 */
export async function findRecentOutboundContextPair(inbound_from, inbound_to, opts = {}) {
  const db = opts.supabase || opts.db || supabase;

  // Normalize both numbers into TextGrid's inbound canonical form for matching.
  const from_e164 = normalizeInboundTextgridPhone(inbound_from);
  const to_e164 = normalizeInboundTextgridPhone(inbound_to);

  if (!from_e164 || !to_e164) {
    warn("context.fallback_pair_invalid_numbers", {
      inbound_from,
      inbound_to,
      from_e164,
      to_e164,
    });

    return {
      found: false,
      reason: "invalid_phone_numbers",
      source: "recent_outbound",
    };
  }

  // Step 1: Try send_queue. Inbound From/To reverses outbound To/From.
  try {
    const { data: sq_rows, error: sq_error } = await db
      .from("send_queue")
      .select("*")
      .eq("to_phone_number", from_e164)
      .eq("from_phone_number", to_e164)
      .order("sent_at", { ascending: false, nullsFirst: false })
      .order("created_at", { ascending: false })
      .limit(SEND_QUEUE_PAIR_LIMIT);

    if (sq_error) {
      warn("context.fallback_pair_send_queue_error", {
        inbound_from,
        inbound_to,
        error: sq_error.message,
      });
    } else if (sq_rows && sq_rows.length > 0) {
      const match = selectSendQueuePairMatch(sq_rows);
      const row = match?.row || sq_rows[0];
      const context = buildSendQueueMatchContext(row, match);

      warn("context.fallback_pair_found_in_send_queue", {
        inbound_from,
        to_phone_number: from_e164,
        from_phone_number: to_e164,
        master_owner_id: context.ids.master_owner_id,
        prospect_id: context.ids.prospect_id,
        property_id: context.ids.property_id,
        queue_id: context.queue_row_id,
        ...context.match,
      });

      return {
        found: true,
        source: "recent_outbound_send_queue",
        context,
      };
    }
  } catch (err) {
    warn("context.fallback_pair_send_queue_exception", {
      inbound_from,
      inbound_to,
      error: err.message,
    });
  }

  // Step 2: Try message_events when send_queue has no pair match.
  try {
    const { data: me_rows, error: me_error } = await db
      .from("message_events")
      .select("*")
      .eq("direction", "outbound")
      .eq("to_phone_number", from_e164)
      .eq("from_phone_number", to_e164)
      .order("sent_at", { ascending: false, nullsFirst: false })
      .order("created_at", { ascending: false })
      .limit(1);

    if (me_error) {
      warn("context.fallback_pair_message_events_error", {
        inbound_from,
        inbound_to,
        error: me_error.message,
      });
    } else if (me_rows && me_rows.length > 0) {
      const row = me_rows[0];
      const context = buildMessageEventMatchContext(row);

      warn("context.fallback_pair_found_in_message_events", {
        inbound_from,
        to_phone_number: from_e164,
        from_phone_number: to_e164,
        master_owner_id: context.ids.master_owner_id,
        prospect_id: context.ids.prospect_id,
        event_id: context.event_id,
      });

      return {
        found: true,
        source: "recent_outbound_message_event",
        context,
      };
    }
  } catch (err) {
    warn("context.fallback_pair_message_events_exception", {
      inbound_from,
      inbound_to,
      error: err.message,
    });
  }

  warn("context.fallback_pair_not_found", {
    inbound_from,
    to_phone_number: from_e164,
    from_phone_number: to_e164,
  });

  return {
    found: false,
    reason: "no_recent_outbound_pair",
    source: "recent_outbound",
  };
}

export default findRecentOutboundContextPair;
