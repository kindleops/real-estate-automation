// ─── find-recent-outbound-pair.js ─────────────────────────────────────────
// Fallback context resolution for inbound SMS when phone lookup fails.
// Looks for recent outbound send_queue or message_events matching From/To pair.

import { supabase } from "@/lib/supabase/client.js";
import { normalizeInboundTextgridPhone } from "@/lib/providers/textgrid.js";
import { warn } from "@/lib/logging/logger.js";

function clean(value) {
  return String(value ?? "").trim();
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
 *     },
 *     recent: {
 *       last_outbound_message?: string,
 *       last_outbound_at?: string,
 *     }
 *   }
 * }>}
 */
export async function findRecentOutboundContextPair(inbound_from, inbound_to) {
  // Normalize both numbers to E164 for matching
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

  // Step 1: Try send_queue
  // Looking for a row where:
  //   - to_phone_number = inbound_from (the seller's number that's sending us the SMS)
  //   - from_phone_number = inbound_to (our TextGrid number)
  //   - Order by sent_at desc, created_at desc to get the most recent
  try {
    const { data: sq_rows, error: sq_error } = await supabase
      .from("send_queue")
      .select(
        "id, master_owner_id, prospect_id, property_id, template_id, textgrid_number_id, message_body, sent_at, created_at"
      )
      .eq("to_phone_number", from_e164)
      .eq("from_phone_number", to_e164)
      .order("sent_at", { ascending: false, nullsFirst: false })
      .order("created_at", { ascending: false })
      .limit(1);

    if (sq_error) {
      warn("context.fallback_pair_send_queue_error", {
        inbound_from,
        inbound_to,
        error: sq_error.message,
      });
    } else if (sq_rows && sq_rows.length > 0) {
      const row = sq_rows[0];

      warn("context.fallback_pair_found_in_send_queue", {
        inbound_from,
        to_phone_number: from_e164,
        from_phone_number: to_e164,
        master_owner_id: row.master_owner_id,
        prospect_id: row.prospect_id,
        queue_id: row.id,
      });

      return {
        found: true,
        source: "recent_outbound_send_queue",
        context: {
          ids: {
            master_owner_id: row.master_owner_id || null,
            prospect_id: row.prospect_id || null,
            property_id: row.property_id || null,
            template_id: row.template_id || null,
            textgrid_number_id: row.textgrid_number_id || null,
          },
          recent: {
            last_outbound_message: clean(row.message_body) || null,
            last_outbound_at: row.sent_at || row.created_at || null,
          },
          queue_row_id: row.id,
        },
      };
    }
  } catch (err) {
    warn("context.fallback_pair_send_queue_exception", {
      inbound_from,
      inbound_to,
      error: err.message,
    });
  }

  // Step 2: Try message_events
  // Looking for an outbound message with:
  //   - direction = outbound
  //   - to_phone_number = inbound_from
  //   - from_phone_number = inbound_to
  try {
    const { data: me_rows, error: me_error } = await supabase
      .from("message_events")
      .select(
        "id, master_owner_id, prospect_id, property_id, template_id, textgrid_number_id, message_body, sent_at, created_at"
      )
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

      warn("context.fallback_pair_found_in_message_events", {
        inbound_from,
        to_phone_number: from_e164,
        from_phone_number: to_e164,
        master_owner_id: row.master_owner_id,
        prospect_id: row.prospect_id,
        event_id: row.id,
      });

      return {
        found: true,
        source: "recent_outbound_message_event",
        context: {
          ids: {
            master_owner_id: row.master_owner_id || null,
            prospect_id: row.prospect_id || null,
            property_id: row.property_id || null,
            template_id: row.template_id || null,
            textgrid_number_id: row.textgrid_number_id || null,
          },
          recent: {
            last_outbound_message: clean(row.message_body) || null,
            last_outbound_at: row.sent_at || row.created_at || null,
          },
          event_id: row.id,
        },
      };
    }
  } catch (err) {
    warn("context.fallback_pair_message_events_exception", {
      inbound_from,
      inbound_to,
      error: err.message,
    });
  }

  // No pair found in either table
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
