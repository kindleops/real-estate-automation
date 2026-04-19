/**
 * sync-supabase-message-events-to-podio.js
 *
 * Async sync layer: reads un-synced rows from the Supabase message_events table
 * and mirrors them as Podio Message Events items.
 *
 * Design goals:
 * - Never blocks or is called from the SMS send path.
 * - Batch-processes up to SYNC_BATCH_SIZE rows per invocation.
 * - On Podio failure: marks the row failed, increments attempts, continues.
 * - On success: records the Podio item id and timestamps the sync.
 */

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { createMessageEvent } from "@/lib/podio/apps/message-events.js";
import {
  SELLER_MESSAGE_EVENT_FIELDS,
  normalizeSellerDeliveryStatus,
  extractOptOutDetails,
} from "@/lib/domain/events/seller-message-event.js";
import { toPodioDateField } from "@/lib/utils/dates.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";
import { captureSystemEvent } from "@/lib/analytics/posthog-server.js";
import { sendCriticalAlert } from "@/lib/alerts/discord.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SYNC_BATCH_SIZE = 50;

/**
 * Only these event_type values are meaningful to mirror to Podio.
 * Delivery-update-only mutations (syncDeliveryEvent) update existing rows in
 * place and never create standalone events, so they don't appear here.
 */
const SYNCABLE_EVENT_TYPES = new Set([
  "outbound_send",
  "outbound_send_failed",
  "inbound_sms",
]);

// ---------------------------------------------------------------------------
// Dependency injection helpers (for tests)
// ---------------------------------------------------------------------------

const defaultDeps = {
  createMessageEvent,
};

let runtimeDeps = { ...defaultDeps };

export function __setSyncPodioDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetSyncPodioDeps() {
  runtimeDeps = { ...defaultDeps };
}

// ---------------------------------------------------------------------------
// Field mapping helpers
// ---------------------------------------------------------------------------

function clean(value) {
  return String(value ?? "").trim();
}

function asArrayRef(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? [parsed] : undefined;
}

/**
 * Map Supabase lowercase direction strings to the Podio category values
 * used in the Message Events app.
 */
function toPodioDirection(direction) {
  const d = clean(direction).toLowerCase();
  if (d === "outbound") return "Outbound";
  if (d === "inbound") return "Inbound";
  return clean(direction) || undefined;
}

/**
 * Map Supabase event_type identifiers to the Podio category option text that
 * matches the live Message Events "category" field options.
 */
function toPodioEventType(event_type) {
  switch (clean(event_type).toLowerCase()) {
    case "outbound_send":        return "Seller Outbound SMS";
    case "outbound_send_failed": return "Send Failure";
    case "inbound_sms":          return "Seller Inbound SMS";
    default:                     return clean(event_type) || undefined;
  }
}

// ---------------------------------------------------------------------------
// Payload builder — exported for unit testing
// ---------------------------------------------------------------------------

/**
 * Converts a Supabase message_events row into a flat Podio fields object
 * suitable for createMessageEvent().
 *
 * Relation fields (master_owner, prospect, etc.) are only set when the row
 * carries a numeric Podio item_id.  String-only phone numbers are skipped
 * because the Podio phone_number field expects an item ref, not a raw string.
 *
 * @param {object} row  A row from the Supabase message_events table.
 * @returns {object}    Podio fields keyed by field slug.
 */
export function buildPodioPayloadForSupabaseEvent(row) {
  const timestamp =
    row.sent_at || row.received_at || row.event_timestamp || row.created_at || null;

  const delivery_status = clean(row.delivery_status) || null;
  const message_body = clean(row.message_body) || "";

  const fields = {
    // Core identifiers
    [SELLER_MESSAGE_EVENT_FIELDS.message_event_key]:
      clean(row.message_event_key) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.provider_message_sid]:
      clean(row.provider_message_sid) || undefined,

    // Timing
    [SELLER_MESSAGE_EVENT_FIELDS.timestamp]: toPodioDateField(timestamp) || undefined,

    // Classification
    [SELLER_MESSAGE_EVENT_FIELDS.direction]:   toPodioDirection(row.direction),
    [SELLER_MESSAGE_EVENT_FIELDS.event_type]:  toPodioEventType(row.event_type),

    // Message content
    [SELLER_MESSAGE_EVENT_FIELDS.message]:          message_body,
    [SELLER_MESSAGE_EVENT_FIELDS.character_count]:  row.character_count ?? message_body.length,

    // Delivery / status
    ...(delivery_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.delivery_status]:
            normalizeSellerDeliveryStatus(delivery_status),
        }
      : {}),
    ...(clean(row.raw_carrier_status)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.raw_carrier_status]: clean(row.raw_carrier_status) }
      : {}),
    ...(clean(row.provider_delivery_status)
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.provider_delivery_status]:
            clean(row.provider_delivery_status),
        }
      : {}),

    // Failure details
    ...(clean(row.failure_bucket)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.failure_bucket]: clean(row.failure_bucket) }
      : {}),
    ...(row.is_final_failure !== null && row.is_final_failure !== undefined
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.is_final_failure]:
            row.is_final_failure ? "Yes" : "No",
        }
      : {}),

    // Stage
    ...(clean(row.stage_before)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.stage_before]: clean(row.stage_before) }
      : {}),
    ...(clean(row.stage_after)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.stage_after]: clean(row.stage_after) }
      : {}),

    // Prior / response chain
    ...(clean(row.prior_message_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.prior_message_id]: clean(row.prior_message_id) }
      : {}),
    ...(clean(row.response_to_message_id)
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.response_to_message_id]:
            clean(row.response_to_message_id),
        }
      : {}),

    // CRM relation fields (only when a Podio item_id is present)
    ...(asArrayRef(row.master_owner_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.master_owner]: asArrayRef(row.master_owner_id) }
      : {}),
    ...(asArrayRef(row.prospect_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.prospect]: asArrayRef(row.prospect_id) }
      : {}),
    ...(asArrayRef(row.property_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.property]: asArrayRef(row.property_id) }
      : {}),
    ...(asArrayRef(row.market_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.market]: asArrayRef(row.market_id) }
      : {}),
    ...(asArrayRef(row.textgrid_number_id)
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.textgrid_number]:
            asArrayRef(row.textgrid_number_id),
        }
      : {}),
    ...(asArrayRef(row.sms_agent_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.sms_agent]: asArrayRef(row.sms_agent_id) }
      : {}),
    ...(asArrayRef(row.template_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.template]: asArrayRef(row.template_id) }
      : {}),
    ...(asArrayRef(row.brain_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.conversation]: asArrayRef(row.brain_id) }
      : {}),
  };

  // Opt-out detection: check both explicit column (if added later) and
  // inbound message body so the Podio record is properly tagged.
  const metadata =
    row.metadata && typeof row.metadata === "object" ? row.metadata : {};
  const opt_out_details = extractOptOutDetails(message_body);
  const is_opt_out =
    row.is_opt_out === true ||
    row.is_opt_out === "Yes" ||
    metadata.is_opt_out === "Yes" ||
    opt_out_details[SELLER_MESSAGE_EVENT_FIELDS.is_opt_out] === "Yes";

  if (is_opt_out) {
    fields[SELLER_MESSAGE_EVENT_FIELDS.is_opt_out] = "Yes";
    const keyword =
      clean(row.opt_out_keyword) ||
      clean(metadata.opt_out_keyword) ||
      clean(opt_out_details[SELLER_MESSAGE_EVENT_FIELDS.opt_out_keyword]);
    if (keyword) {
      fields[SELLER_MESSAGE_EVENT_FIELDS.opt_out_keyword] = keyword;
    }
  }

  // Drop undefined values so Podio doesn't receive nulled-out fields.
  return Object.fromEntries(
    Object.entries(fields).filter(([, v]) => v !== undefined)
  );
}

// ---------------------------------------------------------------------------
// Core sync runner
// ---------------------------------------------------------------------------

/**
 * Loads up to SYNC_BATCH_SIZE un-synced rows and mirrors each to Podio.
 *
 * @param {object} [options]
 * @param {object} [options.supabase]          Injected Supabase client (tests).
 * @param {Function} [options.createMessageEvent] Injected Podio creator (tests).
 * @param {number}  [options.limit]            Override batch size.
 * @returns {Promise<{synced: number, failed: number, skipped: number, total: number}>}
 */
export async function syncSupabaseMessageEventsToPodio(options = {}) {
  const supabase = options.supabase || defaultSupabase;
  const createEvent = options.createMessageEvent || runtimeDeps.createMessageEvent;
  const limit = options.limit ?? SYNC_BATCH_SIZE;

  console.log("PODIO MESSAGE EVENT SYNC STARTED");

  // ------------------------------------------------------------------
  // 1. Load candidate rows
  // ------------------------------------------------------------------
  const { data: rows, error: load_error } = await supabase
    .from("message_events")
    .select("*")
    .in("podio_sync_status", ["pending", "failed"])
    .in("direction", ["outbound", "inbound"])
    .order("created_at", { ascending: true })
    .limit(limit);

  if (load_error) {
    console.error("PODIO MESSAGE EVENT SYNC FAILED (load)", load_error.message);
    throw load_error;
  }

  const events = (rows || []).filter((row) =>
    SYNCABLE_EVENT_TYPES.has(row.event_type)
  );

  const skipped_count = (rows || []).length - events.length;

  console.log(
    `PODIO MESSAGE EVENT SYNC LOADED: ${events.length} events to sync` +
      (skipped_count ? `, ${skipped_count} skipped (noisy/unsupported type)` : "")
  );

  // Mark noisy rows as skipped so they don't re-appear in future batches.
  if (skipped_count > 0) {
    const skipped_ids = (rows || [])
      .filter((row) => !SYNCABLE_EVENT_TYPES.has(row.event_type))
      .map((row) => row.id);

    if (skipped_ids.length > 0) {
      await supabase
        .from("message_events")
        .update({ podio_sync_status: "skipped" })
        .in("id", skipped_ids);
    }
  }

  // ------------------------------------------------------------------
  // 2. Sync each event to Podio
  // ------------------------------------------------------------------
  let synced = 0;
  let failed = 0;

  for (const row of events) {
    try {
      const fields = buildPodioPayloadForSupabaseEvent(row);
      const item = await createEvent(fields);
      const podio_item_id = String(item?.item_id ?? item?.itemId ?? "");

      await supabase
        .from("message_events")
        .update({
          podio_sync_status:      "synced",
          podio_message_event_id: podio_item_id || null,
          podio_synced_at:        new Date().toISOString(),
          podio_sync_error:       null,
        })
        .eq("id", row.id);

      console.log(
        `PODIO MESSAGE EVENT CREATED: item=${podio_item_id} key=${row.message_event_key}`
      );
      synced++;
    } catch (err) {
      const attempts = (row.podio_sync_attempts ?? 0) + 1;

      await supabase
        .from("message_events")
        .update({
          podio_sync_status:     "failed",
          podio_sync_attempts:   attempts,
          podio_sync_error:      String(err?.message ?? err),
        })
        .eq("id", row.id);

      captureRouteException(err, {
        route: "internal/events/sync-podio",
        subsystem: "podio_sync",
        context: {
          message_event_key: row.message_event_key,
          podio_sync_attempts: attempts,
          event_type: row.event_type,
        },
      });

      captureSystemEvent("message_event_sync_to_podio_failed", {
        message_event_key: row.message_event_key,
        event_type: row.event_type,
        podio_sync_attempts: attempts,
        error_message: String(err?.message ?? err),
      });

      sendCriticalAlert({
        title: "Podio Sync Failure",
        description: "Failed to sync message event to Podio",
        color: 0xe74c3c,
        fields: [
          { name: "Message Event Key", value: String(row.message_event_key || "?"), inline: true },
          { name: "Event Type", value: String(row.event_type || "?"), inline: true },
          { name: "Attempts", value: String(attempts), inline: true },
          { name: "Error", value: String(err?.message ?? err).slice(0, 256), inline: false },
        ],
        timestamp: new Date().toISOString(),
        footer: { text: "internal/events/sync-podio" },
      });

      console.error(
        `PODIO MESSAGE EVENT SYNC FAILED: key=${row.message_event_key} attempt=${attempts} error=${err?.message ?? err}`
      );
      failed++;
      // Continue — one failure must never abort the rest of the batch.
    }
  }

  console.log(
    `PODIO MESSAGE EVENT SYNC COMPLETE: synced=${synced} failed=${failed} skipped=${skipped_count} total=${events.length}`
  );

  captureSystemEvent("message_event_sync_to_podio_completed", {
    synced,
    failed,
    skipped: skipped_count,
    total: events.length,
  });

  return {
    synced,
    failed,
    skipped: skipped_count,
    total: events.length,
  };
}

export default syncSupabaseMessageEventsToPodio;
