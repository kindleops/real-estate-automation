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
 * A row is skipped permanently after this many Podio failures.
 * Matches the coalesce(podio_sync_attempts, 0) < 3 condition in the DB design.
 */
const MAX_SYNC_ATTEMPTS = 3;

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

/**
 * Safe normalization for the Podio `delivery-status` category field.
 *
 * Only maps to values that are confirmed safe in the Podio Message Events schema.
 * Returns undefined for values that may not exist (pending, queued, sending,
 * unknown, etc.) so the field is omitted rather than causing a category-value
 * validation error on the Podio API.  Call sites must check for undefined and
 * skip this field when the result is undefined.
 *
 * Known-safe Podio delivery-status options:  Sent, Delivered, Failed, Received.
 * Intentionally excluded:  Queued, Sending, Undelivered, Unknown — Podio
 * schema support is not confirmed; omitting is safer than risking a 400.
 */
function toPodioProviderDeliveryStatus(value) {
  const raw = clean(value).toLowerCase();
  if (!raw) return undefined;
  if (raw === "sent")                                                 return "Sent";
  if (["delivered", "delivery_confirmed", "confirmed"].includes(raw)) return "Delivered";
  if (["failed", "error", "delivery_failed", "undelivered"].includes(raw)) return "Failed";
  if (raw === "received")                                             return "Received";
  // pending, queued, accepted, sending, unknown → omit (not confirmed safe)
  return undefined;
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
/** Placeholder sent when message_body is null/empty so Podio's min-length validator passes. */
const MESSAGE_BODY_PLACEHOLDER = "[Message body unavailable]";

export function buildPodioPayloadForSupabaseEvent(row) {
  const timestamp =
    row.sent_at || row.received_at || row.event_timestamp || row.created_at || null;

  const delivery_status = clean(row.delivery_status) || null;

  // Preserve the raw body length for character_count (reflects actual SMS chars).
  const raw_message_body = clean(row.message_body);
  const message_body_empty = !raw_message_body;
  // Use placeholder so Podio's "must be ≥1 character" validation never rejects
  // the record solely because an inbound message had no body saved.
  const message_body = raw_message_body || MESSAGE_BODY_PLACEHOLDER;

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
    // character_count reflects actual SMS character count, not the placeholder.
    [SELLER_MESSAGE_EVENT_FIELDS.message]:          message_body,
    [SELLER_MESSAGE_EVENT_FIELDS.character_count]:  row.character_count ?? raw_message_body.length,

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
    // provider_delivery_status → delivery-status Podio category field.
    // Must be normalized to a confirmed-valid Podio option; raw values like
    // "pending" are NOT valid category options and will cause a Podio 400 error.
    ...(toPodioProviderDeliveryStatus(row.provider_delivery_status) !== undefined
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.provider_delivery_status]:
            toPodioProviderDeliveryStatus(row.provider_delivery_status),
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
  // Treat 0 / negative / null / undefined as "use default".
  // Max is capped at 100 per-call to prevent runaway batch sizes.
  const MAX_BATCH_LIMIT = 100;
  const effective_limit =
    (options.limit != null && Number(options.limit) > 0)
      ? Math.min(Number(options.limit), MAX_BATCH_LIMIT)
      : SYNC_BATCH_SIZE;
  const limit = effective_limit;

  const started_at = Date.now();
  console.log("PODIO MESSAGE EVENT SYNC STARTED");

  // ------------------------------------------------------------------
  // 1. Load candidate rows
  // ------------------------------------------------------------------
  // Key design decisions:
  //
  //  a) Filter by event_type at DB level with a simple .in() — this is
  //     safe PostgREST syntax and avoids loading irrelevant rows.
  //
  //  b) Use individual .eq operators inside .or() instead of .in.()
  //     syntax.  PostgREST's handling of nested parentheses in
  //     "or=(col.in.(a,b),col.is.null)" is unreliable across versions
  //     and silently returns 0 rows.  "eq.pending,eq.failed" is
  //     equivalent and is proven to work (mirrors the diagnostic route).
  //
  //  c) direction filter removed — event_type already constrains the
  //     rows to outbound_send / outbound_send_failed / inbound_sms;
  //     filtering on direction in addition is redundant and fragile.
  //
  //  d) Order by created_at DESC so oldest-first batching happens
  //     naturally when the caller increases limit across multiple runs.
  const query_filters_used = {
    event_type_in: [...SYNCABLE_EVENT_TYPES],
    podio_sync_status_or: ["pending", "failed", "null"],
    podio_sync_attempts_lt: MAX_SYNC_ATTEMPTS,
    order: "created_at desc",
    limit,
  };

  const { data: raw_rows, error: load_error } = await supabase
    .from("message_events")
    .select("*")
    .in("event_type", [...SYNCABLE_EVENT_TYPES])
    .or("podio_sync_status.eq.pending,podio_sync_status.eq.failed,podio_sync_status.is.null")
    .order("created_at", { ascending: false })
    .limit(limit);

  if (load_error) {
    console.error("PODIO MESSAGE EVENT SYNC FAILED (load)", load_error.message, {
      hint: "If error mentions a missing column, run the pending Supabase migration.",
    });
    throw load_error;
  }

  const raw_rows_loaded_before_filter = (raw_rows || []).length;

  // Re-verify event_type in JS (should always be a no-op since DB filtered,
  // but guards against unexpected rows slipping through).
  const after_syncable_filter = (raw_rows || []).filter((row) =>
    SYNCABLE_EVENT_TYPES.has(row.event_type)
  );
  const rows_after_syncable_filter = after_syncable_filter.length;

  // Apply status + attempts cap in JS.
  // Status re-verify guards against unexpected rows returned by the DB
  // (e.g. a future migration that relaxes the column filter).
  // null podio_sync_status is coalesced to "pending" (pre-migration rows).
  const ELIGIBLE_STATUSES = new Set(["pending", "failed"]);
  const candidates = after_syncable_filter.filter((row) => {
    const status = row.podio_sync_status ?? "pending";
    return ELIGIBLE_STATUSES.has(status) && (row.podio_sync_attempts ?? 0) < MAX_SYNC_ATTEMPTS;
  });
  const rows_after_attempt_filter = candidates.length;

  // Backward-compat aliases used in logging + return value below.
  const loaded_count = raw_rows_loaded_before_filter;
  const events = candidates;

  // Rows skipped because their event_type isn't in SYNCABLE_EVENT_TYPES
  // (should be empty since the DB query filters by event_type, but kept
  // as a safety net).
  const skipped_rows = (raw_rows || []).filter(
    (row) => !SYNCABLE_EVENT_TYPES.has(row.event_type)
  );
  const skipped_count = skipped_rows.length;

  const first_10_skipped_reasons = skipped_rows.slice(0, 10).map((row) => ({
    key: row.message_event_key || null,
    event_type: row.event_type || null,
  }));

  const first_10_candidate_event_keys = candidates.slice(0, 10).map((row) => row.message_event_key || null);
  // Backward-compat alias.
  const first_10_event_keys = first_10_candidate_event_keys;

  console.log(
    `PODIO MESSAGE EVENT SYNC LOADED: raw=${raw_rows_loaded_before_filter}` +
    ` syncable=${rows_after_syncable_filter}` +
    ` candidates=${rows_after_attempt_filter}` +
    (skipped_count ? ` unsupported_type_skipped=${skipped_count}` : "")
  );
  if (first_10_candidate_event_keys.length) {
    console.log("PODIO MESSAGE EVENT SYNC KEYS:", JSON.stringify(first_10_candidate_event_keys));
  }

  // Mark any unsupported-type rows as skipped so they don't re-appear.
  if (skipped_count > 0) {
    const skipped_ids = skipped_rows.map((row) => row.id);
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
  const failed_errors = [];

  for (const row of events) {
    let fields = null;
    try {
      fields = buildPodioPayloadForSupabaseEvent(row);
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

      const error_detail = {
        id:               row.id ?? null,
        key:              row.message_event_key || null,
        event_type:       row.event_type || null,
        direction:        row.direction || null,
        attempts,
        error:            String(err?.message ?? err),
        direction_sent:   fields?.["direction"] ?? null,
        category_sent:    fields?.["category"] ?? null,
        body_empty:       !fields?.["message"],
      };

      if (failed_errors.length < 10) {
        failed_errors.push(error_detail);
      }

      captureRouteException(err, {
        route: "internal/events/sync-podio",
        subsystem: "podio_sync",
        context: {
          message_event_key: row.message_event_key,
          podio_sync_attempts: attempts,
          event_type: row.event_type,
          direction_sent: error_detail.direction_sent,
          category_sent: error_detail.category_sent,
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
        `PODIO MESSAGE EVENT SYNC FAILED: key=${row.message_event_key}` +
        ` attempt=${attempts}` +
        ` direction=${error_detail.direction_sent}` +
        ` category=${error_detail.category_sent}` +
        ` body_empty=${error_detail.body_empty}` +
        ` error=${err?.message ?? err}`
      );
      failed++;
      // Continue — one failure must never abort the rest of the batch.
    }
  }

  console.log(
    `PODIO MESSAGE EVENT SYNC COMPLETE: loaded=${loaded_count} synced=${synced} failed=${failed} skipped=${skipped_count} total=${events.length}`
  );

  captureSystemEvent("message_event_sync_to_podio_completed", {
    loaded_count,
    synced,
    failed,
    skipped: skipped_count,
    total: events.length,
  });

  const duration_ms = Date.now() - started_at;

  return {
    // Named counts (preferred)
    loaded_count,
    synced_count: synced,
    failed_count: failed,
    skipped_count,
    total: events.length,
    // Backward-compatible aliases
    synced,
    failed,
    skipped: skipped_count,
    // Diagnostic arrays (first 10 of each)
    first_10_event_keys,
    first_10_candidate_event_keys,
    first_10_failed_errors: failed_errors,
    first_10_skipped_reasons,
    // Query-filter diagnostics
    query_filters_used,
    raw_rows_loaded_before_filter,
    rows_after_syncable_filter,
    rows_after_attempt_filter,
    // Metadata
    effective_limit,
    duration_ms,
    syncable_event_types: [...SYNCABLE_EVENT_TYPES],
    max_sync_attempts: MAX_SYNC_ATTEMPTS,
  };
}

export default syncSupabaseMessageEventsToPodio;
