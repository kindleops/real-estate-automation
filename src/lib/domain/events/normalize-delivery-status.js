/**
 * normalize-delivery-status.js
 *
 * Utility module for normalizing stale delivery_status fields on Supabase
 * message_events rows.
 *
 * Problem pattern:
 *   A message_events row may have `delivered_at` populated and `failed_at`
 *   null but still carry `delivery_status = 'pending'` (or 'sent').  This
 *   happens when the delivery webhook arrives, updates send_queue correctly,
 *   but the `provider_message_sid` join to message_events missed the row (or
 *   the update was not applied consistently).
 *
 * Guarantees:
 *   - Never touches rows where `failed_at` is populated.
 *   - Never downgrades a row that already has `delivery_status = 'delivered'`.
 *   - All DB writes are idempotent (UPDATE … WHERE delivery_status != 'delivered').
 */

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";

const MESSAGE_EVENTS_TABLE = "message_events";
const SEND_QUEUE_TABLE = "send_queue";

const DEFAULT_RECONCILE_LIMIT = 100;

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function getSupabase(deps = {}) {
  return deps.supabase || defaultSupabase;
}

// ---------------------------------------------------------------------------
// Pure normalization helper
// ---------------------------------------------------------------------------

/**
 * Given a message_events row, returns the delivery correction payload to apply
 * if the row has a populated `delivered_at` and no `failed_at` but its
 * `delivery_status` is not yet 'delivered'.
 *
 * Returns `null` when no correction is needed (already delivered, or has a
 * failure timestamp).
 *
 * @param {object} row  Raw or normalized message_events row.
 * @returns {{ delivery_status, provider_delivery_status, is_final_failure, failure_bucket, failure_reason, error_message } | null}
 */
export function applyDeliveredNormalization(row = {}) {
  if (!row.delivered_at) return null;
  if (row.failed_at) return null;
  if (lower(row.delivery_status) === "delivered") return null;

  return {
    delivery_status: "delivered",
    provider_delivery_status: "delivered",
    is_final_failure: false,
    failure_bucket: null,
    failure_reason: null,
    error_message: null,
  };
}

// ---------------------------------------------------------------------------
// Supabase delivery-status reconcile runner
// ---------------------------------------------------------------------------

/**
 * Reconciles stale delivery_status values in the Supabase message_events table.
 *
 * Two sweep paths run sequentially and are additive:
 *
 * Path 1 — delivered_at sweep:
 *   SELECT message_events WHERE delivered_at IS NOT NULL AND failed_at IS NULL
 *     AND delivery_status != 'delivered'
 *   → UPDATE delivery_status, provider_delivery_status, clear failure fields
 *
 * Path 2 — confirmed queue sweep:
 *   SELECT send_queue WHERE delivery_confirmed = 'confirmed' OR delivered_at IS NOT NULL
 *   → For each, UPDATE matching message_events WHERE provider_message_sid = queue.provider_message_id
 *     AND delivery_status != 'delivered' AND failed_at IS NULL
 *
 * Both paths are idempotent: they only update rows that are not already
 * correct and never touch rows that have an active failure timestamp.
 *
 * @param {object} [opts]
 * @param {number}  [opts.limit=100]   Max rows to inspect per path.
 * @param {string}  [opts.now]         ISO timestamp for updated_at writes.
 * @param {boolean} [opts.dry_run]     If true, skip DB writes (for diagnostics).
 * @param {object}  [deps]             Dependency injection (supabase).
 *
 * @returns {Promise<{
 *   ok: boolean,
 *   dry_run: boolean,
 *   normalized_from_delivered_at: number,
 *   normalized_from_queue_confirmed: number,
 *   total_normalized: number,
 *   errors: string[],
 * }>}
 */
export async function reconcileSupabaseDeliveryStatuses(
  { limit = DEFAULT_RECONCILE_LIMIT, now = null, dry_run = false } = {},
  deps = {}
) {
  const supabase = getSupabase(deps);
  const batch_now = now || nowIso();
  const errors = [];

  // ── Path 1: message_events rows with delivered_at set but stale status ──

  let stale_events = [];
  try {
    const { data, error } = await supabase
      .from(MESSAGE_EVENTS_TABLE)
      .select("id, provider_message_sid, delivery_status, delivered_at, failed_at")
      .not("delivered_at", "is", null)
      .is("failed_at", null)
      .neq("delivery_status", "delivered")
      .limit(limit);

    if (error) throw error;
    stale_events = Array.isArray(data) ? data : [];
  } catch (err) {
    errors.push(`path1_query: ${err?.message ?? String(err)}`);
  }

  let normalized_from_delivered_at = 0;
  for (const row of stale_events) {
    const correction = applyDeliveredNormalization(row);
    if (!correction) continue;

    if (dry_run) {
      normalized_from_delivered_at += 1;
      continue;
    }

    try {
      const { error } = await supabase
        .from(MESSAGE_EVENTS_TABLE)
        .update({ ...correction, updated_at: batch_now })
        .eq("id", row.id)
        .neq("delivery_status", "delivered")
        .is("failed_at", null);

      if (error) throw error;
      normalized_from_delivered_at += 1;
    } catch (err) {
      errors.push(`path1_update:${row.id}: ${err?.message ?? String(err)}`);
    }
  }

  // ── Path 2: send_queue confirmed rows → normalize matching message_events ──

  let confirmed_queue = [];
  try {
    const { data, error } = await supabase
      .from(SEND_QUEUE_TABLE)
      .select("id, provider_message_id, delivered_at, delivery_confirmed")
      .or("delivery_confirmed.eq.confirmed,delivered_at.not.is.null")
      .not("provider_message_id", "is", null)
      .limit(limit);

    if (error) throw error;
    confirmed_queue = Array.isArray(data) ? data : [];
  } catch (err) {
    errors.push(`path2_query: ${err?.message ?? String(err)}`);
  }

  let normalized_from_queue_confirmed = 0;
  for (const queue_row of confirmed_queue) {
    const provider_sid = clean(queue_row.provider_message_id);
    if (!provider_sid) continue;

    const delivered_at = clean(queue_row.delivered_at) || batch_now;
    const correction = {
      delivery_status: "delivered",
      provider_delivery_status: "delivered",
      delivered_at,
      is_final_failure: false,
      failure_bucket: null,
      failure_reason: null,
      updated_at: batch_now,
    };

    if (dry_run) {
      normalized_from_queue_confirmed += 1;
      continue;
    }

    try {
      const { error } = await supabase
        .from(MESSAGE_EVENTS_TABLE)
        .update(correction)
        .eq("provider_message_sid", provider_sid)
        .neq("delivery_status", "delivered")
        .is("failed_at", null);

      if (error) throw error;
      normalized_from_queue_confirmed += 1;
    } catch (err) {
      errors.push(`path2_update:${queue_row.id}: ${err?.message ?? String(err)}`);
    }
  }

  const total_normalized = normalized_from_delivered_at + normalized_from_queue_confirmed;

  return {
    ok: errors.length === 0,
    dry_run,
    normalized_from_delivered_at,
    normalized_from_queue_confirmed,
    total_normalized,
    errors,
  };
}

export default reconcileSupabaseDeliveryStatuses;
