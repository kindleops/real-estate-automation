import { NextResponse } from "next/server";

import { supabase, hasSupabaseConfig } from "@/lib/supabase/client.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const SYNCABLE_EVENT_TYPES = ["outbound_send", "outbound_send_failed", "inbound_sms"];

function clean(value) {
  return String(value ?? "").trim();
}

function requireAuth(request) {
  return requireSharedSecretAuth(request, null, {
    env_name: "INTERNAL_API_SECRET",
    header_names: ["x-internal-api-secret"],
  });
}

function groupBy(rows, key) {
  const counts = {};
  for (const row of rows) {
    const val = String(row[key] ?? "null");
    counts[val] = (counts[val] ?? 0) + 1;
  }
  return counts;
}

/**
 * GET /api/internal/events/sync-podio-diagnostic?limit=20
 * Header: x-internal-api-secret: <INTERNAL_API_SECRET>
 *
 * Returns:
 *  - recent message_events with Podio sync columns
 *  - counts grouped by event_type, direction, podio_sync_status
 *  - latest failed/pending rows for triage
 *  - column readability check (confirms migration ran)
 *  - syncable event type list
 */
export async function GET(request) {
  const auth = requireAuth(request);
  if (!auth.authorized) return auth.response;

  if (!hasSupabaseConfig()) {
    return NextResponse.json({ ok: false, error: "supabase_not_configured" }, { status: 503 });
  }

  const { searchParams } = new URL(request.url);
  const limit = Math.min(
    Number.isFinite(Number(searchParams.get("limit"))) ? Number(searchParams.get("limit")) : 20,
    50
  );

  // ── 1. Recent message_events (any direction) ─────────────────────────────

  let recent_rows = [];
  let load_error = null;
  let columns_readable = false;

  try {
    const { data, error } = await supabase
      .from("message_events")
      .select(
        "id, message_event_key, direction, event_type, message_body, " +
        "podio_sync_status, podio_message_event_id, podio_synced_at, " +
        "podio_sync_error, podio_sync_attempts, created_at"
      )
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    recent_rows = Array.isArray(data) ? data : [];
    columns_readable = true;
  } catch (err) {
    load_error = err?.message || "message_events_query_failed";
    // If error mentions missing column, columns haven't been migrated.
    if (
      String(err?.message ?? "").toLowerCase().includes("column") ||
      String(err?.message ?? "").toLowerCase().includes("podio_sync")
    ) {
      columns_readable = false;
    }
  }

  // ── 2. Latest failed sync rows ───────────────────────────────────────────

  let failed_rows = [];
  let failed_error = null;

  try {
    const { data, error } = await supabase
      .from("message_events")
      .select(
        "id, message_event_key, event_type, direction, " +
        "podio_sync_error, podio_sync_attempts, podio_sync_status, created_at"
      )
      .eq("podio_sync_status", "failed")
      .order("created_at", { ascending: false })
      .limit(10);

    if (error) throw error;
    failed_rows = Array.isArray(data) ? data : [];
  } catch (err) {
    failed_error = err?.message || "failed_query_failed";
  }

  // ── 3. Latest pending sync rows ──────────────────────────────────────────

  let pending_rows = [];
  let pending_error = null;

  try {
    const { data, error } = await supabase
      .from("message_events")
      .select(
        "id, message_event_key, event_type, direction, " +
        "podio_sync_status, podio_sync_attempts, created_at"
      )
      .or("podio_sync_status.eq.pending,podio_sync_status.is.null")
      .in("event_type", SYNCABLE_EVENT_TYPES)
      .order("created_at", { ascending: false })
      .limit(10);

    if (error) throw error;
    pending_rows = Array.isArray(data) ? data : [];
  } catch (err) {
    pending_error = err?.message || "pending_query_failed";
  }

  // ── 4. Group counts ──────────────────────────────────────────────────────

  const by_event_type = groupBy(recent_rows, "event_type");
  const by_direction = groupBy(recent_rows, "direction");
  const by_podio_sync_status = groupBy(recent_rows, "podio_sync_status");

  // ── 5. Map recent rows to diagnostic objects ─────────────────────────────

  const message_events_recent = recent_rows.map((row) => ({
    id: row.id ?? null,
    message_event_key: row.message_event_key ?? null,
    direction: row.direction ?? null,
    event_type: row.event_type ?? null,
    message_body_present: Boolean(clean(row.message_body)),
    podio_sync_status: row.podio_sync_status ?? null,
    podio_message_event_id: row.podio_message_event_id ?? null,
    podio_synced_at: row.podio_synced_at ?? null,
    podio_sync_error: row.podio_sync_error ?? null,
    podio_sync_attempts: row.podio_sync_attempts ?? 0,
    created_at: row.created_at ?? null,
  }));

  const latest_failed_errors = failed_rows.map((row) => ({
    id: row.id ?? null,
    message_event_key: row.message_event_key ?? null,
    event_type: row.event_type ?? null,
    direction: row.direction ?? null,
    podio_sync_error: row.podio_sync_error ?? null,
    podio_sync_attempts: row.podio_sync_attempts ?? 0,
    created_at: row.created_at ?? null,
  }));

  const latest_pending_rows = pending_rows.map((row) => ({
    id: row.id ?? null,
    message_event_key: row.message_event_key ?? null,
    event_type: row.event_type ?? null,
    direction: row.direction ?? null,
    podio_sync_status: row.podio_sync_status ?? null,
    podio_sync_attempts: row.podio_sync_attempts ?? 0,
    created_at: row.created_at ?? null,
  }));

  return NextResponse.json({
    ok: true,
    timestamp: new Date().toISOString(),
    limit,
    migration_status: {
      columns_readable,
      load_error,
    },
    syncable_event_types: SYNCABLE_EVENT_TYPES,
    summary: {
      total_rows_loaded: recent_rows.length,
      by_event_type,
      by_direction,
      by_podio_sync_status,
      failed_count: failed_rows.length,
      pending_count: pending_rows.length,
    },
    message_events_recent,
    latest_failed_errors,
    latest_pending_rows,
    errors: {
      load: load_error,
      failed_query: failed_error,
      pending_query: pending_error,
    },
  });
}
