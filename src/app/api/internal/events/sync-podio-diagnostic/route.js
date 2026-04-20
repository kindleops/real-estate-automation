import { NextResponse } from "next/server";

import { supabase, hasSupabaseConfig } from "@/lib/supabase/client.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

const SYNCABLE_EVENT_TYPES = ["outbound_send", "outbound_send_failed", "inbound_sms"];
const SYNCABLE_EVENT_TYPES_SET = new Set(SYNCABLE_EVENT_TYPES);
const MAX_SYNC_ATTEMPTS = 3;

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

const NO_CACHE_HEADERS = {
  "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
  "Pragma":        "no-cache",
  "Expires":       "0",
};

/**
 * GET /api/internal/events/sync-podio-diagnostic?limit=20
 * Header: x-internal-api-secret: <INTERNAL_API_SECRET>
 *
 * Returns:
 *  - recent_summary:  counts over the most-recent N rows (any sync status)
 *  - sync_eligibility_summary / sync_eligibility_rows: worker-identical query +
 *    JS eligibility filter so you can see exactly what the worker would pick up
 *  - latest_failed_errors: triage view of failed rows
 *  - column readability check (confirms migration ran)
 */
export async function GET(request) {
  const auth = requireAuth(request);
  if (!auth.authorized) return auth.response;

  if (!hasSupabaseConfig()) {
    return NextResponse.json(
      { ok: false, error: "supabase_not_configured" },
      { status: 503, headers: NO_CACHE_HEADERS }
    );
  }

  const { searchParams } = new URL(request.url);
  const raw_limit = Number(searchParams.get("limit"));
  const limit = Math.min(Number.isFinite(raw_limit) && raw_limit > 0 ? raw_limit : 20, 50);

  // ── 1. Recent message_events (any sync status) ───────────────────────────

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
    if (
      String(err?.message ?? "").toLowerCase().includes("column") ||
      String(err?.message ?? "").toLowerCase().includes("podio_sync")
    ) {
      columns_readable = false;
    }
  }

  // ── 2. Worker-identical eligibility query ─────────────────────────────────
  //
  // Mirrors the exact DB query + JS filter from syncSupabaseMessageEventsToPodio
  // so the diagnostic reflects precisely what the worker will process.

  let sync_eligibility_raw = [];
  let sync_eligibility_error = null;

  try {
    const { data, error } = await supabase
      .from("message_events")
      .select(
        "id, message_event_key, event_type, direction, " +
        "podio_sync_status, podio_sync_attempts, podio_message_event_id, " +
        "podio_synced_at, podio_sync_error, created_at"
      )
      .in("event_type", SYNCABLE_EVENT_TYPES)
      .or("podio_sync_status.eq.pending,podio_sync_status.eq.failed,podio_sync_status.is.null")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    sync_eligibility_raw = Array.isArray(data) ? data : [];
  } catch (err) {
    sync_eligibility_error = err?.message || "sync_eligibility_query_failed";
  }

  // JS-level eligibility filter — identical to the worker's candidate logic.
  const ELIGIBLE_STATUSES = new Set(["pending", "failed"]);

  const after_syncable_filter = sync_eligibility_raw.filter((row) =>
    SYNCABLE_EVENT_TYPES_SET.has(row.event_type)
  );

  const candidates = after_syncable_filter.filter((row) => {
    const status = row.podio_sync_status ?? "pending";
    return (
      ELIGIBLE_STATUSES.has(status) &&
      Number(row.podio_sync_attempts ?? 0) < MAX_SYNC_ATTEMPTS
    );
  });

  const sync_eligibility_rows = candidates.map((row) => ({
    id:                     row.id ?? null,
    message_event_key:      row.message_event_key ?? null,
    event_type:             row.event_type ?? null,
    direction:              row.direction ?? null,
    podio_sync_status:      row.podio_sync_status ?? null,
    podio_sync_attempts:    row.podio_sync_attempts ?? 0,
    podio_message_event_id: row.podio_message_event_id ?? null,
    podio_synced_at:        row.podio_synced_at ?? null,
    podio_sync_error:       row.podio_sync_error ?? null,
    created_at:             row.created_at ?? null,
  }));

  const sync_eligibility_summary = {
    raw_rows_loaded_before_filter:  sync_eligibility_raw.length,
    rows_after_syncable_filter:     after_syncable_filter.length,
    rows_after_attempt_filter:      candidates.length,
    eligible_count:                 candidates.length,
    first_10_eligible_event_keys:   candidates.slice(0, 10).map((r) => r.message_event_key ?? null),
  };

  // ── 3. Latest failed sync rows ───────────────────────────────────────────

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

  // ── 4. Group counts over recent rows (recent_summary) ────────────────────

  const by_event_type      = groupBy(recent_rows, "event_type");
  const by_direction       = groupBy(recent_rows, "direction");
  const by_podio_sync_status = groupBy(recent_rows, "podio_sync_status");

  // ── 5. Map recent rows → diagnostic objects ───────────────────────────────

  const message_events_recent = recent_rows.map((row) => ({
    id:                     row.id ?? null,
    message_event_key:      row.message_event_key ?? null,
    direction:              row.direction ?? null,
    event_type:             row.event_type ?? null,
    message_body_present:   Boolean(clean(row.message_body)),
    podio_sync_status:      row.podio_sync_status ?? null,
    podio_message_event_id: row.podio_message_event_id ?? null,
    podio_synced_at:        row.podio_synced_at ?? null,
    podio_sync_error:       row.podio_sync_error ?? null,
    podio_sync_attempts:    row.podio_sync_attempts ?? 0,
    created_at:             row.created_at ?? null,
  }));

  const latest_failed_errors = failed_rows.map((row) => ({
    id:                  row.id ?? null,
    message_event_key:   row.message_event_key ?? null,
    event_type:          row.event_type ?? null,
    direction:           row.direction ?? null,
    podio_sync_error:    row.podio_sync_error ?? null,
    podio_sync_attempts: row.podio_sync_attempts ?? 0,
    created_at:          row.created_at ?? null,
  }));

  return NextResponse.json(
    {
      ok:        true,
      timestamp: new Date().toISOString(),
      limit,
      migration_status: {
        columns_readable,
        load_error,
      },
      syncable_event_types:     SYNCABLE_EVENT_TYPES,
      max_sync_attempts:        MAX_SYNC_ATTEMPTS,
      // ── recent rows summary (not eligibility) ──
      recent_summary: {
        total_rows_loaded:     recent_rows.length,
        by_event_type,
        by_direction,
        by_podio_sync_status,
        failed_count:          failed_rows.length,
      },
      message_events_recent,
      // ── worker-identical eligibility view ──
      sync_eligibility_summary,
      sync_eligibility_rows,
      sync_eligibility_error,
      latest_failed_errors,
      errors: {
        load:             load_error,
        failed_query:     failed_error,
        eligibility_query: sync_eligibility_error,
      },
    },
    { headers: NO_CACHE_HEADERS }
  );
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
