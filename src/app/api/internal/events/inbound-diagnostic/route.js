import { NextResponse } from "next/server";

import { supabase, hasSupabaseConfig } from "@/lib/supabase/client.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
}

function requireAuth(request) {
  return requireSharedSecretAuth(request, null, {
    env_name: "INTERNAL_API_SECRET",
    header_names: ["x-internal-api-secret"],
  });
}

/**
 * Surfaces recent inbound webhook payloads and their stored message_events rows
 * so production body-extraction issues can be diagnosed without secret exposure.
 *
 * GET /api/internal/events/inbound-diagnostic?limit=10
 * Header: x-internal-api-secret: <INTERNAL_API_SECRET>
 *
 * Returns:
 *   - recent_webhook_logs: last N inbound webhook_log rows with payload key/value diagnostics
 *   - recent_message_events: last N inbound message_events rows with body diagnostics
 *   - summary: quick counts and red-flag indicators
 */
export async function GET(request) {
  const auth = requireAuth(request);
  if (!auth.authorized) return auth.response;

  if (!hasSupabaseConfig()) {
    return NextResponse.json({ ok: false, error: "supabase_not_configured" }, { status: 503 });
  }

  const { searchParams } = new URL(request.url);
  const limit = Math.min(
    Number.isFinite(Number(searchParams.get("limit"))) ? Number(searchParams.get("limit")) : 10,
    25
  );

  // ── webhook_log: recent inbound entries ─────────────────────────────────

  let webhook_logs = [];
  let webhook_log_error = null;

  try {
    const { data, error } = await supabase
      .from("webhook_log")
      .select("id, event_type, direction, provider_message_sid, payload, raw_payload, created_at")
      .or("direction.eq.inbound,event_type.eq.inbound")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    webhook_logs = Array.isArray(data) ? data : [];
  } catch (err) {
    webhook_log_error = err?.message || "webhook_log_query_failed";
  }

  // Normalize and extract diagnostic fields from each webhook_log row.
  const body_field_names = ["Body", "body", "MessageBody", "message_body", "Message", "message", "Text", "text", "content"];

  const recent_webhook_logs = webhook_logs.map((row) => {
    const raw = row.payload || row.raw_payload || {};
    const payload_keys = Object.keys(raw);

    const body_fields = {};
    for (const field of body_field_names) {
      const val = raw[field];
      body_fields[field] = val !== undefined ? String(val ?? "").slice(0, 80) || "(empty)" : null;
    }

    const found_body_field = body_field_names.find((f) => clean(raw[f])) || null;
    const found_body_value = found_body_field ? String(raw[found_body_field]).slice(0, 80) : null;

    return {
      id: row.id || null,
      event_type: row.event_type || null,
      direction: row.direction || null,
      provider_message_sid: row.provider_message_sid || null,
      created_at: row.created_at || null,
      payload_keys,
      payload_key_count: payload_keys.length,
      body_fields,
      found_body_field,
      found_body_value,
      has_from: Boolean(clean(raw.From || raw.from || raw.from_phone_number)),
      has_sid: Boolean(
        clean(raw.SmsMessageSid || raw.MessageSid || raw.SmsSid || raw.sid || raw.message_id)
      ),
    };
  });

  // ── message_events: recent inbound entries ───────────────────────────────

  let message_events = [];
  let message_events_error = null;

  try {
    const { data, error } = await supabase
      .from("message_events")
      .select(
        "message_event_key, from_phone_number, to_phone_number, message_body, character_count, created_at, metadata"
      )
      .eq("direction", "inbound")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    message_events = Array.isArray(data) ? data : [];
  } catch (err) {
    message_events_error = err?.message || "message_events_query_failed";
  }

  const recent_message_events = message_events.map((row) => {
    const meta = row.metadata && typeof row.metadata === "object" ? row.metadata : {};
    const body = clean(row.message_body);
    return {
      message_event_key: row.message_event_key || null,
      created_at: row.created_at || null,
      from_phone_number: row.from_phone_number || null,
      to_phone_number: row.to_phone_number || null,
      message_body_present: Boolean(body),
      message_body_length: body.length || null,
      message_body_preview: body.slice(0, 80) || null,
      character_count: row.character_count ?? null,
      body_source: meta.body_source || null,
      body_missing: Boolean(meta.body_missing),
      raw_body_keys: Array.isArray(meta.raw_body_keys) ? meta.raw_body_keys : null,
    };
  });

  // ── summary ──────────────────────────────────────────────────────────────

  const events_with_body = recent_message_events.filter((r) => r.message_body_present).length;
  const events_missing_body = recent_message_events.filter((r) => !r.message_body_present).length;

  const logs_with_known_body_field = recent_webhook_logs.filter(
    (r) => r.found_body_field !== null
  ).length;
  const logs_with_unknown_body_field = recent_webhook_logs.filter(
    (r) => r.found_body_field === null
  ).length;

  // Collect all unique payload key names seen across all webhook_log rows
  const all_payload_keys_seen = [
    ...new Set(recent_webhook_logs.flatMap((r) => r.payload_keys)),
  ].sort();

  return NextResponse.json({
    ok: true,
    timestamp: new Date().toISOString(),
    limit,
    summary: {
      webhook_log_rows: recent_webhook_logs.length,
      message_event_rows: recent_message_events.length,
      events_with_body,
      events_missing_body,
      logs_with_known_body_field,
      logs_with_unknown_body_field,
      all_payload_keys_seen,
    },
    recent_webhook_logs,
    recent_message_events,
    errors: {
      webhook_log: webhook_log_error,
      message_events: message_events_error,
    },
  });
}
