import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { supabase } from "@/lib/supabase/client.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.internal.email.cockpit" });

function clean(value) {
  return String(value ?? "").trim();
}

function mapCounts(rows = [], key = "status") {
  const counts = {};
  for (const row of rows) {
    const bucket = clean(row?.[key] || "unknown").toLowerCase() || "unknown";
    counts[bucket] = (counts[bucket] || 0) + 1;
  }
  return counts;
}

async function buildCockpit() {
  const [{ data: queue_rows, error: queue_error }, { count: templates_active }, { count: suppression_count }, { data: event_rows }] = await Promise.all([
    supabase.from("email_send_queue").select("status, email_address, use_case, template_key, campaign_key, scheduled_for"),
    supabase.from("email_templates").select("id", { count: "exact", head: true }).eq("is_active", true),
    supabase.from("email_suppression").select("id", { count: "exact", head: true }),
    supabase.from("email_events").select("event_type, created_at").order("created_at", { ascending: false }).limit(500),
  ]);

  if (queue_error) {
    return {
      ok: false,
      reason: "email_queue_read_failed",
      error: clean(queue_error?.message) || null,
    };
  }

  const queue_status_counts = mapCounts(queue_rows || [], "status");
  const event_type_counts = mapCounts(event_rows || [], "event_type");

  return {
    ok: true,
    queue_status_counts,
    event_type_counts,
    queue_total: (queue_rows || []).length,
    active_templates: templates_active || 0,
    suppression_total: suppression_count || 0,
    latest_event_at: event_rows?.[0]?.created_at || null,
  };
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const result = await buildCockpit();
    return NextResponse.json({ ok: result.ok, route: "internal/email/cockpit", result }, { status: result.ok ? 200 : 400 });
  } catch (error) {
    logger.error("email.cockpit.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json({ ok: false, error: "email_cockpit_failed" }, { status: 500 });
  }
}

export async function POST(request) {
  return GET(request);
}
