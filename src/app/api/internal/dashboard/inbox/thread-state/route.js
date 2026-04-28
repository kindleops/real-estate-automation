import { NextResponse } from "next/server";

import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";
import { supabase } from "@/lib/supabase/client.js";
import { getSystemFlag } from "@/lib/system-control.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["1", "true", "yes"].includes(normalized)) return true;
  if (["0", "false", "no"].includes(normalized)) return false;
  return fallback;
}

function parseThreadKey(input = {}) {
  const explicit_key = clean(input.thread_key);
  if (explicit_key) return explicit_key;

  const master_owner_id = clean(input.master_owner_id);
  const property_id = clean(input.property_id);
  const from_phone = clean(input.from_phone);
  const to_phone = clean(input.to_phone);

  if (!from_phone || !to_phone) return "";
  const left = from_phone < to_phone ? from_phone : to_phone;
  const right = from_phone < to_phone ? to_phone : from_phone;

  return `${master_owner_id}:${property_id}:${left}:${right}`;
}

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const dashboard_live_enabled = await getSystemFlag("dashboard_live_enabled");
    if (!dashboard_live_enabled) {
      return NextResponse.json(
        {
          ok: false,
          error: "system_control_disabled",
          flag_key: "dashboard_live_enabled",
          context: "dashboard-inbox-thread-state",
        },
        { status: 423 }
      );
    }

    const { searchParams } = new URL(request.url);
    const thread_key = parseThreadKey(Object.fromEntries(searchParams.entries()));
    if (!thread_key) {
      return NextResponse.json(
        { ok: false, error: "missing_thread_key" },
        { status: 400 }
      );
    }

    const { data, error } = await supabase
      .from("inbox_thread_state")
      .select("thread_key,master_owner_id,property_id,is_read,is_archived,read_at,archived_at,updated_at,updated_by")
      .eq("thread_key", thread_key)
      .maybeSingle();

    if (error) throw error;

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/inbox/thread-state",
      data: data || {
        thread_key,
        is_read: false,
        is_archived: false,
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/inbox/thread-state",
        error: "dashboard_inbox_thread_state_get_failed",
        message: error?.message || "Unknown inbox thread state error",
      },
      { status: 500 }
    );
  }
}

export async function PUT(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const dashboard_live_enabled = await getSystemFlag("dashboard_live_enabled");
    if (!dashboard_live_enabled) {
      return NextResponse.json(
        {
          ok: false,
          error: "system_control_disabled",
          flag_key: "dashboard_live_enabled",
          context: "dashboard-inbox-thread-state",
        },
        { status: 423 }
      );
    }

    const body = await request.json().catch(() => ({}));
    const thread_key = parseThreadKey(body || {});
    if (!thread_key) {
      return NextResponse.json(
        { ok: false, error: "missing_thread_key" },
        { status: 400 }
      );
    }

    const next_is_read = asBoolean(body?.is_read, false);
    const next_is_archived = asBoolean(body?.is_archived, false);
    const now = new Date().toISOString();

    const payload = {
      thread_key,
      master_owner_id: clean(body?.master_owner_id) || null,
      property_id: clean(body?.property_id) || null,
      is_read: next_is_read,
      is_archived: next_is_archived,
      read_at: next_is_read ? now : null,
      archived_at: next_is_archived ? now : null,
      updated_by: clean(body?.updated_by) || clean(auth.auth?.identity_label) || "dashboard",
      updated_at: now,
    };

    const { data, error } = await supabase
      .from("inbox_thread_state")
      .upsert(payload, { onConflict: "thread_key" })
      .select("thread_key,master_owner_id,property_id,is_read,is_archived,read_at,archived_at,updated_at,updated_by")
      .maybeSingle();

    if (error) throw error;

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/inbox/thread-state",
      data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/inbox/thread-state",
        error: "dashboard_inbox_thread_state_put_failed",
        message: error?.message || "Unknown inbox thread state update error",
      },
      { status: 500 }
    );
  }
}
