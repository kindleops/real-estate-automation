import { NextResponse } from "next/server";

import { loadThreadContext } from "@/lib/domain/inbox/thread-context-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";
import { supabase } from "@/lib/supabase/client.js";
import { getSystemFlag } from "@/lib/system-control.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
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
          context: "internal-inbox-thread-context",
        },
        { status: 423 }
      );
    }

    const { searchParams } = new URL(request.url);
    const thread_key = clean(searchParams.get("thread_key"));
    if (!thread_key) {
      return NextResponse.json({ ok: false, error: "missing_thread_key" }, { status: 400 });
    }

    const contextPayload = await loadThreadContext({ thread_key, supabase });

    return NextResponse.json({
      ok: true,
      route: "internal/inbox/thread-context",
      ...contextPayload,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/inbox/thread-context",
        error: "inbox_thread_context_failed",
        message: error?.message || "Unknown inbox thread context error",
      },
      { status: 500 }
    );
  }
}
