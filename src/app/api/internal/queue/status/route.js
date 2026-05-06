import { NextResponse } from "next/server";
import { supabase, hasSupabaseConfig } from "@/lib/supabase/client.js";
import { requireInternalSecret } from "@/lib/security/require-internal-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  const auth = requireInternalSecret(request);
  if (!auth.ok) return NextResponse.json({ ok: false, error: auth.error || "unauthorized" }, { status: auth.status || 401 });
  if (!hasSupabaseConfig()) return NextResponse.json({ ok: false, error: "supabase_not_configured" }, { status: 500 });
  const { data, error } = await supabase.from("send_queue").select("queue_status,type").limit(10000);
  if (error) throw error;
  const counts = {};
  for (const row of data || []) {
    const status = String(row.queue_status || "unknown").toLowerCase();
    const type = String(row.type || "outbound").toLowerCase();
    counts[status] = (counts[status] || 0) + 1;
    counts[`${type}:${status}`] = (counts[`${type}:${status}`] || 0) + 1;
  }
  return NextResponse.json({ ok: true, route: "internal/queue/status", counts });
}
