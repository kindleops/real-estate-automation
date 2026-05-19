import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import { cockpitError, cockpitOk, cockpitUnauthorized } from "@/lib/cockpit/cockpit-response.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.queue.status" });

export async function GET(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.status");

  try {
    const { searchParams } = new URL(request.url);
    const limit = Math.min(Math.max(parseInt(searchParams.get("limit") || "200", 10) || 200, 1), 500);

    const { data, error } = await supabase
      .from("send_queue")
      .select("id, queue_status, type, thread_key, to_phone_number, master_owner_id, scheduled_for, created_at, retry_count, is_locked")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;

    const by_status = {};
    for (const row of data || []) {
      const s = String(row.queue_status || "unknown").toLowerCase();
      by_status[s] = (by_status[s] || 0) + 1;
    }

    logger.info("cockpit.queue.status", { limit, total: data?.length ?? 0 });

    return cockpitOk("cockpit.queue.status", {
      total: data?.length ?? 0,
      by_status,
      rows: data ?? [],
    });
  } catch (err) {
    logger.error("cockpit.queue.status.failed", { error: err?.message });
    return cockpitError("cockpit.queue.status", err?.message || "queue_status_failed");
  }
}
