import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitError, cockpitOk,
  cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import { checkCancel, checkQueueRowExists, loadQueueRow } from "@/lib/cockpit/cockpit-safety.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.queue.cancel" });

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.cancel");

  try {
    const body = await request.json().catch(() => ({}));
    const queue_item_id = String(body?.queue_item_id ?? "").trim();
    const dry_run = body?.dry_run === true;
    const reason = String(body?.reason ?? "cockpit_cancel").trim() || "cockpit_cancel";

    if (!queue_item_id) {
      return cockpitValidationError("cockpit.queue.cancel", "missing_queue_item_id");
    }

    const row = await loadQueueRow(queue_item_id, supabase);
    const exists = checkQueueRowExists(row, queue_item_id);
    if (!exists.ok) return cockpitBlocked("cockpit.queue.cancel", exists.reason, exists.diagnostics);

    const cancelCheck = checkCancel(row);
    if (!cancelCheck.ok) return cockpitBlocked("cockpit.queue.cancel", cancelCheck.reason, cancelCheck.diagnostics);

    if (dry_run) {
      logger.info("cockpit.queue.cancel.dry_run", { queue_item_id });
      return cockpitOk("cockpit.queue.cancel", {
        dry_run: true,
        queue_item_id,
        thread_key: row.thread_key,
        previous_status: row.queue_status,
        next_status: "cancelled",
      });
    }

    const now = new Date().toISOString();
    const { error: upd_err } = await supabase
      .from("send_queue")
      .update({
        queue_status: "cancelled",
        updated_at: now,
        metadata: { ...(row.metadata || {}), cockpit_cancelled_at: now, cockpit_cancel_reason: reason },
      })
      .eq("id", queue_item_id)
      .neq("queue_status", "cancelled");

    if (upd_err) throw upd_err;

    logger.info("cockpit.queue.cancel.ok", { queue_item_id, previous_status: row.queue_status });

    return cockpitOk("cockpit.queue.cancel", {
      queue_item_id,
      thread_key: row.thread_key,
      previous_status: row.queue_status,
      next_status: "cancelled",
    });
  } catch (err) {
    logger.error("cockpit.queue.cancel.failed", { error: err?.message });
    return cockpitError("cockpit.queue.cancel", err?.message || "cancel_failed");
  }
}
