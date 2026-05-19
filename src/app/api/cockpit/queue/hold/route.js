import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitError, cockpitOk,
  cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import { checkNotTerminal, checkQueueRowExists, loadQueueRow } from "@/lib/cockpit/cockpit-safety.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.queue.hold" });

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.hold");

  try {
    const body = await request.json().catch(() => ({}));
    const queue_item_id = String(body?.queue_item_id ?? "").trim();
    const dry_run = body?.dry_run === true;
    const note = String(body?.note ?? "").trim() || null;

    if (!queue_item_id) {
      return cockpitValidationError("cockpit.queue.hold", "missing_queue_item_id");
    }

    const row = await loadQueueRow(queue_item_id, supabase);
    const exists = checkQueueRowExists(row, queue_item_id);
    if (!exists.ok) return cockpitBlocked("cockpit.queue.hold", exists.reason, exists.diagnostics);

    const termCheck = checkNotTerminal(row);
    if (!termCheck.ok) return cockpitBlocked("cockpit.queue.hold", termCheck.reason, termCheck.diagnostics);

    if (String(row.queue_status).toLowerCase() === "paused_manual_review") {
      return cockpitOk("cockpit.queue.hold", {
        queue_item_id,
        thread_key: row.thread_key,
        previous_status: row.queue_status,
        next_status: "paused_manual_review",
        note: "already_on_hold",
      });
    }

    if (dry_run) {
      logger.info("cockpit.queue.hold.dry_run", { queue_item_id });
      return cockpitOk("cockpit.queue.hold", {
        dry_run: true,
        queue_item_id,
        thread_key: row.thread_key,
        previous_status: row.queue_status,
        next_status: "paused_manual_review",
      });
    }

    const now = new Date().toISOString();
    const { error: upd_err } = await supabase
      .from("send_queue")
      .update({
        queue_status: "paused_manual_review",
        updated_at: now,
        metadata: { ...(row.metadata || {}), cockpit_held_at: now, cockpit_hold_note: note },
      })
      .eq("id", queue_item_id)
      .not("queue_status", "in", '("cancelled","sent","failed")');

    if (upd_err) throw upd_err;

    logger.info("cockpit.queue.hold.ok", { queue_item_id, previous_status: row.queue_status });

    return cockpitOk("cockpit.queue.hold", {
      queue_item_id,
      thread_key: row.thread_key,
      previous_status: row.queue_status,
      next_status: "paused_manual_review",
    });
  } catch (err) {
    logger.error("cockpit.queue.hold.failed", { error: err?.message });
    return cockpitError("cockpit.queue.hold", err?.message || "hold_failed");
  }
}
