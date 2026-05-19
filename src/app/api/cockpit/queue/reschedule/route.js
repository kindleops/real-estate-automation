import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitDisabled, cockpitError,
  cockpitOk, cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import { runQueueMutationSafety } from "@/lib/cockpit/cockpit-safety.js";
import { getSystemFlags } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.queue.reschedule" });

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.reschedule");

  try {
    const body = await request.json().catch(() => ({}));
    const queue_item_id = String(body?.queue_item_id ?? "").trim();
    const scheduled_for = String(body?.scheduled_for ?? "").trim();
    const dry_run = body?.dry_run === true;

    if (!queue_item_id) {
      return cockpitValidationError("cockpit.queue.reschedule", "missing_queue_item_id");
    }
    if (!scheduled_for || isNaN(Date.parse(scheduled_for))) {
      return cockpitValidationError("cockpit.queue.reschedule", "missing_or_invalid_scheduled_for");
    }
    // Reject schedules in the past (beyond 60s tolerance)
    if (Date.parse(scheduled_for) < Date.now() - 60_000) {
      return cockpitValidationError("cockpit.queue.reschedule", "scheduled_for_in_past");
    }

    const flags = await getSystemFlags(["outbound_sms_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.queue.reschedule", "outbound_sms_enabled");

    const safety = await runQueueMutationSafety(
      queue_item_id,
      { allow_paused_review: false, allow_terminal: false, check_sms: false },
      supabase
    );
    if (!safety.ok) {
      logger.warn("cockpit.queue.reschedule.blocked", { queue_item_id, reason: safety.reason });
      return cockpitBlocked("cockpit.queue.reschedule", safety.reason, safety.diagnostics);
    }

    const row = safety.row;

    if (dry_run) {
      logger.info("cockpit.queue.reschedule.dry_run", { queue_item_id, scheduled_for });
      return cockpitOk("cockpit.queue.reschedule", {
        dry_run: true,
        queue_item_id,
        thread_key: row.thread_key,
        previous_scheduled_for: row.scheduled_for,
        next_scheduled_for: scheduled_for,
        next_status: "scheduled",
      });
    }

    const now = new Date().toISOString();
    const { error: upd_err } = await supabase
      .from("send_queue")
      .update({
        queue_status: "scheduled",
        scheduled_for,
        scheduled_for_utc: scheduled_for,
        is_locked: false,
        lock_token: null,
        updated_at: now,
        metadata: {
          ...(row.metadata || {}),
          cockpit_rescheduled_at: now,
          cockpit_previous_scheduled_for: row.scheduled_for,
        },
      })
      .eq("id", queue_item_id)
      .not("queue_status", "in", '("cancelled","sent","failed")');

    if (upd_err) throw upd_err;

    logger.info("cockpit.queue.reschedule.ok", { queue_item_id, scheduled_for });

    return cockpitOk("cockpit.queue.reschedule", {
      queue_item_id,
      thread_key: row.thread_key,
      previous_scheduled_for: row.scheduled_for,
      next_scheduled_for: scheduled_for,
      next_status: "scheduled",
    });
  } catch (err) {
    logger.error("cockpit.queue.reschedule.failed", { error: err?.message });
    return cockpitError("cockpit.queue.reschedule", err?.message || "reschedule_failed");
  }
}
