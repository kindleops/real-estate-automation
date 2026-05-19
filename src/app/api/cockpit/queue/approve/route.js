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

const logger = child({ module: "api.cockpit.queue.approve" });

// Approvable statuses: any paused state except paused_review (human quarantine)
const APPROVABLE_STATUSES = new Set([
  "paused_manual_review", "paused_name_missing", "paused_invalid_queue_row",
  "paused_max_retries", "paused_duplicate", "paused_global_lock",
  "paused", "hold", "held",
]);

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.approve");

  try {
    const body = await request.json().catch(() => ({}));
    const queue_item_id = String(body?.queue_item_id ?? "").trim();
    const dry_run = body?.dry_run === true;

    if (!queue_item_id) {
      return cockpitValidationError("cockpit.queue.approve", "missing_queue_item_id");
    }

    // ── System control ──────────────────────────────────────────────
    const flags = await getSystemFlags(["outbound_sms_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.queue.approve", "outbound_sms_enabled");

    // ── Safety gate ─────────────────────────────────────────────────
    const safety = await runQueueMutationSafety(
      queue_item_id,
      { allow_paused_review: false, allow_terminal: false, check_sms: false },
      supabase
    );
    if (!safety.ok) {
      logger.warn("cockpit.queue.approve.blocked", { queue_item_id, reason: safety.reason });
      return cockpitBlocked("cockpit.queue.approve", safety.reason, safety.diagnostics);
    }

    const row = safety.row;
    const current_status = String(row.queue_status || "").toLowerCase();

    if (!APPROVABLE_STATUSES.has(current_status)) {
      return cockpitBlocked("cockpit.queue.approve", "QUEUE_SAFETY_BLOCKED", {
        queue_status: row.queue_status,
        issue: "status_not_approvable",
        approvable: [...APPROVABLE_STATUSES],
      });
    }

    if (dry_run) {
      logger.info("cockpit.queue.approve.dry_run", { queue_item_id });
      return cockpitOk("cockpit.queue.approve", {
        dry_run: true,
        queue_item_id,
        thread_key: row.thread_key,
        previous_status: row.queue_status,
        next_status: "queued",
      });
    }

    // ── Mutation ────────────────────────────────────────────────────
    const now = new Date().toISOString();
    const { error: upd_err } = await supabase
      .from("send_queue")
      .update({
        queue_status: "queued",
        updated_at: now,
        metadata: { ...((row.metadata) || {}), cockpit_approved_at: now, cockpit_previous_status: row.queue_status },
      })
      .eq("id", queue_item_id)
      .eq("queue_status", row.queue_status); // optimistic lock on current status

    if (upd_err) throw upd_err;

    logger.info("cockpit.queue.approve.ok", { queue_item_id, previous_status: row.queue_status });

    return cockpitOk("cockpit.queue.approve", {
      queue_item_id,
      thread_key: row.thread_key,
      previous_status: row.queue_status,
      next_status: "queued",
    });
  } catch (err) {
    logger.error("cockpit.queue.approve.failed", { error: err?.message });
    return cockpitError("cockpit.queue.approve", err?.message || "approve_failed");
  }
}
