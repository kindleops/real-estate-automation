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

const logger = child({ module: "api.cockpit.queue.retry" });

const RETRIABLE_STATUSES = new Set([
  "failed", "paused_max_retries", "paused_invalid_queue_row",
  "paused_name_missing", "paused_duplicate", "paused_global_lock",
  "paused_manual_review", "paused",
]);

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.retry");

  try {
    const body = await request.json().catch(() => ({}));
    const queue_item_id = String(body?.queue_item_id ?? "").trim();
    const dry_run = body?.dry_run === true;

    if (!queue_item_id) {
      return cockpitValidationError("cockpit.queue.retry", "missing_queue_item_id");
    }

    // ── System control ──────────────────────────────────────────────
    const flags = await getSystemFlags(["outbound_sms_enabled", "retry_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.queue.retry", "outbound_sms_enabled");
    if (!flags.retry_enabled) return cockpitDisabled("cockpit.queue.retry", "retry_enabled");

    // ── Safety gate ─────────────────────────────────────────────────
    const safety = await runQueueMutationSafety(
      queue_item_id,
      { allow_paused_review: false, allow_terminal: true, check_sms: false },
      supabase
    );
    if (!safety.ok) {
      logger.warn("cockpit.queue.retry.blocked", { queue_item_id, reason: safety.reason });
      return cockpitBlocked("cockpit.queue.retry", safety.reason, safety.diagnostics);
    }

    const row = safety.row;
    const current_status = String(row.queue_status || "").toLowerCase();

    if (!RETRIABLE_STATUSES.has(current_status)) {
      return cockpitBlocked("cockpit.queue.retry", "QUEUE_SAFETY_BLOCKED", {
        queue_status: row.queue_status,
        issue: "status_not_retriable",
        retriable: [...RETRIABLE_STATUSES],
      });
    }

    if (dry_run) {
      logger.info("cockpit.queue.retry.dry_run", { queue_item_id });
      return cockpitOk("cockpit.queue.retry", {
        dry_run: true,
        queue_item_id,
        thread_key: row.thread_key,
        previous_status: row.queue_status,
        next_status: "queued",
        retry_count_reset: true,
      });
    }

    const now = new Date().toISOString();
    const { error: upd_err } = await supabase
      .from("send_queue")
      .update({
        queue_status: "queued",
        retry_count: 0,
        is_locked: false,
        lock_token: null,
        next_retry_at: null,
        updated_at: now,
        metadata: { ...(row.metadata || {}), cockpit_retried_at: now, cockpit_previous_status: row.queue_status },
      })
      .eq("id", queue_item_id);

    if (upd_err) throw upd_err;

    logger.info("cockpit.queue.retry.ok", { queue_item_id, previous_status: row.queue_status });

    return cockpitOk("cockpit.queue.retry", {
      queue_item_id,
      thread_key: row.thread_key,
      previous_status: row.queue_status,
      next_status: "queued",
      retry_count_reset: true,
    });
  } catch (err) {
    logger.error("cockpit.queue.retry.failed", { error: err?.message });
    return cockpitError("cockpit.queue.retry", err?.message || "retry_failed");
  }
}
