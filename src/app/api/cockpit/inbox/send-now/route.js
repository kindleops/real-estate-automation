import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitDisabled, cockpitError,
  cockpitOk, cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import {
  checkCanonicalThreadKey, checkNoNegativeClassification,
  checkNotPausedReview, checkQueueRowExists, loadQueueRow,
} from "@/lib/cockpit/cockpit-safety.js";
import { createInboxSendNowQueueRow } from "@/lib/domain/inbox/send-now-service.js";
import { normalizePhone } from "@/lib/utils/phones.js";
import { getSystemFlags } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.inbox.send_now" });

function clean(v) { return String(v ?? "").trim(); }

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.send-now");

  try {
    const body = await request.json().catch(() => ({}));
    const dry_run = body?.dry_run === true;
    const thread_key = clean(body?.thread_key);
    const message_body = clean(body?.message_body);
    const to_phone = normalizePhone(clean(body?.to_phone_number));

    // ── Validate required fields ────────────────────────────────────
    if (!message_body) {
      return cockpitValidationError("cockpit.inbox.send-now", "TEMPLATE_UNRESOLVED");
    }

    // ── System control ──────────────────────────────────────────────
    const flags = await getSystemFlags(["outbound_sms_enabled", "dashboard_live_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.inbox.send-now", "outbound_sms_enabled");
    if (!flags.dashboard_live_enabled) return cockpitDisabled("cockpit.inbox.send-now", "dashboard_live_enabled");

    // ── Canonical thread_key ─────────────────────────────────────────
    const effective_thread_key = thread_key || to_phone;
    const tkCheck = checkCanonicalThreadKey(effective_thread_key);
    if (!tkCheck.ok) return cockpitBlocked("cockpit.inbox.send-now", tkCheck.reason, tkCheck.diagnostics);

    // ── No negative classification ──────────────────────────────────
    const negCheck = await checkNoNegativeClassification(effective_thread_key, supabase);
    if (!negCheck.ok) {
      logger.warn("cockpit.inbox.send-now.blocked_negative", { thread_key: effective_thread_key, reason: negCheck.reason });
      return cockpitBlocked("cockpit.inbox.send-now", negCheck.reason, negCheck.diagnostics);
    }

    // ── No active paused_review row for this thread ─────────────────
    const { data: active_paused } = await supabase
      .from("send_queue")
      .select("id, queue_status")
      .eq("thread_key", effective_thread_key)
      .eq("queue_status", "paused_review")
      .limit(1)
      .maybeSingle();

    if (active_paused) {
      return cockpitBlocked("cockpit.inbox.send-now", "QUEUE_SAFETY_BLOCKED", {
        issue: "paused_review_row_exists",
        queue_item_id: active_paused.id,
      });
    }

    if (dry_run) {
      logger.info("cockpit.inbox.send-now.dry_run", { thread_key: effective_thread_key });
      return cockpitOk("cockpit.inbox.send-now", {
        dry_run: true,
        thread_key: effective_thread_key,
        message_body_length: message_body.length,
        would_create: true,
      });
    }

    // ── Delegate to existing service (owns validation + insert) ─────
    const result = await createInboxSendNowQueueRow(
      { ...body, thread_key: effective_thread_key },
      { supabase }
    );

    if (!result.ok) {
      logger.warn("cockpit.inbox.send-now.service_blocked", { error: result.error });
      return cockpitBlocked("cockpit.inbox.send-now", result.error || "QUEUE_SAFETY_BLOCKED", {
        service_error: result.error,
      }, result.status || 422);
    }

    logger.info("cockpit.inbox.send-now.ok", { queue_id: result.queue_id, thread_key: effective_thread_key });

    return cockpitOk("cockpit.inbox.send-now", {
      queue_item_id: result.queue_id,
      thread_key: effective_thread_key,
    });
  } catch (err) {
    logger.error("cockpit.inbox.send-now.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.send-now", err?.message || "send_now_failed");
  }
}
