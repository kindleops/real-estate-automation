import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitDisabled, cockpitNotReady,
  cockpitOk, cockpitUnauthorized, cockpitValidationError, cockpitError,
} from "@/lib/cockpit/cockpit-response.js";
import {
  checkCanonicalThreadKey, checkNoNegativeClassification, checkSmsEnabled,
} from "@/lib/cockpit/cockpit-safety.js";
import { getSystemFlags } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.inbox.schedule_reply" });

// BACKEND_ENDPOINT_NOT_READY for live mutation path.
// Pre-flight safety checks are fully implemented. Template resolution is stubbed.

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.schedule-reply");

  try {
    const body = await request.json().catch(() => ({}));
    const dry_run = body?.dry_run === true;
    const thread_key = String(body?.thread_key ?? "").trim();
    const scheduled_for = String(body?.scheduled_for ?? "").trim();

    if (!thread_key) {
      return cockpitValidationError("cockpit.inbox.schedule-reply", "THREAD_CONTEXT_UNRESOLVED");
    }
    if (!scheduled_for || isNaN(Date.parse(scheduled_for))) {
      return cockpitValidationError("cockpit.inbox.schedule-reply", "missing_or_invalid_scheduled_for");
    }

    const flags = await getSystemFlags(["outbound_sms_enabled", "auto_reply_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.inbox.schedule-reply", "outbound_sms_enabled");
    if (!flags.auto_reply_enabled) return cockpitDisabled("cockpit.inbox.schedule-reply", "auto_reply_enabled");

    const tkCheck = checkCanonicalThreadKey(thread_key);
    if (!tkCheck.ok) return cockpitBlocked("cockpit.inbox.schedule-reply", tkCheck.reason, tkCheck.diagnostics);

    const negCheck = await checkNoNegativeClassification(thread_key, supabase);
    if (!negCheck.ok) {
      logger.warn("cockpit.inbox.schedule-reply.blocked_negative", { thread_key });
      return cockpitBlocked("cockpit.inbox.schedule-reply", negCheck.reason, negCheck.diagnostics);
    }

    logger.info("cockpit.inbox.schedule-reply.preflight_passed", { thread_key, scheduled_for, dry_run });

    return cockpitNotReady(
      "cockpit.inbox.schedule-reply",
      "Pre-flight checks passed. Template resolution + scheduled queue insert require " +
      "cockpit-scoped template pipeline (BACKEND_ENDPOINT_NOT_READY). " +
      "Use cockpit/inbox/send-now with scheduled_for + explicit message_body."
    );
  } catch (err) {
    logger.error("cockpit.inbox.schedule-reply.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.schedule-reply", err?.message || "schedule_reply_failed");
  }
}
