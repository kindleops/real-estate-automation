import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitDisabled, cockpitError,
  cockpitNotReady, cockpitOk, cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import {
  checkCanonicalThreadKey, checkNoNegativeClassification,
  checkSmsEnabled, checkAutoReplyEnabled,
} from "@/lib/cockpit/cockpit-safety.js";
import { getSystemFlags } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.inbox.queue_reply" });

// BACKEND_ENDPOINT_NOT_READY for live mutation path.
// The safety pre-flight (auth, flags, thread_key, negative-classification) is
// fully implemented. The template-resolution + queue-insert path is stubbed
// pending a safe cockpit-scoped template pipeline (not yet wired).

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.queue-reply");

  try {
    const body = await request.json().catch(() => ({}));
    const dry_run = body?.dry_run === true;
    const thread_key = String(body?.thread_key ?? "").trim();

    if (!thread_key) {
      return cockpitValidationError("cockpit.inbox.queue-reply", "THREAD_CONTEXT_UNRESOLVED");
    }

    // ── System control ──────────────────────────────────────────────
    const flags = await getSystemFlags(["outbound_sms_enabled", "auto_reply_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.inbox.queue-reply", "outbound_sms_enabled");
    if (!flags.auto_reply_enabled) return cockpitDisabled("cockpit.inbox.queue-reply", "auto_reply_enabled");

    // ── Canonical thread_key ─────────────────────────────────────────
    const tkCheck = checkCanonicalThreadKey(thread_key);
    if (!tkCheck.ok) return cockpitBlocked("cockpit.inbox.queue-reply", tkCheck.reason, tkCheck.diagnostics);

    // ── No negative classification ──────────────────────────────────
    const negCheck = await checkNoNegativeClassification(thread_key, supabase);
    if (!negCheck.ok) {
      logger.warn("cockpit.inbox.queue-reply.blocked_negative", { thread_key, reason: negCheck.reason });
      return cockpitBlocked("cockpit.inbox.queue-reply", negCheck.reason, negCheck.diagnostics);
    }

    logger.info("cockpit.inbox.queue-reply.preflight_passed", { thread_key, dry_run });

    // ── Template pipeline not yet safe for cockpit ──────────────────
    return cockpitNotReady(
      "cockpit.inbox.queue-reply",
      "Pre-flight checks passed. Template resolution + queue insert require " +
      "a cockpit-scoped template pipeline (BACKEND_ENDPOINT_NOT_READY). " +
      "Use cockpit/inbox/send-now with an explicit message_body instead."
    );
  } catch (err) {
    logger.error("cockpit.inbox.queue-reply.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.queue-reply", err?.message || "queue_reply_failed");
  }
}
