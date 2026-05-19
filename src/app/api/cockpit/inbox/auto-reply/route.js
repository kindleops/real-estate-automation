import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitDisabled, cockpitError, cockpitNotReady,
  cockpitOk, cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import {
  checkCanonicalThreadKey, checkNoNegativeClassification,
} from "@/lib/cockpit/cockpit-safety.js";
import { getSystemFlags } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.inbox.auto_reply" });

// BACKEND_ENDPOINT_NOT_READY for live mutation path.
// Pre-flight safety checks are fully implemented. The seller-stage-reply pipeline
// requires a classification context that must be resolved from the latest inbound
// message event — this cockpit path needs a safe replay adapter (not yet built).

export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.auto-reply");

  try {
    const body = await request.json().catch(() => ({}));
    const dry_run = body?.dry_run === true;
    const thread_key = String(body?.thread_key ?? "").trim();

    if (!thread_key) {
      return cockpitValidationError("cockpit.inbox.auto-reply", "THREAD_CONTEXT_UNRESOLVED");
    }

    // ── System control — all three flags required ────────────────────
    const flags = await getSystemFlags(["outbound_sms_enabled", "auto_reply_enabled", "followup_enabled"]);
    if (!flags.outbound_sms_enabled) return cockpitDisabled("cockpit.inbox.auto-reply", "outbound_sms_enabled");
    if (!flags.auto_reply_enabled) return cockpitDisabled("cockpit.inbox.auto-reply", "auto_reply_enabled");
    if (!flags.followup_enabled) return cockpitDisabled("cockpit.inbox.auto-reply", "followup_enabled");

    // ── Canonical thread_key ─────────────────────────────────────────
    const tkCheck = checkCanonicalThreadKey(thread_key);
    if (!tkCheck.ok) return cockpitBlocked("cockpit.inbox.auto-reply", tkCheck.reason, tkCheck.diagnostics);

    // ── Hard veto: negative/wrong-number/DNC ────────────────────────
    const negCheck = await checkNoNegativeClassification(thread_key, supabase);
    if (!negCheck.ok) {
      logger.warn("cockpit.inbox.auto-reply.blocked_negative", { thread_key, reason: negCheck.reason });
      return cockpitBlocked("cockpit.inbox.auto-reply", negCheck.reason, negCheck.diagnostics);
    }

    logger.info("cockpit.inbox.auto-reply.preflight_passed", { thread_key, dry_run });

    // ── Pipeline not yet safe for cockpit-triggered replay ───────────
    return cockpitNotReady(
      "cockpit.inbox.auto-reply",
      "Pre-flight checks passed (no wrong-number/DNC, system flags ok, canonical thread_key). " +
      "Seller-stage-reply pipeline requires classification context replay adapter " +
      "(BACKEND_ENDPOINT_NOT_READY). Use cockpit/inbox/send-now for manual sends."
    );
  } catch (err) {
    logger.error("cockpit.inbox.auto-reply.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.auto-reply", err?.message || "auto_reply_failed");
  }
}
