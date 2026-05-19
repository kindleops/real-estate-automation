import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import {
  cockpitBlocked, cockpitDisabled, cockpitError,
  cockpitOk, cockpitUnauthorized, cockpitValidationError,
} from "@/lib/cockpit/cockpit-response.js";
import { checkCanonicalThreadKey } from "@/lib/cockpit/cockpit-safety.js";
import { upsertInboxThreadState } from "@/lib/supabase/sms-engine.js";
import { getSystemFlag } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.inbox.thread_state" });

function clean(v) { return String(v ?? "").trim(); }
function asBoolean(v, fallback = null) {
  if (typeof v === "boolean") return v;
  const s = clean(v).toLowerCase();
  if (["1", "true", "yes"].includes(s)) return true;
  if (["0", "false", "no"].includes(s)) return false;
  return fallback;
}

export async function GET(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.thread-state");

  try {
    const { searchParams } = new URL(request.url);
    const thread_key = clean(searchParams.get("thread_key"));

    const tkCheck = checkCanonicalThreadKey(thread_key);
    if (!tkCheck.ok) return cockpitBlocked("cockpit.inbox.thread-state", tkCheck.reason, tkCheck.diagnostics);

    const { data, error } = await supabase
      .from("inbox_thread_state")
      .select("thread_key,master_owner_id,property_id,is_read,is_archived,read_at,archived_at,updated_at,updated_by")
      .eq("thread_key", thread_key)
      .maybeSingle();

    if (error) throw error;

    return cockpitOk("cockpit.inbox.thread-state", {
      thread_key,
      data: data || { thread_key, is_read: false, is_archived: false },
    });
  } catch (err) {
    logger.error("cockpit.inbox.thread-state.get.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.thread-state", err?.message || "thread_state_get_failed");
  }
}

export async function PATCH(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.thread-state");

  try {
    const dashboard_live_enabled = await getSystemFlag("dashboard_live_enabled");
    if (!dashboard_live_enabled) return cockpitDisabled("cockpit.inbox.thread-state", "dashboard_live_enabled");

    const body = await request.json().catch(() => ({}));
    const thread_key = clean(body?.thread_key);

    const tkCheck = checkCanonicalThreadKey(thread_key);
    if (!tkCheck.ok) return cockpitBlocked("cockpit.inbox.thread-state", tkCheck.reason, tkCheck.diagnostics);

    const is_read = asBoolean(body?.is_read, null);
    const is_archived = asBoolean(body?.is_archived, null);

    if (is_read === null && is_archived === null) {
      return cockpitValidationError("cockpit.inbox.thread-state", "missing_is_read_or_is_archived");
    }

    const result = await upsertInboxThreadState(
      {
        thread_key,
        master_owner_id: clean(body?.master_owner_id) || null,
        property_id: clean(body?.property_id) || null,
        is_read: is_read ?? false,
        is_archived: is_archived ?? false,
        updated_by: clean(body?.updated_by) || "cockpit",
      },
      { supabase }
    );

    logger.info("cockpit.inbox.thread-state.patched", { thread_key, is_read, is_archived });

    return cockpitOk("cockpit.inbox.thread-state", { thread_key, data: result });
  } catch (err) {
    logger.error("cockpit.inbox.thread-state.patch.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.thread-state", err?.message || "thread_state_patch_failed");
  }
}
