import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import { cockpitError, cockpitOk, cockpitUnauthorized } from "@/lib/cockpit/cockpit-response.js";
import { getSystemFlags } from "@/lib/system-control.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const MONITORED_FLAGS = [
  "outbound_sms_enabled",
  "feeder_enabled",
  "queue_runner_enabled",
  "retry_enabled",
  "auto_reply_enabled",
  "followup_enabled",
  "dashboard_live_enabled",
];

export async function GET(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.health");

  try {
    const flags = await getSystemFlags(MONITORED_FLAGS);
    const all_enabled = MONITORED_FLAGS.every((k) => flags[k]);

    return cockpitOk("cockpit.health", {
      status: all_enabled ? "ok" : "degraded",
      flags,
      checked_at: new Date().toISOString(),
    });
  } catch (err) {
    return cockpitError("cockpit.health", err?.message || "health_check_failed");
  }
}
