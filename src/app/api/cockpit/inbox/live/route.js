import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import { cockpitError, cockpitOk, cockpitUnauthorized } from "@/lib/cockpit/cockpit-response.js";
import { getLiveInbox } from "@/lib/domain/inbox/live-inbox-service.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.cockpit.inbox.live" });

export async function GET(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.inbox.live");

  try {
    const { searchParams } = new URL(request.url);
    const data = await getLiveInbox(Object.fromEntries(searchParams.entries()));
    logger.info("cockpit.inbox.live", { thread_count: data?.threads?.length ?? 0 });
    return cockpitOk("cockpit.inbox.live", data);
  } catch (err) {
    logger.error("cockpit.inbox.live.failed", { error: err?.message });
    return cockpitError("cockpit.inbox.live", err?.message || "inbox_live_failed");
  }
}
