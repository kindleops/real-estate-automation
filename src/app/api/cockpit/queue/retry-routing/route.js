import { requireCockpitAuth } from "@/lib/cockpit/cockpit-auth.js";
import { cockpitNotReady, cockpitUnauthorized } from "@/lib/cockpit/cockpit-response.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// BACKEND_ENDPOINT_NOT_READY
// retry-routing requires re-running the full routing pipeline (phone selection,
// textgrid number assignment, template re-evaluation) against a specific queue row.
// This needs a scoped runSendQueue / routing-only pass that does not yet exist as
// a safe isolated helper. Implementing blindly risks double-sending.
export async function POST(request) {
  const auth = requireCockpitAuth(request);
  if (!auth.authorized) return cockpitUnauthorized("cockpit.queue.retry-routing");

  return cockpitNotReady(
    "cockpit.queue.retry-routing",
    "Requires isolated routing-only pass over a single queue row. " +
    "Safe helper not yet available. Use cockpit/queue/cancel then re-feeder."
  );
}
