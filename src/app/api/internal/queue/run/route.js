import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleQueueRunRequest } from "@/lib/domain/queue/queue-run-request.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

const logger = child({
  module: "api.internal.queue.run",
});

export async function GET(request) {
  console.log("QUEUE ROUTE HIT");
  try {
    return await handleQueueRunRequest(request, "GET", {
      logger,
      jsonResponse: NextResponse.json,
    });
  } catch (error) {
    captureRouteException(error, { route: "internal/queue/run", subsystem: "queue_runner" });
    throw error;
  }
}

export async function POST(request) {
  return GET(request);
}
