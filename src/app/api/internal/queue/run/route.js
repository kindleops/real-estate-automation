import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleQueueRunRequest } from "@/lib/domain/queue/queue-run-request.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

const logger = child({
  module: "api.internal.queue.run",
});

export async function GET(request) {
  console.log("QUEUE ROUTE HIT");
  return handleQueueRunRequest(request, "GET", {
    logger,
    jsonResponse: NextResponse.json,
  });
}

export async function POST(request) {
  return GET(request);
}
