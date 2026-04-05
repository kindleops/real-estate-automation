import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleQueueForceDueRequest } from "@/lib/domain/queue/queue-force-due-request.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.queue.force-due",
});

export async function GET(request) {
  return handleQueueForceDueRequest(request, "GET", {
    logger,
    jsonResponse: NextResponse.json,
  });
}

export async function POST(request) {
  return handleQueueForceDueRequest(request, "POST", {
    logger,
    jsonResponse: NextResponse.json,
  });
}
