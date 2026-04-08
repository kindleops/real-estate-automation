import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleSendNowRequestData } from "@/lib/domain/outbound/send-now-request.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.outbound.send_now",
});

export async function GET(request) {
  const { status, payload } = await handleSendNowRequestData(request, "GET", {
    logger,
  });
  return NextResponse.json(payload, { status });
}

export async function POST(request) {
  const { status, payload } = await handleSendNowRequestData(request, "POST", {
    logger,
  });
  return NextResponse.json(payload, { status });
}
