import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleDirectSendRequestData } from "@/lib/domain/outbound/direct-send-request.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.outbound.direct_send",
});

export async function GET(request) {
  const { status, payload } = await handleDirectSendRequestData(request, "GET", {
    logger,
  });
  return NextResponse.json(payload, { status });
}

export async function POST(request) {
  const { status, payload } = await handleDirectSendRequestData(request, "POST", {
    logger,
  });
  return NextResponse.json(payload, { status });
}
