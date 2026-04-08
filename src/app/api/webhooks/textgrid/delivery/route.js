import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleTextgridDeliveryRequest } from "@/lib/webhooks/textgrid-delivery-request.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.textgrid.delivery",
});

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/textgrid/delivery",
    status: "listening",
  });
}

export async function POST(request) {
  const { status, payload } = await handleTextgridDeliveryRequest(request, {
    logger,
  });

  return NextResponse.json(payload, { status });
}
