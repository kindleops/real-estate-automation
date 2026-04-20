import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleTextgridDeliveryRequest } from "@/lib/webhooks/textgrid-delivery-request.js";
import { captureRouteException } from "@/lib/monitoring/sentry.js";

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
  try {
    const { status, payload } = await handleTextgridDeliveryRequest(request, {
      logger,
    });

    return NextResponse.json(payload, { status });
  } catch (error) {
    captureRouteException(error, {
      route: "webhooks/textgrid/delivery",
      subsystem: "textgrid_delivery",
    });
    throw error;
  }
}
