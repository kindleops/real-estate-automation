import { NextResponse } from "next/server";

import { getOpsQueueSnapshot, parseOpsFilters } from "@/lib/dashboard/ops-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const filters = parseOpsFilters(Object.fromEntries(searchParams.entries()));
    const data = await getOpsQueueSnapshot(filters);

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/ops/queue",
      data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/ops/queue",
        error: "ops_dashboard_queue_failed",
        message: error?.message || "Unknown dashboard queue error",
      },
      { status: 500 }
    );
  }
}
