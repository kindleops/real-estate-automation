import { NextResponse } from "next/server";

import { getOpsFeederSnapshot, parseOpsFilters } from "@/lib/dashboard/ops-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const filters = parseOpsFilters(Object.fromEntries(searchParams.entries()));
    const data = await getOpsFeederSnapshot(filters);

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/ops/feeder",
      data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/ops/feeder",
        error: "ops_dashboard_feeder_failed",
        message: error?.message || "Unknown dashboard feeder error",
      },
      { status: 500 }
    );
  }
}
