import { NextResponse } from "next/server";

import { getOpsMapSnapshot, parseOpsFilters } from "@/lib/dashboard/ops-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const filters = parseOpsFilters(Object.fromEntries(searchParams.entries()));
    const data = await getOpsMapSnapshot(filters);

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/ops/map",
      data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/ops/map",
        error: "ops_dashboard_map_failed",
        message: error?.message || "Unknown dashboard map error",
      },
      { status: 500 }
    );
  }
}
