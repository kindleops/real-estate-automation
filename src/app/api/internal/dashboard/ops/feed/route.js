import { NextResponse } from "next/server";

import { getOpsFeedSnapshot, parseOpsFilters } from "@/lib/dashboard/ops-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const filters = parseOpsFilters(Object.fromEntries(searchParams.entries()));
    const data = await getOpsFeedSnapshot(filters);

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/ops/feed",
      data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/ops/feed",
        error: "ops_dashboard_feed_failed",
        message: error?.message || "Unknown dashboard feed error",
      },
      { status: 500 }
    );
  }
}
