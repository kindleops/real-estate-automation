import { NextResponse } from "next/server";

import { getOpsFilterOptions } from "@/lib/dashboard/ops-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const data = await getOpsFilterOptions();

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/ops/filters",
      data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/ops/filters",
        error: "ops_dashboard_filters_failed",
        message: error?.message || "Unknown dashboard filters error",
      },
      { status: 500 }
    );
  }
}
