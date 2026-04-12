import { NextResponse } from "next/server";
import { getNexusDashboard } from "@/lib/dashboard/nexus/nexus-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin":  process.env.NEXUS_DASHBOARD_ORIGIN || "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, x-ops-dashboard-secret",
};

export async function OPTIONS() {
  return new Response(null, { status: 204, headers: CORS_HEADERS });
}

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) {
      return NextResponse.json(
        { ok: false, error: auth.auth?.reason ?? "unauthorized" },
        { status: 401, headers: CORS_HEADERS }
      );
    }

    const data = await getNexusDashboard();

    return NextResponse.json(
      { ok: true, route: "internal/dashboard/nexus", data },
      { headers: CORS_HEADERS }
    );
  } catch (error) {
    return NextResponse.json(
      {
        ok:      false,
        route:   "internal/dashboard/nexus",
        error:   "nexus_dashboard_failed",
        message: error?.message ?? "Unknown nexus dashboard error",
      },
      { status: 500, headers: CORS_HEADERS }
    );
  }
}
