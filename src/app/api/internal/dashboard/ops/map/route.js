import { NextResponse } from "next/server";

import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";
import { getSystemFlag } from "@/lib/system-control.js";
import { supabase } from "@/lib/supabase/client.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function asNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asLimit(value, fallback = 1500) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(Math.trunc(parsed), 5000);
}

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const dashboard_live_enabled = await getSystemFlag("dashboard_live_enabled");
    if (!dashboard_live_enabled) {
      return NextResponse.json(
        {
          ok: false,
          error: "system_control_disabled",
          flag_key: "dashboard_live_enabled",
          context: "dashboard-map-route",
        },
        { status: 423 }
      );
    }

    const { searchParams } = new URL(request.url);

    const lat_min = asNumber(searchParams.get("lat_min"), null);
    const lat_max = asNumber(searchParams.get("lat_max"), null);
    const lng_min = asNumber(searchParams.get("lng_min"), null);
    const lng_max = asNumber(searchParams.get("lng_max"), null);
    const limit = asLimit(searchParams.get("limit"), 1500);

    let query = supabase
      .from("v_property_map_points")
      .select("property_id,address,city,state,zip,market,lat,lng,status,score,tier,created_at")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (lat_min !== null) query = query.gte("lat", lat_min);
    if (lat_max !== null) query = query.lte("lat", lat_max);
    if (lng_min !== null) query = query.gte("lng", lng_min);
    if (lng_max !== null) query = query.lte("lng", lng_max);

    const { data: points, error } = await query;
    if (error) throw error;

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/ops/map",
      data: {
        generated_at: new Date().toISOString(),
        marker_count: Array.isArray(points) ? points.length : 0,
        points: points || [],
        bounds: {
          lat_min,
          lat_max,
          lng_min,
          lng_max,
        },
      },
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
