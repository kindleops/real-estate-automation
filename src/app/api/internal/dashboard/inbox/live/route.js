import { NextResponse } from "next/server";
import { getLiveInbox } from "@/lib/domain/inbox/live-inbox-service.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  const auth = requireOpsDashboardAuth(request);
  if (!auth.authorized) return auth.response;
  const { searchParams } = new URL(request.url);
  const data = await getLiveInbox(Object.fromEntries(searchParams.entries()));
  return NextResponse.json({ ok: true, route: "internal/dashboard/inbox/live", ...data });
}
