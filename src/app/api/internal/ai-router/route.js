import { NextResponse } from "next/server";
import { routeAiRequest } from "@/lib/ai/ai-router.js";
import { requireInternalSecret } from "@/lib/security/require-internal-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request) {
  const auth = requireInternalSecret(request);
  if (!auth.ok) return NextResponse.json({ ok: false, error: auth.error || "unauthorized" }, { status: auth.status || 401 });
  const body = await request.json().catch(() => ({}));
  const result = await routeAiRequest(body);
  return NextResponse.json({ ok: true, route: "internal/ai-router", ...result });
}
