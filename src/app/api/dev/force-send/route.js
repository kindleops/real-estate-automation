import { requireDevRouteAccess } from "@/lib/security/dev-route-guard.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
}

function isProductionRuntime(env = process.env) {
  return clean(env.NODE_ENV) === "production" || clean(env.VERCEL_ENV) === "production";
}

export async function handleDevForceSendRequest(request, deps = {}) {
  const env = deps.env || process.env;
  const logger = deps.logger || console;

  if (isProductionRuntime(env)) {
    logger.warn?.("dev_force_send_blocked_in_production", {
      node_env: clean(env.NODE_ENV) || null,
      vercel_env: clean(env.VERCEL_ENV) || null,
    });
    return new Response(null, { status: 404 });
  }

  const require_dev_route_access = deps.requireDevRouteAccess || requireDevRouteAccess;
  const denied = require_dev_route_access(request);

  if (denied) {
    return denied;
  }

  try {
    const send_textgrid_sms =
      deps.sendTextgridSMS ||
      (await import("@/lib/providers/textgrid.js")).sendTextgridSMS;

    const result = await send_textgrid_sms({
      from: "+16128060495",
      to: "+16127433952",
      body: "🔥 FORCE SEND TEST",
    });

    logger.log?.("FORCE SEND RESULT:", result);

    return Response.json({
      success: true,
      result,
    });
  } catch (err) {
    logger.error?.("FORCE SEND ERROR:", err);

    return Response.json({
      success: false,
      error: err?.message || "Unknown error",
    });
  }
}

export async function GET(request) {
  return handleDevForceSendRequest(request);
}
