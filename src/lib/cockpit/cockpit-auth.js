import { NextResponse } from "next/server";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";

function clean(value) {
  return String(value ?? "").trim();
}

function getInternalApiSecret() {
  return clean(process.env.INTERNAL_API_SECRET);
}

function checkInternalApiSecret(request) {
  const secret = getInternalApiSecret();
  if (!secret) return null;
  const header = clean(request?.headers?.get("x-internal-api-secret"));
  const bearer = clean(request?.headers?.get("authorization")).replace(/^Bearer\s+/i, "");
  const token = header || bearer;
  if (!token) return null;
  return token === secret ? "internal_api_secret" : null;
}

/**
 * Cockpit auth: accepts ops-dashboard session/header OR INTERNAL_API_SECRET.
 * Both are backend-owned — neither is exposed to the browser.
 */
export function requireCockpitAuth(request, logger = null) {
  // Try ops-dashboard auth first (session cookie or x-ops-dashboard-secret header)
  const ops = requireOpsDashboardAuth(request, logger);
  if (ops.authorized) return ops;

  // Also accept INTERNAL_API_SECRET for machine-to-machine calls
  const via = checkInternalApiSecret(request);
  if (via) {
    return {
      authorized: true,
      auth: { ok: true, authenticated: true, required: true, reason: "authorized", via },
      response: null,
    };
  }

  logger?.warn?.("cockpit_auth.rejected", { reason: ops.auth?.reason });
  return {
    authorized: false,
    auth: ops.auth,
    response: NextResponse.json({ ok: false, error: "cockpit_unauthorized" }, { status: 401 }),
  };
}
