import crypto from "node:crypto";

import { NextResponse } from "next/server";

import { getSharedSecretAuthResult } from "@/lib/security/shared-secret.js";

export const OPS_DASHBOARD_SESSION_COOKIE = "ops_dashboard_session";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function isProductionRuntime() {
  return (
    lower(process.env.VERCEL_ENV) === "production" ||
    lower(process.env.NODE_ENV) === "production"
  );
}

export function getOpsDashboardSecret() {
  return clean(process.env.OPS_DASHBOARD_SECRET);
}

export function buildOpsDashboardSessionToken(secret = getOpsDashboardSecret()) {
  const normalized_secret = clean(secret);
  if (!normalized_secret) return "";

  return crypto
    .createHash("sha256")
    .update(`ops-dashboard:${normalized_secret}`, "utf8")
    .digest("hex");
}

export function hasValidOpsDashboardSessionToken(token, secret = getOpsDashboardSecret()) {
  return clean(token) && clean(token) === buildOpsDashboardSessionToken(secret);
}

export function getOpsDashboardPageGate() {
  const secret = getOpsDashboardSecret();

  if (!secret) {
    if (isProductionRuntime()) {
      return {
        ok: false,
        required: true,
        reason: "missing_ops_dashboard_secret",
      };
    }

    return {
      ok: true,
      required: false,
      reason: "ops_dashboard_secret_not_configured",
    };
  }

  return {
    ok: true,
    required: true,
    reason: "ops_dashboard_secret_configured",
  };
}

export function requireOpsDashboardAuth(request, logger = null) {
  const secret = getOpsDashboardSecret();
  const expected_cookie_token = buildOpsDashboardSessionToken(secret);
  const auth = getSharedSecretAuthResult(request, {
    env_name: "OPS_DASHBOARD_SECRET",
    header_names: ["x-ops-dashboard-secret"],
    cookie_names: expected_cookie_token ? [OPS_DASHBOARD_SESSION_COOKIE] : [],
    expected_token: null,
  });

  if (!auth.ok && auth.reason?.startsWith("invalid_ops_dashboard_secret_token")) {
    const cookie_value = clean(
      request?.cookies?.get(OPS_DASHBOARD_SESSION_COOKIE)?.value
    );

    if (hasValidOpsDashboardSessionToken(cookie_value, secret)) {
      return {
        authorized: true,
        auth: {
          ok: true,
          status: 200,
          reason: "authorized",
          required: true,
          authenticated: true,
          via: `cookie:${OPS_DASHBOARD_SESSION_COOKIE}`,
        },
        response: null,
      };
    }
  }

  if (auth.ok) {
    return {
      authorized: true,
      auth,
      response: null,
    };
  }

  logger?.warn?.("ops_dashboard_auth.rejected", {
    reason: auth.reason,
    via: auth.via,
  });

  return {
    authorized: false,
    auth,
    response: NextResponse.json(
      {
        ok: false,
        error: auth.reason,
      },
      { status: auth.status || 401 }
    ),
  };
}

export default requireOpsDashboardAuth;
