import crypto from "node:crypto";

import { NextResponse } from "next/server";

import {
  OPS_DASHBOARD_SESSION_COOKIE,
  buildOpsDashboardSessionToken,
  getOpsDashboardSecret,
} from "@/lib/security/dashboard-auth.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const DEFAULT_REDIRECT_PATH = "/dashboard/ops";
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 12;

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

function safeEqual(left, right) {
  const left_buffer = Buffer.from(clean(left), "utf8");
  const right_buffer = Buffer.from(clean(right), "utf8");

  if (left_buffer.length === 0 || right_buffer.length === 0) {
    return false;
  }

  if (left_buffer.length !== right_buffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(left_buffer, right_buffer);
}

function getResponseMode(request) {
  const accept = lower(request.headers.get("accept"));
  const content_type = lower(request.headers.get("content-type"));

  if (content_type.includes("application/json")) return "json";
  if (accept.includes("application/json")) return "json";
  return "form";
}

async function parseRequestBody(request) {
  const content_type = lower(request.headers.get("content-type"));

  if (content_type.includes("application/json")) {
    return request.json().catch(() => ({}));
  }

  const form = await request.formData().catch(() => null);
  return form ? Object.fromEntries(form.entries()) : {};
}

function buildRedirectResponse(request, redirect_to, query = "") {
  const url = new URL(clean(redirect_to) || DEFAULT_REDIRECT_PATH, request.url);
  if (query) {
    url.search = query;
  }
  return NextResponse.redirect(url, { status: 303 });
}

function applySessionCookie(response, value, max_age = SESSION_MAX_AGE_SECONDS) {
  response.cookies.set({
    name: OPS_DASHBOARD_SESSION_COOKIE,
    value,
    httpOnly: true,
    sameSite: "lax",
    secure: isProductionRuntime(),
    path: "/",
    maxAge: max_age,
  });
}

export async function POST(request) {
  const mode = getResponseMode(request);
  const payload = await parseRequestBody(request);
  const intent = lower(payload?.intent || "");
  const redirect_to = clean(payload?.redirect_to || DEFAULT_REDIRECT_PATH);
  const secret = getOpsDashboardSecret();

  if (intent === "logout") {
    const response =
      mode === "json"
        ? NextResponse.json({ ok: true, logged_out: true })
        : buildRedirectResponse(request, redirect_to);

    applySessionCookie(response, "", 0);
    return response;
  }

  if (!secret) {
    if (isProductionRuntime()) {
      return NextResponse.json(
        {
          ok: false,
          error: "missing_ops_dashboard_secret",
        },
        { status: 500 }
      );
    }

    return mode === "json"
      ? NextResponse.json({
          ok: true,
          authorized: true,
          reason: "ops_dashboard_secret_not_configured",
        })
      : buildRedirectResponse(request, redirect_to);
  }

  const supplied_secret =
    clean(payload?.secret) ||
    clean(payload?.dashboard_secret) ||
    clean(payload?.ops_dashboard_secret) ||
    clean(request.headers.get("x-ops-dashboard-secret")) ||
    clean(request.headers.get("authorization")).replace(/^bearer\s+/i, "");

  if (!safeEqual(supplied_secret, secret)) {
    return mode === "json"
      ? NextResponse.json(
          {
            ok: false,
            error: "invalid_ops_dashboard_secret",
          },
          { status: 401 }
        )
      : buildRedirectResponse(request, redirect_to, "auth=invalid");
  }

  const response =
    mode === "json"
      ? NextResponse.json({
          ok: true,
          authorized: true,
        })
      : buildRedirectResponse(request, redirect_to);

  applySessionCookie(response, buildOpsDashboardSessionToken(secret));
  return response;
}
