import { NextResponse } from "next/server";

function clean(value) {
  return String(value ?? "").trim();
}

export function getCronAuthResult(request) {
  const cron_secret = clean(process.env.CRON_SECRET);
  const authorization = clean(request?.headers?.get("authorization"));
  const user_agent = clean(request?.headers?.get("user-agent"));
  const is_vercel_production = clean(process.env.VERCEL_ENV).toLowerCase() === "production";
  const is_vercel_cron = user_agent.includes("vercel-cron/1.0");

  if (!cron_secret) {
    if (is_vercel_production) {
      return {
        ok: false,
        status: 500,
        reason: "missing_cron_secret",
        is_vercel_cron,
        user_agent: user_agent || null,
      };
    }

    return {
      ok: true,
      authenticated: false,
      required: false,
      reason: "cron_secret_not_configured",
      is_vercel_cron,
      user_agent: user_agent || null,
    };
  }

  if (authorization !== `Bearer ${cron_secret}`) {
    return {
      ok: false,
      status: 401,
      reason: "invalid_cron_authorization",
      is_vercel_cron,
      user_agent: user_agent || null,
    };
  }

  return {
    ok: true,
    authenticated: true,
    required: true,
    reason: "authorized",
    is_vercel_cron,
    user_agent: user_agent || null,
  };
}

export function requireCronAuth(request, logger = null) {
  const auth = getCronAuthResult(request);

  if (auth.ok) {
    return {
      authorized: true,
      auth,
      response: null,
    };
  }

  logger?.warn?.("cron_auth.rejected", {
    reason: auth.reason,
    is_vercel_cron: auth.is_vercel_cron,
    user_agent: auth.user_agent,
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

export default requireCronAuth;
