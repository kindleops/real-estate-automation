import { NextResponse } from "next/server";
import { getSharedSecretAuthResult } from "./shared-secret.js";

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

export async function requireCronOrEngineAuth(request, logger = null) {
  const cron_result = requireCronAuth(request, logger);
  if (cron_result.authorized) return cron_result;

  const queue_secret = String(process.env.QUEUE_ENGINE_SHARED_SECRET ?? "").trim();
  if (!queue_secret) return cron_result;

  const engine_result = getSharedSecretAuthResult(request, {
    env_name: "QUEUE_ENGINE_SHARED_SECRET",
    header_names: ["x-queue-engine-secret"],
    expected_token: queue_secret,
  });

  if (engine_result.ok) {
    return {
      authorized: true,
      auth: {
        authenticated: true,
        is_vercel_cron: false,
        via: engine_result.via || "x-queue-engine-secret",
      },
      response: null,
    };
  }

  logger?.warn?.("queue_engine_secret.rejected", {
    reason: engine_result.reason,
    via: engine_result.via || null,
  });
  return cron_result;
}

export default requireCronAuth;
