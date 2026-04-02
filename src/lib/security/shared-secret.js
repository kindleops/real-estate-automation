import crypto from "node:crypto";

import { NextResponse } from "next/server";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
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

function isProductionRuntime() {
  return (
    lower(process.env.VERCEL_ENV) === "production" ||
    lower(process.env.NODE_ENV) === "production"
  );
}

function getCookieToken(request, cookie_names = []) {
  for (const cookie_name of cookie_names) {
    const value = clean(request?.cookies?.get(cookie_name)?.value);
    if (value) {
      return {
        token: value,
        via: `cookie:${cookie_name}`,
      };
    }
  }

  return null;
}

function getHeaderToken(request, header_names = []) {
  const authorization = clean(request?.headers?.get("authorization"));
  if (authorization.toLowerCase().startsWith("bearer ")) {
    return {
      token: clean(authorization.slice(7)),
      via: "authorization",
    };
  }

  for (const header_name of header_names) {
    const value = clean(request?.headers?.get(header_name));
    if (value) {
      return {
        token: value,
        via: `header:${header_name}`,
      };
    }
  }

  return null;
}

export function getSharedSecretAuthResult(request, {
  env_name,
  header_names = [],
  cookie_names = [],
  expected_token = null,
} = {}) {
  const secret = clean(expected_token ?? process.env[env_name || ""]);
  const token =
    getHeaderToken(request, header_names) ||
    getCookieToken(request, cookie_names);

  if (!secret) {
    if (isProductionRuntime()) {
      return {
        ok: false,
        status: 500,
        reason: `missing_${lower(env_name)}`,
        required: true,
        authenticated: false,
        via: token?.via || null,
      };
    }

    return {
      ok: true,
      status: 200,
      reason: `${lower(env_name)}_not_configured`,
      required: false,
      authenticated: false,
      via: token?.via || null,
    };
  }

  if (!token?.token) {
    return {
      ok: false,
      status: 401,
      reason: `missing_${lower(env_name)}_token`,
      required: true,
      authenticated: false,
      via: null,
    };
  }

  if (!safeEqual(token.token, secret)) {
    return {
      ok: false,
      status: 401,
      reason: `invalid_${lower(env_name)}_token`,
      required: true,
      authenticated: false,
      via: token.via,
    };
  }

  return {
    ok: true,
    status: 200,
    reason: "authorized",
    required: true,
    authenticated: true,
    via: token.via,
  };
}

export function requireSharedSecretAuth(request, logger = null, options = {}) {
  const auth = getSharedSecretAuthResult(request, options);

  if (auth.ok) {
    return {
      authorized: true,
      auth,
      response: null,
    };
  }

  logger?.warn?.("shared_secret_auth.rejected", {
    env_name: options?.env_name || null,
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

export default requireSharedSecretAuth;
