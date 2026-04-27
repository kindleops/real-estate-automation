function clean(value = "") {
  return String(value ?? "").trim();
}

function getBearerToken(request) {
  const auth_header = clean(request?.headers?.get?.("authorization"));
  if (!auth_header.toLowerCase().startsWith("bearer ")) return "";
  return auth_header.slice(7).trim();
}

export function requireInternalSecret(request) {
  const internal_secret = clean(process.env.INTERNAL_API_SECRET);
  const cron_secret = clean(process.env.CRON_SECRET);

  const provided_internal =
    clean(request?.headers?.get?.("x-internal-api-secret")) ||
    clean(request?.headers?.get?.("x-cron-secret")) ||
    getBearerToken(request);

  const allowed = [internal_secret, cron_secret].filter(Boolean);

  if (!allowed.length) {
    return {
      ok: false,
      error: "internal_secret_not_configured",
      status: 500,
    };
  }

  if (!provided_internal || !allowed.includes(provided_internal)) {
    return {
      ok: false,
      error: "unauthorized",
      status: 401,
    };
  }

  return { ok: true };
}

export default requireInternalSecret;
