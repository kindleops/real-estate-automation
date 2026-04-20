function clean(value) {
  return String(value ?? "").trim();
}

export function isProductionNodeEnv() {
  return clean(process.env.NODE_ENV) === "production";
}

export function requireDevRouteAccess(request) {
  if (!isProductionNodeEnv()) {
    return null;
  }

  const expected_secret = clean(process.env.INTERNAL_API_SECRET);
  const provided_secret = clean(request?.headers?.get?.("x-internal-api-secret"));

  if (!expected_secret || !provided_secret || provided_secret !== expected_secret) {
    return new Response(null, { status: 404 });
  }

  return null;
}
