import crypto from "node:crypto";

import ENV from "@/lib/config/env.js";

export const S3_STORAGE_PROVIDER = "s3";

function clean(value) {
  return String(value ?? "").trim();
}

function encodeRfc3986(value = "") {
  return encodeURIComponent(String(value))
    .replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

function hmac(key, data, encoding = undefined) {
  return crypto.createHmac("sha256", key).update(data, "utf8").digest(encoding);
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function toAmzDate(date = new Date()) {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

function toDateStamp(date = new Date()) {
  return toAmzDate(date).slice(0, 8);
}

function encodeObjectKey(key = "") {
  return String(key)
    .split("/")
    .map((segment) => encodeRfc3986(segment))
    .join("/");
}

function normalizeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "off"].includes(normalized)) return false;
  return fallback;
}

export function buildS3StorageConfig(env = ENV) {
  const endpoint = clean(env.STORAGE_S3_ENDPOINT);
  const bucket = clean(env.STORAGE_BUCKET);
  const region = clean(env.STORAGE_REGION);
  const access_key_id = clean(env.STORAGE_S3_ACCESS_KEY_ID);
  const secret_access_key = clean(env.STORAGE_S3_SECRET_ACCESS_KEY);
  const force_path_style = normalizeBoolean(env.STORAGE_S3_FORCE_PATH_STYLE, false);
  const missing = [];

  if (!endpoint) missing.push("STORAGE_S3_ENDPOINT");
  if (!bucket) missing.push("STORAGE_BUCKET");
  if (!region) missing.push("STORAGE_REGION");
  if (!access_key_id) missing.push("STORAGE_S3_ACCESS_KEY_ID");
  if (!secret_access_key) missing.push("STORAGE_S3_SECRET_ACCESS_KEY");

  return {
    provider: S3_STORAGE_PROVIDER,
    configured: missing.length === 0,
    endpoint,
    bucket,
    region,
    access_key_id,
    secret_access_key,
    force_path_style,
    missing,
  };
}

export function buildS3ObjectUrl({
  key,
  config = buildS3StorageConfig(),
  query = {},
} = {}) {
  const endpoint = new URL(config.endpoint);
  const encoded_key = encodeObjectKey(key);

  if (config.force_path_style) {
    endpoint.pathname = `${endpoint.pathname.replace(/\/+$/, "")}/${encodeRfc3986(config.bucket)}/${encoded_key}`;
  } else {
    endpoint.hostname = `${config.bucket}.${endpoint.hostname}`;
    endpoint.pathname = `${endpoint.pathname.replace(/\/+$/, "")}/${encoded_key}`;
  }

  for (const [name, value] of Object.entries(query || {})) {
    if (value === null || value === undefined || value === "") continue;
    endpoint.searchParams.set(name, String(value));
  }

  return endpoint;
}

function buildCanonicalQuery(searchParams) {
  return [...searchParams.entries()]
    .map(([key, value]) => [encodeRfc3986(key), encodeRfc3986(value)])
    .sort((left, right) =>
      left[0] === right[0] ? left[1].localeCompare(right[1]) : left[0].localeCompare(right[0])
    )
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
}

function buildCanonicalHeaders(headers = {}) {
  return Object.entries(headers)
    .map(([name, value]) => [String(name).toLowerCase(), clean(value).replace(/\s+/g, " ")])
    .sort((left, right) => left[0].localeCompare(right[0]));
}

function buildSignatureKey(secret_access_key, datestamp, region, service = "s3") {
  const k_date = hmac(`AWS4${secret_access_key}`, datestamp);
  const k_region = hmac(k_date, region);
  const k_service = hmac(k_region, service);
  return hmac(k_service, "aws4_request");
}

export function buildS3SignedRequest({
  method = "GET",
  url,
  body_hash = sha256(""),
  config = buildS3StorageConfig(),
  now = new Date(),
  headers = {},
} = {}) {
  const target = typeof url === "string" ? new URL(url) : new URL(url.toString());
  const amz_date = toAmzDate(now);
  const datestamp = toDateStamp(now);
  const normalized_headers = buildCanonicalHeaders({
    host: target.host,
    "x-amz-content-sha256": body_hash,
    "x-amz-date": amz_date,
    ...headers,
  });
  const canonical_headers = normalized_headers
    .map(([name, value]) => `${name}:${value}\n`)
    .join("");
  const signed_headers = normalized_headers.map(([name]) => name).join(";");
  const canonical_request = [
    method.toUpperCase(),
    target.pathname,
    buildCanonicalQuery(target.searchParams),
    canonical_headers,
    signed_headers,
    body_hash,
  ].join("\n");
  const credential_scope = `${datestamp}/${config.region}/s3/aws4_request`;
  const string_to_sign = [
    "AWS4-HMAC-SHA256",
    amz_date,
    credential_scope,
    sha256(canonical_request),
  ].join("\n");
  const signing_key = buildSignatureKey(
    config.secret_access_key,
    datestamp,
    config.region
  );
  const signature = hmac(signing_key, string_to_sign, "hex");

  return {
    authorization: `AWS4-HMAC-SHA256 Credential=${config.access_key_id}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}`,
    amz_date,
    signed_headers,
    headers: Object.fromEntries(normalized_headers),
    signature,
  };
}

export function buildS3PresignedUrl({
  key,
  expires_in_seconds = 3600,
  disposition = "inline",
  filename = "",
  config = buildS3StorageConfig(),
  now = new Date(),
} = {}) {
  if (!config.configured) {
    return {
      ok: false,
      reason: "s3_storage_not_configured",
      missing: config.missing,
      url: null,
    };
  }

  const response_content_disposition = clean(filename)
    ? `${clean(disposition) || "inline"}; filename="${clean(filename).replace(/["\r\n]+/g, " ")}"`
    : clean(disposition) || "inline";
  const target = buildS3ObjectUrl({
    key,
    config,
    query: {
      "response-content-disposition": response_content_disposition,
    },
  });
  const amz_date = toAmzDate(now);
  const datestamp = toDateStamp(now);
  const expires = Math.max(60, Math.min(7 * 24 * 60 * 60, Number(expires_in_seconds || 0)));
  const credential_scope = `${datestamp}/${config.region}/s3/aws4_request`;

  target.searchParams.set("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
  target.searchParams.set(
    "X-Amz-Credential",
    `${config.access_key_id}/${credential_scope}`
  );
  target.searchParams.set("X-Amz-Date", amz_date);
  target.searchParams.set("X-Amz-Expires", String(expires));
  target.searchParams.set("X-Amz-SignedHeaders", "host");

  const canonical_request = [
    "GET",
    target.pathname,
    buildCanonicalQuery(target.searchParams),
    `host:${target.host}\n`,
    "host",
    "UNSIGNED-PAYLOAD",
  ].join("\n");
  const string_to_sign = [
    "AWS4-HMAC-SHA256",
    amz_date,
    credential_scope,
    sha256(canonical_request),
  ].join("\n");
  const signing_key = buildSignatureKey(
    config.secret_access_key,
    datestamp,
    config.region
  );
  const signature = hmac(signing_key, string_to_sign, "hex");
  target.searchParams.set("X-Amz-Signature", signature);

  return {
    ok: true,
    reason: "s3_presigned_url_ready",
    expires_in_seconds: expires,
    expires_at: new Date(now.getTime() + expires * 1000).getTime(),
    signature,
    url: target.toString(),
    path: `${target.pathname}${target.search}`,
  };
}

export default {
  S3_STORAGE_PROVIDER,
  buildS3StorageConfig,
  buildS3ObjectUrl,
  buildS3SignedRequest,
  buildS3PresignedUrl,
};
