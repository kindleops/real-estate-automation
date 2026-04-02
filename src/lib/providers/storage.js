import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import ENV from "@/lib/config/env.js";
import { child } from "@/lib/logging/logger.js";
import {
  S3_STORAGE_PROVIDER,
  buildS3ObjectUrl,
  buildS3PresignedUrl,
  buildS3SignedRequest,
  buildS3StorageConfig,
} from "@/lib/providers/storage-s3.js";

const logger = child({
  module: "providers.storage",
});

export const LOCAL_STORAGE_PROVIDER = "local";
const DEFAULT_SIGNED_URL_TTL_SECONDS = 24 * 60 * 60;
const DEFAULT_STORAGE_HTTP_TIMEOUT_MS = 15_000;
const STORAGE_ACCESS_ROUTE = "/api/storage/access";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function sha256Hex(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function isProductionRuntime() {
  return (
    lower(process.env.VERCEL_ENV) === "production" ||
    lower(process.env.NODE_ENV || ENV.NODE_ENV) === "production"
  );
}

function getStorageProvider(provider_override = null, env = ENV) {
  return clean(provider_override || env.STORAGE_PROVIDER || LOCAL_STORAGE_PROVIDER) || LOCAL_STORAGE_PROVIDER;
}

export function getStorageLocalRootPath(env = ENV) {
  const configured_root = clean(env.STORAGE_LOCAL_ROOT);
  if (configured_root) {
    return path.resolve(configured_root);
  }

  return path.join(process.cwd(), ".data", "storage");
}

function getStorageLocalRoot(env = ENV) {
  return getStorageLocalRootPath(env);
}

function getStorageSigningSecret(env = ENV) {
  return clean(env.STORAGE_SIGNING_SECRET || env.INTERNAL_API_SECRET);
}

function getStorageBaseUrl(env = ENV) {
  return clean(env.APP_BASE_URL);
}

function normalizeKey(key) {
  const raw = clean(key).replace(/\\/g, "/").replace(/^\/+/, "");
  const normalized = path.posix.normalize(raw);

  if (!normalized || normalized === "." || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error("invalid_storage_key");
  }

  return normalized;
}

function toBuffer(body, body_encoding = "utf8") {
  if (Buffer.isBuffer(body)) return body;
  if (body instanceof Uint8Array) return Buffer.from(body);

  if (typeof body === "string") {
    return Buffer.from(body, body_encoding === "base64" ? "base64" : "utf8");
  }

  if (body && typeof body === "object") {
    return Buffer.from(JSON.stringify(body, null, 2), "utf8");
  }

  return Buffer.alloc(0);
}

function normalizeStorageMetadata(metadata = {}) {
  return metadata && typeof metadata === "object" ? metadata : {};
}

function buildObjectPaths(key, env = ENV) {
  const normalized_key = normalizeKey(key);
  const root = getStorageLocalRoot(env);
  const file_path = path.join(root, normalized_key);
  const metadata_path = `${file_path}.meta.json`;

  return {
    root,
    key: normalized_key,
    file_path,
    metadata_path,
  };
}

function buildStorageObject({
  key,
  provider,
  filename = "",
  content_type = "application/octet-stream",
  size_bytes = 0,
  sha256 = "",
  metadata = {},
  created_at = nowIso(),
} = {}) {
  const normalized_key = normalizeKey(key);

  return {
    key: normalized_key,
    provider,
    filename: clean(filename) || path.posix.basename(normalized_key),
    content_type: clean(content_type) || "application/octet-stream",
    size_bytes: Number(size_bytes || 0) || 0,
    sha256: clean(sha256) || null,
    created_at: clean(created_at) || nowIso(),
    metadata: normalizeStorageMetadata(metadata),
  };
}

async function ensureDirectory(file_path) {
  await fs.mkdir(path.dirname(file_path), { recursive: true });
}

function buildStorageSignaturePayload({
  key,
  expires_at,
  disposition = "inline",
  filename = "",
} = {}) {
  return [
    normalizeKey(key),
    clean(expires_at),
    clean(disposition || "inline"),
    clean(filename),
  ].join(":");
}

function signStoragePayload(payload, secret = getStorageSigningSecret()) {
  const normalized_secret = clean(secret);
  if (!normalized_secret) return "";

  return crypto
    .createHmac("sha256", normalized_secret)
    .update(String(payload), "utf8")
    .digest("hex");
}

function buildStorageAccessPath({
  key,
  expires_at,
  signature,
  disposition = "inline",
  filename = "",
} = {}) {
  const params = new URLSearchParams({
    key: normalizeKey(key),
    expires: clean(expires_at),
    signature: clean(signature),
  });

  if (clean(disposition)) params.set("disposition", clean(disposition));
  if (clean(filename)) params.set("filename", clean(filename));

  return `${STORAGE_ACCESS_ROUTE}?${params.toString()}`;
}

async function fetchWithTimeout({
  url,
  method = "GET",
  headers = {},
  body = null,
  timeout_ms = DEFAULT_STORAGE_HTTP_TIMEOUT_MS,
  fetch_impl = globalThis.fetch,
} = {}) {
  if (typeof fetch_impl !== "function") {
    throw new Error("storage_fetch_not_available");
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1_000, Number(timeout_ms || 0)));

  try {
    return await fetch_impl(url, {
      method,
      headers,
      body,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

function buildS3RuntimeConfig(env = ENV) {
  return buildS3StorageConfig(env);
}

function buildS3RequestHeaders({
  method = "GET",
  key,
  body_hash,
  content_type = "",
  config = buildS3RuntimeConfig(),
} = {}) {
  const request_url = buildS3ObjectUrl({ key, config });
  const signed = buildS3SignedRequest({
    method,
    url: request_url,
    body_hash,
    config,
    headers: clean(content_type) ? { "content-type": clean(content_type) } : {},
  });

  return {
    url: request_url.toString(),
    headers: {
      Authorization: signed.authorization,
      "x-amz-content-sha256": body_hash,
      "x-amz-date": signed.amz_date,
      ...(clean(content_type) ? { "content-type": clean(content_type) } : {}),
    },
  };
}

async function putS3Object({
  key,
  body,
  content_type = "application/octet-stream",
  config = buildS3RuntimeConfig(),
  fetch_impl = globalThis.fetch,
} = {}) {
  if (!config.configured) {
    return {
      ok: false,
      reason: "s3_storage_not_configured",
      missing: config.missing,
    };
  }

  const normalized_key = normalizeKey(key);
  const buffer = toBuffer(body);
  const { url, headers } = buildS3RequestHeaders({
    method: "PUT",
    key: normalized_key,
    body_hash: sha256Hex(buffer),
    content_type,
    config,
  });

  try {
    const response = await fetchWithTimeout({
      url,
      method: "PUT",
      headers,
      body: buffer,
      fetch_impl,
    });

    if (!response.ok) {
      return {
        ok: false,
        reason: response.status === 404 ? "s3_storage_object_not_found" : "s3_storage_upload_failed",
        status_code: response.status,
      };
    }

    return {
      ok: true,
      status_code: response.status,
    };
  } catch (error) {
    return {
      ok: false,
      reason: clean(error?.name) === "AbortError" ? "s3_storage_request_timeout" : "s3_storage_request_failed",
      error_message: clean(error?.message) || null,
    };
  }
}

async function getS3Object({
  key,
  config = buildS3RuntimeConfig(),
  fetch_impl = globalThis.fetch,
} = {}) {
  if (!config.configured) {
    return {
      ok: false,
      reason: "s3_storage_not_configured",
      missing: config.missing,
      body: null,
    };
  }

  const normalized_key = normalizeKey(key);
  const { url, headers } = buildS3RequestHeaders({
    method: "GET",
    key: normalized_key,
    body_hash: sha256Hex(""),
    config,
  });

  try {
    const response = await fetchWithTimeout({
      url,
      method: "GET",
      headers,
      fetch_impl,
    });

    if (!response.ok) {
      return {
        ok: false,
        reason: response.status === 404 ? "storage_object_not_found" : "s3_storage_read_failed",
        status_code: response.status,
        body: null,
      };
    }

    const body = Buffer.from(await response.arrayBuffer());
    return {
      ok: true,
      status_code: response.status,
      body,
      content_type: clean(response.headers.get("content-type")) || null,
    };
  } catch (error) {
    return {
      ok: false,
      reason: clean(error?.name) === "AbortError" ? "s3_storage_request_timeout" : "s3_storage_request_failed",
      error_message: clean(error?.message) || null,
      body: null,
    };
  }
}

export function getStorageConfigSummary({
  provider_override = null,
  env_override = ENV,
} = {}) {
  const provider = getStorageProvider(provider_override, env_override);
  const signing_secret = getStorageSigningSecret(env_override);
  const base_url = getStorageBaseUrl(env_override);
  const root = getStorageLocalRoot(env_override);
  const s3 = buildS3RuntimeConfig(env_override);

  if (provider === LOCAL_STORAGE_PROVIDER) {
    const missing = [];
    if (!signing_secret) {
      missing.push("STORAGE_SIGNING_SECRET_OR_INTERNAL_API_SECRET");
    }

    return {
      configured: true,
      provider,
      root,
      signed_access_supported: Boolean(signing_secret),
      external_signed_access_supported: Boolean(signing_secret && base_url),
      base_url_present: Boolean(base_url),
      missing,
    };
  }

  if (provider === S3_STORAGE_PROVIDER) {
    return {
      configured: s3.configured,
      provider,
      root: null,
      bucket: s3.bucket || null,
      region: s3.region || null,
      endpoint: s3.endpoint || null,
      signed_access_supported: s3.configured,
      external_signed_access_supported: s3.configured,
      base_url_present: Boolean(base_url),
      missing: Array.isArray(s3.missing) ? s3.missing : [],
    };
  }

  return {
    configured: false,
    provider,
    root: null,
    signed_access_supported: false,
    external_signed_access_supported: false,
    base_url_present: Boolean(base_url),
    missing: ["unsupported_storage_provider"],
  };
}

export async function uploadFile({
  key,
  body,
  content_type = "application/octet-stream",
  filename = "",
  metadata = {},
  body_encoding = "utf8",
  dry_run = false,
  provider_override = null,
  env_override = ENV,
  fetch_impl = globalThis.fetch,
} = {}) {
  const provider = getStorageProvider(provider_override, env_override);
  const normalized_key = normalizeKey(key);
  const buffer = toBuffer(body, body_encoding);
  const sha256 = crypto.createHash("sha256").update(buffer).digest("hex");
  const size_bytes = buffer.byteLength;

  logger.info("storage.upload_requested", {
    provider,
    key: normalized_key,
    content_type,
    filename: clean(filename) || null,
    size_bytes,
    dry_run,
  });

  const object = buildStorageObject({
    key: normalized_key,
    provider,
    filename,
    content_type,
    size_bytes,
    sha256,
    metadata,
  });

  if (provider === S3_STORAGE_PROVIDER) {
    const s3 = buildS3RuntimeConfig(env_override);
    if (!s3.configured) {
      return {
        ok: false,
        provider,
        reason: "s3_storage_not_configured",
        missing: s3.missing,
        key: normalized_key,
      };
    }

    if (dry_run) {
      return {
        ok: true,
        dry_run: true,
        provider,
        ...object,
        url: null,
        storage_uri: `storage://${provider}/${normalized_key}`,
      };
    }

    const upload_result = await putS3Object({
      key: normalized_key,
      body: buffer,
      content_type: object.content_type,
      config: s3,
      fetch_impl,
    });
    if (!upload_result.ok) {
      return {
        ok: false,
        provider,
        reason: upload_result.reason || "s3_storage_upload_failed",
        key: normalized_key,
        status_code: upload_result.status_code ?? null,
        error_message: upload_result.error_message || null,
      };
    }

    const metadata_key = `${normalized_key}.meta.json`;
    const metadata_upload = await putS3Object({
      key: metadata_key,
      body: JSON.stringify(object, null, 2),
      content_type: "application/json",
      config: s3,
      fetch_impl,
    });
    if (!metadata_upload.ok) {
      return {
        ok: false,
        provider,
        reason: metadata_upload.reason || "s3_storage_metadata_upload_failed",
        key: normalized_key,
        status_code: metadata_upload.status_code ?? null,
        error_message: metadata_upload.error_message || null,
      };
    }

    return {
      ok: true,
      dry_run: false,
      provider,
      ...object,
      url: null,
      storage_uri: `storage://${provider}/${normalized_key}`,
    };
  }

  if (provider !== LOCAL_STORAGE_PROVIDER) {
    return {
      ok: false,
      provider,
      reason: "unsupported_storage_provider",
      key: normalized_key,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      dry_run: true,
      provider,
      ...object,
      url: null,
      storage_uri: `storage://${provider}/${normalized_key}`,
    };
  }

  const { file_path, metadata_path } = buildObjectPaths(normalized_key, env_override);
  await ensureDirectory(file_path);
  await fs.writeFile(file_path, buffer);
  await fs.writeFile(metadata_path, JSON.stringify(object, null, 2), "utf8");

  return {
    ok: true,
    dry_run: false,
    provider,
    ...object,
    url: null,
    storage_uri: `storage://${provider}/${normalized_key}`,
  };
}

export async function getObjectMetadata({
  key,
  provider_override = null,
  env_override = ENV,
  fetch_impl = globalThis.fetch,
} = {}) {
  const provider = getStorageProvider(provider_override, env_override);
  const normalized_key = normalizeKey(key);

  if (provider === S3_STORAGE_PROVIDER) {
    const metadata_result = await getS3Object({
      key: `${normalized_key}.meta.json`,
      config: buildS3RuntimeConfig(env_override),
      fetch_impl,
    });
    if (!metadata_result.ok) {
      return {
        ok: false,
        reason: metadata_result.reason,
        error_message: metadata_result.error_message || null,
        metadata: null,
      };
    }

    try {
      return {
        ok: true,
        metadata: JSON.parse(metadata_result.body.toString("utf8")),
      };
    } catch (error) {
      return {
        ok: false,
        reason: "storage_metadata_read_failed",
        error_message: clean(error?.message) || null,
        metadata: null,
      };
    }
  }

  if (provider !== LOCAL_STORAGE_PROVIDER) {
    return {
      ok: false,
      reason: "unsupported_storage_provider",
      metadata: null,
    };
  }

  const { metadata_path } = buildObjectPaths(normalized_key, env_override);

  try {
    const raw = await fs.readFile(metadata_path, "utf8");
    const parsed = JSON.parse(raw);

    return {
      ok: true,
      metadata: parsed,
    };
  } catch (error) {
    return {
      ok: false,
      reason: error?.code === "ENOENT" ? "storage_object_not_found" : "storage_metadata_read_failed",
      error_message: clean(error?.message) || null,
      metadata: null,
    };
  }
}

export async function readFile({
  key,
  provider_override = null,
  env_override = ENV,
  fetch_impl = globalThis.fetch,
} = {}) {
  const provider = getStorageProvider(provider_override, env_override);
  const normalized_key = normalizeKey(key);
  const metadata_result = await getObjectMetadata({
    key: normalized_key,
    provider_override,
    env_override,
    fetch_impl,
  });
  if (!metadata_result.ok) {
    return {
      ok: false,
      reason: metadata_result.reason,
      body: null,
      metadata: null,
    };
  }

  if (provider === S3_STORAGE_PROVIDER) {
    const object_result = await getS3Object({
      key: normalized_key,
      config: buildS3RuntimeConfig(env_override),
      fetch_impl,
    });
    return {
      ok: object_result.ok,
      reason: object_result.ok ? null : object_result.reason,
      error_message: object_result.error_message || null,
      key: normalized_key,
      body: object_result.body,
      metadata: metadata_result.metadata,
    };
  }

  if (provider !== LOCAL_STORAGE_PROVIDER) {
    return {
      ok: false,
      reason: "unsupported_storage_provider",
      body: null,
      metadata: metadata_result.metadata,
    };
  }

  const { file_path } = buildObjectPaths(normalized_key, env_override);

  try {
    const body = await fs.readFile(file_path);
    return {
      ok: true,
      key: normalized_key,
      body,
      metadata: metadata_result.metadata,
    };
  } catch (error) {
    return {
      ok: false,
      reason: error?.code === "ENOENT" ? "storage_object_not_found" : "storage_file_read_failed",
      error_message: clean(error?.message) || null,
      body: null,
      metadata: metadata_result.metadata,
    };
  }
}

export function verifySignedStorageAccess({
  key,
  expires,
  signature,
  disposition = "inline",
  filename = "",
  secret = getStorageSigningSecret(),
} = {}) {
  const normalized_secret = clean(secret);
  const normalized_signature = clean(signature);
  const normalized_key = normalizeKey(key);
  const expires_at = Number(expires);

  if (!normalized_secret) {
    return {
      ok: false,
      verified: false,
      reason: isProductionRuntime()
        ? "missing_storage_signing_secret"
        : "storage_signing_secret_not_configured",
    };
  }

  if (!normalized_signature) {
    return {
      ok: false,
      verified: false,
      reason: "missing_storage_signature",
    };
  }

  if (!Number.isFinite(expires_at) || expires_at <= 0) {
    return {
      ok: false,
      verified: false,
      reason: "invalid_storage_expiry",
    };
  }

  if (Date.now() > expires_at) {
    return {
      ok: false,
      verified: false,
      reason: "storage_signature_expired",
    };
  }

  const payload = buildStorageSignaturePayload({
    key: normalized_key,
    expires_at,
    disposition,
    filename,
  });
  const expected_signature = signStoragePayload(payload, normalized_secret);

  if (
    !expected_signature ||
    normalized_signature.length !== expected_signature.length ||
    !crypto.timingSafeEqual(
      Buffer.from(normalized_signature, "utf8"),
      Buffer.from(expected_signature, "utf8")
    )
  ) {
    return {
      ok: false,
      verified: false,
      reason: "invalid_storage_signature",
    };
  }

  return {
    ok: true,
    verified: true,
    reason: "storage_signature_verified",
    key: normalized_key,
  };
}

export async function getSignedUrl({
  key,
  expires_in_seconds = DEFAULT_SIGNED_URL_TTL_SECONDS,
  disposition = "inline",
  filename = "",
  base_url = "",
  provider_override = null,
  env_override = ENV,
} = {}) {
  const normalized_key = normalizeKey(key);
  const signing_secret = getStorageSigningSecret(env_override);
  const resolved_base_url = clean(base_url) || getStorageBaseUrl(env_override);

  logger.info("storage.get_signed_url_requested", {
    key: normalized_key,
    expires_in_seconds,
    disposition: clean(disposition) || "inline",
    filename: clean(filename) || null,
    base_url_present: Boolean(clean(resolved_base_url)),
  });

  const provider = getStorageProvider(provider_override, env_override);
  if (provider === S3_STORAGE_PROVIDER) {
    const presigned = buildS3PresignedUrl({
      key: normalized_key,
      expires_in_seconds,
      disposition,
      filename,
      config: buildS3RuntimeConfig(env_override),
    });

    return {
      ok: presigned.ok,
      key: normalized_key,
      provider,
      expires_in_seconds: presigned.expires_in_seconds ?? Math.max(60, Number(expires_in_seconds || 0)),
      expires_at: presigned.expires_at ?? null,
      signature: presigned.signature || null,
      path: presigned.path || null,
      url: presigned.url || null,
      reason: presigned.reason,
      missing: presigned.missing || [],
    };
  }

  if (!signing_secret) {
    return {
      ok: false,
      key: normalized_key,
      url: null,
      path: null,
      reason: isProductionRuntime()
        ? "missing_storage_signing_secret"
        : "storage_signing_secret_not_configured",
    };
  }

  const expires_at = Date.now() + Math.max(60, Number(expires_in_seconds || 0)) * 1000;
  const payload = buildStorageSignaturePayload({
    key: normalized_key,
    expires_at,
    disposition,
    filename,
  });
  const signature = signStoragePayload(payload, signing_secret);
  const relative_path = buildStorageAccessPath({
    key: normalized_key,
    expires_at,
    signature,
    disposition,
    filename,
  });
  const normalized_base_url = clean(resolved_base_url).replace(/\/+$/, "");

  return {
    ok: true,
    key: normalized_key,
    provider,
    expires_in_seconds: Math.max(60, Number(expires_in_seconds || 0)),
    expires_at,
    signature,
    path: relative_path,
    url: normalized_base_url ? `${normalized_base_url}${relative_path}` : null,
    reason: normalized_base_url
      ? "signed_url_ready"
      : "signed_path_ready_missing_app_base_url",
  };
}

export default {
  LOCAL_STORAGE_PROVIDER,
  getStorageLocalRootPath,
  getStorageConfigSummary,
  uploadFile,
  getObjectMetadata,
  readFile,
  verifySignedStorageAccess,
  getSignedUrl,
};
