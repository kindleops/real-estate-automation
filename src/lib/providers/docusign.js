import crypto from "node:crypto";

import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { child } from "@/lib/logging/logger.js";

const logger = child({
  module: "providers.docusign",
});

const DOCUSIGN_DEFAULT_TIMEOUT_MS = 30_000;
const DOCUSIGN_TOKEN_REFRESH_BUFFER_MS = 60_000;
const DOCUSIGN_DEFAULT_JWT_SCOPE = "signature impersonation";
const DOCUSIGN_DEFAULT_AUTH_CODE_SCOPE = "signature extended";

const _token_cache = globalThis.__rea_docusign_token_cache__ || {
  access_token: null,
  expires_at_ms: 0,
  refresh_promise: null,
};

if (!globalThis.__rea_docusign_token_cache__) {
  globalThis.__rea_docusign_token_cache__ = _token_cache;
}

const defaultDeps = {
  fetch: (...args) => fetch(...args),
  nowMs: () => Date.now(),
  recordSystemAlert,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function toBase64Url(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function sha256Base64Url(input) {
  return crypto
    .createHash("sha256")
    .update(String(input ?? ""), "utf8")
    .digest("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function nowEpochSeconds() {
  return Math.floor(runtimeDeps.nowMs() / 1000);
}

function sanitizePrivateKey(value = "") {
  return clean(value).replace(/\\n/g, "\n");
}

function normalizeDocusignEnvironment(raw = "") {
  const value = lower(raw);

  if (["prod", "production", "live"].includes(value)) {
    return "production";
  }

  return "demo";
}

function inferDocusignEnvironmentFromBaseUrl(base_url = "") {
  const normalized = lower(base_url);
  if (!normalized) return null;
  if (normalized.includes("account-d.") || normalized.includes("demo.docusign.net")) {
    return "demo";
  }
  if (normalized.includes("account.docusign.com") || normalized.includes("www.docusign.net")) {
    return "production";
  }
  return null;
}

function defaultBaseUrlForEnvironment(environment = "demo") {
  return environment === "production"
    ? "https://www.docusign.net/restapi"
    : "https://demo.docusign.net/restapi";
}

function defaultOAuthBaseUrlForEnvironment(environment = "demo") {
  return environment === "production"
    ? "https://account.docusign.com"
    : "https://account-d.docusign.com";
}

export function getDocusignConfig(env = process.env) {
  const configured_environment =
    normalizeDocusignEnvironment(env.DOCUSIGN_ENV) ||
    inferDocusignEnvironmentFromBaseUrl(env.DOCUSIGN_BASE_URL) ||
    inferDocusignEnvironmentFromBaseUrl(env.DOCUSIGN_OAUTH_BASE_URL) ||
    "demo";

  const base_url =
    clean(env.DOCUSIGN_BASE_URL) ||
    clean(env.DOCUSIGN_BASE_URI) ||
    defaultBaseUrlForEnvironment(configured_environment);

  const oauth_base_url =
    clean(env.DOCUSIGN_OAUTH_BASE_URL) ||
    clean(env.DOCUSIGN_OAUTH_BASE_URI) ||
    defaultOAuthBaseUrlForEnvironment(configured_environment);

  return {
    environment:
      inferDocusignEnvironmentFromBaseUrl(base_url) ||
      inferDocusignEnvironmentFromBaseUrl(oauth_base_url) ||
      configured_environment,
    base_url,
    oauth_base_url,
    integration_key:
      clean(env.DOCUSIGN_INTEGRATION_KEY) ||
      clean(env.DOCUSIGN_API_KEY),
    user_id: clean(env.DOCUSIGN_USER_ID),
    account_id: clean(env.DOCUSIGN_ACCOUNT_ID),
    client_secret: clean(env.DOCUSIGN_CLIENT_SECRET),
    private_key: sanitizePrivateKey(env.DOCUSIGN_PRIVATE_KEY),
    redirect_uri_local: clean(env.DOCUSIGN_REDIRECT_URI_LOCAL),
    redirect_uri_preview: clean(env.DOCUSIGN_REDIRECT_URI_PREVIEW),
    redirect_uri_prod: clean(env.DOCUSIGN_REDIRECT_URI_PROD),
    jwt_scope:
      clean(env.DOCUSIGN_IMPERSONATION_SCOPE) ||
      DOCUSIGN_DEFAULT_JWT_SCOPE,
    auth_code_scope:
      clean(env.DOCUSIGN_AUTH_CODE_SCOPE) ||
      DOCUSIGN_DEFAULT_AUTH_CODE_SCOPE,
    timeout_ms:
      Number(env.DOCUSIGN_TIMEOUT_MS) || DOCUSIGN_DEFAULT_TIMEOUT_MS,
  };
}

function validateJwtConfig(config) {
  if (!clean(config.integration_key)) return "missing_integration_key";
  if (!clean(config.user_id)) return "missing_user_id";
  if (!clean(config.account_id)) return "missing_account_id";
  if (!clean(config.private_key)) return "missing_private_key";
  if (!clean(config.base_url)) return "missing_base_url";
  if (!clean(config.oauth_base_url)) return "missing_oauth_base_url";
  return null;
}

function validateAuthCodeConfig(config) {
  if (!clean(config.integration_key)) return "missing_integration_key";
  if (!clean(config.client_secret)) return "missing_client_secret";
  if (!clean(config.oauth_base_url)) return "missing_oauth_base_url";
  if (
    !clean(config.redirect_uri_local) &&
    !clean(config.redirect_uri_preview) &&
    !clean(config.redirect_uri_prod)
  ) {
    return "missing_redirect_uri";
  }
  return null;
}

export function getDocusignConfigSummary(env = process.env) {
  const config = getDocusignConfig(env);
  const jwt_missing = [];
  const auth_code_missing = [];

  if (!clean(config.integration_key)) {
    jwt_missing.push("DOCUSIGN_INTEGRATION_KEY");
    auth_code_missing.push("DOCUSIGN_INTEGRATION_KEY");
  }
  if (!clean(config.user_id)) jwt_missing.push("DOCUSIGN_USER_ID");
  if (!clean(config.account_id)) jwt_missing.push("DOCUSIGN_ACCOUNT_ID");
  if (!clean(config.private_key)) jwt_missing.push("DOCUSIGN_PRIVATE_KEY");
  if (!clean(config.client_secret)) auth_code_missing.push("DOCUSIGN_CLIENT_SECRET");
  if (
    !clean(config.redirect_uri_local) &&
    !clean(config.redirect_uri_preview) &&
    !clean(config.redirect_uri_prod)
  ) {
    auth_code_missing.push(
      "DOCUSIGN_REDIRECT_URI_LOCAL|DOCUSIGN_REDIRECT_URI_PREVIEW|DOCUSIGN_REDIRECT_URI_PROD"
    );
  }

  return {
    configured: jwt_missing.length === 0,
    jwt_ready: jwt_missing.length === 0,
    auth_code_ready: auth_code_missing.length === 0,
    missing: jwt_missing,
    jwt_missing,
    auth_code_missing,
    environment: config.environment,
    base_url: config.base_url,
    oauth_base_url: config.oauth_base_url,
    account_id_present: Boolean(clean(config.account_id)),
    integration_key_present: Boolean(clean(config.integration_key)),
    user_id_present: Boolean(clean(config.user_id)),
    private_key_present: Boolean(clean(config.private_key)),
    client_secret_present: Boolean(clean(config.client_secret)),
    redirect_uri_local_present: Boolean(clean(config.redirect_uri_local)),
    redirect_uri_preview_present: Boolean(clean(config.redirect_uri_preview)),
    redirect_uri_prod_present: Boolean(clean(config.redirect_uri_prod)),
  };
}

export function __setDocusignTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetDocusignTestDeps() {
  runtimeDeps = { ...defaultDeps };
  _token_cache.access_token = null;
  _token_cache.expires_at_ms = 0;
  _token_cache.refresh_promise = null;
}

function buildJwtAssertion(config) {
  const aud = (() => {
    try {
      return new URL(config.oauth_base_url).hostname || config.oauth_base_url;
    } catch {
      return config.oauth_base_url;
    }
  })();
  const header = {
    alg: "RS256",
    typ: "JWT",
  };
  const payload = {
    iss: config.integration_key,
    sub: config.user_id,
    aud,
    iat: nowEpochSeconds(),
    exp: nowEpochSeconds() + 3600,
    scope: config.jwt_scope,
  };

  const encoded_header = toBase64Url(JSON.stringify(header));
  const encoded_payload = toBase64Url(JSON.stringify(payload));
  const signing_input = `${encoded_header}.${encoded_payload}`;
  const signer = crypto.createSign("RSA-SHA256");
  signer.update(signing_input);
  signer.end();

  const signature = signer.sign(config.private_key);
  const encoded_signature = signature
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");

  return `${signing_input}.${encoded_signature}`;
}

async function docusignFetchJson(
  url,
  options = {},
  timeout_ms = DOCUSIGN_DEFAULT_TIMEOUT_MS
) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeout_ms);

  try {
    const response = await runtimeDeps.fetch(url, {
      ...options,
      signal: controller.signal,
    });
    const text = await response.text();
    let data = null;

    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = text ? { raw_text: text } : null;
    }

    if (!response.ok) {
      return {
        ok: false,
        status_code: response.status,
        error: data,
      };
    }

    return {
      ok: true,
      status_code: response.status,
      data,
    };
  } catch (error) {
    return {
      ok: false,
      status_code: null,
      error: {
        message: clean(error?.message) || "request_failed",
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

function shouldUseCachedAccessToken() {
  return (
    clean(_token_cache.access_token) &&
    runtimeDeps.nowMs() <
      Number(_token_cache.expires_at_ms || 0) - DOCUSIGN_TOKEN_REFRESH_BUFFER_MS
  );
}

async function requestJwtAccessToken({ config, dry_run = false } = {}) {
  const config_error = validateJwtConfig(config);

  if (config_error) {
    logger.warn("docusign.jwt_config_invalid", {
      reason: config_error,
      environment: config.environment,
    });

    await runtimeDeps.recordSystemAlert({
      subsystem: "docusign",
      code: "jwt_config_invalid",
      severity: "high",
      retryable: false,
      summary: `DocuSign JWT configuration invalid: ${config_error}`,
      dedupe_key: `docusign_jwt_config_${config_error}`,
      metadata: {
        environment: config.environment,
      },
    });

    return {
      ok: false,
      reason: config_error,
      access_token: null,
      expires_in: null,
      config,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      reason: "dry_run",
      access_token: "dry-run-token",
      expires_in: 3600,
      config,
    };
  }

  let assertion = null;

  try {
    assertion = buildJwtAssertion(config);
  } catch (error) {
    logger.warn("docusign.jwt_build_failed", {
      reason: clean(error?.message) || "jwt_build_failed",
      environment: config.environment,
    });

    await runtimeDeps.recordSystemAlert({
      subsystem: "docusign",
      code: "jwt_build_failed",
      severity: "high",
      retryable: false,
      summary: `DocuSign JWT build failed: ${clean(error?.message) || "jwt_build_failed"}`,
      dedupe_key: "docusign_jwt_build_failed",
      metadata: {
        environment: config.environment,
      },
    });

    return {
      ok: false,
      reason: "jwt_build_failed",
      access_token: null,
      expires_in: null,
      config,
    };
  }

  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion,
  });

  const token_result = await docusignFetchJson(
    `${config.oauth_base_url}/oauth/token`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
    },
    config.timeout_ms
  );

  if (!token_result.ok) {
    logger.warn("docusign.jwt_token_request_failed", {
      environment: config.environment,
      status_code: token_result.status_code,
    });

    await runtimeDeps.recordSystemAlert({
      subsystem: "docusign",
      code: "jwt_token_request_failed",
      severity: "high",
      retryable: true,
      summary: "DocuSign JWT token request failed.",
      dedupe_key: `docusign_jwt_token_${clean(token_result.status_code) || "unknown"}`,
      metadata: {
        environment: config.environment,
        status_code: token_result.status_code,
      },
    });

    return {
      ok: false,
      reason: "token_request_failed",
      access_token: null,
      expires_in: null,
      config,
      raw: token_result.error,
    };
  }

  return {
    ok: true,
    reason: "token_ready",
    access_token: clean(token_result.data?.access_token),
    expires_in: Number(token_result.data?.expires_in) || 3600,
    token_type: clean(token_result.data?.token_type) || "Bearer",
    config,
    raw: token_result.data,
  };
}

export async function getDocusignAccessToken({
  dry_run = false,
  force_refresh = false,
} = {}) {
  const config = getDocusignConfig();

  if (dry_run) {
    return requestJwtAccessToken({ config, dry_run: true });
  }

  if (!force_refresh && shouldUseCachedAccessToken()) {
    return {
      ok: true,
      reason: "cached_token",
      access_token: _token_cache.access_token,
      expires_in: Math.max(
        0,
        Math.floor((_token_cache.expires_at_ms - runtimeDeps.nowMs()) / 1000)
      ),
      config,
      cached: true,
    };
  }

  if (!force_refresh && _token_cache.refresh_promise) {
    return _token_cache.refresh_promise;
  }

  _token_cache.refresh_promise = requestJwtAccessToken({
    config,
    dry_run: false,
  })
    .then((result) => {
      if (result?.ok && clean(result.access_token)) {
        _token_cache.access_token = result.access_token;
        _token_cache.expires_at_ms =
          runtimeDeps.nowMs() + Number(result.expires_in || 3600) * 1000;
      }

      return result;
    })
    .finally(() => {
      _token_cache.refresh_promise = null;
    });

  return _token_cache.refresh_promise;
}

export function resolveDocusignRedirectUri({
  target = "auto",
  origin = "",
  env = process.env,
} = {}) {
  const config = getDocusignConfig(env);
  const normalized_target = lower(target);

  if (normalized_target === "local") return clean(config.redirect_uri_local) || null;
  if (normalized_target === "preview") return clean(config.redirect_uri_preview) || null;
  if (normalized_target === "prod" || normalized_target === "production") {
    return clean(config.redirect_uri_prod) || null;
  }

  const normalized_origin = lower(origin);
  if (normalized_origin.includes("localhost") || normalized_origin.includes("127.0.0.1")) {
    return clean(config.redirect_uri_local) || null;
  }

  if (clean(config.redirect_uri_prod) && normalized_origin) {
    const prod_origin = lower(new URL(config.redirect_uri_prod).origin);
    if (normalized_origin === prod_origin) return clean(config.redirect_uri_prod);
  }

  return (
    clean(config.redirect_uri_preview) ||
    clean(config.redirect_uri_prod) ||
    clean(config.redirect_uri_local) ||
    null
  );
}

export function buildDocusignAuthorizationUrl({
  redirect_uri = null,
  target = "auto",
  state = "",
  scopes = [],
  code_challenge = "",
  code_challenge_method = "S256",
  code_verifier = "",
  env = process.env,
} = {}) {
  const config = getDocusignConfig(env);
  const config_error = validateAuthCodeConfig(config);

  if (config_error) {
    return {
      ok: false,
      reason: config_error,
      authorization_url: null,
      redirect_uri: null,
    };
  }

  const resolved_redirect_uri =
    clean(redirect_uri) ||
    resolveDocusignRedirectUri({
      target,
      env,
    });

  if (!resolved_redirect_uri) {
    return {
      ok: false,
      reason: "missing_redirect_uri",
      authorization_url: null,
      redirect_uri: null,
    };
  }

  const params = new URLSearchParams({
    response_type: "code",
    scope:
      safeArray(scopes).map((value) => clean(value)).filter(Boolean).join(" ") ||
      config.auth_code_scope,
    client_id: config.integration_key,
    redirect_uri: resolved_redirect_uri,
  });

  if (clean(state)) {
    params.set("state", clean(state));
  }

  const resolved_code_challenge =
    clean(code_challenge) ||
    (clean(code_verifier) ? sha256Base64Url(clean(code_verifier)) : "");

  if (resolved_code_challenge) {
    params.set("code_challenge", resolved_code_challenge);
    params.set(
      "code_challenge_method",
      clean(code_challenge_method) || "S256"
    );
  }

  return {
    ok: true,
    reason: "authorization_url_ready",
    authorization_url: `${config.oauth_base_url}/oauth/auth?${params.toString()}`,
    redirect_uri: resolved_redirect_uri,
    environment: config.environment,
  };
}

export async function exchangeDocusignAuthorizationCode({
  code = null,
  redirect_uri = null,
  target = "auto",
  code_verifier = null,
  dry_run = false,
  env = process.env,
} = {}) {
  const config = getDocusignConfig(env);
  const config_error = validateAuthCodeConfig(config);

  if (config_error) {
    return {
      ok: false,
      reason: config_error,
      access_token: null,
      refresh_token: null,
      redirect_uri: null,
    };
  }

  const normalized_code = clean(code);
  if (!normalized_code) {
    return {
      ok: false,
      reason: "missing_authorization_code",
      access_token: null,
      refresh_token: null,
      redirect_uri: null,
    };
  }

  const resolved_redirect_uri =
    clean(redirect_uri) ||
    resolveDocusignRedirectUri({
      target,
      env,
    });

  if (!resolved_redirect_uri) {
    return {
      ok: false,
      reason: "missing_redirect_uri",
      access_token: null,
      refresh_token: null,
      redirect_uri: null,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      reason: "dry_run",
      access_token: "dry-run-token",
      refresh_token: "dry-run-refresh",
      redirect_uri: resolved_redirect_uri,
      environment: config.environment,
    };
  }

  const basic_auth = Buffer.from(
    `${config.integration_key}:${config.client_secret}`,
    "utf8"
  ).toString("base64");

  const body = new URLSearchParams({
    grant_type: "authorization_code",
    code: normalized_code,
    redirect_uri: resolved_redirect_uri,
  });

  if (clean(code_verifier)) {
    body.set("code_verifier", clean(code_verifier));
  }

  const token_result = await docusignFetchJson(
    `${config.oauth_base_url}/oauth/token`,
    {
      method: "POST",
      headers: {
        Authorization: `Basic ${basic_auth}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
    },
    config.timeout_ms
  );

  if (!token_result.ok) {
    return {
      ok: false,
      reason: "authorization_code_exchange_failed",
      access_token: null,
      refresh_token: null,
      redirect_uri: resolved_redirect_uri,
      raw: token_result.error,
    };
  }

  return {
    ok: true,
    reason: "authorization_code_exchanged",
    access_token: clean(token_result.data?.access_token),
    refresh_token: clean(token_result.data?.refresh_token),
    expires_in: Number(token_result.data?.expires_in) || null,
    token_type: clean(token_result.data?.token_type) || "Bearer",
    redirect_uri: resolved_redirect_uri,
    environment: config.environment,
    raw: token_result.data,
  };
}

export const exchangeAuthorizationCode = exchangeDocusignAuthorizationCode;

function buildRecipientSummary(recipients = []) {
  const normalized = safeArray(recipients).map((recipient) => ({
    role: clean(recipient.role),
    recipient_type: clean(recipient.recipient_type) || "signer",
    routing_order: clean(recipient.routing_order) || null,
    name_present: Boolean(clean(recipient.name)),
    email_present: Boolean(clean(recipient.email)),
  }));

  return {
    total: normalized.length,
    seller_count: normalized.filter((recipient) => recipient.role === "seller").length,
    buyer_count: normalized.filter((recipient) => recipient.role === "buyer").length,
    internal_cc_count: normalized.filter((recipient) => recipient.role === "internal_cc")
      .length,
    recipients: normalized,
  };
}

function compactAliases(values = []) {
  return [...new Set(safeArray(values).map((value) => lower(value)).filter(Boolean))];
}

function getConfiguredRoleAliases(env = process.env) {
  return {
    seller: compactAliases([
      "seller",
      "seller signer",
      env.DOCUSIGN_SELLER_ROLE_NAME,
    ]),
    buyer: compactAliases([
      "buyer",
      "buyer signer",
      env.DOCUSIGN_BUYER_ROLE_NAME,
    ]),
    internal_cc: compactAliases([
      "internal_cc",
      "internal cc",
      "internalcc",
      "cc",
      "carbon copy",
    ]),
  };
}

function normalizeRecipientRole(value = "") {
  const normalized = lower(value);
  const aliases = getConfiguredRoleAliases();
  if (aliases.seller.includes(normalized)) return "seller";
  if (aliases.buyer.includes(normalized)) return "buyer";
  if (aliases.internal_cc.includes(normalized)) {
    return "internal_cc";
  }
  return normalized || "seller";
}

function normalizeRecipient(recipient = {}, index = 0) {
  const role = normalizeRecipientRole(
    recipient.role ||
      recipient.role_key ||
      recipient.role_name ||
      recipient.recipient_role
  );
  const recipient_type =
    role === "internal_cc"
      ? "carbon_copy"
      : clean(recipient.recipient_type) || "signer";

  return {
    id: clean(recipient.id) || clean(recipient.signer_id) || String(index + 1),
    name: clean(recipient.name),
    email: clean(recipient.email),
    role,
    role_name: clean(recipient.role_name) || role,
    routing_order: clean(recipient.routing_order) || String(index + 1),
    recipient_type,
  };
}

function normalizeRecipients({
  recipients = [],
  signers = [],
} = {}) {
  const base = safeArray(recipients).length ? recipients : signers;
  return safeArray(base).map(normalizeRecipient).filter(Boolean);
}

function normalizeDocument(document = {}, index = 0) {
  return {
    document_id: clean(document.document_id) || clean(document.id) || String(index + 1),
    name: clean(document.name) || `Document ${index + 1}`,
    file_base64: clean(document.file_base64) || clean(document.base64),
    file_extension:
      clean(document.file_extension) || clean(document.extension) || "pdf",
  };
}

function normalizeMetadata(metadata = {}) {
  return isPlainObject(metadata) ? metadata : {};
}

function validateEnvelopeInput({
  subject = "",
  documents = [],
  recipients = [],
  template_id = null,
} = {}) {
  if (!clean(subject)) {
    return {
      ok: false,
      reason: "missing_subject",
    };
  }

  if (!clean(template_id) && !safeArray(documents).length) {
    return {
      ok: false,
      reason: "missing_documents_or_template",
    };
  }

  if (!safeArray(recipients).length) {
    return {
      ok: false,
      reason: "missing_recipients",
    };
  }

  const invalid_recipient = recipients.find(
    (recipient) => !clean(recipient.name) || !clean(recipient.email)
  );

  if (invalid_recipient) {
    return {
      ok: false,
      reason: "invalid_recipient",
    };
  }

  return {
    ok: true,
  };
}

function buildEnvelopeDefinition({
  subject,
  documents = [],
  recipients = [],
  template_id = null,
  email_blurb = "",
  metadata = {},
  status = "created",
} = {}) {
  const normalized_documents = safeArray(documents).map(normalizeDocument);
  const normalized_recipients = normalizeRecipients({ recipients });
  const normalized_metadata = normalizeMetadata(metadata);

  const custom_fields = Object.keys(normalized_metadata).length
    ? {
        textCustomFields: Object.entries(normalized_metadata).map(([name, value]) => ({
          name: clean(name),
          value: clean(value),
          show: "true",
        })),
      }
    : undefined;

  if (clean(template_id)) {
    return {
      emailSubject: clean(subject),
      emailBlurb: clean(email_blurb) || undefined,
      templateId: clean(template_id),
      status: clean(status) || "created",
      templateRoles: normalized_recipients.map((recipient) => ({
        email: recipient.email,
        name: recipient.name,
        roleName: recipient.role_name || recipient.role,
        routingOrder: recipient.routing_order,
      })),
      customFields: custom_fields,
    };
  }

  const signers = normalized_recipients
    .filter((recipient) => recipient.recipient_type !== "carbon_copy")
    .map((recipient) => ({
      email: recipient.email,
      name: recipient.name,
      recipientId: recipient.id,
      routingOrder: recipient.routing_order,
      roleName: recipient.role_name || recipient.role,
    }));

  const carbonCopies = normalized_recipients
    .filter((recipient) => recipient.recipient_type === "carbon_copy")
    .map((recipient) => ({
      email: recipient.email,
      name: recipient.name,
      recipientId: recipient.id,
      routingOrder: recipient.routing_order,
      roleName: recipient.role_name || recipient.role,
    }));

  return {
    emailSubject: clean(subject),
    emailBlurb: clean(email_blurb) || undefined,
    status: clean(status) || "created",
    documents: normalized_documents.map((document) => ({
      documentBase64: document.file_base64,
      name: document.name,
      fileExtension: document.file_extension,
      documentId: document.document_id,
    })),
    recipients: {
      ...(signers.length ? { signers } : {}),
      ...(carbonCopies.length ? { carbonCopies } : {}),
    },
    customFields: custom_fields,
  };
}

function extractEnvelopeTimestamps(raw = null) {
  return {
    created_at:
      clean(raw?.createdDateTime) ||
      clean(raw?.created_at) ||
      null,
    sent_at:
      clean(raw?.sentDateTime) ||
      clean(raw?.statusDateTime) ||
      null,
    delivered_at:
      clean(raw?.deliveredDateTime) ||
      clean(raw?.delivered_at) ||
      null,
    completed_at:
      clean(raw?.completedDateTime) ||
      clean(raw?.completed_at) ||
      null,
  };
}

function extractSigningLink(raw = null) {
  return (
    clean(raw?.signingUri) ||
    clean(raw?.signing_url) ||
    clean(raw?.uri) ||
    null
  );
}

function extractEnvelopeRecipients(raw = null) {
  const recipients = raw?.recipients || raw?.envelopeSummary?.recipients || {};
  const signers = safeArray(recipients?.signers).map((recipient, index) =>
    normalizeRecipient(
      {
        id: recipient?.recipientId,
        name: recipient?.name,
        email: recipient?.email,
        role_name: recipient?.roleName,
        routing_order: recipient?.routingOrder,
        recipient_type: "signer",
      },
      index
    )
  );
  const carbon_copies = safeArray(
    recipients?.carbonCopies || recipients?.carbon_copies
  ).map((recipient, index) =>
    normalizeRecipient(
      {
        id: recipient?.recipientId,
        name: recipient?.name,
        email: recipient?.email,
        role_name: recipient?.roleName,
        routing_order: recipient?.routingOrder,
        recipient_type: "carbon_copy",
      },
      index + signers.length
    )
  );

  return [...signers, ...carbon_copies];
}

export async function diagnoseDocusignJwtAuth({
  dry_run = false,
  force_refresh = false,
} = {}) {
  const auth_result = await getDocusignAccessToken({
    dry_run,
    force_refresh,
  });

  if (!auth_result.ok) {
    return {
      ok: false,
      reason: auth_result.reason || "docusign_auth_failed",
      environment: auth_result?.config?.environment || null,
      account_id: auth_result?.config?.account_id || null,
      access_token_present: Boolean(clean(auth_result?.access_token)),
      raw: auth_result?.raw || null,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      reason: "dry_run",
      environment: auth_result.config.environment,
      account_id: auth_result.config.account_id,
      access_token_present: true,
      userinfo_loaded: false,
      raw: null,
    };
  }

  const userinfo_result = await docusignFetchJson(
    `${auth_result.config.oauth_base_url}/oauth/userinfo`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${auth_result.access_token}`,
      },
    },
    auth_result.config.timeout_ms
  );

  return {
    ok: Boolean(auth_result.ok && userinfo_result.ok),
    reason: userinfo_result.ok ? "docusign_jwt_verified" : "docusign_userinfo_failed",
    environment: auth_result.config.environment,
    account_id: auth_result.config.account_id,
    access_token_present: Boolean(clean(auth_result.access_token)),
    userinfo_loaded: Boolean(userinfo_result.ok),
    user_guid: clean(userinfo_result.data?.sub) || null,
    default_account_id:
      clean(userinfo_result.data?.accounts?.find?.((account) => account?.is_default)?.account_id) ||
      null,
    raw: userinfo_result.ok ? userinfo_result.data : userinfo_result.error,
  };
}

export async function verifyDocusignAuth({ dry_run = false } = {}) {
  const result = await diagnoseDocusignJwtAuth({ dry_run });

  return {
    ok: Boolean(result.ok),
    reason: result.reason || "docusign_auth_failed",
    dry_run: Boolean(dry_run),
    environment: result.environment || null,
    account_id: result.account_id || null,
    access_token_present: Boolean(result.access_token_present),
    userinfo_loaded: Boolean(result.userinfo_loaded),
    raw: result.raw || null,
  };
}

export async function createEnvelope({
  subject,
  documents = [],
  recipients = [],
  signers = [],
  template_id = null,
  email_blurb = "",
  metadata = {},
  dry_run = false,
  status = "created",
} = {}) {
  const normalized_documents = safeArray(documents).map(normalizeDocument).filter(
    (document) => clean(document.file_base64)
  );
  const normalized_recipients = normalizeRecipients({
    recipients,
    signers,
  });
  const validation = validateEnvelopeInput({
    subject,
    documents: normalized_documents,
    recipients: normalized_recipients,
    template_id,
  });

  if (!validation.ok) {
    logger.warn("docusign.create_envelope_invalid_input", {
      reason: validation.reason,
      template_id: clean(template_id) || null,
      documents_count: normalized_documents.length,
      recipients_count: normalized_recipients.length,
      dry_run: Boolean(dry_run),
    });

    return {
      ok: false,
      dry_run: Boolean(dry_run),
      reason: validation.reason,
      envelope_id: null,
      status: null,
      recipient_summary: buildRecipientSummary(normalized_recipients),
      timestamps: extractEnvelopeTimestamps(null),
      raw: null,
    };
  }

  const envelope_definition = buildEnvelopeDefinition({
    subject,
    documents: normalized_documents,
    recipients: normalized_recipients,
    template_id,
    email_blurb,
    metadata,
    status,
  });

  logger.info("docusign.create_envelope_requested", {
    template_id: clean(template_id) || null,
    documents_count: normalized_documents.length,
    recipients_count: normalized_recipients.length,
    dry_run: Boolean(dry_run),
  });

  if (dry_run) {
    return {
      ok: true,
      dry_run: true,
      reason: "dry_run",
      envelope_id: null,
      status: clean(status) || "created",
      recipient_summary: buildRecipientSummary(normalized_recipients),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: envelope_definition,
    };
  }

  const auth_result = await getDocusignAccessToken({
    dry_run: false,
  });

  if (!auth_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: auth_result.reason,
      envelope_id: null,
      status: null,
      recipient_summary: buildRecipientSummary(normalized_recipients),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: auth_result.raw || null,
    };
  }

  const create_result = await docusignFetchJson(
    `${auth_result.config.base_url}/v2.1/accounts/${auth_result.config.account_id}/envelopes`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${auth_result.access_token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(envelope_definition),
    },
    auth_result.config.timeout_ms
  );

  if (!create_result.ok) {
    logger.warn("docusign.create_envelope_failed", {
      status_code: create_result.status_code,
    });

    return {
      ok: false,
      dry_run: false,
      reason: "create_envelope_failed",
      envelope_id: null,
      status: null,
      recipient_summary: buildRecipientSummary(normalized_recipients),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: create_result.error,
    };
  }

  return {
    ok: true,
    dry_run: false,
    reason: "envelope_created",
    envelope_id: clean(create_result.data?.envelopeId) || null,
    status: clean(create_result.data?.status) || clean(status) || "created",
    recipient_summary: buildRecipientSummary(normalized_recipients),
    timestamps: extractEnvelopeTimestamps(create_result.data),
    signing_link: extractSigningLink(create_result.data),
    raw: create_result.data,
  };
}

export async function sendEnvelope({
  envelope_id,
  dry_run = false,
} = {}) {
  const normalized_envelope_id = clean(envelope_id);

  if (!normalized_envelope_id) {
    return {
      ok: false,
      dry_run: Boolean(dry_run),
      reason: "missing_envelope_id",
      envelope_id: null,
      status: null,
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
    };
  }

  logger.info("docusign.send_envelope_requested", {
    envelope_id: normalized_envelope_id,
    dry_run: Boolean(dry_run),
  });

  if (dry_run) {
    return {
      ok: true,
      dry_run: true,
      reason: "dry_run",
      envelope_id: normalized_envelope_id,
      status: "sent",
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
    };
  }

  const auth_result = await getDocusignAccessToken({
    dry_run: false,
  });

  if (!auth_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: auth_result.reason,
      envelope_id: normalized_envelope_id,
      status: null,
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: auth_result.raw || null,
    };
  }

  const send_result = await docusignFetchJson(
    `${auth_result.config.base_url}/v2.1/accounts/${auth_result.config.account_id}/envelopes/${normalized_envelope_id}`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${auth_result.access_token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        status: "sent",
      }),
    },
    auth_result.config.timeout_ms
  );

  if (!send_result.ok) {
    logger.warn("docusign.send_envelope_failed", {
      envelope_id: normalized_envelope_id,
      status_code: send_result.status_code,
    });

    return {
      ok: false,
      dry_run: false,
      reason: "send_envelope_failed",
      envelope_id: normalized_envelope_id,
      status: null,
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: send_result.error,
    };
  }

  return {
    ok: true,
    dry_run: false,
    reason: "envelope_sent",
    envelope_id: normalized_envelope_id,
    status: clean(send_result.data?.status) || "sent",
    timestamps: extractEnvelopeTimestamps(send_result.data),
    signing_link: extractSigningLink(send_result.data),
    raw: send_result.data,
  };
}

export async function getEnvelope({
  envelope_id,
  dry_run = false,
} = {}) {
  const normalized_envelope_id = clean(envelope_id);

  if (!normalized_envelope_id) {
    return {
      ok: false,
      dry_run: Boolean(dry_run),
      reason: "missing_envelope_id",
      envelope_id: null,
      status: null,
      recipient_summary: buildRecipientSummary([]),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: null,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      dry_run: true,
      reason: "dry_run",
      envelope_id: normalized_envelope_id,
      status: null,
      recipient_summary: buildRecipientSummary([]),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: null,
    };
  }

  const auth_result = await getDocusignAccessToken({
    dry_run: false,
  });

  if (!auth_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: auth_result.reason,
      envelope_id: normalized_envelope_id,
      status: null,
      recipient_summary: buildRecipientSummary([]),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: auth_result.raw || null,
    };
  }

  const envelope_result = await docusignFetchJson(
    `${auth_result.config.base_url}/v2.1/accounts/${auth_result.config.account_id}/envelopes/${normalized_envelope_id}`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${auth_result.access_token}`,
        "Content-Type": "application/json",
      },
    },
    auth_result.config.timeout_ms
  );

  if (!envelope_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: "get_envelope_failed",
      envelope_id: normalized_envelope_id,
      status: null,
      recipient_summary: buildRecipientSummary([]),
      timestamps: extractEnvelopeTimestamps(null),
      signing_link: null,
      raw: envelope_result.error,
    };
  }

  return {
    ok: true,
    dry_run: false,
    reason: "envelope_loaded",
    envelope_id: normalized_envelope_id,
    status: clean(envelope_result.data?.status) || null,
    recipient_summary: buildRecipientSummary(
      extractEnvelopeRecipients(envelope_result.data)
    ),
    timestamps: extractEnvelopeTimestamps(envelope_result.data),
    signing_link: extractSigningLink(envelope_result.data),
    raw: envelope_result.data,
  };
}

export default {
  __setDocusignTestDeps,
  __resetDocusignTestDeps,
  getDocusignConfig,
  getDocusignConfigSummary,
  getDocusignAccessToken,
  buildDocusignAuthorizationUrl,
  resolveDocusignRedirectUri,
  exchangeDocusignAuthorizationCode,
  exchangeAuthorizationCode,
  verifyDocusignAuth,
  diagnoseDocusignJwtAuth,
  createEnvelope,
  sendEnvelope,
  getEnvelope,
};
