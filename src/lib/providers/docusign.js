// ─── docusign.js ─────────────────────────────────────────────────────────
import crypto from "node:crypto";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { child } from "@/lib/logging/logger.js";

const logger = child({
  module: "providers.docusign",
});

const DOCUSIGN_DEFAULT_TIMEOUT_MS = 30000;

function clean(value) {
  return String(value ?? "").trim();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function base64UrlEncode(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function nowEpochSeconds() {
  return Math.floor(Date.now() / 1000);
}

function normalizeDocusignEnv(raw = "") {
  const value = clean(raw).toLowerCase();

  if (value === "prod") return "production";
  if (value === "live") return "production";
  if (value === "production") return "production";

  return "demo";
}

function getDocusignConfig() {
  const env_name = normalizeDocusignEnv(process.env.DOCUSIGN_ENV);
  const is_production = env_name === "production";

  const base_uri =
    clean(process.env.DOCUSIGN_BASE_URI) ||
    (is_production
      ? "https://www.docusign.net/restapi"
      : "https://demo.docusign.net/restapi");

  const oauth_base_uri =
    clean(process.env.DOCUSIGN_OAUTH_BASE_URI) ||
    (is_production
      ? "https://account.docusign.com"
      : "https://account-d.docusign.com");

  return {
    env_name,
    is_production,
    base_uri,
    oauth_base_uri,
    account_id: clean(process.env.DOCUSIGN_ACCOUNT_ID),
    integration_key: clean(process.env.DOCUSIGN_INTEGRATION_KEY),
    user_id: clean(process.env.DOCUSIGN_USER_ID),
    private_key: clean(process.env.DOCUSIGN_PRIVATE_KEY),
    impersonation_scope:
      clean(process.env.DOCUSIGN_IMPERSONATION_SCOPE) ||
      "signature impersonation",
    timeout_ms:
      Number(process.env.DOCUSIGN_TIMEOUT_MS) || DOCUSIGN_DEFAULT_TIMEOUT_MS,
  };
}

export function getDocusignConfigSummary() {
  const config = getDocusignConfig();
  const missing = [];

  if (!config.account_id) missing.push("DOCUSIGN_ACCOUNT_ID");
  if (!config.integration_key) missing.push("DOCUSIGN_INTEGRATION_KEY");
  if (!config.user_id) missing.push("DOCUSIGN_USER_ID");
  if (!config.private_key) missing.push("DOCUSIGN_PRIVATE_KEY");

  return {
    configured: missing.length === 0,
    missing,
    env_name: config.env_name,
    is_production: config.is_production,
    base_uri: config.base_uri,
    oauth_base_uri: config.oauth_base_uri,
    account_id_present: Boolean(config.account_id),
    integration_key_present: Boolean(config.integration_key),
    user_id_present: Boolean(config.user_id),
    private_key_present: Boolean(config.private_key),
  };
}

function normalizeDocument(document = {}, index = 0) {
  return {
    document_id:
      clean(document.document_id) ||
      clean(document.id) ||
      String(index + 1),
    name:
      clean(document.name) ||
      `Document ${index + 1}`,
    file_base64:
      clean(document.file_base64) ||
      clean(document.base64) ||
      "",
    file_extension:
      clean(document.file_extension) ||
      clean(document.extension) ||
      "pdf",
  };
}

function normalizeSigner(signer = {}, index = 0) {
  return {
    signer_id:
      clean(signer.signer_id) ||
      clean(signer.id) ||
      String(index + 1),
    name: clean(signer.name),
    email: clean(signer.email),
    routing_order:
      clean(signer.routing_order) || String(index + 1),
    role_name:
      clean(signer.role_name) || "",
    recipient_type:
      clean(signer.recipient_type) || "signer",
  };
}

function normalizeMetadata(metadata = {}) {
  if (!isPlainObject(metadata)) return {};
  return metadata;
}

function validateEnvelopeInput({
  subject = "",
  documents = [],
  signers = [],
  template_id = null,
} = {}) {
  const has_template = Boolean(clean(template_id));

  if (!clean(subject)) {
    return {
      ok: false,
      reason: "missing_subject",
    };
  }

  if (!has_template && !safeArray(documents).length) {
    return {
      ok: false,
      reason: "missing_documents_or_template",
    };
  }

  if (!safeArray(signers).length) {
    return {
      ok: false,
      reason: "missing_signers",
    };
  }

  const normalized_signers = safeArray(signers).map(normalizeSigner);

  const invalid_signer = normalized_signers.find(
    (signer) => !signer.name || !signer.email
  );

  if (invalid_signer) {
    return {
      ok: false,
      reason: "invalid_signer",
    };
  }

  return {
    ok: true,
  };
}

function buildJwtAssertion(config) {
  const header = {
    alg: "RS256",
    typ: "JWT",
  };

  const payload = {
    iss: config.integration_key,
    sub: config.user_id,
    aud: config.oauth_base_uri,
    iat: nowEpochSeconds(),
    exp: nowEpochSeconds() + 3600,
    scope: config.impersonation_scope,
  };

  const encoded_header = base64UrlEncode(JSON.stringify(header));
  const encoded_payload = base64UrlEncode(JSON.stringify(payload));
  const signing_input = `${encoded_header}.${encoded_payload}`;

  const signer = crypto.createSign("RSA-SHA256");
  signer.update(signing_input);
  signer.end();

  const private_key = config.private_key.replace(/\\n/g, "\n");

  const signature = signer.sign(private_key);
  const encoded_signature = signature
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");

  return `${signing_input}.${encoded_signature}`;
}

function validateConfigForLiveSend(config) {
  if (!config.account_id) return "missing_account_id";
  if (!config.integration_key) return "missing_integration_key";
  if (!config.user_id) return "missing_user_id";
  if (!config.private_key) return "missing_private_key";
  if (!config.base_uri) return "missing_base_uri";
  if (!config.oauth_base_uri) return "missing_oauth_base_uri";
  return null;
}

async function docusignFetchJson(url, options = {}, timeout_ms = DOCUSIGN_DEFAULT_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeout_ms);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });

    const text = await response.text();
    let data = null;

    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw_text: text };
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

async function getAccessToken({ dry_run = false } = {}) {
  const config = getDocusignConfig();
  const config_error = validateConfigForLiveSend(config);

  if (config_error) {
    logger.warn("docusign.auth_config_invalid", {
      reason: config_error,
      env_name: config.env_name,
    });

    await recordSystemAlert({
      subsystem: "docusign",
      code: "auth_config_invalid",
      severity: "high",
      retryable: false,
      summary: `DocuSign configuration invalid: ${config_error}`,
      dedupe_key: "docusign_auth_config_invalid",
      metadata: {
        env_name: config.env_name,
      },
    });

    return {
      ok: false,
      reason: config_error,
      access_token: null,
      config,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      reason: "dry_run",
      access_token: "dry-run-token",
      config,
    };
  }

  let assertion = null;

  try {
    assertion = buildJwtAssertion(config);
  } catch (error) {
    logger.warn("docusign.jwt_build_failed", {
      reason: clean(error?.message) || "jwt_build_failed",
      env_name: config.env_name,
    });

    await recordSystemAlert({
      subsystem: "docusign",
      code: "jwt_build_failed",
      severity: "high",
      retryable: false,
      summary: `DocuSign JWT build failed: ${clean(error?.message) || "jwt_build_failed"}`,
      dedupe_key: "docusign_jwt_build_failed",
      metadata: {
        env_name: config.env_name,
      },
    });

    return {
      ok: false,
      reason: "jwt_build_failed",
      access_token: null,
      config,
    };
  }

  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion,
  });

  const token_result = await docusignFetchJson(
    `${config.oauth_base_uri}/oauth/token`,
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
    logger.warn("docusign.token_request_failed", {
      env_name: config.env_name,
      status_code: token_result.status_code,
      error: token_result.error,
    });

    await recordSystemAlert({
      subsystem: "docusign",
      code: "token_request_failed",
      severity: "high",
      retryable: true,
      summary: "DocuSign token request failed.",
      dedupe_key: `docusign_token_${clean(token_result.status_code) || "unknown"}`,
      metadata: {
        env_name: config.env_name,
        status_code: token_result.status_code,
        error: token_result.error,
      },
    });

    return {
      ok: false,
      reason: "token_request_failed",
      access_token: null,
      config,
      raw: token_result.error,
    };
  }

  return {
    ok: true,
    reason: "token_ready",
    access_token: clean(token_result.data?.access_token),
    config,
    raw: token_result.data,
  };
}

export async function verifyDocusignAuth({ dry_run = false } = {}) {
  const result = await getAccessToken({ dry_run });

  return {
    ok: Boolean(result?.ok),
    reason: result?.reason || "docusign_auth_failed",
    dry_run: Boolean(dry_run),
    env_name: result?.config?.env_name || null,
    access_token_present: Boolean(clean(result?.access_token)),
    raw: result?.raw || null,
  };
}

function buildEnvelopeDefinition({
  subject,
  documents = [],
  signers = [],
  template_id = null,
  email_blurb = "",
  metadata = {},
  status = "created",
} = {}) {
  const normalized_documents = safeArray(documents).map(normalizeDocument);
  const normalized_signers = safeArray(signers).map(normalizeSigner);
  const normalized_metadata = normalizeMetadata(metadata);
  const has_template = Boolean(clean(template_id));

  const custom_fields = Object.keys(normalized_metadata).length
    ? {
        textCustomFields: Object.entries(normalized_metadata).map(([name, value]) => ({
          name: clean(name),
          value: clean(value),
          show: "true",
        })),
      }
    : undefined;

  const recipients = {
    signers: normalized_signers.map((signer) => ({
      email: signer.email,
      name: signer.name,
      recipientId: signer.signer_id,
      routingOrder: signer.routing_order,
      roleName: signer.role_name || undefined,
    })),
  };

  if (has_template) {
    return {
      emailSubject: clean(subject),
      emailBlurb: clean(email_blurb) || undefined,
      templateId: clean(template_id),
      status,
      templateRoles: normalized_signers.map((signer) => ({
        email: signer.email,
        name: signer.name,
        roleName: signer.role_name || "Signer",
        routingOrder: signer.routing_order,
      })),
      customFields: custom_fields,
    };
  }

  return {
    emailSubject: clean(subject),
    emailBlurb: clean(email_blurb) || undefined,
    status,
    documents: normalized_documents.map((doc) => ({
      documentBase64: doc.file_base64,
      name: doc.name,
      fileExtension: doc.file_extension,
      documentId: doc.document_id,
    })),
    recipients,
    customFields: custom_fields,
  };
}

export async function createEnvelope({
  subject,
  documents = [],
  signers = [],
  template_id = null,
  email_blurb = "",
  metadata = {},
  dry_run = false,
} = {}) {
  const validation = validateEnvelopeInput({
    subject,
    documents,
    signers,
    template_id,
  });

  if (!validation.ok) {
    logger.warn("docusign.create_envelope_invalid_input", {
      reason: validation.reason,
      subject: clean(subject),
      template_id: clean(template_id) || null,
      documents_count: safeArray(documents).length,
      signers_count: safeArray(signers).length,
    });

    return {
      ok: false,
      dry_run: Boolean(dry_run),
      reason: validation.reason,
      envelope_id: null,
      status: null,
      raw: null,
    };
  }

  const normalized_documents = safeArray(documents).map(normalizeDocument);
  const normalized_signers = safeArray(signers).map(normalizeSigner);
  const normalized_metadata = normalizeMetadata(metadata);

  const payload = {
    subject: clean(subject),
    template_id: clean(template_id) || null,
    email_blurb: clean(email_blurb) || null,
    documents: normalized_documents,
    signers: normalized_signers,
    metadata: normalized_metadata,
    documents_count: normalized_documents.length,
    signers_count: normalized_signers.length,
    dry_run: Boolean(dry_run),
  };

  logger.info("docusign.create_envelope_requested", {
    subject: payload.subject,
    template_id: payload.template_id,
    documents_count: payload.documents_count,
    signers_count: payload.signers_count,
    dry_run: payload.dry_run,
  });

  if (dry_run) {
    return {
      ok: true,
      dry_run: true,
      reason: "dry_run",
      envelope_id: null,
      status: "created_not_sent",
      raw: payload,
    };
  }

  const auth_result = await getAccessToken({ dry_run: false });

  if (!auth_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: auth_result.reason,
      envelope_id: null,
      status: null,
      raw: auth_result.raw || null,
    };
  }

  const envelope_definition = buildEnvelopeDefinition({
    subject: payload.subject,
    documents: normalized_documents,
    signers: normalized_signers,
    template_id: payload.template_id,
    email_blurb: payload.email_blurb,
    metadata: normalized_metadata,
    status: "created",
  });

  const create_result = await docusignFetchJson(
    `${auth_result.config.base_uri}/v2.1/accounts/${auth_result.config.account_id}/envelopes`,
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
      error: create_result.error,
    });

    return {
      ok: false,
      dry_run: false,
      reason: "create_envelope_failed",
      envelope_id: null,
      status: null,
      raw: create_result.error,
    };
  }

  return {
    ok: true,
    dry_run: false,
    reason: "envelope_created",
    envelope_id: clean(create_result.data?.envelopeId) || null,
    status: clean(create_result.data?.status) || "created",
    raw: create_result.data,
  };
}

export async function sendEnvelope({
  envelope_id,
  dry_run = false,
} = {}) {
  const normalized_envelope_id = clean(envelope_id);

  if (!normalized_envelope_id) {
    logger.warn("docusign.send_envelope_invalid_input", {
      reason: "missing_envelope_id",
      dry_run: Boolean(dry_run),
    });

    return {
      ok: false,
      dry_run: Boolean(dry_run),
      reason: "missing_envelope_id",
      envelope_id: null,
      status: null,
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
    };
  }

  const auth_result = await getAccessToken({ dry_run: false });

  if (!auth_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: auth_result.reason,
      envelope_id: normalized_envelope_id,
      status: null,
      raw: auth_result.raw || null,
    };
  }

  const send_result = await docusignFetchJson(
    `${auth_result.config.base_uri}/v2.1/accounts/${auth_result.config.account_id}/envelopes/${normalized_envelope_id}`,
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
      error: send_result.error,
    });

    return {
      ok: false,
      dry_run: false,
      reason: "send_envelope_failed",
      envelope_id: normalized_envelope_id,
      status: null,
      raw: send_result.error,
    };
  }

  return {
    ok: true,
    dry_run: false,
    reason: "envelope_sent",
    envelope_id: normalized_envelope_id,
    status: clean(send_result.data?.status) || "sent",
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
      raw: null,
    };
  }

  const auth_result = await getAccessToken({ dry_run: false });

  if (!auth_result.ok) {
    return {
      ok: false,
      dry_run: false,
      reason: auth_result.reason,
      envelope_id: normalized_envelope_id,
      status: null,
      raw: auth_result.raw || null,
    };
  }

  const envelope_result = await docusignFetchJson(
    `${auth_result.config.base_uri}/v2.1/accounts/${auth_result.config.account_id}/envelopes/${normalized_envelope_id}`,
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
      raw: envelope_result.error,
    };
  }

  return {
    ok: true,
    dry_run: false,
    reason: "envelope_loaded",
    envelope_id: normalized_envelope_id,
    status: clean(envelope_result.data?.status) || null,
    raw: envelope_result.data,
  };
}

export default {
  createEnvelope,
  sendEnvelope,
  getEnvelope,
};
