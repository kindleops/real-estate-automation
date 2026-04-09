// ─── textgrid-verify-webhook.js ───────────────────────────────────────────
//
// Shared signature verification for all TextGrid webhook endpoints.
//
// TextGrid is Twilio-compatible. Twilio's signing algorithm:
//   1. Start with the canonical public URL of the webhook endpoint
//   2. For application/x-www-form-urlencoded POST requests:
//      sort all POST params alphabetically by key, then concatenate
//      key+value (no separator) and append to the URL string
//   3. HMAC-SHA1 the resulting string using the Auth Token as the key
//   4. Base64-encode the digest
//
// Reference: https://www.twilio.com/docs/usage/webhooks/webhooks-security
//
// This module tries multiple (algorithm, secret) pairs and accepts the first
// match so we survive secret rotation and provider quirks:
//   Mode A – Twilio algorithm + TEXTGRID_AUTH_TOKEN    (primary, correct)
//   Mode B – Twilio algorithm + TEXTGRID_WEBHOOK_SECRET (secondary)
//   Mode C – Raw-body HMAC-SHA1 + TEXTGRID_AUTH_TOKEN   (fallback)
//   Mode D – Raw-body HMAC-SHA1 + TEXTGRID_WEBHOOK_SECRET (fallback)
//
// Diagnostics logged on failure include all context except secrets.

import crypto from "node:crypto";
import ENV from "@/lib/config/env.js";

// ── helpers ────────────────────────────────────────────────────────────────

function clean(value) {
  return String(value ?? "").trim();
}

function safeEqual(left, right) {
  const a = Buffer.from(String(left ?? ""), "utf8");
  const b = Buffer.from(String(right ?? ""), "utf8");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function getAuthToken() {
  return clean(ENV.TEXTGRID_AUTH_TOKEN || process.env.TEXTGRID_AUTH_TOKEN);
}

function getWebhookSecret() {
  return clean(ENV.TEXTGRID_WEBHOOK_SECRET || process.env.TEXTGRID_WEBHOOK_SECRET);
}

export function getTextgridWebhookSignatureMode(override = null) {
  const normalized = clean(
    override ||
      process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE ||
      ENV.TEXTGRID_WEBHOOK_SIGNATURE_MODE
  ).toLowerCase();

  if (["strict", "observe", "off"].includes(normalized)) {
    return normalized;
  }

  return "strict";
}

// ── URL canonicalization ───────────────────────────────────────────────────
//
// Next.js on Vercel may surface an internal hostname in request.url.
// We reconstruct the canonical public URL using APP_BASE_URL so the
// signing material exactly matches what was configured in TextGrid's
// webhook settings.

// override_base: explicit base URL (for testing without relying on process.env at call time)
export function buildCanonicalWebhookUrl(request_url, override_base = null) {
  const base = clean(
    override_base || ENV.APP_BASE_URL || process.env.APP_BASE_URL
  ).replace(/\/+$/, "");

  try {
    const url = new URL(request_url);
    if (base) {
      // Keep path + query string; replace scheme+host with public base.
      return base + url.pathname + (url.search || "");
    }
    return request_url;
  } catch {
    return request_url;
  }
}

function getRequestPath(request_url = "") {
  try {
    return new URL(request_url).pathname;
  } catch {
    return request_url || "unknown";
  }
}

function getParsedFormParamKeys(form_params = null) {
  if (!form_params || typeof form_params !== "object") return [];
  return Object.keys(form_params).sort();
}

export function buildTextgridWebhookDiagnostics({
  request_url = "",
  raw_body = "",
  form_params = null,
  content_type = "",
  signature = "",
  signature_header_name = null,
  auth_token = getAuthToken(),
  webhook_secret = getWebhookSecret(),
  canonical_url = "",
  modes_tried = [],
  failure_reason = null,
} = {}) {
  const normalized_sig = clean(signature);
  const normalized_ct = clean(content_type).toLowerCase();
  const parsed_form_param_keys = getParsedFormParamKeys(form_params);
  const resolved_canonical_url = clean(canonical_url) || buildCanonicalWebhookUrl(request_url);

  return {
    signature_header:
      signature_header_name || (normalized_sig ? "present_unknown_header" : "missing"),
    signature_header_name: signature_header_name || null,
    signature_header_present: Boolean(signature_header_name || normalized_sig),
    signature_header_length: normalized_sig.length,
    content_type: normalized_ct || "unknown",
    request_path: getRequestPath(request_url),
    request_url: request_url || "unknown",
    raw_body_present: Boolean(raw_body),
    raw_body_length: String(raw_body ?? "").length,
    auth_token_configured: Boolean(auth_token),
    webhook_secret_configured: Boolean(webhook_secret),
    is_form_encoded: normalized_ct.includes("application/x-www-form-urlencoded"),
    parsed_form_param_keys,
    form_params_count: parsed_form_param_keys.length,
    canonical_url_base: clean(resolved_canonical_url).split("?")[0] || null,
    modes_tried: Array.isArray(modes_tried) ? [...modes_tried] : [],
    failure_reason: clean(failure_reason) || null,
  };
}

export function buildTextgridWebhookBypassResult({
  request_url = "",
  raw_body = "",
  form_params = null,
  content_type = "",
  signature = "",
  signature_header_name = null,
  reason = "signature_verification_disabled",
  auth_token = getAuthToken(),
  webhook_secret = getWebhookSecret(),
} = {}) {
  return {
    ok: true,
    verified: false,
    required: false,
    algorithm: null,
    reason,
    signature_present: Boolean(clean(signature)),
    bypassed: true,
    diagnostics: buildTextgridWebhookDiagnostics({
      request_url,
      raw_body,
      form_params,
      content_type,
      signature,
      signature_header_name,
      auth_token,
      webhook_secret,
      failure_reason: reason,
    }),
  };
}

export function buildTextgridWebhookVerificationMeta({
  verification = {},
  mode = null,
  signature_header_name = null,
} = {}) {
  const signature_verification_mode = getTextgridWebhookSignatureMode(mode);
  const invalid_signature = Boolean(verification?.required && !verification?.ok);
  const signature_failure_reason =
    clean(
      verification?.diagnostics?.failure_reason ||
        (verification?.reason !== "verified" ? verification?.reason : "")
    ) || null;

  return {
    signature_verification_mode,
    signature_verified: Boolean(verification?.verified),
    signature_bypassed:
      Boolean(verification?.bypassed) ||
      signature_verification_mode === "off" ||
      (signature_verification_mode === "observe" && invalid_signature),
    signature_failure_reason,
    signature_header_name:
      signature_header_name || verification?.diagnostics?.signature_header_name || null,
    signature_unverified_observe_mode:
      signature_verification_mode === "observe" && invalid_signature,
  };
}

// ── signing algorithms ─────────────────────────────────────────────────────

// Twilio/TextGrid standard: HMAC-SHA1(url + sorted_form_params, secret) → base64
function buildTwilioSignature(canonical_url, form_params, secret) {
  const sorted_keys = Object.keys(form_params || {}).sort();
  let signing_string = canonical_url;
  for (const key of sorted_keys) {
    signing_string += key + String(form_params[key] ?? "");
  }
  return crypto
    .createHmac("sha1", secret)
    .update(signing_string, "utf8")
    .digest("base64");
}

// Simpler scheme some providers use: HMAC-SHA1(raw_body, secret)
// Returns all candidate representations so we can match any format.
function buildRawBodyCandidates(raw_body, secret) {
  const body = String(raw_body ?? "");
  const hex = crypto.createHmac("sha1", secret).update(body, "utf8").digest("hex");
  const b64 = crypto.createHmac("sha1", secret).update(body, "utf8").digest("base64");
  return [hex, b64, `sha1=${hex}`, `sha1=${b64}`];
}

// ── main export ────────────────────────────────────────────────────────────

/**
 * Verify a TextGrid (Twilio-compatible) webhook request signature.
 *
 * @param {object} opts
 * @param {string}  opts.request_url          Full URL from request.url
 * @param {string}  opts.raw_body             Raw request body string
 * @param {object|null} opts.form_params      Parsed form fields (decoded key/value);
 *                                            null if not form-encoded
 * @param {string}  opts.content_type         Value of Content-Type header
 * @param {string}  opts.signature            Signature value extracted from headers
 * @param {string|null} opts.signature_header_name  Which header the sig came from
 * @param {string}  [opts.auth_token]         Override TEXTGRID_AUTH_TOKEN
 * @param {string}  [opts.webhook_secret]     Override TEXTGRID_WEBHOOK_SECRET
 *
 * @returns {{
 *   ok: boolean,
 *   verified: boolean,
 *   required: boolean,
 *   algorithm: string|null,
 *   reason: string,
 *   signature_present: boolean,
 *   diagnostics: object
 * }}
 */
export function verifyTextgridWebhookRequest({
  request_url = "",
  raw_body = "",
  form_params = null,
  content_type = "",
  signature = "",
  signature_header_name = null,
  auth_token = getAuthToken(),
  webhook_secret = getWebhookSecret(),
} = {}) {
  const normalized_sig = clean(signature);
  const normalized_ct = clean(content_type).toLowerCase();
  const is_form_encoded = normalized_ct.includes("application/x-www-form-urlencoded");
  const canonical_url = buildCanonicalWebhookUrl(request_url);
  const diagnostics_input = {
    request_url,
    raw_body,
    form_params,
    content_type: normalized_ct,
    signature: normalized_sig,
    signature_header_name,
    auth_token,
    webhook_secret,
    canonical_url,
  };

  const has_any_secret = Boolean(auth_token || webhook_secret);

  if (!has_any_secret) {
    return {
      ok: true,
      verified: false,
      required: false,
      algorithm: null,
      reason: "no_secrets_configured",
      signature_present: Boolean(normalized_sig),
      diagnostics: buildTextgridWebhookDiagnostics({
        ...diagnostics_input,
        failure_reason: "no_secrets_configured",
      }),
    };
  }

  if (!normalized_sig) {
    return {
      ok: false,
      verified: false,
      required: true,
      algorithm: null,
      reason: "missing_signature",
      signature_present: false,
      diagnostics: buildTextgridWebhookDiagnostics({
        ...diagnostics_input,
        failure_reason: "missing_signature",
      }),
    };
  }

  // Use sorted parsed params for Twilio mode when the body is form-encoded.
  // For JSON / plain-text bodies Twilio's spec says params are empty, so the
  // signing string is just the URL.
  const twilio_params = is_form_encoded && form_params ? form_params : {};
  const modes_tried = [];

  // Mode A – Twilio + auth_token (primary, most likely correct for TextGrid)
  if (auth_token) {
    const expected = buildTwilioSignature(canonical_url, twilio_params, auth_token);
    modes_tried.push("twilio+auth_token");
    if (safeEqual(expected, normalized_sig)) {
      return {
        ok: true,
        verified: true,
        required: true,
        algorithm: "HMAC-SHA1-Twilio",
        reason: "verified",
        signature_present: true,
        diagnostics: {
          ...buildTextgridWebhookDiagnostics({
            ...diagnostics_input,
            modes_tried,
          }),
          mode: "twilio+auth_token",
        },
      };
    }
  }

  // Mode B – Twilio + webhook_secret
  if (webhook_secret && webhook_secret !== auth_token) {
    const expected = buildTwilioSignature(canonical_url, twilio_params, webhook_secret);
    modes_tried.push("twilio+webhook_secret");
    if (safeEqual(expected, normalized_sig)) {
      return {
        ok: true,
        verified: true,
        required: true,
        algorithm: "HMAC-SHA1-Twilio",
        reason: "verified",
        signature_present: true,
        diagnostics: {
          ...buildTextgridWebhookDiagnostics({
            ...diagnostics_input,
            modes_tried,
          }),
          mode: "twilio+webhook_secret",
        },
      };
    }
  }

  // Mode C – Raw body HMAC + auth_token
  if (auth_token) {
    const candidates = buildRawBodyCandidates(raw_body, auth_token);
    modes_tried.push("raw_body+auth_token");
    if (candidates.some((c) => safeEqual(c, normalized_sig))) {
      return {
        ok: true,
        verified: true,
        required: true,
        algorithm: "HMAC-SHA1-Raw",
        reason: "verified",
        signature_present: true,
        diagnostics: {
          ...buildTextgridWebhookDiagnostics({
            ...diagnostics_input,
            modes_tried,
          }),
          mode: "raw_body+auth_token",
        },
      };
    }
  }

  // Mode D – Raw body HMAC + webhook_secret
  if (webhook_secret && webhook_secret !== auth_token) {
    const candidates = buildRawBodyCandidates(raw_body, webhook_secret);
    modes_tried.push("raw_body+webhook_secret");
    if (candidates.some((c) => safeEqual(c, normalized_sig))) {
      return {
        ok: true,
        verified: true,
        required: true,
        algorithm: "HMAC-SHA1-Raw",
        reason: "verified",
        signature_present: true,
        diagnostics: {
          ...buildTextgridWebhookDiagnostics({
            ...diagnostics_input,
            modes_tried,
          }),
          mode: "raw_body+webhook_secret",
        },
      };
    }
  }

  return {
    ok: false,
    verified: false,
    required: true,
    algorithm: "HMAC-SHA1",
    reason: "invalid_signature",
    signature_present: true,
    diagnostics: buildTextgridWebhookDiagnostics({
      ...diagnostics_input,
      modes_tried,
      failure_reason: "no_mode_produced_matching_digest",
    }),
  };
}
