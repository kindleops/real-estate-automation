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

  // ── diagnostics (safe to log — no secrets) ────────────────────────────
  const diagnostics = {
    signature_header: signature_header_name || (normalized_sig ? "present_unknown_header" : "missing"),
    content_type: normalized_ct || "unknown",
    request_path: (() => {
      try {
        return new URL(request_url).pathname;
      } catch {
        return request_url || "unknown";
      }
    })(),
    raw_body_present: Boolean(raw_body),
    raw_body_length: String(raw_body ?? "").length,
    auth_token_configured: Boolean(auth_token),
    webhook_secret_configured: Boolean(webhook_secret),
    is_form_encoded,
    form_params_count: form_params ? Object.keys(form_params).length : 0,
    canonical_url_base: canonical_url.split("?")[0],
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
      diagnostics,
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
      diagnostics,
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
        diagnostics: { ...diagnostics, mode: "twilio+auth_token" },
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
        diagnostics: { ...diagnostics, mode: "twilio+webhook_secret" },
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
        diagnostics: { ...diagnostics, mode: "raw_body+auth_token" },
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
        diagnostics: { ...diagnostics, mode: "raw_body+webhook_secret" },
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
    diagnostics: {
      ...diagnostics,
      modes_tried,
      failure_reason: "no_mode_produced_matching_digest",
    },
  };
}
