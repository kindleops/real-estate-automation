// ─── textgrid.js ──────────────────────────────────────────────────────────
import crypto from "node:crypto";
import axios from "axios";

import ENV from "@/lib/config/env.js";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { warn } from "@/lib/logging/logger.js";

// ══════════════════════════════════════════════════════════════════════════
// CONFIG & ENV VALIDATION
// ══════════════════════════════════════════════════════════════════════════

const TEXTGRID_API_ORIGIN = "https://api.textgrid.com";
const TEXTGRID_API_VERSION_PATH = "/2010-04-01";
const TEXTGRID_ACCOUNT_SID_PLACEHOLDER = "{ACCOUNT_SID}";
const TEXTGRID_BASE_URL = `${TEXTGRID_API_ORIGIN}${TEXTGRID_API_VERSION_PATH}`;

const REQUEST_TIMEOUT_MS = 15_000;
const MAX_RETRIES = 4;
const RETRY_BASE_DELAY_MS = 300;
const RETRY_MAX_DELAY_MS = 10_000;
// TextGrid send requests must use the fixed, Twilio-compatible REST path:
//   POST /2010-04-01/Accounts/{AccountSid}/Messages.json
const TEXTGRID_MESSAGES_RESOURCE = "/Messages.json";

const TEXTGRID_PROVIDER_CAPABILITIES = Object.freeze({
  message_status_lookup: {
    supported: false,
    reason: "no_verified_public_textgrid_message_status_lookup_endpoint",
  },
});

// ══════════════════════════════════════════════════════════════════════════
// STRUCTURED ERROR
// ══════════════════════════════════════════════════════════════════════════

export class TextGridError extends Error {
  constructor(message, { status, data } = {}) {
    super(message);
    this.name = "TextGridError";
    this.status = status ?? null;
    this.data = data ?? null;
  }
}

function clean(value) {
  return String(value ?? "").trim();
}

function safeEqual(left, right) {
  const leftBuffer = Buffer.from(String(left ?? ""), "utf8");
  const rightBuffer = Buffer.from(String(right ?? ""), "utf8");

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function getTextgridSendCredentials() {
  const account_sid = clean(ENV.TEXTGRID_ACCOUNT_SID || process.env.TEXTGRID_ACCOUNT_SID);
  const auth_token = clean(ENV.TEXTGRID_AUTH_TOKEN || process.env.TEXTGRID_AUTH_TOKEN);
  const missing = [];

  if (!account_sid) missing.push("TEXTGRID_ACCOUNT_SID");
  if (!auth_token) missing.push("TEXTGRID_AUTH_TOKEN");

  return {
    account_sid,
    auth_token,
    configured: missing.length === 0,
    missing,
  };
}

export function getTextgridProviderCapabilities() {
  return {
    message_status_lookup: {
      ...TEXTGRID_PROVIDER_CAPABILITIES.message_status_lookup,
    },
  };
}

export function getTextgridSendCredentialStatus() {
  const credentials = getTextgridSendCredentials();

  return {
    configured: credentials.configured,
    missing: credentials.missing,
    account_sid_present: Boolean(credentials.account_sid),
    auth_token_present: Boolean(credentials.auth_token),
    base_url: TEXTGRID_BASE_URL,
    send_endpoint: getTextgridSendEndpoint(credentials.account_sid),
  };
}

export function hasTextgridSendCredentials() {
  return getTextgridSendCredentials().configured;
}

// Build the fixed TextGrid send endpoint.
//
// The provider was previously configurable enough to drift onto incorrect
// routes. Sending is now pinned to TextGrid's versioned Messages.json path.
export function getTextgridSendEndpoint(account_sid = null) {
  const sid = clean(
    account_sid ||
      ENV.TEXTGRID_ACCOUNT_SID ||
      process.env.TEXTGRID_ACCOUNT_SID
  );
  const account_segment = sid ? encodeURIComponent(sid) : TEXTGRID_ACCOUNT_SID_PLACEHOLDER;
  return `${TEXTGRID_BASE_URL}/Accounts/${account_segment}${TEXTGRID_MESSAGES_RESOURCE}`;
}

export function getTextgridWebhookSecret() {
  return clean(ENV.TEXTGRID_WEBHOOK_SECRET || process.env.TEXTGRID_WEBHOOK_SECRET);
}

export function hasTextgridWebhookSecret() {
  return Boolean(getTextgridWebhookSecret());
}

export function buildTextgridBearerToken({
  account_sid = ENV.TEXTGRID_ACCOUNT_SID || process.env.TEXTGRID_ACCOUNT_SID,
  auth_token = ENV.TEXTGRID_AUTH_TOKEN || process.env.TEXTGRID_AUTH_TOKEN,
} = {}) {
  const normalized_account_sid = clean(account_sid);
  const normalized_auth_token = clean(auth_token);

  if (!normalized_account_sid || !normalized_auth_token) {
    return "";
  }

  return Buffer.from(
    `${normalized_account_sid}:${normalized_auth_token}`,
    "utf8"
  ).toString("base64");
}

export function buildTextgridSendHeaders(credentials = {}) {
  return {
    Authorization: `Bearer ${buildTextgridBearerToken(credentials)}`,
    "Content-Type": "application/json",
  };
}

export function buildTextgridSendPayload({
  body = "",
  from = "",
  to = "",
} = {}) {
  return {
    body: String(body ?? ""),
    from: clean(from),
    to: clean(to),
  };
}

function buildTextgridWebhookDigests(raw_body, webhook_secret) {
  const body = String(raw_body ?? "");
  const secret = clean(webhook_secret);

  return {
    hex: crypto.createHmac("sha1", secret).update(body, "utf8").digest("hex"),
    base64: crypto.createHmac("sha1", secret).update(body, "utf8").digest("base64"),
  };
}

export function verifyTextgridWebhookSignature({
  raw_body = "",
  signature = "",
  webhook_secret = getTextgridWebhookSecret(),
} = {}) {
  const normalized_signature = clean(signature);
  const normalized_secret = clean(webhook_secret);

  if (!normalized_secret) {
    return {
      ok: true,
      verified: false,
      required: false,
      algorithm: "HMAC-SHA1",
      reason: "webhook_secret_not_configured",
      signature_present: Boolean(normalized_signature),
    };
  }

  if (!normalized_signature) {
    return {
      ok: false,
      verified: false,
      required: true,
      algorithm: "HMAC-SHA1",
      reason: "missing_signature",
      signature_present: false,
    };
  }

  const { hex, base64 } = buildTextgridWebhookDigests(raw_body, normalized_secret);
  const candidates = [
    hex,
    base64,
    `sha1=${hex}`,
    `sha1=${base64}`,
  ];
  const matched_signature = candidates.find((candidate) =>
    safeEqual(candidate, normalized_signature)
  );

  return {
    ok: Boolean(matched_signature),
    verified: Boolean(matched_signature),
    required: true,
    algorithm: "HMAC-SHA1",
    reason: matched_signature ? "verified" : "invalid_signature",
    signature_present: true,
  };
}

function toTextGridError(err) {
  const status = err?.response?.status ?? null;
  const data = err?.response?.data ?? null;
  const message = data?.message ?? err?.message ?? "Unknown TextGrid error";
  return new TextGridError(message, { status, data });
}

// ══════════════════════════════════════════════════════════════════════════
// NORMALIZATION
// ══════════════════════════════════════════════════════════════════════════

export function normalizePhone(value) {
  if (!value) return "";
  const digits = String(value).replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  if (digits.length === 10) return `+1${digits}`;
  return "";
}

export function normalizeInboundTextgridPhone(value) {
  const digits = String(value ?? "").replace(/\D/g, "");
  return digits.length === 11 && digits.startsWith("1") ? digits.slice(1) : digits;
}

// ══════════════════════════════════════════════════════════════════════════
// RETRY ENGINE — Exponential Backoff + Full Jitter
// ══════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const RETRYABLE_STATUSES = new Set([408, 409, 420, 425, 429, 500, 502, 503, 504]);

function isRetryable(status) {
  return RETRYABLE_STATUSES.has(status);
}

function calcBackoff(attempt) {
  const exponential = RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
  const capped = Math.min(RETRY_MAX_DELAY_MS, exponential);
  return Math.floor(Math.random() * capped);
}

async function requestWithRetry(config, attempt = 0) {
  try {
    return await axios({ timeout: REQUEST_TIMEOUT_MS, ...config });
  } catch (err) {
    const status = err?.response?.status ?? 0;
    if (attempt < MAX_RETRIES && isRetryable(status)) {
      await sleep(calcBackoff(attempt));
      return requestWithRetry(config, attempt + 1);
    }
    throw err;
  }
}

// ══════════════════════════════════════════════════════════════════════════
// FAILURE BUCKET CLASSIFICATION
// ══════════════════════════════════════════════════════════════════════════

export function mapTextgridFailureBucket(result) {
  if (!result || result.ok) return null;

  const status = result.error_status ?? 0;
  const msg = String(result.error_message ?? "").toLowerCase();

  if (msg.includes("opt out") || msg.includes("dnc")) return "DNC";
  if (msg.includes("spam")) return "Spam";
  if (
    msg.includes("invalid number") ||
    msg.includes("invalid destination") ||
    msg.includes("invalid sending number")
  ) {
    return "Hard Bounce";
  }
  if ([400, 404].includes(status)) return "Hard Bounce";
  if (RETRYABLE_STATUSES.has(status)) return "Soft Bounce";

  return "Other";
}

// ══════════════════════════════════════════════════════════════════════════
// SEND
// ══════════════════════════════════════════════════════════════════════════

export async function sendTextgridSMS({
  to,
  from,
  body,
  media_urls = [],
  client_reference_id = null,
  message_type = "sms",
}) {
  const normalized_to = normalizePhone(to);
  const normalized_from = normalizePhone(from);
  const credentials = getTextgridSendCredentials();

  if (!normalized_to) {
    throw new TextGridError(`sendTextgridSMS: invalid 'to' number — "${to}"`);
  }

  if (!normalized_from) {
    throw new TextGridError(`sendTextgridSMS: invalid 'from' number — "${from}"`);
  }

  const trimmed_body = String(body ?? "").trim();
  if (!trimmed_body) {
    throw new TextGridError("sendTextgridSMS: message body is empty");
  }

  if (!credentials.configured) {
    const missing_message =
      `[TextGrid] Missing required env vars: ${credentials.missing.join(", ")}`;

    const missing_endpoint = getTextgridSendEndpoint();

    warn("textgrid.send_failed", {
      to: normalized_to,
      from: normalized_from,
      status: null,
      message: missing_message,
      error_data: null,
      endpoint: missing_endpoint,
    });

    await recordSystemAlert({
      subsystem: "textgrid",
      code: "send_failed",
      severity: "high",
      retryable: true,
      summary: missing_message,
      dedupe_key: "textgrid_send_missing_credentials",
      affected_ids: [normalized_to, normalized_from],
      metadata: {
        missing: credentials.missing,
      },
    });

    return {
      ok: false,
      provider: "textgrid",
      message_id: null,
      status: "failed",
      error_message: missing_message,
      error_status: null,
      error_data: null,
      to: normalized_to,
      from: normalized_from,
      body: trimmed_body,
      endpoint: missing_endpoint,
    };
  }

  const send_endpoint = getTextgridSendEndpoint(credentials.account_sid);
  // TextGrid rejects the legacy extras we used to send here; only the core
  // body/from/to JSON object is accepted on the Messages.json endpoint.
  const payload = buildTextgridSendPayload({
    body: trimmed_body,
    from: normalized_from,
    to: normalized_to,
  });

  try {
    const res = await requestWithRetry({
      method: "post",
      url: send_endpoint,
      data: JSON.stringify(payload),
      headers: buildTextgridSendHeaders(credentials),
    });

    const data = res.data ?? {};

    return {
      ok: true,
      provider: "textgrid",
      message_id: data.sid ?? data.id ?? data.message_id ?? data.message_sid ?? null,
      status: data.status ?? "sent",
      raw: data,
      to: normalized_to,
      from: normalized_from,
      body: trimmed_body,
      endpoint: send_endpoint,
    };
  } catch (err) {
    const tge = toTextGridError(err);

    warn("textgrid.send_failed", {
      to_input: to,
      from_input: from,
      to: normalized_to,
      from: normalized_from,
      status: tge.status,
      message: tge.message,
      error_data: tge.data,
      endpoint: send_endpoint,
      resource: TEXTGRID_MESSAGES_RESOURCE,
      client_reference_id,
    });

    await recordSystemAlert({
      subsystem: "textgrid",
      code: "send_failed",
      severity: tge.status && tge.status >= 500 ? "high" : "warning",
      retryable: Boolean(tge.status ? isRetryable(tge.status) : true),
      summary: `TextGrid send failed: ${tge.message}`,
      dedupe_key: `textgrid_send_${clean(tge.status) || "unknown"}`,
      affected_ids: [normalized_to, normalized_from],
      metadata: {
        status: tge.status,
        data: tge.data,
        endpoint: send_endpoint,
      },
    });

    return {
      ok: false,
      provider: "textgrid",
      message_id: null,
      status: "failed",
      error_message: tge.message,
      error_status: tge.status,
      error_data: tge.data,
      to: normalized_to,
      from: normalized_from,
      body: trimmed_body,
      endpoint: send_endpoint,
    };
  }
}
