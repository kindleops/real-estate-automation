// ─── textgrid.js ──────────────────────────────────────────────────────────
import crypto from "node:crypto";

import ENV from "@/lib/config/env.js";
import { recordSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { warn, info } from "@/lib/logging/logger.js";
import { normalizeUsPhoneToE164 } from "@/lib/sms/sanitize.js";
import { getSystemFlag } from "@/lib/system-control.js";

// Pre-send content guard patterns.
const BLANK_GREETING_RE = /(Hello|Hi|Hey|Hola)\s*,/i;
const UNRESOLVED_PLACEHOLDER_RE = /\{\{[^}]+\}\}/;

// ══════════════════════════════════════════════════════════════════════════
// CONFIG & ENV VALIDATION
// ══════════════════════════════════════════════════════════════════════════

const TEXTGRID_API_ORIGIN = "https://api.textgrid.com";
const TEXTGRID_API_VERSION_PATH = "/2010-04-01";
const TEXTGRID_ACCOUNT_SID_PLACEHOLDER = "{ACCOUNT_SID}";
const TEXTGRID_BASE_URL = `${TEXTGRID_API_ORIGIN}${TEXTGRID_API_VERSION_PATH}`;

const REQUEST_TIMEOUT_MS = 15_000;
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
  constructor(message, { status, data, raw_text, endpoint, to, from, body } = {}) {
    super(message);
    this.name = "TextGridError";
    this.status = status ?? null;
    this.data = data ?? null;
    this.raw_text = raw_text ?? null;
    this.endpoint = endpoint ?? null;
    this.to = to ?? null;
    this.from = from ?? null;
    this.body = body ?? null;
  }
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
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

// ══════════════════════════════════════════════════════════════════════════
// NORMALIZATION
// ══════════════════════════════════════════════════════════════════════════

export function normalizePhone(value) {
  return normalizeUsPhoneToE164(value);
}

export function normalizeInboundTextgridPhone(value) {
  const digits = String(value ?? "").replace(/\D/g, "");
  return digits.length === 11 && digits.startsWith("1") ? digits.slice(1) : digits;
}

// ══════════════════════════════════════════════════════════════════════════
// RETRYABLE STATUS CLASSIFICATION
// ══════════════════════════════════════════════════════════════════════════

const RETRYABLE_STATUSES = new Set([408, 409, 420, 425, 429, 500, 502, 503, 504]);

function isRetryable(status) {
  return RETRYABLE_STATUSES.has(status);
}

// ══════════════════════════════════════════════════════════════════════════
// FAILURE BUCKET CLASSIFICATION
// ══════════════════════════════════════════════════════════════════════════

export function mapTextgridFailureBucket(result) {
  if (!result || result.ok || result.success) return null;

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
  seller_first_name = null,
}) {
  // ── System control gate ────────────────────────────────────────────────
  const sms_enabled = await getSystemFlag("outbound_sms_enabled", { failClosedOnError: false });
  if (!sms_enabled) {
    info("send.blocked_system_control", {
      flag: "outbound_sms_enabled",
      to_input: to,
      client_reference_id,
    });
    throw new TextGridError(
      "sendTextgridSMS: outbound_sms_enabled flag is false — send blocked by system_control",
      { to, from, body }
    );
  }

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

  // ── Content guards ────────────────────────────────────────────────────
  // Block messages with blank seller greeting ("Hello ,").
  if (BLANK_GREETING_RE.test(trimmed_body)) {
    info("send.blocked_missing_name", {
      reason: "blank_seller_greeting",
      client_reference_id,
      to: normalized_to,
    });
    throw new TextGridError(
      "sendTextgridSMS: message contains blank greeting (missing seller_first_name)",
      { to: normalized_to, from: normalized_from, body: trimmed_body }
    );
  }

  // Block messages with unresolved template placeholders.
  if (UNRESOLVED_PLACEHOLDER_RE.test(trimmed_body)) {
    info("send.blocked_missing_name", {
      reason: "unresolved_placeholder",
      client_reference_id,
      to: normalized_to,
    });
    throw new TextGridError(
      "sendTextgridSMS: message contains unresolved placeholder",
      { to: normalized_to, from: normalized_from, body: trimmed_body }
    );
  }

  // Block if explicit seller_first_name is empty.
  if (seller_first_name !== null && String(seller_first_name).trim() === "") {
    info("send.blocked_missing_name", {
      reason: "seller_first_name_blank",
      client_reference_id,
      to: normalized_to,
    });
    throw new TextGridError(
      "sendTextgridSMS: seller_first_name is blank — send blocked",
      { to: normalized_to, from: normalized_from, body: trimmed_body }
    );
  }

  try {
    if (!credentials.configured) {
      throw new TextGridError(
        `[TextGrid] Missing required env vars: ${credentials.missing.join(", ")}`,
        {
          endpoint: getTextgridSendEndpoint(),
          to: normalized_to,
          from: normalized_from,
          body: trimmed_body,
        }
      );
    }

    const send_endpoint = getTextgridSendEndpoint(credentials.account_sid);
    const payload = buildTextgridSendPayload({
      body: trimmed_body,
      from: normalized_from,
      to: normalized_to,
    });

    const response = await fetch(send_endpoint, {
      method: "POST",
      headers: buildTextgridSendHeaders(credentials),
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    }).catch((error) => {
      throw new TextGridError(
        clean(error?.message) || "TextGrid network request failed",
        {
          endpoint: send_endpoint,
          to: normalized_to,
          from: normalized_from,
          body: trimmed_body,
        }
      );
    });

    const text = await response.text();

    console.log("TEXTGRID STATUS:", response.status);
    console.log("TEXTGRID RESPONSE:", text);

    let data = null;
    try {
      data = JSON.parse(text);
    } catch {
      throw new TextGridError(`Invalid JSON response: ${text}`, {
        status: response.status,
        raw_text: text,
        endpoint: send_endpoint,
        to: normalized_to,
        from: normalized_from,
        body: trimmed_body,
      });
    }

    if (!response.ok) {
      throw new TextGridError(`TextGrid HTTP failure: ${text}`, {
        status: response.status,
        data,
        raw_text: text,
        endpoint: send_endpoint,
        to: normalized_to,
        from: normalized_from,
        body: trimmed_body,
      });
    }

    const sid = clean(data?.sid);
    if (!sid) {
      throw new TextGridError(`Missing SID (NOT SENT): ${text}`, {
        status: response.status,
        data,
        raw_text: text,
        endpoint: send_endpoint,
        to: normalized_to,
        from: normalized_from,
        body: trimmed_body,
      });
    }

    const provider_status = lower(data?.status);
    if (["failed", "undelivered"].includes(provider_status)) {
      throw new TextGridError(`Carrier rejected message: ${text}`, {
        status: response.status,
        data,
        raw_text: text,
        endpoint: send_endpoint,
        to: normalized_to,
        from: normalized_from,
        body: trimmed_body,
      });
    }

    return {
      success: true,
      ok: true,
      provider: "textgrid",
      sid,
      message_id: sid,
      provider_message_id: sid,
      status: clean(data?.status) || "sent",
      raw: data,
      to: normalized_to,
      from: normalized_from,
      body: trimmed_body,
      endpoint: send_endpoint,
    };
  } catch (error) {
    const tge =
      error instanceof TextGridError
        ? error
        : new TextGridError(clean(error?.message) || "Unknown TextGrid error", {
            endpoint: getTextgridSendEndpoint(credentials.account_sid || null),
            to: normalized_to,
            from: normalized_from,
            body: trimmed_body,
          });

    warn("textgrid.send_failed", {
      to_input: to,
      from_input: from,
      to: tge.to || normalized_to,
      from: tge.from || normalized_from,
      status: tge.status,
      message: tge.message,
      error_data: tge.data,
      raw_text: tge.raw_text,
      endpoint: tge.endpoint || getTextgridSendEndpoint(credentials.account_sid || null),
      resource: TEXTGRID_MESSAGES_RESOURCE,
      client_reference_id,
    });

    try {
      await recordSystemAlert({
        subsystem: "textgrid",
        code: "send_failed",
        severity: tge.status && tge.status >= 500 ? "high" : "warning",
        retryable: Boolean(tge.status ? isRetryable(tge.status) : true),
        summary: `TextGrid send failed: ${tge.message}`,
        dedupe_key: `textgrid_send_${clean(tge.status) || "unknown"}`,
        affected_ids: [tge.to || normalized_to, tge.from || normalized_from],
        metadata: {
          status: tge.status,
          data: tge.data,
          raw_text: tge.raw_text,
          endpoint: tge.endpoint || getTextgridSendEndpoint(credentials.account_sid || null),
        },
      });
    } catch (alert_error) {
      warn("textgrid.send_failed_alert_record_failed", {
        message: clean(alert_error?.message) || "unknown",
        original_message: tge.message,
        original_status: tge.status,
        endpoint: tge.endpoint || getTextgridSendEndpoint(credentials.account_sid || null),
      });
    }

    throw tge;
  }
}
