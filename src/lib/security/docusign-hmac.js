import crypto from "node:crypto";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

/**
 * Get a header value by name from either a Next.js Headers object (which has
 * case-insensitive `.get()`) or a plain JS object (scan keys manually).
 */
function getHeader(headers, name) {
  if (typeof headers?.get === "function") {
    return clean(headers.get(name));
  }

  const target = lower(name);
  for (const key of Object.keys(headers ?? {})) {
    if (lower(key) === target) {
      return clean(headers[key]);
    }
  }

  return "";
}

/**
 * Compute HMAC-SHA256 of `body` using `secret` and return the result as a
 * standard Base64 string (not URL-safe).
 */
export function computeDocusignHmacBase64(secret, body) {
  return crypto
    .createHmac("sha256", Buffer.from(secret, "utf8"))
    .update(Buffer.from(body, "utf8"))
    .digest("base64");
}

/**
 * Verify the DocuSign Connect HMAC-SHA256 signature.
 *
 * DocuSign sends:
 *   X-DocuSign-Signature-1  — Base64-encoded HMAC-SHA256 of the raw body
 *   x-authorization-digest  — Algorithm indicator (e.g. "HMAC-SHA256")
 *
 * Returns `{ ok: true, reason: "verified" }` on success or
 *         `{ ok: false, reason: <string> }` on any failure.
 *
 * Intentionally explicit about each failure mode so callers can log the
 * right reason without logging the secret.
 */
export function verifyDocusignConnectHmac(rawBody, headers, secret) {
  if (!clean(secret)) {
    return { ok: false, reason: "missing_docusign_hmac_secret" };
  }

  const signature = getHeader(headers, "x-docusign-signature-1");
  if (!signature) {
    return { ok: false, reason: "missing_docusign_hmac_signature" };
  }

  const digest_header = getHeader(headers, "x-authorization-digest");
  // Accept absent header (DocuSign omits it on some events) but reject
  // a header that explicitly names an algorithm other than SHA-256.
  if (digest_header && !lower(digest_header).includes("sha256") && !lower(digest_header).includes("sha-256")) {
    return { ok: false, reason: "unsupported_docusign_digest_algorithm" };
  }

  const expected_b64 = computeDocusignHmacBase64(secret, rawBody);

  let expected_buf;
  let received_buf;
  try {
    expected_buf = Buffer.from(expected_b64, "base64");
    received_buf = Buffer.from(signature, "base64");
  } catch {
    return { ok: false, reason: "invalid_docusign_hmac_encoding" };
  }

  // timingSafeEqual requires equal-length buffers. A length mismatch is itself
  // a definitive rejection, but we still run the comparison on equal-length
  // slices to keep constant-time behaviour in the common path.
  if (expected_buf.length !== received_buf.length) {
    return { ok: false, reason: "invalid_docusign_hmac_signature" };
  }

  if (!crypto.timingSafeEqual(expected_buf, received_buf)) {
    return { ok: false, reason: "invalid_docusign_hmac_signature" };
  }

  return { ok: true, reason: "verified" };
}
