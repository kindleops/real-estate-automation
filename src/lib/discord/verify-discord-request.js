/**
 * verify-discord-request.js
 *
 * Ed25519 signature verification for Discord interaction webhooks.
 *
 * Discord signs every inbound request with the app's Ed25519 private key.
 * The message to verify is:  timestamp_string + raw_body_string
 * The public key is the DISCORD_PUBLIC_KEY env var (64-char lowercase hex).
 *
 * Security properties:
 * - Uses node:crypto for constant-time operations where possible.
 * - Returns false (never throws) on any malformed input.
 * - Requires all four inputs to be non-empty strings.
 */

import crypto from "node:crypto";

// SubjectPublicKeyInfo DER prefix for Ed25519 (OID 1.3.101.112)
const ED25519_SPKI_PREFIX = Buffer.from("302a300506032b6570032100", "hex");

/**
 * Verify a Discord request's Ed25519 signature.
 *
 * @param {object} params
 * @param {string} params.publicKey  - Hex-encoded 32-byte Ed25519 public key.
 * @param {string} params.signature  - Hex-encoded 64-byte signature from X-Signature-Ed25519.
 * @param {string} params.timestamp  - Raw string from X-Signature-Timestamp.
 * @param {string} params.rawBody    - Raw UTF-8 request body string.
 * @returns {boolean}
 */
export function verifyDiscordRequest({ publicKey, signature, timestamp, rawBody }) {
  try {
    if (!publicKey || !signature || !timestamp || rawBody == null) return false;
    if (typeof publicKey !== "string" || typeof signature !== "string") return false;
    if (typeof timestamp !== "string" || typeof rawBody !== "string") return false;

    const keyBytes = Buffer.from(publicKey, "hex");
    const sigBytes = Buffer.from(signature, "hex");

    // Ed25519 public key must be exactly 32 bytes; signature must be 64 bytes.
    if (keyBytes.length !== 32 || sigBytes.length !== 64) return false;

    // Build the SPKI-wrapped public key so node:crypto can parse it.
    const spkiKey = crypto.createPublicKey({
      key:    Buffer.concat([ED25519_SPKI_PREFIX, keyBytes]),
      format: "der",
      type:   "spki",
    });

    // Message = timestamp (UTF-8) || body (UTF-8)
    const message = Buffer.concat([
      Buffer.from(timestamp, "utf8"),
      Buffer.from(rawBody, "utf8"),
    ]);

    return crypto.verify(null, message, spkiKey, sigBytes);
  } catch {
    // Any crypto error → treat as invalid signature.
    return false;
  }
}
