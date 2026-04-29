function clean(v) {
  return String(v ?? "").trim();
}

/**
 * Normalizes inbound message body aliases for diagnostics/inbound-replay.
 *
 * Aliases supported (first non-empty wins):
 *   - body
 *   - message_body
 *   - inbound_message_body
 *   - text
 *
 * Empty/whitespace-only inputs count as missing.
 */
export function normalizeInboundReplayExampleBody(example = null) {
  const candidates = [
    example?.body,
    example?.message_body,
    example?.inbound_message_body,
    example?.text,
  ];

  for (const candidate of candidates) {
    const normalized = clean(candidate);
    if (normalized) return normalized;
  }

  // Preserve existing behavior: empty/whitespace-only inputs count as missing.
  return "";
}
