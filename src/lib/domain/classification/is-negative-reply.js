// ─── is-negative-reply.js ────────────────────────────────────────────────
// Detects inbound messages that clearly signal Stage-1 negative intent:
// hard opt-outs, not-interested, wrong-person, or strong objections.
//
// Used to trigger immediate queue cancellation and outreach suppression.
// Conservative by design: false-positives on genuine negatives are less
// damaging than duplicate touches after a seller said stop.

const EXACT_NEGATIVE_PHRASES = new Set([
  "no",
  "nope",
  "n",
  "stop",
  "stop it",
  "stop texting",
  "stop texting me",
  "stop messaging me",
  "unsubscribe",
  "remove me",
  "remove",
  "opt out",
  "opt-out",
  "quit",
  "end",
  "cancel",
  "i said no",
  "i already said no",
  "not interested",
  "not now",
  "no thanks",
  "no thank you",
  "wrong number",
  "wrong person",
  "wrong house",
  "who is this",
  "who are you",
  "dont contact me",
  "do not contact me",
  "do not call",
  "do not text",
  "do not text me",
  "leave me alone",
  "go away",
  "please stop",
  "please remove me",
  "not for sale",
  "not selling",
  "im not interested",
  "i am not interested",
  "not interested in selling",
]);

// Pattern-based check for multi-word negative replies
const NEGATIVE_PATTERNS = [
  /\bnot\s+interested\b/i,
  /\bno[,.]?\s+thank\s*(you)?\b/i,
  /\bwrong\s+(number|person|house|address)\b/i,
  /\bstop\s+(texting|messaging|calling|contact(ing)?)\b/i,
  /\bplease\s+stop\b/i,
  /\bdon['']?t\s+(contact|text|call|message)\s*(me)?\b/i,
  /\bdo\s+not\s+(contact|text|call|message)\s*(me)?\b/i,
  /\bi\s+(already\s+)?said\s+no\b/i,
  /\bnot\s+selling\b/i,
  /\bnot\s+for\s+sale\b/i,
  /\bnot\s+interested\s+in\s+selling\b/i,
  /\bopt[\s-]?out\b/i,
  /\bremove\s+(me|my\s+number)\b/i,
  /\bleave\s+me\s+alone\b/i,
  /\bgo\s+away\b/i,
  /\bno\s+longer\s+(own|interested|available)\b/i,
  /\bi\s+said\s+no\b/i,
  /\bdont\s+contact\b/i,
  /\bunsubscribe\b/i,
];

/**
 * Normalizes a message body to ASCII-safe lowercase with punctuation stripped.
 * Preserves word boundaries.
 */
function normalizeBody(value) {
  return String(value ?? "")
    .toLowerCase()
    // Collapse Unicode apostrophes / smart quotes to ASCII
    .replace(/[\u2018\u2019\u201A\u201B']/g, "'")
    // Strip punctuation except apostrophes, keep spaces
    .replace(/[^\p{L}\p{N}'\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Returns true when `message_body` is a clear negative / opt-out reply.
 *
 * Conservative — short ambiguous replies like "ok" or "maybe" return false.
 * Intent: prevent duplicate outbound touches after explicit refusal.
 *
 * @param {string} message_body
 * @returns {boolean}
 */
export function isNegativeReply(message_body) {
  const normalized = normalizeBody(message_body);
  if (!normalized) return false;

  // Exact-phrase match (fastest path)
  if (EXACT_NEGATIVE_PHRASES.has(normalized)) return true;

  // Pattern match (handles variations)
  for (const pattern of NEGATIVE_PATTERNS) {
    if (pattern.test(normalized)) return true;
  }

  return false;
}

export default isNegativeReply;
