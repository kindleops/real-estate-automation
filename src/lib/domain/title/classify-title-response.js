// ─── classify-title-response.js ──────────────────────────────────────────
function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

function wordCount(text) {
  return clean(text).split(/\s+/).filter(Boolean).length;
}

function sanitizeText(value, max = 4000) {
  return clean(value).slice(0, max);
}

function buildCombinedText({ event = "", subject = "", body = "" } = {}) {
  return `${clean(event)} ${clean(subject)} ${clean(body)}`.trim();
}

const TITLE_RESPONSE_PATTERNS = [
  {
    normalized_event: "file_opened",
    routing_status: "Opened",
    closing_status: null,
    should_mark_closed: false,
    reason: "title_file_opened",
    confidence: 0.95,
    phrases: [
      "file opened",
      "opened file",
      "opened escrow",
      "escrow opened",
      "open file",
      "order opened",
      "title opened",
      "we have opened",
      "opened with title",
      "title file is open",
      "preliminary file opened",
    ],
  },
  {
    normalized_event: "awaiting_docs",
    routing_status: "Waiting on Docs",
    closing_status: "Pending Docs",
    should_mark_closed: false,
    reason: "title_awaiting_docs",
    confidence: 0.93,
    phrases: [
      "awaiting docs",
      "need docs",
      "need documents",
      "missing docs",
      "please send",
      "requesting docs",
      "requesting documents",
      "need executed contract",
      "need payoff",
      "need vesting",
      "need hoa",
      "need probate docs",
      "need trust docs",
      "need seller information",
      "need wire instructions",
      "need statement",
      "please provide",
      "send over the contract",
    ],
  },
  {
    normalized_event: "title_review",
    routing_status: "Title Reviewing",
    closing_status: "Pending Docs",
    should_mark_closed: false,
    reason: "title_issue_or_review",
    confidence: 0.91,
    phrases: [
      "title issue",
      "cloud on title",
      "lien issue",
      "probate issue",
      "vesting issue",
      "judgment",
      "open permit",
      "curative",
      "curative needed",
      "title review",
      "under review",
      "requires review",
      "title commitment issue",
      "municipal lien",
      "code lien",
      "tax issue",
      "unreleased mortgage",
      "chain of title",
    ],
  },
  {
    normalized_event: "cleared",
    routing_status: "Clear to Close",
    closing_status: "Clear to Close",
    should_mark_closed: false,
    reason: "title_cleared",
    confidence: 0.96,
    phrases: [
      "cleared",
      "clear to close",
      "ctc",
      "ready to close",
      "clear file",
      "title is clear",
      "approved to close",
      "file is clear",
      "clear for closing",
      "good to close",
    ],
  },
  {
    normalized_event: "scheduled",
    routing_status: "Clear to Close",
    closing_status: "Scheduled",
    should_mark_closed: false,
    reason: "closing_scheduled",
    confidence: 0.94,
    phrases: [
      "scheduled",
      "closing scheduled",
      "close scheduled",
      "set to close",
      "closing date",
      "scheduled to close",
      "set for closing",
      "we are set for",
      "closing is set",
      "close on",
    ],
  },
  {
    normalized_event: "cancelled",
    routing_status: "Cancelled",
    closing_status: "Cancelled",
    should_mark_closed: false,
    reason: "title_or_closing_cancelled",
    confidence: 0.97,
    phrases: [
      "cancelled",
      "canceled",
      "terminated",
      "voided",
      "fell through",
      "won't proceed",
      "will not proceed",
      "file cancelled",
      "file canceled",
      "deal cancelled",
      "deal canceled",
      "transaction cancelled",
      "transaction canceled",
    ],
  },
  {
    normalized_event: "closed",
    routing_status: "Closed",
    closing_status: "Completed",
    should_mark_closed: true,
    reason: "closed_signal_detected",
    confidence: 0.98,
    phrases: [
      "closed",
      "closing complete",
      "deal complete",
      "file complete",
      "completed closing",
      "transaction closed",
      "closed successfully",
    ],
  },
  {
    normalized_event: "funded",
    routing_status: "Closed",
    closing_status: "Completed",
    should_mark_closed: true,
    reason: "funded_signal_detected",
    confidence: 0.98,
    phrases: [
      "funded",
      "funding complete",
      "wire sent",
      "wired",
      "disbursed",
      "disbursement complete",
      "funds released",
      "funds sent",
      "loan funded",
      "seller funded",
      "buyer funds received",
    ],
  },
  {
    normalized_event: "recorded",
    routing_status: "Closed",
    closing_status: "Completed",
    should_mark_closed: true,
    reason: "recorded_signal_detected",
    confidence: 0.97,
    phrases: [
      "recorded",
      "recording confirmed",
      "deed recorded",
      "document recorded",
      "deed is recorded",
      "has been recorded",
      "recording number",
    ],
  },
];

function detectByPatterns(combined_text = "") {
  for (const pattern of TITLE_RESPONSE_PATTERNS) {
    if (includesAny(combined_text, pattern.phrases)) {
      return {
        ok: true,
        normalized_event: pattern.normalized_event,
        routing_status: pattern.routing_status,
        closing_status: pattern.closing_status,
        should_mark_closed: pattern.should_mark_closed,
        reason: pattern.reason,
        confidence: pattern.confidence,
        source: "pattern",
      };
    }
  }

  return null;
}

function fallbackHeuristics({ event = "", subject = "", body = "" } = {}) {
  const combined = buildCombinedText({ event, subject, body });
  const text = lower(combined);

  if (!combined) {
    return {
      ok: true,
      normalized_event: "unclassified",
      routing_status: null,
      closing_status: null,
      should_mark_closed: false,
      reason: "empty_title_response",
      confidence: 0.2,
      source: "heuristic",
    };
  }

  if (wordCount(combined) <= 2) {
    return {
      ok: true,
      normalized_event: "unclassified",
      routing_status: null,
      closing_status: null,
      should_mark_closed: false,
      reason: "short_title_response",
      confidence: 0.35,
      source: "heuristic",
    };
  }

  if (text.includes("close")) {
    return {
      ok: true,
      normalized_event: "scheduled",
      routing_status: "Cleared",
      closing_status: "Scheduled to Close",
      should_mark_closed: false,
      reason: "generic_close_signal",
      confidence: 0.55,
      source: "heuristic",
    };
  }

  if (text.includes("title")) {
    return {
      ok: true,
      normalized_event: "title_review",
      routing_status: "Title Review",
      closing_status: "Pending Docs",
      should_mark_closed: false,
      reason: "generic_title_signal",
      confidence: 0.5,
      source: "heuristic",
    };
  }

  return {
    ok: true,
    normalized_event: "unclassified",
    routing_status: null,
    closing_status: null,
    should_mark_closed: false,
    reason: "unclassified_title_response",
    confidence: 0.4,
    source: "heuristic",
  };
}

export function classifyTitleResponse({
  event = "",
  subject = "",
  body = "",
  sender_email = "",
} = {}) {
  const normalized_event_text = sanitizeText(event, 500);
  const normalized_subject = sanitizeText(subject, 1000);
  const normalized_body = sanitizeText(body, 4000);
  const normalized_sender_email = clean(sender_email);

  const combined_text = buildCombinedText({
    event: normalized_event_text,
    subject: normalized_subject,
    body: normalized_body,
  });

  const pattern_hit = detectByPatterns(combined_text);
  const result =
    pattern_hit ||
    fallbackHeuristics({
      event: normalized_event_text,
      subject: normalized_subject,
      body: normalized_body,
    });

  return {
    ...result,
    sender_email: normalized_sender_email || null,
    event: normalized_event_text || null,
    subject: normalized_subject || null,
    body: normalized_body || null,
    combined_text,
  };
}

export default classifyTitleResponse;
