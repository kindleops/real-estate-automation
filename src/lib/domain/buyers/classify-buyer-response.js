function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function includesAny(text, needles = []) {
  const haystack = lower(text);
  return needles.some((needle) => haystack.includes(lower(needle)));
}

export function classifyBuyerResponse({
  event = "",
  status = "",
  subject = "",
  body = "",
  attachments_count = 0,
} = {}) {
  const combined = [event, status, subject, body].map(clean).filter(Boolean).join(" ");
  const normalized = lower(combined);

  const mentions_proof_of_funds =
    Number(attachments_count || 0) > 0 ||
    includesAny(normalized, [
      "proof of funds",
      "pof",
      "bank statement",
      "wire confirmation",
      "emd attached",
      "funds attached",
    ]);

  if (
    includesAny(normalized, [
      "not interested",
      "no thanks",
      "pass",
      "we will pass",
      "we'll pass",
      "remove me",
      "take me off",
      "not for us",
      "doesn't fit",
      "does not fit",
    ])
  ) {
    return {
      ok: true,
      normalized_response: "passed",
      buyer_response_status: "Passed",
      match_status: null,
      assignment_status: null,
      dispo_outcome: null,
      proof_of_funds_received: false,
      confidence: "high",
      reason: "pass_signal_detected",
    };
  }

  if (
    includesAny(normalized, [
      "we will take it",
      "we'll take it",
      "i will take it",
      "i'll take it",
      "we are all in",
      "we're all in",
      "move forward",
      "ready to sign",
      "send assignment",
      "send the assignment",
      "we want this deal",
      "we can close",
      "we are ready",
      "selected",
    ])
  ) {
    return {
      ok: true,
      normalized_response: "chosen",
      buyer_response_status: "Selected",
      match_status: "Buyers Chosen",
      assignment_status: "Buyer Confirmed",
      dispo_outcome: "Buyer Secured",
      proof_of_funds_received: mentions_proof_of_funds,
      confidence: "high",
      reason: "buyer_commitment_detected",
    };
  }

  if (
    includesAny(normalized, [
      "send details",
      "send more info",
      "more info",
      "pricing details",
      "what is the price",
      "what's the price",
      "what is the address",
      "access notes",
      "send package",
      "send the package",
      "send pics",
      "send pictures",
      "need info",
      "can you send",
      "interested but",
    ])
  ) {
    return {
      ok: true,
      normalized_response: "needs_more_info",
      buyer_response_status: "Needs More Info",
      match_status: "Buyers Interested",
      assignment_status: "In Progress",
      dispo_outcome: null,
      proof_of_funds_received: mentions_proof_of_funds,
      confidence: "medium",
      reason: "buyer_requested_more_information",
    };
  }

  if (
    includesAny(normalized, [
      "interested",
      "yes",
      "call me",
      "call us",
      "let's talk",
      "want to see it",
      "looks good",
      "send it over",
      "we buy in this area",
      "i can close",
      "we can fund",
      "we are interested",
      "we're interested",
    ])
  ) {
    return {
      ok: true,
      normalized_response: "interested",
      buyer_response_status: "Interested",
      match_status: "Buyers Interested",
      assignment_status: "In Progress",
      dispo_outcome: null,
      proof_of_funds_received: mentions_proof_of_funds,
      confidence: "medium",
      reason: "interest_signal_detected",
    };
  }

  if (includesAny(normalized, ["opened"]) || lower(event) === "opened" || lower(status) === "opened") {
    return {
      ok: true,
      normalized_response: "opened",
      buyer_response_status: "Opened",
      match_status: null,
      assignment_status: null,
      dispo_outcome: null,
      proof_of_funds_received: false,
      confidence: "low",
      reason: "open_signal_detected",
    };
  }

  return {
    ok: false,
    normalized_response: "unclassified",
    buyer_response_status: null,
    match_status: null,
    assignment_status: null,
    dispo_outcome: null,
    proof_of_funds_received: mentions_proof_of_funds,
    confidence: "low",
    reason: "buyer_response_unclassified",
  };
}

export default classifyBuyerResponse;
