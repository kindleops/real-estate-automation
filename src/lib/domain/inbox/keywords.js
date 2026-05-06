export const KEYWORD_GROUPS = Object.freeze({
  positive_hot: ["yes", "interested", "maybe", "depends", "i own it", "make offer"],
  offer_requested: ["how much", "offer", "price", "what price"],
  opt_out: ["stop", "remove", "unsubscribe"],
  wrong_number: ["wrong number", "not me", "no soy", "no es mio"],
  manual_review: ["attorney", "lawyer", "lawsuit", "harassment", "legal"],
  legal: ["attorney", "lawyer", "lawsuit", "harassment", "legal"],
});

function clean(value) { return String(value ?? "").trim(); }
function escapeRegExp(value = "") { return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }

export function findMatchedKeywords(messageBody = "", groupsOrTerms = []) {
  const body = clean(messageBody);
  if (!body) return [];
  const requested = Array.isArray(groupsOrTerms) ? groupsOrTerms : [groupsOrTerms];
  const terms = requested.flatMap((entry) => KEYWORD_GROUPS[clean(entry).toLowerCase()] || [entry]).map(clean).filter(Boolean);
  const seen = new Set();
  const matches = [];
  for (const term of terms) {
    const pattern = term.length <= 3 ? `\\b${escapeRegExp(term)}\\b` : escapeRegExp(term);
    const rx = new RegExp(pattern, "ig");
    let match;
    while ((match = rx.exec(body)) !== null) {
      const key = `${term.toLowerCase()}:${match.index}`;
      if (!seen.has(key)) {
        seen.add(key);
        matches.push({ term, start: match.index, end: match.index + match[0].length });
      }
      if (rx.lastIndex === match.index) rx.lastIndex += 1;
    }
  }
  return matches.sort((a, b) => a.start - b.start);
}

export function classifyInboxMessage(row = {}) {
  const body = row?.message_body || row?.message_text || "";
  const positive = findMatchedKeywords(body, ["positive_hot"]);
  const offer = findMatchedKeywords(body, ["offer_requested"]);
  const optOut = findMatchedKeywords(body, ["opt_out"]);
  const wrong = findMatchedKeywords(body, ["wrong_number"]);
  const legal = findMatchedKeywords(body, ["manual_review"]);
  return {
    positive_hot: positive.length > 0,
    offer_requested: offer.length > 0,
    opt_out: optOut.length > 0,
    wrong_number: wrong.length > 0,
    manual_review: legal.length > 0,
    matched_keywords: [...positive, ...offer, ...optOut, ...wrong, ...legal].map((m) => m.term),
  };
}
