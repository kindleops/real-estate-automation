// ─── extract-underwriting-signals.js ─────────────────────────────────────
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

function toNumber(value) {
  if (value === null || value === undefined) return null;
  const cleaned = String(value).replace(/[^0-9.-]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

function parseScaledNumber(value, scale_suffix = "") {
  const base = toNumber(value);
  if (base === null) return null;

  const suffix = lower(scale_suffix);
  if (suffix === "k") return Math.round(base * 1_000);
  if (suffix === "m") return Math.round(base * 1_000_000);
  return base;
}

function unique(list = []) {
  return [...new Set((list || []).filter(Boolean))];
}

function extractDollarAmounts(message = "") {
  const matches = [
    ...String(message).matchAll(
      /\$?\s?(\d{1,3}(?:,\d{3})+|\d{2,7})(?:\.\d+)?\s*([kKmM])?\b/g
    ),
  ];
  const values = matches
    .map((match) => parseScaledNumber(match[1], match[2] || ""))
    .filter((value) => Number.isFinite(value) && value >= 1_000);

  return unique(values);
}

function extractUnitCount(message = "") {
  const text = lower(message);

  const patterns = [
    /(\d+)\s*(unit|units|door|doors)\b/,
    /\bduplex\b/,
    /\btriplex\b/,
    /\bquadplex\b/,
    /\b4[-\s]?plex\b/,
    /\b5[-\s]?unit\b/,
    /\b6[-\s]?unit\b/,
    /\b8[-\s]?unit\b/,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;

    if (match[1]) return Number(match[1]);
    if (pattern.source.includes("duplex")) return 2;
    if (pattern.source.includes("triplex")) return 3;
    if (pattern.source.includes("quadplex") || pattern.source.includes("4[-\\s]?plex")) return 4;
  }

  return null;
}

function extractPropertyType(message = "") {
  const text = lower(message);

  if (
    includesAny(text, [
      "multifamily",
      "multi family",
      "apartment building",
      "apartment complex",
      "apartments",
      "duplex",
      "triplex",
      "quadplex",
      "fourplex",
    ])
  ) {
    return "Multifamily";
  }

  if (
    includesAny(text, [
      "mixed use",
      "mixed-use",
      "commercial",
      "retail",
      "office",
      "industrial",
      "warehouse",
    ])
  ) {
    return "Commercial";
  }

  if (
    includesAny(text, [
      "single family",
      "sfr",
      "house",
      "residential",
      "townhome",
      "townhouse",
      "condo",
      "mobile home",
      "manufactured home",
    ])
  ) {
    return "Residential";
  }

  if (
    includesAny(text, [
      "vacant land",
      "land",
      "lot",
      "acreage",
    ])
  ) {
    return "Land";
  }

  return null;
}

function extractTimeline(message = "") {
  const text = lower(message);

  if (includesAny(text, ["asap", "immediately", "right away", "today", "tomorrow"])) {
    return "Immediate";
  }

  if (includesAny(text, ["this week", "in a few days", "within a week", "next few days"])) {
    return "This Week";
  }

  if (includesAny(text, ["this month", "within 30 days", "30 days", "2 weeks", "two weeks"])) {
    return "This Month";
  }

  if (includesAny(text, ["next month", "in a month", "60 days", "90 days", "few months"])) {
    return "Longer-Term";
  }

  return null;
}

function extractOccupancy(message = "") {
  const text = lower(message);

  if (includesAny(text, [
    "vacant", "empty", "nobody lives there", "no one lives there",
    "unoccupied", "abandoned",
  ])) {
    return "Vacant";
  }

  if (includesAny(text, [
    "owner occupied", "i live here", "we live here", "my primary residence",
  ])) {
    return "Owner Occupied";
  }

  if (includesAny(text, [
    "tenant", "tenants", "rented", "occupied", "lease", "section 8",
    "renting it out", "someone living there",
  ])) {
    return "Tenant Occupied";
  }

  return null;
}

function extractCondition(message = "") {
  const text = lower(message);

  if (includesAny(text, [
    "tear down", "teardown", "gut job", "full rehab", "fire damage",
    "foundation issues", "condemned", "major repairs", "needs everything",
    "uninhabitable",
  ])) {
    return "Heavy";
  }

  if (includesAny(text, [
    "needs work", "needs repairs", "as is", "as-is", "bad condition",
    "rough shape", "water damage", "mold", "roof", "fixer",
  ])) {
    return "Moderate";
  }

  if (includesAny(text, [
    "good condition", "great condition", "updated", "turnkey",
    "rent ready", "move in ready",
  ])) {
    return "Light";
  }

  return null;
}

function extractDealTypeSignals(message = "") {
  const text = lower(message);

  return {
    creative_terms_interest: includesAny(text, [
      "seller finance", "seller financing", "owner finance", "owner financing",
      "subject to", "subto", "terms", "monthly payments", "carry the note",
      "lease option", "installments",
    ]),
    novation_interest: includesAny(text, [
      "list it", "mls", "open market", "retail buyer", "want retail",
      "higher on market", "market it",
    ]),
    proof_of_funds_requested: includesAny(text, [
      "proof of funds", "pof", "show proof", "verify funds",
    ]),
    docs_requested: includesAny(text, [
      "send docs", "send paperwork", "send contract", "email me",
      "written offer", "put it in writing",
    ]),
  };
}

function extractDistressSignals(message = "") {
  const text = lower(message);
  const tags = [];

  if (includesAny(text, ["foreclosure", "pre-foreclosure", "notice of default"])) {
    tags.push("Foreclosure");
  }

  if (includesAny(text, ["behind on payments", "missed payments", "can't afford"])) {
    tags.push("Payment Distress");
  }

  if (includesAny(text, ["tax lien", "back taxes", "tax sale"])) {
    tags.push("Tax Distress");
  }

  if (includesAny(text, ["probate", "estate", "inherited", "heir", "passed away"])) {
    tags.push("Probate");
  }

  if (includesAny(text, ["divorce", "separation", "ex-wife", "ex-husband"])) {
    tags.push("Divorce");
  }

  if (includesAny(text, ["tenant problem", "bad tenant", "eviction"])) {
    tags.push("Tenant Distress");
  }

  return unique(tags);
}

function deriveLatestOutboundUseCase(context = null) {
  const recent_events = Array.isArray(context?.recent?.recent_events)
    ? context.recent.recent_events
    : [];

  const latest_outbound = recent_events.find(
    (event) => lower(event?.direction) === "outbound"
  );

  return lower(
    latest_outbound?.selected_use_case ||
      latest_outbound?.metadata?.selected_use_case ||
      ""
  );
}

function isLikelyPriceContext({
  message = "",
  classification = null,
  route = null,
  context = null,
} = {}) {
  const text = lower(message);
  const last_use_case = deriveLatestOutboundUseCase(context);

  if (
    [
      "asking_price",
      "price_works_confirm_basics",
      "price_high_condition_probe",
      "offer_reveal",
      "offer_reveal_cash",
      "offer_reveal_lease_option",
      "offer_reveal_subject_to",
      "offer_reveal_novation",
      "mf_offer_reveal",
    ].includes(
      last_use_case
    )
  ) {
    return true;
  }

  if (
    ["send_offer_first", "need_more_money", "wants_retail"].includes(
      lower(classification?.objection)
    )
  ) {
    return true;
  }

  if (
    lower(route?.stage) === "offer" ||
    [
      "offer_reveal",
      "offer_reveal_cash",
      "offer_reveal_lease_option",
      "offer_reveal_subject_to",
      "offer_reveal_novation",
      "mf_offer_reveal",
    ].includes(lower(route?.use_case))
  ) {
    return true;
  }

  return includesAny(text, [
    "ask",
    "asking",
    "take",
    "want",
    "would do",
    "i'd do",
    "id do",
    "for it",
    "cash",
    "number",
    "price",
  ]);
}

function extractContextualBareAskingPrice({
  message = "",
  classification = null,
  route = null,
  context = null,
} = {}) {
  if (
    !isLikelyPriceContext({
      message,
      classification,
      route,
      context,
    })
  ) {
    return [];
  }

  const text = clean(message);
  const normalized = lower(text);

  if (
    includesAny(normalized, [
      "unit",
      "units",
      "door",
      "doors",
      "tenant",
      "tenants",
      "occupied",
      "vacant",
      "rent",
      "rents",
      "expense",
      "expenses",
      "bed",
      "beds",
      "bath",
      "baths",
      "built in",
      "year built",
      "month",
      "monthly",
      "year",
      "years",
      "days",
      "%",
    ])
  ) {
    return [];
  }

  const direct_match = text.match(
    /^(?:i(?:'d| would)?\s*(?:do|take)|want|need|asking|ask|at|for)?\s*\$?\s*(\d{2,3})(?:\s*(?:cash|all cash))?\s*$/i
  );

  if (!direct_match?.[1]) return [];

  const numeric = Number(direct_match[1]);
  if (!Number.isFinite(numeric) || numeric < 10) return [];

  return [numeric * 1_000];
}

function pickAskingPrice(message = "", classification = null, route = null, context = null) {
  const amounts = extractDollarAmounts(message);
  const contextual_amounts = extractContextualBareAskingPrice({
    message,
    classification,
    route,
    context,
  });
  const candidates = unique([...amounts, ...contextual_amounts]);
  if (!candidates.length) return null;

  if (classification?.objection === "need_more_money" || classification?.objection === "wants_retail") {
    return Math.max(...candidates);
  }

  return candidates[0];
}

function matchFirstNumber(message = "", patterns = []) {
  const text = String(message || "");

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match?.[1]) continue;

    const value = toNumber(match[1]);
    if (value !== null) return value;
  }

  return null;
}

function extractRentSignals(message = "") {
  const text = lower(message);
  const current_gross_rents = matchFirstNumber(message, [
    /(?:gross rents?|rent roll|monthly rent roll|total rents?|bringing in total|collect(?:ing)? total)[^\d$]{0,25}\$?\s*([\d,]{3,8})/i,
  ]);
  const avg_rent = matchFirstNumber(message, [
    /(?:average rent|avg(?:\.| )?rent|per unit|each unit(?: is| at)?|units? are(?: all)? at)[^\d$]{0,25}\$?\s*([\d,]{3,6})/i,
    /\$?\s*([\d,]{3,6})(?:\s*\/\s*mo|\s*(?:per|a)\s*month|\s*monthly)\b/i,
  ]);
  const rents_present =
    current_gross_rents !== null ||
    avg_rent !== null ||
    includesAny(text, [
      "rent roll",
      "rents",
      "rent",
      "bringing in",
      "monthly income",
      "gross income",
    ]);

  return {
    current_gross_rents,
    avg_rent,
    rents_present,
  };
}

function extractExpenseSignals(message = "") {
  const text = lower(message);
  const estimated_expenses = matchFirstNumber(message, [
    /(?:expenses?|opex|operating expenses?|taxes|insurance|utilities|maintenance)[^\d$]{0,25}\$?\s*([\d,]{3,8})/i,
  ]);
  const expenses_present =
    estimated_expenses !== null ||
    includesAny(text, [
      "expenses",
      "expense",
      "taxes",
      "insurance",
      "utilities",
      "electric",
      "electricity",
      "water",
      "gas",
      "opex",
      "maintenance",
      "deferred maintenance",
    ]);

  return {
    estimated_expenses,
    expenses_present,
  };
}

function extractCreativeStrategy(message = "") {
  const text = lower(message);

  if (includesAny(text, ["subject to", "subject-to", "subto"])) {
    return "Subject-To";
  }

  if (
    includesAny(text, [
      "seller finance",
      "seller financing",
      "owner finance",
      "owner financing",
      "carry the note",
    ])
  ) {
    return "Seller Finance";
  }

  if (includesAny(text, ["lease option", "lease purchase", "rent to own"])) {
    return "Lease Option";
  }

  if (includesAny(text, ["wraparound", "wrap mortgage", "wrap loan"])) {
    return "Wrap";
  }

  if (includesAny(text, ["terms", "monthly payments", "installments"])) {
    return "Creative Terms";
  }

  return null;
}

function extractLoanTermMonths(message = "", enabled = false) {
  if (!enabled) return null;

  const months = matchFirstNumber(message, [
    /(\d+)\s*(?:months|mos)\b/i,
  ]);
  if (months !== null) return months;

  const years = matchFirstNumber(message, [
    /(\d+)\s*(?:years|yrs|year|yr)\b/i,
  ]);

  return years !== null ? years * 12 : null;
}

function extractCreativeTerms(message = "") {
  const strategy = extractCreativeStrategy(message);
  const text = lower(message);
  const has_creative_context =
    Boolean(strategy) ||
    includesAny(text, [
      "seller finance",
      "owner finance",
      "subject to",
      "subto",
      "terms",
      "carry the note",
      "lease option",
    ]);

  const down_payment = has_creative_context
    ? matchFirstNumber(message, [
        /(?:down(?: payment)?|downpay)[^\d$]{0,20}\$?\s*([\d,]{3,8})/i,
      ])
    : null;

  const monthly_payment = has_creative_context
    ? matchFirstNumber(message, [
        /(?:mortgage payment|monthly(?: payment)?|payment(?: is| at)?|piti)[^\d$]{0,20}\$?\s*([\d,]{3,7})/i,
        /\$?\s*([\d,]{3,7})\s*(?:\/\s*mo|per month|monthly)\b/i,
      ])
    : null;

  const interest_rate = has_creative_context
    ? matchFirstNumber(message, [
        /(?:interest(?: rate)?|rate(?: is)?)[^\d]{0,20}(\d+(?:\.\d+)?)\s*%/i,
        /(\d+(?:\.\d+)?)\s*%\s*(?:interest|rate)/i,
      ])
    : null;

  const loan_terms_months = extractLoanTermMonths(message, has_creative_context);

  const balloon_payment = has_creative_context
    ? matchFirstNumber(message, [
        /(?:balloon(?: payment)?)[^\d$]{0,20}\$?\s*([\d,]{3,8})/i,
      ])
    : null;

  const existing_mortgage_balance = has_creative_context
    ? matchFirstNumber(message, [
        /(?:mortgage balance|balance owed|remaining mortgage|loan balance|owe(?:d)? on (?:the )?mortgage)[^\d$]{0,20}\$?\s*([\d,]{3,8})/i,
      ])
    : null;

  const existing_mortgage_payment = has_creative_context
    ? matchFirstNumber(message, [
        /(?:existing mortgage payment|mortgage payment|piti)[^\d$]{0,20}\$?\s*([\d,]{3,7})/i,
      ])
    : null;

  const creative_terms_present = Boolean(
    strategy ||
      down_payment !== null ||
      monthly_payment !== null ||
      interest_rate !== null ||
      loan_terms_months !== null ||
      balloon_payment !== null ||
      existing_mortgage_balance !== null ||
      existing_mortgage_payment !== null
  );

  const creative_terms_summary = [
    strategy ? `Structure: ${strategy}` : "",
    down_payment !== null ? `Down: $${down_payment}` : "",
    monthly_payment !== null ? `Payment: $${monthly_payment}/mo` : "",
    interest_rate !== null ? `Rate: ${interest_rate}%` : "",
    loan_terms_months !== null ? `Term: ${loan_terms_months} months` : "",
    balloon_payment !== null ? `Balloon: $${balloon_payment}` : "",
    existing_mortgage_balance !== null ? `Mortgage Balance: $${existing_mortgage_balance}` : "",
    existing_mortgage_payment !== null ? `Mortgage Payment: $${existing_mortgage_payment}/mo` : "",
  ]
    .filter(Boolean)
    .join("; ");

  return {
    creative_strategy: strategy,
    creative_terms_present,
    creative_terms_summary: creative_terms_summary || null,
    down_payment,
    monthly_payment,
    interest_rate,
    loan_terms_months,
    balloon_payment,
    existing_mortgage_balance,
    existing_mortgage_payment,
  };
}

function extractRepairScope(message = "") {
  const text = lower(message);
  const parts = [];

  if (includesAny(text, ["roof", "leak", "water damage", "flood"])) {
    parts.push("roof or water issues");
  }

  if (includesAny(text, ["foundation", "structural", "settling"])) {
    parts.push("foundation or structural work");
  }

  if (includesAny(text, ["hvac", "ac", "furnace"])) {
    parts.push("hvac");
  }

  if (includesAny(text, ["plumbing", "sewer"])) {
    parts.push("plumbing");
  }

  if (includesAny(text, ["electrical", "panel", "wiring"])) {
    parts.push("electrical");
  }

  if (includesAny(text, ["kitchen", "bath", "bathroom", "cosmetic", "update"])) {
    parts.push("cosmetic updates");
  }

  if (includesAny(text, ["trash out", "cleanout", "clean out", "debris"])) {
    parts.push("cleanout");
  }

  return parts.join(", ") || null;
}

function extractNovationSignals({
  message = "",
  asking_price = null,
  timeline = null,
  condition_level = null,
} = {}) {
  const text = lower(message);
  const target_net_to_seller = matchFirstNumber(message, [
    /(?:net(?: to (?:you|seller))?|walk(?: away)? with|take home|need to net|need out of it)[^\d$]{0,25}\$?\s*([\d,]{3,8})/i,
  ]);
  const estimated_repair_cost = matchFirstNumber(message, [
    /(?:repair budget|repairs?|fix(?:ing)?(?: it)? would take|work needed)[^\d$]{0,25}\$?\s*([\d,]{3,8})/i,
  ]);
  const estimated_days_to_sell = includesAny(text, ["days on market", "days to sell", "sold in"])
    ? matchFirstNumber(message, [/(\d+)\s*days?\b/i])
    : null;

  let listing_readiness = null;
  if (
    includesAny(text, [
      "show-ready",
      "ready to list",
      "ready for photos",
      "vacant",
      "empty",
      "can show",
      "can access",
      "cleaned out",
    ])
  ) {
    listing_readiness = "Listing Ready";
  } else if (
    includesAny(text, [
      "tenant occupied",
      "occupied",
      "needs to be cleaned",
      "not ready",
      "no access",
      "can't show",
    ])
  ) {
    listing_readiness = "Needs Prep";
  }

  const novation_listing_readiness_present =
    listing_readiness !== null ||
    includesAny(text, [
      "showing",
      "showings",
      "show it",
      "listing",
      "photos",
      "access",
      "vacant",
      "occupied",
      "ready to list",
      "realtor",
      "agent can walk it",
    ]);

  const estimated_repair_scope =
    extractRepairScope(message) ||
    (condition_level === "Heavy"
      ? "major repairs likely"
      : condition_level === "Moderate"
        ? "some repairs or updates needed"
        : null);

  const novation_summary = [
    asking_price !== null ? `Ask/List Anchor: $${asking_price}` : "",
    target_net_to_seller !== null ? `Target Net: $${target_net_to_seller}` : "",
    listing_readiness ? `Readiness: ${listing_readiness}` : "",
    timeline ? `Timeline: ${timeline}` : "",
    estimated_repair_scope ? `Repairs: ${estimated_repair_scope}` : "",
  ]
    .filter(Boolean)
    .join("; ");

  return {
    target_net_to_seller,
    listing_readiness,
    novation_listing_readiness_present,
    estimated_repair_scope,
    estimated_repair_cost,
    estimated_days_to_sell,
    novation_summary: novation_summary || null,
  };
}

export function extractUnderwritingSignals({
  message = "",
  classification = null,
  route = null,
  context = null,
} = {}) {
  const text = clean(message);

  if (!text) {
    return {
      ok: true,
      extracted: false,
      reason: "empty_message",
      signals: {},
    };
  }

  const asking_price = pickAskingPrice(text, classification, route, context);
  const unit_count = extractUnitCount(text);
  const property_type = extractPropertyType(text);
  const timeline = extractTimeline(text);
  const occupancy_status = extractOccupancy(text);
  const condition_level = extractCondition(text);
  const distress_tags = extractDistressSignals(text);
  const deal_type_signals = extractDealTypeSignals(text);
  const rent_signals = extractRentSignals(text);
  const expense_signals = extractExpenseSignals(text);
  const creative_signals = extractCreativeTerms(text);
  const novation_signals = extractNovationSignals({
    message: text,
    asking_price,
    timeline,
    condition_level,
  });

  const signals = {
    asking_price,
    desired_price: asking_price,
    timeline,
    occupancy_status,
    condition_level,
    unit_count,
    property_type,
    tenant_present: occupancy_status === "Tenant Occupied",
    creative_terms_interest: deal_type_signals.creative_terms_interest,
    novation_interest: deal_type_signals.novation_interest,
    proof_of_funds_requested: deal_type_signals.proof_of_funds_requested,
    docs_requested: deal_type_signals.docs_requested,
    current_gross_rents: rent_signals.current_gross_rents,
    avg_rent: rent_signals.avg_rent,
    rents_present: rent_signals.rents_present,
    estimated_expenses: expense_signals.estimated_expenses,
    expenses_present: expense_signals.expenses_present,
    creative_strategy: creative_signals.creative_strategy,
    creative_terms_present: creative_signals.creative_terms_present,
    creative_terms_summary: creative_signals.creative_terms_summary,
    down_payment: creative_signals.down_payment,
    monthly_payment: creative_signals.monthly_payment,
    interest_rate: creative_signals.interest_rate,
    loan_terms_months: creative_signals.loan_terms_months,
    balloon_payment: creative_signals.balloon_payment,
    existing_mortgage_balance: creative_signals.existing_mortgage_balance,
    existing_mortgage_payment: creative_signals.existing_mortgage_payment,
    target_net_to_seller: novation_signals.target_net_to_seller,
    listing_readiness: novation_signals.listing_readiness,
    novation_listing_readiness_present:
      novation_signals.novation_listing_readiness_present,
    estimated_repair_scope: novation_signals.estimated_repair_scope,
    estimated_repair_cost: novation_signals.estimated_repair_cost,
    estimated_days_to_sell: novation_signals.estimated_days_to_sell,
    novation_summary: novation_signals.novation_summary,
    distress_tags,
    classification_objection: classification?.objection || null,
    classification_emotion: classification?.emotion || null,
    route_stage: route?.stage || null,
    route_use_case: route?.use_case || null,
    seller_profile: context?.summary?.seller_profile || null,
    motivation_score: classification?.motivation_score ?? context?.summary?.motivation_score ?? null,
    raw_message: text,
  };

  const has_meaningful_signal = Object.values(signals).some((value) => {
    if (Array.isArray(value)) return value.length > 0;
    return value !== null && value !== false && value !== "";
  });

  return {
    ok: true,
    extracted: has_meaningful_signal,
    reason: has_meaningful_signal ? "signals_extracted" : "no_meaningful_signals",
    signals,
  };
}

export default extractUnderwritingSignals;
