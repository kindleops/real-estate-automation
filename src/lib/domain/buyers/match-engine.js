import {
  BUYER_OFFICER_FIELDS,
  getBuyerOfficerItem,
} from "@/lib/podio/apps/buyer-officers.js";
import {
  COMPANY_FIELDS,
  fetchAllCompanyItems,
  findCompanyItems,
  getCompanyItem,
} from "@/lib/podio/apps/companies.js";
import {
  BUYER_MATCH_FIELDS,
  findLatestBuyerMatchByClosingId,
  findLatestBuyerMatchByContractId,
  findLatestBuyerMatchByPropertyId,
  getBuyerMatchItem,
} from "@/lib/podio/apps/buyer-match.js";
import {
  CLOSING_FIELDS,
  findClosingItems,
  getClosingItem,
} from "@/lib/podio/apps/closings.js";
import {
  CONTRACT_FIELDS,
  findContractItems,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  MARKET_FIELDS,
  getMarketItem,
} from "@/lib/podio/apps/markets.js";
import {
  OFFER_FIELDS,
  findOfferItems,
  getOfferItem,
} from "@/lib/podio/apps/offers.js";
import { getPropertyItem } from "@/lib/podio/apps/properties.js";
import {
  findSoldPropertyItems,
  SOLD_PROPERTY_FIELDS,
} from "@/lib/podio/apps/sold-properties.js";
import {
  getZipCodeItem,
  ZIP_CODE_FIELDS,
} from "@/lib/podio/apps/zip-codes.js";
import {
  getAppReferenceIds,
  getCategoryValue,
  getDateValue,
  getFieldValues,
  getFirstAppReferenceId,
  getItem,
  getMoneyValue,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function asNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const numeric =
    typeof value === "number"
      ? value
      : Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(numeric) ? numeric : fallback;
}

function clamp(value, min = 0, max = 100) {
  return Math.min(Math.max(Number(value) || 0, min), max);
}

function round(value, places = 0) {
  const factor = Math.pow(10, places);
  return Math.round((Number(value) || 0) * factor) / factor;
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function sortByScore(items = []) {
  return [...items].sort((left, right) => {
    const score_delta = Number(right?.score || 0) - Number(left?.score || 0);
    if (score_delta !== 0) return score_delta;

    const market_delta =
      Number(right?.history?.same_market_count || 0) -
      Number(left?.history?.same_market_count || 0);
    if (market_delta !== 0) return market_delta;

    const contact_delta =
      Number(right?.contact_summary?.email_count || 0) -
      Number(left?.contact_summary?.email_count || 0);
    if (contact_delta !== 0) return contact_delta;

    return Number(right?.item_id || 0) - Number(left?.item_id || 0);
  });
}

function normalizeBusinessName(value = "") {
  return clean(value)
    .toLowerCase()
    .replace(/\b(llc|inc|corp|corporation|ltd|lp|llp|pllc|holdings?|group|co|company)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function uniqueStrings(values = []) {
  return [...new Set(values.map((value) => clean(value)).filter(Boolean))];
}

function getLocationValue(item, external_id) {
  const first = getFieldValues(item, external_id)[0];
  return first?.value && typeof first.value === "object" ? first.value : null;
}

function getLocationText(item, external_id, fallback = "") {
  const location = getLocationValue(item, external_id);
  return (
    clean(location?.formatted) ||
    clean(location?.value) ||
    clean(location?.street_address) ||
    getTextValue(item, external_id, fallback)
  );
}

function extractZipFromText(value = "") {
  const match = clean(value).match(/\b(\d{5})(?:-\d{4})?\b/);
  return match?.[1] || null;
}

function collectStringsDeep(value, output = []) {
  if (value === null || value === undefined) return output;

  if (typeof value === "string") {
    const normalized = clean(value);
    if (normalized) output.push(normalized);
    return output;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    output.push(String(value));
    return output;
  }

  if (Array.isArray(value)) {
    for (const entry of value) collectStringsDeep(entry, output);
    return output;
  }

  if (typeof value === "object") {
    for (const entry of Object.values(value)) {
      collectStringsDeep(entry, output);
    }
  }

  return output;
}

function collectItemText(item = null) {
  if (!item?.fields) return [];
  return uniqueStrings(collectStringsDeep(item.fields));
}

function extractEmailsFromItem(item = null) {
  const matches = [];
  for (const value of collectItemText(item)) {
    for (const match of value.matchAll(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi)) {
      matches.push(match[0].toLowerCase());
    }
  }
  return uniqueStrings(matches);
}

function extractPhonesFromItem(item = null) {
  const matches = [];
  for (const value of collectItemText(item)) {
    for (const match of value.matchAll(/\+?1?[ -.]?\(?\d{3}\)?[ -.]?\d{3}[ -.]?\d{4}\b/g)) {
      const digits = match[0].replace(/\D/g, "");
      if (digits.length >= 10) matches.push(digits.slice(-10));
    }
  }
  return uniqueStrings(matches);
}

function diffDaysFromNow(date_value = null) {
  if (!date_value) return null;
  const timestamp = new Date(date_value).getTime();
  if (Number.isNaN(timestamp)) return null;
  return Math.ceil((timestamp - Date.now()) / 86_400_000);
}

function appendNote(...values) {
  return values.map((value) => clean(value)).filter(Boolean).join("\n");
}

function normalizePropertyType(value = "") {
  const raw = lower(value);

  if (["single family", "sfr", "single-family"].includes(raw)) return "Single Family";
  if (["multi-family", "multifamily", "apartment", "multifamily buyer"].includes(raw)) {
    return "Multi-Family";
  }
  if (raw.includes("commercial")) return "Commercial";
  if (raw.includes("vacant")) return "Vacant Land";
  if (raw.includes("town")) return "Townhouse";
  if (raw.includes("mobile")) return "Mobile Home";
  if (raw.includes("condo")) return "Condominium";

  return clean(value) || "Unknown";
}

function normalizeBuyerType(value = "") {
  const raw = lower(value);

  if (raw.includes("multifamily")) return "Multifamily Buyer";
  if (raw.includes("hedge")) return "Hedge Fund";
  if (raw.includes("landlord")) return "Landlord";
  if (raw.includes("rehab")) return "Rehabber";
  if (raw.includes("commercial") || raw.includes("hotel")) return "Hotel / Commercial";
  if (raw.includes("cash")) return "Cash Buyer";
  if (raw.includes("unknown")) return "Unknown";

  return clean(value) || "Unknown";
}

export function inferDispositionStrategy({
  offer_type = "",
  property_type = "",
  units = null,
} = {}) {
  const normalized_offer_type = lower(offer_type);
  const normalized_property_type = normalizePropertyType(property_type);
  const normalized_units = asNumber(units);

  if (normalized_offer_type.includes("novation")) return "Novation";
  if (
    normalized_offer_type.includes("subject") ||
    normalized_offer_type.includes("lease")
  ) {
    return "Hybrid";
  }
  if (
    normalized_property_type === "Commercial" ||
    normalized_property_type === "Vacant Land"
  ) {
    return "Hold";
  }
  if (
    normalized_property_type === "Multi-Family" ||
    (normalized_units !== null && normalized_units >= 5)
  ) {
    return "Assignment";
  }

  return "Assignment";
}

export function inferTargetBuyerTypes(context = {}) {
  const property_type = normalizePropertyType(context.property_type);
  const offer_type = lower(context.offer_type);
  const units = asNumber(context.units);
  const types = new Set();

  if (property_type === "Commercial") {
    types.add("Hotel / Commercial");
  } else if (property_type === "Multi-Family" || (units !== null && units >= 5)) {
    types.add("Multifamily Buyer");
    types.add("Hedge Fund");
  } else if (offer_type.includes("subject") || offer_type.includes("lease")) {
    types.add("Landlord");
    types.add("Hedge Fund");
  } else if (offer_type.includes("novation")) {
    types.add("Unknown");
    types.add("Landlord");
  } else {
    types.add("Cash Buyer");
    types.add("Rehabber");
    types.add("Landlord");
  }

  return [...types];
}

function inferCandidateTypes(candidate = {}) {
  const types = new Set();
  const owner_type = lower(candidate.owner_type);
  const sold_property_types = safeArray(candidate.history?.property_types);
  const sold_styles = safeArray(candidate.history?.property_styles).map(lower);
  const total_properties_owned = asNumber(candidate.total_properties_owned, 0) || 0;
  const avg_flip_spread = asNumber(candidate.history?.avg_flip_spread, 0) || 0;

  if (owner_type.includes("hedge")) {
    types.add("Hedge Fund");
    types.add("Cash Buyer");
  }

  if (
    sold_property_types.some((type) => ["Multi-Family", "Apartment"].includes(type)) ||
    sold_styles.some((style) => style.includes("triplex") || style.includes("quad"))
  ) {
    types.add("Multifamily Buyer");
  }

  if (
    sold_property_types.some((type) =>
      ["Single Family", "Townhouse", "Condominium", "Mobile Home"].includes(type)
    )
  ) {
    types.add("Cash Buyer");
  }

  if (avg_flip_spread >= 30000) {
    types.add("Rehabber");
  }

  if (total_properties_owned >= 10) {
    types.add("Landlord");
  }

  if (!types.size) {
    types.add("Unknown");
  }

  return [...types].map(normalizeBuyerType);
}

function scorePropertyTypeFit(context = {}, candidate = {}) {
  const property_type = normalizePropertyType(context.property_type);
  const candidate_types = new Set(inferCandidateTypes(candidate));
  const history_types = new Set(safeArray(candidate.history?.property_types));

  if (
    (property_type === "Multi-Family" &&
      (candidate_types.has("Multifamily Buyer") || history_types.has("Multi-Family"))) ||
    (property_type === "Commercial" && candidate_types.has("Hotel / Commercial"))
  ) {
    return 20;
  }

  if (
    property_type === "Single Family" &&
    (candidate_types.has("Cash Buyer") ||
      candidate_types.has("Rehabber") ||
      candidate_types.has("Landlord"))
  ) {
    return 18;
  }

  if (candidate_types.has("Unknown")) return 6;

  return 10;
}

function scoreTargetTypeFit(context = {}, candidate = {}) {
  const target_types = new Set(safeArray(context.target_buyer_types).map(normalizeBuyerType));
  const candidate_types = new Set(inferCandidateTypes(candidate));

  for (const target of target_types) {
    if (candidate_types.has(target)) {
      return target === "Unknown" ? 6 : 18;
    }
  }

  return candidate_types.has("Unknown") ? 4 : 8;
}

function scoreMarketFit(context = {}, candidate = {}) {
  const same_zip_count = asNumber(candidate.history?.same_zip_count, 0) || 0;
  const same_market_count = asNumber(candidate.history?.same_market_count, 0) || 0;

  if (same_zip_count >= 2) return 20;
  if (same_zip_count >= 1) return 17;
  if (same_market_count >= 3) return 15;
  if (same_market_count >= 1) return 12;

  const market_signal =
    context.primary_target_type === "Multifamily Buyer"
      ? asNumber(context.market_signals?.mf_buyer_density_score, 0)
      : context.primary_target_type === "Hedge Fund"
        ? asNumber(context.market_signals?.hedge_fund_density_score, 0)
        : asNumber(context.market_signals?.cash_buyer_density_score, 0);

  return market_signal >= 70 ? 8 : market_signal >= 45 ? 4 : 0;
}

function scorePriceFit(context = {}, candidate = {}) {
  const purchase_price = asNumber(context.purchase_price);
  const avg_sale_price = asNumber(candidate.history?.avg_sale_price);

  if (purchase_price === null || avg_sale_price === null || avg_sale_price <= 0) {
    return 6;
  }

  const ratio = purchase_price / avg_sale_price;

  if (ratio >= 0.35 && ratio <= 1.15) return 15;
  if (ratio >= 0.2 && ratio <= 1.4) return 10;
  if (ratio <= 1.75) return 5;
  return 0;
}

function scoreContactReadiness(candidate = {}) {
  const email_count = asNumber(candidate.contact_summary?.email_count, 0) || 0;
  const phone_count = asNumber(candidate.contact_summary?.phone_count, 0) || 0;
  const officer_count = asNumber(candidate.contact_summary?.officer_count, 0) || 0;

  let score = 0;
  if (email_count > 0) score += 7;
  if (phone_count > 0) score += 2;
  if (officer_count > 0) score += 1;

  return score;
}

function scoreBuyerPower(candidate = {}) {
  const portfolio_value = asNumber(candidate.estimated_portfolio_value, 0) || 0;
  const total_properties = asNumber(candidate.total_properties_owned, 0) || 0;
  const owner_type = lower(candidate.owner_type);

  if (owner_type.includes("hedge")) return 10;
  if (portfolio_value >= 5_000_000 || total_properties >= 50) return 9;
  if (portfolio_value >= 1_000_000 || total_properties >= 20) return 7;
  if (total_properties >= 5) return 5;
  return 2;
}

function scoreActivity(candidate = {}) {
  const recent_sales = asNumber(candidate.history?.recent_sale_count, 0) || 0;
  const total_sales = asNumber(candidate.history?.sale_count, 0) || 0;

  if (recent_sales >= 5) return 7;
  if (recent_sales >= 2) return 5;
  if (total_sales >= 3) return 4;
  if (total_sales >= 1) return 2;
  return 0;
}

function buildReasonList({
  context = {},
  candidate = {},
} = {}) {
  const reasons = [];

  if ((candidate.history?.same_zip_count || 0) > 0) {
    reasons.push(`active in zip ${context.zip_code || "target zip"}`);
  } else if ((candidate.history?.same_market_count || 0) > 0) {
    reasons.push(`recent purchases in ${context.market_name || "target market"}`);
  }

  const candidate_types = inferCandidateTypes(candidate);
  const matched_type = candidate_types.find((type) =>
    safeArray(context.target_buyer_types).includes(type)
  );
  if (matched_type) {
    reasons.push(`buyer profile fits ${matched_type.toLowerCase()}`);
  }

  if ((candidate.contact_summary?.email_count || 0) > 0) {
    reasons.push("email contact available");
  }

  if ((candidate.history?.recent_sale_count || 0) >= 2) {
    reasons.push("recent closed-buyer activity");
  }

  return reasons;
}

export function scoreBuyerCandidate({
  context = {},
  candidate = {},
} = {}) {
  const property_type_fit = scorePropertyTypeFit(context, candidate);
  const target_type_fit = scoreTargetTypeFit(context, candidate);
  const market_fit = scoreMarketFit(context, candidate);
  const price_fit = scorePriceFit(context, candidate);
  const contact_readiness = scoreContactReadiness(candidate);
  const buyer_power = scoreBuyerPower(candidate);
  const activity = scoreActivity(candidate);

  const score = clamp(
    property_type_fit +
      target_type_fit +
      market_fit +
      price_fit +
      contact_readiness +
      buyer_power +
      activity
  );

  return {
    ...candidate,
    buyer_types: inferCandidateTypes(candidate),
    score,
    score_components: {
      property_type_fit,
      target_type_fit,
      market_fit,
      price_fit,
      contact_readiness,
      buyer_power,
      activity,
    },
    reasons: buildReasonList({
      context,
      candidate,
    }),
  };
}

export function rankBuyerCandidates({
  context = {},
  candidates = [],
  limit = 10,
} = {}) {
  return sortByScore(
    safeArray(candidates).map((candidate) =>
      scoreBuyerCandidate({
        context,
        candidate,
      })
    )
  ).slice(0, limit);
}

function getHistoryKey(item = null) {
  return [
    clean(item?.__app_id),
    clean(item?.item_id),
    clean(getTextValue(item, SOLD_PROPERTY_FIELDS.property_id, "")),
  ]
    .filter(Boolean)
    .join(":");
}

function buildSoldHistoryIndex({
  sold_property_items = [],
  market_item_id = null,
  zip_item_id = null,
} = {}) {
  const by_company = new Map();
  const seen = new Set();

  for (const item of sold_property_items) {
    const dedupe_key = getHistoryKey(item);
    if (dedupe_key && seen.has(dedupe_key)) continue;
    if (dedupe_key) seen.add(dedupe_key);

    const company_name =
      clean(getTextValue(item, SOLD_PROPERTY_FIELDS.company_name, "")) ||
      clean(getTextValue(item, "title", ""));
    const normalized_company_name = normalizeBusinessName(company_name);
    if (!normalized_company_name) continue;

    const same_market =
      Number(getFirstAppReferenceId(item, SOLD_PROPERTY_FIELDS.market, 0) || 0) ===
      Number(market_item_id || 0);
    const same_zip =
      Number(getFirstAppReferenceId(item, SOLD_PROPERTY_FIELDS.zip_code, 0) || 0) ===
      Number(zip_item_id || 0);
    const sale_price =
      asNumber(getNumberValue(item, SOLD_PROPERTY_FIELDS.mls_sold_price, null)) ??
      asNumber(getNumberValue(item, SOLD_PROPERTY_FIELDS.last_sale_price, null));
    const sold_date = getDateValue(item, SOLD_PROPERTY_FIELDS.mls_sold_date, null);
    const sold_days_ago =
      sold_date ? Math.abs(diffDaysFromNow(sold_date) || 0) : null;
    const property_type = normalizePropertyType(
      getCategoryValue(item, SOLD_PROPERTY_FIELDS.property_type, "")
    );
    const property_style = clean(
      getCategoryValue(item, SOLD_PROPERTY_FIELDS.property_style, "")
    );
    const potential_flip_spread = asNumber(
      getNumberValue(item, SOLD_PROPERTY_FIELDS.potential_flip_spread, null)
    );

    if (!by_company.has(normalized_company_name)) {
      by_company.set(normalized_company_name, []);
    }

    by_company.get(normalized_company_name).push({
      item_id: item?.item_id || null,
      company_name,
      same_market,
      same_zip,
      sale_price,
      sold_date,
      sold_days_ago,
      property_type,
      property_style,
      potential_flip_spread,
    });
  }

  return by_company;
}

function summarizeHistory(entries = []) {
  const sale_prices = safeArray(entries.map((entry) => asNumber(entry.sale_price)).filter(Boolean));
  const avg_sale_price = sale_prices.length
    ? sale_prices.reduce((sum, value) => sum + value, 0) / sale_prices.length
    : null;
  const flip_spreads = safeArray(
    entries.map((entry) => asNumber(entry.potential_flip_spread)).filter(Boolean)
  );
  const avg_flip_spread = flip_spreads.length
    ? flip_spreads.reduce((sum, value) => sum + value, 0) / flip_spreads.length
    : null;

  return {
    sale_count: entries.length,
    same_market_count: entries.filter((entry) => entry.same_market).length,
    same_zip_count: entries.filter((entry) => entry.same_zip).length,
    recent_sale_count: entries.filter((entry) => entry.sold_days_ago !== null && entry.sold_days_ago <= 540).length,
    avg_sale_price: avg_sale_price !== null ? round(avg_sale_price) : null,
    avg_flip_spread: avg_flip_spread !== null ? round(avg_flip_spread) : null,
    property_types: uniqueStrings(entries.map((entry) => entry.property_type)),
    property_styles: uniqueStrings(entries.map((entry) => entry.property_style)),
  };
}

function describeAppDataLimits() {
  return [
    {
      source: "buyers_directory",
      note: "Schema is title-only in the attached export; not used as a primary ranking source.",
    },
    {
      source: "buyer_preferences",
      note: "Schema is title-only in the attached export; reserved for later enrichment once field shape is confirmed.",
    },
    {
      source: "buyer_activity",
      note: "Schema is title-only in the attached export; current scoring relies on sold-history instead.",
    },
    {
      source: "zip_buyer_scoring",
      note: "Dedicated app is title-only in the attached export; current scoring uses the richer Zip Codes app already attached in repo schema.",
    },
    {
      source: "auto_match_engine",
      note: "Schema is title-only in the attached export; this tranche implements the first auditable in-repo scorer instead.",
    },
  ];
}

function buildContextSnapshot(context = {}) {
  return {
    property_item_id: context.property_item_id,
    master_owner_item_id: context.master_owner_item_id,
    offer_item_id: context.offer_item_id,
    contract_item_id: context.contract_item_id,
    closing_item_id: context.closing_item_id,
    market_item_id: context.market_item_id,
    zip_item_id: context.zip_item_id,
    property_profile_item_id: context.property_profile_item_id,
    property_address: context.property_address,
    market_name: context.market_name,
    zip_code: context.zip_code,
    property_type: context.property_type,
    property_class: context.property_class,
    property_style: context.property_style,
    units: context.units,
    square_feet: context.square_feet,
    purchase_price: context.purchase_price,
    estimated_value: context.estimated_value,
    offer_type: context.offer_type,
    disposition_strategy: context.disposition_strategy,
    target_buyer_types: context.target_buyer_types,
    primary_target_type: context.primary_target_type,
    urgency_level: context.urgency_level,
    closing_date_target: context.closing_date_target,
    closing_days_remaining: context.closing_days_remaining,
    live_blast_supported: context.live_blast_supported,
  };
}

export function buildBuyerBlastContent({
  context = {},
  candidate = {},
  package_summary_url = null,
  package_manifest_url = null,
} = {}) {
  const subject = [
    clean(context.disposition_strategy) || "Disposition",
    normalizePropertyType(context.property_type) || "Property",
    context.property_address || context.market_name || "Opportunity",
  ]
    .filter(Boolean)
    .join(" | ");

  const lines = [
    `Opportunity: ${context.property_address || "Property details available on request"}`,
    context.market_name ? `Market: ${context.market_name}` : "",
    context.zip_code ? `ZIP: ${context.zip_code}` : "",
    context.property_type ? `Property Type: ${context.property_type}` : "",
    context.units ? `Units: ${context.units}` : "",
    context.purchase_price ? `Acquisition Price: $${Math.round(context.purchase_price).toLocaleString()}` : "",
    context.estimated_value ? `Estimated Value: $${Math.round(context.estimated_value).toLocaleString()}` : "",
    context.closing_date_target ? `Target Close: ${context.closing_date_target}` : "",
    context.disposition_strategy ? `Dispo Strategy: ${context.disposition_strategy}` : "",
    candidate.company_name ? `Why you: ${candidate.company_name} ${candidate.reasons?.length ? `matches because ${candidate.reasons.join(", ")}` : "was selected from current buyer intelligence."}` : "",
    "",
    package_summary_url ? `Disposition Package: ${package_summary_url}` : "",
    package_manifest_url ? `Package Manifest: ${package_manifest_url}` : "",
    package_summary_url
      ? "Reply with pricing questions, proof of funds, or access requests and the dispo loop will update automatically."
      : "Reply if you want the dispo package, pricing details, or access notes.",
    package_summary_url
      ? "Document links are signed and access-controlled. Attachment automation remains a later step."
      : "This tranche sends a plain-text opportunity summary only; attachment automation remains a later step.",
  ].filter(Boolean);

  return {
    subject,
    text: lines.join("\n"),
  };
}

async function resolveRelatedDealItems({
  property_id = null,
  contract_id = null,
  closing_id = null,
} = {}) {
  let closing_item = closing_id ? await getClosingItem(closing_id) : null;
  let contract_item =
    contract_id ||
    getFirstAppReferenceId(closing_item, CLOSING_FIELDS.contract, null)
      ? await getContractItem(
          contract_id || getFirstAppReferenceId(closing_item, CLOSING_FIELDS.contract, null)
        )
      : null;

  const property_item_id =
    property_id ||
    getFirstAppReferenceId(closing_item, CLOSING_FIELDS.property, null) ||
    getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.property, null) ||
    null;

  const property_item = property_item_id ? await getPropertyItem(property_item_id) : null;

  if (!contract_item && property_item_id) {
    const property_contracts = await findContractItems(
      { [CONTRACT_FIELDS.property]: property_item_id },
      25,
      0
    );
    contract_item = sortNewestFirst(property_contracts)[0] || null;
  }

  if (!closing_item && property_item_id) {
    const property_closings = await findClosingItems(
      { [CLOSING_FIELDS.property]: property_item_id },
      25,
      0
    );
    closing_item = sortNewestFirst(property_closings)[0] || null;
  }

  let offer_item = null;
  const offer_item_id = getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.offer, null);
  if (offer_item_id) {
    offer_item = await getOfferItem(offer_item_id);
  } else if (property_item_id) {
    const property_offers = await findOfferItems(
      { [OFFER_FIELDS.property]: property_item_id },
      25,
      0
    );
    offer_item = sortNewestFirst(property_offers)[0] || null;
  }

  return {
    property_item,
    offer_item,
    contract_item,
    closing_item,
  };
}

export async function loadBuyerDispositionContext({
  property_id = null,
  contract_id = null,
  closing_id = null,
} = {}) {
  const { property_item, offer_item, contract_item, closing_item } =
    await resolveRelatedDealItems({
      property_id,
      contract_id,
      closing_id,
    });

  const property_item_id = property_item?.item_id || property_id || null;
  const master_owner_item_id =
    getFirstAppReferenceId(offer_item, OFFER_FIELDS.master_owner, null) ||
    getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.master_owner, null) ||
    getFirstAppReferenceId(closing_item, CLOSING_FIELDS.master_owner, null) ||
    null;
  const market_item_id =
    getFirstAppReferenceId(property_item, "market-2", null) ||
    getFirstAppReferenceId(offer_item, OFFER_FIELDS.market, null) ||
    getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.market, null) ||
    getFirstAppReferenceId(closing_item, CLOSING_FIELDS.market, null) ||
    null;
  const zip_item_id =
    getFirstAppReferenceId(property_item, "relationship", null) ||
    null;

  const [market_item, zip_item] = await Promise.all([
    market_item_id ? getMarketItem(market_item_id) : Promise.resolve(null),
    zip_item_id ? getZipCodeItem(zip_item_id) : Promise.resolve(null),
  ]);

  const property_address =
    getLocationText(property_item, "property-address", "") ||
    clean(getTextValue(property_item, "full-name", ""));
  const zip_code =
    clean(getTextValue(zip_item, ZIP_CODE_FIELDS.title, "")) ||
    extractZipFromText(property_address) ||
    null;
  const market_name =
    clean(getTextValue(market_item, MARKET_FIELDS.title, "")) ||
    clean(getCategoryValue(property_item, "market-3", "")) ||
    clean(getCategoryValue(property_item, "market", "")) ||
    null;
  const property_type = normalizePropertyType(
    getCategoryValue(property_item, "property-type", "")
  );
  const property_class = clean(getCategoryValue(property_item, "property-class", ""));
  const property_style = clean(getCategoryValue(property_item, "property-style", ""));
  const units =
    asNumber(getNumberValue(property_item, "number-of-units", null)) ??
    asNumber(getNumberValue(property_item, "units", null));
  const square_feet =
    asNumber(getNumberValue(property_item, "building-sqft", null)) ??
    asNumber(getNumberValue(property_item, "avg-sqft-per-unit", null));
  const purchase_price =
    asNumber(getMoneyValue(contract_item, CONTRACT_FIELDS.purchase_price_final, null)) ??
    asNumber(getMoneyValue(offer_item, OFFER_FIELDS.offer_sent_price, null)) ??
    asNumber(getMoneyValue(offer_item, OFFER_FIELDS.seller_counter_offer, null)) ??
    asNumber(getMoneyValue(offer_item, OFFER_FIELDS.seller_asking_price, null)) ??
    asNumber(getNumberValue(property_item, "smart-cash-offer-2", null));
  const estimated_value =
    asNumber(getNumberValue(property_item, "estimated-value-2", null)) ??
    asNumber(getNumberValue(property_item, "estimated-value", null));
  const closing_date_target =
    getDateValue(contract_item, CONTRACT_FIELDS.closing_date_target, null) ||
    getDateValue(offer_item, OFFER_FIELDS.closing_date_target, null) ||
    getDateValue(closing_item, CLOSING_FIELDS.closing_date_time, null) ||
    null;
  const closing_days_remaining = diffDaysFromNow(closing_date_target);
  const offer_type = clean(getCategoryValue(offer_item, OFFER_FIELDS.offer_type, ""));
  const property_profile_item_id =
    getFirstAppReferenceId(property_item, "comp-search-profile-hash", null) ||
    getFirstAppReferenceId(property_item, "property-profile", null) ||
    null;
  const disposition_strategy = inferDispositionStrategy({
    offer_type,
    property_type,
    units,
  });
  const target_buyer_types = inferTargetBuyerTypes({
    property_type,
    offer_type,
    units,
  });
  const live_blast_supported = ["Assignment", "Double Close"].includes(disposition_strategy);

  return {
    property_item_id,
    master_owner_item_id,
    offer_item_id: offer_item?.item_id || null,
    contract_item_id: contract_item?.item_id || null,
    closing_item_id: closing_item?.item_id || null,
    market_item_id,
    zip_item_id,
    property_profile_item_id,
    property_item,
    offer_item,
    contract_item,
    closing_item,
    market_item,
    zip_item,
    property_address,
    market_name,
    zip_code,
    property_type,
    property_class,
    property_style,
    units,
    square_feet,
    purchase_price,
    estimated_value,
    offer_type,
    contract_status: clean(getCategoryValue(contract_item, CONTRACT_FIELDS.contract_status, "")),
    closing_status: clean(getCategoryValue(closing_item, CLOSING_FIELDS.closing_status, "")),
    closing_date_target,
    closing_days_remaining,
    disposition_strategy,
    target_buyer_types,
    primary_target_type: target_buyer_types[0] || "Unknown",
    urgency_level:
      closing_days_remaining !== null && closing_days_remaining <= 7
        ? "Urgent"
        : closing_days_remaining !== null && closing_days_remaining <= 14
          ? "High"
          : closing_days_remaining !== null && closing_days_remaining <= 30
            ? "Medium"
            : "Low",
    market_signals: {
      cash_buyer_density_score: asNumber(
        getNumberValue(market_item, MARKET_FIELDS.cash_buyer_density_score, null)
      ),
      hedge_fund_density_score: asNumber(
        getNumberValue(market_item, MARKET_FIELDS.hedge_fund_density_score, null)
      ),
      mf_buyer_density_score: asNumber(
        getNumberValue(market_item, MARKET_FIELDS.mf_buyer_density_score, null)
      ),
      market_hotness_score: asNumber(
        getNumberValue(market_item, MARKET_FIELDS.market_hotness_score, null)
      ),
      best_strategy: clean(getCategoryValue(market_item, MARKET_FIELDS.best_strategy, "")),
    },
    zip_signals: {
      cash_buyer_activity_score: asNumber(
        getNumberValue(zip_item, ZIP_CODE_FIELDS.cash_buyer_activity_score, null)
      ),
      flip_volume_score: asNumber(
        getNumberValue(zip_item, ZIP_CODE_FIELDS.flip_volume_score, null)
      ),
      landlord_density_score: asNumber(
        getNumberValue(zip_item, ZIP_CODE_FIELDS.landlord_density_score, null)
      ),
      median_rent: asNumber(getNumberValue(zip_item, ZIP_CODE_FIELDS.median_rent, null)),
      market_temperature: clean(
        getCategoryValue(zip_item, ZIP_CODE_FIELDS.market_temperature, "")
      ),
    },
    live_blast_supported,
  };
}

async function enrichCandidateContacts(candidate = {}) {
  const company_item = candidate.raw_company_item || null;
  const email_item_ids = getAppReferenceIds(company_item, COMPANY_FIELDS.contact_emails);
  const phone_item_ids = getAppReferenceIds(company_item, COMPANY_FIELDS.contact_phones);
  const officer_item_ids = getAppReferenceIds(company_item, COMPANY_FIELDS.primary_officers);

  const [email_items, phone_items, officer_items] = await Promise.all([
    Promise.all(email_item_ids.slice(0, 5).map((item_id) => getItem(item_id).catch(() => null))),
    Promise.all(phone_item_ids.slice(0, 5).map((item_id) => getItem(item_id).catch(() => null))),
    Promise.all(
      officer_item_ids.slice(0, 5).map((item_id) => getBuyerOfficerItem(item_id).catch(() => null))
    ),
  ]);

  const emails = uniqueStrings([
    ...email_items.flatMap((item) => extractEmailsFromItem(item)),
    ...officer_items.flatMap((item) => extractEmailsFromItem(item)),
  ]);
  const phones = uniqueStrings([
    ...phone_items.flatMap((item) => extractPhonesFromItem(item)),
    ...officer_items.flatMap((item) => extractPhonesFromItem(item)),
  ]);

  return {
    ...candidate,
    emails,
    phones,
    officers: officer_items
      .filter((item) => item?.item_id)
      .map((item) => ({
        item_id: item.item_id,
        name:
          clean(getTextValue(item, BUYER_OFFICER_FIELDS.contact_name, "")) ||
          clean(getTextValue(item, BUYER_OFFICER_FIELDS.owner_full_name, "")) ||
          clean(item?.title),
      })),
  };
}

function normalizeCandidate(company_item = null, sold_history_map = new Map()) {
  const company_name =
    clean(getTextValue(company_item, COMPANY_FIELDS.owner_full_name, "")) ||
    clean(getTextValue(company_item, COMPANY_FIELDS.owner_last_name, "")) ||
    clean(getTextValue(company_item, "title", "")) ||
    clean(company_item?.title);
  const normalized_name = normalizeBusinessName(company_name);
  const history_entries = sold_history_map.get(normalized_name) || [];
  const contact_email_refs = getAppReferenceIds(company_item, COMPANY_FIELDS.contact_emails);
  const contact_phone_refs = getAppReferenceIds(company_item, COMPANY_FIELDS.contact_phones);
  const officer_refs = getAppReferenceIds(company_item, COMPANY_FIELDS.primary_officers);

  return {
    item_id: company_item?.item_id || null,
    company_name,
    normalized_name,
    owner_type: clean(getCategoryValue(company_item, COMPANY_FIELDS.owner_type, "")),
    preferred_contact_method: clean(
      getCategoryValue(company_item, COMPANY_FIELDS.preferred_contact_method, "")
    ),
    total_properties_owned: asNumber(
      getNumberValue(company_item, COMPANY_FIELDS.total_properties_owned, null)
    ),
    estimated_portfolio_value: asNumber(
      getMoneyValue(company_item, COMPANY_FIELDS.estimated_portfolio_value, null)
    ),
    property_profile_item_id: getFirstAppReferenceId(
      company_item,
      COMPANY_FIELDS.property_profile,
      null
    ),
    contact_summary: {
      email_count: contact_email_refs.length,
      phone_count: contact_phone_refs.length,
      officer_count: officer_refs.length,
    },
    history: summarizeHistory(history_entries),
    raw_company_item: company_item,
  };
}

export async function buildBuyerMatchDiagnostics({
  property_id = null,
  contract_id = null,
  closing_id = null,
  candidate_limit = 10,
} = {}) {
  const context = await loadBuyerDispositionContext({
    property_id,
    contract_id,
    closing_id,
  });

  if (!context.property_item_id) {
    return {
      ok: false,
      reason: "missing_property_context",
      context: buildContextSnapshot(context),
      diagnostics: {
        top_candidates: [],
        data_source_limits: describeAppDataLimits(),
      },
    };
  }

  const [companies, same_market_sales, same_zip_sales] = await Promise.all([
    fetchAllCompanyItems({}, { page_size: 500 }),
    context.market_item_id
      ? findSoldPropertyItems({ [SOLD_PROPERTY_FIELDS.market]: context.market_item_id }, 300, 0)
      : Promise.resolve([]),
    context.zip_item_id
      ? findSoldPropertyItems({ [SOLD_PROPERTY_FIELDS.zip_code]: context.zip_item_id }, 200, 0)
      : Promise.resolve([]),
  ]);

  const sold_history_map = buildSoldHistoryIndex({
    sold_property_items: [...same_market_sales, ...same_zip_sales],
    market_item_id: context.market_item_id,
    zip_item_id: context.zip_item_id,
  });

  const normalized_candidates = safeArray(companies)
    .map((company_item) => normalizeCandidate(company_item, sold_history_map))
    .filter((candidate) =>
      candidate.item_id &&
      candidate.company_name &&
      (
        candidate.history.sale_count > 0 ||
        candidate.contact_summary.email_count > 0 ||
        candidate.contact_summary.phone_count > 0 ||
        candidate.contact_summary.officer_count > 0
      )
    );

  const scored_candidates = rankBuyerCandidates({
    context,
    candidates: normalized_candidates,
    limit: Math.max(candidate_limit, 3),
  });

  const enriched_candidates = await Promise.all(
    scored_candidates.map((candidate) => enrichCandidateContacts(candidate))
  );

  const top_candidates = sortByScore(enriched_candidates)
    .slice(0, candidate_limit)
    .map((candidate, index) => ({
      rank: index + 1,
      item_id: candidate.item_id,
      company_name: candidate.company_name,
      owner_type: candidate.owner_type,
      preferred_contact_method: candidate.preferred_contact_method || null,
      score: candidate.score,
      buyer_types: candidate.buyer_types,
      reasons: candidate.reasons,
      history: candidate.history,
      contact_summary: {
        ...candidate.contact_summary,
        resolved_email_count: candidate.emails?.length || 0,
        resolved_phone_count: candidate.phones?.length || 0,
      },
      emails: candidate.emails || [],
      phones: candidate.phones || [],
      officers: candidate.officers || [],
      blast_preview: buildBuyerBlastContent({
        context,
        candidate,
      }),
    }));

  return {
    ok: true,
    reason: top_candidates.length
      ? "buyer_candidates_ranked"
      : "no_buyer_candidates_ranked",
    context: buildContextSnapshot(context),
    diagnostics: {
      data_source_limits: describeAppDataLimits(),
      buyer_universe_count: normalized_candidates.length,
      sold_history_market_count: same_market_sales.length,
      sold_history_zip_count: same_zip_sales.length,
      viable_candidate_count: top_candidates.filter((candidate) => candidate.score >= 45).length,
      top_candidates,
    },
  };
}

export async function resolveExistingBuyerMatch({
  property_id = null,
  contract_id = null,
  closing_id = null,
  buyer_match_item_id = null,
} = {}) {
  if (buyer_match_item_id) {
    const direct = await getBuyerMatchItem(buyer_match_item_id);
    if (direct?.item_id) return direct;
  }

  const by_contract = await findLatestBuyerMatchByContractId(contract_id);
  if (by_contract?.item_id) return by_contract;

  const by_closing = await findLatestBuyerMatchByClosingId(closing_id);
  if (by_closing?.item_id) return by_closing;

  const by_property = await findLatestBuyerMatchByPropertyId(property_id);
  if (by_property?.item_id) return by_property;

  return null;
}

export default {
  buildBuyerBlastContent,
  buildBuyerMatchDiagnostics,
  inferDispositionStrategy,
  inferTargetBuyerTypes,
  loadBuyerDispositionContext,
  rankBuyerCandidates,
  resolveExistingBuyerMatch,
  scoreBuyerCandidate,
};
