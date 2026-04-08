function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export function normalizeMarketLabel(value) {
  return clean(value).replace(/\s+/g, " ");
}

function marketState(value) {
  const normalized = normalizeMarketLabel(value);
  if (!normalized.includes(",")) return "";
  return clean(normalized.split(",").at(-1) || "").toUpperCase();
}

export const MARKET_ALIASES = Object.freeze({
  "St. Paul, MN": "Minneapolis, MN",
  "Fort Worth, TX": "Dallas, TX",
  "West Palm Beach, FL": "Miami, FL",
  "Fort Lauderdale, FL": "Miami, FL",
  "Inland Empire, CA": "Riverside, CA",
  "San Bernardino, CA": "Riverside, CA",
  "Palm Springs, CA": "Riverside, CA",
  "Stockton/Modesto, CA": "Stockton, CA",
  "Kansas City, KS": "Kansas City, MO",
});

const NORMALIZED_ALIASES = new Map(
  Object.entries(MARKET_ALIASES).map(([alias, canonical]) => [
    lower(normalizeMarketLabel(alias)),
    normalizeMarketLabel(canonical),
  ])
);

export const CLUSTER_PHONE_MARKETS = Object.freeze({
  miami_cluster: Object.freeze(["Miami, FL"]),
  jacksonville_cluster: Object.freeze(["Jacksonville, FL"]),
  dallas_houston_cluster: Object.freeze(["Dallas, TX", "Houston, TX"]),
  charlotte_cluster: Object.freeze(["Charlotte, NC"]),
  atlanta_cluster: Object.freeze(["Atlanta, GA"]),
  los_angeles_cluster: Object.freeze(["Los Angeles, CA"]),
  minneapolis_cluster: Object.freeze(["Minneapolis, MN"]),
});

const EXPLICIT_MARKET_CLUSTER = Object.freeze({
  "Miami, FL": "miami_cluster",
  "Jacksonville, FL": "jacksonville_cluster",
  "Orlando, FL": "jacksonville_cluster",
  "Tampa, FL": "jacksonville_cluster",
  "Dallas, TX": "dallas_houston_cluster",
  "Houston, TX": "dallas_houston_cluster",
  "Oklahoma City, OK": "dallas_houston_cluster",
  "Tulsa, OK": "dallas_houston_cluster",
  "Charlotte, NC": "charlotte_cluster",
  "Hampton Roads, VA": "charlotte_cluster",
  "Atlanta, GA": "atlanta_cluster",
  "Los Angeles, CA": "los_angeles_cluster",
  "Riverside, CA": "los_angeles_cluster",
  "Stockton, CA": "los_angeles_cluster",
  "Bakersfield, CA": "los_angeles_cluster",
  "Fresno, CA": "los_angeles_cluster",
  "Sacramento, CA": "los_angeles_cluster",
  "Minneapolis, MN": "minneapolis_cluster",
  "St. Paul, MN": "minneapolis_cluster",
});

const STATE_CLUSTER_MAP = Object.freeze({
  FL: "miami_cluster",
  TX: "dallas_houston_cluster",
  OK: "dallas_houston_cluster",
  NC: "charlotte_cluster",
  VA: "charlotte_cluster",
  GA: "atlanta_cluster",
  AL: "atlanta_cluster",
  TN: "atlanta_cluster",
  LA: "atlanta_cluster",
  CA: "los_angeles_cluster",
  AZ: "los_angeles_cluster",
  NV: "los_angeles_cluster",
  NM: "los_angeles_cluster",
  UT: "los_angeles_cluster",
  CO: "los_angeles_cluster",
  ID: "los_angeles_cluster",
  WA: "los_angeles_cluster",
  MN: "minneapolis_cluster",
  WI: "minneapolis_cluster",
  MI: "minneapolis_cluster",
  IL: "minneapolis_cluster",
  IN: "minneapolis_cluster",
  IA: "minneapolis_cluster",
  MO: "minneapolis_cluster",
  KS: "minneapolis_cluster",
  OH: "minneapolis_cluster",
  KY: "minneapolis_cluster",
  NE: "minneapolis_cluster",
  PA: "minneapolis_cluster",
  MD: "minneapolis_cluster",
});

function resolveClusterForMarket(normalized_market) {
  const explicit = EXPLICIT_MARKET_CLUSTER[normalized_market];
  if (explicit) return explicit;
  return STATE_CLUSTER_MAP[marketState(normalized_market)] || null;
}

export function resolveMarketSendingProfile(raw_market = null) {
  const normalized_raw_market = normalizeMarketLabel(raw_market);
  if (!normalized_raw_market) {
    return {
      ok: false,
      reason: "missing_market",
      raw_market: clean(raw_market) || null,
      normalized_raw_market: null,
      normalized_market: null,
      primary_cluster: null,
      allowed_phone_markets: [],
      priority_chain: [],
    };
  }

  if (lower(normalized_raw_market) === "unmapped") {
    return {
      ok: false,
      reason: "routing_unmapped",
      raw_market: clean(raw_market),
      normalized_raw_market,
      normalized_market: null,
      primary_cluster: null,
      allowed_phone_markets: [],
      priority_chain: [],
    };
  }

  const normalized_market =
    NORMALIZED_ALIASES.get(lower(normalized_raw_market)) || normalized_raw_market;
  const alias_applied = normalized_market !== normalized_raw_market;
  const primary_cluster = resolveClusterForMarket(normalized_market);

  if (!primary_cluster) {
    return {
      ok: false,
      reason: "routing_unmapped",
      raw_market: clean(raw_market),
      normalized_raw_market,
      normalized_market,
      primary_cluster: null,
      allowed_phone_markets: [],
      priority_chain: [],
    };
  }

  return {
    ok: true,
    reason: alias_applied ? "mapped_via_alias" : "mapped_exact_or_cluster",
    raw_market: clean(raw_market),
    normalized_raw_market,
    normalized_market,
    alias_applied,
    primary_cluster,
    allowed_phone_markets: [...(CLUSTER_PHONE_MARKETS[primary_cluster] || [])],
    priority_chain: [
      { tier: "exact_market_match", market: normalized_raw_market },
      { tier: "alias_market_match", market: normalized_market },
      {
        tier: "regional_cluster_fallback",
        markets: [...(CLUSTER_PHONE_MARKETS[primary_cluster] || [])],
      },
    ],
  };
}

export default {
  CLUSTER_PHONE_MARKETS,
  MARKET_ALIASES,
  normalizeMarketLabel,
  resolveMarketSendingProfile,
};
