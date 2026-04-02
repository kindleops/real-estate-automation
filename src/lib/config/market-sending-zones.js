function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export function normalizeMarketLabel(value) {
  return clean(value).replace(/\s+/g, " ");
}

export const SENDING_ZONE_PHONE_MARKETS = Object.freeze({
  "Charlotte, NC": Object.freeze(["Charlotte, NC"]),
  "Los Angeles, CA": Object.freeze(["Los Angeles, CA"]),
  "Dallas, TX": Object.freeze(["Dallas, TX"]),
  "Minneapolis, MN": Object.freeze(["Minneapolis, MN"]),
  "Atlanta, GA": Object.freeze(["Atlanta, GA"]),
});

export const RAW_MARKET_TO_SENDING_ZONE = Object.freeze({
  "Charlotte, NC": "Charlotte, NC",
  "Fayetteville, NC": "Charlotte, NC",
  "Durham, NC": "Charlotte, NC",
  "Rocky Mount, NC": "Charlotte, NC",
  "Los Angeles, CA": "Los Angeles, CA",
  "Riverside, CA": "Los Angeles, CA",
  "Inland Empire, CA": "Los Angeles, CA",
  "San Bernardino, CA": "Los Angeles, CA",
  "Palm Springs, CA": "Los Angeles, CA",
  "Stockton, CA": "Los Angeles, CA",
  "Modesto, CA": "Los Angeles, CA",
  "Dallas, TX": "Dallas, TX",
  "Austin, TX": "Dallas, TX",
  "Tulsa, OK": "Dallas, TX",
  "Minneapolis, MN": "Minneapolis, MN",
  "Spokane, WA": "Minneapolis, MN",
  "Atlanta, GA": "Atlanta, GA",
  "Providence, RI": "Atlanta, GA",
  "Phoenix, AZ": "Dallas, TX",
  Unmapped: null,
});

const NORMALIZED_RAW_MARKET_TO_SENDING_ZONE = new Map(
  Object.entries(RAW_MARKET_TO_SENDING_ZONE).map(([raw_market, sending_zone]) => [
    lower(normalizeMarketLabel(raw_market)),
    sending_zone,
  ])
);

export function resolveMarketSendingProfile(raw_market = null) {
  const normalized_raw_market = normalizeMarketLabel(raw_market);

  if (!normalized_raw_market) {
    return {
      ok: false,
      reason: "missing_market",
      raw_market: clean(raw_market) || null,
      normalized_raw_market: null,
      sending_zone: null,
      allowed_phone_markets: [],
      primary_phone_market: null,
    };
  }

  const sending_zone = NORMALIZED_RAW_MARKET_TO_SENDING_ZONE.get(lower(normalized_raw_market));

  if (!sending_zone) {
    return {
      ok: false,
      reason: "market_unmapped_excluded",
      raw_market: clean(raw_market) || normalized_raw_market,
      normalized_raw_market,
      sending_zone: null,
      allowed_phone_markets: [],
      primary_phone_market: null,
    };
  }

  const allowed_phone_markets = Array.isArray(SENDING_ZONE_PHONE_MARKETS[sending_zone])
    ? [...SENDING_ZONE_PHONE_MARKETS[sending_zone]]
    : [];

  if (!allowed_phone_markets.length) {
    return {
      ok: false,
      reason: "sending_zone_unconfigured",
      raw_market: clean(raw_market) || normalized_raw_market,
      normalized_raw_market,
      sending_zone,
      allowed_phone_markets: [],
      primary_phone_market: null,
    };
  }

  return {
    ok: true,
    reason: "mapped_to_sending_zone",
    raw_market: clean(raw_market) || normalized_raw_market,
    normalized_raw_market,
    sending_zone,
    allowed_phone_markets,
    primary_phone_market: allowed_phone_markets[0] || null,
  };
}

export default {
  SENDING_ZONE_PHONE_MARKETS,
  RAW_MARKET_TO_SENDING_ZONE,
  normalizeMarketLabel,
  resolveMarketSendingProfile,
};
