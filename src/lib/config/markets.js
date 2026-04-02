export const MARKETS = {
  rocky_mount_nc: {
    key: "rocky_mount_nc",
    name: "Rocky Mount, NC",
    state: "NC",
    timezone: "Eastern",
    is_active: true,
  },
  fayetteville_nc: {
    key: "fayetteville_nc",
    name: "Fayetteville, NC",
    state: "NC",
    timezone: "Eastern",
    is_active: true,
  },
  durham_nc: {
    key: "durham_nc",
    name: "Durham, NC",
    state: "NC",
    timezone: "Eastern",
    is_active: true,
  },
  san_bernardino_ca: {
    key: "san_bernardino_ca",
    name: "San Bernardino, CA",
    state: "CA",
    timezone: "Pacific",
    is_active: true,
  },
  palm_springs_ca: {
    key: "palm_springs_ca",
    name: "Palm Springs, CA",
    state: "CA",
    timezone: "Pacific",
    is_active: true,
  },
  riverside_ca: {
    key: "riverside_ca",
    name: "Riverside, CA",
    state: "CA",
    timezone: "Pacific",
    is_active: true,
  },
  stockton_modesto_ca: {
    key: "stockton_modesto_ca",
    name: "Stockton/Modesto, CA",
    state: "CA",
    timezone: "Pacific",
    is_active: true,
  },
  west_palm_beach_fl: {
    key: "west_palm_beach_fl",
    name: "West Palm Beach, FL",
    state: "FL",
    timezone: "Eastern",
    is_active: true,
  },
  fort_lauderdale_fl: {
    key: "fort_lauderdale_fl",
    name: "Fort Lauderdale, FL",
    state: "FL",
    timezone: "Eastern",
    is_active: true,
  },
  colorado_springs_co: {
    key: "colorado_springs_co",
    name: "Colorado Springs, CO",
    state: "CO",
    timezone: "Mountain",
    is_active: true,
  },
  hampton_roads_va: {
    key: "hampton_roads_va",
    name: "Hampton Roads, VA",
    state: "VA",
    timezone: "Eastern",
    is_active: true,
  },
  pittsburgh_pa: {
    key: "pittsburgh_pa",
    name: "Pittsburgh, PA",
    state: "PA",
    timezone: "Eastern",
    is_active: true,
  },
  st_louis_mo: {
    key: "st_louis_mo",
    name: "St. Louis, MO",
    state: "MO",
    timezone: "Central",
    is_active: true,
  },
  unmapped: {
    key: "unmapped",
    name: "Unmapped",
    state: null,
    timezone: "Central",
    is_active: true,
  },
  charlotte_nc: {
    key: "charlotte_nc",
    name: "Charlotte, NC",
    state: "NC",
    timezone: "Eastern",
    is_active: true,
  },
  st_paul_mn: {
    key: "st_paul_mn",
    name: "St. Paul, MN",
    state: "MN",
    timezone: "Central",
    is_active: true,
  },
  kansas_city_ks: {
    key: "kansas_city_ks",
    name: "Kansas City, KS",
    state: "KS",
    timezone: "Central",
    is_active: true,
  },
  fort_worth_tx: {
    key: "fort_worth_tx",
    name: "Fort Worth, TX",
    state: "TX",
    timezone: "Central",
    is_active: true,
  },
  tucson_az: {
    key: "tucson_az",
    name: "Tucson, AZ",
    state: "AZ",
    timezone: "Mountain",
    is_active: true,
  },
  inland_empire_ca: {
    key: "inland_empire_ca",
    name: "Inland Empire, CA",
    state: "CA",
    timezone: "Pacific",
    is_active: true,
  },
  spokane_wa: {
    key: "spokane_wa",
    name: "Spokane, WA",
    state: "WA",
    timezone: "Pacific",
    is_active: true,
  },
  los_angeles_ca: {
    key: "los_angeles_ca",
    name: "Los Angeles, CA",
    state: "CA",
    timezone: "Pacific",
    is_active: true,
  },
  tampa_fl: {
    key: "tampa_fl",
    name: "Tampa, FL",
    state: "FL",
    timezone: "Eastern",
    is_active: true,
  },
  las_vegas_nv: {
    key: "las_vegas_nv",
    name: "Las Vegas, NV",
    state: "NV",
    timezone: "Pacific",
    is_active: true,
  },
  houston_tx: {
    key: "houston_tx",
    name: "Houston, TX",
    state: "TX",
    timezone: "Central",
    is_active: true,
  },
  clayton_ga: {
    key: "clayton_ga",
    name: "Clayton, GA",
    state: "GA",
    timezone: "Eastern",
    is_active: true,
  },
  kansas_city_mo: {
    key: "kansas_city_mo",
    name: "Kansas City, MO",
    state: "MO",
    timezone: "Central",
    is_active: true,
  },
  milwaukee_wi: {
    key: "milwaukee_wi",
    name: "Milwaukee, WI",
    state: "WI",
    timezone: "Central",
    is_active: true,
  },
  minneapolis_mn: {
    key: "minneapolis_mn",
    name: "Minneapolis, MN",
    state: "MN",
    timezone: "Central",
    is_active: true,
  },
  chicago_il: {
    key: "chicago_il",
    name: "Chicago, IL",
    state: "IL",
    timezone: "Central",
    is_active: true,
  },
  philadelphia_pa: {
    key: "philadelphia_pa",
    name: "Philadelphia, PA",
    state: "PA",
    timezone: "Eastern",
    is_active: true,
  },
  baltimore_md: {
    key: "baltimore_md",
    name: "Baltimore, MD",
    state: "MD",
    timezone: "Eastern",
    is_active: true,
  },
  detroit_mi: {
    key: "detroit_mi",
    name: "Detroit, MI",
    state: "MI",
    timezone: "Eastern",
    is_active: true,
  },
  cleveland_oh: {
    key: "cleveland_oh",
    name: "Cleveland, OH",
    state: "OH",
    timezone: "Eastern",
    is_active: true,
  },
  omaha_ne: {
    key: "omaha_ne",
    name: "Omaha, NE",
    state: "NE",
    timezone: "Central",
    is_active: true,
  },
  rochester_ny: {
    key: "rochester_ny",
    name: "Rochester, NY",
    state: "NY",
    timezone: "Eastern",
    is_active: true,
  },
};

export function getMarketByKey(key) {
  return MARKETS[key] || null;
}

export function getMarketByName(name) {
  const target = String(name || "").trim().toLowerCase();

  return (
    Object.values(MARKETS).find(
      (market) => market.name.trim().toLowerCase() === target
    ) || null
  );
}

export function getMarketTimezone(nameOrKey, fallback = "Central") {
  return (
    MARKETS[nameOrKey]?.timezone ||
    getMarketByName(nameOrKey)?.timezone ||
    fallback
  );
}

export function getActiveMarkets() {
  return Object.values(MARKETS).filter((market) => market.is_active);
}

export function getMarketsByTimezone(timezone) {
  const target = String(timezone || "").trim().toLowerCase();

  return Object.values(MARKETS).filter(
    (market) => String(market.timezone || "").trim().toLowerCase() === target
  );
}

export default MARKETS;