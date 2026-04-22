/**
 * targeting-console.js
 *
 * Domain module for the Targeting Console — market-level campaign management.
 *
 * Pure normalisation helpers are exported directly and are safe to use
 * anywhere.  Supabase CRUD functions use injectable deps so they can be
 * unit-tested without a live database connection.
 */

import { supabase } from "@/lib/supabase/client.js";

// ---------------------------------------------------------------------------
// Dependency injection (test support)
// ---------------------------------------------------------------------------

let _deps = { supabase_override: null };

export function __setTargetingConsoleDeps(overrides) {
  _deps = { ..._deps, ...overrides };
}

export function __resetTargetingConsoleDeps() {
  _deps = { supabase_override: null };
}

function getDb() {
  return _deps.supabase_override ?? supabase;
}

// ---------------------------------------------------------------------------
// Normalisation helpers
// ---------------------------------------------------------------------------

/**
 * Convert a market string to a safe slug (lowercase, underscores).
 * e.g. "Los Angeles" → "los_angeles"
 */
export function normalizeMarketSlug(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/**
 * Normalise an asset type to a slug.
 * e.g. "SFR" → "sfr", "Multifamily" → "multifamily"
 */
export function normalizeAssetType(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/** Alias for normalizeAssetType — preferred name for v2 targeting. */
export const normalizeAssetSlug = normalizeAssetType;

/**
 * Normalise a strategy to a slug.
 * e.g. "Cash" → "cash", "Multifamily Underwrite" → "multifamily_underwrite"
 */
export function normalizeStrategy(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/** Alias for normalizeStrategy — preferred name for v2 targeting. */
export const normalizeStrategySlug = normalizeStrategy;

// ---------------------------------------------------------------------------
// V2 — Static choice registries
// ---------------------------------------------------------------------------

const MARKET_LABEL_MAP = {
  los_angeles:     "Los Angeles",
  miami:           "Miami",
  dallas_fort_worth: "Dallas / Fort Worth",
  houston:         "Houston",
  jacksonville:    "Jacksonville",
  new_orleans:     "New Orleans",
  atlanta:         "Atlanta",
  tampa:           "Tampa",
  orlando:         "Orlando",
  phoenix:         "Phoenix",
  las_vegas:       "Las Vegas",
  cleveland:       "Cleveland",
  detroit:         "Detroit",
  memphis:         "Memphis",
  birmingham:      "Birmingham",
  indianapolis:    "Indianapolis",
  charlotte:       "Charlotte",
  san_antonio:     "San Antonio",
  austin:          "Austin",
  chicago:         "Chicago",
  st_louis:        "St. Louis",
  kansas_city:     "Kansas City",
  minneapolis:     "Minneapolis",
  nashville:       "Nashville",
  philadelphia:    "Philadelphia",
};

const ASSET_LABEL_MAP = {
  sfr:                    "SFR / Single Family",
  multifamily:            "Multifamily",
  duplex:                 "Duplex",
  vacant_land:            "Vacant Land",
  distressed_residential: "Distressed Residential",
  commercial:             "Commercial",
  hotel_motel:            "Hotel / Motel",
  self_storage:           "Self Storage",
};

const STRATEGY_LABEL_MAP = {
  cash:                   "Cash Offer",
  creative:               "Creative Finance",
  multifamily_underwrite: "Multifamily Underwrite",
  distress_stack:         "Distress Stack",
  probate:                "Probate / Inherited",
  tired_landlord:         "Tired Landlord",
  pre_foreclosure:        "Pre-Foreclosure",
  high_equity:            "High Equity",
};

const TAG_LABEL_MAP = {
  absentee_owner:      "Absentee Owner",
  out_of_state_owner:  "Out of State Owner",
  vacant:              "Vacant",
  high_equity:         "High Equity",
  free_and_clear:      "Free and Clear",
  tax_delinquent:      "Tax Delinquent",
  pre_foreclosure:     "Pre-Foreclosure",
  probate:             "Probate / Inherited",
  tired_landlord:      "Tired Landlord",
  senior_owner:        "Senior Owner",
  empty_nester:        "Empty Nester",
  corporate_owner:     "Corporate Owner",
  low_equity:          "Low Equity",
  active_lien:         "Active Lien",
  likely_to_move:      "Likely To Move",
  distressed_property: "Distressed Property",
  unknown_equity:      "Unknown Equity",
};

const KNOWN_MARKET_SLUGS = new Set(Object.keys(MARKET_LABEL_MAP));

const MARKET_REGION_MAP = {
  texas: [
    "Dallas, TX",
    "Fort Worth, TX",
    "Houston, TX",
    "Austin, TX",
    "San Antonio, TX",
    "El Paso, TX",
  ],
  florida: [
    "Miami, FL",
    "Fort Lauderdale, FL",
    "West Palm Beach, FL",
    "Tampa, FL",
    "Orlando, FL",
    "Jacksonville, FL",
  ],
  california: [
    "Los Angeles, CA",
    "Riverside, CA",
    "San Bernardino, CA",
    "Sacramento, CA",
    "Fresno, CA",
    "Bakersfield, CA",
    "Stockton, CA",
    "Palm Springs, CA",
    "Inland Empire, CA",
  ],
  southeast: [
    "Atlanta, GA",
    "Charlotte, NC",
    "Birmingham, AL",
    "New Orleans, LA",
    "Memphis, TN",
    "Durham, NC",
    "Fayetteville, NC",
    "Rocky Mount, NC",
    "Richmond, VA",
    "Hampton Roads, VA",
  ],
  midwest: [
    "Chicago, IL",
    "Minneapolis, MN",
    "Milwaukee, WI",
    "Indianapolis, IN",
    "Columbus, OH",
    "Cleveland, OH",
    "Cincinnati, OH",
    "Detroit, MI",
    "St. Louis, MO",
    "Kansas City, MO",
    "Louisville, KY",
    "Pittsburgh, PA",
  ],
  northeast: [
    "Philadelphia, PA",
    "Baltimore, MD",
    "Providence, RI",
    "Hartford, CT",
    "Rochester, NY",
  ],
  west_mountain: [
    "Phoenix, AZ",
    "Las Vegas, NV",
    "Tucson, AZ",
    "Salt Lake City, UT",
    "Colorado Springs, CO",
    "Albuquerque, NM",
    "Omaha, NE",
    "Des Moines, IA",
    "Wichita, KS",
    "Tulsa, OK",
    "Oklahoma City, OK",
    "Spokane, WA",
  ],
  other: ["Unmapped"],
};

const MARKET_REGION_LABELS = {
  texas: "Texas",
  florida: "Florida",
  california: "California",
  southeast: "Southeast",
  midwest: "Midwest",
  northeast: "Northeast",
  west_mountain: "West / Mountain",
  other: "Other",
};

const MARKET_LABEL_CANONICAL_MAP = (() => {
  const map = new Map();
  for (const markets of Object.values(MARKET_REGION_MAP)) {
    for (const label of markets) {
      map.set(normalizeMarketSlug(label), label);
    }
  }
  return map;
})();

const MARKET_LABEL_TO_SYSTEM_MARKET = {
  "miami, fl": "miami",
  "orlando, fl": "orlando",
  "tampa, fl": "tampa",
  "jacksonville, fl": "jacksonville",
  "los angeles, ca": "los_angeles",
  "new orleans, la": "new_orleans",
  "houston, tx": "houston",
  "atlanta, ga": "atlanta",
  "phoenix, az": "phoenix",
  "las vegas, nv": "las_vegas",
  "chicago, il": "chicago",
  "detroit, mi": "detroit",
  "cleveland, oh": "cleveland",
  "memphis, tn": "memphis",
  "birmingham, al": "birmingham",
  "indianapolis, in": "indianapolis",
  "charlotte, nc": "charlotte",
  "san antonio, tx": "san_antonio",
  "austin, tx": "austin",
  "st. louis, mo": "st_louis",
  "kansas city, mo": "kansas_city",
  "minneapolis, mn": "minneapolis",
  "nashville, tn": "nashville",
  "philadelphia, pa": "philadelphia",
  "dallas, tx": "dallas_fort_worth",
  "fort worth, tx": "dallas_fort_worth",
};

export function normalizeMarketRegion(value) {
  const slug = normalizeMarketSlug(value);
  if (slug === "west" || slug === "mountain" || slug === "west_mountain") {
    return "west_mountain";
  }
  if (slug in MARKET_REGION_MAP) return slug;
  return "other";
}

export function getMarketRegions() {
  return Object.keys(MARKET_REGION_MAP).map((value) => ({
    value,
    label: MARKET_REGION_LABELS[value] ?? value,
  }));
}

export function getMarketsForRegion(region) {
  return MARKET_REGION_MAP[normalizeMarketRegion(region)] ?? MARKET_REGION_MAP.other;
}

export function normalizeMarketLabel(value) {
  const slug = normalizeMarketSlug(value);
  const canonical = MARKET_LABEL_CANONICAL_MAP.get(slug);
  return canonical ?? String(value ?? "").trim();
}

export function resolveBuilderMarketToSystemSlug(value) {
  const key = String(normalizeMarketLabel(value)).trim().toLowerCase();
  return MARKET_LABEL_TO_SYSTEM_MARKET[key] ?? normalizeMarketSlug(value);
}

// ---------------------------------------------------------------------------
// V2 — Normalisation helpers
// ---------------------------------------------------------------------------

/**
 * Normalise raw tag inputs (1–3 optional strings) to [{ slug, label }] array.
 * Silently drops null/empty values.
 *
 * @param {(string|null|undefined)[]} tags
 * @returns {{ slug: string, label: string }[]}
 */
export function normalizePropertyTags(tags = []) {
  return tags
    .filter(Boolean)
    .map((t) => {
      const slug = normalizeAssetType(t); // reuse generic slug normalizer
      const label = TAG_LABEL_MAP[slug] ?? slug.replace(/_/g, " ");
      return { slug, label };
    });
}

/**
 * Build a clean filter payload from raw Discord option values.
 * Undefined / null inputs produce a missing key (not null).
 *
 * @param {object} opts
 * @returns {object}
 */
export function buildTargetingFilters({
  zip           = null,
  county        = null,
  min_equity    = null,
  max_year_built = null,
  owner_type    = null,
  phone_status  = null,
  language      = null,
  motivation_min = null,
} = {}) {
  const filters = {};
  if (zip           != null) filters.zip            = String(zip).trim();
  if (county        != null) filters.county         = String(county).trim();
  if (min_equity    != null) filters.min_equity     = Number(min_equity);
  if (max_year_built != null) filters.max_year_built = Number(max_year_built);
  if (owner_type    != null) filters.owner_type     = String(owner_type).trim();
  if (phone_status  != null) filters.phone_status   = String(phone_status).trim();
  if (language      != null) filters.language       = String(language).trim();
  if (motivation_min != null) filters.motivation_min = Number(motivation_min);
  return filters;
}

/**
 * Build a visual theme object from market / asset / strategy slugs.
 *
 * @param {string} market_slug
 * @param {string} asset_slug
 * @param {string} strategy_slug
 * @returns {{ emoji: string, color: string, mode_label: string, intensity_label: string }}
 */
export function buildTargetingTheme(market_slug, asset_slug, strategy_slug) {
  const MARKET_EMOJI = {
    miami:           "🌴",
    los_angeles:     "🌇",
    dallas_fort_worth: "🤠",
    jacksonville:    "🌊",
    new_orleans:     "⚜️",
    houston:         "🛢️",
    atlanta:         "🍑",
    phoenix:         "🌵",
    las_vegas:       "🎰",
    tampa:           "🌴",
    orlando:         "🎡",
    nashville:       "🎸",
    chicago:         "🏙️",
    detroit:         "⚙️",
    cleveland:       "🔩",
    memphis:         "🎵",
    birmingham:      "🏗️",
    indianapolis:    "🏎️",
    charlotte:       "🏦",
    san_antonio:     "🌮",
    austin:          "🎸",
    st_louis:        "🌉",
    kansas_city:     "🥩",
    minneapolis:     "❄️",
    philadelphia:    "🔔",
  };

  const MARKET_COLOR = {
    miami:           "teal_green",
    los_angeles:     "gold_purple",
    dallas_fort_worth: "amber",
    jacksonville:    "blue",
    new_orleans:     "purple",
    houston:         "amber",
    las_vegas:       "gold_purple",
  };

  const ASSET_EMOJI = {
    sfr:                    "🏠",
    multifamily:            "🏢",
    duplex:                 "🏘️",
    vacant_land:            "🌾",
    distressed_residential: "🏚️",
    commercial:             "🏬",
    hotel_motel:            "🏨",
    self_storage:           "📦",
  };

  const STRATEGY_EMOJI = {
    cash:                   "💵",
    creative:               "🧠",
    multifamily_underwrite: "🏢",
    distress_stack:         "🏚️",
    probate:                "🧾",
    tired_landlord:         "🏘️",
    pre_foreclosure:        "🏦",
    high_equity:            "🎯",
  };

  const market_emoji   = MARKET_EMOJI[market_slug]   ?? "📍";
  const asset_emoji    = ASSET_EMOJI[asset_slug]      ?? "🏠";
  const strategy_emoji = STRATEGY_EMOJI[strategy_slug] ?? "🎯";

  const color = MARKET_COLOR[market_slug] ?? "blue";

  const mode_label =
    STRATEGY_LABEL_MAP[strategy_slug] ??
    String(strategy_slug).replace(/_/g, " ");

  const asset_label  = ASSET_LABEL_MAP[asset_slug]    ?? asset_slug;
  const intensity_label = `${asset_label} / ${mode_label}`;

  return {
    emoji:           `${market_emoji} ${asset_emoji} ${strategy_emoji}`,
    market_emoji,
    asset_emoji,
    strategy_emoji,
    color,
    mode_label,
    intensity_label,
  };
}

/**
 * Build a fully normalized targeting payload from raw Discord option values.
 *
 * @param {object} opts
 * @returns {object}  Normalized targeting payload
 */
export function buildNormalizedTargeting({
  market,
  asset,
  strategy,
  tag_1 = null,
  tag_2 = null,
  tag_3 = null,
  zip = null,
  county = null,
  min_equity = null,
  max_year_built = null,
  owner_type = null,
  phone_status = null,
  language = null,
  motivation_min = null,
} = {}) {
  const market_slug   = normalizeMarketSlug(market);
  const asset_slug    = normalizeAssetType(asset);
  const strategy_slug = normalizeStrategy(strategy);

  const market_label   = MARKET_LABEL_MAP[market_slug]   ?? String(market ?? "");
  const asset_label    = ASSET_LABEL_MAP[asset_slug]      ?? String(asset ?? "");
  const strategy_label = STRATEGY_LABEL_MAP[strategy_slug] ?? String(strategy ?? "");

  const tags = normalizePropertyTags([tag_1, tag_2, tag_3]);

  const filters = buildTargetingFilters({
    zip, county, min_equity, max_year_built,
    owner_type, phone_status, language, motivation_min,
  });

  const theme = buildTargetingTheme(market_slug, asset_slug, strategy_slug);

  return {
    market_slug,
    market_label,
    asset_slug,
    asset_label,
    strategy_slug,
    strategy_label,
    tags,
    filters,
    theme,
  };
}

/**
 * Returns true if the given market slug is a known registered market.
 * @param {string} slug
 * @returns {boolean}
 */
export function isKnownMarketSlug(slug) {
  return KNOWN_MARKET_SLUGS.has(String(slug ?? "").toLowerCase());
}

/**
 * Build a deterministic campaign key from market / asset_type / strategy.
 * e.g. { market: "Los Angeles", asset_type: "sfr", strategy: "cash" }
 *       → "los_angeles_sfr_cash"
 */
export function buildCampaignKey({ market, asset_type, strategy }) {
  return [
    normalizeMarketSlug(market),
    normalizeAssetType(asset_type),
    normalizeStrategy(strategy),
  ]
    .filter(Boolean)
    .join("_");
}

// ---------------------------------------------------------------------------
// Display formatting helpers
// ---------------------------------------------------------------------------

const UPPER_ABBREVS = new Set(["sfr", "dnc", "mls", "llc"]);

function titleCaseWord(w) {
  const lower = w.toLowerCase();
  if (UPPER_ABBREVS.has(lower)) return lower.toUpperCase();
  return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
}

function titleCaseSegment(s) {
  return String(s ?? "")
    .replace(/_/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map(titleCaseWord)
    .join(" ");
}

function formatStrategy(strategy) {
  return String(strategy ?? "")
    .split("_")
    .filter(Boolean)
    .map(titleCaseWord)
    .join(" ");
}

/**
 * Resolve the human-readable source view name for a campaign.
 *
 * Priority:
 *  1. Explicit source_view_name override
 *  2. Deterministic derivation from market / asset_type / strategy
 *
 * Examples:
 *   Los Angeles + sfr + cash               → "Los Angeles / SFR / Cash"
 *   Miami + multifamily + multifamily_underwrite → "Miami / Multifamily / Multifamily Underwrite"
 */
export function resolveTargetSourceViewName({
  market,
  asset_type,
  strategy,
  source_view_name,
} = {}) {
  if (source_view_name) return source_view_name;

  const m = titleCaseSegment(market);
  const a = titleCaseSegment(asset_type);
  const s = formatStrategy(strategy);
  return `${m} / ${a} / ${s}`;
}

/**
 * Build the internal feeder dry-run GET URL for a target configuration.
 * This URL is for scheduling/cron use; Discord handlers call the endpoint
 * via POST body instead.
 */
export function buildTargetScanUrl({
  market,
  asset_type,
  strategy,
  limit = 25,
  scan_limit = 100,
  source_view_name,
} = {}) {
  const svn = resolveTargetSourceViewName({ market, asset_type, strategy, source_view_name });
  const params = new URLSearchParams({
    dry_run:          "true",
    limit:            String(limit),
    scan_limit:       String(scan_limit),
    source_view_name: svn,
  });
  return `/api/internal/outbound/feed-master-owners?${params}`;
}

// ---------------------------------------------------------------------------
// Supabase CRUD
// ---------------------------------------------------------------------------

/**
 * Create or upsert a campaign target row.
 *
 * @param {object} payload
 * @returns {Promise<object|null>}  The upserted row, or null on success without data.
 */
export async function createCampaignTarget(payload = {}) {
  const {
    campaign_name,
    market,
    asset_type,
    strategy,
    language = "auto",
    source_view_id = null,
    source_view_name = null,
    daily_cap = 50,
    status = "draft",
    created_by_discord_user_id = null,
    metadata = {},
  } = payload;

  const campaign_key = buildCampaignKey({ market, asset_type, strategy });

  const row = {
    campaign_key,
    campaign_name:              campaign_name || campaign_key,
    market:                     normalizeMarketSlug(market),
    asset_type:                 normalizeAssetType(asset_type),
    strategy:                   normalizeStrategy(strategy),
    language,
    source_view_id,
    source_view_name: source_view_name
      || resolveTargetSourceViewName({ market, asset_type, strategy }),
    daily_cap:        Number(daily_cap) || 50,
    status,
    created_by_discord_user_id,
    metadata,
    updated_at:       new Date().toISOString(),
  };

  const db = getDb();
  const { error } = await db
    .from("campaign_targets")
    .upsert(row, { onConflict: "campaign_key" });

  if (error) throw error;
  return { ...row };
}

/**
 * Load a single campaign target by key.
 *
 * @param {{ campaign_key: string }}
 * @returns {Promise<object|null>}
 */
export async function getCampaignTarget({ campaign_key }) {
  const db = getDb();
  const { data, error } = await db
    .from("campaign_targets")
    .select("*")
    .eq("campaign_key", campaign_key)
    .maybeSingle();

  if (error) throw error;
  return data;
}

/**
 * Write the latest dry-run scan summary back to the campaign row.
 *
 * @param {{ campaign_key: string, scan_summary: object }}
 * @returns {Promise<void>}
 */
export async function updateCampaignTargetScan({ campaign_key, scan_summary }) {
  const db = getDb();
  const { error } = await db
    .from("campaign_targets")
    .update({
      last_scan_summary: scan_summary,
      last_scan_at:      new Date().toISOString(),
      updated_at:        new Date().toISOString(),
    })
    .eq("campaign_key", campaign_key);

  if (error) throw error;
}

/**
 * Update the daily_cap for a campaign (used by scale operations).
 *
 * @param {{ campaign_key: string, daily_cap: number, approved_by_discord_user_id?: string }}
 * @returns {Promise<void>}
 */
export async function updateCampaignTargetScale({
  campaign_key,
  daily_cap,
  approved_by_discord_user_id = null,
}) {
  const db = getDb();
  const update = {
    daily_cap:  Number(daily_cap),
    updated_at: new Date().toISOString(),
  };
  if (approved_by_discord_user_id) {
    update.approved_by_discord_user_id = approved_by_discord_user_id;
  }

  const { error } = await db
    .from("campaign_targets")
    .update(update)
    .eq("campaign_key", campaign_key);

  if (error) throw error;
}

/**
 * Load all campaign targets grouped by market.
 *
 * @returns {Promise<{ [market: string]: object[] }>}
 */
export async function listTerritoryMap() {
  const db = getDb();
  const { data, error } = await db
    .from("campaign_targets")
    .select("*")
    .order("market", { ascending: true })
    .order("status",  { ascending: true });

  if (error) throw error;

  const rows    = data ?? [];
  const grouped = {};
  for (const row of rows) {
    const key = row.market || "unknown";
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(row);
  }
  return grouped;
}

// ─────────────────────────────────────────────────────────────────────────────
// Property Filter Normalizers — Advanced v3
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Normalize square footage range from Discord choice value to Podio filter range.
 * @param {string} value - e.g. "0_1000", "1000_1250", "non_sfr"
 * @returns {{ min?: number, max?: number, non_sfr?: boolean } | null}
 */
export function normalizeSqFtRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "0_1000":    { min: 0, max: 1000 },
    "1000_1250": { min: 1000, max: 1250 },
    "1250_1500": { min: 1250, max: 1500 },
    "1500_1750": { min: 1500, max: 1750 },
    "1750_2000": { min: 1750, max: 2000 },
    "2000_2500": { min: 2000, max: 2500 },
    "2500_3000": { min: 2500, max: 3000 },
    "3000_plus": { min: 3000 },
    "non_sfr":   { non_sfr: true },
  };
  return ranges[v] || null;
}

/**
 * Normalize units (number of units) range.
 * @param {string} value - e.g. "1", "3_4", "51_plus"
 * @returns {{ min?: number, max?: number } | null}
 */
export function normalizeUnitsRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "1":       { min: 1, max: 1 },
    "2":       { min: 2, max: 2 },
    "3_4":     { min: 3, max: 4 },
    "5_10":    { min: 5, max: 10 },
    "11_25":   { min: 11, max: 25 },
    "26_50":   { min: 26, max: 50 },
    "51_plus": { min: 51 },
  };
  return ranges[v] || null;
}

/**
 * Normalize ownership tenure / years in current ownership.
 * @param {string} value - e.g. "0_2", "3_5", "21_plus"
 * @returns {{ min?: number, max?: number } | null}
 */
export function normalizeOwnershipYearsRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "0_2":     { min: 0, max: 2 },
    "3_5":     { min: 3, max: 5 },
    "6_10":    { min: 6, max: 10 },
    "11_20":   { min: 11, max: 20 },
    "21_plus": { min: 21 },
  };
  return ranges[v] || null;
}

/**
 * Normalize estimated property value range.
 * @param {string} value - e.g. "0_100k", "500k_1m", "1m_plus"
 * @returns {{ min?: number, max?: number } | null}
 */
export function normalizeEstimatedValueRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "0_100k":     { min: 0, max: 100000 },
    "100k_200k":  { min: 100000, max: 200000 },
    "200k_350k":  { min: 200000, max: 350000 },
    "350k_500k":  { min: 350000, max: 500000 },
    "500k_1m":    { min: 500000, max: 1000000 },
    "1m_plus":    { min: 1000000 },
  };
  return ranges[v] || null;
}

/**
 * Normalize equity percentage range.
 * @param {string} value - e.g. "0_25", "50_70", "90_100"
 * @returns {{ min?: number, max?: number } | null}
 */
export function normalizeEquityPercentRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "0_25":   { min: 0, max: 25 },
    "25_50":  { min: 25, max: 50 },
    "50_70":  { min: 50, max: 70 },
    "70_90":  { min: 70, max: 90 },
    "90_100": { min: 90, max: 100 },
  };
  return ranges[v] || null;
}

/**
 * Normalize estimated repair cost range.
 * @param {string} value - e.g. "0_10k", "50k_100k", "100k_plus"
 * @returns {{ min?: number, max?: number } | null}
 */
export function normalizeRepairCostRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "0_10k":      { min: 0, max: 10000 },
    "10k_25k":    { min: 10000, max: 25000 },
    "25k_50k":    { min: 25000, max: 50000 },
    "50k_100k":   { min: 50000, max: 100000 },
    "100k_plus":  { min: 100000 },
  };
  return ranges[v] || null;
}

/**
 * Normalize building condition to Podio value.
 * @param {string} value - e.g. "Excellent", "Fair", "Poor"
 * @returns {string | null}
 */
export function normalizeBuildingCondition(value) {
  if (!value) return null;
  // Values match exactly with Podio discrete choices
  const valid = ["Excellent", "Very Good", "Good", "Average", "Fair", "Poor", "Unsound", "Unknown"];
  const v = String(value).trim();
  return valid.includes(v) ? v : null;
}

/**
 * Normalize offer vs loan comparison state.
 * @param {string} value - e.g. "free_and_clear", "offer_less_loan", "no_purchase_data"
 * @returns {string | null}
 */
export function normalizeOfferVsLoan(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const mapping = {
    "free_and_clear":      "Free and Clear",
    "offer_less_loan":     "Offer < Loan",
    "offer_greater_loan":  "Offer > Loan (Clear)",
    "offer_equal_loan":    "Offer ≈ Loan",
    "no_purchase_data":    "No Purchase Data",
  };
  return mapping[v] || null;
}

/**
 * Normalize offer vs last purchase price comparison.
 * @param {string} value - e.g. "offer_less_purchase", "offer_greater_purchase"
 * @returns {string | null}
 */
export function normalizeOfferVsLastPurchasePrice(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const mapping = {
    "no_purchase_data":        "No Purchase Data",
    "offer_less_purchase":     "Offer < Purchase",
    "offer_greater_purchase":  "Offer > Purchase (Win)",
    "offer_equal_purchase":    "Offer ≈ Purchase",
  };
  return mapping[v] || null;
}

/**
 * Normalize year-built range.
 * @param {string} value - e.g. "pre_1940", "1960_1980", "2000_plus"
 * @returns {{ min?: number, max?: number } | null}
 */
export function normalizeYearBuiltRange(value) {
  if (!value) return null;
  const v = String(value).toLowerCase().trim();
  
  const ranges = {
    "pre_1940":  { max: 1940 },
    "1940_1960": { min: 1940, max: 1960 },
    "1960_1980": { min: 1960, max: 1980 },
    "1980_2000": { min: 1980, max: 2000 },
    "2000_plus": { min: 2000 },
  };
  return ranges[v] || null;
}

/**
 * Determine if any property-specific filters are selected.
 * Property-first path is triggered if any of these filters are active.
 * 
 * @param {object} filters - targeting filters object
 * @returns {boolean}
 */
export function isPropertyFirstTargeting(filters = {}) {
  const property_keys = [
    "sq_ft_range",
    "units_range", 
    "ownership_years_range",
    "estimated_value_range",
    "equity_percent_range",
    "repair_cost_range",
    "building_condition",
    "offer_vs_loan",
    "offer_vs_last_purchase_price",
    "year_built_range",
    "min_property_score",
  ];
  
  return property_keys.some(key => filters[key] != null && filters[key] !== "");
}

/**
 * Build Podio filter payload for Properties app scan.
 * Applies property-specific filters only; does not filter by Master Owner attributes.
 * 
 * @param {object} targeting - normalized targeting config
 * @returns {object} - Podio filter object for filterAppItems()
 */
export function buildPropertyPodioFilters(targeting = {}) {
  const filters = {};
  
  // Property type / asset class mapping (if applicable from master owner targeting)
  // Properties can be targeted by app selection post-hoc
  
  // Square footage
  if (targeting.sq_ft_range) {
    const { min, max, non_sfr } = targeting.sq_ft_range;
    if (non_sfr) {
      // For non-SFR: Number of Units > 1 OR Property Type != "Single Family"
      // Store as metadata for handler logic
      filters["sq_ft_non_sfr"] = true;
    } else {
      if (min !== undefined) filters["Square Feet:min"] = min;
      if (max !== undefined) filters["Square Feet:max"] = max;
    }
  }
  
  // Number of units
  if (targeting.units_range) {
    const { min, max } = targeting.units_range;
    if (min !== undefined) filters["Number of Units:min"] = min;
    if (max !== undefined) filters["Number of Units:max"] = max;
  }
  
  // Ownership years (current ownership duration)
  if (targeting.ownership_years_range) {
    const { min, max } = targeting.ownership_years_range;
    if (min !== undefined) filters["Ownership Years:min"] = min;
    if (max !== undefined) filters["Ownership Years:max"] = max;
  }
  
  // Estimated property value
  if (targeting.estimated_value_range) {
    const { min, max } = targeting.estimated_value_range;
    if (min !== undefined) filters["Estimated Value:min"] = min;
    if (max !== undefined) filters["Estimated Value:max"] = max;
  }
  
  // Equity percentage
  if (targeting.equity_percent_range) {
    const { min, max } = targeting.equity_percent_range;
    if (min !== undefined) filters["Estimated Equity Percent:min"] = min;
    if (max !== undefined) filters["Estimated Equity Percent:max"] = max;
  }
  
  // Repair cost
  if (targeting.repair_cost_range) {
    const { min, max } = targeting.repair_cost_range;
    if (min !== undefined) filters["Estimated Repair Cost:min"] = min;
    if (max !== undefined) filters["Estimated Repair Cost:max"] = max;
  }
  
  // Building condition (dropdown)
  if (targeting.building_condition) {
    filters["Building Condition"] = targeting.building_condition;
  }
  
  // Offer vs loan comparison
  if (targeting.offer_vs_loan) {
    filters["Offer VS Loan"] = targeting.offer_vs_loan;
  }
  
  // Offer vs last purchase price
  if (targeting.offer_vs_last_purchase_price) {
    filters["Offer VS Last Purchase Price"] = targeting.offer_vs_last_purchase_price;
  }
  
  // Year built
  if (targeting.year_built_range) {
    const { min, max } = targeting.year_built_range;
    if (min !== undefined) filters["Year Build:min"] = min;
    if (max !== undefined) filters["Year Build:max"] = max;
  }
  
  // FINAL Aquisition Score (primary property quality metric)
  if (targeting.min_property_score != null) {
    filters["FINAL Aquisition Score:min"] = Number(targeting.min_property_score);
  }
  
  return filters;
}

/**
 * Paginated property-first scan across Properties app.
 * 
 * Returns matched properties with linked Master Owner, then deduplicates
 * and checks Master Owner SMS eligibility. Safety checks prevent SMS/mutations.
 * 
 * @param {object} options
 * @param {object}  options.targeting              - normalized targeting with property filters
 * @param {number}  [options.page_size]            - pagination size (default 500)
 * @param {number}  [options.max_scan_count]       - max properties to scan (default 5000)
 * @param {number}  [options.target_eligible_count] - target eligible owners (default 250)
 * @param {boolean} [options.dry_run]              - never mutate or send (default true)
 * @returns {Promise<object>} - full diagnostics object
 */
export async function scanPropertiesForTargeting(options = {}) {
  // Import Podio at call time to avoid circular deps
  const podio = await import("@/lib/providers/podio.js");
  
  const {
    targeting = {},
    page_size = 500,
    max_scan_count = 5000,
    target_eligible_count = 250,
    dry_run = true,
  } = options;
  
  // Safety: always dry-run on this endpoint
  if (!dry_run) {
    throw new Error("scanPropertiesForTargeting must use dry_run=true");
  }
  
  const PROPERTIES_APP_ID = Number(process.env.PODIO_APP_ID_PROPERTIES ?? 0) || null;
  if (!PROPERTIES_APP_ID) {
    return {
      ok: false,
      dry_run: true,
      error: "PODIO_APP_ID_PROPERTIES not configured",
      scan_path: "property_first",
      scanned_property_count: 0,
      matched_property_count: 0,
      final_eligible_count: 0,
    };
  }
  
  const filters = buildPropertyPodioFilters(targeting);
  
  let pages_loaded = 0;
  let scanned_property_count = 0;
  let matched_property_count = 0;
  let linked_master_owner_count = 0;
  let sms_eligible_owner_count = 0;
  let final_eligible_count = 0;
  let property_tag_match_count = 0;
  let deduped_owner_count = 0;
  let skipped_count = 0;
  let stopped_reason = "full_scan_completed";
  let full_scan_completed = false;
  
  const skip_reason_counts = {};
  const eligible_samples = [];
  const master_owner_map = {}; // Dedupe by master_owner_id
  
  try {
    // Paginate through properties
    for (let offset = 0; offset < max_scan_count; offset += page_size) {
      const limit_for_page = Math.min(page_size, max_scan_count - offset);
      
      try {
        const result = await podio.filterAppItems(PROPERTIES_APP_ID, filters, {
          limit: limit_for_page,
          offset,
        });
        
        pages_loaded++;
        const items = result?.items ?? [];
        
        if (!items || items.length === 0) {
          full_scan_completed = true;
          break;
        }
        
        scanned_property_count += items.length;
        
        // Process each property
        for (const property_item of items) {
          matched_property_count++;
          
          // Extract key property fields using Podio field readers
          const property_id = property_item.item_id;
          const property_address = podio.getTextValue(property_item, "Property Address") || "Unknown";
          const market = podio.getTextValue(property_item, "Market") || "";
          const property_type = podio.getTextValue(property_item, "Property Type") || "";
          const final_acquisition_score = podio.getNumberValue(property_item, "FINAL Aquisition Score") ?? 0;
          const estimated_equity_percent = podio.getNumberValue(property_item, "Estimated Equity Percent") ?? 0;
          const estimated_value = podio.getNumberValue(property_item, "Estimated Value") ?? 0;
          const smart_cash_offer = podio.getNumberValue(property_item, "Smart Cash Offer") ?? 0;
          const estimated_repair_cost = podio.getNumberValue(property_item, "Estimated Repair Cost") ?? 0;
          const building_condition = podio.getTextValue(property_item, "Building Condition") || "";
          
          // Extract property tags and motivation layers
          const property_tags_values = podio.getFieldValues(property_item, "Property Tags") || [];
          const motivation_layers_values = podio.getFieldValues(property_item, "Motivation Layers") || [];
          const property_tags = property_tags_values
            .map((v) => String(v?.value?.title || v?.value?.text || v?.value || "").trim())
            .filter(Boolean);
          const motivation_layers = motivation_layers_values
            .map((v) => String(v?.value?.title || v?.value?.text || v?.value || "").trim())
            .filter(Boolean);
          const all_property_tags = [...property_tags, ...motivation_layers];
          
          // Check property tag filter match if applicable
          let tag_match = true;
          if (targeting.tags && targeting.tags.length > 0) {
            const required_tags = targeting.tags.map((t) => t.slug || t.label || t);
            tag_match = required_tags.every((tag) =>
              all_property_tags.some((pt) =>
                String(pt).toLowerCase().includes(String(tag).toLowerCase())
              )
            );
          }
          
          if (!tag_match) {
            skipped_count++;
            skip_reason_counts["tag_mismatch"] = (skip_reason_counts["tag_mismatch"] ?? 0) + 1;
            continue;
          }
          
          property_tag_match_count++;
          
          // Check min_property_score filter
          if (targeting.min_property_score != null && final_acquisition_score < targeting.min_property_score) {
            skipped_count++;
            skip_reason_counts["low_property_score"] = (skip_reason_counts["low_property_score"] ?? 0) + 1;
            continue;
          }
          
          // Extract linked Master Owner reference
          const linked_owner_values = podio.getFieldValues(property_item, "Linked Master Owner") || [];
          if (!linked_owner_values || linked_owner_values.length === 0) {
            skipped_count++;
            skip_reason_counts["no_linked_owner"] = (skip_reason_counts["no_linked_owner"] ?? 0) + 1;
            continue;
          }
          
          const master_owner_ref = linked_owner_values[0]?.value;
          if (!master_owner_ref || !master_owner_ref.item_id) {
            skipped_count++;
            skip_reason_counts["invalid_owner_ref"] = (skip_reason_counts["invalid_owner_ref"] ?? 0) + 1;
            continue;
          }
          
          const master_owner_id = master_owner_ref.item_id;
          linked_master_owner_count++;
          
          // Hydrate Master Owner for SMS eligibility checks
          let master_owner_sms_eligible = false;
          let owner_name = "Unknown Owner";
          
          try {
            const master_owner_item = await podio.getItem(master_owner_id);
            
            // Check SMS Eligible field
            const sms_eligible_value = podio.getTextValue(master_owner_item, "SMS Eligible?") || "";
            master_owner_sms_eligible = String(sms_eligible_value).toLowerCase() === "yes";
            
            // Check DNC status
            const contact_status = podio.getTextValue(master_owner_item, "Contact Status") || "";
            const contact_status_2 = podio.getTextValue(master_owner_item, "Contact Status 2") || "";
            const is_dnc = String(contact_status).toLowerCase().includes("dnc") ||
                           String(contact_status_2).toLowerCase().includes("dnc");
            
            if (is_dnc) {
              master_owner_sms_eligible = false;
            }
            
            // Check best phone exists
            const best_phone = podio.getTextValue(master_owner_item, "Best Phone #1") || "";
            if (!best_phone) {
              master_owner_sms_eligible = false;
            }
            
            // Extract owner name
            owner_name = podio.getTextValue(master_owner_item, "Owner Name") || "Unknown Owner";
          } catch (e) {
            // Non-fatal: owner hydration failed, mark not eligible
            master_owner_sms_eligible = false;
          }
          
          if (!master_owner_sms_eligible) {
            skipped_count++;
            skip_reason_counts["owner_not_sms_eligible"] = (skip_reason_counts["owner_not_sms_eligible"] ?? 0) + 1;
            continue;
          }
          
          sms_eligible_owner_count++;
          final_eligible_count++;
          
          // Dedupe: keep strongest property per Master Owner
          if (!master_owner_map[master_owner_id]) {
            master_owner_map[master_owner_id] = {
              owner_name,
              master_owner_id,
              property_id,
              property_address,
              market,
              property_type,
              final_acquisition_score,
              estimated_equity_percent,
              estimated_value,
              smart_cash_offer,
              estimated_repair_cost,
              building_condition,
              tags: all_property_tags,
            };
            eligible_samples.push(master_owner_map[master_owner_id]);
          } else {
            // Already have this owner; update if this property is stronger
            const existing = master_owner_map[master_owner_id];
            if (
              final_acquisition_score > existing.final_acquisition_score ||
              (final_acquisition_score === existing.final_acquisition_score && estimated_equity_percent > existing.estimated_equity_percent)
            ) {
              // Replace with stronger property
              const idx = eligible_samples.findIndex((s) => s.master_owner_id === master_owner_id);
              master_owner_map[master_owner_id] = {
                owner_name,
                master_owner_id,
                property_id,
                property_address,
                market,
                property_type,
                final_acquisition_score,
                estimated_equity_percent,
                estimated_value,
                smart_cash_offer,
                estimated_repair_cost,
                building_condition,
                tags: all_property_tags,
              };
              if (idx >= 0) {
                eligible_samples[idx] = master_owner_map[master_owner_id];
              }
            }
          }
          
          // Stop if we've reached target eligible count
          if (final_eligible_count >= target_eligible_count) {
            stopped_reason = "target_eligible_count";
            break;
          }
        }
        
        // Check if we've reached max scan
        if (scanned_property_count >= max_scan_count) {
          stopped_reason = "max_scan_count";
          break;
        }
        
        // Check if scan completed
        if (items.length < limit_for_page) {
          full_scan_completed = true;
          break;
        }
      } catch (page_error) {
        // Log but continue
        console.warn(`Property scan page error at offset ${offset}:`, page_error?.message);
        break;
      }
      
      // Stop if target reached
      if (final_eligible_count >= target_eligible_count) {
        stopped_reason = "target_eligible_count";
        break;
      }
    }
  } catch (scan_error) {
    // Top-level scan error
    return {
      ok: false,
      dry_run: true,
      error: String(scan_error?.message || "Property scan failed"),
      scan_path: "property_first",
      scanned_property_count,
      matched_property_count,
      final_eligible_count,
      pages_loaded,
    };
  }
  
  deduped_owner_count = Object.keys(master_owner_map).length;
  const api_request_estimate = pages_loaded + (linked_master_owner_count || 0);
  
  // Return full diagnostics
  return {
    ok: true,
    dry_run: true,
    scan_path: "property_first",
    source_app: "properties",
    pages_loaded,
    scanned_property_count,
    matched_property_count,
    linked_master_owner_count,
    sms_eligible_owner_count,
    final_eligible_count,
    property_tag_match_count,
    deduped_owner_count,
    skipped_count,
    skip_reason_counts,
    api_request_estimate,
    stopped_reason,
    full_scan_completed,
    eligible_samples: eligible_samples.slice(0, 3), // Top 3 samples
  };
}


