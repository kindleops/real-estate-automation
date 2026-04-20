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
