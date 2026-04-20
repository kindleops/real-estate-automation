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
