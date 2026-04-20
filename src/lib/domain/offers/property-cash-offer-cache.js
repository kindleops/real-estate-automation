/**
 * property-cash-offer-cache.js
 *
 * Supabase cache for single-family cash offer snapshots.
 *
 * Key design rules enforced here:
 *
 *  1. ONE active offer per property_id at a time.  Upserting supersedes the
 *     previous active row before inserting / bumping the version counter.
 *
 *  2. Only single-family cash deals belong in this table.  Multifamily,
 *     apartments, 5+ units, and creative-finance deals must be routed to the
 *     Podio Underwriting app via transfer-to-underwriting.js.  This module
 *     does NOT enforce that rule — callers must check shouldRouteToUnderwriting
 *     from deal-routing.js before calling upsertActivePropertyCashOffer.
 *
 *  3. cash_offer is a single number — there is no min/max range logic here.
 *
 *  4. Exports __setOfferCacheDeps / __resetOfferCacheDeps for unit testing
 *     (same DI pattern used across the codebase).
 */

import { supabase } from "@/lib/supabase/client.js";

const TABLE = "property_cash_offer_snapshots";

// ---------------------------------------------------------------------------
// Test dependency injection
// ---------------------------------------------------------------------------

let _deps = { supabase_override: null };

/** Override the Supabase client for unit tests. */
export function __setOfferCacheDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

/** Reset injected dependencies to production defaults. */
export function __resetOfferCacheDeps() {
  _deps = { supabase_override: null };
}

function getDb() {
  return _deps.supabase_override ?? supabase;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toText(value) {
  const s = String(value ?? "").trim();
  return s || null;
}

function toBigint(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * Sanitise and normalise an inbound upsert payload.
 * Strips fields that must not be client-controlled (id, created_at, version).
 */
function buildRow(payload = {}) {
  return {
    property_id:                 toText(payload.property_id),
    podio_property_item_id:      toBigint(payload.podio_property_item_id),
    master_owner_id:             toBigint(payload.master_owner_id),
    owner_id:                    toText(payload.owner_id),

    property_address:            toText(payload.property_address),
    property_city:               toText(payload.property_city),
    property_state:              toText(payload.property_state),
    property_zip:                toText(payload.property_zip),
    market:                      toText(payload.market),
    property_type:               toText(payload.property_type),
    property_class:              toText(payload.property_class),

    // Single-family cash offer — no min/max range
    cash_offer:                  toNumber(payload.cash_offer),
    repair_estimate:             toNumber(payload.repair_estimate),
    estimated_value:             toNumber(payload.estimated_value),
    calculated_value:            toNumber(payload.calculated_value),
    estimated_equity:            toNumber(payload.estimated_equity),
    estimated_mortgage_balance:  toNumber(payload.estimated_mortgage_balance),
    estimated_mortgage_payment:  toNumber(payload.estimated_mortgage_payment),

    offer_source:                toText(payload.offer_source)    ?? "podio",
    valuation_source:            toText(payload.valuation_source),
    confidence_score:            toNumber(payload.confidence_score),
    motivation_score:            toNumber(payload.motivation_score),

    status:                      "active",

    podio_offer_item_id:         toBigint(payload.podio_offer_item_id),
    podio_synced_at:             payload.podio_synced_at ? toText(payload.podio_synced_at) : null,

    metadata:                    (payload.metadata && typeof payload.metadata === "object")
                                   ? payload.metadata
                                   : {},
    generated_at:                payload.generated_at ? toText(payload.generated_at) : nowIso(),
    expires_at:                  payload.expires_at     ? toText(payload.expires_at)    : null,
  };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Return the single active cash offer snapshot for a property.
 *
 * Resolves by property_id first; if that yields nothing and
 * podio_property_item_id is provided, falls back to that lookup.
 *
 * @param {{ property_id?: string|null, podio_property_item_id?: number|null }}
 * @returns {Promise<{ ok: boolean, snapshot: object|null, reason?: string }>}
 */
export async function getActivePropertyCashOffer({
  property_id       = null,
  podio_property_item_id = null,
} = {}) {
  const db = getDb();

  if (!property_id && !podio_property_item_id) {
    return { ok: false, snapshot: null, reason: "missing_lookup_key" };
  }

  try {
    // Primary lookup: property_id
    if (property_id) {
      const { data, error } = await db
        .from(TABLE)
        .select("*")
        .eq("property_id", String(property_id))
        .eq("status", "active")
        .order("version", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (error) throw error;
      if (data) return { ok: true, snapshot: data };
    }

    // Fallback: podio_property_item_id
    if (podio_property_item_id) {
      const pid = toBigint(podio_property_item_id);
      if (!pid) return { ok: false, snapshot: null, reason: "invalid_podio_property_item_id" };

      const { data, error } = await db
        .from(TABLE)
        .select("*")
        .eq("podio_property_item_id", pid)
        .eq("status", "active")
        .order("version", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (error) throw error;
      if (data) return { ok: true, snapshot: data };
    }

    return { ok: true, snapshot: null, reason: "not_found" };
  } catch (err) {
    return {
      ok: false,
      snapshot: null,
      reason: "db_error",
      error: String(err?.message ?? err),
    };
  }
}

/**
 * Upsert (insert or supersede-then-insert) the active cash offer for a
 * property.
 *
 * Steps:
 *   1. Supersede any existing active row for this property_id (sets
 *      status → 'superseded', updated_at → now()).
 *   2. Determine the new version = max(previous_version) + 1 (or 1 if none).
 *   3. Insert the new active row.
 *
 * @param {object} payload  — see buildRow() for accepted fields.
 * @returns {Promise<{ ok: boolean, snapshot: object|null, created: boolean, reason?: string }>}
 */
export async function upsertActivePropertyCashOffer(payload = {}) {
  const db = getDb();
  const row = buildRow(payload);

  if (!row.property_id) {
    return { ok: false, snapshot: null, created: false, reason: "missing_property_id" };
  }

  try {
    // 1. Find current active row (to derive version and supersede it).
    const { data: existing, error: find_error } = await db
      .from(TABLE)
      .select("id, version")
      .eq("property_id", row.property_id)
      .eq("status", "active")
      .order("version", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (find_error) throw find_error;

    const next_version = existing ? (Number(existing.version) || 1) + 1 : 1;

    // 2. Supersede existing active row if present.
    if (existing?.id) {
      const { error: supersede_error } = await db
        .from(TABLE)
        .update({ status: "superseded", updated_at: nowIso() })
        .eq("id", existing.id);

      if (supersede_error) throw supersede_error;
    }

    // 3. Insert new active row.
    const new_row = { ...row, version: next_version };
    const { data: inserted, error: insert_error } = await db
      .from(TABLE)
      .insert(new_row)
      .select()
      .maybeSingle();

    if (insert_error) throw insert_error;

    return {
      ok: true,
      snapshot: inserted ?? new_row,
      created: true,
      superseded_previous: Boolean(existing?.id),
    };
  } catch (err) {
    return {
      ok: false,
      snapshot: null,
      created: false,
      reason: "db_error",
      error: String(err?.message ?? err),
    };
  }
}

/**
 * Mark the current active cash offer for a property as superseded.
 * Call this when a deal is closed, cancelled, or a manual review overrides
 * the cached value.
 *
 * @param {{ property_id: string, reason?: string }}
 * @returns {Promise<{ ok: boolean, superseded: boolean, reason?: string }>}
 */
export async function supersedeActivePropertyCashOffer({
  property_id = null,
  reason      = "manual_supersede",
} = {}) {
  const db = getDb();

  if (!property_id) {
    return { ok: false, superseded: false, reason: "missing_property_id" };
  }

  try {
    const { data, error } = await db
      .from(TABLE)
      .update({
        status:     "superseded",
        metadata:   db.rpc
          ? undefined          // use raw update if rpc not available
          : undefined,
        updated_at: nowIso(),
      })
      .eq("property_id", String(property_id))
      .eq("status", "active")
      .select("id");

    if (error) throw error;

    const count = Array.isArray(data) ? data.length : (data ? 1 : 0);
    return { ok: true, superseded: count > 0, superseded_count: count, reason };
  } catch (err) {
    return {
      ok: false,
      superseded: false,
      reason: "db_error",
      error: String(err?.message ?? err),
    };
  }
}

/**
 * Build a queue-plan diagnostic snapshot object from a stored snapshot row.
 * Safe to embed in a plan object — contains only offer/valuation fields.
 *
 * Caller must resolve the snapshot first via getActivePropertyCashOffer().
 * Do NOT include this in Stage-1 / first-touch message_text.
 *
 * @param {object|null} snapshot
 * @returns {object|null}
 */
export function buildPlanCashOfferSnapshot(snapshot) {
  if (!snapshot) return null;

  return {
    cash_offer:                 toNumber(snapshot.cash_offer),
    repair_estimate:            toNumber(snapshot.repair_estimate),
    estimated_value:            toNumber(snapshot.estimated_value),
    calculated_value:           toNumber(snapshot.calculated_value),
    estimated_equity:           toNumber(snapshot.estimated_equity),
    estimated_mortgage_balance: toNumber(snapshot.estimated_mortgage_balance),
    estimated_mortgage_payment: toNumber(snapshot.estimated_mortgage_payment),
    generated_at:               snapshot.generated_at ?? null,
    version:                    snapshot.version ?? 1,
  };
}
