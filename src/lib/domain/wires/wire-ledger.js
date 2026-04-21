/**
 * wire-ledger.js
 *
 * Wire events ledger for the /wires Discord command center.
 * Tracks expected, received, and cleared wire events without initiating any
 * actual bank transfers.
 *
 * Functions support dependency injection of Supabase client for testing.
 */

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import crypto from "node:crypto";

// ── Wire Key Generation ────────────────────────────────────────────────────

/**
 * Build a unique wire key from input.
 * wire keys are stored as text and used to link Discord interactions to records.
 *
 * Format: wire_{hash}_{timestamp}
 *
 * @param {object} input - { amount, account_key, expected_at, deal_key?, property_id? }
 * @returns {string}
 */
export function buildWireKey(input = {}) {
  const { amount, account_key, expected_at, deal_key, property_id } = input;
  const combined = `${amount}:${account_key}:${expected_at || ""}:${deal_key || ""}:${property_id || ""}`;
  const hash = crypto.createHash("sha256").update(combined).digest("hex").slice(0, 8);
  const ts = Date.now();
  return `wire_${hash}_${ts}`;
}

// ── Account Formatting ─────────────────────────────────────────────────────

/**
 * Format a bank account object as masked string.
 * Returns "Institution ••••last4" format to avoid exposing full account numbers.
 *
 * @param {object} account
 * @returns {string}
 */
export function formatMaskedAccount(account = {}) {
  if (!account?.account_last4) return "—";
  const inst = (account?.institution_name || "Bank").slice(0, 20);
  return `${inst} ••••${account.account_last4}`;
}

// ── Wire Events Lists ──────────────────────────────────────────────────────

/**
 * List wire events with optional filters.
 *
 * @param {object}   options
 * @param {string}   [options.status] - Filter by status (expected, pending, received, cleared, etc.)
 * @param {integer}  [options.limit] - Max results (default 100)
 * @param {string}   [options.account_key] - Filter by account
 * @param {integer}  [options.days] - Only events in last N days (default all)
 * @param {object}   [options.db] - Injected Supabase client for testing
 * @returns {Promise<object[]>}
 */
export async function listWireEvents({
  status = null,
  limit = 100,
  account_key = null,
  days = null,
  db = null,
} = {}) {
  const client = db || defaultSupabase;
  let query = client.from("wire_events").select("*");

  if (status) {
    query = query.eq("status", status);
  }
  if (account_key) {
    query = query.eq("account_key", account_key);
  }
  if (days) {
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    query = query.gte("created_at", since);
  }

  query = query.order("created_at", { ascending: false }).limit(limit);

  const { data, error } = await query;
  if (error) {
    const e = new Error(`Wire events query failed: ${error.message}`);
    e.code = error.code;
    throw e;
  }
  return data || [];
}

// ── Wire Summary ───────────────────────────────────────────────────────────

/**
 * Get a summary of wire events (counts by status).
 *
 * @param {object} options
 * @param {integer} [options.days] - Scope to last N days (default all)
 * @param {object} [options.db] - Injected Supabase client
 * @returns {Promise<object>} - { expected, pending, received, cleared, cancelled, disputed, total_amount}
 */
export async function getWireSummary({ days = null, db = null } = {}) {
  const client = db || defaultSupabase;
  let query = client.from("wire_events").select("status, amount");

  if (days) {
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    query = query.gte("created_at", since);
  }

  const { data, error } = await query;
  if (error) {
    const e = new Error(`Wire summary query failed: ${error.message}`);
    e.code = error.code;
    throw e;
  }

  const counts = {
    expected: 0,
    pending: 0,
    received: 0,
    cleared: 0,
    cancelled: 0,
    disputed: 0,
  };
  let total_amount = 0;

  (data || []).forEach(row => {
    if (counts.hasOwnProperty(row.status)) {
      counts[row.status]++;
    }
    total_amount += Number(row.amount) || 0;
  });

  return {
    ...counts,
    total: (data || []).length,
    total_amount,
  };
}

// ── Wire Creation ──────────────────────────────────────────────────────────

/**
 * Create an expected wire event.
 *
 * @param {object} options
 * @param {number} options.amount - Wire amount
 * @param {string} options.account_key - Account identifier
 * @param {string} [options.deal_key] - Deal identifier
 * @param {bigint} [options.property_id] - Podio property item ID
 * @param {bigint} [options.closing_id] - Podio closing item ID
 * @param {bigint} [options.deal_revenue_id] - Podio deal revenue item ID
 * @param {bigint} [options.title_company_id] - Podio title company ID
 * @param {string} [options.expected_at] - ISO timestamp
 * @param {object} [options.metadata] - Extra metadata
 * @param {string} [options.created_by_discord_user_id] - Discord user ID
 * @param {object} [options.db] - Injected Supabase client
 * @returns {Promise<object>} - Created wire event
 */
export async function createExpectedWire({
  amount,
  account_key,
  deal_key = null,
  property_id = null,
  closing_id = null,
  deal_revenue_id = null,
  title_company_id = null,
  expected_at = null,
  metadata = {},
  created_by_discord_user_id = null,
  db = null,
} = {}) {
  const client = db || defaultSupabase;

  const wire_key = buildWireKey({
    amount,
    account_key,
    expected_at,
    deal_key,
    property_id,
  });

  const { data, error } = await client.from("wire_events").insert({
    wire_key,
    amount: Number(amount),
    account_key,
    deal_key,
    property_id: property_id ? Number(property_id) : null,
    closing_id: closing_id ? Number(closing_id) : null,
    deal_revenue_id: deal_revenue_id ? Number(deal_revenue_id) : null,
    title_company_id: title_company_id ? Number(title_company_id) : null,
    status: "expected",
    expected_at: expected_at ? new Date(expected_at).toISOString() : null,
    metadata,
    created_by_discord_user_id,
  }).select().single();

  if (error) throw new Error(`Failed to create wire event: ${error.message}`);
  return data;
}

// ── Wire Status Updates ────────────────────────────────────────────────────

/**
 * Mark a wire as received.
 *
 * @param {object} options
 * @param {string} options.wire_key - Wire identifier
 * @param {string} [options.received_at] - ISO timestamp (default now)
 * @param {string} [options.status_note] - Notes
 * @param {string} [options.discord_user_id] - User who marked it
 * @param {object} [options.db] - Injected Supabase client
 * @returns {Promise<object>}
 */
export async function markWireReceived({
  wire_key,
  received_at = null,
  status_note = null,
  discord_user_id = null,
  db = null,
} = {}) {
  const client = db || defaultSupabase;

  const { data, error } = await client
    .from("wire_events")
    .update({
      status: "received",
      received_at: received_at ? new Date(received_at).toISOString() : new Date().toISOString(),
      status_note,
      updated_at: new Date().toISOString(),
    })
    .eq("wire_key", wire_key)
    .select()
    .single();

  if (error) throw new Error(`Failed to mark wire received: ${error.message}`);
  return data;
}

/**
 * Mark a wire as cleared.
 *
 * @param {object} options
 * @param {string} options.wire_key - Wire identifier
 * @param {string} [options.cleared_at] - ISO timestamp (default now)
 * @param {string} [options.status_note] - Notes
 * @param {string} [options.discord_user_id] - User who marked it
 * @param {object} [options.db] - Injected Supabase client
 * @returns {Promise<object>}
 */
export async function markWireCleared({
  wire_key,
  cleared_at = null,
  status_note = null,
  discord_user_id = null,
  db = null,
} = {}) {
  const client = db || defaultSupabase;

  const { data, error } = await client
    .from("wire_events")
    .update({
      status: "cleared",
      cleared_at: cleared_at ? new Date(cleared_at).toISOString() : new Date().toISOString(),
      status_note,
      updated_at: new Date().toISOString(),
    })
    .eq("wire_key", wire_key)
    .select()
    .single();

  if (error) throw new Error(`Failed to mark wire cleared: ${error.message}`);
  return data;
}

/**
 * Cancel a wire event.
 *
 * @param {object} options
 * @param {string} options.wire_key - Wire identifier
 * @param {string} [options.status_note] - Cancellation reason
 * @param {string} [options.discord_user_id] - User who cancelled it
 * @param {object} [options.db] - Injected Supabase client
 * @returns {Promise<object>}
 */
export async function cancelWire({
  wire_key,
  status_note = null,
  discord_user_id = null,
  db = null,
} = {}) {
  const client = db || defaultSupabase;

  const { data, error } = await client
    .from("wire_events")
    .update({
      status: "cancelled",
      status_note,
      updated_at: new Date().toISOString(),
    })
    .eq("wire_key", wire_key)
    .select()
    .single();

  if (error) throw new Error(`Failed to cancel wire: ${error.message}`);
  return data;
}
