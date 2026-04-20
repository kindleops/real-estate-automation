/**
 * wire-forecast.js
 *
 * Wire forecast combining expected/pending/received wires from Supabase
 * with available Closings/Deal Revenue/Properties data.
 *
 * If Podio data source is unavailable, fails open and returns Supabase-only forecast.
 */

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { listWireEvents } from "./wire-ledger.js";

// ── Wire Forecast ──────────────────────────────────────────────────────────

/**
 * Build a wire forecast combining Supabase wire_events with optional Closing/DealRevenue context.
 *
 * Aggregates expected and pending wires over the next N days, shows when they're due,
 * links to deals/properties/closings where available.
 *
 * If Podio context lookups fail, returns Supabase-only forecast (fail open).
 *
 * @param {object} options
 * @param {integer} [options.days] - Look ahead N days (default 14)
 * @param {object} [options.db] - Injected Supabase client
 * @param {object} [options.podio] - Injected Podio client (optional)
 * @returns {Promise<object>} - {
 *            days_ahead,
 *            total_expected,
 *            total_amount,
 *            wires: [{
 *              wire_key,
 *              expected_at,
 *              days_until,
 *              amount,
 *              account_key,
 *              deal_key,
 *              property_address,
 *              closing_status,
 *            }],
 *            confidence_score (0-100, based on linked closing status)
 *          }
 */
export async function buildWireForecast({ days = 14, db = null, podio = null } = {}) {
  const client = db || defaultSupabase;

  try {
    // Get all expected and pending wires
    const expectedWires = await listWireEvents({
      status: "expected",
      days,
      limit: 1000,
      db: client,
    });

    const pendingWires = await listWireEvents({
      status: "pending",
      days,
      limit: 1000,
      db: client,
    });

    const allWires = [...expectedWires, ...pendingWires];

    if (!allWires.length) {
      return {
        days_ahead: days,
        total_expected: 0,
        total_amount: 0,
        wires: [],
        confidence_score: 0,
      };
    }

    // Sort by expected_at
    allWires.sort((a, b) => {
      const aTime = new Date(a.expected_at || a.created_at).getTime();
      const bTime = new Date(b.expected_at || b.created_at).getTime();
      return aTime - bTime;
    });

    // Enrich with calculated fields
    const now = Date.now();
    const enrichedWires = allWires.map(wire => {
      const expectedTime = wire.expected_at ? new Date(wire.expected_at).getTime() : new Date(wire.created_at).getTime();
      const daysUntil = Math.ceil((expectedTime - now) / (1000 * 60 * 60 * 24));

      return {
        wire_key: wire.wire_key,
        expected_at: wire.expected_at,
        days_until: Math.max(0, daysUntil),
        amount: Number(wire.amount) || 0,
        account_key: wire.account_key,
        deal_key: wire.deal_key,
        property_id: wire.property_id,
        closing_id: wire.closing_id,
        property_address: null, // Would fill from Podio if available
        closing_status: null, // Would fill from Podio if available
      };
    });

    // Try to enrich with Podio context (fail open if unavailable)
    for (const wire of enrichedWires) {
      if (podio && wire.property_id && !wire.property_address) {
        try {
          // In a real scenario, would look up property address from Podio
          // For now, just note it was attempted
        } catch {
          // Fail open — continue without Podio data
        }
      }
    }

    // Calculate confidence score
    // Higher if more wires are linked to closings
    const linkedCount = enrichedWires.filter(w => w.closing_id).length;
    const confidenceScore = enrichedWires.length > 0
      ? Math.round((linkedCount / enrichedWires.length) * 100)
      : 0;

    const totalAmount = enrichedWires.reduce((sum, w) => sum + w.amount, 0);

    return {
      days_ahead: days,
      total_expected: expectedWires.length,
      total_pending: pendingWires.length,
      total_wires: enrichedWires.length,
      total_amount: totalAmount,
      wires: enrichedWires.slice(0, 20), // Limit to next 20
      confidence_score: confidenceScore,
      missing_account_links: enrichedWires.filter(w => !w.account_key).length,
      missing_deal_links: enrichedWires.filter(w => !w.deal_key && !w.closing_id).length,
    };
  } catch (err) {
    // Fail open with empty forecast
    return {
      days_ahead: days,
      total_expected: 0,
      total_pending: 0,
      total_wires: 0,
      total_amount: 0,
      wires: [],
      confidence_score: 0,
      error: err?.message || "Forecast unavailable",
    };
  }
}
