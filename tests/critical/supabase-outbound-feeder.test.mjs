import assert from "node:assert/strict";
import { test } from "node:test";
import { runSupabaseOutboundFeeder } from "../../src/lib/domain/outbound/run-supabase-outbound-feeder.js";

test("Supabase outbound feeder excludes DNC, pending queue, and recent contacts", async () => {
  const candidateRows = [
    // 1. Eligible candidate
    {
      master_owner_id: "mo_1",
      property_id: "prop_1",
      phone_id: "ph_1",
      best_phone_id: "ph_1",
      canonical_e164: "+15550000001",
      contact_window: "09:00 AM - 08:00 PM",
      market: "test_market",
      state: "tx",
      seller_first_name: "Test",
    },
    // 2. Opt-out (DNC)
    {
      master_owner_id: "mo_2",
      property_id: "prop_2",
      phone_id: "ph_2",
      best_phone_id: "ph_2",
      canonical_e164: "+15550000002",
      active_opt_out: true,
      contact_window: "09:00 AM - 08:00 PM",
      market: "test_market",
      state: "tx",
    },
    // 3. Pending prior touch
    {
      master_owner_id: "mo_3",
      property_id: "prop_3",
      phone_id: "ph_3",
      best_phone_id: "ph_3",
      canonical_e164: "+15550000003",
      pending_prior_touch: true,
      contact_window: "09:00 AM - 08:00 PM",
      market: "test_market",
      state: "tx",
    },
    // 4. Missing phone
    {
      master_owner_id: "mo_4",
      property_id: "prop_4",
      canonical_e164: null,
      contact_window: "09:00 AM - 08:00 PM",
      market: "test_market",
      state: "tx",
    }
  ];

  const mockDeps = {
    supabase: {
      from: (table) => {
        if (table === "v_sms_campaign_queue_candidates") {
          return {
            select: () => {
              return {
                limit: () => ({ data: candidateRows, error: null }),
                range: () => ({ data: candidateRows, error: null })
              };
            }
          };
        }
        if (table === "send_queue") {
          return {
            select: () => ({
              eq: () => ({
                eq: () => ({
                  in: () => ({
                    eq: () => ({
                      limit: () => ({ data: [], error: null })
                    })
                  })
                })
              })
            })
          };
        }
        if (table === "textgrid_numbers") {
          return {
            select: () => ({
              limit: () => ({ data: [
                { id: "tn_1", phone_number: "+15559990000", market: "test_market", status: "active" }
              ], error: null })
            })
          };
        }
        if (table === "sms_templates") {
          return {
            select: () => ({
              eq: () => ({
                in: () => ({
                  limit: () => ({ data: [
                    { id: "tmpl_1", language: "English", use_case: "ownership_check", stage_code: "S1" }
                  ], error: null })
                })
              })
            })
          };
        }
        return { select: () => ({ limit: () => ({ data: [], error: null }) }) };
      }
    },
    hasDuplicateQueueItem: async () => false,
    chooseTextgridNumber: async () => ({
      ok: true,
      reason_code: "OK",
      routing_allowed: true,
      selected: { id: "tn_1", phone_number: "+15559990000", market: "test_market" }
    }),
    renderOutboundTemplate: async () => ({
      ok: true,
      selected_template: { id: "tmpl_1" },
      queue_payload: {}
    }),
    insertSupabaseSendQueueRow: async () => ({ ok: true })
  };

  const result = await runSupabaseOutboundFeeder({
    dry_run: true,
    limit: 10,
    scan_limit: 10,
    market: "test_market"
  }, mockDeps);

  assert.equal(result.ok, true);
  assert.equal(result.scanned_count, 4);
  assert.equal(result.eligible_count, 1);
  assert.equal(result.queued_count, 1);
  assert.equal(result.skipped_count, 3);
  
  assert.ok(result.skip_reasons["TRUE_OPT_OUT"] > 0, "Should skip opted out");
  assert.ok(result.skip_reasons["PENDING_PRIOR_TOUCH"] > 0, "Should skip pending prior touch");
  assert.ok(result.skip_reasons["NO_VALID_PHONE"] > 0 || result.skip_reasons["NO_BEST_PHONE"] > 0, "Should skip missing phone");
});
