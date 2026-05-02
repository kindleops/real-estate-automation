import assert from "node:assert/strict";
import { test } from "node:test";
import { runSupabaseOutboundFeeder } from "../../src/lib/domain/outbound/run-supabase-outbound-feeder.js";

function createMockSupabase(overrides = {}) {
  const query = (data = [], count = 0) => {
    let filteredData = [...data];
    const builder = {
      select: () => builder,
      eq: (col, val) => {
        if (col && val !== undefined) {
          filteredData = filteredData.filter(item => item[col] === val);
        }
        return builder;
      },
      in: (col, vals) => {
        if (col && Array.isArray(vals)) {
          filteredData = filteredData.filter(item => vals.includes(item[col]));
        }
        return builder;
      },
      ilike: () => builder,
      range: () => builder,
      limit: () => builder,
      order: () => builder,
      then: (resolve) => resolve({ data: filteredData, error: null, count: filteredData.length })
    };
    return builder;
  };

  return {
    from: (table) => {
      if (table === "v_sms_campaign_queue_candidates") {
        return query(overrides.candidateRows || []);
      }
      if (table === "send_queue") {
        return query(overrides.sendQueueRows || overrides.duplicateRows || [], (overrides.sendQueueRows || overrides.duplicateRows || []).length);
      }
      if (table === "textgrid_numbers") {
        return query([{ id: "tn_1", phone_number: "+15559990000", market: "test_market", status: "active" }]);
      }
      if (table === "sms_templates") {
        return query([
          { id: "tmpl_1", template_id: "tmpl_1", language: "English", use_case: "ownership_check", stage_code: "S1", is_active: true, template_body: "Hi there!" },
          { id: "tmpl_2", template_id: "tmpl_2", language: "English", use_case: "consider_selling", stage_code: "S2", is_active: true, template_body: "Still interested?" },
          { id: "tmpl_3", template_id: "tmpl_3", language: "English", use_case: "seller_asking_price", stage_code: "S3", is_active: true, template_body: "What is your price?" }
        ]);
      }
      return query([]);
    }
  };
}

const baseCandidate = {
  phone_id: "ph_1",
  best_phone_id: "ph_1",
  canonical_e164: "+15550001234",
  market: "test_market",
  state: "tx",
  seller_first_name: "Test",
  property_address: "123 Main St",
  property_id: "prop_1",
  master_owner_id: "mo_1"
};

test("Supabase outbound feeder progression and suppression logic", async () => {
  const candidateRows = [
    { ...baseCandidate, master_owner_id: "mo_1", property_id: "prop_1", last_touch_number: 0 },
    { ...baseCandidate, master_owner_id: "mo_2", property_id: "prop_2", last_touch_number: 1, use_case_template: "ownership_check" },
    { ...baseCandidate, master_owner_id: "mo_3", property_id: "prop_3", last_touch_number: 2, use_case_template: "consider_selling" },
    { ...baseCandidate, master_owner_id: "mo_4", property_id: "prop_4", active_opt_out: true },
    { ...baseCandidate, master_owner_id: "mo_5", property_id: "prop_5", next_eligible_at: "2099-01-01T00:00:00Z" }
  ];

  const mockDeps = {
    supabase: createMockSupabase({ candidateRows }),
    chooseTextgridNumber: async () => ({ ok: true, selected: { id: "tn_1" } })
  };

  const result = await runSupabaseOutboundFeeder({
    dry_run: true,
    debug: true,
    limit: 10,
    scan_limit: 10,
    within_contact_window_now: false
  }, mockDeps);

  assert.equal(result.ok, true);
  assert.equal(result.queued_count, 3);
});

test("Supabase outbound feeder authoritative progression from send_queue", async () => {
  // Candidate has empty history in view
  const candidateRows = [
    { ...baseCandidate, master_owner_id: "mo_history", property_id: "prop_history", last_touch_number: 0 }
  ];

  // But send_queue has Touch 1
  const sendQueueRows = [
    {
      master_owner_id: "mo_history",
      property_id: "prop_history",
      to_phone_number: "+15550001234",
      touch_number: 1,
      template_use_case: "ownership_check",
      queue_status: "sent"
    }
  ];

  const mockDeps = {
    supabase: createMockSupabase({ candidateRows, sendQueueRows }),
    chooseTextgridNumber: async () => ({ ok: true, selected: { id: "tn_1" } })
  };

  const result = await runSupabaseOutboundFeeder({
    dry_run: true,
    debug: true,
    limit: 10,
    scan_limit: 10,
    within_contact_window_now: false
  }, mockDeps);

  assert.equal(result.ok, true);
  assert.equal(result.queued_count, 1);
  
  const touch_context = result.first_10_candidate_touch_context[0];
  assert.equal(touch_context.history_latest_sent_touch_number, 1);
  assert.equal(touch_context.has_touch_1_ownership_check, true);
  assert.equal(touch_context.proposed_next_use_case, "consider_selling");
});

test("Supabase outbound feeder duplicate-blocks resolved touch", async () => {
  const candidateRows = [
    { ...baseCandidate, master_owner_id: "mo_dup", property_id: "prop_dup", last_touch_number: 1, use_case_template: "ownership_check" }
  ];

  const mockDeps = {
    supabase: createMockSupabase({ candidateRows }),
    hasDuplicateQueueItem: async (candidate) => {
      if (candidate.touch_number === 2 && candidate.template_use_case === "consider_selling") {
        return {
          duplicate: true,
          policy: { match_basis: ["master_owner_id"] },
          matched_row: { id: "existing_touch_2" }
        };
      }
      return { duplicate: false };
    }
  };

  const result = await runSupabaseOutboundFeeder({
    dry_run: true,
    debug: true,
    limit: 10,
    scan_limit: 10,
    within_contact_window_now: false
  }, mockDeps);

  assert.equal(result.skip_reasons["DUPLICATE_QUEUE_ITEM"], 1);
});

test("Supabase outbound feeder dry-run includes progression previews", async () => {
  const candidateRows = [
    { ...baseCandidate, master_owner_id: "mo_dry", property_id: "prop_dry", last_touch_number: 1, use_case_template: "ownership_check" }
  ];

  const mockDeps = {
    supabase: createMockSupabase({ candidateRows }),
    chooseTextgridNumber: async () => ({ ok: true, selected: { id: "tn_1" } }),
    // Explicitly mock renderOutboundTemplate to ensure it returns the expected template_id
    renderOutboundTemplate: async (candidate) => ({
      ok: true,
      template_use_case: candidate.template_use_case,
      language: "English",
      stage_code: "S2",
      selected_template: { id: "tmpl_2", source: "supabase" },
      rendered_message_body: "Still interested?",
      queue_payload: { 
        template_use_case: candidate.template_use_case,
        touch_number: candidate.touch_number,
        message_body: "Still interested?"
      }
    })
  };

  const result = await runSupabaseOutboundFeeder({
    dry_run: true,
    debug: true,
    limit: 10,
    scan_limit: 10,
    within_contact_window_now: false
  }, mockDeps);

  assert.equal(result.queued_count, 1);
  assert.ok(result.selected_templates["tmpl_2"] >= 1);
  assert.equal(result.first_10_would_queue[0].payload.template_use_case, "consider_selling");
  assert.equal(result.first_10_would_queue[0].payload.touch_number, 2);
});
