import test from "node:test";
import assert from "node:assert/strict";

import {
  REASON_CODES,
  chooseTextgridNumber,
  evaluateCandidateEligibility,
  runSupabaseCandidateFeeder,
  normalizeCandidateRow,
} from "@/lib/domain/outbound/supabase-candidate-feeder.js";
import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";

function makeSupabaseWithCandidates(candidates = [], sourceName = "v_sms_campaign_queue_candidates") {
  return {
    from(table) {
      return {
        select() {
          return {
            limit() {
              if (table === sourceName) {
                return Promise.resolve({ data: candidates, error: null });
              }
              return Promise.resolve({ data: [], error: { code: "42P01", message: `missing ${table}` } });
            },
          };
        },
      };
    },
  };
}

function makeCandidate(id = 1, overrides = {}) {
  return {
    master_owner_id: `mo_${String(id).padStart(8, "0")}aabbccdd`,
    property_id: String(id + 2100000000),
    property_export_id: `prop_${String(id).padStart(8, "0")}eeff1122`,
    phone_id: `ph_${String(id).padStart(8, "0")}99887766`,
    canonical_e164: `+12085550${String(100 + id).slice(-3)}`,
    market: "houston",
    property_address_state: "TX",
    contact_window: "9:00 AM - 8:00 PM",
    timezone: "America/Chicago",
    ...overrides,
  };
}

function makeTextgridNumber(id, market, overrides = {}) {
  return {
    id,
    market,
    phone_number: `+1832555${String(1000 + id).slice(-4)}`,
    status: "active",
    messages_sent_today: 0,
    ...overrides,
  };
}

function makeTextgridSupabase(numbers = []) {
  return {
    from(table) {
      return {
        select() {
          return {
            limit() {
              if (table === "textgrid_numbers") {
                return Promise.resolve({ data: numbers, error: null });
              }
              return Promise.resolve({ data: [], error: null });
            },
          };
        },
      };
    },
  };
}

test("runSupabaseCandidateFeeder dry_run returns diagnostics without queue mutation", async () => {
  let create_calls = 0;

  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      limit: 5,
      scan_limit: 5,
      campaign_session_id: "session-dry-run",
      template_use_case: "ownership_check",
      within_contact_window_now: false,
      routing_safe_only: true,
    },
    {
      supabase: makeSupabaseWithCandidates([makeCandidate(1)]),
      hasDuplicateQueueItem: async () => false,
      chooseTextgridNumber: async () => ({
        ok: true,
        routing_allowed: true,
        routing_tier: "exact_market_match",
        selection_reason: "exact_market_match",
        routing_rule_name: "exact_market_match",
        selected: {
          id: 10,
          phone_number: "+18325550101",
          market: "houston",
        },
      }),
      renderOutboundTemplate: async () => ({
        ok: true,
        template: { item_id: "tpl_1", source: "supabase" },
        template_use_case: "ownership_check",
        rendered_message_body: "Hi, is this still your property?",
      }),
      createSendQueueItem: async () => {
        create_calls += 1;
        return { ok: true, queued: false, queue_key: "dry-run-key", queue_row_id: null };
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.dry_run, true);
  assert.equal(result.candidate_source, "v_sms_campaign_queue_candidates");
  assert.equal(result.requested_limit, 5);
  assert.equal(result.effective_candidate_fetch_limit, 25);
  assert.equal(result.fetched_candidate_count, 1);
  assert.equal(result.scanned_count, 1);
  assert.equal(result.queued_count, 1);
  assert.equal(result.sample_created_queue_items[0].routing_rule_name, "exact_market_match");
  assert.equal(result.sample_created_queue_items[0].selected_textgrid_market, "houston");
  assert.equal(result.sample_created_queue_items[0].selected_textgrid_number, "+18325550101");
  assert.equal(create_calls, 0);
});

test("runSupabaseCandidateFeeder live mode respects limit=1", async () => {
  let create_calls = 0;

  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: false,
      limit: 1,
      scan_limit: 10,
      campaign_session_id: "session-live-1",
      template_use_case: "ownership_check",
      within_contact_window_now: false,
      routing_safe_only: true,
    },
    {
      supabase: makeSupabaseWithCandidates([makeCandidate(1), makeCandidate(2)]),
      hasDuplicateQueueItem: async () => false,
      chooseTextgridNumber: async () => ({
        ok: true,
        routing_allowed: true,
        routing_tier: "exact_market_match",
        selection_reason: "exact_market_match",
        selected: {
          id: 11,
          phone_number: "+18325550102",
          market: "houston",
        },
      }),
      renderOutboundTemplate: async () => ({
        ok: true,
        template: { item_id: "tpl_2", source: "supabase" },
        template_use_case: "ownership_check",
        rendered_message_body: "Quick question about your property.",
      }),
      createSendQueueItem: async () => {
        create_calls += 1;
        return { ok: true, queued: true, queue_key: `queue-${create_calls}`, queue_row_id: create_calls };
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.queued_count, 1);
  assert.equal(create_calls, 1);
});

test("runSupabaseCandidateFeeder reports routing diagnostics for blocked routing", async () => {
  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      limit: 5,
      scan_limit: 5,
      campaign_session_id: "session-routing",
      routing_safe_only: true,
      within_contact_window_now: false,
    },
    {
      supabase: makeSupabaseWithCandidates([makeCandidate(3)]),
      hasDuplicateQueueItem: async () => false,
      chooseTextgridNumber: async () => ({
        ok: false,
        reason_code: REASON_CODES.ROUTING_BLOCKED,
        routing_allowed: false,
        routing_tier: "blocked",
        selection_reason: null,
        routing_rule_name: null,
        selected_textgrid_market: null,
        selected_textgrid_number: null,
        seller_market: "Inland Empire, CA",
        seller_state: "CA",
        routing_block_reason: "NO_APPROVED_ROUTING_PATH",
      }),
    }
  );

  assert.equal(result.routing_block_count, 1);
  assert.equal(result.queued_count, 0);
  assert.equal(result.sample_skips[0].reason_code, REASON_CODES.ROUTING_BLOCKED);
  assert.equal(result.sample_skips[0].routing_block_reason, "NO_APPROVED_ROUTING_PATH");
  assert.equal(result.sample_skips[0].seller_market, "Inland Empire, CA");
  assert.equal(result.sample_skips[0].seller_state, "CA");
});

test("runSupabaseCandidateFeeder returns structured source unavailable error", async () => {
  const missingSourceSupabase = {
    from() {
      return {
        select() {
          return {
            limit() {
              return Promise.resolve({
                data: [],
                error: {
                  code: "42P01",
                  message: "Could not find the table 'public.v_sms_campaign_queue_candidates' in the schema cache",
                },
              });
            },
          };
        },
      };
    },
  };

  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      limit: 1,
      scan_limit: 1,
    },
    { supabase: missingSourceSupabase }
  );

  assert.equal(result.ok, false);
  assert.equal(result.error, "CANDIDATE_SOURCE_UNAVAILABLE");
  assert.equal(result.candidate_source, "v_sms_campaign_queue_candidates");
  assert.ok(String(result.candidate_source_error || "").includes("schema cache"));
  assert.deepEqual(result.available_hint, [
    "v_sms_campaign_queue_candidates",
    "v_sms_ready_contacts",
    "v_launch_sms_tier1",
  ]);
});

test("evaluateCandidateEligibility blocks duplicate queue items", async () => {
  const candidate = makeCandidate(9);

  const decision = await evaluateCandidateEligibility(
    {
      ...candidate,
    },
    {
      template_use_case: "ownership_check",
      within_contact_window_now: true,
      now: new Date().toISOString(),
    },
    {
      hasDuplicateQueueItem: async () => true,
    }
  );

  assert.equal(decision.ok, false);
  assert.equal(decision.reason_code, REASON_CODES.DUPLICATE_QUEUE_ITEM);
});

test("runSendQueue dry_run never calls processSendQueueItem", async () => {
  let processed = 0;

  const result = await runSendQueue(
    {
      dry_run: true,
      limit: 5,
      now: "2026-04-25T15:00:00.000Z",
    },
    {
      withRunLock: async ({ fn }) => fn(),
      fetchAllItems: async () => [
        {
          item_id: 999,
          queue_status: "queued",
          scheduled_for: "2026-04-25T14:59:00.000Z",
          message_body: "Test",
        },
      ],
      processSendQueueItem: async () => {
        processed += 1;
        return { ok: true, sent: true };
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.dry_run, true);
  assert.equal(result.skipped_count, 1);
  assert.equal(processed, 0);
});

test("normalizeCandidateRow accepts text IDs (mo_, ph_, prop_ prefixes)", () => {
  const row = {
    master_owner_id: "mo_f3c1cbd62c4a654437347dc4",
    property_id: "2100303759",
    property_export_id: "prop_17f5600c6485298d5ccc8743",
    phone_id: "ph_a5b7789a97742782ff2d595b",
    canonical_e164: "+19197969608",
    market: "Charlotte, NC",
    property_address_state: "NC",
  };

  const candidate = normalizeCandidateRow(row);
  assert.equal(candidate.master_owner_id, "mo_f3c1cbd62c4a654437347dc4");
  assert.equal(candidate.phone_id, "ph_a5b7789a97742782ff2d595b");
  assert.equal(candidate.property_id, "2100303759");
  assert.equal(candidate.property_export_id, "prop_17f5600c6485298d5ccc8743");
  assert.equal(candidate.canonical_e164, "+19197969608");
  assert.equal(candidate.state, "NC");
});

test("normalizeCandidateRow maps v_sms_ready_contacts columns correctly", () => {
  const row = {
    master_owner_id: "mo_f3c1cbd62c4a654437347dc4",
    property_id: "2100303759",
    property_export_id: "prop_17f5600c6485298d5ccc8743",
    phone_id: "ph_a5b7789a97742782ff2d595b",
    canonical_e164: "+19197969608",
    market: "Charlotte, NC",
    property_address_state: "NC",
    property_address_city: "Charlotte",
    property_address_zip: "28202",
    property_address_full: "123 Main St, Charlotte, NC 28202",
    display_name: "John Smith",
    agent_persona: "Alex",
    agent_family: "southeast_residential",
    best_language: "English",
    final_acquisition_score: 87,
    best_phone_score: 92,
    cash_offer: 125000,
    estimated_value: 180000,
    equity_amount: 55000,
    equity_percent: 30.5,
    priority_tier: "tier_1",
    sms_eligible: true,
  };

  const candidate = normalizeCandidateRow(row);
  assert.equal(candidate.owner_display_name, "John Smith");
  assert.equal(candidate.property_city, "Charlotte");
  assert.equal(candidate.property_zip, "28202");
  assert.equal(candidate.property_address_full, "123 Main St, Charlotte, NC 28202");
  assert.equal(candidate.agent_persona, "Alex");
  assert.equal(candidate.agent_family, "southeast_residential");
  assert.equal(candidate.best_language, "English");
  assert.equal(candidate.final_acquisition_score, 87);
  assert.equal(candidate.cash_offer, 125000);
  assert.equal(candidate.priority_tier, "tier_1");
  assert.equal(candidate.sms_eligible, true);
  assert.equal(candidate.state_code, "NC");
});

test("runSupabaseCandidateFeeder limit=1 fetches at most 10 candidates from source", async () => {
  let captured_limit = null;

  const countingSupabase = {
    from() {
      return {
        select() {
          return {
            limit(n) {
              captured_limit = n;
              return Promise.resolve({ data: [makeCandidate(1)], error: null });
            },
          };
        },
      };
    },
  };

  await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      limit: 1,
      campaign_session_id: "session-limit-test",
      within_contact_window_now: false,
    },
    {
      supabase: countingSupabase,
      hasDuplicateQueueItem: async () => false,
      chooseTextgridNumber: async () => ({
        ok: true,
        routing_allowed: true,
        routing_tier: "exact_market_match",
        selection_reason: "exact_market_match",
        selected: { id: 10, phone_number: "+18325550101", market: "houston" },
      }),
      renderOutboundTemplate: async () => ({
        ok: true,
        template: { item_id: "tpl_1", source: "supabase" },
        template_use_case: "ownership_check",
        rendered_message_body: "Hi there.",
      }),
    }
  );

  // limit=1 → effectiveCandidateFetchLimit = min(max(1*5, 10), 100) = 10
  assert.equal(captured_limit, 10);
});

test("runSupabaseCandidateFeeder dry_run sample_skips include normalized candidate preview", async () => {
  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      limit: 5,
      campaign_session_id: "session-preview",
      within_contact_window_now: false,
    },
    {
      supabase: makeSupabaseWithCandidates([
        makeCandidate(1, { phone_id: null, canonical_e164: null }),
      ]),
      hasDuplicateQueueItem: async () => false,
    }
  );

  assert.ok(result.sample_skips.length > 0);
  const skip = result.sample_skips[0];
  assert.ok("candidate_preview" in skip, "dry_run skip should have candidate_preview");
  assert.ok(Array.isArray(skip.candidate_preview.raw_keys));
  assert.ok("normalized_master_owner_id" in skip.candidate_preview);
  assert.ok("normalized_phone_id" in skip.candidate_preview);
});

test("chooseTextgridNumber routes Inland Empire, CA to Los Angeles via approved regional fallback", async () => {
  const result = await chooseTextgridNumber(
    { market: "Inland Empire, CA", state: "CA" },
    { routing_safe_only: true },
    {
      supabase: makeTextgridSupabase([
        makeTextgridNumber(1, "Los Angeles, CA"),
        makeTextgridNumber(2, "Dallas, TX"),
      ]),
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.routing_tier, "approved_regional_fallback");
  assert.equal(result.routing_rule_name, "ca_to_los_angeles");
  assert.equal(result.selected_textgrid_market, "Los Angeles, CA");
  assert.equal(result.routing_allowed, true);
});

test("chooseTextgridNumber routes Stockton, CA to Los Angeles", async () => {
  const result = await chooseTextgridNumber(
    { market: "Stockton, CA", state: "CA" },
    { routing_safe_only: true },
    { supabase: makeTextgridSupabase([makeTextgridNumber(1, "Los Angeles, CA")]) }
  );

  assert.equal(result.ok, true);
  assert.equal(result.selected_textgrid_market, "Los Angeles, CA");
  assert.equal(result.routing_tier, "approved_regional_fallback");
});

test("chooseTextgridNumber routes Boise, ID to Los Angeles", async () => {
  const result = await chooseTextgridNumber(
    { market: "Boise, ID", state: "ID" },
    { routing_safe_only: true },
    { supabase: makeTextgridSupabase([makeTextgridNumber(1, "Los Angeles, CA")]) }
  );

  assert.equal(result.ok, true);
  assert.equal(result.selected_textgrid_market, "Los Angeles, CA");
  assert.equal(result.routing_rule_name, "west_mountain_to_los_angeles");
});

test("chooseTextgridNumber routes Tulsa, OK to Dallas or Houston per approved regional rule", async () => {
  const result = await chooseTextgridNumber(
    { market: "Tulsa, OK", state: "OK" },
    { routing_safe_only: true },
    {
      supabase: makeTextgridSupabase([
        makeTextgridNumber(1, "Dallas, TX"),
        makeTextgridNumber(2, "Houston, TX"),
      ]),
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.routing_tier, "approved_regional_fallback");
  assert.ok(["Dallas, TX", "Houston, TX"].includes(result.selected_textgrid_market));
  assert.equal(result.routing_rule_name, "southern_plains_to_dallas");
});

test("chooseTextgridNumber routes Illinois to Minneapolis", async () => {
  const result = await chooseTextgridNumber(
    { market: "Peoria, IL", state: "IL" },
    { routing_safe_only: true },
    { supabase: makeTextgridSupabase([makeTextgridNumber(1, "Minneapolis, MN")]) }
  );

  assert.equal(result.ok, true);
  assert.equal(result.selected_textgrid_market, "Minneapolis, MN");
  assert.equal(result.routing_rule_name, "midwest_to_minneapolis");
});

test("chooseTextgridNumber routes New York to Miami", async () => {
  const result = await chooseTextgridNumber(
    { market: "Albany, NY", state: "NY" },
    { routing_safe_only: true },
    { supabase: makeTextgridSupabase([makeTextgridNumber(1, "Miami, FL")]) }
  );

  assert.equal(result.ok, true);
  assert.equal(result.selected_textgrid_market, "Miami, FL");
  assert.equal(result.routing_rule_name, "northeast_to_miami");
});

test("chooseTextgridNumber routes Florida to Jacksonville before Miami", async () => {
  const result = await chooseTextgridNumber(
    { market: "Tampa, FL", state: "FL" },
    { routing_safe_only: true },
    {
      supabase: makeTextgridSupabase([
        makeTextgridNumber(1, "Miami, FL"),
        makeTextgridNumber(2, "Jacksonville, FL"),
      ]),
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.selected_textgrid_market, "Jacksonville, FL");
  assert.equal(result.routing_rule_name, "florida_to_jacksonville_then_miami");
});

test("chooseTextgridNumber blocks unknown state with no approved routing rule", async () => {
  const result = await chooseTextgridNumber(
    { market: "Anchorage, AK", state: "AK" },
    { routing_safe_only: true },
    { supabase: makeTextgridSupabase([makeTextgridNumber(1, "Los Angeles, CA")]) }
  );

  assert.equal(result.ok, false);
  assert.equal(result.routing_allowed, false);
  assert.equal(result.routing_block_reason, "NO_APPROVED_ROUTING_PATH");
});

test("chooseTextgridNumber allows approved regional fallback when routing_safe_only=true", async () => {
  const result = await chooseTextgridNumber(
    { market: "Birmingham, AL", state: "AL" },
    { routing_safe_only: true },
    {
      supabase: makeTextgridSupabase([
        makeTextgridNumber(1, "Atlanta, GA"),
        makeTextgridNumber(2, "Charlotte, NC"),
      ]),
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.routing_tier, "approved_regional_fallback");
  assert.equal(result.selected_textgrid_market, "Atlanta, GA");
});

test("chooseTextgridNumber blocks random nationwide fallback when routing_safe_only=true", async () => {
  const result = await chooseTextgridNumber(
    { market: "Anchorage, AK", state: "AK" },
    { routing_safe_only: true },
    {
      supabase: makeTextgridSupabase([
        makeTextgridNumber(1, "Miami, FL", {
          is_nationwide: true,
          allow_nationwide_fallback: true,
        }),
      ]),
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.routing_allowed, false);
  assert.equal(result.routing_block_reason, "NO_APPROVED_ROUTING_PATH");
});

