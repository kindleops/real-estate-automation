import test from "node:test";
import assert from "node:assert/strict";

import {
  createSendQueueItem,
  REASON_CODES,
  chooseTextgridNumber,
  evaluateCandidateEligibility,
  renderOutboundTemplate,
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
    best_phone_id: `ph_best_${String(id).padStart(8, "0")}11223344`,
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
    best_phone_id: "ph_best_00a",
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
  assert.equal(candidate.best_phone_id, "ph_best_00a");
  assert.equal(candidate.phone_id, "ph_best_00a");
});

test("master_owner.best_phone_id is used over other linked phones", () => {
  const candidate = normalizeCandidateRow({
    master_owner_id: "mo_x",
    property_id: "210000001",
    best_phone_id: "ph_best_abc",
    phone_id: "ph_other_xyz",
    canonical_e164: "+19195550111",
  });

  assert.equal(candidate.best_phone_id, "ph_best_abc");
  assert.equal(candidate.phone_id, "ph_best_abc");
});

test("phone_first_name from best phone becomes seller_first_name", () => {
  const candidate = normalizeCandidateRow({
    best_phone_id: "ph_best_1",
    phone_first_name: "Mia",
    phone_full_name: "Mia Johnson",
  });

  assert.equal(candidate.seller_first_name, "Mia");
  assert.equal(candidate.seller_full_name, "Mia Johnson");
});

test("phone_full_name fallback derives seller_first_name", () => {
  const candidate = normalizeCandidateRow({
    best_phone_id: "ph_best_2",
    phone_full_name: "Carlos Vega",
  });

  assert.equal(candidate.seller_first_name, "Carlos");
});

test("corporate owner display_name is not used when best phone name is missing", async () => {
  const candidate = normalizeCandidateRow({
    best_phone_id: "ph_best_3",
    display_name: "Sunrise Property Holdings LLC",
    property_address_full: "10 Market St",
    property_address_state: "TX",
  });

  const rendered = await renderOutboundTemplate(
    candidate,
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-corp",
          use_case: "ownership_check",
          language: "English",
          is_active: true,
          template_body: "Hi {seller_first_name}",
        },
      ],
    }
  );

  assert.equal(rendered.ok, true);
  assert.equal(rendered.variable_payload_preview.seller_first_name, "");
});

test("missing best phone skips with NO_BEST_PHONE unless fallback mode enabled", async () => {
  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      limit: 1,
      within_contact_window_now: false,
    },
    {
      supabase: makeSupabaseWithCandidates([
        makeCandidate(501, {
          best_phone_id: null,
          phone_id: "ph_other_501",
          canonical_e164: "+19195550123",
        }),
      ]),
      hasDuplicateQueueItem: async () => false,
    }
  );

  assert.equal(result.skipped_count, 1);
  assert.equal(result.sample_skips[0].reason_code, REASON_CODES.NO_BEST_PHONE);
});

test("queue key uses best_phone_id when present", async () => {
  const candidateA = normalizeCandidateRow({
    master_owner_id: "mo_qk_1",
    property_id: "2100009999",
    best_phone_id: "ph_best_A",
    phone_id: "ph_other_same",
    canonical_e164: "+19195550999",
    touch_number: 1,
    campaign_session_id: "session-qk",
  });
  const candidateB = normalizeCandidateRow({
    master_owner_id: "mo_qk_1",
    property_id: "2100009999",
    best_phone_id: "ph_best_B",
    phone_id: "ph_other_same",
    canonical_e164: "+19195550999",
    touch_number: 1,
    campaign_session_id: "session-qk",
  });

  const resultA = await createSendQueueItem(
    candidateA,
    {
      dry_run: true,
      template_use_case: "ownership_check",
      rendered_message_body: "hello",
      selected_textgrid_number: "+18325550101",
      selected_textgrid_number_id: 1,
      selected_textgrid_market: "Houston, TX",
    },
    {}
  );
  const resultB = await createSendQueueItem(
    candidateB,
    {
      dry_run: true,
      template_use_case: "ownership_check",
      rendered_message_body: "hello",
      selected_textgrid_number: "+18325550101",
      selected_textgrid_number_id: 1,
      selected_textgrid_market: "Houston, TX",
    },
    {}
  );

  assert.notEqual(resultA.queue_key, resultB.queue_key);
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
  assert.ok("best_phone_id" in skip.candidate_preview);
  assert.ok("phone_first_name" in skip.candidate_preview);
  assert.ok("phone_full_name" in skip.candidate_preview);
  assert.ok("seller_first_name" in skip.candidate_preview);
  assert.ok("seller_full_name" in skip.candidate_preview);
  assert.ok("joined_property_source" in skip.candidate_preview);
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

test("v_sms_ready_contacts candidate without template fields selects ownership_check S1", async () => {
  const candidate = normalizeCandidateRow({
    display_name: "Jane Seller",
    property_address_full: "123 Main St",
    property_address_city: "Charlotte",
    property_address_state: "NC",
    property_address_zip: "28202",
    market: "Charlotte, NC",
    language: "English",
    cash_offer: 120000,
  });

  let captured_selector = null;
  const result = await renderOutboundTemplate(
    candidate,
    {},
    {
      fetchSmsTemplates: async (selector) => {
        captured_selector = selector;
        return [
          {
            id: "tpl-1",
            template_id: "ownership-s1-en",
            use_case: "ownership_check",
            stage_code: "S1",
            stage_label: "Ownership Confirmation",
            language: "English",
            is_active: true,
            template_body: "Hi {{owner_display_name}}, is this still your property at {property_address}?",
          },
        ];
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(captured_selector.use_case, "ownership_check");
  assert.equal(captured_selector.stage_code, "S1");
  assert.equal(captured_selector.is_first_touch, true);
});

test("Spanish candidate selects Spanish ownership_check template when available", async () => {
  const candidate = normalizeCandidateRow({
    display_name: "Maria Lopez",
    best_language: "Spanish",
    property_address_full: "456 Elm St",
    property_address_state: "TX",
  });

  const result = await renderOutboundTemplate(
    candidate,
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-en",
          template_id: "ownership-s1-en",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Hello {owner_display_name}",
        },
        {
          id: "tpl-es",
          template_id: "ownership-s1-es",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Spanish",
          is_active: true,
          template_body: "Hola {owner_display_name}",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.template.template_id, "ownership-s1-es");
  assert.equal(result.language, "Spanish");
});

test("S1 ownership_check rotation spreads template selection across a healthy pool", async () => {
  const templates = Array.from({ length: 25 }, (_, index) => ({
    id: `tpl-${index + 1}`,
    template_id: `ownership-s1-en-${index + 1}`,
    use_case: "ownership_check",
    stage_code: "S1",
    language: "English",
    is_active: true,
    success_rate: 0.95,
    usage_count: 100,
    template_body: "Do you still own {property_address}?",
  }));

  const selected = new Set();
  for (let i = 0; i < 25; i += 1) {
    const candidate = normalizeCandidateRow({
      master_owner_id: `mo-rot-${i}`,
      property_id: `prop-rot-${i}`,
      phone_id: `ph-rot-${i}`,
      best_phone_id: `ph-best-rot-${i}`,
      canonical_e164: `+1832555${String(2000 + i).slice(-4)}`,
      display_name: "Rotate Owner",
      property_address_full: `100${i} Main St, Houston, TX 7700${i % 10}`,
      property_address_state: "TX",
      touch_number: 1,
      stage_code: "S1",
      template_use_case: "ownership_check",
      language: "English",
    });

    const result = await renderOutboundTemplate(
      candidate,
      { now: "2026-04-26T00:00:00.000Z", campaign_key: "campaign-rotate" },
      { fetchSmsTemplates: async () => templates }
    );

    assert.equal(result.ok, true);
    selected.add(result.template.template_id);
  }

  assert.ok(selected.size >= 5);
});

test("S1 ownership_check rotation is deterministic for same seed", async () => {
  const candidate = normalizeCandidateRow({
    master_owner_id: "mo-seed-1",
    property_id: "prop-seed-1",
    phone_id: "ph-seed-1",
    best_phone_id: "ph-best-seed-1",
    canonical_e164: "+18325550901",
    display_name: "Seed Owner",
    property_address_full: "45 Pine St, Atlanta, GA 30301",
    property_address_state: "GA",
    touch_number: 1,
    stage_code: "S1",
    template_use_case: "ownership_check",
    language: "English",
  });

  const templates = [
    {
      id: "tpl-seed-1",
      template_id: "ownership-s1-seed-1",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Seed 1 {property_address}",
    },
    {
      id: "tpl-seed-2",
      template_id: "ownership-s1-seed-2",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Seed 2 {property_address}",
    },
    {
      id: "tpl-seed-3",
      template_id: "ownership-s1-seed-3",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Seed 3 {property_address}",
    },
  ];

  const options = { now: "2026-04-26T00:00:00.000Z", campaign_key: "campaign-seed" };
  const result_a = await renderOutboundTemplate(candidate, options, { fetchSmsTemplates: async () => templates });
  const result_b = await renderOutboundTemplate(candidate, options, { fetchSmsTemplates: async () => templates });

  assert.equal(result_a.ok, true);
  assert.equal(result_b.ok, true);
  assert.equal(result_a.template.template_id, result_b.template.template_id);
  assert.equal(result_a.template_rotation.seed, result_b.template_rotation.seed);
});

test("S1 ownership_check rotation changes selection when identity seed fields differ", async () => {
  const templates = [
    {
      id: "tpl-var-1",
      template_id: "ownership-s1-var-1",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Var 1 {property_address}",
    },
    {
      id: "tpl-var-2",
      template_id: "ownership-s1-var-2",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Var 2 {property_address}",
    },
    {
      id: "tpl-var-3",
      template_id: "ownership-s1-var-3",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Var 3 {property_address}",
    },
  ];

  const left = await renderOutboundTemplate(
    normalizeCandidateRow({
      master_owner_id: "mo-var-1",
      property_id: "prop-var-1",
      best_phone_id: "ph-var-1",
      phone_id: "ph-var-1",
      canonical_e164: "+18325550911",
      display_name: "Var Owner",
      property_address_full: "1 Cedar St, Dallas, TX 75001",
      property_address_state: "TX",
      touch_number: 1,
      stage_code: "S1",
      template_use_case: "ownership_check",
      language: "English",
    }),
    { now: "2026-04-26T00:00:00.000Z", campaign_key: "campaign-var" },
    { fetchSmsTemplates: async () => templates }
  );

  const right = await renderOutboundTemplate(
    normalizeCandidateRow({
      master_owner_id: "mo-var-2",
      property_id: "prop-var-2",
      best_phone_id: "ph-var-2",
      phone_id: "ph-var-2",
      canonical_e164: "+18325550912",
      display_name: "Var Owner",
      property_address_full: "2 Cedar St, Dallas, TX 75001",
      property_address_state: "TX",
      touch_number: 1,
      stage_code: "S1",
      template_use_case: "ownership_check",
      language: "English",
    }),
    { now: "2026-04-26T00:00:00.000Z", campaign_key: "campaign-var" },
    { fetchSmsTemplates: async () => templates }
  );

  assert.equal(left.ok, true);
  assert.equal(right.ok, true);
  assert.notEqual(left.template_rotation.seed, right.template_rotation.seed);
  assert.notEqual(left.template.template_id, right.template.template_id);
});

test("S1 ownership_check rotation excludes recent template ids when alternatives exist", async () => {
  const candidate = normalizeCandidateRow({
    master_owner_id: "mo-recent-1",
    property_id: "prop-recent-1",
    best_phone_id: "ph-recent-1",
    phone_id: "ph-recent-1",
    canonical_e164: "+18325550921",
    display_name: "Recent Owner",
    property_address_full: "22 Apple Rd, Miami, FL 33101",
    property_address_state: "FL",
    touch_number: 1,
    stage_code: "S1",
    template_use_case: "ownership_check",
    language: "English",
  });

  const templates = [
    {
      id: "tpl-r-1",
      template_id: "ownership-s1-r-1",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Recent 1 {property_address}",
    },
    {
      id: "tpl-r-2",
      template_id: "ownership-s1-r-2",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Recent 2 {property_address}",
    },
    {
      id: "tpl-r-3",
      template_id: "ownership-s1-r-3",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      is_active: true,
      template_body: "Recent 3 {property_address}",
    },
  ];

  const result = await renderOutboundTemplate(
    candidate,
    { now: "2026-04-26T00:00:00.000Z", campaign_key: "campaign-recent" },
    {
      fetchSmsTemplates: async () => templates,
      getRecentTemplateIds: async () => ["ownership-s1-r-2"],
    }
  );

  assert.equal(result.ok, true);
  assert.notEqual(result.template.template_id, "ownership-s1-r-2");
  assert.ok(result.template_rotation.excluded_recent_template_ids.includes("ownership-s1-r-2"));
});

test("S1 ownership_check rotation still selects the only eligible template", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      master_owner_id: "mo-single-1",
      property_id: "prop-single-1",
      best_phone_id: "ph-single-1",
      phone_id: "ph-single-1",
      canonical_e164: "+18325550931",
      display_name: "Single Owner",
      property_address_full: "77 Solo St, Phoenix, AZ 85001",
      property_address_state: "AZ",
      touch_number: 1,
      stage_code: "S1",
      template_use_case: "ownership_check",
      language: "English",
    }),
    { now: "2026-04-26T00:00:00.000Z", campaign_key: "campaign-single" },
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-single",
          template_id: "ownership-s1-single",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Single {property_address}",
        },
      ],
      getRecentTemplateIds: async () => ["ownership-s1-single"],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.template.template_id, "ownership-s1-single");
  assert.equal(result.template_rotation.rotation_pool_size, 1);
});

test("createSendQueueItem stores template rotation diagnostics in metadata", async () => {
  const candidate = normalizeCandidateRow({
    master_owner_id: "mo-meta-1",
    property_id: "prop-meta-1",
    best_phone_id: "ph-meta-1",
    phone_id: "ph-meta-1",
    canonical_e164: "+18325550941",
    market: "Houston, TX",
    property_address_state: "TX",
    touch_number: 1,
    template_use_case: "ownership_check",
  });

  const result = await createSendQueueItem(
    candidate,
    {
      dry_run: true,
      template_use_case: "ownership_check",
      template_id: "ownership-s1-meta",
      template_source: "sms_templates",
      template_language: "English",
      template_stage_code: "S1",
      rendered_message_body: "Meta test",
      selected_textgrid_number_id: 3,
      selected_textgrid_number: "+18325550123",
      selected_textgrid_market: "Houston, TX",
      routing_tier: "exact_market_match",
      selection_reason: "exact_market_match",
      routing_allowed: true,
      routing_block_reason: null,
      template_rotation_enabled: true,
      template_rotation_seed: "seed-meta",
      template_rotation_pool_size: 7,
      template_rotation_candidate_ids: ["ownership-s1-meta", "ownership-s1-meta-2"],
      template_rotation_selected_index: 2,
      selected_template_id: "ownership-s1-meta",
      selected_template_source: "sms_templates",
      selected_template_language: "English",
      selected_template_use_case: "ownership_check",
      selected_template_stage_code: "S1",
    },
    {}
  );

  assert.equal(result.ok, true);
  assert.equal(result.payload.metadata.template_rotation_enabled, true);
  assert.equal(result.payload.metadata.template_rotation_seed, "seed-meta");
  assert.equal(result.payload.metadata.template_rotation_pool_size, 7);
  assert.deepEqual(result.payload.metadata.template_rotation_candidate_ids, ["ownership-s1-meta", "ownership-s1-meta-2"]);
  assert.equal(result.payload.metadata.selected_template_id, "ownership-s1-meta");
});

test("Persona mismatch falls back to null persona template", async () => {
  const candidate = normalizeCandidateRow({
    display_name: "Taylor Owner",
    language: "English",
    agent_persona: "Alex",
    property_address_full: "789 Oak Ave",
    property_address_state: "FL",
  });

  const result = await renderOutboundTemplate(
    candidate,
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-persona-other",
          template_id: "tpl-other",
          use_case: "ownership_check",
          language: "English",
          agent_persona: "Jordan",
          is_active: true,
          template_body: "Hello from Jordan",
        },
        {
          id: "tpl-persona-null",
          template_id: "tpl-null",
          use_case: "ownership_check",
          language: "English",
          agent_persona: null,
          is_active: true,
          template_body: "Hello from neutral",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.template.template_id, "tpl-null");
});

test("No template rows return NO_TEMPLATE", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({ display_name: "No Template" }),
    {},
    { fetchSmsTemplates: async () => [] }
  );

  assert.equal(result.ok, false);
  assert.equal(result.reason_code, REASON_CODES.NO_TEMPLATE);
});

test("Empty template body returns TEMPLATE_RENDER_FAILED rendered_message_empty", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({ display_name: "Empty Body" }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-empty",
          use_case: "ownership_check",
          language: "English",
          is_active: true,
          template_body: "",
          english_translation: "",
        },
      ],
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.reason_code, REASON_CODES.TEMPLATE_RENDER_FAILED);
  assert.equal(result.reason, "rendered_message_empty");
});

test("Template rendering supports both {{property_address}} and {property_address}", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Dual Placeholder",
      property_address_full: "1 Sunset Blvd",
      property_address_state: "CA",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-placeholders",
          use_case: "ownership_check",
          language: "English",
          is_active: true,
          template_body: "A: {{property_address}} | B: {property_address}",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.ok(result.rendered_message_body.includes("A: 1 Sunset Blvd"));
  assert.ok(result.rendered_message_body.includes("B: 1 Sunset Blvd"));
});

test("S1 ownership_check English renders street-only address", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner EN",
      property_address_full: "4094 Rue Antoinette, Stone Mountain, GA 30083",
      property_address_state: "GA",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-s1-en-street-only",
          template_id: "ownership-s1-en-street-only",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Do you still own {property_address}?",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Do you still own 4094 Rue Antoinette?");
  assert.equal(result.variable_payload_preview.property_address_full, "4094 Rue Antoinette, Stone Mountain, GA 30083");
  assert.equal(result.variable_payload_preview.property_street_address, "4094 Rue Antoinette");
});

test("S1 ownership_check Spanish renders street-only address", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner ES",
      best_language: "Spanish",
      property_address_full: "4094 Rue Antoinette, Stone Mountain, GA 30083",
      property_address_state: "GA",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-s1-es-street-only",
          template_id: "ownership-s1-es-street-only",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Spanish",
          is_active: true,
          template_body: "Todavia tienes {property_address}?",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Todavia tienes 4094 Rue Antoinette?");
});

test("S1 ownership_check Portuguese renders street-only address", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner PT",
      best_language: "Portuguese",
      property_address_full: "4094 Rue Antoinette, Stone Mountain, GA 30083",
      property_address_state: "GA",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-s1-pt-street-only",
          template_id: "ownership-s1-pt-street-only",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Portuguese",
          is_active: true,
          template_body: "Voce ainda e dono de {property_address}?",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Voce ainda e dono de 4094 Rue Antoinette?");
});

test("S1 ownership_check preserves # unit before city comma", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Unit",
      property_address_full: "188-190 Hunt St # 190, Central Falls, RI 02863",
      property_address_state: "RI",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-s1-unit-hash",
          template_id: "ownership-s1-unit-hash",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Checking {property_address}",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Checking 188-190 Hunt St # 190");
});

test("S1 ownership_check preserves Apt unit before city comma", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Apt",
      property_address_full: "123 Main St Apt 2B, Miami, FL 33101",
      property_address_state: "FL",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-s1-unit-apt",
          template_id: "ownership-s1-unit-apt",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Checking {property_address}",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Checking 123 Main St Apt 2B");
});

test("S1 ownership_check blocks full address rendering and reports diagnostics", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Block",
      property_address_full: "55 Carroll Rd, East Hartford, CT 06108",
      property_address_state: "CT",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-s1-hardcoded-full",
          template_id: "ownership-s1-hardcoded-full",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Do you still own 55 Carroll Rd, East Hartford, CT 06108?",
        },
      ],
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.reason_code, REASON_CODES.TEMPLATE_RENDER_FAILED);
  assert.equal(result.reason, "full_address_rendered_in_cold_sms");
  assert.equal(result.property_address_full, "55 Carroll Rd, East Hartford, CT 06108");
  assert.equal(result.property_street_address, "55 Carroll Rd");
  assert.ok(result.rendered_preview.includes("East Hartford"));
});

test("S1 ownership_check English renders first-name-only agent alias", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Agent EN",
      property_address_full: "10 Palm St, Tampa, FL 33601",
      property_address_state: "FL",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
      agent_persona: "Helen Crawford",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-agent-en",
          template_id: "ownership-s1-agent-en",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          is_active: true,
          template_body: "Hi, this is {agent_name}",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Hi, this is Helen");
  assert.equal(result.variable_payload_preview.agent_first_name, "Helen");
  assert.equal(result.variable_payload_preview.agent_name, "Helen");
});

test("S1 ownership_check Spanish renders first-name-only sender alias", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Agent ES",
      best_language: "Spanish",
      property_address_full: "25 Sol St, Miami, FL 33101",
      property_address_state: "FL",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
      agent_persona: "Carlos Mendez",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-agent-es",
          template_id: "ownership-s1-agent-es",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Spanish",
          is_active: true,
          template_body: "{sender_name} aqui",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Carlos aqui");
});

test("S1 ownership_check Portuguese renders first-name-only sms_agent_name alias", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Agent PT",
      best_language: "Portuguese",
      property_address_full: "50 Luz St, Orlando, FL 32801",
      property_address_state: "FL",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
      agent_persona: "Ana Lima",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-agent-pt",
          template_id: "ownership-s1-agent-pt",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Portuguese",
          is_active: true,
          template_body: "{sms_agent_name} aqui",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Ana aqui");
});

test("S1 ownership_check Mandarin renders first token for agent aliases", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Agent ZH",
      best_language: "Mandarin",
      property_address_full: "99 Maple St, Irvine, CA 92602",
      property_address_state: "CA",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
      agent_persona: "Wei Chen",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-agent-zh",
          template_id: "ownership-s1-agent-zh",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Mandarin",
          is_active: true,
          template_body: "{acquisition_agent_name} zai zheli",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body, "Wei zai zheli");
});

test("S1 ownership_check blocks full agent name rendering and reports diagnostics", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({
      display_name: "Owner Agent Gate",
      property_address_full: "101 Gate St, Austin, TX 78701",
      property_address_state: "TX",
      touch_number: 1,
      template_use_case: "ownership_check",
      stage_code: "S1",
      agent_persona: "Alejandro Vega",
    }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-agent-full-name",
          template_id: "ownership-s1-agent-full-name",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "Spanish",
          is_active: true,
          template_body: "Alejandro Vega aqui para ayudarte",
        },
      ],
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.reason_code, REASON_CODES.TEMPLATE_RENDER_FAILED);
  assert.equal(result.reason, "agent_full_name_rendered");
  assert.equal(result.agent_name_raw, "Alejandro Vega");
  assert.equal(result.agent_first_name, "Alejandro");
  assert.ok(result.rendered_preview.includes("Alejandro Vega"));
});

test("Template rendering strips HTML content", async () => {
  const result = await renderOutboundTemplate(
    normalizeCandidateRow({ display_name: "HTML Owner", property_address_full: "3 Pine St", property_address_state: "GA" }),
    {},
    {
      fetchSmsTemplates: async () => [
        {
          id: "tpl-html",
          use_case: "ownership_check",
          language: "English",
          is_active: true,
          template_body: "<p>Hello <strong>{owner_display_name}</strong>&nbsp;</p>",
        },
      ],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.rendered_message_body.includes("<"), false);
  assert.ok(result.rendered_message_body.includes("Hello"));
  assert.ok(result.rendered_message_body.includes("HTML"));
});

test("debug_templates=true includes template diagnostics in dry-run sample_skips", async () => {
  const result = await runSupabaseCandidateFeeder(
    {
      dry_run: true,
      debug_templates: true,
      limit: 1,
      scan_limit: 10,
      within_contact_window_now: false,
    },
    {
      supabase: makeSupabaseWithCandidates([
        makeCandidate(77, { market: "Houston, TX", property_address_state: "TX", property_address_full: "500 Main" }),
      ]),
      hasDuplicateQueueItem: async () => false,
      chooseTextgridNumber: async () => ({
        ok: true,
        routing_allowed: true,
        routing_tier: "exact_market_match",
        selection_reason: "exact_market_match",
        routing_rule_name: "exact_market_match",
        selected: { id: 1, phone_number: "+18325550111", market: "Houston, TX" },
      }),
      fetchSmsTemplates: async () => [],
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.no_template_count, 1);
  assert.equal(result.template_render_failed_count, 0);
  assert.ok(result.sample_skips.length > 0);
  const skip = result.sample_skips[0];
  assert.equal(skip.template_source, "sms_templates");
  assert.ok("template_lookup_use_case" in skip);
  assert.ok("missing_variables" in skip);
  assert.ok("variable_payload_preview" in skip);
  assert.ok("selected_template_preview" in skip);
  assert.ok("eligible_template_count" in skip);
  assert.ok("rotation_pool_size" in skip);
  assert.ok("rotation_candidate_template_ids" in skip);
  assert.ok("selected_template_id" in skip);
  assert.ok("rotation_seed" in skip);
  assert.ok("excluded_recent_template_ids" in skip);
});

