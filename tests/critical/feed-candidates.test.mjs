import test from "node:test";
import assert from "node:assert/strict";

import {
  REASON_CODES,
  evaluateCandidateEligibility,
  runSupabaseCandidateFeeder,
} from "@/lib/domain/outbound/supabase-candidate-feeder.js";
import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";

function makeSupabaseWithCandidates(candidates = []) {
  return {
    from(table) {
      return {
        select() {
          return {
            limit() {
              if (table === "v_sms_campaign_queue_candidates") {
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
    master_owner_id: id,
    property_id: id + 100,
    best_phone_id: id + 200,
    canonical_e164: `+12085550${String(100 + id).slice(-3)}`,
    seller_market: "houston",
    seller_state: "TX",
    contact_window: "9:00 AM - 8:00 PM",
    timezone: "America/Chicago",
    ...overrides,
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
  assert.equal(result.effective_candidate_fetch_limit, 5);
  assert.equal(result.fetched_candidate_count, 1);
  assert.equal(result.scanned_count, 1);
  assert.equal(result.queued_count, 1);
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
        routing_block_reason: "no_approved_routing_path",
      }),
    }
  );

  assert.equal(result.routing_block_count, 1);
  assert.equal(result.queued_count, 0);
  assert.equal(result.sample_skips[0].reason_code, REASON_CODES.ROUTING_BLOCKED);
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
      phone_id: candidate.best_phone_id,
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
