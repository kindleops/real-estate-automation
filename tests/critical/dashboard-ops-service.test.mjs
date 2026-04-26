import test from "node:test";
import assert from "node:assert/strict";

import { getOpsFeederSnapshot, parseOpsFilters } from "@/lib/dashboard/ops-service.js";

function passthroughCache(_key, _ttl, loader) {
  return loader();
}

test("parseOpsFilters defaults dashboard feeder to v_sms_ready_contacts with safe routing", () => {
  const filters = parseOpsFilters({});

  assert.equal(filters.candidate_source, "v_sms_ready_contacts");
  assert.equal(filters.routing_safe_only, true);
  assert.equal(filters.legacy_feeder, false);
});

test("getOpsFeederSnapshot uses Supabase candidate feeder and preserves dashboard shape", async () => {
  let captured_input = null;

  const result = await getOpsFeederSnapshot(
    {
      limit: 3,
      scan_limit: 15,
    },
    {
      readThroughCache: passthroughCache,
      getOpsFilterOptions: async () => ({
        views: [
          { view_id: 123, name: "SMS / TIER #1 / ALL" },
        ],
      }),
      runSupabaseCandidateFeeder: async (input) => {
        captured_input = input;
        return {
          ok: true,
          dry_run: true,
          candidate_source: "v_sms_ready_contacts",
          fetched_candidate_count: 7,
          eligible_count: 4,
          queued_count: 3,
          skipped_count: 4,
          sample_skips: [{ reason_code: "NO_APPROVED_ROUTING_PATH" }],
          selected_textgrid_market_counts: { "Los Angeles, CA": 2, "Dallas, TX": 1 },
          routing_tier_counts: { approved_regional_fallback: 3 },
          sample_created_queue_items: [
            { master_owner_id: "mo_1" },
            { master_owner_id: "mo_2" },
            { master_owner_id: "mo_3" },
          ],
          error: null,
        };
      },
    }
  );

  assert.deepEqual(captured_input, {
    dry_run: true,
    candidate_source: "v_sms_ready_contacts",
    routing_safe_only: true,
    scan_limit: 15,
    limit: 3,
  });

  assert.equal(result.ok, true);
  assert.equal(result.dry_run, true);
  assert.equal(result.loaded_count, 7);
  assert.equal(result.eligible_count, 4);
  assert.equal(result.inserted_count, 3);
  assert.equal(result.queued_count, 3);
  assert.equal(result.skipped_count, 4);
  assert.deepEqual(result.sample_skips, [{ reason_code: "NO_APPROVED_ROUTING_PATH" }]);
  assert.deepEqual(result.selected_textgrid_market_counts, { "Los Angeles, CA": 2, "Dallas, TX": 1 });
  assert.deepEqual(result.routing_tier_counts, { approved_regional_fallback: 3 });
  assert.equal(result.error, null);
  assert.deepEqual(result.queued_owner_ids, ["mo_1", "mo_2", "mo_3"]);
});

test("getOpsFeederSnapshot rejects legacy feeder requests unless env flag is true", async () => {
  const previous_value = process.env.LEGACY_PODIO_FEEDER_ENABLED;
  delete process.env.LEGACY_PODIO_FEEDER_ENABLED;

  try {
    const result = await getOpsFeederSnapshot(
      { legacy: true },
      {
        readThroughCache: passthroughCache,
        getOpsFilterOptions: async () => ({ views: [] }),
        runSupabaseCandidateFeeder: async () => {
          throw new Error("Supabase feeder should not be called for disabled legacy requests");
        },
      }
    );

    assert.equal(result.ok, false);
    assert.equal(result.error, "LEGACY_PODIO_FEEDER_DISABLED");
    assert.equal(result.message, "Dashboard feeder actions now use Supabase candidate feeder.");
    assert.equal(result.loaded_count, 0);
    assert.equal(result.queued_count, 0);
  } finally {
    if (previous_value === undefined) {
      delete process.env.LEGACY_PODIO_FEEDER_ENABLED;
    } else {
      process.env.LEGACY_PODIO_FEEDER_ENABLED = previous_value;
    }
  }
});