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
test('live inbox exposes cursor pagination, filters, keyword matches, and map pins', async () => {
  const { getLiveInbox } = await import('@/lib/domain/inbox/live-inbox-service.js');
  const rows = Array.from({ length: 260 }, (_, idx) => ({
    id: idx + 1,
    created_at: new Date(Date.UTC(2026, 4, 6, 12, 0, 0) - idx * 1000).toISOString(),
    direction: idx % 3 === 0 ? 'inbound' : 'outbound',
    message_body: idx === 0 ? 'yes I am interested, make offer' : idx === 3 ? 'how much is your offer' : `message ${idx}`,
    from_phone_number: '+15550000001',
    to_phone_number: '+15550000002',
    property_id: idx < 10 ? 101 : null,
    master_owner_id: 201,
    seller_display_name: 'Test Seller',
    property_address: '123 Main St',
    market: 'Test Market',
    metadata: {},
  }));
  const supabase = {
    from(table) {
      const state = { table, limit: 1000, direction: null, q: null };
      const api = {
        select() { return api; },
        order() { return api; },
        limit(n) { state.limit = n; return api; },
        eq(col, val) { if (col === 'direction') state.direction = val; return api; },
        lt() { return api; },
        ilike(_col, val) { state.q = String(val).replaceAll('%', '').toLowerCase(); return api; },
        not() { return api; },
        then(resolve) {
          if (state.table === 'properties') return resolve({ data: [{ id: 101, latitude: 34.1, longitude: -118.2, address: '123 Main St', market: 'Test Market', seller_name: 'Test Seller', stage: 'new' }], error: null });
          let data = rows;
          if (state.direction) data = data.filter((r) => r.direction === state.direction);
          if (state.q) data = data.filter((r) => r.message_body.toLowerCase().includes(state.q));
          return resolve({ data: data.slice(0, state.limit), error: null });
        },
      };
      return api;
    },
  };
  const page = await getLiveInbox({ limit: '100', direction: 'all', map: 'true' }, { supabase });
  assert.strictEqual(page.messages.length, 100);
  assert.ok(page.pagination.has_more);
  assert.ok(page.pagination.next_cursor);
  assert.ok(page.mapPins.length >= 1);

  const inbound = await getLiveInbox({ limit: '100', direction: 'inbound' }, { supabase });
  assert.ok(inbound.messages.every((m) => m.direction === 'inbound'));

  const keyword = await getLiveInbox({ limit: '10', q: 'interested' }, { supabase });
  assert.ok(keyword.messages[0].matched_keywords.includes('interested'));

  const hot = await getLiveInbox({ limit: '10', filter: 'positive_hot' }, { supabase });
  assert.ok(hot.messages.some((m) => m.flags.positive_hot));

  const needsReply = await getLiveInbox({ limit: '10', filter: 'needs_reply' }, { supabase });
  assert.ok(needsReply.messages.every((m) => m.direction === 'inbound'));
});
