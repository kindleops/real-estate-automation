import test from "node:test";
import assert from "node:assert/strict";

import { loadThreadContext } from "@/lib/domain/inbox/thread-context-service.js";

function createMockSupabase() {
  const thread_key = "mo-1:prop-1:+15550001111:+15550002222";

  const tableData = {
    message_events: [{ id: "me-1", thread_key, master_owner_id: "mo-1", property_id: "prop-1", owner_id: "owner-1", prospect_id: "pros-1", market_id: "mkt-1", phone_number_id: "pn-1" }],
    send_queue: [{ id: "sq-1", thread_key, master_owner_id: "mo-1", property_id: "prop-1", market_id: "mkt-1", phone_number_id: "pn-1" }],
    ai_conversation_brain: [{ id: "brain-1", thread_key, master_owner_id: "mo-1", property_id: "prop-1" }],
    properties: [{ id: "prop-1", address: "123 Main St" }],
    master_owners: [{ id: "mo-1", name: "Seller One" }],
    owners: [{ id: "owner-1", full_name: "Owner One" }],
    prospects: [{ id: "pros-1", full_name: "Prospect One" }],
    phone_numbers: [{ id: "pn-1", e164: "+15550001111" }],
    emails: [{ id: "em-1", owner_id: "owner-1", email: "owner@example.com" }],
    markets: [{ id: "mkt-1", name: "Dallas, TX" }],
    zip_codes: [{ id: "zip-1", market_id: "mkt-1", zip_code: "75001" }],
    offers: [{ id: "off-1", master_owner_id: "mo-1", mao: 101000, walkaway_internal: 98000, internal_valuation: 125000 }],
    underwriting: [{ id: "uw-1", property_id: "prop-1", mao: 100500 }],
    contracts: [{ id: "ct-1", master_owner_id: "mo-1" }],
    title_routing_closing_engine: [{ id: "tr-1", property_id: "prop-1" }],
    closings: [{ id: "cl-1", property_id: "prop-1" }],
    buyer_match: [{ id: "bm-1", property_id: "prop-1" }],
    agents: [{ id: "ag-1", name: "Agent One" }],
    templates: [{ id: "tpl-1", key: "consider_selling" }],
    inbox_thread_state: [{ thread_key, is_read: false, is_archived: false }],
  };

  function buildResult(table, state) {
    let rows = Array.isArray(tableData[table]) ? [...tableData[table]] : [];

    for (const clause of state.eq) {
      rows = rows.filter((row) => String(row?.[clause.column] ?? "") === String(clause.value));
    }
    for (const clause of state.in) {
      const allowed = new Set((clause.values || []).map((v) => String(v)));
      rows = rows.filter((row) => allowed.has(String(row?.[clause.column] ?? "")));
    }
    if (Number.isFinite(state.limit) && state.limit >= 0) rows = rows.slice(0, state.limit);

    return { data: rows, error: null };
  }

  function makeChain(table) {
    const state = { eq: [], in: [], limit: null };
    const chain = {
      select() { return chain; },
      eq(column, value) { state.eq.push({ column, value }); return chain; },
      in(column, values) { state.in.push({ column, values }); return Promise.resolve(buildResult(table, state)); },
      order() { return chain; },
      limit(value) { state.limit = Number(value); return Promise.resolve(buildResult(table, state)); },
    };
    return chain;
  }

  return {
    thread_key,
    from(table) {
      return makeChain(table);
    },
  };
}

test("loadThreadContext returns full context and redacts seller-dangerous internals in aiSafeContext", async () => {
  const mockSupabase = createMockSupabase();
  const payload = await loadThreadContext({ thread_key: mockSupabase.thread_key, supabase: mockSupabase });

  assert.equal(payload.thread_key, mockSupabase.thread_key);

  const selected = payload?.context?.selected_thread;
  assert.ok(selected);

  const requiredBlocks = [
    "properties", "master_owners", "owners", "prospects", "phone_numbers", "emails",
    "markets", "zip_codes", "send_queue", "message_events", "ai_conversation_brain",
    "offers", "underwriting", "contracts", "title_routing_closing_engine", "closings",
    "buyer_match", "agents", "templates",
  ];

  for (const key of requiredBlocks) {
    assert.ok(Array.isArray(selected[key]), `selected_thread.${key} must be an array`);
  }

  const internalJson = JSON.stringify(payload?.copilot_context?.internalOnlyContext || {});
  const safeJson = JSON.stringify(payload?.copilot_context?.aiSafeContext || {});

  assert.match(internalJson, /mao/i);
  assert.match(internalJson, /walkaway_internal/i);
  assert.match(internalJson, /internal_valuation/i);

  assert.doesNotMatch(safeJson, /walkaway_internal/i);
  assert.doesNotMatch(safeJson, /internal_valuation/i);
  assert.doesNotMatch(safeJson, /"mao"/i);
  assert.deepEqual(payload.seller_facing_context, payload.copilot_context.aiSafeContext);
  assert.notDeepEqual(payload.seller_facing_context, payload.copilot_context.internalOnlyContext);
});

test("loadThreadContext returns best-effort context with degraded sources and safe redaction", async () => {
  const mockSupabase = createMockSupabase();

  const originalFrom = mockSupabase.from;
  mockSupabase.from = (table) => {
    if (table === "offers") {
      return {
        select() {
          return {
            in() {
              return Promise.resolve({ data: [], error: { message: "offers timeout" } });
            },
          };
        },
      };
    }

    if (["underwriting", "contracts", "buyer_match", "agents", "templates", "send_queue", "ai_conversation_brain"].includes(table)) {
      return {
        select() {
          return {
            eq() {
              return { order() { return { limit() { return Promise.resolve({ data: [], error: null }); } }; } };
            },
            in() {
              return Promise.resolve({ data: [], error: null });
            },
            limit() {
              return Promise.resolve({ data: [], error: null });
            },
          };
        },
      };
    }

    return originalFrom(table);
  };

  const payload = await loadThreadContext({ thread_key: mockSupabase.thread_key, supabase: mockSupabase });

  assert.ok(payload.context?.selected_thread);
  assert.ok(payload.dossier);
  assert.ok(Array.isArray(payload.source_health));
  assert.ok(Array.isArray(payload.missingData));

  const failedOffers = payload.source_health.find((entry) => entry.table === "offers");
  assert.equal(failedOffers?.status, "failed");
  assert.match(String(failedOffers?.error || ""), /offers timeout/i);

  for (const table of ["underwriting", "contracts", "buyer_match", "agents", "templates", "send_queue", "ai_conversation_brain"]) {
    const status = payload.source_health.find((entry) => entry.table === table);
    assert.equal(status?.status, "degraded");
    assert.equal(status?.count, 0);
  }

  const missingBlocks = new Set(payload.missingData.map((entry) => entry.block));
  for (const block of ["offers", "underwriting", "contracts", "buyer_match", "agents", "templates", "send_queue", "ai_conversation_brain"]) {
    assert.equal(missingBlocks.has(block), true, `${block} should be listed in missingData`);
  }

  const safeJson = JSON.stringify(payload?.copilot_context?.aiSafeContext || {});
  assert.doesNotMatch(safeJson, /walkaway_internal/i);
  assert.doesNotMatch(safeJson, /internal_valuation/i);
  assert.doesNotMatch(safeJson, /"mao"/i);

  assert.deepEqual(payload.seller_facing_context, payload.copilot_context.aiSafeContext);
  assert.equal(
    JSON.stringify(payload.seller_facing_context).includes("internalOnlyContext"),
    false
  );
});
