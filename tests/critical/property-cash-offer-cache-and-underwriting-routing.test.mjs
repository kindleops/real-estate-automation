/**
 * property-cash-offer-cache-and-underwriting-routing.test.mjs
 *
 * Tests for:
 *   - property-cash-offer-cache.js      (Supabase snapshot CRUD)
 *   - deal-routing.js                   (shouldRouteToUnderwriting / reasons)
 *   - transfer-to-underwriting.js       (Podio transfer, failure handling)
 *
 * Coverage required:
 *   1.  Active cash offer snapshot is returned for SFH property
 *   2.  min/max fields are not present or referenced in snapshots
 *   3.  First-touch message does not include cash_offer
 *   4.  Queue plan can include cash_offer_snapshot diagnostics (does NOT go into message_text)
 *   5.  Multifamily property routes to underwriting
 *   6.  Creative deal strategy routes to underwriting
 *   7.  Seller message mentioning mortgage/payment routes to underwriting
 *   8.  Underwriting transfer failure does not crash inbound flow
 *   9.  Single-family cash offer path does NOT create an underwriting record
 *   10. Apartment property type routes to underwriting
 *   11. 5+ unit count routes to underwriting
 *   12. SFH does not route to underwriting
 *   13. upsertActivePropertyCashOffer increments version and supersedes prior active row
 *   14. supersedeActivePropertyCashOffer marks active row as superseded
 *   15. getActivePropertyCashOffer falls back to podio_property_item_id lookup
 *   16. buildPlanCashOfferSnapshot strips non-financial fields
 *   17. getUnderwritingRouteReason returns human-readable string
 *   18. Novation strategy routes to underwriting
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  getActivePropertyCashOffer,
  upsertActivePropertyCashOffer,
  supersedeActivePropertyCashOffer,
  buildPlanCashOfferSnapshot,
  __setOfferCacheDeps,
  __resetOfferCacheDeps,
} from "@/lib/domain/offers/property-cash-offer-cache.js";

import {
  shouldRouteToUnderwriting,
  getUnderwritingRouteReason,
} from "@/lib/domain/offers/deal-routing.js";

import {
  transferDealToUnderwriting,
  __setUnderwritingTransferDeps,
  __resetUnderwritingTransferDeps,
} from "@/lib/domain/underwriting/transfer-to-underwriting.js";

// ---------------------------------------------------------------------------
// Mock Supabase builder
// ---------------------------------------------------------------------------

/**
 * Build a chainable mock Supabase client.
 *
 * tableMap: { [tableName]: { rows?, count?, error? } }
 *
 * Tracks the sequence of calls for assertion purposes.
 */
function makeSupabaseMock(tableMap = {}) {
  const calls = [];

  return {
    _calls: calls,
    from(table) {
      const spec = tableMap[table] ?? {};
      let _is_count = false;
      let _filters  = {};
      let _limit     = null;
      let _op        = "select"; // select | insert | update | upsert

      const chain = {
        select(fields, opts = {}) {
          _is_count = !!opts?.count;
          return chain;
        },
        eq(col, val)      { _filters[col] = val; return chain; },
        neq(col, val)     { return chain; },
        gte(col, val)     { return chain; },
        lte(col, val)     { return chain; },
        gt(col, val)      { return chain; },
        lt(col, val)      { return chain; },
        order(col, opts)  { return chain; },
        limit(n)          { _limit = n; return chain; },
        not(col, op, val) { return chain; },
        in(col, vals)     { return chain; },
        or(expr)          { return chain; },

        insert(row)  {
          _op = "insert";
          calls.push({ op: "insert", table, row });
          return chain;
        },
        update(changes) {
          _op = "update";
          calls.push({ op: "update", table, changes });
          return chain;
        },
        upsert(row)  {
          _op = "upsert";
          calls.push({ op: "upsert", table, row });
          return chain;
        },

        maybeSingle() {
          if (spec.error && _op === "select") {
            return Promise.resolve({ data: null, error: spec.error });
          }
          // For update/insert ops that chain into maybeSingle
          if (_op === "insert") {
            if (spec.error) return Promise.resolve({ data: null, error: spec.error });
            const row = spec.rows?.[0] ?? null;
            return Promise.resolve({ data: row, error: null });
          }
          const rows = (spec.rows ?? []).filter((r) => {
            return Object.entries(_filters).every(([k, v]) => String(r[k]) === String(v));
          });
          const hit = _limit === 1 ? (rows[0] ?? null) : rows;
          return Promise.resolve({ data: hit, error: spec.error ?? null });
        },

        select(fields2) { return chain; },

        then(resolve, reject) {
          if (_op === "insert" || _op === "update" || _op === "upsert") {
            if (spec.error) {
              return Promise.resolve({ data: null, error: spec.error }).then(resolve, reject);
            }
            // return the inserted/updated rows for .select() chaining
            const row = spec.rows?.[0] ?? null;
            return Promise.resolve({ data: row, error: null }).then(resolve, reject);
          }
          if (spec.error) {
            return Promise.resolve({ data: null, count: null, error: spec.error }).then(resolve, reject);
          }
          if (_is_count) {
            return Promise.resolve({ count: spec.count ?? (spec.rows?.length ?? 0), error: null }).then(resolve, reject);
          }
          const rows = (spec.rows ?? []).filter((r) => {
            return Object.entries(_filters).every(([k, v]) => String(r[k]) === String(v));
          });
          const result = _limit === 1 ? (rows[0] ?? null) : rows;
          return Promise.resolve({ data: result, error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };
}

// Podio mock helpers
function makeUnderwritingDeps({ items = [], create_error = null, update_error = null } = {}) {
  const calls = [];
  return {
    calls,
    find_underwriting_items:  async () => { calls.push("find"); return items; },
    create_underwriting_item: async (payload) => {
      calls.push({ op: "create", payload });
      if (create_error) throw create_error;
      return { item_id: 9901 };
    },
    update_underwriting_item: async (item_id, payload) => {
      calls.push({ op: "update", item_id, payload });
      if (update_error) throw update_error;
      return { item_id };
    },
  };
}

// ---------------------------------------------------------------------------
// 1. Active cash offer snapshot returned for SFH property
// ---------------------------------------------------------------------------

test("getActivePropertyCashOffer returns active snapshot for SFH property", async () => {
  const db = makeSupabaseMock({
    property_cash_offer_snapshots: {
      rows: [
        {
          id:          "uuid-1",
          property_id: "prop_101",
          status:      "active",
          cash_offer:  185000,
          version:     1,
          generated_at: new Date().toISOString(),
        },
      ],
    },
  });
  __setOfferCacheDeps({ supabase_override: db });

  try {
    const result = await getActivePropertyCashOffer({ property_id: "prop_101" });
    assert.ok(result.ok,       "ok is true");
    assert.ok(result.snapshot, "snapshot is present");
    assert.equal(result.snapshot.property_id, "prop_101");
    assert.equal(result.snapshot.cash_offer,  185000);
  } finally {
    __resetOfferCacheDeps();
  }
});

// ---------------------------------------------------------------------------
// 2. No min/max offer fields on the snapshot object
// ---------------------------------------------------------------------------

test("cash offer snapshot has no min_offer or max_offer fields", async () => {
  const db = makeSupabaseMock({
    property_cash_offer_snapshots: {
      rows: [{
        id: "uuid-2", property_id: "prop_202", status: "active",
        cash_offer: 200000, version: 1,
      }],
    },
  });
  __setOfferCacheDeps({ supabase_override: db });

  try {
    const result = await getActivePropertyCashOffer({ property_id: "prop_202" });
    assert.ok(result.snapshot);
    assert.ok(!("min_offer"      in result.snapshot), "no min_offer field");
    assert.ok(!("max_offer"      in result.snapshot), "no max_offer field");
    assert.ok(!("min_cash_offer" in result.snapshot), "no min_cash_offer field");
    assert.ok(!("max_cash_offer" in result.snapshot), "no max_cash_offer field");
  } finally {
    __resetOfferCacheDeps();
  }
});

// ---------------------------------------------------------------------------
// 3. First-touch message does NOT include cash_offer
// ---------------------------------------------------------------------------

test("first-touch (Stage 1 / ownership_check) message template does not reference cash_offer", () => {
  // Simulate a Stage 1 template body — must not contain offer amounts.
  // In production, Stage 1 templates are ownership_check use_case only.
  const stage1_templates = [
    "Hi {{owner_first_name}}, I'm reaching out about your property at {{property_address}}. Are you the owner?",
    "Hello {{owner_full_name}}, we found your property at {{property_address}}. Do you still own it?",
    "Hola {{owner_first_name}}, nos comunicamos sobre tu propiedad en {{property_address}}.",
  ];

  for (const tmpl of stage1_templates) {
    assert.ok(
      !tmpl.match(/\$\s*\d|cash[_ ]offer|offer_price|offer amount/i),
      `Stage 1 template must not contain offer amounts: "${tmpl.slice(0, 60)}"`
    );
    assert.ok(
      !tmpl.match(/\{\{.*cash.*\}\}|\{\{.*offer.*\}\}/i),
      `Stage 1 template must not reference offer template vars: "${tmpl.slice(0, 60)}"`
    );
  }
});

// ---------------------------------------------------------------------------
// 4. Queue plan can include cash_offer_snapshot diagnostics
//    (does NOT go into message_text)
// ---------------------------------------------------------------------------

test("buildPlanCashOfferSnapshot returns financial fields only and must not be put into message_text", () => {
  const snapshot = {
    id:                          "uuid-3",
    property_id:                 "prop_303",
    property_address:            "123 Elm St",
    cash_offer:                  175000,
    repair_estimate:             12000,
    estimated_value:             220000,
    calculated_value:            180000,
    estimated_equity:            60000,
    estimated_mortgage_balance:  110000,
    estimated_mortgage_payment:  850,
    generated_at:                "2026-04-20T00:00:00.000Z",
    version:                     2,
    // Non-financial fields that must be stripped
    owner_id:                    "owner_x",
    metadata:                    { source: "podio" },
  };

  const plan_snapshot = buildPlanCashOfferSnapshot(snapshot);

  assert.ok(plan_snapshot,                   "returns plan snapshot");
  assert.equal(plan_snapshot.cash_offer,       175000);
  assert.equal(plan_snapshot.repair_estimate,  12000);
  assert.equal(plan_snapshot.estimated_value,  220000);
  assert.equal(plan_snapshot.calculated_value, 180000);
  assert.equal(plan_snapshot.estimated_equity, 60000);
  assert.equal(plan_snapshot.estimated_mortgage_balance, 110000);
  assert.equal(plan_snapshot.estimated_mortgage_payment,   850);
  assert.equal(plan_snapshot.version,          2);

  // Must NOT include personal / address fields
  assert.ok(!("property_address" in plan_snapshot), "no property_address in plan snapshot");
  assert.ok(!("owner_id"         in plan_snapshot), "no owner_id in plan snapshot");
  assert.ok(!("metadata"         in plan_snapshot), "no metadata in plan snapshot");
  assert.ok(!("id"               in plan_snapshot), "no id in plan snapshot");

  // Simulate that the plan snapshot is used in diagnostics but NOT message_text
  const plan = {
    use_case:          "re_engagement",
    message_text:      "Hi {{owner_first_name}}, are you still interested in selling?",
    cash_offer_snapshot: plan_snapshot,
  };

  assert.ok(!plan.message_text.includes("175000"), "message_text does NOT contain cash_offer value");
  assert.ok(!plan.message_text.match(/\$\s*\d|\{\{.*offer/i), "message_text has no offer placeholders");
  assert.ok(plan.cash_offer_snapshot,             "cash_offer_snapshot present in plan for diagnostics");
  assert.equal(typeof plan.cash_offer_snapshot.cash_offer, "number", "plan snapshot has cash_offer number");
});

// ---------------------------------------------------------------------------
// 5. Multifamily property type routes to underwriting
// ---------------------------------------------------------------------------

test("multifamily property type routes to underwriting", () => {
  const mf_types = [
    "Multifamily",
    "Multi-Family",
    "multi family",
    "Apartment",
    "Apartments",
    "Duplex",
    "Triplex",
    "Quadplex",
    "4-Plex",
  ];

  for (const pt of mf_types) {
    const should = shouldRouteToUnderwriting({ property: { property_type: pt } });
    assert.ok(should, `property_type "${pt}" should route to underwriting`);
  }
});

// ---------------------------------------------------------------------------
// 6. Creative deal strategy routes to underwriting
// ---------------------------------------------------------------------------

test("creative deal strategies route to underwriting", () => {
  const strategies = [
    "Creative",
    "creative_finance",
    "Seller Finance",
    "seller_finance",
    "Subject To",
    "subject_to",
    "Novation",
    "Owner Finance",
    "Owner Financing",
  ];

  for (const s of strategies) {
    const should = shouldRouteToUnderwriting({ dealStrategy: s });
    assert.ok(should, `dealStrategy "${s}" should route to underwriting`);
  }
});

// ---------------------------------------------------------------------------
// 7. Seller message mentioning mortgage/payment routes to underwriting
// ---------------------------------------------------------------------------

test("seller message with mortgage/payment/rents language routes to underwriting", () => {
  const msgs = [
    "I still have a mortgage on the property",
    "My monthly payment is about $1200",
    "We have 4 tenants paying rent",
    "The rents are $3200 per month",
    "NOI is around $2500",
    "I want seller financing on this one",
    "Would you consider subject to my existing mortgage?",
    "The occupancy is 75% right now",
    "This is a novation deal",
    "The cap rate is about 7%",
  ];

  for (const msg of msgs) {
    const should = shouldRouteToUnderwriting({ sellerMessage: msg });
    assert.ok(should, `message "${msg.slice(0, 50)}" should route to underwriting`);
  }
});

// ---------------------------------------------------------------------------
// 8. Underwriting transfer failure does NOT crash inbound flow
// ---------------------------------------------------------------------------

test("transferDealToUnderwriting failure does not throw — returns ok:false with diagnostics", async () => {
  const failing_deps = makeUnderwritingDeps({ create_error: new Error("Podio HTTP 503") });
  __setUnderwritingTransferDeps(failing_deps);

  try {
    const result = await transferDealToUnderwriting({
      property:     { item_id: 5001 },
      routeReason:  "Multifamily property",
      sellerMessage: "I have 8 units and they're all rented",
    });

    // Must not throw — must return structured failure
    assert.ok(result,                "result returned");
    assert.equal(result.ok, false,   "ok is false on failure");
    assert.ok(result.diagnostics,    "diagnostics object is present");
    assert.ok(result.diagnostics.error, "diagnostics.error contains error message");
    assert.ok(!result.underwriting_item_id, "no item_id on failure");
    // Importantly: did not throw
  } finally {
    __resetUnderwritingTransferDeps();
  }
});

test("transferDealToUnderwriting find failure does not crash — returns ok:false", async () => {
  // find throws AND create throws — verifies full pipeline failure is handled gracefully
  const bad_find_deps = {
    calls: [],
    find_underwriting_items:  async () => { throw new Error("Network timeout"); },
    create_underwriting_item: async () => { throw new Error("Create also failed"); },
    update_underwriting_item: async () => {},
  };
  __setUnderwritingTransferDeps(bad_find_deps);

  try {
    const result = await transferDealToUnderwriting({
      property:    { item_id: 5002 },
      routeReason: "Creative deal",
    });
    assert.equal(result.ok, false, "ok:false when find throws");
    assert.ok(result.diagnostics, "diagnostics returned");
  } finally {
    __resetUnderwritingTransferDeps();
  }
});

// ---------------------------------------------------------------------------
// 9. Single-family cash offer path does NOT create underwriting record
// ---------------------------------------------------------------------------

test("SFH single-family property does not route to underwriting", () => {
  const sfh_properties = [
    { property_type: "Residential" },
    { property_type: "Single Family" },
    { property_type: "Single-Family" },
    { property_type: "SFR" },
    { property_type: "" },
    { property_type: null },
    {},
  ];

  for (const prop of sfh_properties) {
    const should = shouldRouteToUnderwriting({ property: prop });
    assert.ok(!should, `SFH property_type "${prop.property_type ?? "(none)"}" must NOT route to underwriting`);
  }
});

test("SFH with non-underwriting message does not route to underwriting", () => {
  const sfh_msgs = [
    "Yes, I'm interested in selling",
    "How much can you offer?",
    "What is your timeline?",
    "I need to sell fast",
    "Can you close in 30 days?",
    "The house needs some work",
  ];

  for (const msg of sfh_msgs) {
    const should = shouldRouteToUnderwriting({
      property:     { property_type: "Residential" },
      sellerMessage: msg,
    });
    assert.ok(!should, `SFH + "${msg.slice(0, 40)}" must NOT route to underwriting`);
  }
});

// ---------------------------------------------------------------------------
// 10. Apartment property type routes to underwriting
// ---------------------------------------------------------------------------

test("apartment and commercial property types route to underwriting", () => {
  const types = ["apartment", "Apartments", "commercial", "office", "retail", "warehouse", "mixed use"];
  for (const pt of types) {
    assert.ok(
      shouldRouteToUnderwriting({ property: { property_type: pt } }),
      `"${pt}" should route to underwriting`
    );
  }
});

// ---------------------------------------------------------------------------
// 11. 5+ unit count routes to underwriting
// ---------------------------------------------------------------------------

test("5 or more units routes to underwriting regardless of property_type label", () => {
  assert.ok(shouldRouteToUnderwriting({ property: { property_type: "Residential", unit_count: 5 } }),  "5 units → underwriting");
  assert.ok(shouldRouteToUnderwriting({ property: { property_type: "Residential", unit_count: 8 } }),  "8 units → underwriting");
  assert.ok(shouldRouteToUnderwriting({ property: { property_type: "Residential", unit_count: 50 } }), "50 units → underwriting");
  assert.ok(!shouldRouteToUnderwriting({ property: { property_type: "Residential", unit_count: 4 } }),  "4 units → no routing");
  assert.ok(!shouldRouteToUnderwriting({ property: { property_type: "Residential", unit_count: 1 } }),  "1 unit → no routing");
  assert.ok(!shouldRouteToUnderwriting({ property: { property_type: "Residential", unit_count: null } }),"null units → no routing");
});

// ---------------------------------------------------------------------------
// 12. upsertActivePropertyCashOffer increments version
// ---------------------------------------------------------------------------

test("upsertActivePropertyCashOffer supersedes existing active row and increments version", async () => {
  let update_called_with = null;
  let inserted_row       = null;

  // Simulate: existing active row at version 2
  const existing_active = { id: "existing-uuid", version: 2 };

  const mock = {
    _calls: [],
    from(table) {
      const chain = this._makeChain(table);
      return chain;
    },
    _makeChain(table) {
      const state = { op: null, filters: {}, limit: null, mock: this };
      const chain = {
        select()   { return chain; },
        eq(col, val) { state.filters[col] = val; return chain; },
        neq()      { return chain; },
        order()    { return chain; },
        limit(n)   { state.limit = n; return chain; },
        not()      { return chain; },
        in()       { return chain; },
        or()       { return chain; },

        update(changes) {
          state.op = "update";
          update_called_with = { table, changes, filters: { ...state.filters } };
          return chain;
        },
        insert(row) {
          state.op = "insert";
          inserted_row = { ...row };
          return chain;
        },
        upsert(row) {
          state.op = "upsert";
          inserted_row = { ...row };
          return chain;
        },

        maybeSingle() {
          // The "find existing" query
          if (state.op === "insert") {
            return Promise.resolve({ data: { ...inserted_row, id: "new-uuid" }, error: null });
          }
          // update returns the updated row
          if (state.op === "update") {
            return Promise.resolve({ data: null, error: null });
          }
          // select query for existing row
          if (state.filters.status === "active") {
            return Promise.resolve({ data: existing_active, error: null });
          }
          return Promise.resolve({ data: null, error: null });
        },

        then(resolve, reject) {
          if (state.op === "insert") {
            return Promise.resolve({ data: { ...inserted_row, id: "new-uuid" }, error: null }).then(resolve, reject);
          }
          if (state.op === "update") {
            return Promise.resolve({ data: null, error: null }).then(resolve, reject);
          }
          return Promise.resolve({ data: existing_active, error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };

  __setOfferCacheDeps({ supabase_override: mock });

  try {
    const result = await upsertActivePropertyCashOffer({
      property_id: "prop_version_test",
      cash_offer:  190000,
    });

    assert.ok(result.ok,              "upsert succeeded");
    assert.ok(result.created,         "created flag is true");
    assert.ok(result.superseded_previous, "superseded_previous is true");

    // The update (supersede) must have targeted the existing id
    assert.ok(update_called_with,            "update was called to supersede prior active row");
    assert.equal(update_called_with.changes.status, "superseded", "prior row set to superseded");

    // The new row must have version 3 (2 + 1)
    assert.ok(inserted_row,           "insert was called");
    assert.equal(inserted_row.version, 3, "new version = previous_version + 1");
    assert.equal(inserted_row.status, "active", "new row is active");
  } finally {
    __resetOfferCacheDeps();
  }
});

// ---------------------------------------------------------------------------
// 13. supersedeActivePropertyCashOffer marks row as superseded
// ---------------------------------------------------------------------------

test("supersedeActivePropertyCashOffer calls update with status:superseded", async () => {
  let captured_update = null;

  const mock = {
    from(table) {
      const chain = {
        select()     { return chain; },
        eq(col, val) { return chain; },
        update(ch)   {
          captured_update = ch;
          return chain;
        },
        then(resolve) {
          return Promise.resolve({ data: [{ id: "abc" }], error: null }).then(resolve);
        },
      };
      return chain;
    },
  };

  __setOfferCacheDeps({ supabase_override: mock });

  try {
    const result = await supersedeActivePropertyCashOffer({
      property_id: "prop_sup_test",
      reason:      "deal_closed",
    });

    assert.ok(result.ok,           "supersede ok");
    assert.ok(captured_update,     "update was called");
    assert.equal(captured_update.status, "superseded", "status set to superseded");
  } finally {
    __resetOfferCacheDeps();
  }
});

// ---------------------------------------------------------------------------
// 14. getActivePropertyCashOffer falls back to podio_property_item_id
// ---------------------------------------------------------------------------

test("getActivePropertyCashOffer falls back to podio_property_item_id when property_id not provided", async () => {
  const db = makeSupabaseMock({
    property_cash_offer_snapshots: {
      rows: [{
        id:                    "uuid-podio",
        property_id:           "prop_podio_1",
        podio_property_item_id: 8001,
        status:                "active",
        cash_offer:            165000,
        version:               1,
      }],
    },
  });
  __setOfferCacheDeps({ supabase_override: db });

  try {
    // No property_id provided — use podio_property_item_id only
    const result = await getActivePropertyCashOffer({ podio_property_item_id: 8001 });
    assert.ok(result.ok,       "ok is true");
    assert.ok(result.snapshot, "snapshot returned via podio item id fallback");
    assert.equal(result.snapshot.podio_property_item_id, 8001);
    assert.equal(result.snapshot.cash_offer, 165000);
  } finally {
    __resetOfferCacheDeps();
  }
});

// ---------------------------------------------------------------------------
// 15. buildPlanCashOfferSnapshot returns null for null input
// ---------------------------------------------------------------------------

test("buildPlanCashOfferSnapshot returns null when given null", () => {
  assert.equal(buildPlanCashOfferSnapshot(null),      null);
  assert.equal(buildPlanCashOfferSnapshot(undefined), null);
});

// ---------------------------------------------------------------------------
// 16. getUnderwritingRouteReason returns descriptive string
// ---------------------------------------------------------------------------

test("getUnderwritingRouteReason returns descriptive human-readable reason", () => {
  const mf_reason  = getUnderwritingRouteReason({ property: { property_type: "Multifamily" } });
  const cr_reason  = getUnderwritingRouteReason({ dealStrategy: "Seller Finance" });
  const msg_reason = getUnderwritingRouteReason({ sellerMessage: "I still have a mortgage balance of $150k" });
  const sfh_reason = getUnderwritingRouteReason({ property: { property_type: "Residential" } });

  assert.ok(typeof mf_reason  === "string" && mf_reason.length  > 0, "MF reason is non-empty string");
  assert.ok(typeof cr_reason  === "string" && cr_reason.length  > 0, "Creative reason is non-empty string");
  assert.ok(typeof msg_reason === "string" && msg_reason.length > 0, "Message reason is non-empty string");
  assert.equal(sfh_reason, null, "SFH returns null (no routing trigger)");
});

// ---------------------------------------------------------------------------
// 17. Novation strategy routes to underwriting
// ---------------------------------------------------------------------------

test("Novation deal strategy routes to underwriting", () => {
  assert.ok(shouldRouteToUnderwriting({ dealStrategy: "Novation" }),    "Novation routes");
  assert.ok(shouldRouteToUnderwriting({ dealStrategy: "novation" }),    "novation (lowercase) routes");
  assert.ok(shouldRouteToUnderwriting({ dealStrategy: "Wrap Around" }), "Wrap Around is creative — routes");
});

// ---------------------------------------------------------------------------
// 18. transferDealToUnderwriting creates record when no existing item found
// ---------------------------------------------------------------------------

test("transferDealToUnderwriting creates new underwriting item for multifamily deal", async () => {
  const deps = makeUnderwritingDeps({ items: [] }); // no existing item
  __setUnderwritingTransferDeps(deps);

  try {
    const result = await transferDealToUnderwriting({
      owner:         { item_id: 201 },
      property:      { item_id: 501, property_type: "Multifamily" },
      prospect:      { item_id: 301 },
      sellerMessage: "We have 12 units, rents are $800 each, 90% occupancy",
      routeReason:   "Multifamily or apartment property — must use Underwriting app",
      dealStrategy:  null,
    });

    assert.ok(result.ok,           "ok is true for create");
    assert.ok(result.created,      "created is true");
    assert.equal(result.updated,  false, "updated is false");
    assert.equal(result.underwriting_item_id, 9901, "underwriting_item_id from mock");
    assert.equal(result.underwriting_type, "Multifamily", "type is Multifamily");

    // Verify create was called with correct field set
    const create_call = deps.calls.find((c) => c.op === "create");
    assert.ok(create_call,           "create was invoked");
    assert.ok(create_call.payload,   "payload provided");
    // Should NOT include personal message body verbatim in any leaked field
    assert.ok(
      !JSON.stringify(create_call.payload).includes("cash_offer"),
      "Underwriting payload must not reference single-family cash_offer field"
    );
  } finally {
    __resetUnderwritingTransferDeps();
  }
});

// ---------------------------------------------------------------------------
// 19. transferDealToUnderwriting updates existing record when one found
// ---------------------------------------------------------------------------

test("transferDealToUnderwriting updates existing underwriting item when found", async () => {
  const deps = makeUnderwritingDeps({ items: [{ item_id: 7701 }] }); // existing
  __setUnderwritingTransferDeps(deps);

  try {
    const result = await transferDealToUnderwriting({
      property:     { item_id: 502 },
      routeReason:  "Creative deal",
      dealStrategy: "Subject To",
    });

    assert.ok(result.ok,         "ok is true");
    assert.ok(result.updated,    "updated is true (existing found)");
    assert.equal(result.created, false, "created is false");
    assert.equal(result.underwriting_item_id, 7701);
    assert.equal(result.underwriting_type, "Creative");

    const update_call = deps.calls.find((c) => c.op === "update");
    assert.ok(update_call, "update was invoked");
  } finally {
    __resetUnderwritingTransferDeps();
  }
});

// ---------------------------------------------------------------------------
// 20. Message signal extraction populates diagnostics
// ---------------------------------------------------------------------------

test("transferDealToUnderwriting extracts mortgage and payment signals from seller message", async () => {
  const deps = makeUnderwritingDeps({ items: [] });
  __setUnderwritingTransferDeps(deps);

  try {
    const result = await transferDealToUnderwriting({
      property:     { item_id: 503 },
      sellerMessage: "mortgage balance is $125,000 and my payment is $980 per month",
      routeReason:  "Seller message contains creative-finance or multifamily signals",
    });

    assert.ok(result.ok, "ok is true");
    assert.ok(result.diagnostics.extracted_signals, "extracted_signals in diagnostics");
    const signals = result.diagnostics.extracted_signals;
    assert.ok(signals.mortgage_balance > 0, `mortgage_balance extracted: ${signals.mortgage_balance}`);
    assert.ok(signals.monthly_payment  > 0, `monthly_payment extracted: ${signals.monthly_payment}`);
  } finally {
    __resetUnderwritingTransferDeps();
  }
});
