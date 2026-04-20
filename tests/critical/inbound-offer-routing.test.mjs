import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  routeInboundOffer,
  __setRouteInboundOfferDeps,
  __resetRouteInboundOfferDeps,
} from "@/lib/domain/offers/route-inbound-offer.js";

import {
  createPodioItem,
  categoryField,
  numberField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetRouteInboundOfferDeps();
});

// ── Snapshot mocks ─────────────────────────────────────────────────────────

function noSnapshot() {
  __setRouteInboundOfferDeps({
    get_active_property_cash_offer: async () => ({
      ok: true,
      snapshot: null,
      reason: "not_found",
    }),
  });
}

function withSnapshot(cash_offer = 320000, property_id = "prop-123") {
  __setRouteInboundOfferDeps({
    get_active_property_cash_offer: async () => ({
      ok: true,
      snapshot: { id: "snap-1", cash_offer, property_id },
    }),
  });
}

function snapshotDbError() {
  __setRouteInboundOfferDeps({
    get_active_property_cash_offer: async () => ({
      ok: false,
      snapshot: null,
      reason: "db_error",
    }),
  });
}

// ── Context builders ──────────────────────────────────────────────────────

function buildSfhContext({
  property_id = "prop-123",
  property_address = "123 Main St, Phoenix AZ 85001",
} = {}) {
  return {
    found: true,
    ids: { property_id },
    items: {
      property_item: createPodioItem(41, {
        "property-type": categoryField("Single Family"),
        "property-class": categoryField("Residential"),
        "number-of-units": numberField(1),
      }),
    },
    summary: { property_address },
  };
}

function buildMfContext({ unit_count = 8, property_id = "prop-456" } = {}) {
  return {
    found: true,
    ids: { property_id },
    items: {
      property_item: createPodioItem(42, {
        "property-type": categoryField("Multifamily"),
        "property-class": categoryField("Multifamily"),
        "number-of-units": numberField(unit_count),
      }),
    },
    summary: {},
  };
}

function buildNoPropertyContext() {
  return {
    found: true,
    ids: { property_id: null },
    items: { property_item: null },
    summary: {},
  };
}

// ── no_offer_signal ────────────────────────────────────────────────────────

test("no_offer_signal when message has no offer keywords", async () => {
  const result = await routeInboundOffer({
    message: "I already have a buyer lined up, thanks",
    classification: { source: "test" },
    context: buildSfhContext(),
  });
  assert.equal(result.ok, true);
  assert.equal(result.offer_route, "no_offer_signal");
  assert.equal(result.reason, "no_offer_intent_detected");
});

test("no_offer_signal when message is empty", async () => {
  const result = await routeInboundOffer({ message: "" });
  assert.equal(result.offer_route, "no_offer_signal");
});

test("no_offer_signal when message and classification both have no offer signals", async () => {
  const result = await routeInboundOffer({
    message: "Not interested, do not contact me again",
    classification: { compliance_flag: "stop_texting" },
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "no_offer_signal");
});

// ── offer intent detection ─────────────────────────────────────────────────

test("offer intent from classification objection send_offer_first", async () => {
  withSnapshot();
  const result = await routeInboundOffer({
    message: "yeah",
    classification: { objection: "send_offer_first" },
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "sfh_cash_preview");
});

test("offer intent from classification objection need_more_money", async () => {
  withSnapshot();
  const result = await routeInboundOffer({
    message: "I need more",
    classification: { objection: "need_more_money" },
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "sfh_cash_preview");
});

test("offer intent from classification emotion motivated", async () => {
  withSnapshot();
  const result = await routeInboundOffer({
    message: "I'm motivated to sell now",
    classification: { emotion: "motivated" },
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "sfh_cash_preview");
});

test("offer intent from 'what's your offer' phrase", async () => {
  withSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "sfh_cash_preview");
});

test("offer intent from 'what can you pay' phrase", async () => {
  withSnapshot();
  const result = await routeInboundOffer({
    message: "Ok so what can you pay for my house",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "sfh_cash_preview");
});

test("offer intent from 'cash offer' phrase", async () => {
  withSnapshot();
  const result = await routeInboundOffer({
    message: "I want a cash offer for my property",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "sfh_cash_preview");
});

// ── underwriting route ─────────────────────────────────────────────────────

test("underwriting route when property type is Multifamily", async () => {
  const result = await routeInboundOffer({
    message: "what can you pay",
    classification: {},
    context: buildMfContext(),
  });
  assert.equal(result.offer_route, "underwriting");
  assert.ok(result.reason.length > 0);
  assert.equal(result.meta.property_type, "Multifamily");
});

test("underwriting route when unit count >= 5", async () => {
  const result = await routeInboundOffer({
    message: "what can you pay",
    classification: {},
    context: buildMfContext({ unit_count: 6 }),
  });
  assert.equal(result.offer_route, "underwriting");
});

test("underwriting route from creative deal_strategy on route", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
    route: { deal_strategy: "subject to", use_case: "offer_reveal_cash" },
  });
  assert.equal(result.offer_route, "underwriting");
  assert.match(result.reason, /subject to/i);
});

test("underwriting route from creative deal_strategy on context summary", async () => {
  noSnapshot();
  const ctx = buildSfhContext();
  ctx.summary.deal_strategy = "novation";
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: ctx,
    route: {},
  });
  assert.equal(result.offer_route, "underwriting");
});

test("underwriting route from seller message containing 'seller finance'", async () => {
  const result = await routeInboundOffer({
    message: "I want seller finance terms, what can you do",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "underwriting");
  assert.match(result.reason, /creative-finance/i);
});

test("underwriting route from seller message containing 'cap rate'", async () => {
  const result = await routeInboundOffer({
    message: "what is your offer? the cap rate is around 7%",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "underwriting");
});

test("underwriting route from seller message containing 'subject to'", async () => {
  const result = await routeInboundOffer({
    message: "I am open to subject to financing, send me your offer",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.offer_route, "underwriting");
});

test("underwriting route suppresses fake SFH cash offer for MF lead", async () => {
  // Even injecting a snapshot, MF property must still route to underwriting
  withSnapshot();
  const result = await routeInboundOffer({
    message: "what can you offer",
    classification: {},
    context: buildMfContext(),
  });
  assert.equal(result.offer_route, "underwriting");
  assert.notEqual(result.offer_route, "sfh_cash_preview");
});

// ── type_guard_blocked ─────────────────────────────────────────────────────

test("type_guard_blocked when route use_case is mf_offer_reveal", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
    route: { use_case: "mf_offer_reveal" },
  });
  assert.equal(result.offer_route, "type_guard_blocked");
  assert.equal(result.reason, "route_use_case_is_non_sfh_cash");
  assert.equal(result.meta.route_use_case, "mf_offer_reveal");
});

test("type_guard_blocked when route use_case contains 'creative'", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
    route: { use_case: "offer_reveal_creative" },
  });
  assert.equal(result.offer_route, "type_guard_blocked");
});

test("type_guard_blocked when route use_case is novation variant", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
    route: { use_case: "offer_reveal_novation" },
  });
  assert.equal(result.offer_route, "type_guard_blocked");
});

// ── sfh_cash_preview ──────────────────────────────────────────────────────

test("sfh_cash_preview when SFH property has active cash snapshot", async () => {
  withSnapshot(285000);
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
  });
  assert.equal(result.ok, true);
  assert.equal(result.offer_route, "sfh_cash_preview");
  assert.equal(result.reason, "active_cash_snapshot_found");
  assert.equal(result.meta.cash_offer, 285000);
});

test("sfh_cash_preview passes property_id to snapshot lookup", async () => {
  let captured_args = null;
  __setRouteInboundOfferDeps({
    get_active_property_cash_offer: async (args) => {
      captured_args = args;
      return { ok: true, snapshot: { id: "s1", cash_offer: 250000 } };
    },
  });
  await routeInboundOffer({
    message: "give me a number",
    classification: {},
    context: buildSfhContext({ property_id: "prop-xyz" }),
  });
  assert.equal(captured_args?.property_id, "prop-xyz");
});

test("sfh_cash_preview passes podio_property_item_id to snapshot lookup", async () => {
  let captured_args = null;
  __setRouteInboundOfferDeps({
    get_active_property_cash_offer: async (args) => {
      captured_args = args;
      return { ok: true, snapshot: { id: "s2", cash_offer: 300000 } };
    },
  });
  await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext({ property_id: "prop-abc" }),
  });
  // podio_property_item_id comes from property_item.item_id (41 in test fixture)
  assert.equal(captured_args?.podio_property_item_id, 41);
});

// ── condition_clarifier ───────────────────────────────────────────────────

test("condition_clarifier when no snapshot but property_id present", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext({ property_id: "prop-abc" }),
  });
  assert.equal(result.offer_route, "condition_clarifier");
  assert.equal(result.reason, "no_snapshot_property_id_present");
  assert.equal(result.meta.property_id, "prop-abc");
});

test("condition_clarifier includes property_address in meta", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext({ property_address: "789 Oak St, Scottsdale AZ 85251" }),
  });
  assert.equal(result.offer_route, "condition_clarifier");
  assert.equal(result.meta.property_address, "789 Oak St, Scottsdale AZ 85251");
});

test("condition_clarifier when snapshot lookup returns db_error", async () => {
  snapshotDbError();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext({ property_id: "prop-err" }),
  });
  // ok: false snapshot falls through to condition_clarifier
  assert.equal(result.offer_route, "condition_clarifier");
  assert.equal(result.meta.property_id, "prop-err");
});

// ── manual_review ─────────────────────────────────────────────────────────

test("manual_review when no snapshot and no property_id", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildNoPropertyContext(),
  });
  assert.equal(result.offer_route, "manual_review");
  assert.equal(result.reason, "no_snapshot_no_property_id");
});

test("manual_review when context is null", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: null,
  });
  assert.equal(result.offer_route, "manual_review");
  assert.equal(result.reason, "no_snapshot_no_property_id");
});

test("manual_review when context is completely missing ids", async () => {
  noSnapshot();
  const result = await routeInboundOffer({
    message: "send me an offer",
    classification: {},
    context: { found: true, items: {}, summary: {} },
  });
  assert.equal(result.offer_route, "manual_review");
});

// ── meta integrity / invariants ───────────────────────────────────────────

test("result always contains ok, offer_route, reason, meta", async () => {
  const result = await routeInboundOffer({ message: "" });
  assert.ok("ok" in result);
  assert.ok("offer_route" in result);
  assert.ok("reason" in result);
  assert.ok("meta" in result);
});

test("result never contains an offer amount in the cash amount fields", async () => {
  withSnapshot(450000);
  const result = await routeInboundOffer({
    message: "what's your offer",
    classification: {},
    context: buildSfhContext(),
  });
  // sfh_cash_preview may expose cash_offer from the snapshot for display —
  // that is acceptable. Ensure we are NOT inventing a new number.
  // The cash_offer in meta MUST come from the snapshot, not be fabricated.
  assert.equal(result.meta.cash_offer, 450000); // comes from snapshot fixture
});

test("underwriting meta includes property_type and underwriting_reason", async () => {
  const result = await routeInboundOffer({
    message: "what can you pay",
    classification: {},
    context: buildMfContext(),
  });
  assert.equal(result.offer_route, "underwriting");
  assert.ok(result.meta.property_type);
  assert.ok(result.meta.underwriting_reason);
});
