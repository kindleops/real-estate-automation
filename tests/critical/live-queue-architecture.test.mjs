/**
 * Live queue architecture tests — Parts 1–4.
 *
 * Part 1: Real property gate — live queue creation requires a real property
 *         item.id; synthetic fallback is only allowed in dry_run.
 *
 * Part 2: Properties + Market hydration — the queue row must include the
 *         Properties relation (property_id) and Market relation (market_id)
 *         when a real property item is present in the context.
 *
 * Part 3: Template rotation — brain_item loads recently_used_template_ids so
 *         the cooldown filter works; rotation_key uses seller_id / synthetic
 *         address for per-owner spread.
 *
 * Part 4: Duplicate detection — findPendingDuplicate checks touch_number so
 *         only same-touch pending rows are considered duplicates; different
 *         touches for the same phone are allowed.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  buildSyntheticPropertyFromSellerId,
  findPendingDuplicate,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";

import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";

import {
  appRefField,
  categoryField,
  createPodioItem,
  numberField,
  textField,
} from "../helpers/test-helpers.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeActivePhoneItem(item_id = 401, overrides = {}) {
  return createPodioItem(item_id, {
    "phone-activity-status": categoryField("Active for 12 months or longer"),
    "phone-hidden": textField("9188102617"),
    "canonical-e164": textField("+19188102617"),
    "linked-master-owner": appRefField(overrides["linked-master-owner"] ?? 201),
    "linked-contact": appRefField(overrides["linked-contact"] ?? 301),
  });
}

function makeBaseContext({ property_item = null, market_item = null } = {}) {
  return {
    found: true,
    items: {
      phone_item: makeActivePhoneItem(),
      brain_item: null,
      master_owner_item: createPodioItem(201),
      property_item,
      agent_item: null,
      market_item,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: property_item?.item_id ?? null,
      market_id: market_item?.item_id ?? null,
      assigned_agent_id: null,
    },
    recent: { touch_count: 0 },
    summary: { total_messages_sent: 0 },
  };
}

function makeQueueItem(item_id, { status = "Queued", phone_item_id = 401, touch_number = 1 } = {}) {
  return createPodioItem(item_id, {
    "queue-status": categoryField(status),
    "phone-number": appRefField(phone_item_id),
    "touch-number": numberField(touch_number),
  });
}

// ── Part 1: Real property gate ────────────────────────────────────────────────

test("Part 1 — buildSyntheticPropertyFromSellerId creates a synthetic item from seller_id", () => {
  const owner_item = createPodioItem(1001, {
    "seller-id": textField("SFR~123 Main St|Springfield|IL|62701"),
  });

  const synthetic = buildSyntheticPropertyFromSellerId(owner_item);

  assert.ok(synthetic, "should return a synthetic property");
  assert.equal(synthetic.item_id, null, "synthetic item_id must be null (no real Podio item)");
  assert.equal(synthetic.synthetic, true);
  assert.equal(synthetic._synthetic_property_address, "123 Main St");
  assert.equal(synthetic._synthetic_property_city, "Springfield");
  assert.equal(synthetic._synthetic_property_state, "IL");
});

test("Part 1 — buildSyntheticPropertyFromSellerId returns null for unparseable seller_id", () => {
  const owner_item = createPodioItem(1002, {
    "seller-id": textField("NO_ADDRESS_HERE"),
  });

  const synthetic = buildSyntheticPropertyFromSellerId(owner_item);
  assert.equal(synthetic, null, "must return null when seller_id has no parseable address");
});

test("Part 1 — buildSendQueueItem correctly writes property relation when real property_id is in context", async () => {
  let captured_fields = null;
  const real_property = createPodioItem(5001);
  const context = makeBaseContext({ property_item: real_property });

  const result = await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "9AM-8PM CT",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9001 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok, "queue creation must succeed");
  assert.deepEqual(
    captured_fields?.["properties"],
    [5001],
    "Properties relation must be written with the real property_item_id"
  );
});

test("Part 1 — buildSendQueueItem omits properties relation when property_id is null (synthetic or missing)", async () => {
  let captured_fields = null;
  const context = makeBaseContext({ property_item: null });

  const result = await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9002 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.equal(
    "properties" in (captured_fields || {}),
    false,
    "properties field must be absent when there is no real property item"
  );
});

// ── Part 2: Market hydration ──────────────────────────────────────────────────

test("Part 2 — buildSendQueueItem writes market relation when market_id is in context", async () => {
  let captured_fields = null;
  const real_property = createPodioItem(5001);
  const real_market = createPodioItem(7001);
  const context = makeBaseContext({
    property_item: real_property,
    market_item: real_market,
  });

  await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9003 };
    },
    update_item: async () => {},
  });

  assert.deepEqual(
    captured_fields?.["market"],
    [7001],
    "Market relation must be written with the real market item_id"
  );
});

test("Part 2 — buildSendQueueItem omits market relation when no market_id is available", async () => {
  let captured_fields = null;
  const context = makeBaseContext({ property_item: null, market_item: null });

  await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9004 };
    },
    update_item: async () => {},
  });

  assert.equal(
    "market" in (captured_fields || {}),
    false,
    "market field must be absent when no market item is available"
  );
});

test("Part 2 — buildSendQueueItem writes both properties and market relations together", async () => {
  let captured_fields = null;
  const real_property = createPodioItem(5002);
  const real_market = createPodioItem(7002);
  const context = makeBaseContext({
    property_item: real_property,
    market_item: real_market,
  });

  const result = await buildSendQueueItem({
    context,
    rendered_message_text: "Hi there",
    textgrid_number_item_id: 601,
    scheduled_for_local: "2026-04-04 09:00:00",
    contact_window: "9AM-8PM CT",
    create_item: async (_app_id, fields) => {
      captured_fields = fields;
      return { item_id: 9005 };
    },
    update_item: async () => {},
  });

  assert.ok(result.ok);
  assert.deepEqual(captured_fields?.["properties"], [5002], "Properties relation must match real property");
  assert.deepEqual(captured_fields?.["market"], [7002], "Market relation must match real market");
});

// ── Part 3: Template rotation helpers ────────────────────────────────────────

test("Part 3 — buildSyntheticPropertyFromSellerId preserves unique address for rotation spread", () => {
  // Two owners with different seller_ids should produce different synthetic addresses,
  // which means different rotation keys and different template selections.
  const owner_a = createPodioItem(2001, {
    "seller-id": textField("SFR~100 Oak Ave|Dallas|TX|75001"),
  });
  const owner_b = createPodioItem(2002, {
    "seller-id": textField("SFR~200 Elm St|Houston|TX|77001"),
  });

  const synthetic_a = buildSyntheticPropertyFromSellerId(owner_a);
  const synthetic_b = buildSyntheticPropertyFromSellerId(owner_b);

  assert.ok(synthetic_a && synthetic_b);
  assert.notEqual(
    synthetic_a._synthetic_property_address,
    synthetic_b._synthetic_property_address,
    "Different owners must produce different synthetic addresses for rotation spread"
  );
});

// ── Part 4: findPendingDuplicate ─────────────────────────────────────────────

test("Part 4 — findPendingDuplicate blocks same-phone same-touch pending row", () => {
  const history = {
    queue_items: [
      makeQueueItem(8001, { status: "Queued", phone_item_id: 401, touch_number: 1 }),
    ],
    outbound_events: [],
  };

  const duplicate = findPendingDuplicate(history, 401, 1);
  assert.ok(duplicate, "must find pending duplicate for same phone + same touch");
  assert.equal(duplicate.item_id, 8001);
});

test("Part 4 — findPendingDuplicate allows different touch for same phone (does not over-block)", () => {
  // touch-1 is already Queued; touch-2 should NOT be blocked by it.
  const history = {
    queue_items: [
      makeQueueItem(8002, { status: "Queued", phone_item_id: 401, touch_number: 1 }),
    ],
    outbound_events: [],
  };

  const duplicate = findPendingDuplicate(history, 401, 2);
  assert.equal(duplicate, null, "touch-2 must not be blocked by an existing touch-1 row");
});

test("Part 4 — findPendingDuplicate blocks regardless of touch when touch_number not provided", () => {
  // Legacy call without touch_number: any pending row for that phone blocks.
  const history = {
    queue_items: [
      makeQueueItem(8003, { status: "Queued", phone_item_id: 401, touch_number: 3 }),
    ],
    outbound_events: [],
  };

  const duplicate = findPendingDuplicate(history, 401);
  assert.ok(duplicate, "without touch_number arg, any pending row for the phone must block");
});

test("Part 4 — findPendingDuplicate ignores Sent rows", () => {
  const history = {
    queue_items: [
      makeQueueItem(8004, { status: "Sent", phone_item_id: 401, touch_number: 1 }),
    ],
    outbound_events: [],
  };

  const duplicate = findPendingDuplicate(history, 401, 1);
  assert.equal(duplicate, null, "Sent rows are not pending duplicates");
});

test("Part 4 — findPendingDuplicate ignores Failed rows", () => {
  const history = {
    queue_items: [
      makeQueueItem(8005, { status: "Failed", phone_item_id: 401, touch_number: 1 }),
    ],
    outbound_events: [],
  };

  const duplicate = findPendingDuplicate(history, 401, 1);
  assert.equal(duplicate, null, "Failed rows are not pending duplicates");
});

test("Part 4 — findPendingDuplicate catches Sending status as well as Queued", () => {
  const history = {
    queue_items: [
      makeQueueItem(8006, { status: "Sending", phone_item_id: 401, touch_number: 2 }),
    ],
    outbound_events: [],
  };

  const duplicate = findPendingDuplicate(history, 401, 2);
  assert.ok(duplicate, "Sending rows must be treated as pending duplicates");
});

test("Part 4 — findPendingDuplicate returns null when history is empty", () => {
  const history = { queue_items: [], outbound_events: [] };
  const duplicate = findPendingDuplicate(history, 401, 1);
  assert.equal(duplicate, null);
});

test("Part 4 — two cron runs creating same owner+phone+touch are blocked by second-run pending check", () => {
  // Simulates the second cron run: run-1 already created a Queued row for
  // phone 401 touch 1.  Run-2 must find it and skip.
  const history = {
    queue_items: [
      makeQueueItem(8007, { status: "Queued", phone_item_id: 401, touch_number: 1 }),
    ],
    outbound_events: [],
  };

  // Run-2 tries to create touch-1 for the same phone
  const run_2_duplicate = findPendingDuplicate(history, 401, 1);
  assert.ok(run_2_duplicate, "run-2 must be blocked by run-1's pending queue row");

  // Run-2 trying touch-2 for the same phone should be allowed
  const run_2_touch_2 = findPendingDuplicate(history, 401, 2);
  assert.equal(run_2_touch_2, null, "run-2 touch-2 must not be blocked by run-1's touch-1 row");
});
