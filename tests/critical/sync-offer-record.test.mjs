/**
 * sync-offer-record.test.mjs
 *
 * Tests for src/lib/domain/offers/sync-offer-record.js
 *
 * Coverage:
 *   1.  shouldSyncOfferRecord returns false when no cash_offer_snapshot_id
 *   2.  shouldSyncOfferRecord returns false for Stage 1 / ownership_check
 *   3.  shouldSyncOfferRecord returns true for cash-offer use cases
 *   4.  shouldSyncOfferRecord uses metadata fallback for use_case detection
 *   5.  syncOfferRecord is skipped (ok:true, skipped:true) for Stage 1
 *   6.  syncOfferRecord is skipped when no snapshot_id (multifamily/creative path)
 *   7.  SFH cash snapshot exists → creates Podio Offer (ok:true, created:true)
 *   8.  buildOfferPayload links property / master_owner / prospect / phone / market / agent
 *   9.  buildOfferPayload puts financial details and diagnostics in notes
 *   10. buildOfferPayload includes message_text in notes (truncated to 500 chars)
 *   11. buildOfferPayload sets offer_status = "Offer Sent", offer_type = "Cash"
 *   12. buildOfferPayload omits offer_sent_price when cash_offer is null
 *   13. syncOfferRecord updates existing open Offer when one is found
 *   14. Podio Offer creation failure → ok:false, marks queue + message event offer_record_sync_failed, Discord alert
 *   15. Discord alert is NOT sent when Podio create succeeds
 *   16. Multifamily / creative path — no snapshot_id → skipped, no Offer created
 *   17. syncOfferRecord: snapshot not in DB → skipped gracefully (ok:true, skipped:true)
 *   18. syncOfferRecord back-fills offer_podio_item_id on send_queue row
 *   19. syncOfferRecord back-fills podio_offer_item_id on snapshot row
 *   20. Second parallel call for same queue row: updates existing offer (idempotent)
 *   21. Creative/multifamily route skips Offer sync even if snapshot_id exists
 *   22. Message missing snapshot cash offer amount skips Offer sync
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  shouldSyncOfferRecord,
  buildOfferPayload,
  syncOfferRecord,
  __setSyncOfferRecordDeps,
  __resetSyncOfferRecordDeps,
} from "@/lib/domain/offers/sync-offer-record.js";

import { OFFER_FIELDS } from "@/lib/podio/apps/offers.js";

// ---------------------------------------------------------------------------
// Mock builders
// ---------------------------------------------------------------------------

/** Minimal snapshot row. */
function makeSnapshot(overrides = {}) {
  return {
    id:                          "snap-uuid-001",
    property_id:                 "prop_sfh_001",
    podio_property_item_id:      5501,
    master_owner_id:             2201,
    status:                      "active",
    cash_offer:                  180000,
    repair_estimate:             12000,
    estimated_value:             225000,
    calculated_value:            185000,
    estimated_equity:            65000,
    estimated_mortgage_balance:  110000,
    estimated_mortgage_payment:  850,
    offer_source:                "podio",
    version:                     1,
    ...overrides,
  };
}

/** Normalized send_queue row with a cash offer snapshot attached. */
function makeQueueRow(overrides = {}) {
  return {
    id:                       "queue-row-abc123",
    queue_status:             "sent",
    message_body:             "Hi Frank, we can offer you $180,000 cash for 123 Elm St. Interested?",
    property_address:         "123 Elm St",
    master_owner_id:          2201,
    prospect_id:              3301,
    property_id:              5501,
    market_id:                7701,
    sms_agent_id:             8801,
    template_id:              9901,
    use_case_template:        "cash_offer_present",
    cash_offer_snapshot_id:   "snap-uuid-001",
    metadata:                 {},
    ...overrides,
  };
}

/** Minimal Supabase mock sufficient for snapshot lookup + queue/snapshot updates. */
function makeSupabaseMock({ snapshot = null, snapshot_error = null } = {}) {
  const updates = [];
  return {
    updates,
    from(table) {
      const chain = {
        select() { return chain; },
        eq()     { return chain; },
        order()  { return chain; },
        limit()  { return chain; },
        update(payload) {
          updates.push({ table, payload });
          return chain;
        },
        maybeSingle() {
          if (table === "property_cash_offer_snapshots") {
            if (snapshot_error) return Promise.resolve({ data: null, error: snapshot_error });
            return Promise.resolve({ data: snapshot, error: null });
          }
          return Promise.resolve({ data: null, error: null });
        },
        then(resolve) {
          return Promise.resolve({ data: null, error: null }).then(resolve);
        },
      };
      return chain;
    },
  };
}

/** Build DI overrides that simulate successful Podio Offer ops. */
function makeOfferDeps({
  existing_offer         = null,
  create_result          = { item_id: 6601 },
  create_error           = null,
  update_error           = null,
  alert_spy              = null,
  message_event_spy      = null,
  snapshot               = null,
  snapshot_error         = null,
} = {}) {
  const calls = [];
  const db    = makeSupabaseMock({ snapshot, snapshot_error });

  return {
    calls,
    db,
    supabase_override:          db,
    find_offer_by_property:     async () => { calls.push("find_by_property");     return existing_offer; },
    find_offer_by_master_owner: async () => { calls.push("find_by_master_owner"); return null; },
    create_offer_item: async (payload) => {
      calls.push({ op: "create", payload });
      if (create_error) throw create_error;
      return create_result;
    },
    update_offer_item: async (item_id, payload) => {
      calls.push({ op: "update", item_id, payload });
      if (update_error) throw update_error;
      return { item_id };
    },
    update_message_event: async (item_id, fields) => {
      calls.push({ op: "update_message_event", item_id, fields });
      if (message_event_spy) message_event_spy({ item_id, fields });
      return { item_id };
    },
    send_critical_alert: async (alert) => {
      calls.push({ op: "alert", alert });
      if (alert_spy) alert_spy(alert);
    },
  };
}

// ---------------------------------------------------------------------------
// 1-4: shouldSyncOfferRecord unit tests
// ---------------------------------------------------------------------------

test("shouldSyncOfferRecord returns false when cash_offer_snapshot_id is absent", () => {
  assert.ok(!shouldSyncOfferRecord({}),                                               "empty row");
  assert.ok(!shouldSyncOfferRecord({ cash_offer_snapshot_id: null }),                 "null snapshot_id");
  assert.ok(!shouldSyncOfferRecord({ cash_offer_snapshot_id: "" }),                   "empty string");
  assert.ok(!shouldSyncOfferRecord({ cash_offer_snapshot_id: 0 }),                    "zero is falsy");
});

test("shouldSyncOfferRecord returns false for Stage 1 / ownership_check", () => {
  const base = { cash_offer_snapshot_id: "snap-123" };
  assert.ok(!shouldSyncOfferRecord({ ...base, use_case_template: "ownership_check" }), "exact match");
  assert.ok(!shouldSyncOfferRecord({ ...base, use_case_template: "Ownership_Check" }), "case insensitive");
  assert.ok(!shouldSyncOfferRecord({
    ...base,
    use_case_template:  null,
    metadata: { selected_use_case: "ownership_check" },
  }), "metadata fallback");
  assert.ok(!shouldSyncOfferRecord({
    ...base,
    use_case_template: null,
    metadata: { template_use_case: "ownership_check" },
  }), "metadata template_use_case fallback");
});

test("shouldSyncOfferRecord returns true for cash-offer use cases", () => {
  const accepted = [
    "cash_offer_present",
    "re_engagement",
    "follow_up",
    "offer_follow_up",
    "price_reduction",
    "",          // unknown use_case + snapshot_id = allow (not ownership_check)
    null,        // null use_case + snapshot_id = allow
  ];
  for (const uc of accepted) {
    const row = { cash_offer_snapshot_id: "snap-abc", use_case_template: uc };
    assert.ok(
      shouldSyncOfferRecord(row),
      `use_case "${uc ?? "(null)"}" with snapshot_id should sync`
    );
  }
});

test("shouldSyncOfferRecord uses metadata.use_case as last fallback", () => {
  const row = {
    cash_offer_snapshot_id: "snap-xyz",
    use_case_template: null,
    metadata: { use_case: "ownership_check" },
  };
  assert.ok(!shouldSyncOfferRecord(row), "metadata.use_case ownership_check should block sync");
});

// ---------------------------------------------------------------------------
// 5. syncOfferRecord skipped for Stage 1 (ownership_check)
// ---------------------------------------------------------------------------

test("syncOfferRecord skips and returns ok:true for Stage 1 / ownership_check queue row", async () => {
  const deps = makeOfferDeps({ snapshot: makeSnapshot() });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({
      queue_row: makeQueueRow({ use_case_template: "ownership_check" }),
      now:       "2026-04-20T10:00:00.000Z",
    });

    assert.ok(result.ok,      "ok is true (skipped gracefully)");
    assert.ok(result.skipped, "skipped is true");
    assert.ok(!result.created, "no Offer created");
    assert.ok(!result.updated, "no Offer updated");

    // Create must NOT have been called
    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 0, "Podio createOfferItem was NOT called");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 6. syncOfferRecord can resolve active snapshot when no snapshot_id on row
// ---------------------------------------------------------------------------

test("syncOfferRecord resolves active snapshot and creates Offer when snapshot_id is absent but sent message contains offer amount", async () => {
  const deps = makeOfferDeps({ snapshot: makeSnapshot(), create_result: { item_id: 6611 } });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({
      queue_row: makeQueueRow({ cash_offer_snapshot_id: null }),
    });

    assert.ok(result.ok,       "ok:true");
    assert.ok(!result.skipped, "not skipped");
    assert.ok(result.created,  "created:true");
    assert.equal(result.offer_item_id, 6611, "created offer id returned");

    assert.equal(
      result.diagnostics.snapshot_resolution,
      "active_snapshot_lookup",
      "resolved via active snapshot lookup"
    );
    assert.ok(
      result.diagnostics.message_contains_offer_amount,
      "message amount match guard passed"
    );

    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 1, "Offer created from resolved active snapshot");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 7. SFH cash snapshot → creates Podio Offer
// ---------------------------------------------------------------------------

test("SFH with cash snapshot creates Podio Offer and returns ok:true, created:true", async () => {
  const snapshot = makeSnapshot();
  const deps     = makeOfferDeps({ snapshot, create_result: { item_id: 7701 } });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({
      queue_row:         makeQueueRow(),
      outbound_event_id: 4401,
      now:               "2026-04-20T11:00:00.000Z",
    });

    assert.ok(result.ok,        "ok is true");
    assert.ok(!result.skipped,  "not skipped");
    assert.ok(result.created,   "created is true");
    assert.ok(!result.updated,  "updated is false");
    assert.equal(result.offer_item_id, 7701, "offer_item_id from mock");
    assert.equal(result.diagnostics.cash_offer, 180000, "diagnostics.cash_offer");

    const create_call = deps.calls.find((c) => c?.op === "create");
    assert.ok(create_call, "createOfferItem was called");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 8. buildOfferPayload — relationship fields correct
// ---------------------------------------------------------------------------

test("buildOfferPayload includes correct relationship refs for property/owner/prospect/phone/market/agent", () => {
  const snapshot  = makeSnapshot();
  const queue_row = makeQueueRow({
    master_owner_id: 2201,
    prospect_id:     3301,
    property_id:     5501,
    market_id:       7701,
    sms_agent_id:    8801,
    metadata:        { phone_item_id: 4401 },
  });

  const payload = buildOfferPayload({ queue_row, snapshot, outbound_event_id: 1234 });

  assert.deepEqual(payload[OFFER_FIELDS.master_owner],    [2201], "master_owner ref");
  assert.deepEqual(payload[OFFER_FIELDS.property],        [5501], "property ref");
  assert.deepEqual(payload[OFFER_FIELDS.prospect],        [3301], "prospect ref");
  assert.deepEqual(payload[OFFER_FIELDS.market],          [7701], "market ref");
  assert.deepEqual(payload[OFFER_FIELDS.assigned_agent],  [8801], "agent ref");
  assert.deepEqual(payload[OFFER_FIELDS.phone_number],    [4401], "phone ref from metadata");
});

// ---------------------------------------------------------------------------
// 9 & 10. buildOfferPayload — notes block
// ---------------------------------------------------------------------------

test("buildOfferPayload puts all financial details and diagnostics into notes", () => {
  const snapshot  = makeSnapshot();
  const queue_row = makeQueueRow();

  const payload = buildOfferPayload({ queue_row, snapshot, outbound_event_id: 5555 });
  const notes   = payload[OFFER_FIELDS.notes] ?? "";

  assert.ok(notes.includes("SMS Automation"),       "offer_source in notes");
  assert.ok(notes.includes("$180,000"),              "cash_offer in notes");
  assert.ok(notes.includes("$12,000"),               "repair_estimate in notes");
  assert.ok(notes.includes("$225,000"),              "estimated_value in notes");
  assert.ok(notes.includes("$185,000"),              "calculated_value in notes");
  assert.ok(notes.includes("$65,000"),               "estimated_equity in notes");
  assert.ok(notes.includes("$110,000"),              "est_mortgage_balance in notes");
  assert.ok(notes.includes("$850"),                  "est_mortgage_payment in notes");
  assert.ok(notes.includes("queue-row-abc123"),      "queue_row_id in notes");
  assert.ok(notes.includes("5555"),                  "outbound_event_id in notes");
  assert.ok(notes.includes("snap-uuid-001"),         "snapshot_id in notes");
});

test("buildOfferPayload includes message_text in notes (truncated if long)", () => {
  const long_body = "A".repeat(600);
  const payload = buildOfferPayload({
    queue_row: makeQueueRow({ message_body: long_body }),
    snapshot:  makeSnapshot(),
  });
  const notes = payload[OFFER_FIELDS.notes] ?? "";
  assert.ok(notes.includes("=== Message Text ==="), "message text header present");
  assert.ok(notes.includes("A".repeat(500)),          "first 500 chars included");
  assert.ok(!notes.includes("A".repeat(501)),         "truncated at 500");
});

// ---------------------------------------------------------------------------
// 11 & 12. buildOfferPayload — core status / type / price fields
// ---------------------------------------------------------------------------

test("buildOfferPayload sets offer_status=Offer Sent, offer_type=Cash", () => {
  const payload = buildOfferPayload({ queue_row: makeQueueRow(), snapshot: makeSnapshot() });

  assert.equal(payload[OFFER_FIELDS.offer_status], "Offer Sent", "offer_status");
  assert.equal(payload[OFFER_FIELDS.offer_type],   "Cash",       "offer_type");
});

test("buildOfferPayload omits offer_sent_price when cash_offer is null", () => {
  const payload = buildOfferPayload({
    queue_row: makeQueueRow(),
    snapshot:  makeSnapshot({ cash_offer: null }),
  });
  assert.ok(!(OFFER_FIELDS.offer_sent_price in payload), "offer_sent_price omitted when null");
});

// ---------------------------------------------------------------------------
// 13. Updates existing open Offer
// ---------------------------------------------------------------------------

test("syncOfferRecord updates existing open Offer instead of creating a new one", async () => {
  const existing = { item_id: 5501 };
  const deps     = makeOfferDeps({
    snapshot:       makeSnapshot(),
    existing_offer: existing,
  });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({ queue_row: makeQueueRow() });

    assert.ok(result.ok,                          "ok:true");
    assert.ok(result.updated,                     "updated:true");
    assert.ok(!result.created,                    "created:false");
    assert.equal(result.offer_item_id, 5501,      "offer_item_id is existing item");
    assert.equal(result.diagnostics.existing_offer_id, 5501);

    const update_call = deps.calls.find((c) => c?.op === "update");
    assert.ok(update_call,                        "updateOfferItem was called");
    assert.equal(update_call.item_id, 5501,       "updated correct item");

    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 0,               "createOfferItem was NOT called");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 14. Podio creation failure → ok:false, DB marked, Discord alert fired
// ---------------------------------------------------------------------------

test("Podio Offer creation failure returns ok:false and marks offer_record_sync_failed", async () => {
  const alert_calls = [];
  const message_event_updates = [];
  const deps = makeOfferDeps({
    snapshot:     makeSnapshot(),
    create_error: new Error("Podio 429 Rate Limit"),
    alert_spy:    (a) => alert_calls.push(a),
    message_event_spy: (e) => message_event_updates.push(e),
  });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({
      queue_row: makeQueueRow(),
      outbound_event_id: 4411,
      now:       "2026-04-20T12:00:00.000Z",
    });

    assert.ok(!result.ok,             "ok is false");
    assert.ok(!result.skipped,        "not skipped");
    assert.ok(!result.created,        "not created");
    assert.ok(result.error,           "error string present");
    assert.ok(result.error.includes("Podio 429"), "error contains message");

    // Discord alert must have been sent
    assert.equal(alert_calls.length, 1,             "exactly 1 alert sent");
    const alert = alert_calls[0];
    assert.ok(alert.title.toLowerCase().includes("offer record sync failed"), "correct alert title");
    assert.equal(alert.color, 0xFF4444,             "red alert color");
    const fields_str = JSON.stringify(alert.fields);
    assert.ok(fields_str.includes("queue-row-abc123"), "queue_row_id in alert fields");
    assert.ok(fields_str.includes("snap-uuid-001"),    "snapshot_id in alert fields");
    assert.ok(fields_str.includes("Podio 429"),        "error message in alert fields");

    // DB update must have been called to mark 'failed'
    const update_calls = deps.db.updates.filter(
      (u) => u.table === "send_queue" && u.payload?.offer_record_sync_status === "failed"
    );
    assert.ok(update_calls.length > 0, "send_queue row marked offer_record_sync_status=failed");
    assert.ok(
      String(update_calls[0].payload.offer_record_sync_error ?? "").includes("Podio 429"),
      "error message stored in offer_record_sync_error"
    );

    // Message Event should be marked with offer_record_sync_failed:*
    assert.equal(message_event_updates.length, 1, "message event updated exactly once");
    assert.equal(message_event_updates[0].item_id, 4411, "correct outbound event id updated");
    const fields_json = JSON.stringify(message_event_updates[0].fields || {});
    assert.ok(
      fields_json.includes("offer_record_sync_failed:Podio 429"),
      "message event includes offer_record_sync_failed marker"
    );
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 15. Discord alert NOT sent on success
// ---------------------------------------------------------------------------

test("Discord alert is NOT sent when Podio Offer is created successfully", async () => {
  const alert_calls = [];
  const deps = makeOfferDeps({
    snapshot:     makeSnapshot(),
    create_result: { item_id: 8801 },
    alert_spy:    (a) => alert_calls.push(a),
  });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({ queue_row: makeQueueRow() });
    assert.ok(result.ok, "ok:true");
    assert.equal(alert_calls.length, 0, "no Discord alert on success");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 16. Multifamily path — no snapshot_id → skipped, no Offer created
// ---------------------------------------------------------------------------

test("Multifamily queue row has no cash_offer_snapshot_id → skipped, no Offer", async () => {
  const alert_calls = [];
  const deps = makeOfferDeps({
    snapshot:  null,   // no snapshot either
    alert_spy: (a) => alert_calls.push(a),
  });
  __setSyncOfferRecordDeps(deps);

  try {
    // Simulate a multifamily queue row: no snapshot_id
    const mf_queue_row = makeQueueRow({
      cash_offer_snapshot_id: null,
      use_case_template:      "multifamily_intake",
    });

    const result = await syncOfferRecord({ queue_row: mf_queue_row });

    assert.ok(result.ok,       "ok:true");
    assert.ok(result.skipped,  "skipped:true");

    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 0, "Podio Offer NOT created for MF queue row");
    assert.equal(alert_calls.length, 0, "no Discord alert for graceful skip");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

test("Creative/multifamily route skips Offer sync even when snapshot_id is present", async () => {
  const deps = makeOfferDeps({ snapshot: makeSnapshot() });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({
      queue_row: makeQueueRow({
        use_case_template: "mf_offer_reveal",
        cash_offer_snapshot_id: "snap-uuid-001",
      }),
    });

    assert.ok(result.ok, "ok:true");
    assert.ok(result.skipped, "skipped:true for underwriting route");
    assert.equal(result.diagnostics.skip_reason, "underwriting_route");

    const creates = deps.calls.filter((c) => c?.op === "create");
    const updates = deps.calls.filter((c) => c?.op === "update");
    assert.equal(creates.length, 0, "no Offer create for underwriting route");
    assert.equal(updates.length, 0, "no Offer update for underwriting route");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

test("Message missing snapshot cash offer amount skips Offer sync", async () => {
  const deps = makeOfferDeps({
    snapshot: makeSnapshot({ cash_offer: 180000 }),
  });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({
      queue_row: makeQueueRow({
        message_body: "Hi, just checking in to see if you are still considering a sale.",
      }),
    });

    assert.ok(result.ok, "ok:true");
    assert.ok(result.skipped, "skipped:true");
    assert.equal(result.diagnostics.skip_reason, "message_missing_snapshot_offer_amount");

    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 0, "no Offer created when sent message has no snapshot offer amount");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

test("buildOfferPayload notes include message event, queue row, template, and snapshot diagnostics", () => {
  const payload = buildOfferPayload({
    queue_row: makeQueueRow({ template_id: 9901 }),
    snapshot: makeSnapshot({ id: "snap-uuid-001" }),
    outbound_event_id: 4411,
  });

  const notes = String(payload[OFFER_FIELDS.notes] || "");
  assert.ok(notes.includes("Queue Row ID: queue-row-abc123"), "notes include send_queue row linkage");
  assert.ok(notes.includes("Message Event ID: 4411"), "notes include message event linkage");
  assert.ok(notes.includes("Template ID: 9901"), "notes include template linkage");
  assert.ok(notes.includes("Snapshot ID (Supabase): snap-uuid-001"), "notes include snapshot linkage");
});

// ---------------------------------------------------------------------------
// 17. Snapshot not found in DB → skipped gracefully
// ---------------------------------------------------------------------------

test("syncOfferRecord skips gracefully when snapshot is not found in DB", async () => {
  // Supabase returns null for snapshot lookup
  const deps = makeOfferDeps({ snapshot: null });
  __setSyncOfferRecordDeps(deps);

  try {
    const result = await syncOfferRecord({ queue_row: makeQueueRow() });

    assert.ok(result.ok,      "ok:true (not an error)");
    assert.ok(result.skipped, "skipped:true when snapshot missing");
    assert.equal(result.diagnostics.skip_reason, "snapshot_not_found");

    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 0, "no Offer created");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 18. Back-fills offer_podio_item_id on send_queue row
// ---------------------------------------------------------------------------

test("syncOfferRecord back-fills offer_podio_item_id on send_queue row after create", async () => {
  const deps = makeOfferDeps({
    snapshot:       makeSnapshot(),
    create_result:  { item_id: 9901 },
  });
  __setSyncOfferRecordDeps(deps);

  try {
    await syncOfferRecord({ queue_row: makeQueueRow(), now: "2026-04-20T13:00:00.000Z" });

    const queue_update = deps.db.updates.find(
      (u) => u.table === "send_queue" && u.payload?.offer_podio_item_id != null
    );
    assert.ok(queue_update, "send_queue update with offer_podio_item_id was issued");
    assert.equal(queue_update.payload.offer_podio_item_id, 9901, "correct item_id stored");
    assert.equal(queue_update.payload.offer_record_sync_status, "synced", "status = synced");
    assert.equal(queue_update.payload.offer_record_sync_error, null,      "error cleared");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 19. Back-fills podio_offer_item_id on snapshot row
// ---------------------------------------------------------------------------

test("syncOfferRecord back-fills podio_offer_item_id on property_cash_offer_snapshots row", async () => {
  const deps = makeOfferDeps({
    snapshot:      makeSnapshot(),
    create_result: { item_id: 9901 },
  });
  __setSyncOfferRecordDeps(deps);

  try {
    await syncOfferRecord({ queue_row: makeQueueRow(), now: "2026-04-20T13:00:00.000Z" });

    const snapshot_update = deps.db.updates.find(
      (u) => u.table === "property_cash_offer_snapshots"
    );
    assert.ok(snapshot_update, "property_cash_offer_snapshots update was issued");
    assert.equal(snapshot_update.payload.podio_offer_item_id, 9901, "correct Podio item_id stored");
    assert.ok(snapshot_update.payload.podio_synced_at, "podio_synced_at set");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// 20. Idempotent update path — existing open offer matched → update, not duplicate
// ---------------------------------------------------------------------------

test("syncOfferRecord calls update (not create) when existing open Offer is found — idempotent", async () => {
  const deps = makeOfferDeps({
    snapshot:       makeSnapshot(),
    existing_offer: { item_id: 7777 },
  });
  __setSyncOfferRecordDeps(deps);

  try {
    const r1 = await syncOfferRecord({ queue_row: makeQueueRow() });
    const r2 = await syncOfferRecord({ queue_row: makeQueueRow() });

    for (const result of [r1, r2]) {
      assert.ok(result.ok,                       "ok:true");
      assert.ok(result.updated,                  "updated:true");
      assert.ok(!result.created,                 "created:false");
      assert.equal(result.offer_item_id, 7777,   "uses existing item_id");
    }

    // All calls should be updates, no creates
    const creates = deps.calls.filter((c) => c?.op === "create");
    assert.equal(creates.length, 0, "zero creates — idempotent via update path");
  } finally {
    __resetSyncOfferRecordDeps();
  }
});

// ---------------------------------------------------------------------------
// Bonus: buildOfferPayload falls back to snapshot.podio_property_item_id
// when queue_row.property_id is missing
// ---------------------------------------------------------------------------

test("buildOfferPayload falls back to snapshot.podio_property_item_id for property ref", () => {
  const payload = buildOfferPayload({
    queue_row: makeQueueRow({ property_id: null }),
    snapshot:  makeSnapshot({ podio_property_item_id: 6699 }),
  });
  assert.deepEqual(payload[OFFER_FIELDS.property], [6699], "falls back to snapshot item_id");
});
