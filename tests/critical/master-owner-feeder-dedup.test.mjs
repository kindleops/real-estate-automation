import test from "node:test";
import assert from "node:assert/strict";

import {
  applyPostPropertyEvaluationGuards,
  applyPreDeepEvaluationGuards,
  buildEvaluationLockScope,
  buildFeederRunCounters,
  buildMasterOwnerQueueId,
  summarizeSource,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  numberField,
  textField,
} from "../helpers/test-helpers.js";

function makePhoneRecord(phone_item_id = 401) {
  return {
    phone_item_id,
    summary: {
      phone_item_id,
      normalized_phone: "+19185551212",
    },
  };
}

function makeOwnerSummary(item_id = 201) {
  return {
    item_id,
    seller_id: `seller-${item_id}`,
    owner_name: "Owner Example",
  };
}

function makePropertyItem(item_id = 601) {
  return createPodioItem(item_id, {
    title: textField("123 Main St"),
    "property-address": textField("123 Main St"),
  });
}

function makeQueueItem(
  item_id,
  {
    status = "Queued",
    master_owner_id = 201,
    phone_item_id = 401,
    property_item_id = null,
    touch_number = 1,
    queue_id = null,
  } = {}
) {
  return createPodioItem(item_id, {
    "queue-status": categoryField(status),
    "master-owner": appRefField(master_owner_id),
    ...(phone_item_id ? { "phone-number": appRefField(phone_item_id) } : {}),
    ...(property_item_id ? { properties: appRefField(property_item_id) } : {}),
    ...(touch_number ? { "touch-number": numberField(touch_number) } : {}),
    ...(queue_id ? { "queue-id-2": textField(queue_id) } : {}),
  });
}

function makeHistory(queue_items = []) {
  return {
    queue_items,
    outbound_events: [],
    inbound_events: [],
  };
}

test("applyPreDeepEvaluationGuards skips seller when duplicate queue id already exists", () => {
  const queue_id = buildMasterOwnerQueueId(201, 401, 1);
  const result = applyPreDeepEvaluationGuards({
    history: makeHistory([
      makeQueueItem(9001, {
        status: "Sent",
        queue_id,
      }),
    ]),
    owner_summary: makeOwnerSummary(),
    selected_phone_record: makePhoneRecord(),
    touch_number: 1,
    queue_id,
  });

  assert.equal(result?.reason, "duplicate_queue_id");
  assert.equal(result?.queue_id, queue_id);
  assert.equal(result?.duplicate_queue_item_id, 9001);
});

test("applyPreDeepEvaluationGuards skips seller when already queued on the same phone/touch", () => {
  const result = applyPreDeepEvaluationGuards({
    history: makeHistory([
      makeQueueItem(9002, {
        status: "Queued",
        phone_item_id: 401,
        touch_number: 2,
      }),
    ]),
    owner_summary: makeOwnerSummary(),
    selected_phone_record: makePhoneRecord(401),
    touch_number: 2,
    queue_id: buildMasterOwnerQueueId(201, 401, 2),
  });

  assert.equal(result?.reason, "duplicate_pending_queue_item");
  assert.equal(result?.duplicate_queue_item_id, 9002);
});

test("applyPreDeepEvaluationGuards skips seller when another active queue row is already in flight", () => {
  const result = applyPreDeepEvaluationGuards({
    history: makeHistory([
      makeQueueItem(9003, {
        status: "Sending",
        phone_item_id: 999,
        touch_number: 2,
      }),
    ]),
    owner_summary: makeOwnerSummary(),
    selected_phone_record: makePhoneRecord(401),
    touch_number: 2,
    queue_id: buildMasterOwnerQueueId(201, 401, 2),
  });

  assert.equal(result?.reason, "already_in_flight_active_queue_row");
  assert.equal(result?.active_queue_item_id, 9003);
  assert.equal(result?.active_queue_touch_number, 2);
});

test("applyPreDeepEvaluationGuards skips seller when the same touch was recently evaluated", () => {
  const evaluation_lock_scope = buildEvaluationLockScope({
    master_owner_id: 201,
    phone_item_id: 401,
    touch_number: 2,
  });

  const result = applyPreDeepEvaluationGuards({
    history: makeHistory(),
    owner_summary: makeOwnerSummary(),
    selected_phone_record: makePhoneRecord(401),
    touch_number: 2,
    queue_id: buildMasterOwnerQueueId(201, 401, 2),
    recently_evaluated_lock: {
      active: true,
      scope: evaluation_lock_scope,
      expires_at: "2026-04-17T18:00:00.000Z",
      record_item_id: 7777,
    },
  });

  assert.equal(result?.reason, "recently_evaluated_lock_active");
  assert.equal(result?.evaluation_lock_scope, evaluation_lock_scope);
  assert.equal(result?.evaluation_lock_record_item_id, 7777);
});

test("applyPostPropertyEvaluationGuards blocks active duplicate rows for the same property/touch", () => {
  const result = applyPostPropertyEvaluationGuards({
    history: makeHistory([
      makeQueueItem(9004, {
        status: "Queued",
        phone_item_id: 999,
        property_item_id: 601,
        touch_number: 2,
      }),
    ]),
    owner_summary: makeOwnerSummary(),
    owner_item: createPodioItem(201),
    selected_phone_record: makePhoneRecord(401),
    property_item: makePropertyItem(601),
    touch_number: 2,
    queue_id: buildMasterOwnerQueueId(201, 401, 2),
  });

  assert.equal(result?.reason, "already_in_flight_active_queue_row");
  assert.equal(result?.active_queue_item_id, 9004);
  assert.equal(result?.active_queue_property_item_id, 601);
});

test("cheap pre-deep guards block duplicate queue ids without needing property or template context", () => {
  const queue_id = buildMasterOwnerQueueId(201, 401, 1);
  const result = applyPreDeepEvaluationGuards({
    history: makeHistory([
      makeQueueItem(9005, {
        status: "Queued",
        phone_item_id: 401,
        touch_number: 1,
        queue_id,
      }),
    ]),
    owner_summary: makeOwnerSummary(),
    selected_phone_record: makePhoneRecord(401),
    touch_number: 1,
    queue_id,
  });

  assert.equal(result?.reason, "duplicate_queue_id");
  assert.equal(result?.phone?.phone_item_id, 401);
});

test("legitimate next-touch follow-up is still allowed after a prior touch is already sent", () => {
  const result = applyPreDeepEvaluationGuards({
    history: makeHistory([
      makeQueueItem(9006, {
        status: "Sent",
        phone_item_id: 401,
        touch_number: 1,
        queue_id: buildMasterOwnerQueueId(201, 401, 1),
      }),
    ]),
    owner_summary: makeOwnerSummary(),
    selected_phone_record: makePhoneRecord(401),
    touch_number: 2,
    queue_id: buildMasterOwnerQueueId(201, 401, 2),
  });

  assert.equal(result, null);
});

test("buildFeederRunCounters returns the requested duplicate and queue-create counters", () => {
  const counters = buildFeederRunCounters({
    results: [
      { skipped: true, reason: "duplicate_queue_id" },
      { skipped: true, reason: "already_in_flight_active_queue_row" },
      { skipped: true, reason: "recently_evaluated_lock_active" },
      { skipped: true, reason: "recent_contact_within_suppression_window" },
      { skipped: true, reason: "no_usable_phone" },
      { skipped: true, reason: "real_property_required_for_live_queue" },
      { skipped: true, reason: "template_not_found" },
      { skipped: false, ok: true, reason: "master_owner_touch_queued" },
    ],
    deep_eval_count: 4,
    template_eval_count: 3,
    queue_create_attempt_count: 2,
    queue_create_success_count: 1,
    queue_create_duplicate_cancel_count: 1,
  });

  assert.equal(counters.cheap_skip_count, 5);
  assert.equal(counters.deep_eval_count, 4);
  assert.equal(counters.template_eval_count, 3);
  assert.equal(counters.duplicate_skip_count, 1);
  assert.equal(counters.already_in_flight_skip_count, 1);
  assert.equal(counters.recently_evaluated_skip_count, 1);
  assert.equal(counters.suppression_skip_count, 1);
  assert.equal(counters.no_phone_skip_count, 1);
  assert.equal(counters.no_property_skip_count, 1);
  assert.equal(counters.template_not_found_count, 1);
  assert.equal(counters.queue_create_attempt_count, 2);
  assert.equal(counters.queue_create_success_count, 1);
  assert.equal(counters.queue_create_duplicate_cancel_count, 1);
});

test("summarizeSource exposes requested and resolved source-view diagnostics", () => {
  const source = summarizeSource({
    type: "view",
    view_id: 61752339,
    view_name: "SMS / TIER #1 / ALL",
    requested_view_id: null,
    requested_view_name: "SMS / TIER #1 / ALL",
    resolved_view_id: 61752339,
    resolved_view_name: "SMS / TIER #1 / ALL",
    fallback_occurred: false,
    fallback_reason: null,
    resolution_strategy: "view_name_exact_match",
  });

  assert.equal(source.requested_view_name, "SMS / TIER #1 / ALL");
  assert.equal(source.resolved_view_name, "SMS / TIER #1 / ALL");
  assert.equal(source.resolved_view_id, 61752339);
  assert.equal(source.fallback_occurred, false);
  assert.equal(source.resolution_strategy, "view_name_exact_match");
});
