import test from "node:test";
import assert from "node:assert/strict";

import { forceDueQueuedItems } from "@/lib/domain/queue/force-due-queued-items.js";
import { handleQueueForceDueRequest } from "@/lib/domain/queue/queue-force-due-request.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  dateField,
} from "../helpers/test-helpers.js";

// ─── podio field reader stubs (mirror the real helpers, no axios needed) ─────

function getCategoryValue(item, external_id, fallback = null) {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  const val = field?.values?.[0];
  return val?.value?.text ?? fallback;
}

function getDateValue(item, external_id, fallback = null) {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  const val = field?.values?.[0];
  return val?.start ?? fallback;
}

function getFirstAppReferenceId(item, external_id, fallback = null) {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  const val = field?.values?.[0];
  return val?.value?.item_id ?? fallback;
}

// ─── constants ────────────────────────────────────────────────────────────────

const FIXED_NOW = "2026-04-05T18:00:00.000Z";
// Expected Podio UTC string for FIXED_NOW
const FIXED_NOW_PODIO_UTC = "2026-04-05 18:00:00";
// Central (CDT = UTC-5): 18:00 UTC = 13:00 CDT
const FIXED_NOW_CENTRAL_LOCAL = "2026-04-05 13:00:00";

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeQueuedItem(item_id, overrides = {}) {
  return createPodioItem(item_id, {
    "queue-status": categoryField(overrides["queue-status"] ?? "Queued"),
    "scheduled-for-utc": dateField(
      overrides["scheduled-for-utc"] ?? "2026-04-06 10:00:00"
    ),
    "scheduled-for-local": dateField(
      overrides["scheduled-for-local"] ?? "2026-04-06 05:00:00"
    ),
    timezone: categoryField(overrides.timezone ?? "Central"),
    "contact-window": categoryField(overrides["contact-window"] ?? "9 AM - 8 PM"),
    "master-owner": appRefField(overrides["master-owner"] ?? 999),
  });
}

function makeDeps(items = [], updates = []) {
  return {
    fetchAllItems: async () => items,
    updateItem: async (item_id, payload) => {
      updates.push({ item_id, payload });
    },
    getCategoryValue,
    getDateValue,
    getFirstAppReferenceId,
    info: () => {},
    warn: () => {},
  };
}

// ─── 1. dry run returns candidates without mutating ───────────────────────────

test("forceDueQueuedItems dry_run=true returns eligible rows without calling updateItem", async () => {
  const updates = [];
  const items = [makeQueuedItem(101), makeQueuedItem(102)];
  const deps = makeDeps(items, updates);

  const result = await forceDueQueuedItems(
    { dry_run: true, now: FIXED_NOW },
    deps
  );

  assert.equal(result.ok, true);
  assert.equal(result.dry_run, true);
  assert.equal(result.total_rows_loaded, 2);
  assert.equal(result.eligible_rows, 2);
  assert.equal(result.rescheduled_count, 0, "dry_run must not increment rescheduled_count");
  assert.equal(updates.length, 0, "dry_run must not call updateItem");

  // first_10_actions must be populated for preview purposes
  assert.equal(result.first_10_actions.length, 2);
  assert.equal(result.first_10_candidate_item_ids.length, 2);
});

// ─── 2. live mode rewrites schedule fields to now ────────────────────────────

test("forceDueQueuedItems dry_run=false rewrites scheduled-for-utc and scheduled-for-local", async () => {
  const updates = [];
  const items = [makeQueuedItem(201)];
  const deps = makeDeps(items, updates);

  const result = await forceDueQueuedItems(
    { dry_run: false, now: FIXED_NOW },
    deps
  );

  assert.equal(result.ok, true);
  assert.equal(result.dry_run, false);
  assert.equal(result.rescheduled_count, 1);
  assert.equal(updates.length, 1);

  const { item_id, payload } = updates[0];
  assert.equal(item_id, 201);

  // UTC field must be set to now in Podio format
  assert.deepEqual(payload["scheduled-for-utc"], { start: FIXED_NOW_PODIO_UTC });

  // Local field must be set to now in Central local time
  assert.deepEqual(payload["scheduled-for-local"], { start: FIXED_NOW_CENTRAL_LOCAL });
});

// ─── 3. non-Queued rows are excluded ─────────────────────────────────────────

test("forceDueQueuedItems skips rows whose queue-status is not Queued", async () => {
  const updates = [];
  const items = [
    makeQueuedItem(301, { "queue-status": "Sent" }),
    makeQueuedItem(302, { "queue-status": "Failed" }),
    makeQueuedItem(303, { "queue-status": "Queued" }),
  ];
  const deps = makeDeps(items, updates);

  const result = await forceDueQueuedItems(
    { dry_run: false, now: FIXED_NOW },
    deps
  );

  assert.equal(result.total_rows_loaded, 3);
  assert.equal(result.eligible_rows, 1, "only the Queued row is eligible");
  assert.equal(result.rescheduled_count, 1);
  assert.equal(updates.length, 1);
  assert.equal(updates[0].item_id, 303);
});

// ─── 4. master_owner_id scope filter works ───────────────────────────────────

test("forceDueQueuedItems filters to master_owner_id when provided", async () => {
  const updates = [];
  const items = [
    makeQueuedItem(401, { "master-owner": 111 }),
    makeQueuedItem(402, { "master-owner": 222 }),
    makeQueuedItem(403, { "master-owner": 111 }),
  ];
  const deps = makeDeps(items, updates);

  const result = await forceDueQueuedItems(
    { dry_run: false, master_owner_id: 111, now: FIXED_NOW },
    deps
  );

  assert.equal(result.eligible_rows, 2);
  assert.equal(result.rescheduled_count, 2);
  assert.equal(result.master_owner_id, 111);
  assert.deepEqual(
    updates.map((u) => u.item_id).sort(),
    [401, 403]
  );
});

// ─── 5. response summary shape and first_10_actions content ──────────────────

test("forceDueQueuedItems response summary contains required fields and first_10_actions shape", async () => {
  const items = [makeQueuedItem(501), makeQueuedItem(502)];
  const deps = makeDeps(items);

  const result = await forceDueQueuedItems(
    { dry_run: true, now: FIXED_NOW },
    deps
  );

  // Top-level summary fields
  for (const key of [
    "ok", "dry_run", "run_started_at", "total_rows_loaded", "eligible_rows",
    "rescheduled_count", "skipped_count", "master_owner_id",
    "first_10_candidate_item_ids", "first_10_actions",
  ]) {
    assert.ok(key in result, `missing key: ${key}`);
  }

  // Per-action shape
  const action = result.first_10_actions[0];
  for (const key of [
    "queue_item_id", "old_scheduled_utc", "new_scheduled_utc",
    "old_scheduled_local", "new_scheduled_local", "timezone", "reason",
  ]) {
    assert.ok(key in action, `action missing key: ${key}`);
  }

  // new values must match now
  assert.equal(action.new_scheduled_utc, FIXED_NOW_PODIO_UTC);
  assert.equal(action.new_scheduled_local, FIXED_NOW_CENTRAL_LOCAL);
  assert.equal(action.reason, "force_due_rescheduled");
});

// ─── 6. limit cap is respected ────────────────────────────────────────────────

test("forceDueQueuedItems caps eligible_rows to the requested limit (hard cap 25)", async () => {
  // Generate 30 items
  const items = Array.from({ length: 30 }, (_, i) =>
    makeQueuedItem(600 + i)
  );
  const deps = makeDeps(items);

  const result_5 = await forceDueQueuedItems(
    { dry_run: true, limit: 5, now: FIXED_NOW },
    deps
  );
  assert.equal(result_5.eligible_rows, 5);
  assert.equal(result_5.first_10_actions.length, 5);

  const result_hard_cap = await forceDueQueuedItems(
    { dry_run: true, limit: 100, now: FIXED_NOW },  // over hard cap
    deps
  );
  assert.equal(
    result_hard_cap.eligible_rows,
    25,
    "hard cap of 25 must be enforced even when limit=100"
  );
});

// ─── 7. older_than_minutes filter: future rows pass, recent-due rows skip ─────

test("forceDueQueuedItems older_than_minutes targets future and long-overdue rows only", async () => {
  // All timestamps use the UTC "Z" suffix so new Date() parses them as UTC
  // regardless of the test machine's local timezone.  Space-separated strings
  // without "Z" are parsed as local time in V8, which breaks comparisons with
  // FIXED_NOW (which IS in UTC format).
  const FUTURE = "2030-01-01T10:00:00.000Z";       // far future → eligible
  const JUST_PAST = "2026-04-05T17:50:00.000Z";    // 10 min before FIXED_NOW (18:00Z) → inside 30-min threshold → skip
  const LONG_PAST = "2026-04-05T10:00:00.000Z";    // 8 h before FIXED_NOW → overdue past threshold → eligible

  const items = [
    makeQueuedItem(701, { "scheduled-for-utc": FUTURE }),    // eligible
    makeQueuedItem(702, { "scheduled-for-utc": JUST_PAST }), // skipped (within 30-min threshold)
    makeQueuedItem(703, { "scheduled-for-utc": LONG_PAST }), // eligible
  ];
  const deps = makeDeps(items);

  const result = await forceDueQueuedItems(
    { dry_run: true, older_than_minutes: 30, now: FIXED_NOW },
    deps
  );

  assert.equal(result.eligible_rows, 2);
  assert.deepEqual(
    result.first_10_candidate_item_ids.sort(),
    [701, 703]
  );
});

// ─── 8. timezone localization for non-Central rows ────────────────────────────

test("forceDueQueuedItems localizes scheduled-for-local to the row's timezone", async () => {
  const updates = [];
  const items = [
    makeQueuedItem(801, { timezone: "Eastern" }),
    makeQueuedItem(802, { timezone: "Pacific" }),
  ];
  const deps = makeDeps(items, updates);

  await forceDueQueuedItems({ dry_run: false, now: FIXED_NOW }, deps);

  // FIXED_NOW = 2026-04-05T18:00:00Z
  // Eastern (EDT = UTC-4): 14:00
  // Pacific (PDT = UTC-7): 11:00
  assert.equal(updates.length, 2);

  const eastern = updates.find((u) => u.item_id === 801);
  assert.ok(
    eastern.payload["scheduled-for-local"].start.includes("14:00"),
    `Eastern local expected 14:00, got ${eastern.payload["scheduled-for-local"].start}`
  );

  const pacific = updates.find((u) => u.item_id === 802);
  assert.ok(
    pacific.payload["scheduled-for-local"].start.includes("11:00"),
    `Pacific local expected 11:00, got ${pacific.payload["scheduled-for-local"].start}`
  );
});

// ─── 9. route handler: logs lifecycle events and defaults to dry_run=true ─────

test("handleQueueForceDueRequest logs lifecycle events and defaults to dry_run=true", async () => {
  const log_calls = [];
  const logger = {
    info: (event, meta) => log_calls.push({ level: "info", event, meta }),
    warn: (event, meta) => log_calls.push({ level: "warn", event, meta }),
    error: (event, meta) => log_calls.push({ level: "error", event, meta }),
  };

  const responses = [];
  const json_response = (body, init) => {
    const r = { body, status: init?.status ?? 200 };
    responses.push(r);
    return r;
  };

  const stub_result = {
    ok: true,
    dry_run: true,
    total_rows_loaded: 3,
    eligible_rows: 2,
    rescheduled_count: 0,
    skipped_count: 1,
    master_owner_id: null,
    first_10_candidate_item_ids: [101, 102],
    first_10_actions: [],
  };

  await handleQueueForceDueRequest(
    { url: "https://app.example.com/api/internal/queue/force-due" },
    "GET",
    {
      requireCronAuth: () => ({
        authorized: true,
        auth: { authenticated: true, is_vercel_cron: false },
        response: null,
      }),
      forceDueQueuedItems: async () => stub_result,
      logger,
      jsonResponse: json_response,
    }
  );

  assert.equal(responses.length, 1);
  assert.equal(responses[0].status, 200);
  assert.equal(responses[0].body.ok, true);
  assert.equal(responses[0].body.route, "internal/queue/force-due");
  assert.deepEqual(responses[0].body.result, stub_result);

  const events = log_calls.map((c) => c.event);
  assert.ok(events.includes("queue_force_due.route_enter"), "must log route_enter");
  assert.ok(events.includes("queue_force_due.requested"), "must log requested");
  assert.ok(events.includes("queue_force_due.completed"), "must log completed");
});

// ─── 10. route handler: unauthorized returns early ───────────────────────────

test("handleQueueForceDueRequest returns early without calling forceDueQueuedItems when auth fails", async () => {
  const force_due_calls = [];
  const sentinel = { sentinel: true };

  const returned = await handleQueueForceDueRequest(
    { url: "https://app.example.com/api/internal/queue/force-due" },
    "GET",
    {
      requireCronAuth: () => ({
        authorized: false,
        auth: { authenticated: false, is_vercel_cron: false },
        response: sentinel,
      }),
      forceDueQueuedItems: async () => {
        force_due_calls.push(1);
        return { ok: true };
      },
      logger: { info: () => {}, warn: () => {}, error: () => {} },
      jsonResponse: () => {},
    }
  );

  assert.equal(force_due_calls.length, 0, "forceDueQueuedItems must not be called when auth fails");
  assert.equal(returned, sentinel, "must return the auth rejection response");
});

// ─── 11. skipped_count reflects excluded rows ────────────────────────────────

test("forceDueQueuedItems skipped_count equals total_rows_loaded minus eligible_rows", async () => {
  const items = [
    makeQueuedItem(901),
    makeQueuedItem(902, { "queue-status": "Sent" }),
    makeQueuedItem(903, { "queue-status": "Failed" }),
    makeQueuedItem(904),
  ];
  const deps = makeDeps(items);

  const result = await forceDueQueuedItems(
    { dry_run: true, now: FIXED_NOW },
    deps
  );

  assert.equal(result.total_rows_loaded, 4);
  assert.equal(result.eligible_rows, 2);
  assert.equal(result.skipped_count, 2);
  assert.equal(result.skipped_count, result.total_rows_loaded - result.eligible_rows);
});
