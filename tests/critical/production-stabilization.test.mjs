/**
 * production-stabilization.test.mjs
 *
 * Focused regression tests for the production stabilization tasks.
 * Route files (.route.js) use next/server which is unavailable in raw Node
 * test runs, so those are tested via:
 *   - fs.existsSync (file presence / alias wiring)
 *   - inline auth replica (matching the pattern in podio-message-event-sync.test.mjs)
 *   - business-logic imports only (no NextResponse usage)
 *
 * Release-lock routes:
 *  1.  runs/release-lock route file exists
 *  2.  run-locks/release alias file exists and re-exports from runs/release-lock
 *  3.  auth guard rejects empty or matching secrets (inline)
 *
 * Feeder buildZeroInsertReason logic (file-private; replicated inline):
 *  4.  returns null when inserted_count > 0
 *  5.  returns no_source_items when loaded_count = 0
 *  6.  returns all_duplicates when eligible > 0, inserted = 0, dupes only
 *  7.  returns reason from result.skipped when lock is active
 *  8.  returns all_dnc when skip reasons include dnc
 *
 * Sync-podio worker:
 *  9.  syncSupabaseMessageEventsToPodio treats null podio_sync_status as pending
 * 10.  syncSupabaseMessageEventsToPodio marks null-status row synced on success
 * 11.  response includes non-negative duration_ms
 * 12.  response includes syncable_event_types with expected values
 * 13.  failure error detail includes row id
 *
 * Diagnostic route files (file existence only):
 * 14.  inbound-diagnostic route file exists
 * 15.  sync-podio-diagnostic route file exists
 */

import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

// ── Sync worker (no next/server dependency) ───────────────────────────────

import {
  syncSupabaseMessageEventsToPodio,
} from "@/lib/domain/events/sync-supabase-message-events-to-podio.js";

// ── Helpers ───────────────────────────────────────────────────────────────

const PROJECT_ROOT = process.cwd();
const src = (...parts) => path.join(PROJECT_ROOT, "src", ...parts);

function makeOutboundRow(overrides = {}) {
  return {
    id: 1001,
    message_event_key: "outbound_queue-42",
    provider_message_sid: "SM_abc123",
    direction: "outbound",
    event_type: "outbound_send",
    message_body: "Hello, interested in your property.",
    character_count: 35,
    delivery_status: "sent",
    sent_at: "2026-04-19T12:00:00.000Z",
    created_at: "2026-04-19T12:00:00.000Z",
    master_owner_id: 201,
    prospect_id: null,
    property_id: null,
    market_id: null,
    sms_agent_id: null,
    textgrid_number_id: null,
    template_id: null,
    brain_id: null,
    podio_sync_status: "pending",
    podio_sync_attempts: 0,
    metadata: {},
    ...overrides,
  };
}

/**
 * Minimal fake Supabase client satisfying the sync worker's chaining patterns:
 *   from().select().or().in().order().limit()  — returns rows
 *   from().update(...).eq("id", n)             — records update
 *   from().update(...).in("id", [...])         — records bulk update
 */
function makeSyncFakeSupabase({ rows = [] } = {}) {
  const updates = [];

  function updateTerminal(payload) {
    updates.push(payload);
    const terminal = { data: null, error: null };
    return { eq: () => terminal, in: () => terminal };
  }

  function selectChain(result_rows) {
    const c = {
      select: () => c,
      or:     () => c,
      in:     () => c,
      order:  () => c,
      limit:  () => ({ data: result_rows, error: null }),
      update: updateTerminal,
      eq:     () => ({ data: null, error: null }),
    };
    return c;
  }

  return {
    updates,
    client: { from: () => selectChain(rows) },
  };
}

// ── Inline replica of buildZeroInsertReason (private to route.js) ─────────

function buildZeroInsertReason(result = {}, summary = {}) {
  if (summary.inserted_count > 0) return null;
  if (result?.skipped) return result.reason || "feeder_run_skipped";
  if (summary.loaded_count === 0) return "no_source_items";
  if (summary.eligible_count === 0) {
    const reasons = Array.isArray(result?.skip_reason_counts) ? result.skip_reason_counts : [];
    const reason_keys = reasons.map((r) => String(r?.reason || r || ""));
    if (reason_keys.some((r) => r.includes("dnc") || r.includes("opt_out"))) return "all_dnc";
    if (reason_keys.some((r) => r.includes("phone"))) return "all_missing_phone";
    if (reason_keys.some((r) => r.includes("window"))) return "outside_contact_window";
    if (reason_keys.some((r) => r.includes("from") || r.includes("number"))) return "all_missing_from_number";
    return "no_eligible_items";
  }
  if (summary.duplicate_count > 0 && summary.error_count === 0) return "all_duplicates";
  if (summary.error_count > 0 && summary.duplicate_count === 0) return "all_errors";
  return "unknown_no_insert_reason";
}

// ── Inline auth check (mirrors shared-secret.js timing-safe compare) ──────

function checkInternalAuth(headers, secret) {
  const provided = String(headers["x-internal-api-secret"] ?? "").trim();
  if (!provided || !secret) return false;
  const a = Buffer.from(provided, "utf8");
  const b = Buffer.from(secret, "utf8");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

// ─── 1. runs/release-lock route file exists ──────────────────────────────

test("runs/release-lock route: file exists at expected path", () => {
  const file = src("app", "api", "internal", "runs", "release-lock", "route.js");
  assert.ok(existsSync(file), `expected route file to exist at ${file}`);
});

// ─── 2. run-locks/release alias file exists and re-exports from original ─

test("run-locks/release alias: file exists at expected path", () => {
  const file = src("app", "api", "internal", "run-locks", "release", "route.js");
  assert.ok(existsSync(file), `expected alias route file to exist at ${file}`);
});

test("run-locks/release alias: file content re-exports GET and POST from runs/release-lock", () => {
  const file = src("app", "api", "internal", "run-locks", "release", "route.js");
  const content = readFileSync(file, "utf8");
  assert.ok(
    content.includes("runs/release-lock"),
    "alias file must reference the original runs/release-lock route"
  );
  assert.ok(
    content.includes("GET") && content.includes("POST"),
    "alias file must export GET and POST"
  );
});

// ─── 3. auth guard rejects empty/wrong secrets ───────────────────────────

test("internal API auth: accepts correct x-internal-api-secret header", () => {
  const secret = "test-internal-secret-xyz";
  assert.ok(checkInternalAuth({ "x-internal-api-secret": secret }, secret));
});

test("internal API auth: rejects wrong x-internal-api-secret value", () => {
  assert.ok(!checkInternalAuth({ "x-internal-api-secret": "wrong" }, "correct"));
});

test("internal API auth: rejects missing x-internal-api-secret header", () => {
  assert.ok(!checkInternalAuth({}, "some-secret"));
});

// ─── 4-8. buildZeroInsertReason logic ────────────────────────────────────

test("buildZeroInsertReason: returns null when inserted_count > 0", () => {
  assert.equal(
    buildZeroInsertReason({}, { inserted_count: 3, loaded_count: 10, eligible_count: 5, duplicate_count: 0, error_count: 0 }),
    null
  );
});

test("buildZeroInsertReason: returns no_source_items when loaded_count = 0", () => {
  assert.equal(
    buildZeroInsertReason({}, { inserted_count: 0, loaded_count: 0, eligible_count: 0, duplicate_count: 0, error_count: 0 }),
    "no_source_items"
  );
});

test("buildZeroInsertReason: returns all_duplicates when eligible>0, inserted=0, duplicate_count>0", () => {
  assert.equal(
    buildZeroInsertReason({}, { inserted_count: 0, loaded_count: 10, eligible_count: 5, duplicate_count: 5, error_count: 0 }),
    "all_duplicates"
  );
});

test("buildZeroInsertReason: returns skipped reason from result.reason when lock is active", () => {
  assert.equal(
    buildZeroInsertReason({ skipped: true, reason: "run_lock_active" }, { inserted_count: 0, loaded_count: 0, eligible_count: 0, duplicate_count: 0, error_count: 0 }),
    "run_lock_active"
  );
});

test("buildZeroInsertReason: returns all_dnc when skip reasons include dnc", () => {
  assert.equal(
    buildZeroInsertReason(
      { skip_reason_counts: [{ reason: "dnc_blocked", count: 5 }] },
      { inserted_count: 0, loaded_count: 5, eligible_count: 0, duplicate_count: 0, error_count: 0 }
    ),
    "all_dnc"
  );
});

// ─── 9-10. sync worker treats null podio_sync_status as pending ──────────

test("syncSupabaseMessageEventsToPodio: picks up rows with null podio_sync_status", async () => {
  const row = makeOutboundRow({ podio_sync_status: null });
  const { client } = makeSyncFakeSupabase({ rows: [row] });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 77001 }),
  });

  assert.equal(result.synced_count, 1, "null-status row must count as synced");
  assert.equal(result.failed_count, 0);
});

test("syncSupabaseMessageEventsToPodio: marks null-status row synced with podio_message_event_id", async () => {
  const row = makeOutboundRow({ podio_sync_status: null });
  const { client, updates } = makeSyncFakeSupabase({ rows: [row] });

  await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 77001 }),
  });

  const success_update = updates.find((u) => u.podio_sync_status === "synced");
  assert.ok(success_update, "row must be marked synced");
  assert.equal(success_update.podio_message_event_id, "77001");
});

// ─── 11. sync response includes duration_ms ──────────────────────────────

test("syncSupabaseMessageEventsToPodio: response includes non-negative duration_ms", async () => {
  const { client } = makeSyncFakeSupabase({ rows: [] });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 0 }),
  });

  assert.equal(typeof result.duration_ms, "number", "duration_ms must be a number");
  assert.ok(result.duration_ms >= 0);
});

// ─── 12. sync response includes syncable_event_types ─────────────────────

test("syncSupabaseMessageEventsToPodio: response includes syncable_event_types with known values", async () => {
  const { client } = makeSyncFakeSupabase({ rows: [] });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 0 }),
  });

  assert.ok(Array.isArray(result.syncable_event_types));
  assert.ok(result.syncable_event_types.includes("outbound_send"));
  assert.ok(result.syncable_event_types.includes("inbound_sms"));
  assert.ok(result.syncable_event_types.includes("outbound_send_failed"));
});

// ─── 13. sync failure error detail includes row id ───────────────────────

test("syncSupabaseMessageEventsToPodio: failure error detail includes row id and error message", async () => {
  const row = makeOutboundRow({ id: 9999 });
  const { client } = makeSyncFakeSupabase({ rows: [row] });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => { throw new Error("Podio 500 error"); },
  });

  assert.equal(result.failed_count, 1);
  assert.equal(result.first_10_failed_errors.length, 1);
  assert.equal(result.first_10_failed_errors[0].id, 9999);
  assert.ok(result.first_10_failed_errors[0].error.includes("Podio 500 error"));
});

// ─── New sync-worker row-selection tests ─────────────────────────────────

test("syncSupabaseMessageEventsToPodio: loads failed row with attempts < max_attempts", async () => {
  const row = makeOutboundRow({ podio_sync_status: "failed", podio_sync_attempts: 1 });
  const { client } = makeSyncFakeSupabase({ rows: [row] });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 88002 }),
  });

  assert.equal(result.synced_count, 1, "failed row with attempts < max must be retried");
  assert.equal(result.failed_count, 0);
});

test("syncSupabaseMessageEventsToPodio: skips synced rows (status = synced excluded by filter)", async () => {
  const row = makeOutboundRow({ podio_sync_status: "synced", podio_message_event_id: "77001" });
  const { client } = makeSyncFakeSupabase({ rows: [row] });

  let podio_calls = 0;
  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => { podio_calls++; return { item_id: 0 }; },
  });

  assert.equal(podio_calls, 0, "Podio must NOT be called for already-synced rows");
  assert.equal(result.synced_count, 0);
  assert.equal(result.rows_after_attempt_filter, 0);
});

test("syncSupabaseMessageEventsToPodio: skips failed rows with attempts >= max_attempts", async () => {
  const row = makeOutboundRow({ podio_sync_status: "failed", podio_sync_attempts: 3 });
  const { client } = makeSyncFakeSupabase({ rows: [row] });

  let podio_calls = 0;
  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => { podio_calls++; return { item_id: 0 }; },
  });

  assert.equal(podio_calls, 0, "Podio must NOT be called for max-attempts-exhausted rows");
  assert.equal(result.rows_after_attempt_filter, 0);
});

test("syncSupabaseMessageEventsToPodio: does not skip inbound_sms with null message_body", async () => {
  const row = makeOutboundRow({
    id: 5001,
    event_type: "inbound_sms",
    direction: "inbound",
    message_body: null,
    podio_sync_status: "pending",
    podio_sync_attempts: 0,
  });
  const { client } = makeSyncFakeSupabase({ rows: [row] });

  const synced_payloads = [];
  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async (fields) => { synced_payloads.push(fields); return { item_id: 60001 }; },
  });

  assert.equal(result.synced_count, 1, "null-body inbound_sms must sync successfully");
  assert.equal(synced_payloads[0]["message"], "", "Podio message field must be empty string for null body");
});

test("syncSupabaseMessageEventsToPodio: response includes query_filters_used with event types", async () => {
  const { client } = makeSyncFakeSupabase({ rows: [] });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 0 }),
  });

  assert.ok(result.query_filters_used, "query_filters_used must be present");
  assert.ok(
    Array.isArray(result.query_filters_used.event_type_in),
    "query_filters_used.event_type_in must be array"
  );
  assert.ok(result.query_filters_used.event_type_in.includes("outbound_send"));
  assert.ok(result.query_filters_used.event_type_in.includes("inbound_sms"));
  assert.ok(typeof result.query_filters_used.podio_sync_attempts_lt === "number");
});

test("syncSupabaseMessageEventsToPodio: response includes row-filter diagnostic counts", async () => {
  const rows = [
    makeOutboundRow({ id: 1, podio_sync_status: "pending",  podio_sync_attempts: 0 }),
    makeOutboundRow({ id: 2, podio_sync_status: "failed",   podio_sync_attempts: 1 }),
  ];
  const { client } = makeSyncFakeSupabase({ rows });

  const result = await syncSupabaseMessageEventsToPodio({
    supabase: client,
    createMessageEvent: async () => ({ item_id: 70001 }),
  });

  assert.equal(result.raw_rows_loaded_before_filter, 2);
  assert.equal(result.rows_after_syncable_filter, 2);
  assert.equal(result.rows_after_attempt_filter, 2);
  assert.ok(Array.isArray(result.first_10_candidate_event_keys));
  assert.equal(result.first_10_candidate_event_keys.length, 2);
});

// ─── 14. inbound-diagnostic route file exists ────────────────────────────

test("inbound-diagnostic route: file exists at expected path", () => {
  const file = src("app", "api", "internal", "events", "inbound-diagnostic", "route.js");
  assert.ok(existsSync(file), `expected route file to exist at ${file}`);
});

// ─── 15. sync-podio-diagnostic route file exists ─────────────────────────

test("sync-podio-diagnostic route: file exists at expected path", () => {
  const file = src("app", "api", "internal", "events", "sync-podio-diagnostic", "route.js");
  assert.ok(existsSync(file), `expected route file to exist at ${file}`);
});

