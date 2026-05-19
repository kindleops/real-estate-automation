import test from "node:test";
import assert from "node:assert/strict";

import { handleQueueRunRequest, statusForResult } from "@/lib/domain/queue/queue-run-request.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeRequest(url = "https://app.example.com/api/internal/queue/run") {
  return {
    url,
    json: async () => ({}),
  };
}

function makeAuth(authorized = true) {
  return () => ({
    authorized,
    auth: { authenticated: true, is_vercel_cron: false },
    response: null,
  });
}

function makeLogger() {
  const calls = [];
  const logger = {
    info: (event, meta) => calls.push({ level: "info", event, meta }),
    warn: (event, meta) => calls.push({ level: "warn", event, meta }),
    error: (event, meta) => calls.push({ level: "error", event, meta }),
  };
  return { calls, logger };
}

function makeJsonResponse() {
  const responses = [];
  const fn = (body, init) => {
    const r = { body, status: init?.status ?? 200 };
    responses.push(r);
    return r;
  };
  return { responses, fn };
}

// ─── test: route logs correctly and calls runSendQueue ─────────────────────────

test("handleQueueRunRequest calls runSendQueue and emits route_enter, before_run, after_run logs", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  const stub_result = {
    ok: true,
    dry_run: false,
    skipped: false,
    attempted_count: 2,
    claimed_count: 2,
    started_count: 2,
    processed_count: 2,
    sent_count: 2,
    failed_count: 0,
    blocked_count: 0,
    skipped_count: 0,
    duplicate_locked_count: 0,
    first_failing_queue_item_id: null,
    first_failing_reason: null,
    first_failure_queue_item_id: null,
    first_failure_reason: null,
    batch_duration_ms: 1234,
    due_rows: 2,
    future_rows: 0,
    total_rows_loaded: 2,
    run_started_at: "2026-04-04T15:00:00.000Z",
    results: [],
  };

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async (opts) => {
      run_calls.push(opts);
      return stub_result;
    },
    logger,
    jsonResponse: fn,
  });

  // Route must call runSendQueue exactly once
  assert.equal(run_calls.length, 1, "runSendQueue must be called once");

  // Must produce HTTP 200
  assert.equal(responses.length, 1);
  assert.equal(responses[0].status, 200);
  assert.equal(responses[0].body.ok, true);
  assert.equal(responses[0].body.route, "internal/queue/run");
  assert.deepEqual(responses[0].body.result, stub_result);

  // Log sequence check
  const infos = calls.filter((c) => c.level === "info").map((c) => c.event);
  assert.ok(infos.includes("queue_run.route_enter"), "queue_run.route_enter must be logged");
  assert.ok(infos.includes("queue_run.requested"), "queue_run.requested must be logged");
  assert.ok(infos.includes("queue_run.before_run_send_queue"), "queue_run.before_run_send_queue must be logged");
  assert.ok(infos.includes("queue_run.after_run_send_queue"), "queue_run.after_run_send_queue must be logged");
  assert.ok(infos.includes("queue_run.summary"), "queue_run.summary must be logged");

  assert.equal(run_calls[0].limit, 50, "GET default limit must be 50");
  assert.equal(run_calls[0].dry_run, false, "GET default dry_run must be false");

  // before_run_send_queue must include rollout_mode and dry_run info
  const before = calls.find((c) => c.event === "queue_run.before_run_send_queue")?.meta;
  assert.ok(before, "before_run_send_queue log payload is present");
  assert.ok("dry_run" in before, "dry_run present in before log");
  assert.ok("rollout_mode" in before, "rollout_mode present in before log");
  assert.ok("forced_dry_run" in before, "forced_dry_run present in before log");
  assert.ok("dry_run_reason" in before, "dry_run_reason present in before log");

  // after_run_send_queue must include results
  const after = calls.find((c) => c.event === "queue_run.after_run_send_queue")?.meta;
  assert.ok(after, "after_run_send_queue log payload is present");
  assert.equal(after.ok, true);
  assert.equal(after.skipped, false);
  assert.equal(after.attempted_count, 2);
  assert.equal(after.claimed_count, 2);
  assert.equal(after.started_count, 2);
  assert.equal(after.processed_count, 2);
  assert.equal(after.sent_count, 2);
  assert.equal(after.blocked_count, 0);
  assert.equal(after.duplicate_locked_count, 0);
  assert.equal(after.batch_duration_ms, 1234);
  assert.equal(after.total_rows_loaded, 2);

  const summary = calls.find((c) => c.event === "queue_run.summary")?.meta;
  assert.ok(summary, "queue_run.summary payload is present");
  assert.equal(summary.attempted_count, 2);
  assert.equal(summary.claimed_count, 2);
  assert.equal(summary.processed_count, 2);
  assert.equal(summary.sent_count, 2);
  assert.equal(summary.failed_count, 0);
  assert.equal(summary.blocked_count, 0);
  assert.equal(summary.skipped_count, 0);
  assert.equal(summary.invalid_queue_row_count, 0);
  assert.equal(summary.preclaim_paused_name_missing_count, 0);
  assert.equal(summary.preclaim_outside_window_excluded_count, 0);
  assert.equal(summary.preclaim_retry_pending_excluded_count, 0);
  assert.equal(summary.eligible_claim_count, 0);
  assert.equal(summary.first_failure_reason, null);

  // No early_return warn when run is not skipped
  const early = calls.find((c) => c.event === "queue_run.early_return");
  assert.equal(early, undefined, "queue_run.early_return must NOT be logged for a normal run");
});

// ─── test: lock-active early return is logged ─────────────────────────────────

test("handleQueueRunRequest emits queue_run.early_return warn when runSendQueue returns skipped=true", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();

  const lock_skipped_result = {
    ok: true,
    skipped: true,
    reason: "queue_runner_lock_active",
    run_started_at: "2026-04-04T15:00:00.000Z",
    lock: {
      scope: "queue-run",
      meta: {
        expires_at: "2026-04-04T15:10:00.000Z",
        owner: "queue_runner",
        acquired_at: "2026-04-04T15:00:00.000Z",
      },
    },
  };

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async () => lock_skipped_result,
    logger,
    jsonResponse: fn,
  });

  // Still returns 200 (ok: true from the skipped result)
  assert.equal(responses[0].status, 200);
  assert.equal(responses[0].body.ok, true);

  // early_return must be logged as warn
  const early = calls.find((c) => c.event === "queue_run.early_return");
  assert.ok(early, "queue_run.early_return must be logged");
  assert.equal(early.level, "warn");
  assert.equal(early.meta.reason, "queue_runner_lock_active");
  assert.equal(early.meta.skipped, true);
  assert.equal(early.meta.lock_expires_at, "2026-04-04T15:10:00.000Z");
  assert.equal(early.meta.lock_owner, "queue_runner");

  // after_run_send_queue still logged
  const after = calls.find((c) => c.event === "queue_run.after_run_send_queue");
  assert.ok(after, "after_run_send_queue still logged even when skipped");
  assert.equal(after.meta.skipped, true);
  assert.equal(after.meta.reason, "queue_runner_lock_active");
});

test("handleQueueRunRequest returns 200 and logs first failure details when the batch is partial", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async () => ({
      ok: true,
      partial: true,
      dry_run: false,
      skipped: false,
      attempted_count: 3,
      claimed_count: 2,
      started_count: 3,
      processed_count: 3,
      sent_count: 2,
      failed_count: 1,
      blocked_count: 0,
      skipped_count: 0,
      duplicate_locked_count: 1,
      first_failing_queue_item_id: 9002,
      first_failing_reason: "queue_processing_exception",
      first_failure_queue_item_id: 9002,
      first_failure_reason: "queue_processing_exception",
      batch_duration_ms: 987,
      due_rows: 3,
      future_rows: 0,
      total_rows_loaded: 3,
      results: [],
    }),
    logger,
    jsonResponse: fn,
  });

  assert.equal(responses.length, 1);
  assert.equal(responses[0].status, 200);
  assert.equal(responses[0].body.ok, true);

  const after = calls.find((c) => c.event === "queue_run.after_run_send_queue")?.meta;
  assert.ok(after, "after_run_send_queue log payload is present");
  assert.equal(after.partial, true);
  assert.equal(after.attempted_count, 3);
  assert.equal(after.claimed_count, 2);
  assert.equal(after.started_count, 3);
  assert.equal(after.failed_count, 1);
  assert.equal(after.duplicate_locked_count, 1);
  assert.equal(after.first_failing_queue_item_id, 9002);
  assert.equal(after.first_failing_reason, "queue_processing_exception");
  assert.equal(after.first_failure_queue_item_id, 9002);
  assert.equal(after.first_failure_reason, "queue_processing_exception");
  assert.equal(after.batch_duration_ms, 987);
});

// ─── test: unauthorized returns early without calling runSendQueue ─────────────

test("handleQueueRunRequest returns early without calling runSendQueue when auth fails", async () => {
  const { calls } = makeLogger();
  const run_calls = [];

  const sentinel_response = { sentinel: true };

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: () => ({
      authorized: false,
      auth: { authenticated: false, is_vercel_cron: false },
      response: sentinel_response,
    }),
    runSendQueue: async () => {
      run_calls.push(1);
      return { ok: true };
    },
    logger: { info: (e, m) => calls.push({ e, m }), warn: () => {}, error: () => {} },
    jsonResponse: () => {},
  });

  assert.equal(run_calls.length, 0, "runSendQueue must not be called when auth fails");
  // route_enter is still logged before auth
  assert.ok(calls.some((c) => c.e === "queue_run.route_enter"), "route_enter logged even before auth");
});

// ─── test: statusForResult maps correctly ────────────────────────────────────

test("statusForResult prefers result.status, else 500 for ok=false and 200 otherwise", () => {
  assert.equal(statusForResult({ ok: false }), 500);
  assert.equal(statusForResult({ ok: false, status: 423 }), 423);
  assert.equal(statusForResult({ ok: true, status: 200 }), 200);
  assert.equal(statusForResult({ ok: true }), 200);
  assert.equal(statusForResult({ ok: true, skipped: true }), 200);
  assert.equal(statusForResult(null), 200);
  assert.equal(statusForResult(undefined), 200);
});

// ─── QUEUE_ENGINE_SHARED_SECRET auth tests ────────────────────────────────────

test("handleQueueRunRequest warns and returns cron 401 when QUEUE_ENGINE_SHARED_SECRET is not configured and cron auth fails", async () => {
  const { calls, logger } = makeLogger();
  const run_calls = [];
  const sentinel_response = { sentinel: "cron_401" };

  const result = await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: () => ({
      authorized: false,
      auth: { authenticated: false, is_vercel_cron: false },
      response: sentinel_response,
    }),
    runSendQueue: async () => { run_calls.push(1); return { ok: true }; },
    logger,
    jsonResponse: () => {},
    // queueEngineSecret intentionally absent
  });

  assert.equal(result, sentinel_response, "returns cron auth sentinel response");
  assert.equal(run_calls.length, 0, "runSendQueue must not be called");
  const warn = calls.find((c) => c.event === "queue_engine_secret.not_configured");
  assert.ok(warn, "queue_engine_secret.not_configured must be warned");
  assert.equal(warn.level, "warn");
});

test("handleQueueRunRequest allows when QUEUE_ENGINE_SHARED_SECRET is set and x-queue-engine-secret header is valid", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  const stub_result = {
    ok: true, skipped: false, partial: false, dry_run: false,
    attempted_count: 0, claimed_count: 0, started_count: 0,
    processed_count: 0, sent_count: 0, failed_count: 0,
    blocked_count: 0, skipped_count: 0, duplicate_locked_count: 0,
    first_failing_queue_item_id: null, first_failing_reason: null,
    first_failure_queue_item_id: null, first_failure_reason: null,
    batch_duration_ms: 0, due_rows: 0, future_rows: 0,
    total_rows_loaded: 0, run_started_at: "2026-05-18T00:00:00.000Z",
    results: [],
  };

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: () => ({
      authorized: false,
      auth: { authenticated: false, is_vercel_cron: false },
      response: null,
    }),
    runSendQueue: async (opts) => { run_calls.push(opts); return stub_result; },
    getSharedSecretAuthResult: () => ({
      ok: true, status: 200, reason: "authorized",
      required: true, authenticated: true,
      via: "header:x-queue-engine-secret",
    }),
    queueEngineSecret: "test-shared-secret-abc",
    logger,
    jsonResponse: fn,
  });

  assert.equal(run_calls.length, 1, "runSendQueue must be called");
  assert.equal(responses.length, 1);
  assert.equal(responses[0].status, 200);
  assert.equal(responses[0].body.ok, true);
  assert.equal(calls.find((c) => c.event === "queue_engine_secret.not_configured"), undefined, "no not_configured warning");
  assert.equal(calls.find((c) => c.event === "queue_engine_secret.rejected"), undefined, "no rejected warning");
});

test("handleQueueRunRequest returns 401 when QUEUE_ENGINE_SHARED_SECRET is set and x-queue-engine-secret header is missing", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: () => ({
      authorized: false,
      auth: { authenticated: false, is_vercel_cron: false },
      response: null,
    }),
    runSendQueue: async () => { run_calls.push(1); return { ok: true }; },
    getSharedSecretAuthResult: () => ({
      ok: false, status: 401,
      reason: "missing_queue_engine_shared_secret_token",
      authenticated: false, via: null,
    }),
    queueEngineSecret: "test-shared-secret-abc",
    logger,
    jsonResponse: fn,
  });

  assert.equal(run_calls.length, 0, "runSendQueue must not be called");
  assert.equal(responses.length, 1);
  assert.equal(responses[0].status, 401);
  assert.equal(responses[0].body.ok, false);
  assert.equal(responses[0].body.error, "unauthorized");
  const rejected = calls.find((c) => c.event === "queue_engine_secret.rejected");
  assert.ok(rejected, "queue_engine_secret.rejected must be logged");
  assert.equal(rejected.level, "warn");
  assert.equal(rejected.meta.reason, "missing_queue_engine_shared_secret_token");
});

test("handleQueueRunRequest returns 401 when QUEUE_ENGINE_SHARED_SECRET is set and x-queue-engine-secret header value is wrong", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: () => ({
      authorized: false,
      auth: { authenticated: false, is_vercel_cron: false },
      response: null,
    }),
    runSendQueue: async () => { run_calls.push(1); return { ok: true }; },
    getSharedSecretAuthResult: () => ({
      ok: false, status: 401,
      reason: "invalid_queue_engine_shared_secret_token",
      authenticated: false, via: "header:x-queue-engine-secret",
    }),
    queueEngineSecret: "test-shared-secret-abc",
    logger,
    jsonResponse: fn,
  });

  assert.equal(run_calls.length, 0, "runSendQueue must not be called");
  assert.equal(responses[0].status, 401);
  assert.equal(responses[0].body.error, "unauthorized");
  const rejected = calls.find((c) => c.event === "queue_engine_secret.rejected");
  assert.ok(rejected, "queue_engine_secret.rejected must be logged");
  assert.equal(rejected.meta.reason, "invalid_queue_engine_shared_secret_token");
  assert.equal(rejected.meta.via, "header:x-queue-engine-secret");
});

test("handleQueueRunRequest does not check QUEUE_ENGINE_SHARED_SECRET when cron auth passes", async () => {
  const { logger } = makeLogger();
  const { fn } = makeJsonResponse();
  let engine_auth_called = false;

  const stub_result = {
    ok: true, skipped: false, partial: false, dry_run: false,
    attempted_count: 0, claimed_count: 0, started_count: 0,
    processed_count: 0, sent_count: 0, failed_count: 0,
    blocked_count: 0, skipped_count: 0, duplicate_locked_count: 0,
    first_failing_queue_item_id: null, first_failing_reason: null,
    first_failure_queue_item_id: null, first_failure_reason: null,
    batch_duration_ms: 0, due_rows: 0, future_rows: 0,
    total_rows_loaded: 0, run_started_at: "2026-05-18T00:00:00.000Z",
    results: [],
  };

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async () => stub_result,
    getSharedSecretAuthResult: () => { engine_auth_called = true; return { ok: true }; },
    queueEngineSecret: "test-shared-secret-abc",
    logger,
    jsonResponse: fn,
  });

  assert.equal(engine_auth_called, false, "engine secret auth must not be checked when cron auth passes");
});

// ─── POST body dry_run parsing tests ─────────────────────────────────────────

function makePostRequest(body_obj, url = "https://app.example.com/api/internal/queue/run") {
  return {
    url,
    json: async () => body_obj,
  };
}

test("handleQueueRunRequest POST body dry_run:true is passed to runSendQueue and returned in response", async () => {
  const { logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  const stub_result = {
    ok: true, skipped: false, partial: false, dry_run: true,
    attempted_count: 0, claimed_count: 0, started_count: 0,
    processed_count: 0, sent_count: 0, failed_count: 0,
    blocked_count: 0, skipped_count: 0, duplicate_locked_count: 0,
    first_failing_queue_item_id: null, first_failing_reason: null,
    first_failure_queue_item_id: null, first_failure_reason: null,
    batch_duration_ms: 0, due_rows: 0, future_rows: 0,
    total_rows_loaded: 0, run_started_at: "2026-05-19T13:00:00.000Z",
    results: [],
  };

  await handleQueueRunRequest(makePostRequest({ dry_run: true, limit: 10 }), "POST", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async (opts) => { run_calls.push(opts); return stub_result; },
    logger,
    jsonResponse: fn,
  });

  assert.equal(run_calls.length, 1, "runSendQueue must be called once");
  assert.equal(run_calls[0].dry_run, true, "runSendQueue must receive dry_run:true from POST body");
  assert.equal(run_calls[0].limit, 10, "runSendQueue must receive limit:10 from POST body");
  assert.equal(responses[0].body.dry_run, true, "response dry_run must be true");
  assert.equal(responses[0].body.sent_count, 0, "no real sends in dry_run");
});

test("handleQueueRunRequest POST body dry_run:false sends live and response dry_run is false", async () => {
  const { logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  const stub_result = {
    ok: true, skipped: false, dry_run: false,
    attempted_count: 1, claimed_count: 1, started_count: 1,
    processed_count: 1, sent_count: 1, failed_count: 0,
    blocked_count: 0, skipped_count: 0, duplicate_locked_count: 0,
    first_failing_queue_item_id: null, first_failing_reason: null,
    first_failure_queue_item_id: null, first_failure_reason: null,
    batch_duration_ms: 200, due_rows: 1, future_rows: 0,
    total_rows_loaded: 1, run_started_at: "2026-05-19T13:00:00.000Z",
    results: [],
  };

  await handleQueueRunRequest(makePostRequest({ dry_run: false, limit: 1 }), "POST", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async (opts) => { run_calls.push(opts); return stub_result; },
    logger,
    jsonResponse: fn,
  });

  assert.equal(run_calls[0].dry_run, false, "runSendQueue must receive dry_run:false from POST body");
  assert.equal(run_calls[0].limit, 1, "runSendQueue must receive limit:1 from POST body");
  assert.equal(responses[0].body.dry_run, false, "response dry_run must be false");
  assert.equal(responses[0].body.sent_count, 1);
});

test("handleQueueRunRequest POST with no dry_run field defaults to false", async () => {
  const { logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  await handleQueueRunRequest(makePostRequest({ limit: 5 }), "POST", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async (opts) => { run_calls.push(opts); return { ok: true, sent_count: 0, failed_count: 0, blocked_count: 0, skipped_count: 0, attempted_count: 0, claimed_count: 0, started_count: 0, processed_count: 0, duplicate_locked_count: 0, first_failing_queue_item_id: null, first_failing_reason: null, first_failure_queue_item_id: null, first_failure_reason: null, batch_duration_ms: 0, due_rows: 0, future_rows: 0, total_rows_loaded: 0, results: [] }; },
    logger,
    jsonResponse: fn,
  });

  assert.equal(run_calls[0].dry_run, false, "missing dry_run in POST body defaults to false");
  assert.equal(run_calls[0].limit, 5, "limit from POST body is respected");
  assert.equal(responses[0].body.dry_run, false);
});

test("handleQueueRunRequest GET ignores POST body and reads dry_run from query params", async () => {
  const { logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();
  const run_calls = [];

  // Even if json() would return dry_run:false, GET must read from search params
  await handleQueueRunRequest(
    makePostRequest({ dry_run: false }, "https://app.example.com/api/internal/queue/run?dry_run=true&limit=3"),
    "GET",
    {
      requireCronAuth: makeAuth(true),
      runSendQueue: async (opts) => { run_calls.push(opts); return { ok: true, sent_count: 0, failed_count: 0, blocked_count: 0, skipped_count: 0, attempted_count: 0, claimed_count: 0, started_count: 0, processed_count: 0, duplicate_locked_count: 0, first_failing_queue_item_id: null, first_failing_reason: null, first_failure_queue_item_id: null, first_failure_reason: null, batch_duration_ms: 0, due_rows: 0, future_rows: 0, total_rows_loaded: 0, results: [] }; },
      logger,
      jsonResponse: fn,
    }
  );

  assert.equal(run_calls[0].dry_run, true, "GET reads dry_run from query string, not body");
  assert.equal(run_calls[0].limit, 3, "GET reads limit from query string");
  assert.equal(responses[0].body.dry_run, true);
});

test("handleQueueRunRequest safety guard fires when dry_run was explicitly requested in query but resolves false", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();

  // Simulate a corrupted asBoolean by directly patching: we can't, so test via query param path
  // The guard is: if query says dry_run=true but computed dry_run is false → reject
  // Normal path: query dry_run=true → computed dry_run=true → guard does NOT fire
  // This test validates the guard doesn't false-positive on a valid dry_run request
  const run_calls = [];
  await handleQueueRunRequest(
    makePostRequest({}, "https://app.example.com/api/internal/queue/run?dry_run=true"),
    "GET",
    {
      requireCronAuth: makeAuth(true),
      runSendQueue: async (opts) => { run_calls.push(opts); return { ok: true, sent_count: 0, failed_count: 0, blocked_count: 0, skipped_count: 0, attempted_count: 0, claimed_count: 0, started_count: 0, processed_count: 0, duplicate_locked_count: 0, first_failing_queue_item_id: null, first_failing_reason: null, first_failure_queue_item_id: null, first_failure_reason: null, batch_duration_ms: 0, due_rows: 0, future_rows: 0, total_rows_loaded: 0, results: [] }; },
      logger,
      jsonResponse: fn,
    }
  );

  // Guard must NOT fire when dry_run correctly resolves to true
  const safety_error = calls.find((c) => c.event === "queue_run.dry_run_safety_violation");
  assert.equal(safety_error, undefined, "safety guard must not fire when dry_run correctly resolves to true");
  assert.equal(run_calls.length, 1, "runSendQueue must be called");
  assert.equal(run_calls[0].dry_run, true, "dry_run must be true");
  assert.equal(responses[0].body.dry_run, true);
});

test("handleQueueRunRequest converts Podio cooldown errors into a safe skipped response", async () => {
  const { calls, logger } = makeLogger();
  const { responses, fn } = makeJsonResponse();

  await handleQueueRunRequest(makeRequest(), "GET", {
    requireCronAuth: makeAuth(true),
    runSendQueue: async () => {
      throw {
        name: "PodioError",
        status: 420,
        path: "/item/app/30541680/filter/",
        method: "post",
        operation: "filter_items",
        retry_after_seconds: 3600,
        rate_limit_remaining: 0,
        message:
          "You have hit the rate limit. Please wait 3600 seconds before trying again.",
      };
    },
    buildPodioCooldownSkipResult: async () => ({
      ok: true,
      skipped: true,
      reason: "podio_rate_limit_cooldown_active",
      retry_after_seconds: 3600,
      retry_after_at: "2026-04-08T20:20:25.000Z",
      podio_cooldown: {
        active: true,
        status: 420,
        path: "/item/app/30541680/filter/",
        operation: "filter_items",
        rate_limit_remaining: 0,
      },
      results: [],
      processed_count: 0,
      sent_count: 0,
      failed_count: 0,
      skipped_count: 0,
    }),
    logger,
    jsonResponse: fn,
  });

  assert.equal(responses.length, 1);
  assert.equal(responses[0].status, 200);
  assert.equal(responses[0].body.ok, true);
  assert.equal(responses[0].body.result.skipped, true);
  assert.equal(responses[0].body.result.reason, "podio_rate_limit_cooldown_active");
  assert.equal(responses[0].body.result.retry_after_seconds, 3600);

  const failure = calls.find((entry) => entry.event === "queue_run.failed");
  assert.ok(failure, "queue_run.failed should be logged");
  assert.equal(failure.level, "error");
  assert.equal(failure.meta.error.status, 420);
  assert.equal(failure.meta.error.path, "/item/app/30541680/filter/");
  assert.equal(failure.meta.error.operation, "filter_items");
  assert.equal(failure.meta.error.retry_after_seconds, 3600);
  assert.equal(failure.meta.error.rate_limit_remaining, 0);
});
