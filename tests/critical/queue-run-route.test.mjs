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
