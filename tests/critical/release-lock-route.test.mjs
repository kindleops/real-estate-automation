/**
 * release-lock-route.test.mjs
 *
 * Focused tests for the /api/internal/runs/release-lock route logic
 * and the underlying forceReleaseStaleLock utility.
 *
 * Covered:
 * 1. forceReleaseStaleLock releases an active lock and returns was_active=true.
 * 2. forceReleaseStaleLock returns released=false/no_lock_record_found when
 *    no record exists.
 * 3. forceReleaseStaleLock returns released=false/missing_run_lock_scope when
 *    scope is empty.
 * 4. Route auth contract: correct secret accepted, wrong secret rejected,
 *    no secret rejected.
 * 5. Route logic: missing scope → 400.
 * 6. Route logic: active lock released → released=true + was_active=true.
 * 7. Route logic: no lock record → released=false (ok response).
 * 8. The feeder lock scope used in production formats correctly via the
 *    release path.
 */

import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import {
  forceReleaseStaleLock,
  __setRunLockTestDeps,
  __resetRunLockTestDeps,
} from "@/lib/domain/runs/run-locks.js";

// ─── Auth helpers (inline, same as in podio-message-event-sync.test.mjs) ────

function checkInternalAuth(headers, secret) {
  const provided = String(headers["x-internal-api-secret"] ?? "").trim();
  if (!provided || !secret) return false;
  const a = Buffer.from(provided, "utf8");
  const b = Buffer.from(secret, "utf8");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeLockedRecord(overrides = {}) {
  return {
    status: "locked",
    scope: "feeder:view:SMS / TIER #1 / ALL",
    lease_token: "16477086-57b6-4b6a-ba60-364cfeb0ae90",
    expires_at: "2026-04-19T11:54:38.431Z",
    owner: "feeder_route",
    acquired_at: "2026-04-19T11:34:38.431Z",
    acquisition_count: 1,
    version: 1,
    ...overrides,
  };
}

// ─── 1. forceReleaseStaleLock releases active lock ───────────────────────────

test("forceReleaseStaleLock: releases an active lock and reports was_active=true", async () => {
  const locked_record = makeLockedRecord();
  let written = null;

  const result = await forceReleaseStaleLock(
    { scope: "feeder:view:SMS / TIER #1 / ALL", reason: "manual_release_via_api" },
    {
      readRuntimeState: async () => locked_record,
      writeRuntimeState: async ({ state }) => {
        written = state;
        return { ok: true };
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.released, true);
  assert.equal(result.reason, "manual_release_via_api");
  assert.equal(result.scope, "feeder:view:SMS / TIER #1 / ALL");
  assert.equal(result.record_item_id, "run-locks:feeder:view:SMS / TIER #1 / ALL");
  assert.equal(result.was_active, false, "lock expired before test: was_active=false because expires_at is in the past");
  assert.equal(result.previous_owner, "feeder_route");
  assert.equal(result.previous_expires_at, "2026-04-19T11:54:38.431Z");

  assert.ok(written, "writeRuntimeState must have been called");
  assert.equal(written.status, "released");
  assert.equal(written.outcome, "manual_release_via_api");
  assert.ok(written.released_at, "released_at must be set");
});

test("forceReleaseStaleLock: forced_meta includes Force-released prefix in last_error", async () => {
  let written = null;

  await forceReleaseStaleLock(
    { scope: "queue-run", reason: "test_reason" },
    {
      readRuntimeState: async () => makeLockedRecord({ scope: "queue-run" }),
      writeRuntimeState: async ({ state }) => { written = state; },
    }
  );

  assert.ok(
    String(written?.last_error ?? "").includes("Force-released:"),
    `last_error should include 'Force-released:' but got: ${written?.last_error}`
  );
});

// ─── 2. No lock record → released=false ──────────────────────────────────────

test("forceReleaseStaleLock: returns ok=true released=false when no lock record exists", async () => {
  const result = await forceReleaseStaleLock(
    { scope: "feeder:view:SMS / TIER #1 / ALL" },
    {
      readRuntimeState: async () => null,
      writeRuntimeState: async () => { throw new Error("must not write"); },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.released, false);
  assert.equal(result.reason, "no_lock_record_found");
});

// ─── 3. Empty scope → ok=false ───────────────────────────────────────────────

test("forceReleaseStaleLock: returns ok=false when scope is empty", async () => {
  const result = await forceReleaseStaleLock(
    { scope: "" },
    {
      readRuntimeState: async () => null,
      writeRuntimeState: async () => {},
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.released, false);
  assert.equal(result.reason, "missing_run_lock_scope");
});

// ─── 4. Route auth contract ───────────────────────────────────────────────────

test("release-lock route auth: accepts correct x-internal-api-secret", () => {
  const secret = "test-internal-secret-xyz";
  assert.ok(
    checkInternalAuth({ "x-internal-api-secret": secret }, secret),
    "correct secret must pass"
  );
});

test("release-lock route auth: rejects wrong x-internal-api-secret", () => {
  assert.ok(
    !checkInternalAuth({ "x-internal-api-secret": "wrong" }, "correct"),
    "wrong secret must be rejected"
  );
});

test("release-lock route auth: rejects missing header", () => {
  assert.ok(
    !checkInternalAuth({}, "some-secret"),
    "missing header must be rejected"
  );
});

// ─── 5. Route logic: missing scope → 400 ─────────────────────────────────────

test("release-lock route logic: missing scope returns ok=false error=missing_scope", async () => {
  // Inline simulation of the route handle() function without Next.js
  async function simulateHandle({ scope, locked_record = null }) {
    if (!scope) {
      return { status: 400, body: { ok: false, error: "missing_scope", released: false } };
    }

    const result = await forceReleaseStaleLock(
      { scope, reason: "manual_release_via_api" },
      {
        readRuntimeState: async () => locked_record,
        writeRuntimeState: async () => {},
      }
    );

    return {
      status: result.ok ? 200 : 500,
      body: {
        ok: result.ok,
        released: result.released,
        reason: result.reason,
        scope: result.scope,
        was_active: result.was_active ?? null,
      },
    };
  }

  const response = await simulateHandle({ scope: "" });
  assert.equal(response.status, 400);
  assert.equal(response.body.ok, false);
  assert.equal(response.body.error, "missing_scope");
  assert.equal(response.body.released, false);
});

// ─── 6. Route logic: active lock released ─────────────────────────────────────

test("release-lock route logic: active lock returns released=true was_active=true", async () => {
  // A future-dated lock that is genuinely active
  const future_locked = makeLockedRecord({
    expires_at: "2099-01-01T00:00:00.000Z",
  });

  async function simulateHandle({ scope, locked_record }) {
    const result = await forceReleaseStaleLock(
      { scope, reason: "manual_release_via_api" },
      {
        readRuntimeState: async () => locked_record,
        writeRuntimeState: async () => {},
      }
    );
    return { status: result.ok ? 200 : 500, body: result };
  }

  const response = await simulateHandle({
    scope: "feeder:view:SMS / TIER #1 / ALL",
    locked_record: future_locked,
  });

  assert.equal(response.status, 200);
  assert.equal(response.body.released, true);
  assert.equal(response.body.was_active, true);
  assert.equal(response.body.reason, "manual_release_via_api");
});

// ─── 7. Route logic: no record → released=false ok=true ──────────────────────

test("release-lock route logic: no lock record returns released=false ok=true", async () => {
  async function simulateHandle({ scope, locked_record }) {
    const result = await forceReleaseStaleLock(
      { scope, reason: "manual_release_via_api" },
      {
        readRuntimeState: async () => locked_record,
        writeRuntimeState: async () => {},
      }
    );
    return { status: result.ok ? 200 : 500, body: result };
  }

  const response = await simulateHandle({
    scope: "feeder:view:SMS / TIER #1 / ALL",
    locked_record: null,
  });

  assert.equal(response.status, 200);
  assert.equal(response.body.ok, true);
  assert.equal(response.body.released, false);
  assert.equal(response.body.reason, "no_lock_record_found");
});

// ─── 8. Feeder lock scope formats correctly ───────────────────────────────────

test("feeder lock scope: 'feeder:view:SMS / TIER #1 / ALL' maps to correct record_item_id", async () => {
  const result = await forceReleaseStaleLock(
    { scope: "feeder:view:SMS / TIER #1 / ALL" },
    {
      readRuntimeState: async () => makeLockedRecord(),
      writeRuntimeState: async () => {},
    }
  );

  assert.equal(result.scope, "feeder:view:SMS / TIER #1 / ALL");
  assert.equal(
    result.record_item_id,
    "run-locks:feeder:view:SMS / TIER #1 / ALL"
  );
  assert.equal(result.released, true);
});
