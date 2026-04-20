/**
 * sentry-instrumentation.test.mjs
 *
 * Focused tests for the shared Sentry monitoring helper and the critical
 * instrumentation points across the SMS automation routes.
 *
 * Covered:
 * 1. captureRouteException: calls withScope and captureException.
 * 2. captureRouteException: sets route, subsystem, and environment tags.
 * 3. captureRouteException: attaches route_context when context is provided.
 * 4. captureRouteException: never includes known secret key names in context.
 * 5. captureRouteException: tolerates missing options without throwing.
 * 6. addSentryBreadcrumb: calls addBreadcrumb with category, message, data.
 * 7. addSentryBreadcrumb: always includes level: "info".
 * 8. writeOutboundFailureMessageEvent: calls captureRouteException with the
 *    original send error and safe queue context.
 * 9. writeOutboundFailureMessageEvent: never passes secret values to Sentry.
 * 10. syncSupabaseMessageEventsToPodio: calls captureRouteException on a
 *     per-row Podio failure (one failure never aborts the rest of the batch).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  captureRouteException,
  addSentryBreadcrumb,
  __setSentryDeps,
  __resetSentryDeps,
} from "@/lib/monitoring/sentry.js";

import {
  writeOutboundFailureMessageEvent,
} from "@/lib/supabase/sms-engine.js";

import {
  syncSupabaseMessageEventsToPodio,
  __setSyncPodioDeps,
  __resetSyncPodioDeps,
} from "@/lib/domain/events/sync-supabase-message-events-to-podio.js";

// ─── Mock Sentry builder ─────────────────────────────────────────────────────

function makeMockSentry() {
  const calls = {
    withScope: [],
    captureException: [],
    addBreadcrumb: [],
    tags: [],
    contexts: [],
  };

  const scope = {
    setTag: (key, value) => calls.tags.push({ key, value }),
    setContext: (name, ctx) => calls.contexts.push({ name, ctx }),
  };

  return {
    calls,
    withScope: (callback) => {
      calls.withScope.push(callback);
      callback(scope);
    },
    captureException: (error) => {
      calls.captureException.push(error);
    },
    addBreadcrumb: (breadcrumb) => {
      calls.addBreadcrumb.push(breadcrumb);
    },
  };
}

// ─── 1. captureRouteException calls withScope and captureException ────────────

test("captureRouteException: calls withScope and captureException", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    const error = new Error("test error");
    captureRouteException(error, { route: "test/route", subsystem: "test_sub" });

    assert.equal(mock.calls.withScope.length, 1, "withScope should be called once");
    assert.equal(mock.calls.captureException.length, 1, "captureException should be called once");
    assert.equal(mock.calls.captureException[0], error, "captureException receives the original error");
  } finally {
    __resetSentryDeps();
  }
});

// ─── 2. captureRouteException sets required tags ──────────────────────────────

test("captureRouteException: sets route, subsystem, and environment tags", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    captureRouteException(new Error("oops"), {
      route: "webhooks/textgrid/inbound",
      subsystem: "textgrid_inbound",
    });

    const tag_map = Object.fromEntries(mock.calls.tags.map(({ key, value }) => [key, value]));

    assert.equal(tag_map["route"], "webhooks/textgrid/inbound");
    assert.equal(tag_map["subsystem"], "textgrid_inbound");
    assert.ok(tag_map["environment"], "environment tag should be set");
  } finally {
    __resetSentryDeps();
  }
});

// ─── 3. captureRouteException attaches route_context ─────────────────────────

test("captureRouteException: attaches route_context when context is provided", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    captureRouteException(new Error("ctx error"), {
      route: "sms-engine/test",
      subsystem: "sms_engine",
      context: { queue_row_id: 42, queue_key: "abc123", master_owner_id: 99 },
    });

    assert.equal(mock.calls.contexts.length, 1, "setContext should be called once");
    const { name, ctx } = mock.calls.contexts[0];
    assert.equal(name, "route_context");
    assert.equal(ctx.queue_row_id, 42);
    assert.equal(ctx.queue_key, "abc123");
    assert.equal(ctx.master_owner_id, 99);
  } finally {
    __resetSentryDeps();
  }
});

// ─── 4. captureRouteException never includes known secret keys ────────────────

const FORBIDDEN_CONTEXT_KEYS = [
  "INTERNAL_API_SECRET",
  "SUPABASE_SERVICE_ROLE_KEY",
  "PODIO_CLIENT_SECRET",
  "CRON_SECRET",
  "authorization",
  "x-internal-api-secret",
];

test("captureRouteException: never includes secret key names in context", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    // Simulate someone accidentally passing a context with a secret key.
    // The wrapper should still pass through — the contract is on the CALLER
    // not to include secrets. This test verifies we don't accidentally add them
    // ourselves.
    captureRouteException(new Error("safe"), {
      route: "test/route",
      subsystem: "test",
      context: { queue_row_id: 1, queue_key: "k1" },
    });

    const captured_context = mock.calls.contexts[0]?.ctx || {};
    for (const forbidden_key of FORBIDDEN_CONTEXT_KEYS) {
      assert.ok(
        !(forbidden_key in captured_context),
        `Forbidden key "${forbidden_key}" must not appear in Sentry context`
      );
    }
  } finally {
    __resetSentryDeps();
  }
});

// ─── 5. captureRouteException tolerates missing options ───────────────────────

test("captureRouteException: does not throw when called with no options", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    assert.doesNotThrow(() => {
      captureRouteException(new Error("bare call"));
    });
    assert.equal(mock.calls.captureException.length, 1);
  } finally {
    __resetSentryDeps();
  }
});

// ─── 6. addSentryBreadcrumb calls addBreadcrumb with correct shape ────────────

test("addSentryBreadcrumb: calls addBreadcrumb with category, message, and data", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    addSentryBreadcrumb("sms_send", "sms_send_succeeded", {
      queue_row_id: 7,
      queue_key: "mykey",
    });

    assert.equal(mock.calls.addBreadcrumb.length, 1);
    const crumb = mock.calls.addBreadcrumb[0];
    assert.equal(crumb.category, "sms_send");
    assert.equal(crumb.message, "sms_send_succeeded");
    assert.equal(crumb.data.queue_row_id, 7);
    assert.equal(crumb.data.queue_key, "mykey");
  } finally {
    __resetSentryDeps();
  }
});

// ─── 7. addSentryBreadcrumb always sets level: "info" ────────────────────────

test("addSentryBreadcrumb: always includes level info", () => {
  const mock = makeMockSentry();
  __setSentryDeps({ sentry: mock });

  try {
    addSentryBreadcrumb("textgrid_inbound", "inbound_message_accepted", {});

    assert.equal(mock.calls.addBreadcrumb[0].level, "info");
  } finally {
    __resetSentryDeps();
  }
});

// ─── 8. writeOutboundFailureMessageEvent calls captureRouteException ──────────

test("writeOutboundFailureMessageEvent: captures the original send error to Sentry", async () => {
  const captured = [];
  const mock_sentry = {
    ...makeMockSentry(),
    captureException: (err) => captured.push(err),
    withScope: (callback) => {
      callback({
        setTag: () => {},
        setContext: () => {},
      });
    },
  };
  __setSentryDeps({ sentry: mock_sentry });

  const send_error = new Error("textgrid_send_failed: upstream error");

  const fake_row = {
    id: 101,
    queue_key: "test-key",
    master_owner_id: 55,
    message_body: "Hello there",
    to_phone_number: "+15551234567",
    from_phone_number: "+15559876543",
    retry_count: 0,
    max_retries: 3,
    queue_status: "queued",
  };

  try {
    await writeOutboundFailureMessageEvent(fake_row, send_error, {
      writeOutboundFailureMessageEvent: async () => ({ ok: true }),
    });

    assert.equal(
      captured.length,
      1,
      "captureException should be called once with the send error"
    );
    assert.equal(captured[0], send_error, "captured error must be the original send error");
  } finally {
    __resetSentryDeps();
  }
});

// ─── 9. writeOutboundFailureMessageEvent: no secrets in Sentry context ────────

test("writeOutboundFailureMessageEvent: Sentry context contains no secret keys", async () => {
  const contexts_captured = [];
  const mock_sentry = {
    withScope: (callback) => {
      const scope = {
        setTag: () => {},
        setContext: (name, ctx) => contexts_captured.push({ name, ctx }),
      };
      callback(scope);
    },
    captureException: () => {},
  };
  __setSentryDeps({ sentry: mock_sentry });

  const fake_row = {
    id: 202,
    queue_key: "secret-test",
    master_owner_id: 77,
    message_body: "msg",
    to_phone_number: "+15550000001",
    from_phone_number: "+15550000002",
    retry_count: 0,
    max_retries: 3,
    queue_status: "queued",
  };

  try {
    await writeOutboundFailureMessageEvent(fake_row, new Error("fail"), {
      writeOutboundFailureMessageEvent: async () => ({ ok: true }),
    });

    for (const { ctx } of contexts_captured) {
      for (const forbidden_key of FORBIDDEN_CONTEXT_KEYS) {
        assert.ok(
          !(forbidden_key in (ctx || {})),
          `Forbidden key "${forbidden_key}" must not appear in Sentry failure context`
        );
      }
    }
  } finally {
    __resetSentryDeps();
  }
});

// ─── 10. syncSupabaseMessageEventsToPodio: captures per-row Podio error ───────

test("syncSupabaseMessageEventsToPodio: calls captureRouteException on per-row Podio failure without aborting batch", async () => {
  const captured_errors = [];
  const mock_sentry = {
    withScope: (callback) => {
      callback({
        setTag: () => {},
        setContext: () => {},
      });
    },
    captureException: (err) => captured_errors.push(err),
    addBreadcrumb: () => {},
  };
  __setSentryDeps({ sentry: mock_sentry });

  const podio_error = new Error("Podio 503");

  const rows = [
    {
      id: 1,
      event_type: "outbound_send",
      direction: "outbound",
      message_event_key: "row-1",
      podio_sync_attempts: 0,
    },
    {
      id: 2,
      event_type: "inbound_sms",
      direction: "inbound",
      message_event_key: "row-2",
      podio_sync_attempts: 1,
    },
  ];

  const updates = [];

  const fake_supabase = {
    from: () => ({
      select: () => ({
        in: () => ({
          or: () => ({
            order: () => ({
              limit: async () => ({ data: rows, error: null }),
            }),
          }),
        }),
      }),
      update: (payload) => ({
        eq: (col, val) => {
          updates.push({ payload, col, val });
          return { data: null, error: null };
        },
        in: () => ({ data: null, error: null }),
      }),
    }),
  };

  __setSyncPodioDeps({
    createMessageEvent: async () => {
      throw podio_error;
    },
  });

  try {
    const result = await syncSupabaseMessageEventsToPodio({
      supabase: fake_supabase,
      limit: 10,
    });

    // Both rows failed — ensures batch continued despite per-row failure
    assert.equal(result.failed, 2, "both rows should be counted as failed");
    assert.equal(result.synced, 0, "no rows should be synced");

    // Sentry should have been called once per row failure
    assert.equal(
      captured_errors.length,
      2,
      "captureException should be called for each failed row"
    );
    assert.equal(captured_errors[0], podio_error);
    assert.equal(captured_errors[1], podio_error);
  } finally {
    __resetSentryDeps();
    __resetSyncPodioDeps();
  }
});
