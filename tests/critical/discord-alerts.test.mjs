/**
 * discord-alerts.test.mjs
 *
 * Verifies Discord alert helper behaviour and wiring across the SMS
 * automation engine.
 *
 * Coverage:
 *   - no-op when DISCORD_*_WEBHOOK_URL env var is not set
 *   - no-op for unknown channel names
 *   - never throws when fetch throws
 *   - formats critical alert embed payload correctly
 *   - strips fields with forbidden names (secrets)
 *   - sendCriticalAlert routes to the "critical" channel
 *   - sendHotLeadAlert routes to the "hot_leads" channel
 *   - sendSystemErrorAlert routes to the "sentry_errors" channel
 *   - writeOutboundFailureMessageEvent fires a Discord critical alert
 *   - syncSupabaseMessageEventsToPodio per-row failure fires a Discord alert
 *   - captureRouteException fires Discord system-error alert when Sentry available
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  __setDiscordDeps,
  __resetDiscordDeps,
  sendDiscordAlert,
  sendCriticalAlert,
  sendHotLeadAlert,
  sendSystemErrorAlert,
} from "@/lib/alerts/discord.js";

import {
  writeOutboundFailureMessageEvent,
} from "@/lib/supabase/sms-engine.js";

import { syncSupabaseMessageEventsToPodio } from "@/lib/domain/events/sync-supabase-message-events-to-podio.js";

import {
  __setSentryDeps,
  __resetSentryDeps,
  captureRouteException,
} from "@/lib/monitoring/sentry.js";

import {
  __setPostHogDeps,
  __resetPostHogDeps,
} from "@/lib/analytics/posthog-server.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Inject a fake fetch and return the captured call list */
function injectFakeFetch() {
  const calls = [];
  const fakeFetch = async (url, options) => {
    calls.push({ url, body: JSON.parse(options?.body ?? "{}") });
    return { ok: true, status: 204 };
  };
  __setDiscordDeps({ fetch: fakeFetch });
  return calls;
}

/** Set a Discord channel env var, return a cleanup function */
function setWebhookUrl(envKey, url = "https://discord.example.com/webhook/test") {
  const previous = process.env[envKey];
  process.env[envKey] = url;
  return () => {
    if (previous === undefined) {
      delete process.env[envKey];
    } else {
      process.env[envKey] = previous;
    }
  };
}

// ---------------------------------------------------------------------------
// Tests: discord.js helper
// ---------------------------------------------------------------------------

test("sendDiscordAlert no-ops when webhook URL env var is not set", async () => {
  // Ensure the env var is absent
  const key = "DISCORD_CRITICAL_ALERTS_WEBHOOK_URL";
  const previous = process.env[key];
  delete process.env[key];

  const calls = injectFakeFetch();

  try {
    await sendDiscordAlert("critical", { title: "Test", description: "Should not send" });
    assert.equal(calls.length, 0, "fetch should not be called when URL is missing");
  } finally {
    if (previous !== undefined) process.env[key] = previous;
    __resetDiscordDeps();
  }
});

test("sendDiscordAlert no-ops for unknown channel name", async () => {
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL");
  const calls = injectFakeFetch();

  try {
    await sendDiscordAlert("unknown_channel", { title: "Test" });
    assert.equal(calls.length, 0, "fetch should not be called for unknown channel");
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

test("sendDiscordAlert does not throw when fetch throws", async () => {
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL");
  __setDiscordDeps({
    fetch: async () => { throw new Error("Network error"); },
  });

  try {
    // Must resolve without throwing
    await assert.doesNotReject(
      () => sendDiscordAlert("critical", { title: "Test" })
    );
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

test("sendCriticalAlert formats Discord embed payload correctly", async () => {
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL");
  const calls = injectFakeFetch();

  try {
    await sendCriticalAlert({
      title: "Queue Run Failures",
      description: "3 sends failed",
      color: 0xe74c3c,
      fields: [
        { name: "Failed", value: "3", inline: true },
        { name: "Sent", value: "7", inline: true },
      ],
      timestamp: "2026-01-01T00:00:00.000Z",
      footer: { text: "queue_run_completed" },
    });

    assert.equal(calls.length, 1, "fetch should be called once");
    const { body } = calls[0];
    assert.equal(Array.isArray(body.embeds), true, "body should contain embeds array");

    const embed = body.embeds[0];
    assert.equal(embed.title, "Queue Run Failures");
    assert.equal(embed.description, "3 sends failed");
    assert.equal(embed.color, 0xe74c3c);
    assert.equal(embed.timestamp, "2026-01-01T00:00:00.000Z");
    assert.deepEqual(embed.footer, { text: "queue_run_completed" });
    assert.equal(embed.fields.length, 2);
    assert.equal(embed.fields[0].name, "Failed");
    assert.equal(embed.fields[0].value, "3");
    assert.equal(embed.fields[0].inline, true);
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

test("sendCriticalAlert strips fields with forbidden names (secrets)", async () => {
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL");
  const calls = injectFakeFetch();

  try {
    await sendCriticalAlert({
      title: "Alert With Secrets",
      fields: [
        { name: "Queue Row ID", value: "123", inline: true },
        { name: "api_key", value: "sk-should-be-stripped", inline: false },
        { name: "internal_api_secret", value: "stripped-too", inline: false },
        { name: "message_body", value: "stripped-body", inline: false },
        { name: "Error", value: "something went wrong", inline: false },
      ],
    });

    assert.equal(calls.length, 1);
    const { body } = calls[0];
    const fields = body.embeds[0].fields;

    const fieldNames = fields.map((f) => f.name);
    assert.ok(fieldNames.includes("Queue Row ID"), "safe field should be included");
    assert.ok(!fieldNames.includes("api_key"), "api_key should be stripped");
    assert.ok(!fieldNames.includes("internal_api_secret"), "internal_api_secret should be stripped");
    assert.ok(!fieldNames.includes("message_body"), "message_body should be stripped");
    assert.ok(fieldNames.includes("Error"), "Error field should be included");
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

test("sendCriticalAlert routes to DISCORD_CRITICAL_ALERTS_WEBHOOK_URL", async () => {
  const webhook_url = "https://discord.example.com/webhook/critical-test";
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL", webhook_url);
  const calls = injectFakeFetch();

  try {
    await sendCriticalAlert({ title: "Critical Test" });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, webhook_url);
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

test("sendHotLeadAlert routes to DISCORD_HOT_LEADS_WEBHOOK_URL", async () => {
  const webhook_url = "https://discord.example.com/webhook/hot-leads-test";
  const cleanup = setWebhookUrl("DISCORD_HOT_LEADS_WEBHOOK_URL", webhook_url);
  const calls = injectFakeFetch();

  try {
    await sendHotLeadAlert({ title: "Hot Lead Test" });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, webhook_url);
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

test("sendSystemErrorAlert routes to DISCORD_SENTRY_ERRORS_WEBHOOK_URL", async () => {
  const webhook_url = "https://discord.example.com/webhook/sentry-test";
  const cleanup = setWebhookUrl("DISCORD_SENTRY_ERRORS_WEBHOOK_URL", webhook_url);
  const calls = injectFakeFetch();

  try {
    await sendSystemErrorAlert({ title: "Error Test" });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, webhook_url);
  } finally {
    cleanup();
    __resetDiscordDeps();
  }
});

// ---------------------------------------------------------------------------
// Integration: writeOutboundFailureMessageEvent fires Discord alert
// ---------------------------------------------------------------------------

test("writeOutboundFailureMessageEvent fires Discord critical alert", async () => {
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL");
  const discordCalls = injectFakeFetch();

  // Silence PostHog
  __setPostHogDeps({ client: null });

  // Silence Sentry (no withScope → captureRouteException returns early,
  // but we still need the fake Sentry object to satisfy the guard)
  const fakeSentry = {
    withScope: (fn) => fn({ setTag: () => {}, setContext: () => {} }),
    captureException: () => {},
    addBreadcrumb: () => {},
  };
  __setSentryDeps({ sentry: fakeSentry });

  try {
    const row = {
      id: 42,
      queue_key: "owner_42_touch_1",
      master_owner_id: 1001,
      touch_number: 1,
      template_id: 99,
    };
    const err = new Error("TextGrid 500");

    await writeOutboundFailureMessageEvent(row, err, {
      writeOutboundFailureMessageEvent: () => ({ ok: false }),
    });

    assert.ok(discordCalls.length >= 1, "Discord should be called at least once");
    const embed = discordCalls[0].body.embeds[0];
    assert.equal(embed.title, "SMS Send Failed");
    assert.ok(
      embed.description.includes("42"),
      "description should reference the queue row id"
    );
    const fieldNames = embed.fields.map((f) => f.name);
    assert.ok(fieldNames.includes("Error"), "embed should have an Error field");
  } finally {
    cleanup();
    __resetDiscordDeps();
    __resetPostHogDeps();
    __resetSentryDeps();
  }
});

// ---------------------------------------------------------------------------
// Integration: syncSupabaseMessageEventsToPodio per-row failure fires alert
// ---------------------------------------------------------------------------

test("syncSupabaseMessageEventsToPodio row failure fires Discord critical alert", async () => {
  const cleanup = setWebhookUrl("DISCORD_CRITICAL_ALERTS_WEBHOOK_URL");
  const discordCalls = injectFakeFetch();

  // Silence PostHog
  __setPostHogDeps({ client: null });

  const { __setSyncPodioDeps, __resetSyncPodioDeps } = await import(
    "@/lib/domain/events/sync-supabase-message-events-to-podio.js"
  );

  // createMessageEvent throws to simulate Podio failure
  __setSyncPodioDeps({
    createMessageEvent: async () => { throw new Error("Podio API error"); },
  });

  // Supabase stub: returns one syncable row, update/select chains resolve cleanly
  const makeChain = () => {
    const chain = {
      from() { return chain; },
      select() { return chain; },
      update() { return chain; },
      eq() { return chain; },
      limit() { return Promise.resolve({ data: [], error: null }); },
    };
    return chain;
  };

  const rows = [{
    id: 7,
    message_event_key: "evt_test_007",
    event_type: "outbound_send",
    direction: "outbound",
    podio_sync_attempts: 0,
  }];

  let selectCallCount = 0;
  const supabase = {
    from(table) {
      return {
        select() { return this; },
        not() { return this; },
        in() { return this; },
        eq() { return this; },
        is() { return this; },
        or() { return this; },
        order() { return this; },
        limit() {
          selectCallCount++;
          // First call returns the row; subsequent calls (for update) resolve empty
          if (selectCallCount === 1) {
            return Promise.resolve({ data: rows, error: null });
          }
          return Promise.resolve({ data: [], error: null });
        },
        update() { return this; },
      };
    },
  };

  try {
    await syncSupabaseMessageEventsToPodio({ supabase });

    assert.ok(discordCalls.length >= 1, "Discord should be called on Podio sync failure");
    const embed = discordCalls[0].body.embeds[0];
    assert.equal(embed.title, "Podio Sync Failure");
    const fieldNames = embed.fields.map((f) => f.name);
    assert.ok(fieldNames.includes("Message Event Key"), "embed should have Message Event Key field");
    assert.ok(fieldNames.includes("Error"), "embed should have Error field");
  } finally {
    cleanup();
    __resetDiscordDeps();
    __resetPostHogDeps();
    __resetSyncPodioDeps();
  }
});

// ---------------------------------------------------------------------------
// Integration: captureRouteException fires Discord system-error alert
// ---------------------------------------------------------------------------

test("captureRouteException fires Discord system-error alert when Sentry is available", async () => {
  const cleanup = setWebhookUrl("DISCORD_SENTRY_ERRORS_WEBHOOK_URL");
  const discordCalls = injectFakeFetch();

  const fakeSentry = {
    withScope: (fn) => fn({ setTag: () => {}, setContext: () => {} }),
    captureException: () => {},
  };
  __setSentryDeps({ sentry: fakeSentry });

  try {
    captureRouteException(new Error("Critical route failure"), {
      route: "sms-engine/test",
      subsystem: "sms_engine",
    });

    // sendSystemErrorAlert is async — wait a tick for the microtask to settle
    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.ok(discordCalls.length >= 1, "Discord should be called when Sentry captures");
    const embed = discordCalls[0].body.embeds[0];
    assert.equal(embed.title, "Route Exception Captured");
    assert.ok(
      embed.description.includes("Critical route failure"),
      "description should include error message"
    );
    const fieldNames = embed.fields.map((f) => f.name);
    assert.ok(fieldNames.includes("Route"), "embed should have Route field");
  } finally {
    cleanup();
    __resetDiscordDeps();
    __resetSentryDeps();
  }
});
