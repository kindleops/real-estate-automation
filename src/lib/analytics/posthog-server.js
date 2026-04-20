/**
 * posthog-server.js
 *
 * Thin PostHog server-side wrapper for SMS automation analytics.
 *
 * Safety rules:
 * - Never throw — analytics must never block the SMS pipeline.
 * - No-op when POSTHOG_KEY is absent (test/dev environments).
 * - Never include auth secrets, service role keys, or raw message bodies.
 * - Use distinctId "system" for automated pipeline events.
 *
 * Events captured by this module:
 *   feeder_run_completed, feeder_run_skipped
 *   queue_run_completed, queue_item_claimed
 *   sms_send_started, sms_send_succeeded, sms_send_failed
 *   message_event_created
 *   inbound_sms_logged, inbound_sms_classified
 *   sms_delivery_updated
 *   message_event_sync_to_podio_completed, message_event_sync_to_podio_failed
 */

import { PostHog } from "posthog-node";

// ---------------------------------------------------------------------------
// Dependency injection (for tests)
// ---------------------------------------------------------------------------

function makeDefaultDeps() {
  const key = process.env.POSTHOG_KEY;
  const host = process.env.POSTHOG_HOST || "https://us.i.posthog.com";

  if (!key) {
    return { client: null };
  }

  const client = new PostHog(key, {
    host,
    flushAt: 20,
    flushInterval: 10_000,
  });

  return { client };
}

let runtimeDeps = makeDefaultDeps();

export function __setPostHogDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetPostHogDeps() {
  runtimeDeps = makeDefaultDeps();
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const FORBIDDEN_KEYS = new Set([
  "INTERNAL_API_SECRET",
  "CRON_SECRET",
  "SUPABASE_SERVICE_ROLE_KEY",
  "SUPABASE_ANON_KEY",
  "PODIO_CLIENT_ID",
  "PODIO_CLIENT_SECRET",
  "PODIO_APP_TOKEN",
  "PODIO_REFRESH_TOKEN",
  "TEXTGRID_ACCOUNT_SID",
  "TEXTGRID_AUTH_TOKEN",
  "TEXTGRID_WEBHOOK_SIGNING_KEY",
  "POSTHOG_KEY",
  "password",
  "token",
  "secret",
  "auth",
]);

function isForbiddenKey(key) {
  const lower = String(key).toLowerCase();
  for (const forbidden of FORBIDDEN_KEYS) {
    if (lower.includes(forbidden.toLowerCase())) return true;
  }
  return false;
}

function sanitizeProperties(props) {
  if (!props || typeof props !== "object") return {};
  const safe = {};
  for (const [key, value] of Object.entries(props)) {
    if (isForbiddenKey(key)) continue;
    // Never include raw message bodies
    if (key === "message_body" || key === "body" || key === "message_text") continue;
    safe[key] = value;
  }
  return safe;
}

function getEnvironmentTag() {
  return process.env.VERCEL_ENV || process.env.NODE_ENV || "development";
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Capture a server-side PostHog event.
 *
 * @param {string} distinctId  Identity of the actor (use "system" for automated events)
 * @param {string} event       PostHog event name (snake_case)
 * @param {object} [properties] Non-secret properties to attach
 */
export function captureEvent(distinctId, event, properties = {}) {
  try {
    const { client } = runtimeDeps;
    if (!client) return;

    client.capture({
      distinctId: distinctId || "system",
      event,
      properties: {
        environment: getEnvironmentTag(),
        ...sanitizeProperties(properties),
      },
    });
  } catch (_err) {
    // Never propagate analytics errors
  }
}

/**
 * Capture a pipeline-level event with distinctId = "system".
 *
 * @param {string} event
 * @param {object} [properties]
 */
export function captureSystemEvent(event, properties = {}) {
  captureEvent("system", event, properties);
}

/**
 * Flush pending events and shut down the PostHog client.
 * Call during graceful shutdown only — not needed for short-lived route handlers.
 */
export async function shutdownPostHog() {
  try {
    const { client } = runtimeDeps;
    if (!client) return;
    await client.shutdown();
  } catch (_err) {
    // Never propagate
  }
}
