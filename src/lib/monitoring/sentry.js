/**
 * sentry.js
 *
 * Thin wrapper around @sentry/nextjs that provides consistent tagging and
 * context for all critical SMS automation routes.
 *
 * Safety rules:
 * - Never include auth headers, INTERNAL_API_SECRET, service role keys, or
 *   Podio client secrets in the `context` object passed here.
 * - Only include operationally useful, non-sensitive identifiers:
 *   queue_row_id, queue_key, provider_message_id, master_owner_id, etc.
 */

import * as Sentry from "@sentry/nextjs";
import { sendSystemErrorAlert } from "@/lib/alerts/discord.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";

// ---------------------------------------------------------------------------
// Dependency injection (for tests)
// ---------------------------------------------------------------------------

const defaultDeps = {
  sentry: Sentry,
};

let runtimeDeps = { ...defaultDeps };

export function __setSentryDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetSentryDeps() {
  runtimeDeps = { ...defaultDeps };
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

/**
 * Capture an exception with standard route/subsystem tags.
 *
 * @param {Error | unknown} error
 * @param {object} [opts]
 * @param {string} [opts.route]      Human-readable route identifier, e.g. "webhooks/textgrid/inbound"
 * @param {string} [opts.subsystem]  Subsystem tag, e.g. "textgrid_inbound"
 * @param {object} [opts.context]    Non-secret context key/values attached to the Sentry event
 */
export function captureRouteException(error, { route, subsystem, context } = {}) {
  // Guard: Sentry may not be initialized in test environments without an init call.
  if (typeof runtimeDeps.sentry.withScope !== "function") return;

  runtimeDeps.sentry.withScope((scope) => {
    if (route) scope.setTag("route", route);
    if (subsystem) scope.setTag("subsystem", subsystem);
    const env = process.env.VERCEL_ENV || process.env.NODE_ENV || "development";
    scope.setTag("environment", env);
    if (context && typeof context === "object") {
      scope.setContext("route_context", context);
    }
    runtimeDeps.sentry.captureException(error);
  });

  sendSystemErrorAlert({
    title: "Route Exception Captured",
    description: (error?.message || String(error)).slice(0, 256),
    color: 0xe74c3c,
    fields: [
      ...(route ? [{ name: "Route", value: String(route), inline: true }] : []),
      ...(subsystem ? [{ name: "Subsystem", value: String(subsystem), inline: true }] : []),
    ],
    timestamp: new Date().toISOString(),
    footer: { text: subsystem || route || "sentry" },
  });

  // Best-effort Discord Ops OS routing; never blocks primary error handling.
  notifyDiscordOps({
    event_type: "sentry_error",
    severity: "critical",
    domain: "sentry",
    title: "Sentry Route Exception",
    summary: (error?.message || String(error)).slice(0, 240),
    fields: [
      ...(route ? [{ name: "Route", value: String(route), inline: true }] : []),
      ...(subsystem ? [{ name: "Subsystem", value: String(subsystem), inline: true }] : []),
    ],
    metadata: context || {},
    should_alert_critical: true,
  }).catch(() => {});
}

/**
 * Add a breadcrumb for tracing key events within a request lifecycle.
 *
 * @param {string} category  e.g. "sms_send"
 * @param {string} message   Human-readable description
 * @param {object} [data]    Additional non-secret key/value details
 */
export function addSentryBreadcrumb(category, message, data = {}) {
  if (typeof runtimeDeps.sentry.addBreadcrumb !== "function") return;
  runtimeDeps.sentry.addBreadcrumb({ category, message, data, level: "info" });
}
