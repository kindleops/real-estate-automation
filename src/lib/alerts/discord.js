/**
 * discord.js
 *
 * Thin Discord webhook alert helper for high-signal operational alerts.
 *
 * Safety rules:
 * - Never includes auth headers, INTERNAL_API_SECRET, service role keys,
 *   Podio client secrets, or message bodies in alert payloads.
 * - Never throws — all errors are silently swallowed.
 * - No-ops when the target webhook URL env var is not configured.
 */

// ---------------------------------------------------------------------------
// Channel → env var mapping
// ---------------------------------------------------------------------------

const CHANNEL_ENV_MAP = {
  critical:       "DISCORD_CRITICAL_ALERTS_WEBHOOK_URL",
  hot_leads:      "DISCORD_HOT_LEADS_WEBHOOK_URL",
  sentry_errors:  "DISCORD_SENTRY_ERRORS_WEBHOOK_URL",
  posthog_alerts: "DISCORD_POSTHOG_ALERTS_WEBHOOK_URL",
  deploys:        "DISCORD_DEPLOYS_WEBHOOK_URL",
};

// ---------------------------------------------------------------------------
// Secret sanitizer
// ---------------------------------------------------------------------------

/**
 * Field names (or substrings) whose values must never appear in alerts.
 * Checked case-insensitively against embed field names.
 */
const FORBIDDEN_KEY_FRAGMENTS = [
  "secret",
  "token",
  "auth",
  "password",
  "api_key",
  "apikey",
  "credential",
  "message_body",
  "message_text",
  "body",
  "service_role",
  "client_secret",
  "posthog_key",
  "internal_api",
];

function isForbiddenFieldName(name) {
  const lower = String(name ?? "").toLowerCase();
  return FORBIDDEN_KEY_FRAGMENTS.some((fragment) => lower.includes(fragment));
}

function sanitizeFields(fields) {
  if (!Array.isArray(fields)) return [];
  return fields
    .filter((field) => field && !isForbiddenFieldName(field.name))
    .slice(0, 25)
    .map((field) => ({
      name: String(field.name ?? "").slice(0, 256),
      value: String(field.value ?? "").slice(0, 1024) || "\u200b",
      ...(typeof field.inline === "boolean" ? { inline: field.inline } : {}),
    }));
}

// ---------------------------------------------------------------------------
// Embed builder
// ---------------------------------------------------------------------------

function buildEmbed(payload) {
  const embed = {};

  if (payload.title) {
    embed.title = String(payload.title).slice(0, 256);
  }
  if (payload.description) {
    embed.description = String(payload.description).slice(0, 4096);
  }
  if (typeof payload.color === "number") {
    embed.color = payload.color;
  }
  if (payload.fields) {
    embed.fields = sanitizeFields(payload.fields);
  }
  if (payload.timestamp) {
    embed.timestamp = String(payload.timestamp);
  }
  if (payload.footer) {
    embed.footer = {
      text: String(payload.footer.text ?? "").slice(0, 2048),
    };
  }

  return embed;
}

// ---------------------------------------------------------------------------
// Dependency injection (for tests)
// ---------------------------------------------------------------------------

const defaultDeps = {};

let runtimeDeps = { ...defaultDeps };

export function __setDiscordDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetDiscordDeps() {
  runtimeDeps = { ...defaultDeps };
}

// ---------------------------------------------------------------------------
// Core send
// ---------------------------------------------------------------------------

/**
 * Send a Discord embed alert to the named channel.
 *
 * @param {string} channel - One of: "critical", "hot_leads", "sentry_errors",
 *   "posthog_alerts", "deploys"
 * @param {object} payload - Embed payload fields:
 *   title, description, color, fields, timestamp, footer
 */
export async function sendDiscordAlert(channel, payload) {
  try {
    const env_key = CHANNEL_ENV_MAP[channel];
    if (!env_key) return;

    const webhook_url = process.env[env_key];
    if (!webhook_url) return;

    const embed = buildEmbed(payload ?? {});
    const body = JSON.stringify({ embeds: [embed] });

    const fetch_fn = runtimeDeps.fetch ?? globalThis.fetch;
    if (typeof fetch_fn !== "function") return;

    await fetch_fn(webhook_url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
  } catch {
    // Never propagate — alert delivery must never affect the primary path.
  }
}

// ---------------------------------------------------------------------------
// Convenience wrappers
// ---------------------------------------------------------------------------

/**
 * Send a critical ops alert (errors, failures, stuck locks).
 */
export async function sendCriticalAlert(payload) {
  return sendDiscordAlert("critical", payload);
}

/**
 * Send a hot-lead alert (positive inbound replies, deal advances).
 */
export async function sendHotLeadAlert(payload) {
  return sendDiscordAlert("hot_leads", payload);
}

/**
 * Send a system-error alert to the Sentry-errors channel.
 */
export async function sendSystemErrorAlert(payload) {
  return sendDiscordAlert("sentry_errors", payload);
}
