/**
 * discord-channel-router.js
 *
 * Maps event and action types to Discord channel keys, then resolves channel IDs
 * from environment variables via the channel registry.
 *
 * Usage:
 *   import { resolveChannelForEvent } from "@/lib/discord/discord-channel-router.js";
 *   const { channel_id, channel_key } = resolveChannelForEvent("inbound_sms_reply");
 */

import {
  getDiscordChannelId,
  resolveDiscordChannel,
  listConfiguredDiscordChannels,
  CHANNEL_ENV_MAP,
} from "./channel-registry.js";

// ---------------------------------------------------------------------------
// Event-type → channel_key routing table
// ---------------------------------------------------------------------------

/**
 * Maps semantic event/action types to Discord channel keys.
 *
 * Keys use underscore_case to match internal event names emitted across
 * the codebase.  Values must exist in CHANNEL_ENV_MAP.
 */
const EVENT_CHANNEL_MAP = Object.freeze({
  // COMMAND (mission control / daily briefing)
  "mission_status":            "mission_control",
  "launch_preflight":          "mission_control",
  "campaign_create":           "mission_control",
  "campaign_approve_launch":   "mission_control",
  "campaign_scale":            "mission_control",
  "campaign_pause":            "mission_control",
  "campaign_close":            "mission_control",
  "daily_briefing":            "daily_briefing",
  "morning_checkin":           "daily_briefing",
  "ops_alert":                 "critical_alerts",
  "critical_error":            "critical_alerts",
  "win":                       "wins",

  // SMS ENGINE
  "sms_engine_event":          "sms_engine",
  "queue_status":              "queue_health",
  "queue_run":                 "queue_health",
  "queue_cockpit":             "queue_health",
  "feeder_run":                "feeder_runs",
  "feeder_scan":               "feeder_runs",
  "feeder_launch":             "feeder_runs",
  "textgrid_number_event":     "textgrid_numbers",
  "inbound_sms_reply":         "inbound_replies",
  "inbound_sms_alert":         "inbound_replies",
  "sms_reply_queued":          "inbound_replies",
  "opt_out":                   "opt_outs",
  "opt_out_event":             "opt_outs",

  // DEAL FLOW
  "hot_lead":                  "hot_leads",
  "hot_lead_marked":           "hot_leads",
  "not_lead":                  "not_leads",
  "seller_reply":              "seller_replies",
  "seller_reply_received":     "seller_replies",
  "offer_needed":              "offers_needed",
  "under_contract":            "under_contract",
  "closing":                   "closings",
  "closing_event":             "closings",

  // SYSTEMS
  "podio_sync":                "podio_sync",
  "podio_error":               "podio_sync",
  "supabase_event":            "supabase",
  "vercel_deploy":             "vercel_deploys",
  "deploy_error":              "vercel_deploys",
  "github_pr":                 "github_prs",
  "posthog_alert":             "posthog_alerts",
  "sentry_error":              "sentry_errors",

  // TESTING
  "preview_test":              "preview_testing",
  "postman_test":              "postman_tests",
  "failed_run":                "failed_runs",
  "debug_log":                 "debug_logs",
  "replay_inbound":            "debug_logs",
  "unknown_button":            "debug_logs",
});

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Resolve a Discord channel ID for a semantic event type.
 *
 * @param {string} event_type   - Semantic event name (e.g. "inbound_sms_reply")
 * @param {{ critical?: boolean, env?: object }} [opts]
 * @returns {{ channel_id: string|null, channel_key: string|null, fallback: boolean }}
 */
export function resolveChannelForEvent(event_type, { critical = false, env = process.env } = {}) {
  const channel_key = EVENT_CHANNEL_MAP[String(event_type ?? "")] ?? null;

  if (!channel_key) {
    // Unknown event — route to debug_logs or critical_alerts.
    const resolved = resolveDiscordChannel({ channelKey: critical ? "critical_alerts" : "debug_logs", critical, env });
    return { channel_id: resolved.channel_id, channel_key: resolved.resolved_from, fallback: true };
  }

  const channel_id = getDiscordChannelId(channel_key, env);
  if (channel_id) {
    return { channel_id, channel_key, fallback: false };
  }

  // Channel not configured — use registry fallback.
  const resolved = resolveDiscordChannel({ channelKey: channel_key, critical, env });
  return { channel_id: resolved.channel_id, channel_key: resolved.resolved_from, fallback: true };
}

/**
 * Get the channel key for a given event type, without resolving the ID.
 * Useful for embedding channel_key in audit logs.
 *
 * @param {string} event_type
 * @returns {string|null}
 */
export function getChannelKeyForEvent(event_type) {
  return EVENT_CHANNEL_MAP[String(event_type ?? "")] ?? null;
}

// Re-export registry helpers so callers only need this file.
export { getDiscordChannelId, resolveDiscordChannel, listConfiguredDiscordChannels, CHANNEL_ENV_MAP, EVENT_CHANNEL_MAP };
