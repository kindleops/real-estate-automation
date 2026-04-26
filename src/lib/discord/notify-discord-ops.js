import { resolveDiscordChannel } from "@/lib/discord/channel-registry.js";
import { buildOpsEmbed, buildOpsDebugEmbed } from "@/lib/discord/build-ops-embed.js";

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function buildActionButton(action = {}, index = 0) {
  const actionId = clean(action.action || action.type || action.id || `action_${index}`)
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 40);

  const token = clean(action.token || action.approval_request_key || action.request_key || "")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 40);

  const custom_id = token ? `ops_action:${actionId}:${token}` : `ops_action:${actionId}`;

  return {
    type: 2,
    style: Number(action.style) || 2,
    label: clean(action.label || actionId).slice(0, 80),
    custom_id: custom_id.slice(0, 100),
  };
}

function buildActionRows(actions = []) {
  if (!Array.isArray(actions) || !actions.length) return [];
  const buttons = actions.slice(0, 10).map(buildActionButton);
  const rows = [];
  while (buttons.length) {
    rows.push({ type: 1, components: buttons.splice(0, 5) });
  }
  return rows;
}

const EVENT_ROUTE_MAP = Object.freeze({
  mission_control_summary: ["mission_control"],
  daily_briefing: ["daily_briefing"],
  ai_recommendation: ["ai_recommendations", "mission_control"],
  campaign_approval_requested: ["campaign_approvals", "mission_control"],

  feed_candidates_started: ["feeder_runs"],
  feed_candidates_completed: ["feeder_runs"],
  feed_candidates_failed: ["feeder_runs", "failed_runs"],

  queue_run_started: ["queue_health"],
  queue_run_completed: ["queue_health"],
  queue_run_failed: ["queue_health", "failed_runs", "critical_alerts"],
  queue_stuck_rows_detected: ["queue_health", "critical_alerts"],
  queue_retry_pending: ["queue_health"],

  sms_sent: ["sms_engine"],
  sms_delivered: ["sms_engine"],
  sms_delivery_milestone: ["wins"],
  sms_failed: ["textgrid_numbers", "failed_runs"],
  textgrid_blacklist_21610: ["opt_outs", "textgrid_numbers"],
  textgrid_number_health: ["number_health", "textgrid_numbers"],
  textgrid_daily_limit_warning: ["textgrid_numbers", "critical_alerts"],

  inbound_known_reply: ["inbound_replies"],
  inbound_unknown: ["unknown_inbound", "seller_replies"],
  inbound_hot_lead: ["hot_leads"],
  inbound_not_lead: ["not_leads"],
  wrong_number: ["opt_outs", "not_leads"],
  opt_out: ["opt_outs"],

  offer_needed: ["offers_needed"],
  contract_created: ["under_contract"],
  contract_signed: ["under_contract", "wins"],
  sent_to_title: ["closings"],
  clear_to_close: ["closings", "wins"],
  deal_closed: ["closings", "wins", "mission_control"],

  podio_sync_success: ["podio_sync"],
  podio_sync_failed: ["podio_sync", "critical_alerts"],
  supabase_health: ["supabase"],
  supabase_timeout: ["supabase", "critical_alerts"],
  vercel_deploy_success: ["vercel_deploys"],
  vercel_deploy_failed: ["vercel_deploys", "critical_alerts"],
  github_pr_opened: ["github_prs"],
  github_pr_merged: ["github_prs", "vercel_deploys"],
  posthog_anomaly: ["posthog_alerts"],
  sentry_error: ["sentry_errors", "critical_alerts"],

  preview_test_result: ["preview_testing"],
  postman_test_result: ["postman_tests"],
  failed_run: ["failed_runs"],
  debug_log: ["debug_logs"],
});

const dedupeCache = new Map();
const digestState = new Map();

const DIGEST_WINDOW_MS = 15 * 60 * 1000;
const DIGEST_EVENT_TYPES = new Set(["sms_delivered", "queue_run_completed"]);
const BYPASS_THROTTLE = new Set(["critical", "hot", "approval"]);

const defaultDeps = {
  fetch: globalThis.fetch,
  now: () => Date.now(),
};

let runtimeDeps = { ...defaultDeps };

export function __setNotifyDiscordOpsDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetNotifyDiscordOpsDeps() {
  runtimeDeps = { ...defaultDeps };
}

function shouldSkipDebugEvent(event_type, env = process.env) {
  if (event_type !== "debug_log") return false;
  return clean(env.DEBUG_DISCORD_OPS).toLowerCase() !== "true";
}

function resolveRouteChannels(event_type, force_channel) {
  if (force_channel) return [force_channel];
  const mapped = EVENT_ROUTE_MAP[event_type];
  return Array.isArray(mapped) && mapped.length ? mapped : ["mission_control"];
}

function shouldThrottle({ dedupe_key, severity, throttle_window_seconds }) {
  if (!dedupe_key) return false;
  if (BYPASS_THROTTLE.has(clean(severity).toLowerCase())) return false;

  const windowMs = Math.max(0, asNumber(throttle_window_seconds, 0)) * 1000;
  if (!windowMs) return false;

  const now = runtimeDeps.now();
  const last = dedupeCache.get(dedupe_key);
  if (last && now - last < windowMs) {
    return true;
  }

  dedupeCache.set(dedupe_key, now);
  return false;
}

function updateDigestAndMaybeSkip({ event_type, severity, title, summary }) {
  if (BYPASS_THROTTLE.has(clean(severity).toLowerCase())) return { skip: false, digest: null };
  if (!DIGEST_EVENT_TYPES.has(event_type)) return { skip: false, digest: null };

  const now = runtimeDeps.now();
  const state = digestState.get(event_type) || {
    count: 0,
    lastFlushAt: 0,
    latestTitle: null,
    latestSummary: null,
  };

  state.count += 1;
  state.latestTitle = clean(title) || state.latestTitle;
  state.latestSummary = clean(summary) || state.latestSummary;

  if (state.lastFlushAt === 0) {
    state.lastFlushAt = now;
    digestState.set(event_type, state);
    return { skip: true, digest: null };
  }

  if (now - state.lastFlushAt < DIGEST_WINDOW_MS) {
    digestState.set(event_type, state);
    return { skip: true, digest: null };
  }

  const digest = {
    count: state.count,
    title: state.latestTitle,
    summary: state.latestSummary,
  };

  state.count = 0;
  state.lastFlushAt = now;
  state.latestTitle = null;
  state.latestSummary = null;
  digestState.set(event_type, state);

  return { skip: false, digest };
}

async function postToDiscordChannel(channel_id, body) {
  const token = clean(process.env.DISCORD_BOT_TOKEN);
  if (!channel_id || !token || typeof runtimeDeps.fetch !== "function") {
    return { ok: false, skipped: true };
  }

  try {
    const response = await runtimeDeps.fetch(`https://discord.com/api/v10/channels/${channel_id}/messages`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bot ${token}`,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      return { ok: false, status: response.status };
    }

    const data = await response.json().catch(() => null);
    return { ok: true, message_id: data?.id || null };
  } catch {
    return { ok: false };
  }
}

export async function notifyDiscordOps({
  event_type,
  severity = "info",
  domain = "command",
  title,
  summary,
  fields = [],
  metadata = {},
  actions = [],
  force_channel = null,
  dedupe_key = null,
  throttle_window_seconds = 0,
  create_thread = false,
  thread_name = null,
  should_alert_critical = false,
} = {}) {
  try {
    if (shouldSkipDebugEvent(event_type)) {
      return { ok: true, skipped: true, reason: "debug_disabled" };
    }

    if (shouldThrottle({ dedupe_key, severity, throttle_window_seconds })) {
      return { ok: true, skipped: true, reason: "throttled" };
    }

    const digestDecision = updateDigestAndMaybeSkip({ event_type, severity, title, summary });
    if (digestDecision.skip) {
      return { ok: true, skipped: true, reason: "digest_buffered" };
    }

    const routeChannels = resolveRouteChannels(event_type, force_channel);
    const normalizedSeverity = clean(severity).toLowerCase();
    if (
      (should_alert_critical || ["error", "critical"].includes(normalizedSeverity)) &&
      !routeChannels.includes("critical_alerts")
    ) {
      routeChannels.push("critical_alerts");
    }

    const uniqueChannelKeys = [...new Set(routeChannels)];
    const payloadSummary = digestDecision.digest
      ? `${clean(summary)}\n\nDigest count (15m): ${digestDecision.digest.count}`.trim()
      : summary;

    const embed = buildOpsEmbed({
      event_type,
      severity,
      domain,
      title,
      summary: payloadSummary,
      fields,
      metadata,
      dedupe_key,
      suggested_action: metadata?.suggested_action || null,
    });

    const debugEmbed = buildOpsDebugEmbed({
      title: `${clean(title)} | diagnostics`,
      summary,
      metadata,
    });

    const components = buildActionRows(actions);

    const results = [];
    for (const channelKey of uniqueChannelKeys) {
      const resolved = resolveDiscordChannel({
        channelKey,
        severity,
        critical: should_alert_critical,
      });

      if (!resolved.channel_id) {
        results.push({ channel_key: channelKey, ok: false, skipped: true, reason: "missing_channel" });
        continue;
      }

      const isDebug = resolved.resolved_from === "debug_logs" || channelKey === "debug_logs";
      const message = {
        embeds: isDebug ? [debugEmbed] : [embed],
        ...(components.length && !isDebug ? { components } : {}),
      };

      const postResult = await postToDiscordChannel(resolved.channel_id, message);

      if (create_thread && postResult.ok && thread_name) {
        // Best-effort only; thread creation failures never block main flow.
        const token = clean(process.env.DISCORD_BOT_TOKEN);
        if (token && typeof runtimeDeps.fetch === "function") {
          try {
            await runtimeDeps.fetch(
              `https://discord.com/api/v10/channels/${resolved.channel_id}/messages/${postResult.message_id}/threads`,
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  Authorization: `Bot ${token}`,
                },
                body: JSON.stringify({
                  name: clean(thread_name).slice(0, 100) || "Ops Thread",
                  auto_archive_duration: 1440,
                }),
              }
            );
          } catch {
            // no-op
          }
        }
      }

      results.push({
        channel_key: channelKey,
        resolved_channel: resolved.resolved_from,
        fallback: resolved.fallback,
        ...postResult,
      });
    }

    return { ok: true, event_type, severity, posted: results };
  } catch {
    return { ok: false, event_type, severity };
  }
}

export { EVENT_ROUTE_MAP };
