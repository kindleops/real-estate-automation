const CHANNEL_ENV_MAP = Object.freeze({
  mission_control: "DISCORD_CHANNEL_MISSION_CONTROL",
  daily_briefing: "DISCORD_CHANNEL_DAILY_BRIEFING",
  critical_alerts: "DISCORD_CHANNEL_CRITICAL_ALERTS",
  wins: "DISCORD_CHANNEL_WINS",
  ai_recommendations: "DISCORD_CHANNEL_AI_RECOMMENDATIONS",
  campaign_approvals: "DISCORD_CHANNEL_CAMPAIGN_APPROVALS",
  sms_engine: "DISCORD_CHANNEL_SMS_ENGINE",
  queue_health: "DISCORD_CHANNEL_QUEUE_HEALTH",
  feeder_runs: "DISCORD_CHANNEL_FEEDER_RUNS",
  textgrid_numbers: "DISCORD_CHANNEL_TEXTGRID_NUMBERS",
  number_health: "DISCORD_CHANNEL_NUMBER_HEALTH",
  inbound_replies: "DISCORD_CHANNEL_INBOUND_REPLIES",
  unknown_inbound: "DISCORD_CHANNEL_UNKNOWN_INBOUND",
  opt_outs: "DISCORD_CHANNEL_OPT_OUTS",
  hot_leads: "DISCORD_CHANNEL_HOT_LEADS",
  not_leads: "DISCORD_CHANNEL_NOT_LEADS",
  seller_replies: "DISCORD_CHANNEL_SELLER_REPLIES",
  offers_needed: "DISCORD_CHANNEL_OFFERS_NEEDED",
  under_contract: "DISCORD_CHANNEL_UNDER_CONTRACT",
  closings: "DISCORD_CHANNEL_CLOSINGS",
  podio_sync: "DISCORD_CHANNEL_PODIO_SYNC",
  supabase: "DISCORD_CHANNEL_SUPABASE",
  vercel_deploys: "DISCORD_CHANNEL_VERCEL_DEPLOYS",
  github_prs: "DISCORD_CHANNEL_GITHUB_PRS",
  posthog_alerts: "DISCORD_CHANNEL_POSTHOG_ALERTS",
  sentry_errors: "DISCORD_CHANNEL_SENTRY_ERRORS",
  preview_testing: "DISCORD_CHANNEL_PREVIEW_TESTING",
  postman_tests: "DISCORD_CHANNEL_POSTMAN_TESTS",
  failed_runs: "DISCORD_CHANNEL_FAILED_RUNS",
  debug_logs: "DISCORD_CHANNEL_DEBUG_LOGS",
});

function clean(value) {
  return String(value ?? "").trim();
}

export function getDiscordChannelId(channelKey, env = process.env) {
  const envName = CHANNEL_ENV_MAP[channelKey];
  if (!envName) return null;
  const value = clean(env?.[envName]);
  return value || null;
}

export function resolveDiscordChannel({
  channelKey,
  severity = "info",
  critical = false,
  env = process.env,
} = {}) {
  const direct = getDiscordChannelId(channelKey, env);
  if (direct) return { channel_id: direct, resolved_from: channelKey, fallback: false };

  const isCritical = critical || ["error", "critical"].includes(clean(severity).toLowerCase());

  if (!isCritical) {
    const debugFallback = getDiscordChannelId("debug_logs", env);
    if (debugFallback) {
      return {
        channel_id: debugFallback,
        resolved_from: "debug_logs",
        fallback: true,
      };
    }
  }

  const criticalFallback = getDiscordChannelId("critical_alerts", env);
  if (criticalFallback) {
    return {
      channel_id: criticalFallback,
      resolved_from: "critical_alerts",
      fallback: true,
    };
  }

  return {
    channel_id: null,
    resolved_from: null,
    fallback: true,
  };
}

export function listConfiguredDiscordChannels(env = process.env) {
  const out = {};
  for (const [key] of Object.entries(CHANNEL_ENV_MAP)) {
    out[key] = getDiscordChannelId(key, env);
  }
  return out;
}

export { CHANNEL_ENV_MAP };
