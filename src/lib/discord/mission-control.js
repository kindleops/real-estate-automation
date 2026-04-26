import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";

function clean(value) {
  return String(value ?? "").trim();
}

const defaultDeps = {
  supabase: defaultSupabase,
  notifyDiscordOps,
};

let runtimeDeps = { ...defaultDeps };

export function __setMissionControlDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetMissionControlDeps() {
  runtimeDeps = { ...defaultDeps };
}

async function safeSingle(dbQuery) {
  try {
    const { data } = await dbQuery;
    return data || null;
  } catch {
    return null;
  }
}

async function safeCount(dbQuery) {
  try {
    const { count } = await dbQuery;
    return Number(count || 0);
  } catch {
    return 0;
  }
}

export async function buildMissionControlHeartbeat() {
  const db = runtimeDeps.supabase;

  const lastSend = await safeSingle(
    db.from("message_events")
      .select("created_at,to_phone_number")
      .eq("direction", "outbound")
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle()
  );

  const lastInbound = await safeSingle(
    db.from("message_events")
      .select("created_at,from_phone_number,event_type")
      .eq("direction", "inbound")
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle()
  );

  const feederHealth = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).eq("queue_status", "queued")
  );

  const queueStuck = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).eq("queue_status", "sending")
  );

  const textgridFailed = await safeCount(
    db.from("message_events")
      .select("id", { count: "exact", head: true })
      .eq("direction", "outbound")
      .in("delivery_status", ["failed", "undelivered"])
  );

  const supabaseHealth = textgridFailed < 50 ? "healthy" : "warning";
  const podioSyncHealth = "healthy";
  const mode = clean(process.env.ROLLOUT_MODE || "live");

  const recommendedNextAction =
    queueStuck > 0
      ? "Run queue health check and unblock stuck rows"
      : feederHealth === 0
        ? "Run feeder dry-run to refill queue"
        : "Continue live cadence";

  return {
    mode,
    feeder_health: feederHealth > 0 ? "healthy" : "warning",
    queue_health: queueStuck > 0 ? "warning" : "healthy",
    textgrid_health: textgridFailed > 0 ? "warning" : "healthy",
    supabase_health: supabaseHealth,
    podio_sync_health: podioSyncHealth,
    last_successful_send: lastSend?.created_at || null,
    last_inbound_reply: lastInbound?.created_at || null,
    recommended_next_action: recommendedNextAction,
    stats: {
      queued_count: feederHealth,
      stuck_rows: queueStuck,
      failed_deliveries: textgridFailed,
    },
  };
}

export async function postMissionControlHeartbeat() {
  const heartbeat = await buildMissionControlHeartbeat();

  await runtimeDeps.notifyDiscordOps({
    event_type: "mission_control_summary",
    severity: heartbeat.queue_health === "warning" ? "warning" : "info",
    domain: "command",
    title: "Mission Control Heartbeat",
    summary: `Mode: ${heartbeat.mode.toUpperCase()} | Queue: ${heartbeat.queue_health.toUpperCase()} | TextGrid: ${heartbeat.textgrid_health.toUpperCase()}`,
    fields: [
      { name: "Mode", value: heartbeat.mode, inline: true },
      { name: "Feeder Health", value: heartbeat.feeder_health, inline: true },
      { name: "Queue Health", value: heartbeat.queue_health, inline: true },
      { name: "TextGrid Health", value: heartbeat.textgrid_health, inline: true },
      { name: "Supabase Health", value: heartbeat.supabase_health, inline: true },
      { name: "Podio Sync Health", value: heartbeat.podio_sync_health, inline: true },
      { name: "Last Successful Send", value: heartbeat.last_successful_send || "n/a", inline: false },
      { name: "Last Inbound Reply", value: heartbeat.last_inbound_reply || "n/a", inline: false },
      { name: "Recommended Next Action", value: heartbeat.recommended_next_action, inline: false },
    ],
    metadata: {
      mode: heartbeat.mode,
      queue_stuck_rows: heartbeat.stats.stuck_rows,
      failed_deliveries: heartbeat.stats.failed_deliveries,
    },
    actions: [
      { action: "show_queue_health", label: "Queue Health", style: 1 },
      { action: "run_feed_dry", label: "Run Feed Dry", style: 2 },
      { action: "run_queue_live", label: "Run Queue Live", style: 3 },
    ],
  });

  return heartbeat;
}
