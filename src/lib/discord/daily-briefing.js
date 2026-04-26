import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";

function clean(value) {
  return String(value ?? "").trim();
}

function startOfTodayIso() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  return start.toISOString();
}

function startOfYesterdayIso() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
  return start.toISOString();
}

function riskLevel({ delivery_rate, failed_count, opt_out_count, stuck_rows }) {
  if (stuck_rows > 0 || failed_count > 100 || delivery_rate < 0.8) return "high";
  if (failed_count > 20 || opt_out_count > 10 || delivery_rate < 0.92) return "medium";
  return "low";
}

const defaultDeps = {
  supabase: defaultSupabase,
  notifyDiscordOps,
};

let runtimeDeps = { ...defaultDeps };

export function __setDailyBriefingDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetDailyBriefingDeps() {
  runtimeDeps = { ...defaultDeps };
}

async function safeCount(queryBuilder) {
  try {
    const { count } = await queryBuilder;
    return Number(count || 0);
  } catch {
    return 0;
  }
}

async function fetchOpsRecommendation(db) {
  try {
    const { data } = await db
      .from("ops_recommendations")
      .select("recommendation_type, priority, title, reason, suggested_action")
      .in("status", ["pending", "open", "new"])
      .order("priority", { ascending: false })
      .limit(3);
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
  }
}

export async function generateDailyBriefing({ period = "morning" } = {}) {
  const db = runtimeDeps.supabase;
  const today = startOfTodayIso();
  const yesterday = startOfYesterdayIso();

  const sends_today = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).gte("created_at", today)
  );
  const sends_yesterday = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).gte("created_at", yesterday).lt("created_at", today)
  );
  const delivered_count = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "outbound").eq("delivery_status", "delivered").gte("created_at", today)
  );
  const failed_count = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "outbound").in("delivery_status", ["failed", "undelivered"]).gte("created_at", today)
  );
  const opt_out_count = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "inbound").eq("is_opt_out", true).gte("created_at", today)
  );
  const inbound_replies = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "inbound").gte("created_at", today)
  );
  const hot_leads = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "inbound").or("event_type.eq.inbound_hot_lead,metadata->>route_stage.eq.Hot").gte("created_at", today)
  );
  const unknown_inbound = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "inbound").eq("event_type", "inbound_unknown").gte("created_at", today)
  );
  const stuck_rows = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).eq("queue_status", "sending")
  );

  const delivery_rate = sends_today > 0 ? delivered_count / sends_today : 0;
  const mode = clean(process.env.ROLLOUT_MODE || "live").toLowerCase();
  const risk = riskLevel({ delivery_rate, failed_count, opt_out_count, stuck_rows });

  const recommendations = await fetchOpsRecommendation(db);

  const topWins = [];
  if (hot_leads > 0) topWins.push(`${hot_leads} hot lead${hot_leads === 1 ? "" : "s"}`);
  if (delivered_count > 0) topWins.push(`${delivered_count} delivered messages`);
  if (!topWins.length) topWins.push("Engine online and collecting signal");

  const risks = [];
  if (failed_count > 0) risks.push(`${failed_count} failed sends`);
  if (opt_out_count > 0) risks.push(`${opt_out_count} opt-outs`);
  if (stuck_rows > 0) risks.push(`${stuck_rows} stuck queue rows`);
  if (!risks.length) risks.push("No major risks detected");

  const nextMoves = recommendations.length
    ? recommendations.map((r) => r.suggested_action || r.title).filter(Boolean)
    : [
        risk === "high" ? "Run queue health check and pause risky campaign slices" : "Continue current run cadence",
        unknown_inbound > 0 ? "Inspect unknown inbound and map contacts" : "No unknown inbound backlog",
      ];

  const intro = clean(period).toLowerCase() === "evening"
    ? "Evening check-in — here's what the engine did today."
    : "GM Ryan — REI Automation is awake.";

  const briefing = {
    period,
    intro,
    mode,
    risk_level: risk,
    metrics: {
      sends_today,
      sends_yesterday,
      delivered_count,
      failed_count,
      delivery_rate,
      opt_out_count,
      inbound_replies,
      hot_leads,
      unknown_inbound,
      stuck_rows,
    },
    top_wins: topWins,
    risks,
    recommended_next_moves: nextMoves,
    recommendations,
  };

  return briefing;
}

export async function postDailyBriefing({ period = "morning" } = {}) {
  const briefing = await generateDailyBriefing({ period });

  const actions = [
    { action: "show_queue_health", label: "Show Queue Health", style: 1 },
    { action: "run_feed_dry", label: "Run Feed Dry", style: 2 },
    { action: "run_queue_dry", label: "Run Queue Dry", style: 2 },
  ];

  await runtimeDeps.notifyDiscordOps({
    event_type: "daily_briefing",
    severity: "info",
    domain: "command",
    title: `Daily Briefing (${briefing.period})`,
    summary: `${briefing.intro}\nMode: ${briefing.mode.toUpperCase()} | Risk: ${briefing.risk_level.toUpperCase()}`,
    fields: [
      { name: "Sends Today", value: String(briefing.metrics.sends_today), inline: true },
      { name: "Delivery Rate", value: `${(briefing.metrics.delivery_rate * 100).toFixed(1)}%`, inline: true },
      { name: "Failed", value: String(briefing.metrics.failed_count), inline: true },
      { name: "Opt-outs", value: String(briefing.metrics.opt_out_count), inline: true },
      { name: "Inbound Replies", value: String(briefing.metrics.inbound_replies), inline: true },
      { name: "Hot Leads", value: String(briefing.metrics.hot_leads), inline: true },
      { name: "Top Wins", value: briefing.top_wins.join("\n").slice(0, 1024), inline: false },
      { name: "Risks", value: briefing.risks.join("\n").slice(0, 1024), inline: false },
      { name: "Recommended Next Moves", value: briefing.recommended_next_moves.join("\n").slice(0, 1024), inline: false },
    ],
    metadata: {
      mode: briefing.mode,
      risk_level: briefing.risk_level,
      recommendations: briefing.recommendations,
    },
    actions,
  });

  return briefing;
}
