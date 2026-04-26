/**
 * POST /api/internal/discord/morning-checkin
 *
 * Generates and posts the daily Morning Command Check-In card to the
 * daily-briefing Discord channel.  Typically triggered by a cron job
 * or a manual slash command.
 *
 * Requires:  X-Internal-API-Secret header or Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from "next/server";
import { requireInternalSecret } from "@/lib/security/require-internal-secret.js";
import { generateDailyBriefing } from "@/lib/discord/daily-briefing.js";
import { resolveDiscordChannel } from "@/lib/discord/channel-registry.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

const logger = child({ module: "api.internal.discord.morning-checkin" });

// ---------------------------------------------------------------------------
// Discord message utility
// ---------------------------------------------------------------------------

async function postToDiscordChannel({ channel_id, payload }) {
  const bot_token = String(process.env.DISCORD_BOT_TOKEN ?? "").trim();
  if (!bot_token || !channel_id) return { ok: false, error: "missing_config" };

  const res = await fetch(`https://discord.com/api/v10/channels/${channel_id}/messages`, {
    method:  "POST",
    headers: {
      Authorization:  `Bot ${bot_token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    logger.error("discord.morning_checkin.post_failed", { status: res.status, channel_id_tail: channel_id.slice(-4) });
    return { ok: false, error: `discord_api_${res.status}` };
  }

  return { ok: true };
}

// ---------------------------------------------------------------------------
// Build the check-in card embed + components
// ---------------------------------------------------------------------------

function buildCheckinEmbed(briefing) {
  const risk_color = briefing.risk_level === "high" ? 0xE74C3C : briefing.risk_level === "medium" ? 0xF39C12 : 0x2ECC71;

  return {
    title:       "☀️ Morning Command Check-In",
    description: briefing.intro,
    color:       risk_color,
    fields: [
      {
        name:   "📊 Queue",
        value:  `Sends today: **${briefing.metrics.sends_today}** | Delivered: **${briefing.metrics.delivered_count}** | Failed: **${briefing.metrics.failed_count}** | Opt-outs: **${briefing.metrics.opt_out_count}**`,
        inline: false,
      },
      {
        name:   "🔥 Hot Leads",
        value:  briefing.metrics.hot_leads > 0 ? `**${briefing.metrics.hot_leads}** hot lead${briefing.metrics.hot_leads === 1 ? "" : "s"} today` : "No hot leads yet today",
        inline: true,
      },
      {
        name:   "⚡ Risk Level",
        value:  `**${briefing.risk_level.toUpperCase()}**`,
        inline: true,
      },
      {
        name:   "🏆 Wins",
        value:  briefing.top_wins.join("\n").slice(0, 512) || "Engine online",
        inline: false,
      },
      {
        name:   "⚠️ Warnings",
        value:  briefing.risks.join("\n").slice(0, 512) || "No warnings",
        inline: false,
      },
      {
        name:   "🚀 Suggested Next Move",
        value:  briefing.recommended_next_moves.slice(0, 2).join("\n").slice(0, 512) || "Continue current cadence",
        inline: false,
      },
    ],
    footer:    { text: `Mode: ${(briefing.mode ?? "live").toUpperCase()} · ${new Date().toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" })}` },
    timestamp: new Date().toISOString(),
  };
}

function buildCheckinComponents() {
  return [
    {
      type: 1,
      components: [
        { type: 2, style: 3, label: "Approve Suggested Move",    custom_id: "checkin:approve_suggested_move" },
        { type: 2, style: 1, label: "Review Hot Leads",          custom_id: "checkin:review_hot_leads" },
        { type: 2, style: 2, label: "Run Feeder Dry Run",        custom_id: "checkin:run_feeder_dry_run" },
        { type: 2, style: 1, label: "Scale Winning Campaign",    custom_id: "checkin:scale_winning_campaign" },
        { type: 2, style: 4, label: "Hold Outreach Today",       custom_id: "checkin:hold_outreach" },
      ],
    },
  ];
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

export async function POST(request) {
  const auth = requireInternalSecret(request);
  if (!auth.ok) {
    return NextResponse.json({ ok: false, error: auth.error }, { status: auth.status ?? 401 });
  }

  try {
    const briefing = await generateDailyBriefing({ period: "morning" });

    const { channel_id } = resolveDiscordChannel({ channelKey: "daily_briefing", severity: "info" });

    if (!channel_id) {
      logger.warn("discord.morning_checkin.no_channel", {});
      return NextResponse.json({ ok: false, error: "DISCORD_CHANNEL_DAILY_BRIEFING not configured" }, { status: 200 });
    }

    const embed      = buildCheckinEmbed(briefing);
    const components = buildCheckinComponents();

    const result = await postToDiscordChannel({
      channel_id,
      payload: { embeds: [embed], components },
    });

    if (!result.ok) {
      return NextResponse.json({ ok: false, error: result.error }, { status: 200 });
    }

    logger.info("discord.morning_checkin.posted", { risk_level: briefing.risk_level, channel_id_tail: channel_id.slice(-4) });

    return NextResponse.json({ ok: true, risk_level: briefing.risk_level, metrics: briefing.metrics });
  } catch (err) {
    logger.error("discord.morning_checkin.failed", { error: err?.message });
    return NextResponse.json({ ok: false, error: "internal_error" }, { status: 500 });
  }
}
