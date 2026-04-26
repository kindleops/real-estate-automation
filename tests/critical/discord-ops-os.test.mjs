import test from "node:test";
import assert from "node:assert/strict";

import {
  notifyDiscordOps,
  __setNotifyDiscordOpsDeps,
  __resetNotifyDiscordOpsDeps,
} from "@/lib/discord/notify-discord-ops.js";
import {
  postDailyBriefing,
  __setDailyBriefingDeps,
  __resetDailyBriefingDeps,
} from "@/lib/discord/daily-briefing.js";
import { routeDiscordInteraction } from "@/lib/discord/discord-action-router.js";

function makeFetchRecorder() {
  const calls = [];
  const fetch = async (url, options = {}) => {
    calls.push({ url, options });
    return {
      ok: true,
      status: 200,
      json: async () => ({ id: `msg_${calls.length}` }),
    };
  };
  return { calls, fetch };
}

function makeInteraction(custom_id) {
  return {
    type: 3,
    data: { custom_id },
    guild_id: "guild_1",
    member: {
      user: { id: "user_1", username: "Tester" },
      roles: ["owner_role"],
    },
  };
}

function makeDailyBriefingDb() {
  return {
    from(table) {
      const chain = {
        select(_columns, options) {
          chain.__countHead = options?.head;
          return chain;
        },
        eq() { return chain; },
        in() { return chain; },
        or() { return chain; },
        gte() { return chain; },
        lt() { return chain; },
        order() { return chain; },
        limit() { return chain; },
        async maybeSingle() {
          return { data: null, error: null };
        },
        then(resolve) {
          if (table === "ops_recommendations") {
            return Promise.resolve({
              data: [
                {
                  recommendation_type: "scale_campaign",
                  priority: 8,
                  title: "Scale Campaign",
                  reason: "Healthy delivery and reply trend",
                  suggested_action: "Approve small test expansion",
                },
              ],
              error: null,
            }).then(resolve);
          }

          const byTable = {
            send_queue: 120,
            message_events: 40,
          };

          return Promise.resolve({
            count: byTable[table] || 0,
            data: null,
            error: null,
          }).then(resolve);
        },
      };
      return chain;
    },
  };
}

test("event routes to mission-control channel", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_MISSION_CONTROL = "chan_mission";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 1_000 });

  const result = await notifyDiscordOps({
    event_type: "mission_control_summary",
    severity: "info",
    domain: "command",
    title: "Mission",
    summary: "All systems nominal",
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(result.ok, true);
  assert.equal(recorder.calls.length, 1);
  assert.match(recorder.calls[0].url, /channels\/chan_mission\/messages/);
});

test("critical/error events duplicate to critical-alerts", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_FEEDER_RUNS = "chan_feeder";
  process.env.DISCORD_CHANNEL_FAILED_RUNS = "chan_failed";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 2_000 });

  await notifyDiscordOps({
    event_type: "feed_candidates_failed",
    severity: "error",
    domain: "feeder",
    title: "Feeder Failed",
    summary: "failed",
  });

  __resetNotifyDiscordOpsDeps();

  const urls = recorder.calls.map((c) => c.url).join("\n");
  assert.match(urls, /chan_feeder/);
  assert.match(urls, /chan_failed/);
  assert.match(urls, /chan_critical/);
});

test("missing channel does not throw and falls back", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  delete process.env.DISCORD_CHANNEL_HOT_LEADS;
  process.env.DISCORD_CHANNEL_DEBUG_LOGS = "chan_debug";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 3_000 });

  const result = await notifyDiscordOps({
    event_type: "inbound_hot_lead",
    severity: "hot",
    domain: "deal_flow",
    title: "Hot",
    summary: "hot lead",
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(result.ok, true);
  assert.equal(recorder.calls.length, 1);
  assert.match(recorder.calls[0].url, /chan_debug/);
});

test("hot lead routes to hot-leads", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_HOT_LEADS = "chan_hot";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 4_000 });

  await notifyDiscordOps({
    event_type: "inbound_hot_lead",
    severity: "hot",
    domain: "deal_flow",
    title: "Hot Lead",
    summary: "Revenue signal",
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(recorder.calls.length, 1);
  assert.match(recorder.calls[0].url, /chan_hot/);
});

test("opt-out routes to opt-outs", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_OPT_OUTS = "chan_opt";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 5_000 });

  await notifyDiscordOps({
    event_type: "opt_out",
    severity: "warning",
    domain: "inbound",
    title: "Opt Out",
    summary: "STOP",
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(recorder.calls.length, 1);
  assert.match(recorder.calls[0].url, /chan_opt/);
});

test("feed completed routes to feeder-runs", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_FEEDER_RUNS = "chan_feeder";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 6_000 });

  await notifyDiscordOps({
    event_type: "feed_candidates_completed",
    severity: "success",
    domain: "feeder",
    title: "Feed Complete",
    summary: "done",
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(recorder.calls.length, 1);
  assert.match(recorder.calls[0].url, /chan_feeder/);
});

test("queue failed routes to failed-runs and critical-alerts", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_QUEUE_HEALTH = "chan_queue";
  process.env.DISCORD_CHANNEL_FAILED_RUNS = "chan_failed";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 7_000 });

  await notifyDiscordOps({
    event_type: "queue_run_failed",
    severity: "critical",
    domain: "queue",
    title: "Queue Failed",
    summary: "failed",
  });

  __resetNotifyDiscordOpsDeps();

  const urls = recorder.calls.map((c) => c.url).join("\n");
  assert.match(urls, /chan_queue/);
  assert.match(urls, /chan_failed/);
  assert.match(urls, /chan_critical/);
});

test("daily briefing includes metrics and actions", async () => {
  const posted = [];
  __setDailyBriefingDeps({
    supabase: makeDailyBriefingDb(),
    notifyDiscordOps: async (payload) => {
      posted.push(payload);
      return { ok: true };
    },
  });

  const briefing = await postDailyBriefing({ period: "morning" });

  __resetDailyBriefingDeps();

  assert.equal(briefing.period, "morning");
  assert.ok(briefing.metrics.sends_today >= 0);
  assert.ok(Array.isArray(briefing.recommended_next_moves));
  assert.equal(posted.length, 1);
  assert.equal(posted[0].event_type, "daily_briefing");
  assert.ok(Array.isArray(posted[0].actions));
  assert.ok(posted[0].actions.length > 0);
});

test("dedupe suppresses repeated events inside throttle window", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_QUEUE_HEALTH = "chan_queue";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: (() => {
    let t = 10_000;
    return () => t;
  })() });

  const first = await notifyDiscordOps({
    event_type: "queue_retry_pending",
    severity: "info",
    domain: "queue",
    title: "Retry Pending",
    summary: "pending",
    dedupe_key: "queue:retry:1",
    throttle_window_seconds: 120,
  });

  const second = await notifyDiscordOps({
    event_type: "queue_retry_pending",
    severity: "info",
    domain: "queue",
    title: "Retry Pending",
    summary: "pending",
    dedupe_key: "queue:retry:1",
    throttle_window_seconds: 120,
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(first.ok, true);
  assert.equal(second.skipped, true);
  assert.equal(second.reason, "throttled");
  assert.equal(recorder.calls.length, 1);
});

test("unsupported action responds gracefully", async () => {
  process.env.DISCORD_GUILD_ID = "guild_1";
  process.env.DISCORD_ROLE_OWNER_ID = "owner_role";

  const response = await routeDiscordInteraction(makeInteraction("ops_action:not_wired"));
  const content = String(response?.data?.content || "");

  assert.match(content, /Action not wired yet/i);
});

test("debug events only post when DEBUG_DISCORD_OPS=true", async () => {
  process.env.DISCORD_BOT_TOKEN = "bot_token_test";
  process.env.DISCORD_CHANNEL_DEBUG_LOGS = "chan_debug";
  process.env.DISCORD_CHANNEL_CRITICAL_ALERTS = "chan_critical";

  const recorder = makeFetchRecorder();
  __setNotifyDiscordOpsDeps({ fetch: recorder.fetch, now: () => 11_000 });

  process.env.DEBUG_DISCORD_OPS = "false";
  const off = await notifyDiscordOps({
    event_type: "debug_log",
    severity: "debug",
    domain: "testing",
    title: "Debug",
    summary: "off",
  });

  process.env.DEBUG_DISCORD_OPS = "true";
  const on = await notifyDiscordOps({
    event_type: "debug_log",
    severity: "debug",
    domain: "testing",
    title: "Debug",
    summary: "on",
  });

  __resetNotifyDiscordOpsDeps();

  assert.equal(off.skipped, true);
  assert.equal(on.ok, true);
  assert.equal(recorder.calls.length, 1);
  assert.match(recorder.calls[0].url, /chan_debug/);
});
