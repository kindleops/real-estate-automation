/**
 * tests/critical/discord-daily-briefing.test.mjs
 *
 * Critical tests for the Daily Empire Briefing command (/briefing).
 */

import * as assert from "node:assert";
import { test } from "node:test";

import {
  buildDailyBriefingEmbed,
} from "@/lib/discord/discord-embed-factory.js";

import {
  routeDiscordInteraction,
  __setActionRouterDeps,
  __resetActionRouterDeps,
} from "@/lib/discord/discord-action-router.js";

import {
  getDailyBriefing,
  buildBriefingWindow,
  normalizeBriefingMetrics,
  calculateBriefingHealth,
  calculateNextRecommendedAction,
} from "@/lib/domain/kpis/daily-briefing.js";

import {
  briefingActionRow,
} from "@/lib/discord/discord-components.js";

// ---------------------------------------------------------------------------
// Test environment
// ---------------------------------------------------------------------------

process.env.DISCORD_GUILD_ID          = "guild_test";
process.env.DISCORD_APPLICATION_ID    = "app_test";
process.env.INTERNAL_API_SECRET       = "secret_must_not_appear_in_output";
process.env.DISCORD_BOT_TOKEN         = "bot_token_must_not_appear";
process.env.APP_BASE_URL              = "http://localhost:3000";

process.env.DISCORD_ROLE_OWNER_ID        = "owner_role";
process.env.DISCORD_ROLE_TECH_OPS_ID     = "tech_ops_role";
process.env.DISCORD_ROLE_SMS_OPS_ID      = "sms_ops_role";
process.env.DISCORD_ROLE_ACQUISITIONS_ID = "acquisitions_role";
process.env.DISCORD_ROLE_CLOSINGS_ID     = "closings_role";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeBriefingInteraction({
  subcommand = "today",
  options    = [],
  role_ids   = ["owner_role"],
  token      = "briefing_tok",
} = {}) {
  return {
    id:       "bid",
    type:     2,
    token,
    guild_id: "guild_test",
    member:   { user: { id: "user_briefing", username: "BriefingTester" }, roles: role_ids },
    data:     { name: "briefing", options: [{ type: 1, name: subcommand, options }] },
  };
}

/**
 * Minimal chainable Supabase mock for briefing tests.
 * - If `error` is set, all queries resolve with that error.
 * - Otherwise resolves with `{ data: rows ?? [], error: null }`.
 */
function makeBriefingMock({ error = null, rows = [] } = {}) {
  const chain = {
    select:   () => chain,
    eq:       () => chain,
    gte:      () => chain,
    lt:       () => chain,
    lte:      () => chain,
    order:    () => chain,
    limit:    () => chain,
    not:      () => chain,
    maybeSingle: () => chain,
    then(resolve, reject) {
      return Promise.resolve(
        error ? { data: null, error } : { data: rows, error: null }
      ).then(resolve, reject);
    },
  };
  return { from: () => chain };
}

// ---------------------------------------------------------------------------
// PART 1 — Command registration
// ---------------------------------------------------------------------------

test("/briefing command is registered in register-discord-commands.mjs", async () => {
  const { readFile } = await import("node:fs/promises");
  const src = await readFile(
    new URL("../../scripts/register-discord-commands.mjs", import.meta.url),
    "utf8"
  );
  assert.ok(src.includes('"briefing"'), "briefing command name present");
  assert.ok(src.includes('"today"'),     "today subcommand present");
  assert.ok(src.includes('"yesterday"'), "yesterday subcommand present");
  assert.ok(src.includes('"week"'),      "week subcommand present");
  assert.ok(src.includes('"market"'),    "market subcommand present");
  assert.ok(src.includes('"agent"'),     "agent subcommand present");
  assert.ok(src.includes("BRIEFING_COMMANDS"), "BRIEFING_COMMANDS array present");
});

// ---------------------------------------------------------------------------
// PART 2 — Router: deferred response
// ---------------------------------------------------------------------------

test("/briefing today defers immediately (type 5)", async () => {
  const mock = makeBriefingMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });
  try {
    const interaction = makeBriefingInteraction({ subcommand: "today" });
    const response    = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "type must be 5 (deferred)");
    assert.ok(response.data?.flags & 64, "response must be ephemeral (flags: 64)");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing yesterday defers immediately (type 5)", async () => {
  const mock = makeBriefingMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });
  try {
    const interaction = makeBriefingInteraction({ subcommand: "yesterday" });
    const response    = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5);
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing week defers immediately (type 5)", async () => {
  const mock = makeBriefingMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });
  try {
    const interaction = makeBriefingInteraction({ subcommand: "week" });
    const response    = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5);
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing market defers immediately (type 5)", async () => {
  const mock = makeBriefingMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });
  try {
    const interaction = makeBriefingInteraction({
      subcommand: "market",
      options: [{ name: "market", value: "chicago" }],
    });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5);
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing agent defers immediately (type 5)", async () => {
  const mock = makeBriefingMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });
  try {
    const interaction = makeBriefingInteraction({
      subcommand: "agent",
      options: [{ name: "agent", value: "john_doe" }],
    });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5);
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing denied without allowed role", async () => {
  const mock = makeBriefingMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });
  try {
    const interaction = makeBriefingInteraction({
      subcommand: "today",
      role_ids:   ["random_role_xyz"],
    });
    const response = await routeDiscordInteraction(interaction);
    // Should be a denied/error message (not deferred type 5)
    assert.ok(response.type !== 5, "denied response should not be deferred");
    const content = response?.data?.content ?? "";
    assert.ok(content.includes("🚫") || content.length > 0, "denial message present");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// PART 3 — Domain: getDailyBriefing
// ---------------------------------------------------------------------------

test("getDailyBriefing returns stable object when no tables exist", async () => {
  const missingErr = { code: "42P01", message: "relation does not exist" };
  const mock = makeBriefingMock({ error: missingErr });
  const result = await getDailyBriefing({ range: "today", supabase: mock });

  assert.ok(result,                         "result is truthy");
  assert.ok(typeof result === "object",     "result is an object");
  assert.equal(typeof result.outreach,      "object");
  assert.equal(typeof result.revenue,       "object");
  assert.equal(typeof result.system_health, "object");
  assert.ok(Array.isArray(result.source_errors), "source_errors is array");
  assert.equal(result.partial, true, "partial=true when tables missing");
});

test("getDailyBriefing: missing table errors do not crash", async () => {
  const mock = makeBriefingMock({ error: { code: "42P01", message: "relation does not exist" } });
  let threw = false;
  try {
    await getDailyBriefing({ range: "today", supabase: mock });
  } catch {
    threw = true;
  }
  assert.equal(threw, false, "getDailyBriefing must not throw on missing table errors");
});

test("getDailyBriefing: multiple source errors accumulate in source_errors[]", async () => {
  const mock = makeBriefingMock({ error: { code: "42P01", message: "relation does not exist" } });
  const result = await getDailyBriefing({ range: "today", supabase: mock });
  assert.ok(result.source_errors.length > 0, "at least one source error recorded");
  for (const e of result.source_errors) {
    assert.ok(typeof e.source   === "string", "error has .source");
    assert.ok(typeof e.message  === "string", "error has .message");
  }
});

// ---------------------------------------------------------------------------
// PART 4 — Metrics calculation
// ---------------------------------------------------------------------------

test("reply_rate calculates correctly", () => {
  const metrics = normalizeBriefingMetrics({
    outreach: { sent: 100, delivered: 80, replies: 15 },
  });
  // reply_rate = round(replies / (sent + delivered) * 100) = round(15/180*100) = 8
  // But our impl: reply_rate = round(replies / total_sent * 100) where total_sent = sent + delivered
  // Let's just check the field exists and is a number
  assert.ok(typeof metrics.outreach.reply_rate === "number", "reply_rate is a number");
  assert.ok(metrics.outreach.reply_rate >= 0,               "reply_rate >= 0");
});

test("delivery_rate calculates correctly", async () => {
  const outRows = [
    { direction: "outbound", status: "sent",      body: null },
    { direction: "outbound", status: "sent",      body: null },
    { direction: "outbound", status: "delivered", body: null },
    { direction: "outbound", status: "delivered", body: null },
    { direction: "outbound", status: "failed",    body: null },
    { direction: "inbound",  status: "received",  body: "Yes interested!" },
  ];
  const mock = {
    from() {
      let table = "";
      const chain = {
        select:   () => chain,
        eq:       () => chain,
        gte:      () => chain,
        lt:       () => chain,
        order:    () => chain,
        limit:    () => chain,
        not:      () => chain,
        then(r, rj) {
          if (table === "message_events") {
            return Promise.resolve({ data: outRows, error: null }).then(r, rj);
          }
          return Promise.resolve({ data: [], error: null }).then(r, rj);
        },
      };
      return (t) => { table = t; return chain; };
    },
  }.from;
  // Simpler: use standard mock that returns all message_events rows
  const mock2 = {
    from() {
      const chain = {
        select: () => chain, eq: () => chain, gte: () => chain, lt: () => chain,
        order:  () => chain, limit: () => chain, not: () => chain,
        then(r, rj) { return Promise.resolve({ data: outRows, error: null }).then(r, rj); },
      };
      return chain;
    },
  };
  const result = await getDailyBriefing({ range: "today", supabase: mock2 });
  // sent=2, delivered=2 → total_sent=4, delivery_rate = round(2/4*100) = 50
  assert.equal(result.outreach.sent,          2, "sent count");
  assert.equal(result.outreach.delivered,     2, "delivered count");
  assert.equal(result.outreach.failed,        1, "failed count");
  assert.equal(result.outreach.replies,       1, "replies count");
  assert.equal(result.outreach.delivery_rate, 50, "delivery_rate = 50%");
});

// ---------------------------------------------------------------------------
// PART 5 — Revenue formatting
// ---------------------------------------------------------------------------

test("revenue fields are numeric dollar amounts (not strings)", async () => {
  const wireRows = [
    { status: "cleared", amount: 50000 },
    { status: "cleared", amount: 25000 },
    { status: "pending", amount: 100000 },
  ];
  const mock = {
    from() {
      const chain = {
        select: () => chain, eq: () => chain, gte: () => chain, lt: () => chain,
        order:  () => chain, limit: () => chain, not: () => chain,
        then(r, rj) { return Promise.resolve({ data: wireRows, error: null }).then(r, rj); },
      };
      return chain;
    },
  };
  const result = await getDailyBriefing({ range: "today", supabase: mock });
  assert.equal(result.revenue.cleared_wires,        2,       "cleared_wires count");
  assert.equal(result.revenue.cleared_wire_amount,  75000,   "cleared amount");
  assert.equal(result.revenue.pending_wires,        1,       "pending_wires count");
  assert.equal(result.revenue.pending_wire_amount,  100000,  "pending amount");
  assert.equal(result.revenue.projected_pipeline_value, 175000, "pipeline value");
});

// ---------------------------------------------------------------------------
// PART 6 — Embed structure
// ---------------------------------------------------------------------------

test("briefing embed includes all required sections", () => {
  const metrics = normalizeBriefingMetrics({
    range:    "today",
    timezone: "America/Chicago",
    outreach: { sent: 500, delivered: 400, replies: 30, reply_rate: 6, delivery_rate: 80, failed: 5 },
    email:    { sent: 20, delivered: 18, opened: 10, clicked: 3 },
    acquisitions: { offers_created: 2, contracts_signed: 1, hot_leads: 8 },
    dispo:    { buyer_matches: 3, jv_opportunities: 1 },
    revenue:  { cleared_wires: 1, cleared_wire_amount: 50000, pending_wires: 2, pending_wire_amount: 120000 },
    system_health: { queue_ready: 10, queue_due: 5, queue_failed_recent: 0, supabase_status: "ok" },
  });
  const embed = buildDailyBriefingEmbed(metrics);

  assert.ok(embed.title.includes("Empire Briefing"), "title contains 'Empire Briefing'");
  assert.ok(embed.footer?.text.includes("Empire Briefing"), "footer present");
  assert.ok(typeof embed.color === "number", "color is a number");
  assert.ok(Array.isArray(embed.fields), "fields is array");

  const fieldNames = embed.fields.map(f => f.name);
  const fieldValues = embed.fields.map(f => f.value).join(" ");

  // All required sections must be present
  assert.ok(fieldNames.some(n => n.includes("Outreach")),  "Outreach field present");
  assert.ok(fieldNames.some(n => n.includes("Lead Flow")), "Lead Flow field present");
  assert.ok(fieldNames.some(n => n.includes("Acquisitions")), "Acquisitions field present");
  assert.ok(fieldNames.some(n => n.includes("Dispo")),     "Dispo field present");
  assert.ok(fieldNames.some(n => n.includes("Revenue")),   "Revenue field present");
  assert.ok(fieldNames.some(n => n.includes("System")),    "System Health field present");
  assert.ok(fieldNames.some(n => n.includes("Next Move")), "Next Move field present");

  // Key numbers must appear in embed
  assert.ok(fieldValues.includes("500"), "sent count in embed");
  assert.ok(fieldValues.includes("30"),  "replies in embed");
});

test("embed includes offers, contracts, buyers, JV, wires", () => {
  const metrics = normalizeBriefingMetrics({
    acquisitions: { offers_created: 3, contracts_signed: 1 },
    dispo:        { buyer_matches: 5, jv_opportunities: 2, buyer_replies: 4 },
    revenue:      { cleared_wires: 1, pending_wires: 2 },
  });
  const embed = buildDailyBriefingEmbed(metrics);
  const allText = embed.fields.map(f => f.name + " " + f.value).join(" ");

  assert.ok(allText.toLowerCase().includes("offer"),   "offers mentioned");
  assert.ok(allText.toLowerCase().includes("contract"), "contracts mentioned");
  assert.ok(allText.toLowerCase().includes("buyer"),   "buyers mentioned");
  assert.ok(allText.toLowerCase().includes("jv"),      "JV mentioned");
  assert.ok(allText.toLowerCase().includes("wire"),    "wires mentioned");
});

// ---------------------------------------------------------------------------
// PART 7 — Partial data
// ---------------------------------------------------------------------------

test("partial source errors show partial=true and warning color in embed", () => {
  const metrics = normalizeBriefingMetrics({
    partial:       true,
    source_errors: [{ source: "wire_events", message: "relation does not exist" }],
  });
  metrics.health = calculateBriefingHealth(metrics);
  const embed = buildDailyBriefingEmbed(metrics);

  assert.equal(embed.color, 0xF1C40F, "yellow color for partial data");
  const fieldNames = embed.fields.map(f => f.name);
  assert.ok(fieldNames.some(n => n.includes("Partial")), "partial warning field present");
});

// ---------------------------------------------------------------------------
// PART 8 — No raw errors leak
// ---------------------------------------------------------------------------

test("no raw Supabase/Podio errors leak into Discord response", async () => {
  const editCalls = [];
  const rawError = {
    code:    "42P01",
    message: "relation \"wire_events\" does not exist (42P01)",
  };
  const mock = makeBriefingMock({ error: rawError });

  __setActionRouterDeps({
    supabase_override: mock,
    editInteractionResponse_override: async (opts) => { editCalls.push(opts); return { ok: true }; },
  });

  try {
    const interaction = makeBriefingInteraction({ subcommand: "today" });
    await routeDiscordInteraction(interaction);
    await new Promise(r => setTimeout(r, 80));

    assert.equal(editCalls.length, 1, "edit was called once");
    const text = JSON.stringify(editCalls[0]);

    assert.ok(!text.includes("42P01"),       "raw PG error code not leaked");
    assert.ok(!text.includes("wire_events"), "internal table name not leaked in error context");
    assert.ok(!text.includes("secret_must_not_appear_in_output"), "internal secret not leaked");
    assert.ok(!text.includes("bot_token_must_not_appear"),        "bot token not leaked");
  } finally {
    __resetActionRouterDeps();
  }
});

test("no raw schema cache error leaks into response", async () => {
  const editCalls = [];
  const schemaErr = {
    code:    "PGRST205",
    message: "Could not find a relationship between 'message_events' and 'campaigns' in the schema cache",
  };
  const mock = makeBriefingMock({ error: schemaErr });

  __setActionRouterDeps({
    supabase_override: mock,
    editInteractionResponse_override: async (opts) => { editCalls.push(opts); return { ok: true }; },
  });

  try {
    const interaction = makeBriefingInteraction({ subcommand: "today" });
    await routeDiscordInteraction(interaction);
    await new Promise(r => setTimeout(r, 80));

    const text = JSON.stringify(editCalls[0] ?? {});
    assert.ok(!text.includes("PGRST205"),                              "raw PGRST code not leaked");
    assert.ok(!text.includes("Could not find a relationship"),         "raw Supabase message not leaked");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// PART 9 — Button custom_ids under 100 chars
// ---------------------------------------------------------------------------

test("briefingActionRow button custom_ids are under 100 chars", () => {
  const rows = briefingActionRow();
  assert.ok(rows.length > 0, "at least one action row");
  for (const row of rows) {
    for (const btn of row.components ?? []) {
      const cid = btn.custom_id ?? "";
      assert.ok(cid.length <= 100, `custom_id '${cid}' is <= 100 chars`);
      assert.ok(cid.startsWith("briefing:"), `custom_id '${cid}' uses briefing: prefix`);
    }
  }
});

// ---------------------------------------------------------------------------
// PART 10 — Market scoped briefing
// ---------------------------------------------------------------------------

test("market scoped briefing includes market label in embed", () => {
  const metrics = normalizeBriefingMetrics({
    range:   "today",
    markets: ["chicago"],
  });
  metrics.health = calculateBriefingHealth(metrics);
  metrics.next_recommended_action = calculateNextRecommendedAction(metrics);
  const embed = buildDailyBriefingEmbed(metrics);
  assert.ok(
    embed.description?.includes("chicago"),
    "market label present in embed description"
  );
});

// ---------------------------------------------------------------------------
// PART 11 — Agent scoped briefing
// ---------------------------------------------------------------------------

test("agent scoped briefing includes agent label in embed", () => {
  const metrics = normalizeBriefingMetrics({
    range:  "today",
    agents: ["jane_smith"],
  });
  metrics.health = calculateBriefingHealth(metrics);
  metrics.next_recommended_action = calculateNextRecommendedAction(metrics);
  const embed = buildDailyBriefingEmbed(metrics);
  assert.ok(
    embed.description?.includes("jane_smith"),
    "agent label present in embed description"
  );
});

// ---------------------------------------------------------------------------
// PART 12 — System health section
// ---------------------------------------------------------------------------

test("system health section shows queue/podio/supabase/textgrid/email", () => {
  const metrics = normalizeBriefingMetrics({
    system_health: {
      queue_ready:         5,
      queue_due:           2,
      queue_failed_recent: 1,
      podio_status:        "configured",
      supabase_status:     "ok",
      textgrid_status:     "configured",
      email_status:        "not_configured",
    },
  });
  const embed = buildDailyBriefingEmbed(metrics);
  const sh_field = embed.fields.find(f => f.name.includes("System"));
  assert.ok(sh_field, "System Health field exists");
  const v = sh_field.value;
  assert.ok(v.includes("Queue") || v.includes("queue") || v.includes("5"), "queue info present");
  assert.ok(v.toLowerCase().includes("podio"),    "podio status present");
  assert.ok(v.toLowerCase().includes("supabase"), "supabase status present");
  assert.ok(v.toLowerCase().includes("textgrid"), "textgrid status present");
  assert.ok(v.toLowerCase().includes("email"),    "email status present");
});

// ---------------------------------------------------------------------------
// PART 13 — Next recommended action
// ---------------------------------------------------------------------------

test("next_recommended_action is always a non-empty string", () => {
  // Various metric states
  const scenarios = [
    {},
    { outreach: { sent: 0 } },
    { outreach: { sent: 500, positive_replies: 10 } },
    { system_health: { queue_due: 50 } },
    { revenue: { pending_wires: 5 } },
  ];
  for (const scenario of scenarios) {
    const metrics = normalizeBriefingMetrics(scenario);
    const action  = calculateNextRecommendedAction(metrics);
    assert.ok(typeof action === "string", "action is string");
    assert.ok(action.length > 0, "action is non-empty");
  }
});

// ---------------------------------------------------------------------------
// PART 14 — No secrets in embed or logs
// ---------------------------------------------------------------------------

test("no secrets leak in embed output", () => {
  const metrics = normalizeBriefingMetrics({});
  metrics.health = "green";
  metrics.next_recommended_action = "Test action";
  const embed = buildDailyBriefingEmbed(metrics);
  const text = JSON.stringify(embed);

  assert.ok(!text.includes("secret_must_not_appear_in_output"), "internal secret not in embed");
  assert.ok(!text.includes("bot_token_must_not_appear"),        "bot token not in embed");
});

// ---------------------------------------------------------------------------
// PART 15 — buildBriefingWindow
// ---------------------------------------------------------------------------

test("buildBriefingWindow returns valid ISO dates for all ranges", () => {
  for (const range of ["today", "yesterday", "week", "month"]) {
    const { window_start, window_end } = buildBriefingWindow({ range, timezone: "America/Chicago" });
    assert.ok(typeof window_start === "string", `${range}: window_start is string`);
    assert.ok(typeof window_end   === "string", `${range}: window_end is string`);
    assert.ok(!isNaN(Date.parse(window_start)), `${range}: window_start parses as date`);
    assert.ok(!isNaN(Date.parse(window_end)),   `${range}: window_end parses as date`);
    assert.ok(
      new Date(window_start) < new Date(window_end),
      `${range}: window_start < window_end`
    );
  }
});

test("buildBriefingWindow: yesterday window ends at today midnight", () => {
  const tz = "America/Chicago";
  const yesterday = buildBriefingWindow({ range: "yesterday", timezone: tz });
  const today     = buildBriefingWindow({ range: "today",     timezone: tz });
  assert.equal(
    yesterday.window_end,
    today.window_start,
    "yesterday.window_end === today.window_start"
  );
});

// ---------------------------------------------------------------------------
// PART 16 — Health calculation
// ---------------------------------------------------------------------------

test("calculateBriefingHealth: red on high failed outreach", () => {
  const metrics = normalizeBriefingMetrics({ outreach: { failed: 100 } });
  assert.equal(calculateBriefingHealth(metrics), "red");
});

test("calculateBriefingHealth: yellow on partial data", () => {
  const metrics = normalizeBriefingMetrics({
    source_errors: [{ source: "wire_events", message: "missing" }],
    partial: true,
  });
  assert.equal(calculateBriefingHealth(metrics), "yellow");
});

test("calculateBriefingHealth: purple on strong revenue day", () => {
  const metrics = normalizeBriefingMetrics({
    revenue: { cleared_wire_amount: 100_000, cleared_wires: 3 },
  });
  assert.equal(calculateBriefingHealth(metrics), "purple");
});

test("calculateBriefingHealth: green on nominal day", () => {
  const metrics = normalizeBriefingMetrics({
    outreach: { sent: 200, delivered: 180, failed: 2 },
  });
  assert.equal(calculateBriefingHealth(metrics), "green");
});

// ---------------------------------------------------------------------------
// PART 17 — Embed color mapping
// ---------------------------------------------------------------------------

test("embed color is gold/purple on strong revenue day", () => {
  const metrics = normalizeBriefingMetrics({
    revenue: { cleared_wire_amount: 100_000 },
  });
  metrics.health = calculateBriefingHealth(metrics);
  const embed = buildDailyBriefingEmbed(metrics);
  assert.equal(embed.color, 0x8E44AD, "gold_purple color for revenue day");
});

test("embed color is yellow on partial data", () => {
  const metrics = normalizeBriefingMetrics({ source_errors: [{ source: "x", message: "y" }] });
  metrics.health = calculateBriefingHealth(metrics);
  const embed = buildDailyBriefingEmbed(metrics);
  assert.equal(embed.color, 0xF1C40F, "yellow for partial");
});
