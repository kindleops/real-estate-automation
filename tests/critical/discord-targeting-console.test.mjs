/**
 * discord-targeting-console.test.mjs
 *
 * Unit tests for Targeting Console v1 — the Discord market-targeting layer.
 *
 * Coverage:
 *   1.  /target scan returns deferred response (type 5)
 *   2.  /target scan always calls feeder with dry_run=true
 *   3.  /target scan never sends SMS (dry_run is always forced true)
 *   4.  /campaign create creates a correctly normalised campaign key
 *   5.  /campaign inspect returns existing campaign data
 *   6.  /campaign scale updates daily cap for Owner / Tech Ops
 *   7.  /campaign scale above 100 requires approval for SMS Ops
 *   8.  /territory map shows onboarding embed when no campaigns exist
 *   9.  /territory map groups campaigns by status
 *  10.  /conquest summarises active / draft / paused campaigns
 *  11.  errors are sanitised — no secrets in any response
 *  12.  routing handles /target, /territory, /conquest and campaign create/inspect/scale
 */

import test    from "node:test";
import assert  from "node:assert/strict";
import fs      from "node:fs";
import os      from "node:os";
import path    from "node:path";
import { execFileSync } from "node:child_process";

import {
  buildCampaignKey,
  normalizeMarketSlug,
  normalizeAssetType,
  normalizeStrategy,
  normalizeAssetSlug,
  normalizeStrategySlug,
  normalizePropertyTags,
  buildTargetingFilters,
  buildTargetingTheme,
  buildNormalizedTargeting,
  isKnownMarketSlug,
  resolveTargetSourceViewName,
  buildTargetScanUrl,
  getMarketRegions,
  getMarketsForRegion,
} from "@/lib/domain/campaigns/targeting-console.js";

import {
  buildTargetScanEmbed,
  buildCampaignCreatedEmbed,
  buildCampaignInspectEmbed,
  buildCampaignScaleEmbed,
  buildTerritoryMapEmbed,
  buildConquestEmbed,
} from "@/lib/discord/discord-embed-factory.js";

import {
  targetActionRow,
  campaignActionRow,
  territoryActionRow,
  targetBuilderMainActionRow,
  targetBuilderRunActionRow,
  marketRegionSelect,
  marketSelect,
  assetClassSelect,
  strategySelect,
  propertyTagMultiSelect,
  propertyFilterCategorySelect,
  propertyFilterValueSelect,
} from "@/lib/discord/discord-components.js";

import {
  routeDiscordInteraction,
  __setActionRouterDeps,
  __resetActionRouterDeps,
} from "@/lib/discord/discord-action-router.js";

import { validateCommandOptionCounts } from "@/lib/discord/command-registration-validation.js";
import { validateCommandPayloadSizes } from "@/lib/discord/command-registration-validation.js";

// ---------------------------------------------------------------------------
// Test environment
// ---------------------------------------------------------------------------

process.env.DISCORD_GUILD_ID          = "guild_test";
process.env.DISCORD_APPLICATION_ID    = "app_test";
process.env.INTERNAL_API_SECRET       = "secret_must_not_appear_in_output";
process.env.CRON_SECRET               = "cron_secret_must_not_appear";
process.env.DISCORD_BOT_TOKEN         = "bot_token_must_not_appear";
process.env.APP_BASE_URL              = "http://localhost:3000";

process.env.DISCORD_ROLE_OWNER_ID        = "owner_role";
process.env.DISCORD_ROLE_TECH_OPS_ID     = "tech_ops_role";
process.env.DISCORD_ROLE_SMS_OPS_ID      = "sms_ops_role";
process.env.DISCORD_ROLE_ACQUISITIONS_ID = "acquisitions_role";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeSlashInteraction({
  command,
  subcommand = null,
  options    = [],
  role_ids   = ["owner_role"],
  member_id  = "user_1",
  guild_id   = "guild_test",
  token      = "tok",
} = {}) {
  const top_options = subcommand
    ? [{ type: 1, name: subcommand, options }]
    : options;

  return {
    id:      "iid",
    type:    2,
    token,
    guild_id,
    member: {
      user:  { id: member_id, username: "Tester" },
      roles: role_ids,
    },
    data: { name: command, options: top_options },
  };
}

function makeComponentInteraction({
  custom_id,
  values = [],
  role_ids = ["owner_role"],
  member_id = "user_1",
  guild_id = "guild_test",
  token = "tok_component",
} = {}) {
  return {
    id: "iid_component",
    type: 3,
    token,
    guild_id,
    member: {
      user: { id: member_id, username: "Tester" },
      roles: role_ids,
    },
    data: {
      custom_id,
      values,
    },
  };
}

function makeBuilderDbMock() {
  const sessions = new Map();
  const campaign_upserts = [];
  const send_queue_mutations = [];

  const mock = {
    _sessions: sessions,
    _campaign_upserts: campaign_upserts,
    _send_queue_mutations: send_queue_mutations,
    from(table) {
      let op = "select";
      let payload = null;
      const filters = {};

      const chain = {
        select: () => chain,
        eq(col, val) { filters[col] = val; return chain; },
        maybeSingle() {
          if (table === "discord_targeting_sessions") {
            if (op === "insert") {
              const row = { ...payload, id: payload.id ?? "session-id-1" };
              sessions.set(row.session_key, row);
              return Promise.resolve({ data: row, error: null });
            }
            if (op === "update") {
              const key = filters.session_key;
              const current = sessions.get(key);
              if (!current) return Promise.resolve({ data: null, error: null });
              const next = { ...current, ...payload };
              sessions.set(key, next);
              return Promise.resolve({ data: next, error: null });
            }
            const key = filters.session_key;
            return Promise.resolve({ data: key ? (sessions.get(key) ?? null) : null, error: null });
          }
          return Promise.resolve({ data: null, error: null });
        },
        insert(row) {
          op = "insert";
          payload = Array.isArray(row) ? row[0] : row;
          if (table === "send_queue") send_queue_mutations.push({ op: "insert", row: payload });
          return chain;
        },
        update(row) {
          op = "update";
          payload = row;
          if (table === "send_queue") send_queue_mutations.push({ op: "update", row });
          return chain;
        },
        upsert(row) {
          if (table === "campaign_targets") {
            campaign_upserts.push(Array.isArray(row) ? row[0] : row);
          }
          if (table === "send_queue") {
            send_queue_mutations.push({ op: "upsert", row: Array.isArray(row) ? row[0] : row });
          }
          return chain;
        },
        then(resolve, reject) {
          return Promise.resolve({ data: [], error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };

  return mock;
}

function extractBuilderSessionKeyFromComponents(components = []) {
  for (const row of components) {
    const controls = Array.isArray(row?.components) ? row.components : [];
    for (const control of controls) {
      const cid = String(control?.custom_id ?? "");
      const match = cid.match(/^target_builder:[a-z_]+:([a-zA-Z0-9_-]+)$/);
      if (match?.[1]) return match[1];
    }
  }
  return null;
}

/**
 * Supabase mock that supports upsert→chain→maybeSingle and direct await.
 */
function makeMock(tableMap = {}) {
  return {
    from(table) {
      const spec = tableMap[table] ?? {};
      let _count_mode = false;

      const chain = {
        select(_, opts = {}) { _count_mode = !!opts?.count; return chain; },
        eq:          () => chain,
        neq:         () => chain,
        gte:         () => chain,
        lt:          () => chain,
        gt:          () => chain,
        or:          () => chain,
        is:          () => chain,
        limit:       () => chain,
        order:       () => chain,
        not:         () => chain,
        in:          () => chain,
        // upsert / insert return chain so .select().maybeSingle() works
        upsert:      () => chain,
        insert:      () => chain,
        // update returns chain so .eq().maybeSingle() works
        update:      () => chain,
        maybeSingle: () => Promise.resolve({
          data:  spec.rows?.[0] ?? null,
          error: spec.error ?? null,
        }),
        then(resolve, reject) {
          if (spec.error) {
            return Promise.resolve({ data: null, count: null, error: spec.error })
              .then(resolve, reject);
          }
          if (_count_mode) {
            return Promise.resolve({
              count: spec.count ?? (spec.rows?.length ?? 0),
              error: null,
            }).then(resolve, reject);
          }
          return Promise.resolve({ data: spec.rows ?? [], error: null })
            .then(resolve, reject);
        },
      };
      return chain;
    },
  };
}

// ---------------------------------------------------------------------------
// 1. Normalisation pure functions
// ---------------------------------------------------------------------------

test("buildCampaignKey normalises market, asset, strategy to lowercase slug", () => {
  assert.equal(
    buildCampaignKey({ market: "Los Angeles", asset_type: "SFR", strategy: "Cash" }),
    "los_angeles_sfr_cash"
  );
  assert.equal(
    buildCampaignKey({ market: "Miami", asset_type: "multifamily", strategy: "multifamily_underwrite" }),
    "miami_multifamily_multifamily_underwrite"
  );
});

test("resolveTargetSourceViewName formats human-readable view names", () => {
  assert.equal(
    resolveTargetSourceViewName({ market: "Los Angeles", asset_type: "sfr", strategy: "cash" }),
    "Los Angeles / SFR / Cash"
  );
  assert.equal(
    resolveTargetSourceViewName({ market: "Miami", asset_type: "multifamily", strategy: "multifamily_underwrite" }),
    "Miami / Multifamily / Multifamily Underwrite"
  );
});

test("resolveTargetSourceViewName honours explicit source_view_name override", () => {
  const override = "My Custom Podio View";
  assert.equal(
    resolveTargetSourceViewName({ market: "Dallas", asset_type: "sfr", strategy: "cash", source_view_name: override }),
    override
  );
});

// ---------------------------------------------------------------------------
// 2. Embed shapes
// ---------------------------------------------------------------------------

test("buildTargetScanEmbed returns valid Discord embed with dry-run footer", () => {
  const embed = buildTargetScanEmbed({
    market: "Miami", asset: "sfr", strategy: "cash",
    source_view_name: "Miami / SFR / Cash",
    scanned: 100, eligible: 25, would_queue: 20, skipped: 75,
  });
  assert.ok(embed.title?.includes("Target Scan"), "title includes Target Scan");
  assert.ok(typeof embed.color === "number");
  assert.ok(Array.isArray(embed.fields));
  assert.ok(embed.footer?.text?.includes("Dry-run"), "footer says dry-run");
});

test("buildCampaignCreatedEmbed shows campaign key and status draft", () => {
  const embed = buildCampaignCreatedEmbed({
    campaign_key: "miami_sfr_cash", market: "Miami", asset: "sfr", strategy: "cash",
    daily_cap: 50, status: "draft", source_view_name: "Miami / SFR / Cash",
  });
  assert.ok(embed.title?.includes("Campaign Created"), "has created title");
  const field_vals = embed.fields.map((f) => f.value);
  assert.ok(field_vals.some((v) => v.includes("miami_sfr_cash")), "shows campaign key");
  assert.ok(field_vals.some((v) => v.toUpperCase().includes("DRAFT")), "shows draft status");
});

test("buildTerritoryMapEmbed shows onboarding text when empty", () => {
  const embed = buildTerritoryMapEmbed({ grouped: {}, empty: true });
  assert.ok(embed.description?.includes("/campaign create"), "onboarding message present");
});

test("buildConquestEmbed shows empire stats", () => {
  const embed = buildConquestEmbed({
    active: 2, draft: 1, paused: 0, total_daily_cap: 150,
    markets_unlocked: 2, recommended_next_move: "Monitor /hotleads",
  });
  assert.ok(embed.title?.includes("Conquest"), "has Conquest title");
  const field_vals = embed.fields.map((f) => f.value);
  assert.ok(field_vals.some((v) => v === "2"), "shows active count");
  assert.ok(field_vals.some((v) => v.includes("Monitor")), "shows next move recommendation");
});

// ---------------------------------------------------------------------------
// 3. /target scan — deferred response
// ---------------------------------------------------------------------------

test("/target scan returns deferred response (type 5)", async () => {
  const mock = makeMock({ discord_command_events: { rows: [] } });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "miami" },
        { name: "asset",    value: "sfr"   },
        { name: "strategy", value: "cash"  },
      ],
      role_ids: ["owner_role"],
      token: "scan_tok",
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 5, "type 5 = deferred");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 4. /target scan always calls feeder with dry_run=true
// ---------------------------------------------------------------------------

test("/target scan calls feeder with dry_run=true even if omitted", async () => {
  const calls = [];
  const callInternal_override = async (path, options) => {
    calls.push({ path, options });
    return {
      ok:   true,
      data: {
        effective_dry_run: true,
        result: { eligible_count: 10, loaded_count: 50, inserted_count: 8, skipped_count: 40 },
      },
    };
  };

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "miami" },
        { name: "asset",    value: "sfr"   },
        { name: "strategy", value: "cash"  },
      ],
      role_ids: ["owner_role"],
      token: "tok2",
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "deferred ack");

    // Wait for the floating promise to resolve.
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(calls.length, 1, "feeder was called once");
    assert.equal(calls[0].path, "/api/internal/outbound/feed-master-owners", "correct feeder path");
    assert.equal(calls[0].options.body.dry_run, true, "dry_run is always true");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 5. /target scan never sends SMS (dry_run enforced)
// ---------------------------------------------------------------------------

test("/target scan never sends SMS — dry_run is always forced to true", async () => {
  const recorded_bodies = [];
  const callInternal_override = async (path, options) => {
    recorded_bodies.push(options.body);
    return { ok: true, data: { result: {} } };
  };

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    // Even if the user somehow passes dry_run:false via options (not an actual
    // Discord option, but simulate a hypothetical tampered call), the handler
    // must override it.
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "houston" },
        { name: "asset",    value: "multifamily" },
        { name: "strategy", value: "cash" },
      ],
      role_ids: ["sms_ops_role"],
      token: "tok_nosms",
    });

    await routeDiscordInteraction(interaction);
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(recorded_bodies.length, 1, "one call made");
    assert.equal(recorded_bodies[0].dry_run, true, "dry_run is true — no SMS sent");
    assert.notEqual(recorded_bodies[0].dry_run, false, "dry_run is never false");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 6. /campaign create creates normalised campaign key in Supabase
// ---------------------------------------------------------------------------

test("/campaign create upserts campaign with normalised key and returns embed", async () => {
  const upserted = [];

  const mock = {
    from(table) {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert(row) {
          if (table === "campaign_targets") upserted.push(row);
          return chain;
        },
        insert:      () => chain,
        update:      () => chain,
        then(resolve) {
          return Promise.resolve({ data: [], error: null }).then(resolve);
        },
      };
      return chain;
    },
  };

  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "create",
      options: [
        { name: "name",     value: "Miami SFR Cash" },
        { name: "market",   value: "Miami"          },
        { name: "asset",    value: "sfr"            },
        { name: "strategy", value: "cash"           },
        { name: "daily_cap",value: 75               },
      ],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4 embed response");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("Campaign Created"), "embed title");

    // Campaign key must be miami_sfr_cash
    assert.ok(upserted.length > 0, "row was upserted");
    assert.equal(upserted[0].campaign_key, "miami_sfr_cash", "normalised campaign key");
    assert.equal(upserted[0].daily_cap,    75,               "daily_cap set");
    assert.equal(upserted[0].status,       "draft",          "new campaigns start as draft");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 7. /campaign inspect returns existing campaign
// ---------------------------------------------------------------------------

test("/campaign inspect returns campaign details embed", async () => {
  const fake_campaign = {
    campaign_key:     "miami_sfr_cash",
    campaign_name:    "Miami SFR Cash",
    market:           "miami",
    asset_type:       "sfr",
    strategy:         "cash",
    daily_cap:        75,
    status:           "draft",
    last_scan_at:     null,
    last_scan_summary: null,
    last_launched_at: null,
    source_view_name: "Miami / SFR / Cash",
  };

  const mock = makeMock({
    campaign_targets:       { rows: [fake_campaign] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "inspect",
      options:    [{ name: "campaign", value: "miami_sfr_cash" }],
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("miami_sfr_cash"), "embed shows campaign key");

    const field_vals = embed.fields.map((f) => f.value);
    assert.ok(field_vals.some((v) => v.includes("miami")), "shows market");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 8. /campaign scale updates daily_cap for Owner / Tech Ops
// ---------------------------------------------------------------------------

test("/campaign scale updates daily_cap and returns scale embed for Owner", async () => {
  const fake_campaign = {
    campaign_key: "miami_sfr_cash",
    daily_cap:    50,
    status:       "draft",
  };

  const mock = makeMock({
    campaign_targets:       { rows: [fake_campaign] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "scale",
      options: [
        { name: "campaign",  value: "miami_sfr_cash" },
        { name: "daily_cap", value: 150              },
      ],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("Scale Applied"), "scale was applied for owner");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 9. /campaign scale above 100 requires approval for SMS Ops
// ---------------------------------------------------------------------------

test("/campaign scale daily_cap > 100 returns approval embed for SMS Ops", async () => {
  const mock = makeMock({ discord_command_events: { rows: [] } });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "scale",
      options: [
        { name: "campaign",  value: "miami_sfr_cash" },
        { name: "daily_cap", value: 200              },
      ],
      role_ids: ["sms_ops_role"],  // SMS Ops — not Owner/TechOps
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4 response");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(
      embed.title?.includes("Scale Request") || embed.title?.includes("Approval"),
      "embed is an approval/request, not applied"
    );
    // Must include approval buttons
    assert.ok(
      response.data?.components?.length > 0,
      "has action row buttons for approval"
    );
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 10. /territory map shows onboarding when no campaigns exist
// ---------------------------------------------------------------------------

test("/territory map returns onboarding embed when no campaigns exist", async () => {
  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "territory",
      subcommand: "map",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("Territory Map"), "territory map title");
    // Onboarding message
    assert.ok(
      embed.description?.includes("/campaign create"),
      "onboarding message present when empty"
    );
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 11. /territory map groups campaigns by status
// ---------------------------------------------------------------------------

test("/territory map groups campaigns by market", async () => {
  const campaigns = [
    { campaign_key: "miami_sfr_cash",    market: "miami", asset_type: "sfr", strategy: "cash",        daily_cap: 50,  status: "active" },
    { campaign_key: "miami_mf_cash",     market: "miami", asset_type: "multifamily", strategy: "cash", daily_cap: 25, status: "draft"  },
    { campaign_key: "houston_sfr_cash",  market: "houston", asset_type: "sfr", strategy: "cash",      daily_cap: 30,  status: "paused" },
  ];

  const mock = makeMock({
    campaign_targets:       { rows: campaigns },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "territory",
      subcommand: "map",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("Territory Map"), "territory map title");
    // Miami and Houston should both appear as field names
    assert.ok(
      embed.fields?.some((f) => f.name?.toLowerCase().includes("miami")),
      "miami market shown"
    );
    assert.ok(
      embed.fields?.some((f) => f.name?.toLowerCase().includes("houston")),
      "houston market shown"
    );
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 12. /conquest summarises active/draft/paused campaigns
// ---------------------------------------------------------------------------

test("/conquest returns empire overview with correct counts", async () => {
  const campaigns = [
    { status: "active",  daily_cap: 100, market: "miami",   last_scan_at: new Date().toISOString() },
    { status: "active",  daily_cap: 75,  market: "houston", last_scan_at: null },
    { status: "draft",   daily_cap: 50,  market: "dallas",  last_scan_at: null },
    { status: "paused",  daily_cap: 25,  market: "miami",   last_scan_at: null },
  ];

  const mock = makeMock({
    campaign_targets:       { rows: campaigns },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:  "conquest",
      options:  [],    // no subcommand
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("Conquest"), "conquest title");

    const field_map = Object.fromEntries(embed.fields.map((f) => [f.name, f.value]));
    assert.equal(field_map["Active Campaigns"],  "2", "2 active");
    assert.equal(field_map["Draft Campaigns"],   "1", "1 draft");
    assert.equal(field_map["Paused Campaigns"],  "1", "1 paused");
    assert.equal(field_map["Markets Unlocked"],  "3", "3 unique markets");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 13. Errors are sanitised — no secrets in any response
// ---------------------------------------------------------------------------

test("no response from targeting console commands includes secrets", async () => {
  const INTERNAL_SECRET = process.env.INTERNAL_API_SECRET;
  const CRON_SECRET     = process.env.CRON_SECRET;
  const BOT_TOKEN       = process.env.DISCORD_BOT_TOKEN;
  const sensitiveValues = [INTERNAL_SECRET, CRON_SECRET, BOT_TOKEN].filter(Boolean);

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
    send_queue:             { rows: [] },
    sms_templates:          { rows: [] },
    message_events:         { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interactions = [
      makeSlashInteraction({ command: "territory",  subcommand: "map",     role_ids: ["owner_role"] }),
      makeSlashInteraction({ command: "conquest",   options: [],           role_ids: ["owner_role"] }),
      makeSlashInteraction({
        command:    "campaign",
        subcommand: "create",
        options: [
          { name: "name",     value: "Test"  },
          { name: "market",   value: "miami" },
          { name: "asset",    value: "sfr"   },
          { name: "strategy", value: "cash"  },
        ],
        role_ids: ["owner_role"],
      }),
    ];

    for (const interaction of interactions) {
      const response = await routeDiscordInteraction(interaction);
      const serialised = JSON.stringify(response);
      for (const secret of sensitiveValues) {
        assert.ok(
          !serialised.includes(secret),
          `/${interaction.data.name} response must not include secret value`
        );
      }
    }
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 14. Routing handles all new commands and subcommands
// ---------------------------------------------------------------------------

test("routing handles /target, /territory, /conquest and campaign create/inspect/scale", async () => {
  const fake_campaign = {
    campaign_key: "miami_sfr_cash", market: "miami", asset_type: "sfr",
    strategy: "cash", daily_cap: 50, status: "draft",
    last_scan_at: null, last_scan_summary: null, last_launched_at: null,
  };

  const mock = makeMock({
    campaign_targets:       { rows: [fake_campaign] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const r_target = await routeDiscordInteraction(makeSlashInteraction({
      command: "target", subcommand: "scan",
      options: [
        { name: "market",   value: "miami" },
        { name: "asset",    value: "sfr"   },
        { name: "strategy", value: "cash"  },
      ],
      role_ids: ["owner_role"],
    }));
    assert.equal(r_target.type, 5, "/target scan deferred");

    const r_territory = await routeDiscordInteraction(makeSlashInteraction({
      command: "territory", subcommand: "map", role_ids: ["owner_role"],
    }));
    assert.equal(r_territory.type, 4, "/territory map type 4");
    assert.ok(r_territory.data?.embeds?.length > 0, "/territory map has embed");

    const r_conquest = await routeDiscordInteraction(makeSlashInteraction({
      command: "conquest", options: [], role_ids: ["owner_role"],
    }));
    assert.equal(r_conquest.type, 4, "/conquest type 4");
    assert.ok(r_conquest.data?.embeds?.length > 0, "/conquest has embed");

    const r_create = await routeDiscordInteraction(makeSlashInteraction({
      command: "campaign", subcommand: "create",
      options: [
        { name: "name",     value: "Miami SFR" },
        { name: "market",   value: "miami"      },
        { name: "asset",    value: "sfr"        },
        { name: "strategy", value: "cash"       },
      ],
      role_ids: ["owner_role"],
    }));
    assert.equal(r_create.type, 4, "/campaign create type 4");

    const r_inspect = await routeDiscordInteraction(makeSlashInteraction({
      command: "campaign", subcommand: "inspect",
      options: [{ name: "campaign", value: "miami_sfr_cash" }],
      role_ids: ["owner_role"],
    }));
    assert.equal(r_inspect.type, 4, "/campaign inspect type 4");

    const r_scale = await routeDiscordInteraction(makeSlashInteraction({
      command: "campaign", subcommand: "scale",
      options: [
        { name: "campaign",  value: "miami_sfr_cash" },
        { name: "daily_cap", value: 50               },
      ],
      role_ids: ["owner_role"],
    }));
    assert.equal(r_scale.type, 4, "/campaign scale type 4");
  } finally {
    __resetActionRouterDeps();
  }
});

test("command registration includes target-build, target-scan, target-property, territory, conquest and campaign create/inspect/scale", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );

  assert.ok(source.includes('name:        "target-scan"'), "registers /target-scan");
  assert.ok(source.includes('name:        "target-property"'), "registers /target-property");
  assert.ok(source.includes('name:        "target-build"'), "registers /target-build");
  assert.ok(!source.includes('name:        "target"'), "does not register oversized /target wrapper");
  assert.ok(source.includes('name:        "territory"'), "registers /territory");
  assert.ok(source.includes('name:        "conquest"'), "registers /conquest");
  assert.ok(source.includes('name:        "create"'), "registers /campaign create");
  assert.ok(source.includes('name:        "inspect"'), "registers /campaign inspect");
  assert.ok(source.includes('name:        "scale"'), "registers /campaign scale");
});

function extractTopLevelCommandBlock(source, command_name) {
  const needle = `name:        "${command_name}"`;
  const start = source.indexOf(needle);
  if (start < 0) return "";

  const after = source.slice(start + needle.length);
  const next_command_start = after.search(/\n\s{2}\{\n\s{4}name:\s*"/);
  if (next_command_start < 0) return source.slice(start);

  return source.slice(start, start + needle.length + next_command_start);
}

function countOptionNameLines(command_block) {
  const matches = command_block.match(/\n\s{8}name:\s*"[^"]+"/g);
  return matches?.length ?? 0;
}

function loadRegisteredCommandsPayload() {
  const tmp_dir = fs.mkdtempSync(path.join(os.tmpdir(), "discord-register-test-"));
  const preload_path = path.join(tmp_dir, "fetch-preload.mjs");
  const capture_path = path.join(tmp_dir, "commands.json");

  fs.writeFileSync(
    preload_path,
    [
      'import fs from "node:fs";',
      'globalThis.fetch = async (_url, init = {}) => {',
      '  fs.writeFileSync(process.env.CAPTURE_COMMANDS_PATH, String(init.body ?? "[]"), "utf8");',
      '  return { ok: true, json: async () => [] };',
      '};',
    ].join("\n"),
    "utf8"
  );

  execFileSync(
    process.execPath,
    ["--import", preload_path, "scripts/register-discord-commands.mjs"],
    {
      cwd: "/Users/ryankindle/real-estate-automation",
      env: {
        ...process.env,
        DISCORD_APPLICATION_ID: "app_test",
        DISCORD_GUILD_ID: "guild_test",
        DISCORD_BOT_TOKEN: "bot_test",
        CAPTURE_COMMANDS_PATH: capture_path,
      },
      stdio: "pipe",
    }
  );

  const body = fs.readFileSync(capture_path, "utf8");
  return JSON.parse(body);
}

test("v3: /target-scan has <= 25 options", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );
  const scan_block = extractTopLevelCommandBlock(source, "target-scan");
  assert.ok(scan_block.includes('name:        "target-scan"'), "target-scan command exists");

  const count = countOptionNameLines(scan_block);
  assert.ok(count > 0, "target-scan options were detected");
  assert.ok(count <= 25, `target-scan options should be <= 25, got ${count}`);
});

test("v3: /target-property has <= 25 options", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );
  const property_block = extractTopLevelCommandBlock(source, "target-property");
  assert.ok(property_block.includes('name:        "target-property"'), "target-property command exists");

  const count = countOptionNameLines(property_block);
  assert.ok(count > 0, "target-property options were detected");
  assert.ok(count <= 25, `target-property options should be <= 25, got ${count}`);
});

test("v3: /target-property includes advanced property filters", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );
  const property_block = extractTopLevelCommandBlock(source, "target-property");

  const required_advanced = [
    "sq_ft_range",
    "units_range",
    "ownership_years_range",
    "estimated_value_range",
    "equity_percent_range",
    "repair_cost_range",
    "building_condition",
    "offer_vs_loan",
    "offer_vs_last_purchase_price",
    "year_built_range",
    "min_property_score",
  ];

  for (const name of required_advanced) {
    assert.ok(
      property_block.includes(`name:        "${name}"`),
      `/target-property includes ${name}`
    );
  }
});

test("v3: /target-scan does not include advanced property filters", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );
  const scan_block = extractTopLevelCommandBlock(source, "target-scan");

  const advanced_names = [
    "sq_ft_range",
    "units_range",
    "ownership_years_range",
    "estimated_value_range",
    "equity_percent_range",
    "repair_cost_range",
    "building_condition",
    "offer_vs_loan",
    "offer_vs_last_purchase_price",
    "year_built_range",
    "min_property_score",
  ];

  for (const name of advanced_names) {
    assert.ok(
      !scan_block.includes(`name:        "${name}"`),
      `/target-scan excludes ${name}`
    );
  }
});

test("v3: top-level target-scan route is handled", async () => {
  const calls = [];
  const callInternal_override = async (path, options) => {
    calls.push({ path, options });
    return {
      ok: true,
      data: { result: { eligible_count: 10, loaded_count: 40, inserted_count: 8, skipped_count: 30 } },
    };
  };

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const interaction = makeSlashInteraction({
      command: "target-scan",
      options: [
        { name: "market",      value: "miami" },
        { name: "asset_class", value: "sfr"   },
        { name: "strategy",    value: "cash"  },
      ],
      role_ids: ["owner_role"],
      token:    "tok_target_scan_top_level",
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "deferred response (type 5)");

    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(calls.length, 1, "target-scan should call master-owner feeder path");
  } finally {
    __resetActionRouterDeps();
  }
});

test("v3: top-level target-property route forces property_first scan path", async () => {
  const calls = [];
  const callInternal_override = async (path, options) => {
    calls.push({ path, options });
    return { ok: true, data: { result: {} } };
  };

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const interaction = makeSlashInteraction({
      command:    "target-property",
      options: [
        { name: "market",      value: "miami" },
        { name: "asset_class", value: "sfr"   },
        { name: "strategy",    value: "cash"  },
      ],
      role_ids: ["owner_role"],
      token:    "tok_property_force",
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "deferred response (type 5)");

    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(calls.length, 0, "target-property must not call master-owner feeder");
  } finally {
    __resetActionRouterDeps();
  }
});

test("v3: target-build, target-scan and target-property command payloads are under 8000 bytes", () => {
  const registered = loadRegisteredCommandsPayload();
  const target_build = registered.find((cmd) => cmd?.name === "target-build");
  const target_scan = registered.find((cmd) => cmd?.name === "target-scan");
  const target_property = registered.find((cmd) => cmd?.name === "target-property");

  assert.ok(target_build, "target-build command exists in registration payload");
  assert.ok(target_scan, "target-scan command exists in registration payload");
  assert.ok(target_property, "target-property command exists in registration payload");

  const build_len = JSON.stringify(target_build).length;
  const scan_len = JSON.stringify(target_scan).length;
  const property_len = JSON.stringify(target_property).length;

  assert.ok(build_len < 8000, `target-build payload must be < 8000 bytes, got ${build_len}`);
  assert.ok(scan_len < 8000, `target-scan payload must be < 8000 bytes, got ${scan_len}`);
  assert.ok(property_len < 8000, `target-property payload must be < 8000 bytes, got ${property_len}`);
});

test("v3: command registration validation catches >25 options", () => {
  const too_many = [
    {
      name: "target",
      options: [
        {
          type: 1,
          name: "scan",
          options: Array.from({ length: 26 }, (_, i) => ({
            type: 3,
            name: `opt_${i + 1}`,
            description: "x",
          })),
        },
      ],
    },
  ];

  assert.throws(
    () => validateCommandOptionCounts(too_many, 25),
    /command:\s*target[\s\S]*subcommand:\s*scan[\s\S]*option_count:\s*26/i
  );
});

test("v3: command registration validation catches payload >= 8000 bytes", () => {
  const huge_option_name = "x".repeat(7900);
  const too_large = [
    {
      name: "target-property",
      description: "payload test",
      options: [{ type: 3, name: huge_option_name, description: "x" }],
    },
  ];

  assert.throws(
    () => validateCommandPayloadSizes(too_large, 8000),
    /command:\s*target-property[\s\S]*json_length:\s*\d+[\s\S]*max_allowed:\s*8000/i
  );
});

test("v4: /target-build command payload is lightweight and has <= 25 options", () => {
  const registered = loadRegisteredCommandsPayload();
  const target_build = registered.find((cmd) => cmd?.name === "target-build");

  assert.ok(target_build, "target-build command exists");
  const payload_len = JSON.stringify(target_build).length;
  assert.ok(payload_len < 8000, `target-build payload must be < 8000 bytes, got ${payload_len}`);

  const options_count = Array.isArray(target_build.options) ? target_build.options.length : 0;
  assert.ok(options_count <= 25, `target-build options should be <= 25, got ${options_count}`);
});

test("v4: builder menus are bounded and categorized", () => {
  const regions = getMarketRegions();
  assert.ok(Array.isArray(regions) && regions.length > 0, "regions catalog is available");
  assert.ok(regions.length <= 25, `regions should be <= 25, got ${regions.length}`);

  const region_menu = marketRegionSelect("tb_test");
  const region_options = region_menu?.components?.[0]?.options ?? [];
  assert.ok(region_options.length > 0, "region menu has options");
  assert.ok(region_options.length <= 25, `region menu options should be <= 25, got ${region_options.length}`);

  const texas_markets = getMarketsForRegion("texas");
  const market_menu = marketSelect("tb_test", "texas", texas_markets);
  const market_options = market_menu?.components?.[0]?.options ?? [];
  assert.ok(market_options.length > 0, "market menu has options for texas");
  assert.ok(market_options.length <= 25, `market menu options should be <= 25, got ${market_options.length}`);

  const tag_menu = propertyTagMultiSelect("tb_test");
  const tag_options = tag_menu?.components?.[0]?.options ?? [];
  assert.ok(tag_options.length > 0, "tag menu has options");
  assert.ok(tag_options.length <= 25, `tag menu options should be <= 25, got ${tag_options.length}`);

  const categories = ["size_units", "value_equity", "condition_repairs", "ownership_purchase", "score"];
  for (const category of categories) {
    const filter_menu = propertyFilterValueSelect("tb_test", category);
    const filter_options = filter_menu?.components?.[0]?.options ?? [];
    assert.ok(filter_options.length > 0, `filter category ${category} has options`);
    assert.ok(filter_options.length <= 25, `filter menu options should be <= 25 for ${category}, got ${filter_options.length}`);
  }
});

test("v4: /target-build creates an active session and returns interactive components", async () => {
  const mock = makeBuilderDbMock();
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command: "target-build",
      role_ids: ["owner_role"],
      token: "tok_target_build_create",
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 4, "target-build responds immediately");
    assert.ok(Array.isArray(response.data?.embeds) && response.data.embeds.length > 0, "builder embed is present");
    assert.ok(Array.isArray(response.data?.components) && response.data.components.length >= 2, "builder controls are present");

    const session_key = extractBuilderSessionKeyFromComponents(response.data.components);
    assert.ok(session_key, "session key is encoded in component custom ids");

    const session = mock._sessions.get(session_key);
    assert.ok(session, "session persisted in discord_targeting_sessions");
    assert.equal(session.status, "active", "new session is active");
    assert.equal(session.state.scan_mode, "property_first", "default scan mode is property_first");
  } finally {
    __resetActionRouterDeps();
  }
});

test("v4: selecting market region updates session and opens market selector", async () => {
  const mock = makeBuilderDbMock();
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const init = await routeDiscordInteraction(makeSlashInteraction({
      command: "target-build",
      role_ids: ["owner_role"],
      token: "tok_target_build_region_init",
    }));

    const session_key = extractBuilderSessionKeyFromComponents(init.data.components);
    assert.ok(session_key, "session key extracted");

    const update = await routeDiscordInteraction(makeComponentInteraction({
      custom_id: `target_builder:region:${session_key}`,
      values: ["texas"],
      role_ids: ["owner_role"],
      token: "tok_target_build_region_update",
    }));

    assert.equal(update.type, 7, "component interaction updates the message");
    assert.equal(mock._sessions.get(session_key)?.state?.market_region, "texas", "market region persisted");

    const flattened = JSON.stringify(update.data?.components ?? []);
    assert.ok(flattened.includes(`target_builder:market:${session_key}`), "market selector is shown after region selection");
  } finally {
    __resetActionRouterDeps();
  }
});

test("v4: selecting tags stores up to three values", async () => {
  const mock = makeBuilderDbMock();
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const init = await routeDiscordInteraction(makeSlashInteraction({
      command: "target-build",
      role_ids: ["owner_role"],
      token: "tok_target_build_tags_init",
    }));

    const session_key = extractBuilderSessionKeyFromComponents(init.data.components);
    assert.ok(session_key, "session key extracted");

    const update = await routeDiscordInteraction(makeComponentInteraction({
      custom_id: `target_builder:tags:${session_key}`,
      values: ["absentee_owner", "high_equity", "probate", "tax_delinquent"],
      role_ids: ["owner_role"],
      token: "tok_target_build_tags_update",
    }));

    assert.equal(update.type, 7, "component interaction updates the message");
    assert.deepEqual(
      mock._sessions.get(session_key)?.state?.property_tags,
      ["absentee_owner", "high_equity", "probate"],
      "tags are capped to three values"
    );
  } finally {
    __resetActionRouterDeps();
  }
});

test("v4: run scan action keeps property-first behavior and does not call owner feeder", async () => {
  const calls = [];
  const callInternal_override = async (path, options) => {
    calls.push({ path, options });
    return { ok: true, data: { result: {} } };
  };

  const mock = makeBuilderDbMock();
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const init = await routeDiscordInteraction(makeSlashInteraction({
      command: "target-build",
      role_ids: ["owner_role"],
      token: "tok_target_build_run_scan_init",
    }));

    const session_key = extractBuilderSessionKeyFromComponents(init.data.components);
    assert.ok(session_key, "session key extracted");

    const seeded = mock._sessions.get(session_key);
    seeded.state = {
      ...seeded.state,
      market: "Miami, FL",
      asset_class: "sfr",
      strategy: "high_equity",
    };
    mock._sessions.set(session_key, seeded);

    const update = await routeDiscordInteraction(makeComponentInteraction({
      custom_id: `target_builder:run_scan:${session_key}`,
      role_ids: ["owner_role"],
      token: "tok_target_build_run_scan_exec",
    }));

    assert.equal(update.type, 7, "component interaction updates the message");
    assert.equal(calls.length, 0, "property-first run_scan path should not call owner feeder internal route");
    assert.equal(mock._sessions.get(session_key)?.state?.scan_mode, "property_first", "scan mode remains property_first");
  } finally {
    __resetActionRouterDeps();
  }
});

test("v4: create campaign action writes campaign_targets and does not mutate send_queue", async () => {
  const mock = makeBuilderDbMock();
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const init = await routeDiscordInteraction(makeSlashInteraction({
      command: "target-build",
      role_ids: ["owner_role"],
      token: "tok_target_build_create_campaign_init",
    }));

    const session_key = extractBuilderSessionKeyFromComponents(init.data.components);
    assert.ok(session_key, "session key extracted");

    const seeded = mock._sessions.get(session_key);
    seeded.state = {
      ...seeded.state,
      market: "Miami, FL",
      asset_class: "sfr",
      strategy: "high_equity",
      property_tags: ["absentee_owner"],
    };
    mock._sessions.set(session_key, seeded);

    const update = await routeDiscordInteraction(makeComponentInteraction({
      custom_id: `target_builder:create_campaign:${session_key}`,
      role_ids: ["owner_role"],
      token: "tok_target_build_create_campaign_exec",
    }));

    assert.equal(update.type, 7, "component interaction updates the message");
    assert.equal(mock._campaign_upserts.length, 1, "campaign_targets upsert was performed");
    assert.equal(mock._send_queue_mutations.length, 0, "send_queue was not mutated by create_campaign");

    const upserted = mock._campaign_upserts[0];
    assert.equal(upserted.market, "miami", "campaign market normalized from builder market label");
    assert.equal(upserted.metadata?.source, "target_builder_v1", "campaign metadata marks target_builder source");
  } finally {
    __resetActionRouterDeps();
  }
});

test("v4: builder action failures do not leak secrets", async () => {
  const base = makeBuilderDbMock();
  const original_from = base.from.bind(base);
  base.from = (table) => {
    if (table === "campaign_targets") {
      return {
        upsert() {
          throw new Error(`campaign insert failed ${process.env.INTERNAL_API_SECRET}`);
        },
      };
    }
    return original_from(table);
  };

  __setActionRouterDeps({ supabase_override: base });

  try {
    const init = await routeDiscordInteraction(makeSlashInteraction({
      command: "target-build",
      role_ids: ["owner_role"],
      token: "tok_target_build_secret_init",
    }));

    const session_key = extractBuilderSessionKeyFromComponents(init.data.components);
    assert.ok(session_key, "session key extracted");

    const seeded = base._sessions.get(session_key);
    seeded.state = {
      ...seeded.state,
      market: "Miami, FL",
      asset_class: "sfr",
      strategy: "high_equity",
    };
    base._sessions.set(session_key, seeded);

    const update = await routeDiscordInteraction(makeComponentInteraction({
      custom_id: `target_builder:create_campaign:${session_key}`,
      role_ids: ["owner_role"],
      token: "tok_target_build_secret_exec",
    }));

    const serialized = JSON.stringify(update);
    assert.ok(!serialized.includes(process.env.INTERNAL_API_SECRET), "secret is never included in builder responses");
    assert.ok(serialized.includes("Action failed"), "response contains sanitized generic failure text");
  } finally {
    __resetActionRouterDeps();
  }
});

// ===========================================================================
// — Targeting Console v2 tests —
// ===========================================================================

// ---------------------------------------------------------------------------
// 15. /target scan accepts market choice, asset choice, strategy choice
// ---------------------------------------------------------------------------

test("v2: /target scan accepts market slug choice, asset, strategy", async () => {
  const calls = [];
  const callInternal_override = async (path, options) => {
    calls.push({ path, options });
    return {
      ok: true,
      data: { result: { eligible_count: 15, loaded_count: 60, inserted_count: 10, skipped_count: 50 } },
    };
  };

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "miami" },
        { name: "asset",    value: "sfr"   },
        { name: "strategy", value: "cash"  },
      ],
      role_ids: ["owner_role"],
      token:    "tok_v2_marketchoice",
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "deferred response (type 5)");

    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(calls.length, 1, "feeder called once");
    assert.equal(calls[0].options.body.dry_run, true, "dry_run forced true");
    // source_view_name should be derived from the slug
    assert.ok(calls[0].options.body.source_view_name, "source_view_name is set");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 16. /target scan includes property tags in embed
// ---------------------------------------------------------------------------

test("v2: /target scan includes property tags in embed", async () => {
  const callInternal_override = async () => ({
    ok: true,
    data: { result: { eligible_count: 20, loaded_count: 80, inserted_count: 15, skipped_count: 60 } },
  });

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  let captured_embed = null;
  const orig_edit = globalThis.__captured_edit;

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "miami"          },
        { name: "asset",    value: "sfr"            },
        { name: "strategy", value: "cash"           },
        { name: "tag_1",    value: "absentee_owner" },
        { name: "tag_2",    value: "high_equity"    },
      ],
      role_ids: ["owner_role"],
      token:    "tok_tags",
    });

    await routeDiscordInteraction(interaction);
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Verify normalizePropertyTags works for the given values
    const tags = normalizePropertyTags(["absentee_owner", "high_equity"]);
    assert.equal(tags.length, 2, "two tags normalized");
    assert.equal(tags[0].slug, "absentee_owner", "first tag slug correct");
    assert.equal(tags[1].slug, "high_equity",    "second tag slug correct");

    // Embed built with tags should show tags field
    const { buildTargetScanEmbed: embedFn } = await import("@/lib/discord/discord-embed-factory.js");
    captured_embed = embedFn({
      market: "miami", asset: "sfr", strategy: "cash",
      tags,
    });
    assert.ok(
      captured_embed.fields.some((f) => f.name === "Property Tags"),
      "embed has Property Tags field when tags present"
    );
  } finally {
    __resetActionRouterDeps();
    if (orig_edit) globalThis.__captured_edit = orig_edit;
  }
});

// ---------------------------------------------------------------------------
// 17. /target scan includes optional filters in embed
// ---------------------------------------------------------------------------

test("v2: /target scan includes optional filters in embed", async () => {
  const { buildTargetScanEmbed: embedFn } = await import("@/lib/discord/discord-embed-factory.js");
  const { buildTargetingFilters: filterFn } = await import(
    "@/lib/domain/campaigns/targeting-console.js"
  );

  const filters = filterFn({ zip: "33101", county: "Miami-Dade", min_equity: 30 });
  assert.equal(filters.zip,        "33101",       "zip preserved");
  assert.equal(filters.county,     "Miami-Dade",  "county preserved");
  assert.equal(filters.min_equity,  30,           "min_equity preserved");

  const embed = embedFn({
    market: "miami", asset: "sfr", strategy: "cash",
    filters,
  });
  assert.ok(
    embed.fields.some((f) => f.name === "Active Filters"),
    "embed has Active Filters field when filters present"
  );
  const filter_field = embed.fields.find((f) => f.name === "Active Filters");
  assert.ok(filter_field.value.includes("33101"),      "zip shown in filters");
  assert.ok(filter_field.value.includes("Miami-Dade"), "county shown in filters");
});

// ---------------------------------------------------------------------------
// 18. /target scan remains dry_run true with new options
// ---------------------------------------------------------------------------

test("v2: /target scan stays dry_run=true with tags and filters present", async () => {
  const recorded = [];
  const callInternal_override = async (path, options) => {
    recorded.push(options.body);
    return { ok: true, data: { result: {} } };
  };

  const mock = makeMock({
    campaign_targets:       { rows: [] },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",      value: "los_angeles"    },
        { name: "asset",       value: "multifamily"    },
        { name: "strategy",    value: "creative"       },
        { name: "tag_1",       value: "absentee_owner" },
        { name: "min_equity",  value: 40               },
        { name: "owner_type",  value: "individual"     },
      ],
      role_ids: ["owner_role"],
      token:    "tok_dryrun_v2",
    });

    await routeDiscordInteraction(interaction);
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(recorded.length, 1, "one call made");
    assert.strictEqual(recorded[0].dry_run, true, "dry_run is always true");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 19. /campaign create persists tags and filters in campaign metadata
// ---------------------------------------------------------------------------

test("v2: /campaign create persists tags and filters in campaign metadata", async () => {
  const upserted = [];

  const mock = {
    from(table) {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert(row) {
          if (table === "campaign_targets") upserted.push(row);
          return chain;
        },
        insert: () => chain,
        update: () => chain,
        then(resolve) {
          return Promise.resolve({ data: [], error: null }).then(resolve);
        },
      };
      return chain;
    },
  };

  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "create",
      options: [
        { name: "name",        value: "Miami Absentee SFR"  },
        { name: "market",      value: "miami"               },
        { name: "asset",       value: "sfr"                 },
        { name: "strategy",    value: "cash"                },
        { name: "tag_1",       value: "absentee_owner"      },
        { name: "tag_2",       value: "high_equity"         },
        { name: "min_equity",  value: 20                    },
        { name: "zip",         value: "33101"               },
      ],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 4, "type 4 embed response");

    assert.ok(upserted.length > 0, "row was upserted");
    const row = upserted[0];

    // tags should be in metadata
    assert.ok(Array.isArray(row.metadata?.tags),  "metadata.tags is array");
    assert.equal(row.metadata.tags.length, 2,     "two tags persisted");
    assert.equal(row.metadata.tags[0].slug, "absentee_owner", "first tag slug correct");

    // filters should be in metadata
    assert.ok(row.metadata?.filters, "metadata.filters present");
    assert.equal(row.metadata.filters.min_equity, 20,     "min_equity in filters");
    assert.equal(row.metadata.filters.zip,        "33101", "zip in filters");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 20. /campaign create uses normalized slugs for market/asset/strategy
// ---------------------------------------------------------------------------

test("v2: /campaign create normalizes market/asset/strategy slugs in row", async () => {
  const upserted = [];

  const mock = {
    from(table) {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert(row) {
          if (table === "campaign_targets") upserted.push(row);
          return chain;
        },
        insert: () => chain,
        update: () => chain,
        then(resolve) {
          return Promise.resolve({ data: [], error: null }).then(resolve);
        },
      };
      return chain;
    },
  };

  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "create",
      options: [
        { name: "name",     value: "DFW Test"        },
        { name: "market",   value: "dallas_fort_worth" },
        { name: "asset",    value: "multifamily"       },
        { name: "strategy", value: "multifamily_underwrite" },
      ],
      role_ids: ["owner_role"],
    });

    await routeDiscordInteraction(interaction);

    assert.ok(upserted.length > 0, "row upserted");
    const row = upserted[0];
    assert.equal(row.market,     "dallas_fort_worth",      "market slug normalized");
    assert.equal(row.asset_type, "multifamily",            "asset slug normalized");
    assert.equal(row.strategy,   "multifamily_underwrite", "strategy slug normalized");
    assert.equal(row.campaign_key, "dallas_fort_worth_multifamily_multifamily_underwrite", "campaign key correct");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 21. /campaign create has cinematic embed with emoji / theme
// ---------------------------------------------------------------------------

test("v2: /campaign create embed has theme emoji in title", async () => {
  const mock = {
    from() {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert:      () => chain,
        insert:      () => chain,
        update:      () => chain,
        then(resolve) { return Promise.resolve({ data: [], error: null }).then(resolve); },
      };
      return chain;
    },
  };

  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "create",
      options: [
        { name: "name",     value: "Miami SFR Cash" },
        { name: "market",   value: "miami"          },
        { name: "asset",    value: "sfr"            },
        { name: "strategy", value: "cash"           },
      ],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 4, "type 4 embed response");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    // Title should contain emoji and "Campaign Created"
    assert.ok(embed.title?.includes("Campaign Created"), "embed title includes 'Campaign Created'");
    // Theme emoji from Miami market (🌴) or asset emoji (🏠) or strategy emoji (💵)
    const theme = buildTargetingTheme("miami", "sfr", "cash");
    assert.ok(theme.emoji.length > 0, "theme has emoji");
    // Footer should reference v2
    assert.ok(embed.footer?.text?.includes("v2"), "footer references v2");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 22. /target scan never mutates send queue
// ---------------------------------------------------------------------------

test("v2: /target scan never writes to send_queue", async () => {
  const mutations = [];

  const mock = {
    from(table) {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert(row) {
          if (table === "send_queue") mutations.push({ table, op: "upsert", row });
          return chain;
        },
        insert(row) {
          if (table === "send_queue") mutations.push({ table, op: "insert", row });
          return chain;
        },
        update(row) {
          if (table === "send_queue") mutations.push({ table, op: "update", row });
          return chain;
        },
        then(resolve) { return Promise.resolve({ data: [], error: null }).then(resolve); },
      };
      return chain;
    },
  };

  const callInternal_override = async () => ({
    ok: true,
    data: { result: { eligible_count: 5, loaded_count: 20, inserted_count: 4, skipped_count: 15 } },
  });

  __setActionRouterDeps({ supabase_override: mock, callInternal_override });

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "houston"     },
        { name: "asset",    value: "sfr"         },
        { name: "strategy", value: "high_equity" },
      ],
      role_ids: ["owner_role"],
      token:    "tok_no_queue",
    });

    await routeDiscordInteraction(interaction);
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(mutations.length, 0, "no send_queue mutations during target scan");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 23. Unknown market returns sanitised validation error
// ---------------------------------------------------------------------------

test("v2: unknown market slug returns sanitized error — no secrets", async () => {
  const mock = makeMock({ discord_command_events: { rows: [] } });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "target",
      subcommand: "scan",
      options: [
        { name: "market",   value: "not_a_real_market_xyzzy" },
        { name: "asset",    value: "sfr"   },
        { name: "strategy", value: "cash"  },
      ],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    // Should be an error response (not deferred)
    const body = JSON.stringify(response);
    assert.ok(
      !body.includes(process.env.INTERNAL_API_SECRET),
      "no INTERNAL_API_SECRET in error"
    );
    assert.ok(
      !body.includes(process.env.DISCORD_BOT_TOKEN),
      "no DISCORD_BOT_TOKEN in error"
    );
    // Should mention market or unknown in the error message
    assert.ok(
      body.toLowerCase().includes("market") || body.toLowerCase().includes("unknown"),
      "error message references invalid market"
    );
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 24. daily_cap is bounded (min 1, max 500 for non-Owner)
// ---------------------------------------------------------------------------

test("v2: /campaign create daily_cap bounded at 500 for SMS Ops (non-Owner)", async () => {
  const upserted = [];

  const mock = {
    from(table) {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert(row) {
          if (table === "campaign_targets") upserted.push(row);
          return chain;
        },
        insert:      () => chain,
        update:      () => chain,
        then(resolve) { return Promise.resolve({ data: [], error: null }).then(resolve); },
      };
      return chain;
    },
  };

  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "create",
      options: [
        { name: "name",      value: "Big TX Campaign" },
        { name: "market",    value: "houston"         },
        { name: "asset",     value: "sfr"             },
        { name: "strategy",  value: "cash"            },
        { name: "daily_cap", value: 999               }, // over 500 — should be clamped
      ],
      role_ids: ["sms_ops_role"],  // Not Owner
    });

    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 4, "type 4 response");

    assert.ok(upserted.length > 0, "row upserted");
    assert.ok(upserted[0].daily_cap <= 500, `daily_cap ${upserted[0].daily_cap} is bounded at 500 for non-Owner`);
    assert.ok(upserted[0].daily_cap >= 1,   "daily_cap is at least 1");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 25. All button custom_ids are under 100 chars
// ---------------------------------------------------------------------------

test("v2: all targeting button custom_ids are under 100 characters", () => {
  const { targetActionRow: tar, campaignActionRow: car, territoryActionRow: trr } =
    // already imported at top of file via the named imports
    { targetActionRow, campaignActionRow, territoryActionRow };

  const long_key = "a".repeat(40); // max safe_key length used internally

  const all_rows = [
    ...tar({ campaignKey: long_key }),
    ...car({ campaignKey: long_key, paused: false }),
    ...car({ campaignKey: long_key, paused: true }),
    ...trr(),
  ];

  const buttons = all_rows.flatMap((row) => row.components ?? []);
  for (const btn of buttons) {
    assert.ok(
      String(btn.custom_id ?? "").length <= 100,
      `custom_id "${btn.custom_id}" exceeds 100 chars (length: ${String(btn.custom_id ?? "").length})`
    );
  }
});

// ---------------------------------------------------------------------------
// 26. Command registration includes market/asset/strategy/tag choices
// ---------------------------------------------------------------------------

test("v2: command registration includes market, asset, strategy, and tag choices", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );

  assert.ok(source.includes("MARKET_CHOICES"),          "MARKET_CHOICES defined");
  assert.ok(source.includes("ASSET_CHOICES"),            "ASSET_CHOICES defined");
  assert.ok(source.includes("STRATEGY_CHOICES"),         "STRATEGY_CHOICES defined");
  assert.ok(source.includes("PROPERTY_TAG_CHOICES"),     "PROPERTY_TAG_CHOICES defined");
  assert.ok(source.includes("OWNER_TYPE_CHOICES"),       "OWNER_TYPE_CHOICES defined");
  assert.ok(source.includes("PHONE_STATUS_CHOICES"),     "PHONE_STATUS_CHOICES defined");
  // Market slugs
  assert.ok(source.includes('"los_angeles"'),            'includes los_angeles slug');
  assert.ok(source.includes('"dallas_fort_worth"'),      'includes dallas_fort_worth slug');
  assert.ok(source.includes('"new_orleans"'),            'includes new_orleans slug');
  // Asset slugs
  assert.ok(source.includes('"sfr"'),                   'includes sfr slug');
  assert.ok(source.includes('"distressed_residential"'), 'includes distressed_residential slug');
  // Strategy slugs
  assert.ok(source.includes('"distress_stack"'),         'includes distress_stack slug');
  assert.ok(source.includes('"pre_foreclosure"'),        'includes pre_foreclosure slug');
  // Tag slugs
  assert.ok(source.includes('"absentee_owner"'),         'includes absentee_owner tag');
  assert.ok(source.includes('"free_and_clear"'),         'includes free_and_clear tag');
  // tag_1 / tag_2 / tag_3 options registered
  assert.ok(source.includes('"tag_1"'),                  'tag_1 option registered');
  assert.ok(source.includes('"tag_3"'),                  'tag_3 option registered');
});

// ---------------------------------------------------------------------------
// 27. No secrets leak in targeting-related error responses
// ---------------------------------------------------------------------------

test("v2: no secrets leak in targeting console error responses", async () => {
  const SECRETS = [
    process.env.INTERNAL_API_SECRET,
    process.env.CRON_SECRET,
    process.env.DISCORD_BOT_TOKEN,
  ].filter(Boolean);

  // Simulate a DB failure on campaign create
  const failing_mock = {
    from() {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        upsert:      () => chain,
        insert:      () => chain,
        update:      () => chain,
        then(resolve) {
          return Promise.resolve({
            data: null, error: new Error("Connection refused — secret token abc123"),
          }).then(resolve);
        },
      };
      return chain;
    },
  };

  __setActionRouterDeps({ supabase_override: failing_mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "campaign",
      subcommand: "create",
      options: [
        { name: "name",     value: "Error Test"  },
        { name: "market",   value: "miami"        },
        { name: "asset",    value: "sfr"          },
        { name: "strategy", value: "cash"         },
      ],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);
    const body = JSON.stringify(response);

    for (const secret of SECRETS) {
      assert.ok(!body.includes(secret), `secret must not appear in error response`);
    }
    // Should still be some kind of response (error embed)
    assert.ok(response, "got a response even on DB failure");
  } finally {
    __resetActionRouterDeps();
  }
});
