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

import {
  buildCampaignKey,
  normalizeMarketSlug,
  normalizeAssetType,
  normalizeStrategy,
  resolveTargetSourceViewName,
  buildTargetScanUrl,
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
} from "@/lib/discord/discord-components.js";

import {
  routeDiscordInteraction,
  __setActionRouterDeps,
  __resetActionRouterDeps,
} from "@/lib/discord/discord-action-router.js";

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

test("command registration includes target, territory, conquest and campaign create/inspect/scale", () => {
  const source = fs.readFileSync(
    "/Users/ryankindle/real-estate-automation/scripts/register-discord-commands.mjs",
    "utf8"
  );

  assert.ok(source.includes('name:        "target"'), "registers /target");
  assert.ok(source.includes('name:        "territory"'), "registers /territory");
  assert.ok(source.includes('name:        "conquest"'), "registers /conquest");
  assert.ok(source.includes('name:        "create"'), "registers /campaign create");
  assert.ok(source.includes('name:        "inspect"'), "registers /campaign inspect");
  assert.ok(source.includes('name:        "scale"'), "registers /campaign scale");
});
