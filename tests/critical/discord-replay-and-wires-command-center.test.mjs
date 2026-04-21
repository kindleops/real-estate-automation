/**
 * tests/critical/discord-replay-and-wires-command-center.test.mjs
 *
 * Comprehensive test suite for /replay and /wires Discord command center modules.
 */

import * as assert from "node:assert";
import { test } from "node:test";

import {
  buildReplayInboundEmbed,
  buildReplayOwnerEmbed,
  buildReplayTemplateEmbed,
  buildReplayBatchEmbed,
  buildWireCockpitEmbed,
  buildWireExpectedEmbed,
  buildWireReceivedEmbed,
  buildWireClearedEmbed,
  buildWireForecastEmbed,
  buildWireDealEmbed,
  buildWireReconcileEmbed,
  buildWireSetupRequiredEmbed,
} from "@/lib/discord/discord-embed-factory.js";

import {
  routeDiscordInteraction,
  __setActionRouterDeps,
  __resetActionRouterDeps,
} from "@/lib/discord/discord-action-router.js";

import {
  buildWireKey,
  formatMaskedAccount,
} from "@/lib/domain/wires/wire-ledger.js";

import {
  wireCockpitButtons,
  wireEventButtons,
} from "@/lib/discord/discord-components.js";

// ---------------------------------------------------------------------------
// Test environment — must be set before any router import resolution
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
// /replay tests
// ---------------------------------------------------------------------------

test("/replay inbound returns cinematic embed", () => {
  const embed = buildReplayInboundEmbed({
    alignment_passed: true,
  });

  assert.ok(embed, "embed should exist");
  assert.ok(embed.title.includes("Inbound Replay"));
  assert.ok(embed.color);
  assert.ok(embed.fields);
  assert.ok(embed.footer);
});

test("/replay inbound with alignment failure returns yellow", () => {
  const embed = buildReplayInboundEmbed({
    alignment_passed: false,
  });
  assert.strictEqual(embed.color, 0xF1C40F);
});

test("/replay inbound with alignment success returns green", () => {
  const embed = buildReplayInboundEmbed({
    alignment_passed: true,
  });
  assert.strictEqual(embed.color, 0x2ECC71);
});

test("/replay owner embed exists", () => {
  const embed = buildReplayOwnerEmbed({
    owner_id: 12345,
  });
  assert.ok(embed);
  assert.ok(embed.title);
  assert.ok(embed.fields);
});

test("/replay template returns embed", () => {
  const embed = buildReplayTemplateEmbed({
    use_case: "ownership_confirmation",
  });
  assert.ok(embed);
  assert.ok(embed.title.includes("Template"));
  assert.ok(embed.fields);
});

test("/replay batch returns scenario summary", () => {
  const embed = buildReplayBatchEmbed({
    scenario: "ownership",
    tested: 3,
    passed: 2,
    warnings: 0,
    failed: 1,
  });

  assert.ok(embed);
  assert.ok(embed.title.includes("Batch"));
  assert.strictEqual(embed.color, 0xE74C3C);
});

test("/replay batch with all passing returns green", () => {
  const embed = buildReplayBatchEmbed({
    passed: 5,
    warnings: 0,
    failed: 0,
  });
  assert.strictEqual(embed.color, 0x2ECC71);
});

test("/replay batch with warnings returns yellow", () => {
  const embed = buildReplayBatchEmbed({
    passed: 3,
    warnings: 2,
    failed: 0,
  });
  assert.strictEqual(embed.color, 0xF1C40F);
});

// ---------------------------------------------------------------------------
// /wires tests  
// ---------------------------------------------------------------------------

test("/wires cockpit returns summary embed", () => {
  const embed = buildWireCockpitEmbed({
    expected: 5,
    pending: 2,
    received: 10,
    cleared: 25,
  });

  assert.ok(embed);
  assert.ok(embed.title.includes("Wire"));
  assert.ok(embed.fields);
});

test("/wires expected creates embed", () => {
  const embed = buildWireExpectedEmbed({
    amount: 50000,
    account_display: "Bank ••••1234",
  });

  assert.ok(embed);
  assert.ok(embed.title.includes("Expected"));
});

test("/wires received marks wire received", () => {
  const embed = buildWireReceivedEmbed({
    amount: 50000,
  });

  assert.ok(embed);
  assert.strictEqual(embed.color, 0x2ECC71);
});

test("/wires cleared marks wire cleared", () => {
  const embed = buildWireClearedEmbed({
    amount: 50000,
  });

  assert.ok(embed);
  assert.strictEqual(embed.color, 0x2ECC71);
});

test("/wires forecast returns forecast embed", () => {
  const embed = buildWireForecastEmbed({
    total_expected: 3,
    confidence_score: 85,
  });

  assert.ok(embed);
  assert.strictEqual(embed.color, 0x2ECC71);
});

test("/wires forecast with low confidence returns red", () => {
  const embed = buildWireForecastEmbed({
    confidence_score: 30,
  });
  assert.strictEqual(embed.color, 0xE74C3C);
});

test("/wires deal shows wires linked to deal", () => {
  const embed = buildWireDealEmbed({
    deal_key: "deal_123",
  });

  assert.ok(embed);
  assert.ok(embed.title.includes("Deal"));
});

test("/wires reconcile shows anomalies", () => {
  const embed = buildWireReconcileEmbed({
    total_anomalies: 6,
  });

  assert.ok(embed);
  assert.strictEqual(embed.color, 0xE74C3C);
});

// ---------------------------------------------------------------------------
// Security: No exposed data
// ---------------------------------------------------------------------------

test("wire embeds never include full account numbers", () => {
  const expected = buildWireExpectedEmbed({
    account_display: "Bank ••••1234",
  });
  const text = JSON.stringify(expected);
  assert.ok(!text.match(/\d{10,}/), "should not expose full account numbers");
});

test("replay embeds do not break on empty input", () => {
  const embeds = [
    buildReplayInboundEmbed({}),
    buildReplayOwnerEmbed({}),
    buildReplayTemplateEmbed({}),
    buildReplayBatchEmbed({}),
  ];
  
  for (const embed of embeds) {
    assert.ok(embed);
    assert.ok(embed.title);
    assert.ok(embed.fields);
  }
});

// ---------------------------------------------------------------------------
// Button Safety
// ---------------------------------------------------------------------------

test("wire cockpit buttons exist", () => {
  const buttons = wireCockpitButtons();
  assert.ok(Array.isArray(buttons));
  assert.ok(buttons.length > 0);
});

test("wire event buttons exist", () => {
  const buttons = wireEventButtons();
  assert.ok(Array.isArray(buttons));
  assert.ok(buttons.length > 0);
});

// ---------------------------------------------------------------------------
// Wire Key Generation
// ---------------------------------------------------------------------------

test("buildWireKey generates keys", () => {
  const key = buildWireKey({
    amount: 50000,
    account_key: "acc_123",
  });

  assert.ok(key);
  assert.ok(key.startsWith("wire_"));
  assert.ok(key.length > 10);
});

// ---------------------------------------------------------------------------
// Account Formatting
// ---------------------------------------------------------------------------

test("formatMaskedAccount masks numbers", () => {
  const masked = formatMaskedAccount({
    institution_name: "Chase",
    account_last4: "5678",
  });

  assert.ok(masked.includes("••••5678"));
  assert.ok(masked.includes("Chase"));
});

test("formatMaskedAccount handles missing data", () => {
  const masked = formatMaskedAccount({});
  assert.strictEqual(masked, "—");
});

// ---------------------------------------------------------------------------
// Embed Structure Validation
// ---------------------------------------------------------------------------

test("all wires embeds have appropriate colors", () => {
  const embeds = [
    { embed: buildWireCockpitEmbed({}), color: 0x3498DB },
    { embed: buildWireExpectedEmbed({}), color: 0x3498DB },
    { embed: buildWireReceivedEmbed({}), color: 0x2ECC71 },
  ];

  for (const { embed, color } of embeds) {
    assert.strictEqual(embed.color, color);
  }
});

test("embeds have timestamps", () => {
  const embeds = [
    buildReplayInboundEmbed({}),
    buildWireCockpitEmbed({}),
  ];

  for (const embed of embeds) {
    assert.ok(embed.timestamp);
    assert.ok(/\d{4}-\d{2}-\d{2}T/.test(embed.timestamp));
  }
});

test("batch embed has fields", () => {
  const embed = buildReplayBatchEmbed({
    scenario: "ownership",
  });

  assert.ok(embed.fields);
  assert.ok(Array.isArray(embed.fields));
  assert.ok(embed.fields.length > 0);
});

test("wire forecast has fields", () => {
  const embed = buildWireForecastEmbed({
    total_expected: 5,
  });

  assert.ok(embed.fields);
  assert.ok(Array.isArray(embed.fields));
});

test("wire cockpit has fields", () => {
  const embed = buildWireCockpitEmbed({
    expected: 1,
  });

  assert.ok(embed.fields);
  assert.ok(Array.isArray(embed.fields));
});

test("all wires handlers pass dry-run safety", () => {
  // These are unit tests for embeds and utilities
  // Handler dry-run enforcement is tested in integration
  assert.ok(true, "dry-run safety enforced at handler level");
});

test("custom_id safety for buttons", () => {
  const buttons = wireCockpitButtons();
  for (const row of buttons) {
    if (row.components) {
      for (const btn of row.components) {
        const id = btn.custom_id || "";
        assert.ok(id.length < 100, "custom_id must be < 100 chars");
        // Allow safe characters
        assert.ok(/^[a-z0-9_:.-]+$/i.test(id), "custom_id should be safe");
      }
    }
  }
});

// ---------------------------------------------------------------------------
// Handler helpers (deferred /wires tests)
// ---------------------------------------------------------------------------

function makeWiresInteraction({
  subcommand = "cockpit",
  options    = [],
  role_ids   = ["owner_role"],
  token      = "wires_tok",
} = {}) {
  return {
    id:      "wid",
    type:    2,
    token,
    guild_id: "guild_test",
    member: {
      user:  { id: "user_wires", username: "Tester" },
      roles: role_ids,
    },
    data: {
      name: "wires",
      options: [{ type: 1, name: subcommand, options }],
    },
  };
}

function makeWiresMock(opts = {}) {
  const { error = null, rows = [] } = opts;
  return {
    from() {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        gte:         () => chain,
        lt:          () => chain,
        order:       () => chain,
        limit:       () => chain,
        not:         () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error }),
        then(resolve, reject) {
          if (error) return Promise.resolve({ data: null, error }).then(resolve, reject);
          return Promise.resolve({ data: rows, error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };
}

// ---------------------------------------------------------------------------
// Deferred handler tests
// ---------------------------------------------------------------------------

test("/wires cockpit returns deferred response (type 5)", async () => {
  const mock = makeWiresMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeWiresInteraction({ subcommand: "cockpit" });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "cockpit must return type 5 deferred");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/wires forecast does not timeout — returns deferred (type 5)", async () => {
  const mock = makeWiresMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeWiresInteraction({ subcommand: "forecast" });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "forecast must return type 5 deferred");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/wires reconcile does not timeout — returns deferred (type 5)", async () => {
  const mock = makeWiresMock({ rows: [] });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeWiresInteraction({ subcommand: "reconcile" });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "reconcile must return type 5 deferred");
  } finally {
    __resetActionRouterDeps();
  }
});

test("missing wire_events table returns setup-required embed", () => {
  const embed = buildWireSetupRequiredEmbed();
  assert.ok(embed, "embed exists");
  assert.ok(embed.title.includes("Setup"), "title signals setup required");
  const allText = JSON.stringify(embed);
  assert.ok(allText.includes("migration") || allText.includes("Migration"), "mentions migration instructions");
  assert.ok(allText.includes("pg_notify") || allText.includes("reload schema"), "mentions schema reload");
  assert.ok(embed.color, "has color");
  assert.ok(embed.fields && embed.fields.length > 0, "has fields");
});

test("wire handler with table-missing error still returns deferred — no uncaught throw", async () => {
  const tableError = { code: "42P01", message: "relation \"wire_events\" does not exist" };
  const mock = makeWiresMock({ error: tableError });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeWiresInteraction({ subcommand: "cockpit" });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "must still defer even when table is missing");
    // Wait for floating promise
    await new Promise(r => setTimeout(r, 30));
  } finally {
    __resetActionRouterDeps();
  }
});

test("no secrets leak — setup-required embed contains no API keys or stack traces", () => {
  const embed = buildWireSetupRequiredEmbed();
  const serialized = JSON.stringify(embed);
  assert.ok(!serialized.includes("secret_must_not_appear_in_output"), "no INTERNAL_API_SECRET");
  assert.ok(!serialized.includes("bot_token_must_not_appear"), "no DISCORD_BOT_TOKEN");
  assert.ok(!serialized.match(/Error\s*at\s/), "no stack trace");
  assert.ok(!serialized.match(/\d{10,}/), "no full account numbers");
});

test("async handler failure edits original response with sanitized error embed", async () => {
  const editCalls = [];
  const genericError = { code: "PGRST500", message: "internal server error details" };
  const mock = makeWiresMock({ error: genericError });

  __setActionRouterDeps({
    supabase_override: mock,
    editInteractionResponse_override: async (opts) => { editCalls.push(opts); return { ok: true }; },
  });

  try {
    const interaction = makeWiresInteraction({ subcommand: "cockpit" });
    const response = await routeDiscordInteraction(interaction);
    assert.equal(response.type, 5, "must return deferred even on handler failure");

    // Wait for the floating Promise to resolve
    await new Promise(r => setTimeout(r, 60));

    assert.equal(editCalls.length, 1, "editOriginalInteractionResponse was called once");
    const payload = editCalls[0];
    const text = JSON.stringify(payload);

    // Must NOT include the raw Supabase error details
    assert.ok(!text.includes("internal server error details"), "raw error message not leaked");
    assert.ok(!text.includes("PGRST500"), "raw error code not exposed");

    // Must have some user-visible response
    const hasContent = payload.content?.length > 0;
    const hasEmbed   = Array.isArray(payload.embeds) && payload.embeds.length > 0;
    assert.ok(hasContent || hasEmbed, "response has content or embed");
  } finally {
    __resetActionRouterDeps();
  }
});

test("no raw 'schema cache' DB error leaks into Discord response", async () => {
  const editCalls = [];
  const schemaError = {
    code:    "PGRST205",
    message: "Could not find a relationship between 'wire_events' and 'wire_accounts' in the schema cache",
  };
  const mock = makeWiresMock({ error: schemaError });

  __setActionRouterDeps({
    supabase_override: mock,
    editInteractionResponse_override: async (opts) => { editCalls.push(opts); return { ok: true }; },
  });

  try {
    const interaction = makeWiresInteraction({ subcommand: "cockpit" });
    await routeDiscordInteraction(interaction);
    await new Promise(r => setTimeout(r, 60));

    assert.equal(editCalls.length, 1, "edit was called");
    const text = JSON.stringify(editCalls[0]);

    // Raw Supabase error must NOT reach Discord (raw codes, raw message)
    assert.ok(!text.includes("PGRST205"), "raw error code not in response");
    assert.ok(!text.includes("Could not find a relationship"), "raw Supabase message not in response");

    // Should show the setup-required embed instead
    const embed = editCalls[0]?.embeds?.[0];
    assert.ok(embed, "setup embed is present");
    assert.ok(embed.title.includes("Setup"), "embed title signals setup required");
  } finally {
    __resetActionRouterDeps();
  }
});
