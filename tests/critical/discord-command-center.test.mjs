/**
 * discord-command-center.test.mjs
 *
 * Unit tests for the cinematic Discord command-center upgrade.
 *
 * Coverage:
 *   - buildMissionStatusEmbed returns a valid Discord embed shape
 *   - missionButtons returns action rows with mission: custom_id prefix
 *   - approvalButtons uses approval:approve: and approval:deny: prefixes
 *   - /templates audit reads sms_templates and builds template audit embed
 *   - /templates stage1 counts active Stage 1 / first-touch ownership templates
 *   - /mission status handles missing optional tables without crashing
 *   - /feeder scan returns deferred response (type 5)
 *   - errorResponse is always ephemeral (flags includes 64)
 *   - No response includes env secrets (INTERNAL_API_SECRET, CRON_SECRET, BOT_TOKEN)
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  buildMissionStatusEmbed,
  buildLaunchPreflightEmbed,
  buildQueueCockpitEmbed,
  buildTemplateAuditEmbed,
  buildLeadInspectEmbed,
  buildHotLeadEmbed,
  buildSuccessEmbed,
  buildErrorEmbed,
  buildApprovalEmbed,
} from "@/lib/discord/discord-embed-factory.js";

import {
  missionButtons,
  queueButtons,
  preflightButtons,
  templateAuditButtons,
  leadInspectButtons,
  campaignControlButtons,
  approvalButtons,
} from "@/lib/discord/discord-components.js";

import {
  deferredPublicResponse,
  deferredEphemeralResponse,
} from "@/lib/discord/discord-followups.js";

import {
  errorResponse,
  cinematicMessage,
  deferMessage,
  safeAllowedMentions,
  formatCommandError,
} from "@/lib/discord/discord-response-helpers.js";

import {
  routeDiscordInteraction,
  __setActionRouterDeps,
  __resetActionRouterDeps,
  encodeFeederPayloadForCustomId,
  decodeFeederPayloadFromCustomId,
} from "@/lib/discord/discord-action-router.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Build a minimal Discord APPLICATION_COMMAND interaction (type 2).
 */
function makeSlashInteraction({
  command,
  subcommand = null,
  options    = [],
  role_ids   = ["owner_role"],
  member_id  = "user_1234",
  guild_id   = "guild_5678",
  token      = "test_token",
} = {}) {
  const top_options = subcommand
    ? [{ type: 1, name: subcommand, options }]
    : options;

  return {
    id:      "interaction_id",
    type:    2,
    token,
    guild_id,
    member: {
      user:  { id: member_id, username: "TestUser" },
      roles: role_ids,
    },
    data: {
      name:    command,
      options: top_options,
    },
  };
}

function makeComponentInteraction({
  custom_id,
  role_ids  = ["owner_role"],
  member_id = "user_1234",
  guild_id  = "guild_5678",
  token     = "component_token",
} = {}) {
  return {
    id: `component_${String(custom_id || "unknown").slice(0, 20)}`,
    type: 3,
    token,
    guild_id,
    member: {
      user: { id: member_id, username: "TestUser" },
      roles: role_ids,
    },
    data: { custom_id },
  };
}

/**
 * Chainable Supabase query mock.
 * tableMap: { [tableName]: { rows?, count?, error? } }
 */
function makeSupabaseMock(tableMap = {}) {
  const calls = [];
  const mock = {
    _calls: calls,
    getUpdates(table) {
      return calls.filter((call) => call.type === "update" && call.table === table);
    },
    from(table) {
      const spec = tableMap[table] ?? {};
      let _count_mode = false;
      let _pending_write = null;

      const chain = {
        select(fields, opts = {}) {
          _count_mode = !!opts?.count;
          return chain;
        },
        eq:          (column, value) => {
          if (_pending_write) _pending_write.filters.push({ op: "eq", column, value });
          return chain;
        },
        neq:         () => chain,
        gte:         () => chain,
        lte:         () => chain,
        gt:          () => chain,
        lt:          () => chain,
        or:          () => chain,
        is:          () => chain,
        limit:       () => chain,
        order:       () => chain,
        not:         () => chain,
        range:       () => chain,
        in:          (...a) => chain,
        upsert:      (row, options) => {
          calls.push({ type: "upsert", table, row, options });
          return Promise.resolve({ error: spec.error ?? null });
        },
        insert:      (row) => {
          calls.push({ type: "insert", table, row });
          return Promise.resolve({ error: spec.error ?? null });
        },
        update:      (values) => {
          _pending_write = { type: "update", table, values, filters: [] };
          calls.push(_pending_write);
          return chain;
        },
        maybeSingle: () => Promise.resolve({ data: spec.rows?.[0] ?? null, error: spec.error ?? null }),
        then(resolve, reject) {
          if (spec.error) {
            return Promise.resolve({ data: null, count: null, error: spec.error }).then(resolve, reject);
          }
          if (_count_mode) {
            return Promise.resolve({ count: spec.count ?? (spec.rows?.length ?? 0), error: null }).then(resolve, reject);
          }
          return Promise.resolve({ data: spec.rows ?? [], error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };
  return mock;
}

// Suppress audit log and callInternal noise in tests.
process.env.DISCORD_GUILD_ID         = "guild_5678";
process.env.INTERNAL_API_SECRET      = "super_secret_that_must_not_leak";
process.env.CRON_SECRET              = "cron_secret_value_that_must_not_leak";
process.env.DISCORD_BOT_TOKEN        = "bot_token_that_must_not_leak";
process.env.APP_BASE_URL             = "http://localhost:3000";
process.env.DISCORD_APPLICATION_ID   = "app_id_1";

// Map role label names to the synthetic IDs used in test interactions.
process.env.DISCORD_ROLE_OWNER_ID        = "owner_role";
process.env.DISCORD_ROLE_TECH_OPS_ID     = "tech_ops_role";
process.env.DISCORD_ROLE_SMS_OPS_ID      = "sms_ops_role";
process.env.DISCORD_ROLE_ACQUISITIONS_ID = "acquisitions_role";

// ---------------------------------------------------------------------------
// 1. Embed factory — valid Discord embed shape
// ---------------------------------------------------------------------------

test("buildMissionStatusEmbed returns a valid Discord embed shape", () => {
  const embed = buildMissionStatusEmbed({
    overall_status:  "healthy",
    queue_counts:    { queued: 5, sending: 2, failed: 0 },
    active_templates: 14,
    stage1_templates: 3,
    recent_events:   42,
    failed_syncs:    0,
    supabase_ok:     true,
    podio_ok:        true,
    textgrid_ok:     true,
  });

  assert.ok(embed, "embed exists");
  assert.equal(typeof embed.title, "string", "title is a string");
  assert.ok(embed.title.length > 0, "title is non-empty");
  assert.ok(typeof embed.color === "number", "color is a number");
  assert.ok(Array.isArray(embed.fields), "fields is an array");
  assert.ok(embed.fields.length > 0, "has at least one field");

  const field = embed.fields[0];
  assert.equal(typeof field.name,   "string", "field name is a string");
  assert.equal(typeof field.value,  "string", "field value is a string");
  assert.equal(typeof field.inline, "boolean","field inline is a boolean");

  // Green for healthy
  assert.equal(embed.color, 0x2ECC71, "healthy status → green color");
});

test("buildLaunchPreflightEmbed reflects GO/WARN/HOLD colour correctly", () => {
  const go   = buildLaunchPreflightEmbed({ overall_status: "GO",   checks: [] });
  const warn = buildLaunchPreflightEmbed({ overall_status: "WARN", checks: [] });
  const hold = buildLaunchPreflightEmbed({ overall_status: "HOLD", checks: [] });

  assert.equal(go.color,   0x2ECC71, "GO → green");
  assert.equal(warn.color, 0xF1C40F, "WARN → yellow");
  assert.equal(hold.color, 0xE74C3C, "HOLD → red");
});

test("buildTemplateAuditEmbed includes inventory and stage1 fields", () => {
  const embed = buildTemplateAuditEmbed({
    total: 20, active: 15, inactive: 5,
    by_language: { en: 12, es: 3 },
    by_use_case: { ownership_check: 8, follow_up: 7 },
    by_stage_code: { S1: 3, S2: 9 },
    active_first_touch: 3,
    active_ownership_check: 8,
    missing_template_body: 0,
    missing_language: 0,
    missing_use_case: 0,
    missing_stage_code: 2,
    blockers: [],
  });

  assert.ok(embed.fields.some(f => f.name.includes("Inventory")), "has Inventory field");
  assert.ok(embed.fields.some(f => f.name.includes("Stage 1")),   "has Stage 1 field");
  assert.ok(embed.fields.some(f => f.name.includes("Language")),  "has By Language field");
});

// ---------------------------------------------------------------------------
// 2. Components — correct custom_id prefixes
// ---------------------------------------------------------------------------

test("missionButtons returns action rows with mission: custom_id prefix", () => {
  const rows = missionButtons();
  assert.ok(Array.isArray(rows), "returns an array");
  assert.ok(rows.length > 0, "has at least one action row");
  assert.equal(rows[0].type, 1, "type 1 = action row");
  assert.ok(Array.isArray(rows[0].components), "has components");
  const ids = rows[0].components.map(c => c.custom_id);
  assert.ok(ids.every(id => id.startsWith("mission:")), `all custom_ids start with 'mission:': ${ids}`);
});

test("queueButtons returns action row with queue: custom_ids", () => {
  const rows = queueButtons();
  const ids  = rows.flatMap(r => r.components.map(c => c.custom_id));
  assert.ok(ids.some(id => id.startsWith("queue:")), "at least one queue: id");
});

test("preflightButtons returns preflight: custom_ids", () => {
  const rows = preflightButtons();
  const ids  = rows.flatMap(r => r.components.map(c => c.custom_id));
  assert.ok(ids.some(id => id.startsWith("preflight:")), "has preflight: id");
});

test("approvalButtons uses approval:approve: and approval:deny: prefixes", () => {
  const rows = approvalButtons({ actionId: "abc123", approveLabel: "Launch", denyLabel: "Cancel" });
  assert.ok(Array.isArray(rows), "returns array");
  const ids = rows.flatMap(r => r.components.map(c => c.custom_id));
  assert.ok(ids.some(id => id.startsWith("approval:approve:")), "has approval:approve: id");
  assert.ok(ids.some(id => id.startsWith("approval:deny:")),    "has approval:deny: id");
  // actionId preserved
  assert.ok(ids.some(id => id.includes("abc123")), "actionId is embedded in custom_id");
});

test("approvalButtons safely strips unsafe characters from actionId", () => {
  const rows = approvalButtons({ actionId: "id with <script>!!" });
  const ids  = rows.flatMap(r => r.components.map(c => c.custom_id));
  for (const id of ids) {
    assert.ok(!id.includes("<"), "no angle brackets in custom_id");
    assert.ok(!id.includes(">"), "no angle brackets in custom_id");
  }
});

test("leadInspectButtons returns lead: prefixed ids", () => {
  const rows = leadInspectButtons({ ownerId: "42", phone: "+15551234567" });
  const ids  = rows.flatMap(r => r.components.map(c => c.custom_id));
  assert.ok(ids.some(id => id.startsWith("lead:")), "has lead: prefix");
});

// ---------------------------------------------------------------------------
// 3. discord-followups — deferral helpers
// ---------------------------------------------------------------------------

test("deferredPublicResponse returns type 5 without flags", () => {
  const r = deferredPublicResponse();
  assert.equal(r.type, 5, "type is 5 (deferred)");
  assert.ok(!r.data?.flags, "no flags set");
});

test("deferredEphemeralResponse returns type 5 with ephemeral flag", () => {
  const r = deferredEphemeralResponse();
  assert.equal(r.type, 5, "type is 5 (deferred)");
  assert.equal(r.data?.flags, 64, "ephemeral flag set");
});

// ---------------------------------------------------------------------------
// 4. discord-response-helpers additions
// ---------------------------------------------------------------------------

test("errorResponse is ephemeral (flags includes 64)", () => {
  const r = errorResponse("something went wrong");
  assert.equal(r.type, 4, "type 4");
  assert.ok(r.data?.flags & 64, "ephemeral flag set");
});

test("cinematicMessage returns type 4 with safe allowed_mentions", () => {
  const embed = buildSuccessEmbed({ title: "Test" });
  const r = cinematicMessage({ embeds: [embed] });
  assert.equal(r.type, 4, "type 4");
  assert.ok(Array.isArray(r.data?.embeds), "has embeds");
  assert.deepEqual(r.data?.allowed_mentions, { parse: [] }, "no mention parse");
});

test("cinematicMessage ephemeral=true sets flags 64", () => {
  const r = cinematicMessage({ content: "test", ephemeral: true });
  assert.ok(r.data?.flags & 64, "ephemeral flag set");
});

test("deferMessage returns type 5", () => {
  const r = deferMessage({});
  assert.equal(r.type, 5, "type 5");
});

test("safeAllowedMentions returns {parse:[]}", () => {
  assert.deepEqual(safeAllowedMentions(), { parse: [] });
});

test("formatCommandError does not expose long secrets", () => {
  const secret = process.env.INTERNAL_API_SECRET;
  const result = formatCommandError(new Error(`failed with secret ${secret}`));
  assert.ok(!result.includes(secret), "secret not exposed in formatted error");
});

// ---------------------------------------------------------------------------
// 5. /templates audit reads sms_templates
// ---------------------------------------------------------------------------

test("/templates audit reads sms_templates and builds embed", async () => {
  const mockRows = [
    { id: 1, is_active: true,  language: "en", use_case: "ownership_check", stage_code: "S1", is_first_touch: true,  template_body: "Hello {{name}}" },
    { id: 2, is_active: true,  language: "es", use_case: "ownership_check", stage_code: "S2", is_first_touch: false, template_body: "Hola {{name}}" },
    { id: 3, is_active: false, language: "en", use_case: "follow_up",       stage_code: "S3", is_first_touch: false, template_body: "" },
  ];

  const mock = makeSupabaseMock({
    sms_templates: { rows: mockRows, count: mockRows.length },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "templates",
      subcommand: "audit",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got a response");
    assert.equal(response.type, 4, "type 4 response (not deferred)");
    assert.ok(response.data?.embeds?.length > 0, "response has embeds");

    const embed = response.data.embeds[0];
    assert.ok(embed.title?.includes("Audit") || embed.title?.includes("sms_templates"), "embed title references audit or sms_templates");
    assert.ok(Array.isArray(embed.fields), "embed has fields");

    // Verify inventory field contains count information
    const inventoryField = embed.fields.find(f => f.name?.includes("Inventory"));
    assert.ok(inventoryField, "has Inventory field");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 7. /mission status handles missing optional tables gracefully
// ---------------------------------------------------------------------------

test("/mission status does not crash when optional tables throw", async () => {
  const mock = makeSupabaseMock({
    // send_queue throws, message_events throws, etc.
    send_queue:             { error: { message: "relation does not exist" } },
    message_events:         { error: { message: "relation does not exist" } },
    sms_templates:          { error: { message: "relation does not exist" } },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "mission",
      subcommand: "status",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got a response (did not throw)");
    assert.equal(response.type, 4, "returns type 4 even when tables are missing");
    assert.ok(response.data?.embeds?.length > 0, "still returns an embed");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 8. /feeder scan returns deferred response (type 5)
// ---------------------------------------------------------------------------

test("/feeder scan returns deferred response type 5", async () => {
  const mock = makeSupabaseMock({
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "feeder",
      subcommand: "scan",
      options:    [{ name: "limit", value: 10 }],
      role_ids:   ["owner_role"],
      token:      "feeder_scan_token",
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got a response");
    assert.equal(response.type, 5, "type 5 = deferred (feeder scan must defer immediately)");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/feeder scan permission check — non-team role is denied", async () => {
  const mock = makeSupabaseMock({
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "feeder",
      subcommand: "scan",
      role_ids:   ["some_random_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response.data?.content?.includes("🚫") || response.data?.flags & 64,
      "non-team member gets denied / ephemeral response");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 9. No response includes env-var secrets
// ---------------------------------------------------------------------------

test("no response includes INTERNAL_API_SECRET or CRON_SECRET", async () => {
  const INTERNAL_API_SECRET = process.env.INTERNAL_API_SECRET;
  const CRON_SECRET         = process.env.CRON_SECRET;
  const BOT_TOKEN           = process.env.DISCORD_BOT_TOKEN;

  const sensitiveValues = [INTERNAL_API_SECRET, CRON_SECRET, BOT_TOKEN].filter(Boolean);

  const mock = makeSupabaseMock({
    send_queue:             { rows: [{ queue_status: "queued" }] },
    message_events:         { rows: [], count: 0 },
    sms_templates:          { rows: [], count: 0 },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interactions = [
      makeSlashInteraction({ command: "queue",     subcommand: "status" }),
      makeSlashInteraction({ command: "mission",   subcommand: "status", role_ids: ["owner_role"] }),
      makeSlashInteraction({ command: "templates", subcommand: "audit",  role_ids: ["owner_role"] }),
    ];

    for (const interaction of interactions) {
      const response = await routeDiscordInteraction(interaction);
      const serialised = JSON.stringify(response);
      for (const secret of sensitiveValues) {
        assert.ok(!serialised.includes(secret),
          `Response for /${interaction.data.name} must not include secret value`);
      }
    }
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 10. /launch preflight returns a cinematic embed
// ---------------------------------------------------------------------------

test("/launch preflight returns type 4 with a preflight embed", async () => {
  const mock = makeSupabaseMock({
    sms_templates:          { rows: [{ id: 1, is_active: true }], count: 1 },
    send_queue:             { rows: [], count: 0 },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "launch",
      subcommand: "preflight",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");
    const embed = response.data.embeds[0];
    assert.ok(
      embed.title?.toLowerCase().includes("preflight") ||
      embed.title?.toLowerCase().includes("launch"),
      "embed title references preflight or launch"
    );
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 11. /hotleads returns embed with lead data
// ---------------------------------------------------------------------------

test("/hotleads returns a cinematic response with hot lead embed", async () => {
  const mockEvents = [
    { id: "evt1", phone: "+15551234567", direction: "inbound", body: "Yes I am interested", created_at: new Date().toISOString(), podio_sync_status: "synced" },
    { id: "evt2", phone: "+15559876543", direction: "inbound", body: "Call me back please",  created_at: new Date().toISOString(), podio_sync_status: null },
  ];

  const mock = makeSupabaseMock({
    message_events:         { rows: mockEvents, count: mockEvents.length },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:  "hotleads",
      options:  [{ name: "limit", value: 5 }],
      role_ids: ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 12. /queue cockpit returns queue summary embed
// ---------------------------------------------------------------------------

test("/queue cockpit returns a queue cockpit embed", async () => {
  const mock = makeSupabaseMock({
    send_queue:             { rows: [
      { queue_status: "queued" },
      { queue_status: "queued" },
      { queue_status: "sent" },
      { queue_status: "failed" },
    ], count: 4 },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "queue",
      subcommand: "cockpit",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0, "has embed");
    const embed = response.data.embeds[0];
    assert.ok(embed.title?.toLowerCase().includes("cockpit"), "title references cockpit");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 13. Template audit paginates across multiple pages
// ---------------------------------------------------------------------------

test("/templates audit paginates and reports full 2500-row inventory", async () => {
  // Simulate 3 pages: 1000 + 1000 + 500 = 2500 rows total
  const page1 = Array.from({ length: 1000 }, (_, i) => ({
    id: i + 1, is_active: true, language: "en",
    use_case: "ownership_check", stage_code: "S1",
    is_first_touch: true, template_body: "Hello",
  }));
  const page2 = Array.from({ length: 1000 }, (_, i) => ({
    id: i + 1001, is_active: true, language: "es",
    use_case: "follow_up", stage_code: "S2",
    is_first_touch: false, template_body: "Hola",
  }));
  const page3 = Array.from({ length: 500 }, (_, i) => ({
    id: i + 2001, is_active: false, language: "en",
    use_case: "offer_reveal_cash", stage_code: "S3",
    is_first_touch: false, template_body: "",
  }));

  let page_call = 0;
  const mock = {
    from(table) {
      const chain = {
        select:      () => chain,
        eq:          () => chain,
        or:          () => chain,
        limit:       () => chain,
        order:       () => chain,
        range(from, to) {
          // Return next page each call
          const pages = [page1, page2, page3];
          const p = page_call++;
          chain._page_data = pages[p] ?? [];
          return chain;
        },
        insert:      () => Promise.resolve({ error: null }),
        upsert:      () => Promise.resolve({ error: null }),
        update:      () => chain,
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        then(resolve, reject) {
          return Promise.resolve({ data: chain._page_data ?? [], error: null }).then(resolve, reject);
        },
        _page_data: [],
      };
      return chain;
    },
  };
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "templates",
      subcommand: "audit",
      role_ids:   ["owner_role"],
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got a response");
    assert.equal(response.type, 4, "type 4 synchronous response");
    assert.ok(response.data?.embeds?.length > 0, "has embed");

    const embed = response.data.embeds[0];
    const inventoryField = embed.fields.find(f => f.name?.includes("Inventory"));
    assert.ok(inventoryField, "has Inventory field");
    // The inventory count should reflect all 2500 rows
    assert.ok(inventoryField.value.includes("2500") || inventoryField.value.includes("2,500"),
      "inventory should show full total of 2500 rows across all pages");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 14. /email cockpit is deferred (type 5)
// ---------------------------------------------------------------------------

test("/email cockpit returns deferred response type 5", async () => {
  const mock = makeSupabaseMock({
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "email",
      subcommand: "cockpit",
      role_ids:   ["owner_role"],
      token:      "email_cockpit_token",
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got a response");
    assert.equal(response.type, 5, "type 5 = deferred (email cockpit must defer immediately)");
    assert.ok(response.data?.flags & 64, "deferred response is ephemeral");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 15. /replay inbound is deferred (type 5)
// ---------------------------------------------------------------------------

test("/replay inbound returns deferred response type 5", async () => {
  const mock = makeSupabaseMock({
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "replay",
      subcommand: "inbound",
      options:    [{ name: "text", value: "What is your offer?" }],
      role_ids:   ["owner_role"],
      token:      "replay_inbound_token",
    });

    const response = await routeDiscordInteraction(interaction);

    assert.ok(response, "got a response");
    assert.equal(response.type, 5, "type 5 = deferred (replay inbound must defer immediately)");
    assert.ok(response.data?.flags & 64, "deferred response is ephemeral");
  } finally {
    __resetActionRouterDeps();
  }
});

// ---------------------------------------------------------------------------
// 16. Deferred handlers always call editInteractionResponse (never silent-fail)
// ---------------------------------------------------------------------------

test("/feeder scan deferred handler calls editInteractionResponse on callInternal failure", async () => {
  let edit_called = false;

  const mock = makeSupabaseMock({
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({
    supabase_override: mock,
    callInternal_override: async () => ({ ok: false, error: "HTTP 400" }),
    editInteractionResponse_override: async (opts) => {
      edit_called = true;
      assert.ok(opts.content?.includes("400") || opts.content?.includes("error"),
        "edit should surface the error message");
    },
  });

  try {
    const interaction = makeSlashInteraction({
      command:    "feeder",
      subcommand: "scan",
      options:    [{ name: "limit", value: 10 }],
      role_ids:   ["owner_role"],
      token:      "feeder_fail_token",
    });

    await routeDiscordInteraction(interaction);

    // Wait for the floating Promise to settle
    await new Promise(r => setTimeout(r, 50));
    assert.ok(edit_called, "editInteractionResponse must be called even on callInternal failure");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/replay inbound deferred handler calls editInteractionResponse on callInternal failure", async () => {
  let edit_called = false;

  const mock = makeSupabaseMock({
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({
    supabase_override: mock,
    callInternal_override: async () => ({ ok: false, status: 401, error: "HTTP 401" }),
    editInteractionResponse_override: async (opts) => {
      edit_called = true;
      // Should not expose the raw HTTP status as a confusing error
      const full = JSON.stringify(opts);
      assert.ok(!full.includes("undefined"), "content must not contain 'undefined'");
    },
  });

  try {
    const interaction = makeSlashInteraction({
      command:    "replay",
      subcommand: "inbound",
      options:    [{ name: "text", value: "Test message" }],
      role_ids:   ["owner_role"],
      token:      "replay_fail_token",
    });

    await routeDiscordInteraction(interaction);
    await new Promise(r => setTimeout(r, 50));
    assert.ok(edit_called, "editInteractionResponse must be called on 401 from replay-inbound route");
  } finally {
    __resetActionRouterDeps();
  }
});


// ---------------------------------------------------------------------------
// New tests: fixes for routing, buttons, alerts mode, channel router
// ---------------------------------------------------------------------------

test("/templates stage1 counts active first-touch and ownership templates", async () => {
  const mockRows = [
    { id: 1, is_active: true,  language: "en", use_case: "ownership_check", stage_code: "S1", is_first_touch: true,  template_body: "Hi" },
    { id: 2, is_active: true,  language: "es", use_case: "ownership_check", stage_code: "S1", is_first_touch: false, template_body: "Hola" },
    { id: 3, is_active: true,  language: "en", use_case: "follow_up",       stage_code: "S2", is_first_touch: false, template_body: "Follow up" },
    { id: 4, is_active: false, language: "en", use_case: "ownership_check", stage_code: "S1", is_first_touch: true,  template_body: "Inactive" },
  ];
  const mock = makeSupabaseMock({
    sms_templates: { rows: mockRows, count: mockRows.length },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "templates",
      subcommand: "stage1",
      role_ids:   ["owner_role"],
    });
    const response = await routeDiscordInteraction(interaction);
    assert.ok(response, "got a response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0 || response.data?.content, "has content or embeds");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/alerts mode without argument returns mode selector, not error", async () => {
  const db = makeSupabaseMock({
    app_config: { rows: [{ key: "alert_mode", value: "normal" }] },
  });
  __setActionRouterDeps({ supabase_override: db });

  try {
    const interaction = makeSlashInteraction({
      command:    "alerts",
      subcommand: "mode",
      options:    [],
      role_ids:   ["owner_role"],
    });
    const result = await routeDiscordInteraction(interaction);
    const content = result?.data?.content ?? "";
    assert.ok(!content.toLowerCase().includes("mode is required"), "/alerts mode with no arg must not return \'mode is required\'");
    const embeds = result?.data?.embeds ?? [];
    assert.ok(embeds.length > 0, "must return an embed for mode selector");
    const components = result?.data?.components ?? [];
    assert.ok(components.length > 0, "must return buttons for mode selector");
    const button_ids = components.flatMap(r => r.components ?? []).map(b => b.custom_id);
    assert.ok(button_ids.some(id => id === "alerts:mode:normal"),  "must include alerts:mode:normal button");
    assert.ok(button_ids.some(id => id === "alerts:mode:quiet"),   "must include alerts:mode:quiet button");
    assert.ok(button_ids.some(id => id === "alerts:mode:verbose"), "must include alerts:mode:verbose button");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing today returns deferred response (type 5)", async () => {
  __setActionRouterDeps({ editInteractionResponse_override: async () => {} });
  try {
    const interaction = {
      id: "brief_interaction", type: 2, token: "brief_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { name: "briefing", options: [{ type: 1, name: "today", options: [] }] },
    };
    const result = await routeDiscordInteraction(interaction);
    assert.equal(result?.type, 5, "/briefing today must return deferred response (type 5)");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing yesterday returns deferred response (type 5)", async () => {
  __setActionRouterDeps({ editInteractionResponse_override: async () => {} });
  try {
    const interaction = {
      id: "brief_y", type: 2, token: "brief_token_y", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { name: "briefing", options: [{ type: 1, name: "yesterday", options: [] }] },
    };
    const result = await routeDiscordInteraction(interaction);
    assert.equal(result?.type, 5, "/briefing yesterday must return deferred response (type 5)");
  } finally {
    __resetActionRouterDeps();
  }
});

for (const action of ["approve_launch", "approve", "launch"]) {
  test(`campaign:${action} button sets campaign target active`, async () => {
    const campaign_key = "dallas_sfr_absentee";
    const db = makeSupabaseMock({ campaign_targets: { rows: [{ campaign_key }] } });
    __setActionRouterDeps({ supabase_override: db });
    try {
      const interaction = {
        id: `btn_${action}`, type: 3, token: "btn_token", guild_id: "guild_5678",
        member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
        data: { custom_id: `campaign:${action}:${campaign_key}` },
      };
      const result = await routeDiscordInteraction(interaction);
      const content = String(result?.data?.content ?? "");
      assert.ok(!content.toLowerCase().includes("unsupported interaction"), "must not return Unsupported interaction");
      assert.ok(!content.toLowerCase().includes("unknown interaction type"), "must not return unknown interaction type");

      const [update] = db.getUpdates("campaign_targets");
      assert.equal(update?.values?.status, "active", "campaign_targets status must be set active");
      assert.deepEqual(update?.filters, [
        { op: "eq", column: "campaign_key", value: campaign_key },
      ]);
    } finally {
      __resetActionRouterDeps();
    }
  });
}

test("campaign:close button sets campaign target closed", async () => {
  const campaign_key = "dallas_sfr_absentee";
  const db = makeSupabaseMock({ campaign_targets: { rows: [{ campaign_key }] } });
  __setActionRouterDeps({ supabase_override: db });
  try {
    const interaction = {
      id: "btn_close", type: 3, token: "btn_close_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { custom_id: `campaign:close:${campaign_key}` },
    };
    const result = await routeDiscordInteraction(interaction);
    const content = String(result?.data?.content ?? "");
    assert.ok(!content.toLowerCase().includes("unsupported interaction"), "must not return Unsupported interaction");

    const [update] = db.getUpdates("campaign_targets");
    assert.equal(update?.values?.status, "closed", "campaign_targets status must be set closed");
    assert.deepEqual(update?.filters, [
      { op: "eq", column: "campaign_key", value: campaign_key },
    ]);
  } finally {
    __resetActionRouterDeps();
  }
});

test("unknown campaign button action returns structured ephemeral diagnostic", async () => {
  const db = makeSupabaseMock({ discord_command_events: {} });
  __setActionRouterDeps({ supabase_override: db });
  try {
    const interaction = {
      id: "unknown_campaign_btn", type: 3, token: "unknown_campaign_btn_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { custom_id: "campaign:warp:dallas_sfr_absentee" },
    };
    const result = await routeDiscordInteraction(interaction);
    const content = String(result?.data?.content ?? "");
    assert.ok(content.includes("Unsupported campaign action"), "unknown campaign button must return a campaign diagnostic");
    assert.ok(content.includes("campaign:warp:dallas_sfr_absentee"), "must include the received custom_id");
    const flags = result?.data?.flags ?? 0;
    assert.ok(flags & 64, "unknown campaign button response must be ephemeral");
  } finally {
    __resetActionRouterDeps();
  }
});

function componentIdsFromPayload(payload = {}) {
  return (payload.components ?? [])
    .flatMap((row) => row.components ?? [])
    .map((component) => component.custom_id)
    .filter(Boolean);
}

test("feeder auto scan ranks best offset correctly", async () => {
  let edited_payload = null;
  const results_by_offset = new Map([
    [0,   { ok: true, queued_count: 20, duplicate_queue_block_count: 1 }],
    [100, { ok: true, queued_count: 50, duplicate_queue_block_count: 10 }],
    [200, { ok: true, queued_count: 50, duplicate_queue_block_count: 0 }],
    [350, { ok: true, queued_count: 30, duplicate_queue_block_count: 0 }],
  ]);

  __setActionRouterDeps({
    supabase_override: makeSupabaseMock({ discord_command_events: { rows: [] } }),
    callInternal_override: async (path, options) => {
      assert.equal(path, "/api/internal/outbound/feed-candidates");
      assert.equal(options.body.dry_run, true, "auto scan must always dry-run");
      const offset = options.body.candidate_offset;
      return {
        ok: true,
        data: {
          ok: true,
          inserted_count: 0,
          queued_count: 0,
          skipped_count: 0,
          ...(results_by_offset.get(offset) ?? {}),
        },
      };
    },
    editInteractionResponse_override: async (payload) => {
      edited_payload = payload;
    },
  });

  try {
    const response = await routeDiscordInteraction(makeComponentInteraction({ custom_id: "feeder:auto_scan" }));
    assert.equal(response.type, 5, "auto scan is deferred");
    await new Promise((resolve) => setTimeout(resolve, 80));

    const launch_id = componentIdsFromPayload(edited_payload).find((id) => id.startsWith("feeder:launch:"));
    assert.ok(launch_id, "auto scan should return a live launch button for the best band");
    const decoded = decodeFeederPayloadFromCustomId(launch_id.slice("feeder:launch:".length));
    assert.equal(decoded.candidate_offset, 200, "tie should pick lower duplicate count");
  } finally {
    __resetActionRouterDeps();
  }
});

test("feeder auto scan ignores ok=false and queued_count=0 bands", async () => {
  let edited_payload = null;

  __setActionRouterDeps({
    supabase_override: makeSupabaseMock({ discord_command_events: { rows: [] } }),
    callInternal_override: async (path, options) => {
      const offset = options.body.candidate_offset;
      if (offset === 0) return { ok: false, error: "HTTP 500" };
      if (offset === 100) return { ok: true, data: { ok: true, queued_count: 0 } };
      if (offset === 200) return { ok: true, data: { ok: true, queued_count: 7 } };
      return { ok: true, data: { ok: true, queued_count: 0 } };
    },
    editInteractionResponse_override: async (payload) => {
      edited_payload = payload;
    },
  });

  try {
    await routeDiscordInteraction(makeComponentInteraction({ custom_id: "feeder:auto_scan" }));
    await new Promise((resolve) => setTimeout(resolve, 80));

    const launch_id = componentIdsFromPayload(edited_payload).find((id) => id.startsWith("feeder:launch:"));
    assert.ok(launch_id, "should still expose launch for the valid non-zero band");
    const decoded = decodeFeederPayloadFromCustomId(launch_id.slice("feeder:launch:".length));
    assert.equal(decoded.candidate_offset, 200);
  } finally {
    __resetActionRouterDeps();
  }
});

test("feeder launch button converts dry_run to false and preserves params", async () => {
  let captured_body = null;
  let edited_payload = null;
  const payload = encodeFeederPayloadForCustomId({
    candidate_offset: 350,
    limit: 60,
    scan_limit: 500,
    schedule_start_local: "13:00",
    schedule_end_local: "18:00",
    schedule_interval_seconds_min: 300,
  });

  __setActionRouterDeps({
    supabase_override: makeSupabaseMock({ discord_command_events: { rows: [] } }),
    callInternal_override: async (path, options) => {
      captured_body = options.body;
      return {
        ok: true,
        data: {
          ok: true,
          queued_count: 12,
          inserted_count: 12,
          first_scheduled_for: "2026-04-27T18:00:00.000Z",
          last_scheduled_for: "2026-04-27T18:33:00.000Z",
          selected_textgrid_market_counts: { houston: 12 },
          routing_tier_counts: { exact_market_match: 12 },
          sample_skips: [{ reason_code: "DUPLICATE_QUEUE_ITEM" }],
        },
      };
    },
    editInteractionResponse_override: async (payload) => {
      edited_payload = payload;
    },
  });

  try {
    const response = await routeDiscordInteraction(makeComponentInteraction({ custom_id: `feeder:launch:${payload}` }));
    assert.equal(response.type, 5, "live launch is deferred");
    await new Promise((resolve) => setTimeout(resolve, 80));

    assert.equal(captured_body.dry_run, false);
    assert.equal(captured_body.candidate_offset, 350);
    assert.equal(captured_body.limit, 60);
    assert.equal(captured_body.scan_limit, 500);
    assert.equal(captured_body.schedule_start_local, "13:00");
    assert.equal(captured_body.schedule_end_local, "18:00");
    assert.equal(captured_body.schedule_interval_seconds_min, 300);
    assert.equal(captured_body.candidate_source, "v_sms_ready_contacts");
    assert.equal(captured_body.routing_safe_only, true);
    assert.ok(componentIdsFromPayload(edited_payload).includes("feeder:queue_status"), "result includes Queue Status button");
  } finally {
    __resetActionRouterDeps();
  }
});

test("feeder payload decoding cannot inject arbitrary URL params", async () => {
  let captured_body = null;
  const malicious_payload = Buffer.from(JSON.stringify({
    o: 100,
    l: 75,
    n: 250,
    s: "12:30",
    e: "20:00",
    i: 180,
    dry_run: true,
    candidate_source: "evil_view",
    x_internal_api_secret: process.env.INTERNAL_API_SECRET,
  }), "utf8").toString("base64url");

  __setActionRouterDeps({
    supabase_override: makeSupabaseMock({ discord_command_events: { rows: [] } }),
    callInternal_override: async (path, options) => {
      captured_body = options.body;
      return { ok: true, data: { ok: true, queued_count: 1, inserted_count: 1 } };
    },
    editInteractionResponse_override: async () => {},
  });

  try {
    await routeDiscordInteraction(makeComponentInteraction({ custom_id: `feeder:launch:${malicious_payload}` }));
    await new Promise((resolve) => setTimeout(resolve, 80));

    assert.equal(captured_body.dry_run, false, "button handler owns dry_run mode");
    assert.equal(captured_body.candidate_source, "v_sms_ready_contacts", "source is fixed by the cockpit");
    assert.equal(captured_body.candidate_offset, 100);
    assert.ok(!Object.hasOwn(captured_body, "x_internal_api_secret"), "unknown payload keys are dropped");
  } finally {
    __resetActionRouterDeps();
  }
});

test("feeder cockpit responses do not expose INTERNAL_API_SECRET or CRON_SECRET", async () => {
  let edited_payload = null;
  __setActionRouterDeps({
    supabase_override: makeSupabaseMock({ discord_command_events: { rows: [] } }),
    callInternal_override: async () => ({
      ok: false,
      error: `boom ${process.env.INTERNAL_API_SECRET} ${process.env.CRON_SECRET}`,
    }),
    editInteractionResponse_override: async (payload) => {
      edited_payload = payload;
    },
  });

  try {
    await routeDiscordInteraction(makeComponentInteraction({ custom_id: "feeder:auto_scan" }));
    await new Promise((resolve) => setTimeout(resolve, 80));
    const full = JSON.stringify(edited_payload);
    assert.ok(!full.includes(process.env.INTERNAL_API_SECRET), "must not expose internal secret");
    assert.ok(!full.includes(process.env.CRON_SECRET), "must not expose cron secret");
  } finally {
    __resetActionRouterDeps();
  }
});

test("channel router resolves event types to channel keys", async () => {
  const { resolveChannelForEvent, getChannelKeyForEvent } = await import(
    "@/lib/discord/discord-channel-router.js"
  );
  assert.equal(getChannelKeyForEvent("inbound_sms_reply"), "inbound_replies");
  assert.equal(getChannelKeyForEvent("campaign_create"),   "mission_control");
  assert.equal(getChannelKeyForEvent("hot_lead"),          "hot_leads");
  assert.equal(getChannelKeyForEvent("feeder_run"),        "feeder_runs");
  assert.equal(getChannelKeyForEvent("opt_out"),           "opt_outs");
  assert.equal(getChannelKeyForEvent("debug_log"),         "debug_logs");
  assert.equal(getChannelKeyForEvent("unknown_xyz"),       null);

  const result = resolveChannelForEvent("inbound_sms_reply", { env: {} });
  assert.ok(result, "resolveChannelForEvent must return a result object");
  assert.equal(result.fallback, true, "must indicate fallback when no env var set");
});


// ---------------------------------------------------------------------------
// New tests: routing fixes, button handlers, alerts mode, channel router
// ---------------------------------------------------------------------------

test("/templates stage1 counts active first-touch and ownership templates", async () => {
  const mockRows = [
    { id: 1, is_active: true,  language: "en", use_case: "ownership_check", stage_code: "S1", is_first_touch: true,  template_body: "Hi" },
    { id: 2, is_active: true,  language: "es", use_case: "ownership_check", stage_code: "S1", is_first_touch: false, template_body: "Hola" },
    { id: 3, is_active: true,  language: "en", use_case: "follow_up",       stage_code: "S2", is_first_touch: false, template_body: "Follow up" },
    { id: 4, is_active: false, language: "en", use_case: "ownership_check", stage_code: "S1", is_first_touch: true,  template_body: "Inactive" },
  ];
  const mock = makeSupabaseMock({
    sms_templates: { rows: mockRows, count: mockRows.length },
    discord_command_events: { rows: [] },
  });
  __setActionRouterDeps({ supabase_override: mock });

  try {
    const interaction = makeSlashInteraction({
      command:    "templates",
      subcommand: "stage1",
      role_ids:   ["owner_role"],
    });
    const response = await routeDiscordInteraction(interaction);
    assert.ok(response, "got a response");
    assert.equal(response.type, 4, "type 4");
    assert.ok(response.data?.embeds?.length > 0 || response.data?.content, "has content or embeds");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/alerts mode without argument returns mode selector, not error", async () => {
  const db = makeSupabaseMock({
    app_config: { rows: [{ key: "alert_mode", value: "normal" }] },
  });
  __setActionRouterDeps({ supabase_override: db });

  try {
    const interaction = makeSlashInteraction({
      command:    "alerts",
      subcommand: "mode",
      options:    [],
      role_ids:   ["owner_role"],
    });
    const result = await routeDiscordInteraction(interaction);
    const content = result?.data?.content ?? "";
    assert.ok(!content.toLowerCase().includes("mode is required"), "must not return mode is required");
    const embeds = result?.data?.embeds ?? [];
    assert.ok(embeds.length > 0, "must return an embed for mode selector");
    const components = result?.data?.components ?? [];
    assert.ok(components.length > 0, "must return buttons for mode selector");
    const button_ids = components.flatMap(r => r.components ?? []).map(b => b.custom_id);
    assert.ok(button_ids.some(id => id === "alerts:mode:normal"),  "must include alerts:mode:normal button");
    assert.ok(button_ids.some(id => id === "alerts:mode:quiet"),   "must include alerts:mode:quiet button");
    assert.ok(button_ids.some(id => id === "alerts:mode:verbose"), "must include alerts:mode:verbose button");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing today returns deferred response (type 5)", async () => {
  __setActionRouterDeps({ editInteractionResponse_override: async () => {} });
  try {
    const interaction = {
      id: "brief_interaction", type: 2, token: "brief_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { name: "briefing", options: [{ type: 1, name: "today", options: [] }] },
    };
    const result = await routeDiscordInteraction(interaction);
    assert.equal(result?.type, 5, "/briefing today must return deferred response (type 5)");
  } finally {
    __resetActionRouterDeps();
  }
});

test("/briefing yesterday returns deferred response (type 5)", async () => {
  __setActionRouterDeps({ editInteractionResponse_override: async () => {} });
  try {
    const interaction = {
      id: "brief_y", type: 2, token: "brief_token_y", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { name: "briefing", options: [{ type: 1, name: "yesterday", options: [] }] },
    };
    const result = await routeDiscordInteraction(interaction);
    assert.equal(result?.type, 5, "/briefing yesterday must return deferred response (type 5)");
  } finally {
    __resetActionRouterDeps();
  }
});

test("campaign:approve_launch button routes without Unknown interaction", async () => {
  const db = makeSupabaseMock({ campaign_targets: { rows: [{ campaign_key: "dallas_sfr_absentee" }] } });
  __setActionRouterDeps({ supabase_override: db });
  try {
    const interaction = {
      id: "btn_al", type: 3, token: "btn_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { custom_id: "campaign:approve_launch:dallas_sfr_absentee" },
    };
    const result = await routeDiscordInteraction(interaction);
    const content = String(result?.data?.content ?? "");
    assert.ok(!content.toLowerCase().includes("unsupported interaction"), "must not return Unsupported interaction");
  } finally {
    __resetActionRouterDeps();
  }
});

test("campaign:close button routes without Unknown interaction", async () => {
  const db = makeSupabaseMock({ campaign_targets: { rows: [{ campaign_key: "dallas_sfr_absentee" }] } });
  __setActionRouterDeps({ supabase_override: db });
  try {
    const interaction = {
      id: "btn_close", type: 3, token: "btn_close_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { custom_id: "campaign:close:dallas_sfr_absentee" },
    };
    const result = await routeDiscordInteraction(interaction);
    const content = String(result?.data?.content ?? "");
    assert.ok(!content.toLowerCase().includes("unsupported interaction"), "must not return Unsupported interaction");
  } finally {
    __resetActionRouterDeps();
  }
});

test("unknown button custom_id returns structured ephemeral diagnostic", async () => {
  const db = makeSupabaseMock({ discord_command_events: {} });
  __setActionRouterDeps({ supabase_override: db });
  try {
    const interaction = {
      id: "unknown_btn", type: 3, token: "unknown_btn_token", guild_id: "guild_5678",
      member: { user: { id: "user_1234", username: "TestUser" }, roles: ["owner_role"] },
      data: { custom_id: "totally:unknown:custom_id_xyz" },
    };
    const result = await routeDiscordInteraction(interaction);
    const content = String(result?.data?.content ?? "");
    assert.ok(content.includes("Unsupported interaction"), "unknown button must return Unsupported interaction: <custom_id>");
    assert.ok(content.includes("totally:unknown:custom_id_xyz"), "must include the received custom_id");
    const flags = result?.data?.flags ?? 0;
    assert.ok(flags & 64, "unknown button response must be ephemeral");
  } finally {
    __resetActionRouterDeps();
  }
});

test("channel router resolves event types to channel keys", async () => {
  const { resolveChannelForEvent, getChannelKeyForEvent } = await import(
    "@/lib/discord/discord-channel-router.js"
  );
  assert.equal(getChannelKeyForEvent("inbound_sms_reply"), "inbound_replies");
  assert.equal(getChannelKeyForEvent("campaign_create"),   "mission_control");
  assert.equal(getChannelKeyForEvent("hot_lead"),          "hot_leads");
  assert.equal(getChannelKeyForEvent("feeder_run"),        "feeder_runs");
  assert.equal(getChannelKeyForEvent("opt_out"),           "opt_outs");
  assert.equal(getChannelKeyForEvent("debug_log"),         "debug_logs");
  assert.equal(getChannelKeyForEvent("unknown_xyz"),       null);

  const result = resolveChannelForEvent("inbound_sms_reply", { env: {} });
  assert.ok(result, "resolveChannelForEvent must return a result object");
  assert.equal(result.fallback, true, "must indicate fallback when no env var set");
});
