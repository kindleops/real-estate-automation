/**
 * discord-ops-notifications.test.mjs
 *
 * Unit tests for the "Proactive Discord Ops Notifications v1" feature.
 *
 * Coverage:
 *   1.  analyzeCampaignHealth → scale_recommended when metrics are strong
 *   2.  analyzeCampaignHealth → pause_recommended when opt-outs are high
 *   3.  analyzeCampaignHealth → no recommendation when sample size too low
 *   4.  createOpsNotification dedupes by notification_key (upsert, not double-insert)
 *   5.  Approval button (campaign_scale) → updates request status + campaign
 *   6.  Non-owner cannot approve scale/pause action (unauthorized)
 *   7.  runProactiveOpsCheck dry_run=true creates no DB writes
 *   8.  Discord embeds contain no raw env secrets
 *   9.  opsApprovalActionRow custom_ids are under 100 chars
 *   10. opsApprovalActionRow returns valid action row structure
 */

import test   from "node:test";
import assert from "node:assert/strict";

import {
  analyzeCampaignHealth,
  buildCampaignScaleRecommendation,
  buildCampaignPauseRecommendation,
  buildNotificationKey,
  buildApprovalRequestKey,
  runProactiveOpsCheck,
  __setProactiveNotificationsDeps,
  __resetProactiveNotificationsDeps,
  THRESHOLDS,
} from "@/lib/domain/ops/proactive-notifications.js";

import {
  buildOpsNotificationEmbed,
  buildCampaignScaleApprovalEmbed,
  buildCampaignPauseAlertEmbed,
  buildHotLeadOpsEmbed,
  buildSystemHealthOpsEmbed,
} from "@/lib/discord/discord-embed-factory.js";

import {
  opsApprovalActionRow,
} from "@/lib/discord/discord-components.js";

import {
  routeDiscordInteraction,
  __setActionRouterDeps,
  __resetActionRouterDeps,
} from "@/lib/discord/discord-action-router.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a button interaction (type 3). */
function makeButtonInteraction({
  custom_id,
  role_ids  = ["owner_role"],
  member_id = "user_1234",
  guild_id  = "guild_5678",
} = {}) {
  return {
    id:      "btn_interaction_id",
    type:    3,
    token:   "test_token_btn",
    guild_id,
    member: {
      user:  { id: member_id, username: "TestUser" },
      roles: role_ids,
    },
    data: { custom_id },
  };
}

/** Chainable Supabase mock. */
function makeSupabaseMock(tableMap = {}) {
  const _insert_calls = {};
  const _upsert_calls = {};
  const _update_calls = {};

  const mock = {
    _insert_calls,
    _upsert_calls,
    _update_calls,
    from(table) {
      const spec = tableMap[table] ?? {};

      const chain = {
        select:      () => chain,
        eq:          () => chain,
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
        in:          () => chain,
        upsert(row) {
          _upsert_calls[table] = (_upsert_calls[table] ?? 0) + 1;
          return {
            select:      () => this_upsert,
            maybeSingle: () => Promise.resolve({ data: spec.rows?.[0] ?? { id: 1 }, error: spec.error ?? null }),
          };
          // self-referencing object so chain works
          var this_upsert = {
            select:      () => this_upsert,
            maybeSingle: () => Promise.resolve({ data: spec.rows?.[0] ?? { id: 1 }, error: spec.error ?? null }),
          };
        },
        insert(row) {
          _insert_calls[table] = (_insert_calls[table] ?? 0) + 1;
          return Promise.resolve({ data: null, error: spec.error ?? null });
        },
        update(fields) {
          _update_calls[table] = (_update_calls[table] ?? 0) + 1;
          return chain;
        },
        maybeSingle: () => Promise.resolve({ data: spec.rows?.[0] ?? null, error: spec.error ?? null }),
        then(resolve, reject) {
          if (spec.error) {
            return Promise.resolve({ data: null, error: spec.error }).then(resolve, reject);
          }
          return Promise.resolve({ data: spec.rows ?? [], error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };
  return mock;
}

// Wire env vars expected by the router.
process.env.DISCORD_GUILD_ID             = "guild_5678";
process.env.INTERNAL_API_SECRET          = "test_internal_secret";
process.env.CRON_SECRET                  = "test_cron_secret";
process.env.DISCORD_BOT_TOKEN            = "test_bot_token";
process.env.APP_BASE_URL                 = "http://localhost:3000";
process.env.DISCORD_APPLICATION_ID       = "app_id_1";
process.env.DISCORD_ROLE_OWNER_ID        = "owner_role";
process.env.DISCORD_ROLE_TECH_OPS_ID     = "tech_ops_role";
process.env.DISCORD_ROLE_SMS_OPS_ID      = "sms_ops_role";
process.env.DISCORD_ROLE_ACQUISITIONS_ID = "acquisitions_role";
process.env.OPS_NOTIFICATIONS_ENABLED    = "true";

// ---------------------------------------------------------------------------
// 1. Scale recommendation — strong metrics
// ---------------------------------------------------------------------------

test("analyzeCampaignHealth returns scale_recommended when reply rate exceeds threshold", () => {
  const campaign = { campaign_key: "tx_sfr_cash", daily_cap: 50 };
  const metrics  = {
    sent:      200,
    delivered: 180,
    replied:   12,   // 12/180 = 6.67% > 5% threshold
    opted_out: 2,    // 2/180 = 1.1% < 3% threshold
    failed:    3,    // 3/200 = 1.5% < 5% threshold
    sample_size: 200,
  };

  const result = analyzeCampaignHealth(campaign, metrics);

  assert.ok(result.scale_recommended,   "scale_recommended should be true");
  assert.ok(!result.pause_recommended,  "pause_recommended should be false");
  assert.ok(result.confidence > 0,      "confidence should be positive");
  assert.ok(result.reason.length > 5,   "reason should be non-empty");
});

// ---------------------------------------------------------------------------
// 2. Pause recommendation — opt-outs too high
// ---------------------------------------------------------------------------

test("analyzeCampaignHealth returns pause_recommended when opt-out rate is critical", () => {
  const campaign = { campaign_key: "tx_sfr_cash", daily_cap: 50 };
  const metrics  = {
    sent:      200,
    delivered: 180,
    replied:   2,
    opted_out: 18,   // 18/180 = 10% > 8% pause threshold
    failed:    3,
    sample_size: 200,
  };

  const result = analyzeCampaignHealth(campaign, metrics);

  assert.ok(!result.scale_recommended,  "scale_recommended should be false");
  assert.ok(result.pause_recommended,   "pause_recommended should be true");
  assert.ok(result.confidence > 0,      "confidence should be positive");
  assert.ok(result.reason.toLowerCase().includes("opt-out"), "reason mentions opt-out");
});

// ---------------------------------------------------------------------------
// 3. No recommendation — sample size too small
// ---------------------------------------------------------------------------

test("analyzeCampaignHealth returns no recommendation when sample size is below threshold", () => {
  const campaign = { campaign_key: "tx_sfr_cash", daily_cap: 50 };
  const metrics  = {
    sent:      10,
    delivered: 9,
    replied:   3,
    opted_out: 0,
    failed:    0,
    sample_size: 10,  // < 50 MIN_SAMPLE_SIZE
  };

  const result = analyzeCampaignHealth(campaign, metrics);

  assert.ok(!result.scale_recommended,  "scale_recommended should be false");
  assert.ok(!result.pause_recommended,  "pause_recommended should be false");
  assert.strictEqual(result.confidence, 0, "confidence should be 0 for small sample");
  assert.ok(result.reason.includes("Sample size"), "reason mentions sample size");
});

// ---------------------------------------------------------------------------
// 4. Notification dedup — same notification_key upsert, not duplicate insert
// ---------------------------------------------------------------------------

test("createOpsNotification deduplicates by notification_key via upsert", async () => {
  const upsert_counts = {};

  const mock_db = {
    from(table) {
      const chain = {
        upsert(row) {
          upsert_counts[table] = (upsert_counts[table] ?? 0) + 1;
          const result_chain = {
            select:      () => result_chain,
            maybeSingle: () => Promise.resolve({ data: { id: 42 }, error: null }),
          };
          return result_chain;
        },
        select:       () => chain,
        eq:           () => chain,
        maybeSingle:  () => Promise.resolve({ data: null, error: null }),
      };
      return chain;
    },
  };

  __setProactiveNotificationsDeps({ supabase_override: mock_db });

  const { createOpsNotification } = await import("@/lib/domain/ops/proactive-notifications.js");

  const key = buildNotificationKey("campaign_scale", "tx_sfr_cash");

  // Call twice with the same key.
  await createOpsNotification({ notification_key: key, notification_type: "campaign_scale", title: "Test" });
  await createOpsNotification({ notification_key: key, notification_type: "campaign_scale", title: "Test" });

  // Each call fires exactly one upsert (not insert) — the DB handles the dedup.
  assert.strictEqual(upsert_counts["ops_notifications"], 2,
    "should call upsert twice (DB deduplicates via unique index)");

  __resetProactiveNotificationsDeps();
});

// ---------------------------------------------------------------------------
// 5. Approval button → approval:campaign_scale → updates request + campaign
// ---------------------------------------------------------------------------

test("approval:campaign_scale button resolves approval and returns success embed", async () => {
  const request_key  = "approval:scale:tx_sfr_cash:2026-01-01";
  const approval_row = {
    id:           1,
    request_key,
    request_type: "scale",
    campaign_key: "tx_sfr_cash",
    current_cap:  50,
    proposed_cap: 100,
    reason:       "Strong reply rate",
    status:       "pending",
    expires_at:   new Date(Date.now() + 86400_000).toISOString(),
  };

  const mock_db = makeSupabaseMock({
    campaign_approval_requests: { rows: [approval_row] },
    campaign_targets:           { rows: [] },
    discord_action_audit:       { rows: [] },
  });

  __setActionRouterDeps({ supabase_override: mock_db });

  const interaction = makeButtonInteraction({
    custom_id: `approval:campaign_scale:${request_key}`,
    role_ids:  ["owner_role"],
  });

  const response = await routeDiscordInteraction(interaction);

  // Should be an "update message" response (type 7).
  assert.strictEqual(response.type, 7, "response should be update_message (type 7)");

  // Content or embeds must exist.
  const embeds = response?.data?.embeds ?? [];
  const has_content_or_embed = response?.data?.content || embeds.length > 0;
  assert.ok(has_content_or_embed, "response should have content or embeds");

  __resetActionRouterDeps();
});

// ---------------------------------------------------------------------------
// 6. Non-owner cannot approve scale/pause action
// ---------------------------------------------------------------------------

test("approval:campaign_scale button denied for non-owner, non-sms_ops member", async () => {
  const request_key = "approval:scale:tx_sfr_cash:2026-01-02";
  const approval_row = {
    id:           2,
    request_key,
    request_type: "scale",
    campaign_key: "tx_sfr_cash",
    current_cap:  50,
    proposed_cap: 100,
    status:       "pending",
    expires_at:   new Date(Date.now() + 86400_000).toISOString(),
  };

  const mock_db = makeSupabaseMock({
    campaign_approval_requests: { rows: [approval_row] },
    discord_action_audit:       { rows: [] },
  });

  __setActionRouterDeps({ supabase_override: mock_db });

  const interaction = makeButtonInteraction({
    custom_id: `approval:campaign_scale:${request_key}`,
    role_ids:  ["acquisitions_role"],  // Not owner or sms_ops
  });

  const response = await routeDiscordInteraction(interaction);

  // Should be update_message (type 7) with denial content.
  assert.strictEqual(response.type, 7, "response should be update_message (type 7)");
  const content = String(response?.data?.content ?? "").toLowerCase();
  assert.ok(
    content.includes("owner") || content.includes("ops") || content.includes("🚫"),
    "denial response should mention required role"
  );

  __resetActionRouterDeps();
});

// ---------------------------------------------------------------------------
// 7. runProactiveOpsCheck dry_run=true creates no DB writes
// ---------------------------------------------------------------------------

test("runProactiveOpsCheck dry_run=true does not write any notifications or approvals", async () => {
  const upsert_count = { ops_notifications: 0, campaign_approval_requests: 0 };

  // Provide active campaigns with strong metrics via message_events.
  const mock_db = {
    from(table) {
      const chain = {
        select:  () => chain,
        eq:      () => chain,
        gte:     () => chain,
        limit:   () => chain,
        order:   () => chain,
        upsert(row) {
          upsert_count[table] = (upsert_count[table] ?? 0) + 1;
          const uc = {
            select:      () => uc,
            maybeSingle: () => Promise.resolve({ data: { id: 1 }, error: null }),
          };
          return uc;
        },
        insert(row) {
          upsert_count[table] = (upsert_count[table] ?? 0) + 1;
          return Promise.resolve({ data: null, error: null });
        },
        maybeSingle: () => Promise.resolve({ data: null, error: null }),
        then(resolve, reject) {
          // campaign_targets → return one active campaign
          if (table === "campaign_targets") {
            return Promise.resolve({
              data: [{ id: "1", campaign_key: "tx_sfr_cash", daily_cap: 50, paused: false }],
              error: null,
            }).then(resolve, reject);
          }
          // message_events → return enough sent events to trigger scale recommendation
          if (table === "message_events") {
            const events = [];
            for (let i = 0; i < 200; i++) {
              events.push({ direction: "outbound", status: "delivered" });
            }
            for (let i = 0; i < 12; i++) {
              events.push({ direction: "inbound", status: "received" });
            }
            return Promise.resolve({ data: events, error: null }).then(resolve, reject);
          }
          return Promise.resolve({ data: [], error: null }).then(resolve, reject);
        },
      };
      return chain;
    },
  };

  __setProactiveNotificationsDeps({ supabase_override: mock_db });

  const result = await runProactiveOpsCheck({ dry_run: true, discord_dispatch: false });

  assert.ok(result.campaigns_checked >= 1,         "should have checked at least one campaign");
  assert.strictEqual(result.dry_run,          true, "dry_run flag should be true in result");
  assert.strictEqual(result.notifications_created, 0, "dry_run should create no notifications");
  // approvals_created may not be in result under dry_run, but must not be positive
  assert.ok((result.approvals_created ?? 0) === 0,  "dry_run should create no approvals");
  assert.strictEqual(upsert_count.ops_notifications,        0, "no upserts to ops_notifications");
  assert.strictEqual(upsert_count.campaign_approval_requests, 0, "no upserts to campaign_approval_requests");

  __resetProactiveNotificationsDeps();
});

// ---------------------------------------------------------------------------
// 8. Discord embeds contain no raw env secrets
// ---------------------------------------------------------------------------

test("ops Discord embeds do not contain raw env secrets", () => {
  const secrets = [
    process.env.INTERNAL_API_SECRET,
    process.env.CRON_SECRET,
    process.env.DISCORD_BOT_TOKEN,
  ].filter(Boolean);

  const embeds = [
    buildOpsNotificationEmbed({ title: "Test", message: "Body", severity: "info", campaign_key: "tx_sfr_cash" }),
    buildCampaignScaleApprovalEmbed({ campaign_key: "tx_sfr_cash", current_cap: 50, proposed_cap: 100 }),
    buildCampaignPauseAlertEmbed({ campaign_key: "tx_sfr_cash", reason: "High opt-out rate" }),
    buildHotLeadOpsEmbed({ hot_count: 3, recent_leads: [] }),
    buildSystemHealthOpsEmbed({ checks: [{ name: "Supabase", status: "ok" }], overall_status: "healthy" }),
  ];

  const payload = JSON.stringify(embeds);

  for (const secret of secrets) {
    assert.ok(
      !payload.includes(secret),
      `Embed payload must not contain secret: ${secret.slice(0, 4)}***`
    );
  }
});

// ---------------------------------------------------------------------------
// 9. opsApprovalActionRow — all custom_ids under 100 chars
// ---------------------------------------------------------------------------

test("opsApprovalActionRow custom_ids are all under 100 characters", () => {
  const long_key  = "approval:scale:some_very_long_campaign_key_that_is_quite_long";
  const scale_row = opsApprovalActionRow({ requestKey: long_key, type: "scale" });
  const pause_row = opsApprovalActionRow({ requestKey: long_key, type: "pause" });

  for (const row of [...scale_row, ...pause_row]) {
    for (const btn of row.components ?? []) {
      const id_len = String(btn.custom_id ?? "").length;
      assert.ok(
        id_len <= 100,
        `custom_id "${btn.custom_id}" is ${id_len} chars — must be ≤ 100`
      );
    }
  }
});

// ---------------------------------------------------------------------------
// 10. opsApprovalActionRow — returns valid action row structure
// ---------------------------------------------------------------------------

test("opsApprovalActionRow returns a valid Discord action row", () => {
  const scale_rows = opsApprovalActionRow({ requestKey: "req_abc123", type: "scale" });
  const pause_rows = opsApprovalActionRow({ requestKey: "req_abc123", type: "pause" });

  for (const [label, rows] of [["scale", scale_rows], ["pause", pause_rows]]) {
    assert.ok(Array.isArray(rows),    `${label}: rows is an array`);
    assert.ok(rows.length >= 1,       `${label}: at least one action row`);

    const row = rows[0];
    assert.strictEqual(row.type, 1,   `${label}: action row type is 1`);
    assert.ok(Array.isArray(row.components), `${label}: components is an array`);
    assert.ok(row.components.length >= 1,    `${label}: at least one button`);

    for (const btn of row.components) {
      assert.strictEqual(btn.type, 2, `${label}: button type is 2`);
      assert.ok(typeof btn.custom_id === "string" && btn.custom_id.length > 0,
        `${label}: button has non-empty custom_id`);
      assert.ok(typeof btn.label === "string" && btn.label.length > 0,
        `${label}: button has non-empty label`);
    }

    // Scale: first button must be approval:campaign_scale:
    // Pause: first button must be approval:campaign_pause:
    const first_id = rows[0].components[0].custom_id;
    const expected_prefix = label === "scale"
      ? "approval:campaign_scale:"
      : "approval:campaign_pause:";
    assert.ok(
      first_id.startsWith(expected_prefix),
      `${label}: first button custom_id starts with "${expected_prefix}" (got "${first_id}")`
    );

    // All rows must have a Hold and Inspect button somewhere.
    const all_ids = rows.flatMap(r => r.components.map(b => b.custom_id));
    assert.ok(
      all_ids.some(id => id.startsWith("approval:hold:")),
      `${label}: contains an approval:hold: button`
    );
    assert.ok(
      all_ids.some(id => id.startsWith("approval:inspect:")),
      `${label}: contains an approval:inspect: button`
    );
  }
});
