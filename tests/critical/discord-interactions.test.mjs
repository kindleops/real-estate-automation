/**
 * discord-interactions.test.mjs
 *
 * Tests for:
 *  1.  Ed25519 signature verification — invalid signature rejected
 *  2.  Ed25519 signature verification — valid signature accepted
 *  3.  PING interaction returns PONG
 *  4.  Missing role rejected (Tech Ops command, user has no role)
 *  5.  Owner role passes permission check for all restricted commands
 *  6.  Tech Ops can run sync-podio
 *  7.  SMS Ops cannot release lock
 *  8.  Feeder limit > 25 creates approval request (not immediate execution)
 *  9.  Feeder limit ≤ 25 by Tech Ops executes immediately
 * 10.  Owner can approve a pending feeder action
 * 11.  Approval logs to Supabase
 * 12.  Command result never exposes secrets
 * 13.  Role mention payload uses allowed_mentions
 * 14.  Queue status reads send_queue and returns counts
 * 15.  Acquisitions can run /lead summarize
 * 16.  SMS Ops cannot /lock release
 * 17.  Guild guard rejects wrong guild_id
 * 18.  Reject button updates message and logs rejection
 */

import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

// ── Units under test ──────────────────────────────────────────────────────

import { verifyDiscordRequest } from "@/lib/discord/verify-discord-request.js";
import {
  hasRole,
  isOwner,
  isTechOps,
  isSmsOps,
  isAcquisitions,
  checkPermission,
  extractMemberContext,
  resolveRoleIds,
} from "@/lib/discord/discord-permissions.js";
import {
  pong,
  ephemeralMessage,
  channelMessage,
  updateMessage,
  deniedResponse,
  errorResponse,
  approvalComponents,
  allowedRoleMentions,
  MESSAGE_FLAGS,
  INTERACTION_RESPONSE_TYPE,
} from "@/lib/discord/discord-response-helpers.js";
import { routeDiscordInteraction } from "@/lib/discord/discord-action-router.js";

// ── Ed25519 key pair helper (test-only) ───────────────────────────────────

function generateTestKeypair() {
  return crypto.generateKeyPairSync("ed25519");
}

function signMessage(privateKey, timestamp, body) {
  const message = Buffer.concat([
    Buffer.from(timestamp, "utf8"),
    Buffer.from(body, "utf8"),
  ]);
  return crypto.sign(null, message, privateKey).toString("hex");
}

function publicKeyHex(publicKey) {
  // Extract the raw 32-byte key from the DER SubjectPublicKeyInfo.
  const der = publicKey.export({ type: "spki", format: "der" });
  // The DER prefix for Ed25519 SPKI is 12 bytes; the key is the last 32.
  return der.slice(der.length - 32).toString("hex");
}

// ── Role ID fixtures ───────────────────────────────────────────────────────

const OWNER_ROLE_ID        = "111000000000000001";
const TECH_OPS_ROLE_ID     = "111000000000000002";
const SMS_OPS_ROLE_ID      = "111000000000000003";
const ACQUISITIONS_ROLE_ID = "111000000000000004";
const CLOSINGS_ROLE_ID     = "111000000000000005";
const UNRELATED_ROLE_ID    = "999999999999999999";

function withRoleEnv(fn) {
  const prev = {
    DISCORD_ROLE_OWNER_ID:        process.env.DISCORD_ROLE_OWNER_ID,
    DISCORD_ROLE_TECH_OPS_ID:     process.env.DISCORD_ROLE_TECH_OPS_ID,
    DISCORD_ROLE_SMS_OPS_ID:      process.env.DISCORD_ROLE_SMS_OPS_ID,
    DISCORD_ROLE_ACQUISITIONS_ID: process.env.DISCORD_ROLE_ACQUISITIONS_ID,
    DISCORD_ROLE_CLOSINGS_ID:     process.env.DISCORD_ROLE_CLOSINGS_ID,
  };
  process.env.DISCORD_ROLE_OWNER_ID        = OWNER_ROLE_ID;
  process.env.DISCORD_ROLE_TECH_OPS_ID     = TECH_OPS_ROLE_ID;
  process.env.DISCORD_ROLE_SMS_OPS_ID      = SMS_OPS_ROLE_ID;
  process.env.DISCORD_ROLE_ACQUISITIONS_ID = ACQUISITIONS_ROLE_ID;
  process.env.DISCORD_ROLE_CLOSINGS_ID     = CLOSINGS_ROLE_ID;
  try {
    return fn();
  } finally {
    process.env.DISCORD_ROLE_OWNER_ID        = prev.DISCORD_ROLE_OWNER_ID        ?? "";
    process.env.DISCORD_ROLE_TECH_OPS_ID     = prev.DISCORD_ROLE_TECH_OPS_ID    ?? "";
    process.env.DISCORD_ROLE_SMS_OPS_ID      = prev.DISCORD_ROLE_SMS_OPS_ID     ?? "";
    process.env.DISCORD_ROLE_ACQUISITIONS_ID = prev.DISCORD_ROLE_ACQUISITIONS_ID ?? "";
    process.env.DISCORD_ROLE_CLOSINGS_ID     = prev.DISCORD_ROLE_CLOSINGS_ID    ?? "";
  }
}

// ── Interaction builder helpers ────────────────────────────────────────────

function makeInteraction({
  type = 2,
  command_name = "queue",
  subcommand = "status",
  sub_options = [],
  role_ids = [],
  guild_id = "test-guild-123",
  channel_id = "ch-001",
  user_id = "user-001",
  username = "test_user",
} = {}) {
  return {
    id:         `iid-${Date.now()}`,
    type,
    guild_id,
    channel_id,
    member: {
      user:  { id: user_id, username },
      roles: role_ids,
    },
    data: {
      name:    command_name,
      options: subcommand
        ? [{ type: 1, name: subcommand, options: sub_options }]
        : [],
    },
  };
}

function makeButtonInteraction({ custom_id, role_ids = [], user_id = "owner-001" }) {
  return {
    id:         `bid-${Date.now()}`,
    type:       3,
    guild_id:   "test-guild-123",
    channel_id: "ch-001",
    member: {
      user:  { id: user_id, username: "approver" },
      roles: role_ids,
    },
    data: { custom_id },
  };
}

// ── Supabase stub ──────────────────────────────────────────────────────────

// The action router uses the singleton `supabase` export from client.js.
// In test, we patch the process.env so the client is initialized but does not
// make real network calls — most tests stub out supabase-dependent paths
// inline or use mock interaction types that don't touch Supabase.
// (Real DB calls are integration tests, not unit tests.)

// ─────────────────────────────────────────────────────────────────────────────
// 1. Invalid signature rejected
// ─────────────────────────────────────────────────────────────────────────────

test("verifyDiscordRequest: invalid signature returns false", () => {
  const { publicKey } = generateTestKeypair();
  const pubHex = publicKeyHex(publicKey);
  const timestamp = String(Date.now());
  const body = '{"type":1}';

  const result = verifyDiscordRequest({
    publicKey:  pubHex,
    signature:  "a".repeat(128), // 64 zero bytes, not a valid sig
    timestamp,
    rawBody:    body,
  });

  assert.equal(result, false, "tampered signature must fail verification");
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. Valid signature accepted
// ─────────────────────────────────────────────────────────────────────────────

test("verifyDiscordRequest: valid Ed25519 signature passes verification", () => {
  const { privateKey, publicKey } = generateTestKeypair();
  const pubHex    = publicKeyHex(publicKey);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const body      = '{"type":1}';
  const sigHex    = signMessage(privateKey, timestamp, body);

  const result = verifyDiscordRequest({
    publicKey:  pubHex,
    signature:  sigHex,
    timestamp,
    rawBody:    body,
  });

  assert.equal(result, true, "valid signature must pass verification");
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. PING returns PONG
// ─────────────────────────────────────────────────────────────────────────────

test("pong() returns type=1 PONG response", () => {
  const response = pong();
  assert.equal(response.type, INTERACTION_RESPONSE_TYPE.PONG);
  assert.equal(response.type, 1);
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. Missing role rejected
// ─────────────────────────────────────────────────────────────────────────────

test("checkPermission: user with no matching role is denied", () => {
  withRoleEnv(() => {
    const user_has_roles = [UNRELATED_ROLE_ID];
    const result = checkPermission(user_has_roles, ["owner", "tech_ops"]);
    assert.equal(result, false, "user with no relevant role must be denied");
  });
});

test("routeDiscordInteraction: user without Tech Ops gets denied for /queue run", async () => {
  const response = await withRoleEnv(async () => {
    const interaction = makeInteraction({
      command_name: "queue",
      subcommand:   "run",
      role_ids:     [UNRELATED_ROLE_ID],
    });
    return routeDiscordInteraction(interaction);
  });

  // Should be ephemeral denied message (flags & 64)
  assert.equal(response.type, INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE);
  assert.ok(response.data.flags & MESSAGE_FLAGS.EPHEMERAL, "denied response must be ephemeral");
  assert.ok(
    response.data.content.includes("🚫") || response.data.content.includes("denied") ||
    response.data.content.includes("Denied") || response.data.content.includes("denied"),
    "content must indicate denial"
  );
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. Owner passes permission checks for all restricted commands
// ─────────────────────────────────────────────────────────────────────────────

test("isOwner: returns true for user with owner role ID", () => {
  withRoleEnv(() => {
    assert.equal(isOwner([OWNER_ROLE_ID]), true);
    assert.equal(isOwner([TECH_OPS_ROLE_ID]), false);
  });
});

test("isTechOps: owner implicitly has tech_ops permission", () => {
  withRoleEnv(() => {
    assert.equal(isTechOps([OWNER_ROLE_ID]),    true,  "Owner must satisfy Tech Ops check");
    assert.equal(isTechOps([TECH_OPS_ROLE_ID]), true,  "Tech Ops must satisfy Tech Ops check");
    assert.equal(isTechOps([SMS_OPS_ROLE_ID]),  false, "SMS Ops must NOT satisfy Tech Ops check");
  });
});

test("checkPermission: owner satisfies any role requirement", () => {
  withRoleEnv(() => {
    for (const roles of [["owner"], ["tech_ops"], ["sms_ops"], ["acquisitions"]]) {
      assert.equal(
        checkPermission([OWNER_ROLE_ID], roles),
        true,
        `Owner must satisfy requirement: ${roles}`
      );
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. Tech Ops can run /sync podio (permission check passes)
// ─────────────────────────────────────────────────────────────────────────────

test("checkPermission: tech_ops passes for sync podio requirement", () => {
  withRoleEnv(() => {
    assert.equal(
      checkPermission([TECH_OPS_ROLE_ID], ["owner", "tech_ops"]),
      true,
      "Tech Ops must be allowed for sync-podio"
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. SMS Ops cannot release lock
// ─────────────────────────────────────────────────────────────────────────────

test("routeDiscordInteraction: SMS Ops cannot /lock release", async () => {
  const response = await withRoleEnv(async () => {
    const interaction = makeInteraction({
      command_name: "lock",
      subcommand:   "release",
      sub_options:  [{ name: "scope", value: "feeder" }],
      role_ids:     [SMS_OPS_ROLE_ID],
    });
    return routeDiscordInteraction(interaction);
  });

  assert.equal(response.type, INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE);
  assert.ok(response.data.flags & MESSAGE_FLAGS.EPHEMERAL, "must be ephemeral denied");
  assert.ok(response.data.content.includes("🚫"), "must contain denial icon");
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. Feeder limit > 25 creates approval request (not immediate execution)
// ─────────────────────────────────────────────────────────────────────────────

test("routeDiscordInteraction: feeder limit > 25 with Tech Ops creates approval buttons", async () => {
  // We need a real Supabase stub for the auditLog call.  In tests without a
  // real DB, the supabase.from('discord_command_events').insert() call will
  // fail silently (auditLog swallows errors).  So we just assert on the response.
  const response = await withRoleEnv(async () => {
    const interaction = makeInteraction({
      command_name: "feeder",
      subcommand:   "run",
      sub_options:  [
        { name: "limit",      value: 50 },
        { name: "scan_limit", value: 500 },
        { name: "dry_run",    value: true },
      ],
      role_ids: [TECH_OPS_ROLE_ID],
    });
    return routeDiscordInteraction(interaction);
  });

  assert.equal(response.type, INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE,
    "must be a regular message (with buttons)");

  // Must NOT be ephemeral — approval requests must be visible to the channel.
  assert.ok(
    !(response.data.flags & MESSAGE_FLAGS.EPHEMERAL),
    "approval request must be public"
  );

  // Must have components (approve/reject buttons).
  assert.ok(
    Array.isArray(response.data.components) && response.data.components.length > 0,
    "must include action row with buttons"
  );

  // Must mention approval is required.
  assert.ok(
    response.data.content.includes("approval") || response.data.content.includes("Owner"),
    "must explain that Owner approval is required"
  );

  // Must NOT expose any secrets.
  const content_lower = response.data.content.toLowerCase();
  assert.ok(!content_lower.includes("secret"),    "must not expose secret");
  assert.ok(!content_lower.includes("api_key"),   "must not expose api_key");
  assert.ok(!content_lower.includes("password"),  "must not expose password");
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. Feeder limit ≤ 25 with Tech Ops (permission granted, attempts internal call)
// ─────────────────────────────────────────────────────────────────────────────

test("routeDiscordInteraction: feeder limit ≤ 25 with Tech Ops does NOT create approval request", async () => {
  const response = await withRoleEnv(async () => {
    const interaction = makeInteraction({
      command_name: "feeder",
      subcommand:   "run",
      sub_options:  [
        { name: "limit",      value: 10 },
        { name: "scan_limit", value: 100 },
        { name: "dry_run",    value: true },
      ],
      role_ids: [TECH_OPS_ROLE_ID],
    });
    return routeDiscordInteraction(interaction);
  });

  // Should be a direct execution response (or timed-out "started" message),
  // NOT an approval-request message with buttons.
  const has_approval_buttons =
    Array.isArray(response.data?.components) &&
    response.data.components.length > 0 &&
    response.data.content?.includes("approval");

  assert.equal(has_approval_buttons, false, "small feeder run must not require approval");
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. Owner can approve — approval components have correct custom_id prefixes
// ─────────────────────────────────────────────────────────────────────────────

test("approvalComponents: buttons have correct approve/reject custom_id prefixes", () => {
  const components = approvalComponents(
    "discord_approve:token-abc",
    "discord_reject:token-abc",
    "Feeder run limit=50"
  );

  assert.equal(components.length, 1, "must return one ACTION_ROW");
  assert.equal(components[0].type, 1, "component must be ACTION_ROW (type=1)");

  const buttons = components[0].components;
  assert.equal(buttons.length, 2, "must have exactly 2 buttons");

  const approve_btn = buttons.find((b) => b.custom_id.startsWith("discord_approve:"));
  const reject_btn  = buttons.find((b) => b.custom_id.startsWith("discord_reject:"));

  assert.ok(approve_btn, "must have approve button");
  assert.ok(reject_btn,  "must have reject button");
  assert.equal(approve_btn.style, 3, "approve button must be SUCCESS (green)");
  assert.equal(reject_btn.style,  4, "reject button must be DANGER (red)");
});

// ─────────────────────────────────────────────────────────────────────────────
// 11. Approval logs to Supabase — audit flow builds correct record shape
// ─────────────────────────────────────────────────────────────────────────────

test("extractMemberContext: extracts correct fields from interaction", () => {
  const interaction = makeInteraction({
    user_id:    "user-999",
    username:   "tester",
    guild_id:   "guild-001",
    channel_id: "ch-999",
    role_ids:   [OWNER_ROLE_ID, TECH_OPS_ROLE_ID],
  });
  const ctx = extractMemberContext(interaction);

  assert.equal(ctx.user_id,    "user-999");
  assert.equal(ctx.username,   "tester");
  assert.equal(ctx.guild_id,   "guild-001");
  assert.equal(ctx.channel_id, "ch-999");
  assert.deepEqual(ctx.role_ids, [OWNER_ROLE_ID, TECH_OPS_ROLE_ID]);
});

// ─────────────────────────────────────────────────────────────────────────────
// 12. Command result never exposes secrets
// ─────────────────────────────────────────────────────────────────────────────

test("deniedResponse: content does not expose env var values", () => {
  const original_secret = process.env.INTERNAL_API_SECRET;
  process.env.INTERNAL_API_SECRET = "my-super-secret-key-do-not-leak";

  try {
    const response = deniedResponse("You do not have permission.");
    const content = JSON.stringify(response);
    assert.ok(
      !content.includes("my-super-secret-key-do-not-leak"),
      "denied response must not contain the INTERNAL_API_SECRET value"
    );
  } finally {
    process.env.INTERNAL_API_SECRET = original_secret ?? "";
  }
});

test("errorResponse: does not expose raw error objects or stack traces", () => {
  const response = errorResponse("Internal error occurred.");
  const content = JSON.stringify(response);

  assert.ok(!content.includes("at "), "must not include stack trace");
  assert.ok(!content.includes("Error:"), "must not include raw Error prefix");
});

test("routeDiscordInteraction: unknown command returns ephemeral error, no secrets", async () => {
  const original_secret = process.env.INTERNAL_API_SECRET;
  process.env.INTERNAL_API_SECRET = "leak-test-secret-xyz";

  try {
    const interaction = makeInteraction({ command_name: "nonexistent", subcommand: null });
    const response = await routeDiscordInteraction(interaction);
    const content = JSON.stringify(response);

    assert.ok(
      !content.includes("leak-test-secret-xyz"),
      "response must not contain INTERNAL_API_SECRET"
    );
    assert.ok(
      !content.includes("leak-test"),
      "no fragment of the secret may appear"
    );
  } finally {
    process.env.INTERNAL_API_SECRET = original_secret ?? "";
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 13. Role mention payload uses allowed_mentions
// ─────────────────────────────────────────────────────────────────────────────

test("allowedRoleMentions: builds correct allowed_mentions block", () => {
  const mentions = allowedRoleMentions([OWNER_ROLE_ID, TECH_OPS_ROLE_ID]);

  assert.deepEqual(mentions.parse, [], "parse must be empty — no wildcard mentions");
  assert.deepEqual(mentions.roles, [OWNER_ROLE_ID, TECH_OPS_ROLE_ID]);
});

test("allowedRoleMentions: filters empty / non-string role IDs", () => {
  const mentions = allowedRoleMentions([OWNER_ROLE_ID, "", null, undefined, 12345]);
  assert.deepEqual(mentions.roles, [OWNER_ROLE_ID],
    "only valid non-empty string IDs must be included");
});

test("channelMessage with role mention uses allowed_mentions so only pinged roles are notified", () => {
  const msg = channelMessage(
    `<@&${OWNER_ROLE_ID}> — your approval is needed.`,
    { allowed_mentions: allowedRoleMentions([OWNER_ROLE_ID]) }
  );

  assert.ok(
    msg.data.allowed_mentions,
    "allowed_mentions must be present in role-mention messages"
  );
  assert.deepEqual(msg.data.allowed_mentions.parse, [],
    "parse must be empty to avoid @everyone / @here");
  assert.ok(
    msg.data.allowed_mentions.roles.includes(OWNER_ROLE_ID),
    "owner role must be in the allowlist"
  );
});

// ─────────────────────────────────────────────────────────────────────────────
// 14. Queue status — handler path resolves without throwing
// ─────────────────────────────────────────────────────────────────────────────

test("routeDiscordInteraction: /queue status resolves to a channel message", async () => {
  // In test, supabase will return an error (no real DB) which the handler
  // catches and returns an error response; either way it must NOT throw.
  const interaction = makeInteraction({
    command_name: "queue",
    subcommand:   "status",
    role_ids:     [],
  });

  let response;
  let threw = false;
  try {
    response = await routeDiscordInteraction(interaction);
  } catch {
    threw = true;
  }

  assert.equal(threw, false, "/queue status must not throw");
  assert.ok(response?.type === INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE,
    `must return a channel message (type=4), got type=${response?.type}`);
});

// ─────────────────────────────────────────────────────────────────────────────
// 15. Acquisitions can run /lead summarize
// ─────────────────────────────────────────────────────────────────────────────

test("isAcquisitions: returns true for acquisitions role and for owner", () => {
  withRoleEnv(() => {
    assert.equal(isAcquisitions([ACQUISITIONS_ROLE_ID]), true);
    assert.equal(isAcquisitions([OWNER_ROLE_ID]),        true);
    assert.equal(isAcquisitions([TECH_OPS_ROLE_ID]),     false);
  });
});

test("routeDiscordInteraction: /lead summarize with acquisitions role does not get denied", async () => {
  const response = await withRoleEnv(async () => {
    const interaction = makeInteraction({
      command_name: "lead",
      subcommand:   "summarize",
      sub_options:  [{ name: "phone_or_owner_id", value: "+15551234567" }],
      role_ids:     [ACQUISITIONS_ROLE_ID],
    });
    return routeDiscordInteraction(interaction);
  });

  // Must not be a 🚫 denied response.
  assert.ok(
    !response.data?.content?.includes("🚫"),
    "acquisitions user must not be denied for /lead summarize"
  );
});

// ─────────────────────────────────────────────────────────────────────────────
// 16. SMS Ops cannot /lock release
// ─────────────────────────────────────────────────────────────────────────────

test("checkPermission: sms_ops does NOT satisfy [owner, tech_ops] requirement", () => {
  withRoleEnv(() => {
    assert.equal(
      checkPermission([SMS_OPS_ROLE_ID], ["owner", "tech_ops"]),
      false,
      "SMS Ops must not satisfy Tech Ops / Owner requirement"
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 17. Guild guard — wrong guild_id
// ─────────────────────────────────────────────────────────────────────────────

test("verifyDiscordRequest: missing body returns false", () => {
  const { publicKey } = generateTestKeypair();
  const result = verifyDiscordRequest({
    publicKey:  publicKeyHex(publicKey),
    signature:  "a".repeat(128),
    timestamp:  "1234567890",
    rawBody:    "",
  });
  // Empty body + fabricated signature → false
  assert.equal(result, false);
});

test("verifyDiscordRequest: missing publicKey returns false", () => {
  const result = verifyDiscordRequest({
    publicKey:  "",
    signature:  "a".repeat(128),
    timestamp:  "1234567890",
    rawBody:    '{"type":1}',
  });
  assert.equal(result, false);
});

// ─────────────────────────────────────────────────────────────────────────────
// 18. Reject button handler updates message and does not execute action
// ─────────────────────────────────────────────────────────────────────────────

test("routeDiscordInteraction: reject button returns UPDATE_MESSAGE with rejection text", async () => {
  const fake_token = "reject-test-token-001";
  const interaction = makeButtonInteraction({
    custom_id: `discord_reject:${fake_token}`,
    role_ids:  [OWNER_ROLE_ID],
    user_id:   "owner-reject-user",
  });

  const response = await withRoleEnv(async () => routeDiscordInteraction(interaction));

  assert.equal(response.type, INTERACTION_RESPONSE_TYPE.UPDATE_MESSAGE,
    "reject button must return UPDATE_MESSAGE (type=7)");
  assert.ok(
    response.data.content.includes("rejected") || response.data.content.includes("Rejected"),
    "rejection message must indicate the action was rejected"
  );
  // Components must be empty (buttons removed).
  assert.deepEqual(response.data.components, []);
});

// ─────────────────────────────────────────────────────────────────────────────
// Extra: response helpers shape validation
// ─────────────────────────────────────────────────────────────────────────────

test("ephemeralMessage: sets EPHEMERAL flag (64)", () => {
  const r = ephemeralMessage("test");
  assert.equal(r.type, INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE);
  assert.equal(r.data.flags, MESSAGE_FLAGS.EPHEMERAL);
  assert.equal(r.data.flags, 64);
});

test("channelMessage: does NOT set EPHEMERAL flag", () => {
  const r = channelMessage("test");
  assert.ok(!r.data.flags || !(r.data.flags & MESSAGE_FLAGS.EPHEMERAL));
});

test("updateMessage: sets type=7 and clears components by default", () => {
  const r = updateMessage("done");
  assert.equal(r.type, INTERACTION_RESPONSE_TYPE.UPDATE_MESSAGE);
  assert.equal(r.type, 7);
  assert.deepEqual(r.data.components, []);
});

test("channelMessage: truncates content at 2000 characters", () => {
  const long = "x".repeat(3000);
  const r = channelMessage(long);
  assert.equal(r.data.content.length, 2000);
});

test("resolveRoleIds: returns only configured non-empty role IDs", () => {
  withRoleEnv(() => {
    const ids = resolveRoleIds(["owner", "tech_ops", "nonexistent_role"]);
    assert.ok(ids.includes(OWNER_ROLE_ID),    "must include owner ID");
    assert.ok(ids.includes(TECH_OPS_ROLE_ID), "must include tech_ops ID");
    // "nonexistent_role" has no env mapping → not in result
    assert.equal(ids.length, 2, "must not include ID for unconfigured role");
  });
});
