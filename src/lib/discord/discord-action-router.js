/**
 * discord-action-router.js
 *
 * Routes Discord slash commands and button interactions to internal API routes
 * or Supabase queries, enforces role permissions, and writes an audit record
 * to the discord_command_events table for every invocation.
 *
 * Design decisions:
 *
 *  1. All internal-route calls use a 2.5-second timeout.  If the downstream
 *     route completes in time the result is included in the Discord response.
 *     If it times out the user gets "action started — monitor alerts channel"
 *     and the separate serverless invocation continues independently.
 *
 *  2. High-risk commands (feeder run > 25, campaign resume) require an Owner
 *     approval button before execution.  The pending action is stored in
 *     Supabase; when the Owner clicks Approve, the button handler looks it up
 *     and executes it.
 *
 *  3. Secrets are NEVER included in Discord response content.
 *
 *  4. Audit logging is best-effort; failures are swallowed so a Supabase
 *     outage never blocks command execution.
 */

import crypto from "node:crypto";

import { supabase } from "@/lib/supabase/client.js";
import {
  extractMemberContext,
  isOwner,
  isTechOps,
  isSmsOps,
  isAcquisitions,
  checkPermission,
} from "./discord-permissions.js";
import {
  channelMessage,
  ephemeralMessage,
  updateMessage,
  approvalComponents,
  deniedResponse,
  errorResponse,
} from "./discord-response-helpers.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Maximum ms we wait for an internal route before responding "started". */
const CALL_TIMEOUT_MS = 2500;

/** Maximum limit allowed for /queue run without higher scrutiny. */
const QUEUE_RUN_MAX_LIMIT = 50;

/** Feeder limits: ≤ this threshold Tech Ops can approve; above requires Owner. */
const FEEDER_TECH_OPS_LIMIT = 25;

/** Hard cap on feeder limit even with Owner approval. */
const FEEDER_HARD_CAP = 200;

/** Approval button custom_id prefixes. */
const APPROVE_PREFIX = "discord_approve:";
const REJECT_PREFIX  = "discord_reject:";

// ---------------------------------------------------------------------------
// Internal route caller
// ---------------------------------------------------------------------------

/**
 * Call an internal API route with the shared secret and an optional timeout.
 * Returns { ok, data } on success or { ok: false, error, timed_out? }.
 *
 * The outgoing request uses INTERNAL_API_SECRET for routes that accept it,
 * and also includes Authorization: Bearer CRON_SECRET so feeder routes that
 * only accept cron auth are also covered.
 *
 * @param {string} path    - e.g. "/api/internal/events/sync-podio"
 * @param {object} options - { method, body, timeout_ms }
 */
async function callInternal(path, options = {}) {
  const base   = String(process.env.APP_BASE_URL ?? "").replace(/\/$/, "");
  const url    = `${base}${path}`;
  const secret = String(process.env.INTERNAL_API_SECRET ?? "");
  const cron   = String(process.env.CRON_SECRET ?? secret);

  const controller = new AbortController();
  const timeout_ms = options.timeout_ms ?? CALL_TIMEOUT_MS;
  const timer = setTimeout(() => controller.abort(), timeout_ms);

  try {
    const response = await fetch(url, {
      method:  options.method ?? "POST",
      headers: {
        "Content-Type":          "application/json",
        "x-internal-api-secret": secret,
        "Authorization":         `Bearer ${cron}`,
      },
      body:   options.body != null ? JSON.stringify(options.body) : undefined,
      signal: controller.signal,
    });
    clearTimeout(timer);

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      return { ok: false, status: response.status, error: `HTTP ${response.status}` };
    }

    const data = await response.json().catch(() => ({}));
    return { ok: true, data };
  } catch (err) {
    clearTimeout(timer);
    if (err?.name === "AbortError") {
      return { ok: false, timed_out: true, error: "timed_out" };
    }
    return { ok: false, error: "internal_call_failed" };
  }
}

// ---------------------------------------------------------------------------
// Audit logger
// ---------------------------------------------------------------------------

/**
 * Insert one row into discord_command_events.  Never throws.
 */
async function auditLog(fields) {
  try {
    await supabase.from("discord_command_events").insert({
      created_at: new Date().toISOString(),
      ...fields,
    });
  } catch {
    // Non-fatal.
  }
}

// ---------------------------------------------------------------------------
// Option extractor helpers
// ---------------------------------------------------------------------------

function getOption(options_array, name) {
  if (!Array.isArray(options_array)) return undefined;
  return options_array.find((o) => o?.name === name)?.value;
}

function getSubcommand(options_array) {
  if (!Array.isArray(options_array)) return null;
  const sub = options_array.find((o) => o?.type === 1);
  return sub ? { name: sub.name, options: sub.options ?? [] } : null;
}

// ---------------------------------------------------------------------------
// Safe result summarizer (never exposes secrets or message bodies)
// ---------------------------------------------------------------------------

const FORBIDDEN_RESULT_KEYS = new Set([
  "secret", "token", "password", "api_key", "apikey", "credential",
  "service_role", "client_secret", "internal_api", "authorization",
  "message_body", "message_text", "body",
]);

function isForbiddenKey(key) {
  const lower = String(key ?? "").toLowerCase();
  return [...FORBIDDEN_RESULT_KEYS].some((f) => lower.includes(f));
}

function safeResultSummary(data, fields_to_show) {
  if (!data || typeof data !== "object") return {};
  const out = {};
  for (const key of fields_to_show) {
    if (!isForbiddenKey(key) && data[key] !== undefined) {
      out[key] = data[key];
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Command: /queue status
// ---------------------------------------------------------------------------

async function handleQueueStatus(context) {
  try {
    // Fetch status column only; cap at 10 000 rows to avoid pagination.
    const { data, error } = await supabase
      .from("send_queue")
      .select("queue_status")
      .limit(10000);

    if (error) throw error;

    const counts = {};
    for (const row of data ?? []) {
      const s = String(row.queue_status ?? "unknown");
      counts[s] = (counts[s] ?? 0) + 1;
    }

    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    const lines = Object.entries(counts)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([status, count]) => `  **${status}**: ${count}`);

    return channelMessage(
      `📊 **Send Queue Status**\n${lines.join("\n") || "  _(empty)_"}\n**Total**: ${total}`
    );
  } catch {
    return errorResponse("Failed to read queue status.");
  }
}

// ---------------------------------------------------------------------------
// Command: /queue run <limit>
// ---------------------------------------------------------------------------

async function handleQueueRun({ options_array, context }) {
  const limit = Math.min(
    Math.max(1, Number(getOption(options_array, "limit")) || 10),
    QUEUE_RUN_MAX_LIMIT
  );

  const result = await callInternal("/api/internal/queue/run", {
    method: "POST",
    body:   { limit },
  });

  if (result.timed_out) {
    return channelMessage(
      `⏳ **Queue Run** started (limit=${limit}).\n` +
      `The job is running — check the alerts channel for completion.`
    );
  }
  if (!result.ok) {
    return errorResponse(`Queue run call failed (${result.error ?? "unknown"}).`);
  }

  const d = safeResultSummary(result.data, [
    "sent_count", "sent", "failed_count", "failed",
    "skipped_count", "skipped", "total",
  ]);
  return channelMessage(
    `✅ **Queue Run** (limit=${limit})\n` +
    `Sent: **${d.sent_count ?? d.sent ?? "?"}** | ` +
    `Failed: **${d.failed_count ?? d.failed ?? "?"}** | ` +
    `Skipped: **${d.skipped_count ?? d.skipped ?? "?"}**`
  );
}

// ---------------------------------------------------------------------------
// Command: /sync podio <limit>
// ---------------------------------------------------------------------------

async function handleSyncPodio({ options_array, context }) {
  const limit = Math.min(
    Math.max(1, Number(getOption(options_array, "limit")) || 20),
    100
  );

  const result = await callInternal("/api/internal/events/sync-podio", {
    method: "POST",
    body:   { limit },
  });

  if (result.timed_out) {
    return channelMessage(
      `⏳ **Podio Sync** started (limit=${limit}).\n` +
      `Check the alerts channel for completion.`
    );
  }
  if (!result.ok) {
    return errorResponse(`Podio sync call failed (${result.error ?? "unknown"}).`);
  }

  const d = safeResultSummary(result.data, [
    "loaded_count", "synced_count", "failed_count",
    "skipped_count", "duration_ms",
  ]);
  return channelMessage(
    `✅ **Podio Sync** (limit=${limit})\n` +
    `Loaded: **${d.loaded_count ?? "?"}** | ` +
    `Synced: **${d.synced_count ?? "?"}** | ` +
    `Failed: **${d.failed_count ?? "?"}** | ` +
    `Skipped: **${d.skipped_count ?? "?"}**` +
    (d.duration_ms != null ? ` | ${d.duration_ms}ms` : "")
  );
}

// ---------------------------------------------------------------------------
// Command: /diagnostic inbound
// ---------------------------------------------------------------------------

async function handleDiagnosticInbound({ context }) {
  const result = await callInternal("/api/internal/events/inbound-diagnostic", {
    method: "GET",
  });

  if (result.timed_out) {
    return channelMessage("⏳ **Inbound Diagnostic** running — check alerts channel.");
  }
  if (!result.ok) {
    return errorResponse(`Diagnostic call failed (${result.error ?? "unknown"}).`);
  }

  const d = safeResultSummary(result.data, [
    "total", "recent_24h", "pending_count", "failed_count",
    "inbound_count", "outbound_count",
  ]);
  const lines = Object.entries(d).map(([k, v]) => `  **${k}**: ${v}`);
  return channelMessage(
    `🔍 **Inbound Diagnostic**\n${lines.join("\n") || "  _(no summary fields returned)_"}`
  );
}

// ---------------------------------------------------------------------------
// Command: /diagnostic podio-sync
// ---------------------------------------------------------------------------

async function handleDiagnosticPodioSync({ options_array, context }) {
  const limit = Math.min(
    Math.max(1, Number(getOption(options_array, "limit")) || 20),
    100
  );

  const result = await callInternal(
    `/api/internal/events/sync-podio-diagnostic?limit=${limit}`,
    { method: "GET" }
  );

  if (result.timed_out) {
    return channelMessage("⏳ **Podio Sync Diagnostic** running — check alerts channel.");
  }
  if (!result.ok) {
    return errorResponse(`Diagnostic call failed (${result.error ?? "unknown"}).`);
  }

  const d = safeResultSummary(result.data, [
    "total_pending", "total_failed", "total_synced",
    "oldest_pending_hours", "recent_errors",
  ]);
  const lines = Object.entries(d).map(([k, v]) => `  **${k}**: ${v}`);
  return channelMessage(
    `🔍 **Podio Sync Diagnostic**\n${lines.join("\n") || "  _(no summary fields returned)_"}`
  );
}

// ---------------------------------------------------------------------------
// Command: /lock release <scope>
// ---------------------------------------------------------------------------

async function handleLockRelease({ options_array, context }) {
  const scope = String(getOption(options_array, "scope") ?? "feeder").trim() || "feeder";

  const result = await callInternal("/api/internal/runs/release-lock", {
    method: "POST",
    body:   { scope },
  });

  if (result.timed_out) {
    return channelMessage(`⏳ **Lock Release** (scope=${scope}) — check alerts channel.`);
  }
  if (!result.ok) {
    return errorResponse(`Lock release failed (${result.error ?? "unknown"}).`);
  }

  const d = safeResultSummary(result.data, ["ok", "released", "scope", "was_locked"]);
  return channelMessage(
    `🔓 **Lock Release** (scope=${scope})\n` +
    `Released: **${d.released ?? d.ok ?? "?"}**`
  );
}

// ---------------------------------------------------------------------------
// Command: /feeder run <limit> [scan_limit] [dry_run]
// ---------------------------------------------------------------------------

async function handleFeederRun({ options_array, context, interaction }) {
  const limit      = Math.min(
    Math.max(1, Number(getOption(options_array, "limit")) || 10),
    FEEDER_HARD_CAP
  );
  const scan_limit = Math.max(1, Number(getOption(options_array, "scan_limit")) || 500);
  const dry_run    = Boolean(getOption(options_array, "dry_run") ?? false);
  const role_ids   = context.role_ids;

  // /feeder run with dry_run=false and limit > 100 always requires Owner.
  const needs_owner_approval =
    limit > FEEDER_TECH_OPS_LIMIT || (!dry_run && limit > 100);

  if (needs_owner_approval && !isOwner(role_ids)) {
    // Insufficient role even to request approval.
    if (!isTechOps(role_ids)) {
      return deniedResponse("You need Tech Ops or Owner to run the feeder.");
    }

    // Tech Ops may request Owner approval.
    const approval_token = crypto.randomUUID();
    const action_payload = { command: "feeder_run", limit, scan_limit, dry_run };

    await auditLog({
      interaction_id:    interaction.id,
      guild_id:          context.guild_id,
      channel_id:        context.channel_id,
      user_id:           context.user_id,
      username:          context.username,
      command_name:      "feeder",
      subcommand:        "run",
      options:           action_payload,
      role_ids:          context.role_ids,
      permission_outcome: "allowed",
      action_outcome:    "approval_pending",
      approval_token,
    });

    return channelMessage(
      `⚠️ **Feeder Run** (limit=${limit}, scan_limit=${scan_limit}, dry_run=${dry_run})\n` +
      `Requested by <@${context.user_id}> — requires **Owner** approval.`,
      {
        components:       approvalComponents(
          `${APPROVE_PREFIX}${approval_token}`,
          `${REJECT_PREFIX}${approval_token}`,
          `Feeder run limit=${limit}`
        ),
        allowed_mentions: { parse: [] },
      }
    );
  }

  // Either Owner or limit ≤ 25 with Tech Ops — execute immediately.
  const result = await callInternal("/api/internal/outbound/feed-master-owners", {
    method: "POST",
    body:   { limit, scan_limit, dry_run },
  });

  if (result.timed_out) {
    return channelMessage(
      `⏳ **Feeder Run** started (limit=${limit}, dry_run=${dry_run}).\n` +
      `Check the alerts channel for completion.`
    );
  }
  if (!result.ok) {
    return errorResponse(`Feeder run failed (${result.error ?? "unknown"}).`);
  }

  const d = safeResultSummary(result.data, [
    "loaded_count", "eligible_count", "inserted_count",
    "duplicate_count", "skipped_count", "error_count",
  ]);
  return channelMessage(
    `✅ **Feeder Run** (limit=${limit})\n` +
    `Loaded: **${d.loaded_count ?? "?"}** | ` +
    `Eligible: **${d.eligible_count ?? "?"}** | ` +
    `Inserted: **${d.inserted_count ?? "?"}** | ` +
    `Dupes: **${d.duplicate_count ?? "?"}** | ` +
    `Skipped: **${d.skipped_count ?? "?"}**`
  );
}

// ---------------------------------------------------------------------------
// Command: /campaign pause <campaign_id>
// ---------------------------------------------------------------------------

async function handleCampaignPause({ options_array, context }) {
  const campaign_id = String(getOption(options_array, "campaign_id") ?? "").trim();
  if (!campaign_id) return errorResponse("campaign_id is required.");

  // Campaign pause calls a dedicated internal route.  If the route does not
  // exist yet the caller receives a clean error rather than an uncaught throw.
  const result = await callInternal("/api/internal/outbound/campaign-pause", {
    method: "POST",
    body:   { campaign_id },
  });

  if (result.timed_out) {
    return channelMessage(`⏳ **Campaign Pause** (id=${campaign_id}) — check alerts channel.`);
  }
  if (!result.ok) {
    if (result.status === 404) {
      return errorResponse("Campaign pause route not yet implemented.");
    }
    return errorResponse(`Campaign pause failed (${result.error ?? "unknown"}).`);
  }

  return channelMessage(`⏸️ **Campaign ${campaign_id}** paused.`);
}

// ---------------------------------------------------------------------------
// Command: /campaign resume <campaign_id>  (requires Owner approval)
// ---------------------------------------------------------------------------

async function handleCampaignResume({ options_array, context, interaction }) {
  const campaign_id = String(getOption(options_array, "campaign_id") ?? "").trim();
  if (!campaign_id) return errorResponse("campaign_id is required.");

  const approval_token = crypto.randomUUID();
  const action_payload = { command: "campaign_resume", campaign_id };

  await auditLog({
    interaction_id:    interaction.id,
    guild_id:          context.guild_id,
    channel_id:        context.channel_id,
    user_id:           context.user_id,
    username:          context.username,
    command_name:      "campaign",
    subcommand:        "resume",
    options:           action_payload,
    role_ids:          context.role_ids,
    permission_outcome: "allowed",
    action_outcome:    "approval_pending",
    approval_token,
  });

  return channelMessage(
    `⚠️ **Campaign Resume** (id=${campaign_id})\n` +
    `Requested by <@${context.user_id}> — requires **Owner** approval.`,
    {
      components:       approvalComponents(
        `${APPROVE_PREFIX}${approval_token}`,
        `${REJECT_PREFIX}${approval_token}`,
        `Resume campaign ${campaign_id}`
      ),
      allowed_mentions: { parse: [] },
    }
  );
}

// ---------------------------------------------------------------------------
// Command: /lead summarize <phone_or_owner_id>
// ---------------------------------------------------------------------------

async function handleLeadSummarize({ options_array, context }) {
  const query = String(getOption(options_array, "phone_or_owner_id") ?? "").trim();
  if (!query) return errorResponse("phone_or_owner_id is required.");

  try {
    // Query message_events for this identifier (phone number or owner id).
    const is_numeric = /^\d+$/.test(query);

    let builder = supabase
      .from("message_events")
      .select("event_type, direction, delivery_status, created_at")
      .order("created_at", { ascending: false })
      .limit(100);

    if (is_numeric) {
      builder = builder.eq("master_owner_id", Number(query));
    } else {
      // Phone number — stored various ways; search by provider metadata
      builder = builder
        .or(`metadata->>to_phone.eq.${query},metadata->>from_phone.eq.${query}`);
    }

    const { data, error } = await builder;
    if (error) throw error;

    const rows = data ?? [];
    if (rows.length === 0) {
      return ephemeralMessage(`🔍 No records found for **${query}**.`);
    }

    const by_type = {};
    for (const row of rows) {
      const k = `${row.direction ?? "?"}/${row.event_type ?? "?"}`;
      by_type[k] = (by_type[k] ?? 0) + 1;
    }
    const lines = Object.entries(by_type)
      .map(([k, v]) => `  **${k}**: ${v}`)
      .join("\n");

    const most_recent = rows[0].created_at
      ? new Date(rows[0].created_at).toISOString().slice(0, 10)
      : "unknown";

    return ephemeralMessage(
      `🔍 **Lead Summary** for \`${query}\`\n` +
      `Events (last ${rows.length}):\n${lines}\n` +
      `Most recent: **${most_recent}**`
    );
  } catch {
    return errorResponse("Failed to fetch lead records.");
  }
}

// ---------------------------------------------------------------------------
// Button: approve / reject
// ---------------------------------------------------------------------------

async function handleApproval({ interaction, custom_id }) {
  const is_approve = custom_id.startsWith(APPROVE_PREFIX);
  const token = custom_id.slice(
    is_approve ? APPROVE_PREFIX.length : REJECT_PREFIX.length
  );

  if (!token) return updateMessage("❌ Invalid approval token.");

  const context = extractMemberContext(interaction);

  // Only Owner can approve.
  if (is_approve && !isOwner(context.role_ids)) {
    return updateMessage("🚫 Only the **Owner** role can approve this action.");
  }

  if (!is_approve) {
    // Rejection — update Supabase record, update message.
    await auditLog({
      interaction_id:    interaction.id,
      guild_id:          context.guild_id,
      channel_id:        context.channel_id,
      user_id:           context.user_id,
      username:          context.username,
      command_name:      "approval",
      subcommand:        "reject",
      options:           { approval_token: token },
      role_ids:          context.role_ids,
      permission_outcome: "allowed",
      action_outcome:    "rejected",
      approved_by_user_id: context.user_id,
    });

    try {
      await supabase
        .from("discord_command_events")
        .update({ action_outcome: "rejected", approved_by_user_id: context.user_id })
        .eq("approval_token", token)
        .eq("action_outcome", "approval_pending");
    } catch { /* non-fatal */ }

    return updateMessage(`❌ Action rejected by <@${context.user_id}>.`, {
      allowed_mentions: { parse: [] },
    });
  }

  // Approval — look up the pending action.
  let pending = null;
  try {
    const { data } = await supabase
      .from("discord_command_events")
      .select("id, options, command_name, subcommand")
      .eq("approval_token", token)
      .eq("action_outcome", "approval_pending")
      .limit(1)
      .maybeSingle();
    pending = data;
  } catch { /* non-fatal */ }

  if (!pending) {
    return updateMessage("⚠️ Approval record not found or already processed.");
  }

  const action_payload = pending.options ?? {};
  let exec_result = null;

  // Mark as approved before executing (idempotency guard).
  try {
    await supabase
      .from("discord_command_events")
      .update({
        action_outcome:      "approved",
        approved_by_user_id: context.user_id,
        executed_at:         new Date().toISOString(),
      })
      .eq("id", pending.id);
  } catch { /* non-fatal */ }

  // Execute the stored action.
  if (action_payload.command === "feeder_run") {
    const { limit, scan_limit, dry_run } = action_payload;
    exec_result = await callInternal("/api/internal/outbound/feed-master-owners", {
      method: "POST",
      body:   { limit, scan_limit, dry_run },
    });
  } else if (action_payload.command === "campaign_resume") {
    const { campaign_id } = action_payload;
    exec_result = await callInternal("/api/internal/outbound/campaign-resume", {
      method: "POST",
      body:   { campaign_id },
    });
  } else {
    return updateMessage(
      `⚠️ Approved by <@${context.user_id}> but unknown action type "${pending.command_name}/${pending.subcommand}".`,
      { allowed_mentions: { parse: [] } }
    );
  }

  await auditLog({
    interaction_id:     interaction.id,
    guild_id:           context.guild_id,
    channel_id:         context.channel_id,
    user_id:            context.user_id,
    username:           context.username,
    command_name:       "approval",
    subcommand:         "execute",
    options:            { approval_token: token, ...action_payload },
    role_ids:           context.role_ids,
    permission_outcome: "allowed",
    action_outcome:     exec_result?.ok ? "executed" : "error",
    approved_by_user_id: context.user_id,
    result_summary:     exec_result?.ok
      ? JSON.stringify(safeResultSummary(exec_result.data, [
          "loaded_count", "inserted_count", "synced_count",
          "sent_count", "failed_count", "duration_ms",
        ])).slice(0, 500)
      : null,
    error_message: exec_result?.ok ? null : exec_result?.error,
  });

  if (exec_result?.timed_out) {
    return updateMessage(
      `⏳ Approved by <@${context.user_id}>. Job started — check alerts channel.`,
      { allowed_mentions: { parse: [] } }
    );
  }
  if (!exec_result?.ok) {
    return updateMessage(
      `✅ Approved by <@${context.user_id}> but execution failed: ${exec_result?.error ?? "unknown"}.`,
      { allowed_mentions: { parse: [] } }
    );
  }

  const d = safeResultSummary(exec_result.data, [
    "loaded_count", "inserted_count", "synced_count",
    "sent_count", "failed_count", "duration_ms",
  ]);
  const summary = Object.entries(d)
    .map(([k, v]) => `**${k}**: ${v}`)
    .join(" | ") || "complete";

  return updateMessage(
    `✅ Approved by <@${context.user_id}>. ${summary}`,
    { allowed_mentions: { parse: [] } }
  );
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/**
 * Route a Discord interaction to the appropriate handler.
 *
 * @param {object} interaction  - Parsed Discord interaction body.
 * @returns {Promise<object>}   - Discord interaction response object.
 */
export async function routeDiscordInteraction(interaction) {
  const context = extractMemberContext(interaction);
  const role_ids = context.role_ids;

  // ── MESSAGE_COMPONENT (button click) ──────────────────────────────────────
  if (interaction.type === 3) {
    const custom_id = String(interaction?.data?.custom_id ?? "");
    if (custom_id.startsWith(APPROVE_PREFIX) || custom_id.startsWith(REJECT_PREFIX)) {
      return handleApproval({ interaction, custom_id });
    }
    return updateMessage("⚠️ Unknown button interaction.");
  }

  // ── APPLICATION_COMMAND (slash command) ───────────────────────────────────
  if (interaction.type !== 2) {
    return errorResponse("Unsupported interaction type.");
  }

  const command_name   = String(interaction?.data?.name ?? "").toLowerCase();
  const top_options    = interaction?.data?.options ?? [];
  const sub            = getSubcommand(top_options);
  const sub_name       = sub?.name ?? null;
  const sub_opts       = sub?.options ?? top_options;

  let response;

  try {
    // ── /queue ──────────────────────────────────────────────────────────────
    if (command_name === "queue") {
      if (sub_name === "status") {
        // Any authenticated guild member can check status.
        response = await handleQueueStatus(context);
      } else if (sub_name === "run") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleQueueRun({ options_array: sub_opts, context });
        }
      } else {
        response = errorResponse(`Unknown /queue subcommand: ${sub_name}`);
      }

    // ── /sync ────────────────────────────────────────────────────────────────
    } else if (command_name === "sync") {
      if (sub_name === "podio") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleSyncPodio({ options_array: sub_opts, context });
        }
      } else {
        response = errorResponse(`Unknown /sync subcommand: ${sub_name}`);
      }

    // ── /diagnostic ──────────────────────────────────────────────────────────
    } else if (command_name === "diagnostic") {
      if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
        response = deniedResponse("Requires **Tech Ops** or **Owner**.");
      } else if (sub_name === "inbound") {
        response = await handleDiagnosticInbound({ context });
      } else if (sub_name === "podio-sync") {
        response = await handleDiagnosticPodioSync({ options_array: sub_opts, context });
      } else {
        response = errorResponse(`Unknown /diagnostic subcommand: ${sub_name}`);
      }

    // ── /lock ────────────────────────────────────────────────────────────────
    } else if (command_name === "lock") {
      if (sub_name === "release") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleLockRelease({ options_array: sub_opts, context });
        }
      } else {
        response = errorResponse(`Unknown /lock subcommand: ${sub_name}`);
      }

    // ── /feeder ──────────────────────────────────────────────────────────────
    } else if (command_name === "feeder") {
      if (sub_name === "run") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederRun({
            options_array: sub_opts,
            context,
            interaction,
          });
        }
      } else {
        response = errorResponse(`Unknown /feeder subcommand: ${sub_name}`);
      }

    // ── /campaign ────────────────────────────────────────────────────────────
    } else if (command_name === "campaign") {
      if (sub_name === "pause") {
        if (!checkPermission(role_ids, ["owner", "sms_ops"])) {
          response = deniedResponse("Requires **SMS Ops** or **Owner**.");
        } else {
          response = await handleCampaignPause({ options_array: sub_opts, context });
        }
      } else if (sub_name === "resume") {
        if (!checkPermission(role_ids, ["owner", "sms_ops"])) {
          response = deniedResponse("Requires **SMS Ops** or **Owner**.");
        } else {
          response = await handleCampaignResume({
            options_array: sub_opts,
            context,
            interaction,
          });
        }
      } else {
        response = errorResponse(`Unknown /campaign subcommand: ${sub_name}`);
      }

    // ── /lead ────────────────────────────────────────────────────────────────
    } else if (command_name === "lead") {
      if (sub_name === "summarize") {
        if (!checkPermission(role_ids, ["owner", "acquisitions"])) {
          response = deniedResponse("Requires **Acquisitions** or **Owner**.");
        } else {
          response = await handleLeadSummarize({ options_array: sub_opts, context });
        }
      } else {
        response = errorResponse(`Unknown /lead subcommand: ${sub_name}`);
      }

    } else {
      response = errorResponse(`Unknown command: /${command_name}`);
    }
  } catch {
    response = errorResponse("Unexpected error processing your command.");
  }

  // ── Audit every slash command ─────────────────────────────────────────────
  // (Button approvals are audited within handleApproval itself.)
  if (interaction.type === 2) {
    const permission_ok = response?.data?.flags !== undefined
      ? response.data.flags & 64 && response.data.content?.includes("🚫")
        ? "denied"
        : "allowed"
      : "allowed";

    auditLog({
      interaction_id:     interaction.id,
      guild_id:           context.guild_id,
      channel_id:         context.channel_id,
      user_id:            context.user_id,
      username:           context.username,
      command_name,
      subcommand:         sub_name,
      options:            sub_opts,
      role_ids:           context.role_ids,
      permission_outcome: permission_ok,
      action_outcome:     response?.data?.content?.includes("started") ? "started" : "responded",
      result_summary:     null,
    }).catch(() => {});
  }

  return response;
}
