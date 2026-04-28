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

import { supabase, hasSupabaseConfig } from "@/lib/supabase/client.js";
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
  cinematicMessage,
} from "./discord-response-helpers.js";
import {
  buildMissionStatusEmbed,
  buildLaunchPreflightEmbed,
  buildQueueCockpitEmbed,
  buildTemplateAuditEmbed,
  buildLeadInspectEmbed,
  buildHotLeadEmbed,
  buildApprovalEmbed,
  buildSuccessEmbed,
  buildTargetScanEmbed,
  buildTargetBuilderEmbed,
  buildCampaignCreatedEmbed,
  buildCampaignInspectEmbed,
  buildCampaignScaleEmbed,
  buildTerritoryMapEmbed,
  buildConquestEmbed,
  buildEmailCockpitEmbed,
  buildEmailPreviewEmbed,
  buildEmailSendTestEmbed,
  buildEmailStatsEmbed,
  buildEmailSuppressionEmbed,
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
  buildDailyBriefingEmbed,
  buildCampaignScaleApprovalEmbed,
  buildCampaignPauseAlertEmbed,
  buildOpsNotificationEmbed,
} from "./discord-embed-factory.js";
import {
  missionButtons,
  queueButtons,
  preflightButtons,
  templateAuditButtons,
  leadInspectButtons,
  approvalButtons,
  targetActionRow,
  targetBuilderMainActionRow,
  targetBuilderRunActionRow,
  marketRegionSelect,
  marketSelect,
  assetClassSelect,
  strategySelect,
  propertyTagMultiSelect,
  propertyFilterCategorySelect,
  propertyFilterValueSelect,
  campaignActionRow,
  territoryActionRow,
  emailActionRow,
  wireCockpitButtons,
  wireEventButtons,
  briefingActionRow,
  opsApprovalActionRow,
  feederCockpitButtons,
} from "./discord-components.js";
import {
  buildCampaignKey,
  normalizeMarketSlug,
  normalizeAssetType,
  normalizeStrategy,
  resolveTargetSourceViewName,
  buildNormalizedTargeting,
  isKnownMarketSlug,
  isPropertyFirstTargeting,
  normalizeSqFtRange,
  normalizeUnitsRange,
  normalizeOwnershipYearsRange,
  normalizeEstimatedValueRange,
  normalizeEquityPercentRange,
  normalizeRepairCostRange,
  normalizeBuildingCondition,
  normalizeOfferVsLoan,
  normalizeOfferVsLastPurchasePrice,
  normalizeYearBuiltRange,
  scanPropertiesForTargeting,
  getMarketRegions,
  getMarketsForRegion,
  normalizeMarketRegion,
  normalizeMarketLabel,
  resolveBuilderMarketToSystemSlug,
} from "../domain/campaigns/targeting-console.js";
import {
  listWireEvents,
  getWireSummary,
  createExpectedWire,
  markWireReceived,
  markWireCleared,
  buildWireKey,
  formatMaskedAccount,
} from "../domain/wires/wire-ledger.js";
import {
  buildWireForecast,
} from "../domain/wires/wire-forecast.js";
import {
  getDailyBriefing,
} from "../domain/kpis/daily-briefing.js";
import {
  loadApprovalRequest,
  resolveApprovalRequest,
  writeDiscordActionAudit,
} from "../domain/ops/proactive-notifications.js";
import {
  deferredPublicResponse,
  deferredEphemeralResponse,
  editOriginalInteractionResponse,
} from "./discord-followups.js";
import { info, warn, error as logError } from "../logging/logger.js";
import { getSystemFlag } from "@/lib/system-control.js";

// ---------------------------------------------------------------------------
// Test dependency injection
// ---------------------------------------------------------------------------

let _router_deps = { supabase_override: null, callInternal_override: null, editInteractionResponse_override: null };

/**
 * Override internal dependencies for unit testing.
 * @param {{ supabase_override?: object, callInternal_override?: Function, editInteractionResponse_override?: Function }} overrides
 */
export function __setActionRouterDeps(overrides) {
  _router_deps = { ..._router_deps, ...overrides };
}

/** Reset injected dependencies to production defaults. */
export function __resetActionRouterDeps() {
  _router_deps = { supabase_override: null, callInternal_override: null, editInteractionResponse_override: null };
}

/** Return the active Supabase client (real or injected mock). */
function getDb() {
  return _router_deps.supabase_override ?? supabase;
}

/** Edit an interaction response, using override if injected (for testing). */
async function doEditInteractionResponse(opts) {
  if (_router_deps.editInteractionResponse_override) {
    return _router_deps.editInteractionResponse_override(opts);
  }
  return editOriginalInteractionResponse(opts);
}

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

const FEEDER_CANDIDATE_SOURCE = "v_sms_ready_contacts";
const FEEDER_SCAN_OFFSETS = Object.freeze([
  0, 100, 200, 350, 500, 750, 1000, 1500, 2000, 2500, 3000, 3500, 4500, 5500, 6500,
]);
const FEEDER_SCAN_NEXT_OFFSETS = Object.freeze([7500, 8500, 10000, 12500, 15000]);
const FEEDER_COCKPIT_DEFAULTS = Object.freeze({
  limit: 75,
  scan_limit: 250,
  schedule_start_local: "12:30",
  schedule_end_local: "20:00",
  schedule_interval_seconds_min: 180,
});

/** Approval button custom_id prefixes. */
const APPROVE_PREFIX = "discord_approve:";
const REJECT_PREFIX  = "discord_reject:";

/**
 * Returns true when a Supabase/PostgREST error indicates the target table
 * does not exist (migration not applied or schema cache stale).
 * Never throws.
 */
function isTableMissingError(err) {
  if (!err) return false;
  const code = String(err.code ?? "");
  const msg  = String(err.message ?? "").toLowerCase();
  return (
    code === "42P01"    ||  // PostgreSQL undefined_table
    code === "PGRST116" ||  // PostgREST "not found"
    code === "PGRST204" ||  // PostgREST column/schema miss
    code === "PGRST205" ||  // PostgREST schema cache relationship miss
    msg.includes("does not exist") ||
    (msg.includes("relation") && msg.includes("not exist")) ||
    msg.includes("schema cache")
  );
}

/**
 * Immediately returns a deferred ephemeral response (type 5 + flags:64).
 * Fires the async work in a floating Promise, then edits the original
 * interaction response with the result or a sanitised error embed.
 * Never leaks raw Supabase errors, stack traces, or secrets.
 *
 * @param {object}   interaction - Raw Discord interaction object
 * @param {Function} asyncFn    - Async work; must resolve to { embeds?, components?, content? }
 * @param {string}   label      - Short name used for logging (cockpit/forecast/reconcile/deal)
 * @returns {{ type: 5, data: { flags: 64 } }}
 */
/**
 * General-purpose deferred ephemeral handler.
 * Returns type 5 immediately; fires asyncFn as a floating Promise and always
 * calls doEditInteractionResponse with the result or a sanitised error embed.
 *
 * @param {object}   interaction
 * @param {Function} asyncFn       - Must resolve to { embeds?, components?, content? }
 * @param {string}   domain        - e.g. "command", "email", "replay"
 * @param {string}   label         - e.g. "audit", "cockpit", "inbound"
 * @param {{ fallback_content?: string, fallback_embeds?: Function }} [opts]
 */
function runDeferredDiscordHandler(interaction, asyncFn, domain, label, opts = {}) {
  const app_id  = String(process.env.DISCORD_APPLICATION_ID ?? "");
  const token   = interaction.token;
  const user_id = interaction.member?.user?.id ?? "unknown";

  info(`discord.${domain}.deferred`,             { label, user_id });
  info(`discord.${domain}.${label}.started`,      { user_id });

  Promise.resolve()
    .then(async () => {
      let payload;
      try {
        payload = await asyncFn();
        info(`discord.${domain}.${label}.completed`, { user_id });
      } catch (err) {
        logError(`discord.${domain}.${label}.failed`, {});
        const fallback_embeds_fn = opts.fallback_embeds;
        if (fallback_embeds_fn) {
          payload = fallback_embeds_fn(err);
        } else {
          payload = {
            content: opts.fallback_content ?? `${label} unavailable. Check server logs.`,
            embeds:  [],
          };
        }
      }
      await doEditInteractionResponse({
        applicationId:    app_id,
        token,
        content:          payload.content    ?? "",
        embeds:           payload.embeds     ?? [],
        components:       payload.components ?? [],
        flags:            64,
        allowed_mentions: { parse: [] },
      }).catch(() => {});
    })
    .catch(() => {});

  return deferredEphemeralResponse();
}

function runDeferredWiresHandler(interaction, asyncFn, label) {
  const app_id  = String(process.env.DISCORD_APPLICATION_ID ?? "");
  const token   = interaction.token;
  const user_id = interaction.member?.user?.id ?? "unknown";

  info("discord.wires.deferred",         { label, user_id });
  info(`discord.wires.${label}.started`, { user_id });

  Promise.resolve()
    .then(async () => {
      let payload;
      try {
        payload = await asyncFn();
        info(`discord.wires.${label}.completed`, { user_id });
      } catch (err) {
        logError(`discord.wires.${label}.failed`, {});
        payload = isTableMissingError(err)
          ? { embeds: [buildWireSetupRequiredEmbed()], components: wireCockpitButtons() }
          : { content: `Wire ${label} unavailable. Check server logs.`, embeds: [] };
      }
      await doEditInteractionResponse({
        applicationId:    app_id,
        token,
        content:          payload.content    ?? "",
        embeds:           payload.embeds     ?? [],
        components:       payload.components ?? [],
        flags:            64,
        allowed_mentions: { parse: [] },
      }).catch(() => {});
    })
    .catch(() => {});

  return deferredEphemeralResponse();
}
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
  if (_router_deps.callInternal_override) {
    return _router_deps.callInternal_override(path, options);
  }
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

function sanitizeUserVisibleErrorText(raw_error) {
  const text = String(raw_error ?? "unknown error");
  const secrets = [
    process.env.INTERNAL_API_SECRET,
    process.env.CRON_SECRET,
    process.env.DISCORD_BOT_TOKEN,
  ].filter(Boolean);
  if (secrets.some((s) => text.includes(String(s)))) return "unknown error";
  return text.slice(0, 160);
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function clampInteger(value, fallback, min, max) {
  const parsed = Number(value);
  const number = Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
  return Math.min(Math.max(number, min), max);
}

function normalizeLocalTimeOption(value, fallback) {
  const raw = cleanText(value) || fallback;
  const match = raw.match(/^(\d{1,2}):([0-5]\d)$/);
  if (!match) return fallback;
  const hour = Number(match[1]);
  if (!Number.isFinite(hour) || hour < 0 || hour > 23) return fallback;
  return `${String(hour).padStart(2, "0")}:${match[2]}`;
}

function normalizeFeederCockpitParams(input = {}) {
  return {
    candidate_offset: clampInteger(input.candidate_offset ?? input.offset ?? input.o, 0, 0, 100_000),
    limit: clampInteger(input.limit ?? input.l, FEEDER_COCKPIT_DEFAULTS.limit, 1, FEEDER_HARD_CAP),
    scan_limit: clampInteger(input.scan_limit ?? input.n, FEEDER_COCKPIT_DEFAULTS.scan_limit, 1, 10_000),
    schedule_start_local: normalizeLocalTimeOption(
      input.schedule_start_local ?? input.s,
      FEEDER_COCKPIT_DEFAULTS.schedule_start_local
    ),
    schedule_end_local: normalizeLocalTimeOption(
      input.schedule_end_local ?? input.e,
      FEEDER_COCKPIT_DEFAULTS.schedule_end_local
    ),
    schedule_interval_seconds_min: clampInteger(
      input.schedule_interval_seconds_min ?? input.i,
      FEEDER_COCKPIT_DEFAULTS.schedule_interval_seconds_min,
      1,
      86_400
    ),
  };
}

function getFeederCockpitParamsFromOptions(options_array = []) {
  return normalizeFeederCockpitParams({
    candidate_offset: getOption(options_array, "candidate_offset"),
    limit: getOption(options_array, "limit"),
    scan_limit: getOption(options_array, "scan_limit"),
    schedule_start_local: getOption(options_array, "schedule_start_local"),
    schedule_end_local: getOption(options_array, "schedule_end_local"),
    schedule_interval_seconds_min: getOption(options_array, "schedule_interval_seconds_min"),
  });
}

function buildCandidateFeederBody(params = {}, { dry_run = true } = {}) {
  const p = normalizeFeederCockpitParams(params);
  return {
    dry_run: Boolean(dry_run),
    limit: p.limit,
    scan_limit: p.scan_limit,
    candidate_offset: p.candidate_offset,
    candidate_source: FEEDER_CANDIDATE_SOURCE,
    routing_safe_only: true,
    debug_templates: false,
    within_contact_window_now: false,
    schedule_spread: true,
    schedule_start_local: p.schedule_start_local,
    schedule_end_local: p.schedule_end_local,
    schedule_interval_seconds_min: p.schedule_interval_seconds_min,
  };
}

export function encodeFeederPayloadForCustomId(input = {}) {
  const p = normalizeFeederCockpitParams(input);
  const compact = {
    o: p.candidate_offset,
    l: p.limit,
    n: p.scan_limit,
    s: p.schedule_start_local,
    e: p.schedule_end_local,
    i: p.schedule_interval_seconds_min,
  };
  return Buffer.from(JSON.stringify(compact), "utf8").toString("base64url");
}

export function decodeFeederPayloadFromCustomId(token = "") {
  try {
    const parsed = JSON.parse(Buffer.from(String(token), "base64url").toString("utf8"));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    return normalizeFeederCockpitParams(parsed);
  } catch {
    return null;
  }
}

function numberFromResult(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function summarizeFeederCallResult(offset, result) {
  const data = result?.data ?? {};
  const queued_count = numberFromResult(data.queued_count ?? data.inserted_count);
  const ok = Boolean(result?.ok) && data.ok !== false && queued_count > 0;
  return {
    ok,
    candidate_offset: offset,
    queued_count,
    inserted_count: numberFromResult(data.inserted_count ?? data.queued_count),
    duplicate_queue_block_count: numberFromResult(data.duplicate_queue_block_count ?? data.duplicate_count),
    template_block_count: numberFromResult(
      data.template_block_count ??
      (numberFromResult(data.no_template_count) + numberFromResult(data.template_render_failed_count))
    ),
    schedule_window_full_count: numberFromResult(data.schedule_window_full_count),
    schedule_overflow_blocked_count: numberFromResult(data.schedule_overflow_blocked_count),
    skipped_count: numberFromResult(data.skipped_count),
    scanned_count: numberFromResult(data.scanned_count ?? data.loaded_count),
    eligible_count: numberFromResult(data.eligible_count),
    first_scheduled_for: data.first_scheduled_for || null,
    last_scheduled_for: data.last_scheduled_for || null,
    error: result?.ok ? null : sanitizeUserVisibleErrorText(result?.error),
  };
}

export function rankFeederScanBands(bands = []) {
  return [...bands]
    .filter((band) => band?.ok && numberFromResult(band.queued_count) > 0)
    .sort((left, right) =>
      numberFromResult(right.queued_count) - numberFromResult(left.queued_count) ||
      numberFromResult(left.duplicate_queue_block_count) - numberFromResult(right.duplicate_queue_block_count) ||
      numberFromResult(left.template_block_count) - numberFromResult(right.template_block_count) ||
      numberFromResult(left.schedule_window_full_count) - numberFromResult(right.schedule_window_full_count) ||
      numberFromResult(left.schedule_overflow_blocked_count) - numberFromResult(right.schedule_overflow_blocked_count) ||
      numberFromResult(left.candidate_offset) - numberFromResult(right.candidate_offset)
    );
}

function formatObjectCounts(value = {}) {
  if (!value || typeof value !== "object") return "—";
  const lines = Object.entries(value)
    .filter(([, count]) => Number(count) > 0)
    .sort(([a], [b]) => a.localeCompare(b))
    .slice(0, 8)
    .map(([key, count]) => `${key}: **${count}**`);
  return lines.join("\n") || "—";
}

function formatSkipReasons(skips = []) {
  if (!Array.isArray(skips) || skips.length === 0) return "—";
  const counts = {};
  for (const skip of skips.slice(0, 20)) {
    const key = cleanText(skip?.reason_code || skip?.reason || "unknown").slice(0, 64) || "unknown";
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.entries(counts)
    .map(([key, count]) => `${key}: **${count}**`)
    .join("\n")
    .slice(0, 1024) || "—";
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
// Command: /mission status
// ---------------------------------------------------------------------------

async function handleMissionStatus(context) {
  const db = getDb();

  // Queue counts ─────────────────────────────────────────────────────────────
  let queue_counts = {};
  try {
    const { data } = await db.from("send_queue").select("queue_status").limit(10000);
    for (const row of data ?? []) {
      const s = String(row.queue_status ?? "unknown");
      queue_counts[s] = (queue_counts[s] ?? 0) + 1;
    }
  } catch { /* optional table — fail soft */ }

  // Recent events last 24 h ──────────────────────────────────────────────────
  let recent_events = null;
  try {
    const since = new Date(Date.now() - 86_400_000).toISOString();
    const { count } = await db
      .from("message_events")
      .select("id", { count: "exact", head: true })
      .gte("created_at", since);
    recent_events = count ?? null;
  } catch { /* optional */ }

  // Failed Podio syncs ───────────────────────────────────────────────────────
  let failed_syncs = null;
  try {
    const { count } = await db
      .from("message_events")
      .select("id", { count: "exact", head: true })
      .eq("podio_sync_status", "failed");
    failed_syncs = count ?? null;
  } catch { /* optional */ }

  // Template counts from sms_templates ──────────────────────────────────────
  let active_templates = null;
  let stage1_templates = null;
  try {
    const { count: a } = await db
      .from("sms_templates")
      .select("id", { count: "exact", head: true })
      .eq("is_active", true);
    active_templates = a ?? null;

    const { count: s } = await db
      .from("sms_templates")
      .select("id", { count: "exact", head: true })
      .eq("is_active", true)
      .or("use_case.eq.ownership_check,stage_code.eq.S1,is_first_touch.eq.true");
    stage1_templates = s ?? null;
  } catch { /* optional */ }

  // Config checks ────────────────────────────────────────────────────────────
  const supabase_ok  = hasSupabaseConfig();
  const textgrid_ok  = Boolean(
    String(process.env.TEXTGRID_ACCOUNT_SID  ?? "").trim() &&
    String(process.env.TEXTGRID_AUTH_TOKEN   ?? "").trim()
  );
  const podio_ok = Boolean(String(process.env.PODIO_CLIENT_ID ?? "").trim());

  const overall_status =
    !supabase_ok || !textgrid_ok || !podio_ok ? "warning" :
    (queue_counts.failed ?? 0) > 0            ? "warning" :
    "healthy";

  const embed = buildMissionStatusEmbed({
    overall_status,
    queue_counts,
    active_templates,
    stage1_templates,
    recent_events,
    failed_syncs,
    supabase_ok,
    podio_ok,
    textgrid_ok,
  });

  return cinematicMessage({ embeds: [embed], components: missionButtons() });
}

// ---------------------------------------------------------------------------
// Command: /launch preflight
// ---------------------------------------------------------------------------

async function handleLaunchPreflight(context) {
  const db = getDb();
  const checks = [];

  // Helper to push a check result.
  const check = (name, pass, detail) =>
    checks.push({ name, status: pass ? "pass" : "fail", detail: String(detail ?? "") });

  // Supabase ─────────────────────────────────────────────────────────────────
  const sb_ok = hasSupabaseConfig();
  check("Supabase configured", sb_ok, sb_ok ? "Connected" : "SUPABASE_URL or key missing");

  // TextGrid ─────────────────────────────────────────────────────────────────
  const tg_ok = Boolean(
    String(process.env.TEXTGRID_ACCOUNT_SID ?? "").trim() &&
    String(process.env.TEXTGRID_AUTH_TOKEN  ?? "").trim()
  );
  check("TextGrid configured", tg_ok, tg_ok ? "Credentials present" : "TEXTGRID_ACCOUNT_SID or AUTH_TOKEN missing");

  // Podio ────────────────────────────────────────────────────────────────────
  const podio_ok = Boolean(String(process.env.PODIO_CLIENT_ID ?? "").trim());
  check("Podio configured", podio_ok, podio_ok ? "Credentials present" : "PODIO_CLIENT_ID missing");

  // Template readiness ───────────────────────────────────────────────────────
  let active_tpl = 0;
  let stage1_tpl = 0;
  try {
    const { count: a } = await db
      .from("sms_templates")
      .select("id", { count: "exact", head: true })
      .eq("is_active", true);
    active_tpl = a ?? 0;

    const { count: s } = await db
      .from("sms_templates")
      .select("id", { count: "exact", head: true })
      .eq("is_active", true)
      .or("use_case.eq.ownership_check,stage_code.eq.S1,is_first_touch.eq.true");
    stage1_tpl = s ?? 0;
  } catch { /* Supabase unavailable */ }

  check("Active templates > 0",  active_tpl > 0,  `${active_tpl} active templates`);
  check("Stage 1 templates > 0", stage1_tpl > 0,  `${stage1_tpl} Stage 1 templates`);

  // Queue health ─────────────────────────────────────────────────────────────
  let q_due = 0;
  let q_sending = 0;
  let q_failed = 0;
  try {
    const { data } = await db.from("send_queue").select("queue_status").limit(10000);
    for (const row of data ?? []) {
      const s = String(row.queue_status ?? "");
      if (s === "queued")   q_due++;
      if (s === "sending")  q_sending++;
      if (s === "failed")   q_failed++;
    }
  } catch { /* optional */ }

  const q_ok = q_failed === 0;
  checks.push({
    name:   "Queue health",
    status: q_failed > 0 ? "warn" : "pass",
    detail: `Due: ${q_due}  |  Sending: ${q_sending}  |  Failed: ${q_failed}`,
  });

  // Overall ─────────────────────────────────────────────────────────────────
  const has_fail = checks.some((c) => c.status === "fail");
  const has_warn = checks.some((c) => c.status === "warn");
  const overall_status = has_fail ? "HOLD" : has_warn ? "WARN" : "GO";

  const embed = buildLaunchPreflightEmbed({ overall_status, checks });

  return cinematicMessage({ embeds: [embed], components: preflightButtons() });
}

// ---------------------------------------------------------------------------
// Command: /queue cockpit
// ---------------------------------------------------------------------------

async function handleQueueCockpit(context) {
  const db = getDb();

  try {
    const { data } = await db.from("send_queue").select("queue_status, scheduled_at").limit(10000);
    const rows = data ?? [];

    const counts = {};
    let due_now = 0;
    let future  = 0;
    const now_ts = Date.now();

    for (const row of rows) {
      const s = String(row.queue_status ?? "unknown");
      counts[s] = (counts[s] ?? 0) + 1;

      if (s === "queued") {
        const sched = row.scheduled_at ? new Date(row.scheduled_at).getTime() : 0;
        if (!sched || sched <= now_ts) due_now++;
        else future++;
      }
    }

    // Rows stuck in "sending" for > 10 min (heuristic; no timestamp available in select).
    const stuck_sending = null; // would require a created_at/updated_at join

    const embed = buildQueueCockpitEmbed({ counts, due_now, future, stuck_sending });
    return cinematicMessage({ embeds: [embed], components: queueButtons() });
  } catch {
    return errorResponse("Failed to read queue data.");
  }
}

// ---------------------------------------------------------------------------
// Command: /templates audit
// ---------------------------------------------------------------------------

async function handleTemplatesAudit(context) {
  const db = getDb();

  try {
    // Paginate to load all rows (Supabase default page size is 1000).
    const PAGE_SIZE = 1000;
    const all_rows = [];
    let page = 0;

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const from = page * PAGE_SIZE;
      const to   = from + PAGE_SIZE - 1;
      const { data: page_data, error: page_error } = await db
        .from("sms_templates")
        .select("*")
        .range(from, to);
      if (page_error) throw page_error;
      const chunk = page_data ?? [];
      all_rows.push(...chunk);
      if (chunk.length < PAGE_SIZE) break;
      page++;
    }

    const rows = all_rows;
    const total    = rows.length;
    const active   = rows.filter((r) => r.is_active).length;
    const inactive = total - active;

    const by_language   = {};
    const by_use_case   = {};
    const by_stage_code = {};
    let active_first_touch     = 0;
    let active_ownership_check = 0;
    let missing_template_body  = 0;
    let missing_language       = 0;
    let missing_use_case       = 0;
    let missing_stage_code     = 0;
    const blockers = [];

    for (const r of rows) {
      const lang  = String(r.language    ?? "").trim() || null;
      const uc    = String(r.use_case    ?? "").trim() || null;
      const sc    = String(r.stage_code  ?? "").trim() || null;
      const body  = String(r.template_body ?? "").replace(/<[^>]*>/g, "").trim();

      if (lang)  by_language[lang]   = (by_language[lang]   ?? 0) + 1;
      if (uc)    by_use_case[uc]     = (by_use_case[uc]     ?? 0) + 1;
      if (sc)    by_stage_code[sc]   = (by_stage_code[sc]   ?? 0) + 1;

      if (r.is_active) {
        if (r.is_first_touch || sc === "S1" || uc === "ownership_check") {
          active_first_touch++;
        }
        if (uc === "ownership_check") active_ownership_check++;
      }

      if (!body)  { missing_template_body++; if (r.is_active) blockers.push(`id=${r.id}: empty template_body`); }
      if (!lang)  { missing_language++; }
      if (!uc)    { missing_use_case++;   if (r.is_active) blockers.push(`id=${r.id}: missing use_case`);   }
      if (!sc)    { missing_stage_code++; }
    }

    const embed = buildTemplateAuditEmbed({
      total, active, inactive,
      by_language, by_use_case, by_stage_code,
      active_first_touch, active_ownership_check,
      missing_template_body, missing_language, missing_use_case, missing_stage_code,
      blockers,
    });

    return cinematicMessage({ embeds: [embed], components: templateAuditButtons() });
  } catch {
    return errorResponse("Failed to read sms_templates.");
  }
}

// ---------------------------------------------------------------------------
// Command: /templates stage1
// ---------------------------------------------------------------------------

async function handleTemplatesStage1(context) {
  const db = getDb();

  try {
    const { data, error } = await db
      .from("sms_templates")
      .select("id, template_id, use_case, stage_code, stage_label, is_first_touch, language, is_active, template_name, metadata")
      .eq("is_active", true)
      .or("use_case.eq.ownership_check,stage_code.eq.S1,is_first_touch.eq.true");

    if (error) throw error;

    const rows = data ?? [];

    // Also check stage_label / metadata for additional Stage 1 signals.
    const is_stage1 = (r) => {
      if (r.is_first_touch)              return true;
      if (r.stage_code === "S1")         return true;
      if (r.use_case === "ownership_check") return true;
      const sl = String(r.stage_label ?? "").toLowerCase();
      if (sl.includes("stage 1") || sl.includes("ownership") || sl.includes("initial")) return true;
      const ms = JSON.stringify(r.metadata ?? {}).toLowerCase();
      if (ms.includes("ownership") || ms.includes("first touch")) return true;
      return false;
    };

    const stage1_rows = rows.filter(is_stage1);

    const by_language = {};
    for (const r of stage1_rows) {
      const lang = String(r.language ?? "unknown");
      by_language[lang] = (by_language[lang] ?? 0) + 1;
    }

    const samples = stage1_rows.slice(0, 5).map((r) => {
      const name = r.template_name || r.template_id || r.id;
      const lang = r.language ?? "—";
      const uc   = r.use_case ?? "—";
      return `• ${String(name).slice(0, 40)} [${lang} / ${uc}]`;
    });

    const lang_lines = Object.entries(by_language)
      .map(([k, v]) => `${k}: **${v}**`)
      .join("  |  ") || "—";

    return cinematicMessage({
      embeds: [buildSuccessEmbed({
        title:       "Stage 1 Active Templates",
        description: `**${stage1_rows.length}** active Stage 1 candidates found.\n\nBy language: ${lang_lines}`,
        fields: samples.length > 0
          ? [{ name: "Sample templates", value: samples.join("\n").slice(0, 1024), inline: false }]
          : [],
      })],
    });
  } catch {
    return errorResponse("Failed to read sms_templates.");
  }
}

// ---------------------------------------------------------------------------
// Command: /feeder scan  (deferred — dry_run=true)
// ---------------------------------------------------------------------------

async function handleFeederScan({ options_array, context, interaction }) {
  const limit      = Math.min(Math.max(1, Number(getOption(options_array, "limit")) || 50), 200);
  const scan_limit = Math.max(1, Number(getOption(options_array, "scan_limit")) || 500);

  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/outbound/feed-master-owners", {
      method:     "POST",
      body:       { limit, scan_limit, dry_run: true },
      timeout_ms: 25_000,
    });

    if (result.timed_out) {
      return { content: `Feeder scan (limit=${limit}) is still running — check alerts channel.` };
    }
    if (!result.ok) {
      return { content: `Feeder scan error: ${result.error ?? "unknown"}.` };
    }
    const d = safeResultSummary(result.data, [
      "eligible_count", "skipped_count", "loaded_count",
      "total_scanned", "error_count",
    ]);
    return {
      embeds: [buildSuccessEmbed({
        title:       `Feeder Scan — dry_run (limit=${limit})`,
        description: [
          `Eligible:  **${d.eligible_count  ?? "—"}**`,
          `Skipped:   **${d.skipped_count   ?? "—"}**`,
          `Scanned:   **${d.total_scanned ?? d.loaded_count ?? "—"}**`,
        ].join("  |  "),
      })],
    };
  }, "command", "feeder_scan", {
    fallback_content: `Feeder scan encountered an error. Check server logs.`,
  });
}

// ---------------------------------------------------------------------------
// Command: /feeder launch  (approval-gated for limit > 25)
// ---------------------------------------------------------------------------

async function handleFeederLaunch({ options_array, context, interaction }) {
  const limit      = Math.min(Math.max(1, Number(getOption(options_array, "limit")) || 10), FEEDER_HARD_CAP);
  const scan_limit = Math.max(1, Number(getOption(options_array, "scan_limit")) || 500);
  const role_ids   = context.role_ids;

  // High-limit runs require Owner approval.
  if (limit > FEEDER_TECH_OPS_LIMIT && !isOwner(role_ids)) {
    if (!isTechOps(role_ids)) {
      return deniedResponse("Requires **Tech Ops** or **Owner** to launch the feeder.");
    }

    const approval_token = crypto.randomUUID();
    const action_payload = { command: "feeder_launch", limit, scan_limit, dry_run: false };

    await auditLog({
      interaction_id:    interaction.id,
      guild_id:          context.guild_id,
      channel_id:        context.channel_id,
      user_id:           context.user_id,
      username:          context.username,
      command_name:      "feeder",
      subcommand:        "launch",
      options:           action_payload,
      role_ids:          context.role_ids,
      permission_outcome: "allowed",
      action_outcome:    "approval_pending",
      approval_token,
    });

    return cinematicMessage({
      embeds: [buildApprovalEmbed({
        action:    `Feeder launch (limit=${limit})`,
        requester: context.username || `<@${context.user_id}>`,
        details:   `limit=${limit}, scan_limit=${scan_limit}, dry_run=false`,
      })],
      components: approvalButtons({
        actionId:     approval_token,
        approveLabel: `Launch (${limit})`,
        denyLabel:    "Cancel",
      }),
    });
  }

  // Owner or limit ≤ 25 — execute with deferred response.
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/outbound/feed-master-owners", {
      method:     "POST",
      body:       { limit, scan_limit, dry_run: false },
      timeout_ms: 30_000,
    });

    if (result.timed_out) {
      return { content: `Feeder launch (limit=${limit}) is running — check alerts channel.` };
    }
    if (!result.ok) {
      return { content: `Feeder launch error: ${result.error ?? "unknown"}.` };
    }
    const d = safeResultSummary(result.data, [
      "eligible_count", "inserted_count",
      "duplicate_count", "skipped_count", "loaded_count",
    ]);
    return {
      embeds: [buildSuccessEmbed({
        title:       `Feeder Launch — Complete (limit=${limit})`,
        description: [
          `Inserted:  **${d.inserted_count  ?? "—"}**`,
          `Eligible:  **${d.eligible_count  ?? "—"}**`,
          `Skipped:   **${d.skipped_count   ?? "—"}**`,
          `Dupes:     **${d.duplicate_count ?? "—"}**`,
        ].join("  |  "),
      })],
    };
  }, "command", "feeder_launch", {
    fallback_content: `Feeder launch encountered an error. Check server logs.`,
  });
}

async function runCandidateFeederDryRunBand(offset, params) {
  const body = buildCandidateFeederBody({ ...params, candidate_offset: offset }, { dry_run: true });
  const result = await callInternal("/api/internal/outbound/feed-candidates", {
    method: "POST",
    body,
    timeout_ms: 30_000,
  });
  return summarizeFeederCallResult(offset, result);
}

function buildFeederAutoScanPayload({ bands = [], ranked = [], params = {}, title = "SMS Feeder Auto Scan" } = {}) {
  const top = ranked.slice(0, 5);
  const rejected_count = Math.max(0, bands.length - ranked.length);
  const lines = top.map((band, index) => {
    return [
      `**${index + 1}. Offset ${band.candidate_offset}**`,
      `queue **${band.queued_count}**`,
      `dupes ${band.duplicate_queue_block_count}`,
      `templates ${band.template_block_count}`,
      `window ${band.schedule_window_full_count}`,
      `overflow ${band.schedule_overflow_blocked_count}`,
    ].join(" | ");
  });

  const best = top[0] || null;
  const launch_payload = best
    ? encodeFeederPayloadForCustomId({ ...params, candidate_offset: best.candidate_offset })
    : "";

  return {
    embeds: [buildSuccessEmbed({
      title,
      description: best
        ? [
            `Best clean band: **offset ${best.candidate_offset}** with **${best.queued_count}** queueable rows.`,
            `Window: **${params.schedule_start_local}–${params.schedule_end_local}** local, interval **${params.schedule_interval_seconds_min}s**.`,
            rejected_count ? `Rejected dry-run bands: **${rejected_count}**.` : "No rejected dry-run bands.",
          ].join("\n")
        : [
            "No clean offset band found.",
            "Auto scan is dry-run only. Try Scan Next or adjust the window/limit.",
            rejected_count ? `Rejected dry-run bands: **${rejected_count}**.` : "",
          ].filter(Boolean).join("\n"),
      fields: [
        {
          name: "Top Offset Bands",
          value: lines.join("\n").slice(0, 1024) || "No dry-run band had queued_count > 0.",
          inline: false,
        },
      ],
    })],
    components: feederCockpitButtons({
      includeLaunch: Boolean(best && launch_payload.length < 80),
      launchPayload: launch_payload,
      dryLaunchPayload: launch_payload,
    }),
  };
}

async function runFeederAutoScan({ params = {}, offsets = FEEDER_SCAN_OFFSETS, title } = {}) {
  const normalized = normalizeFeederCockpitParams(params);
  const bands = await Promise.all(offsets.map((offset) => runCandidateFeederDryRunBand(offset, normalized)));
  const ranked = rankFeederScanBands(bands);
  return buildFeederAutoScanPayload({ bands, ranked, params: normalized, title });
}

async function handleFeederAutoScan({ options_array = [], interaction, offsets = FEEDER_SCAN_OFFSETS, title }) {
  const params = getFeederCockpitParamsFromOptions(options_array);
  return runDeferredDiscordHandler(interaction, async () => {
    return runFeederAutoScan({ params, offsets, title });
  }, "command", "feeder_auto_scan", {
    fallback_content: "Feeder auto scan unavailable. Check server logs.",
  });
}

async function handleFeederLaunchPreview({ options_array = [], interaction }) {
  const params = getFeederCockpitParamsFromOptions(options_array);
  return runDeferredDiscordHandler(interaction, async () => {
    const body = buildCandidateFeederBody(params, { dry_run: true });
    const result = await callInternal("/api/internal/outbound/feed-candidates", {
      method: "POST",
      body,
      timeout_ms: 30_000,
    });
    if (!result.ok) {
      return { content: `Feeder dry launch preview failed: ${sanitizeUserVisibleErrorText(result.error)}.` };
    }
    const data = result.data ?? {};
    const payload = encodeFeederPayloadForCustomId(params);
    return {
      embeds: [buildSuccessEmbed({
        title: "Feeder Launch Preview — Dry Run",
        description: [
          `Offset: **${params.candidate_offset}**`,
          `Would queue: **${data.queued_count ?? data.inserted_count ?? 0}**`,
          `Scanned: **${data.scanned_count ?? data.loaded_count ?? "—"}**`,
          `Skipped: **${data.skipped_count ?? "—"}**`,
          "",
          "Use the **LIVE LAUNCH** button to enqueue this exact batch.",
        ].join("\n"),
        fields: [
          {
            name: "Schedule",
            value: `${params.schedule_start_local}–${params.schedule_end_local} local, every ${params.schedule_interval_seconds_min}s`,
            inline: false,
          },
        ],
      })],
      components: feederCockpitButtons({
        includeLaunch: payload.length < 80,
        launchPayload: payload,
        dryLaunchPayload: payload,
      }),
    };
  }, "command", "feeder_launch_preview", {
    fallback_content: "Feeder launch preview unavailable. Check server logs.",
  });
}

function buildFeederLaunchResultPayload(data = {}, params = {}, { dry_run = false } = {}) {
  return {
    embeds: [buildSuccessEmbed({
      title: dry_run ? "Feeder Dry Launch Complete" : "Feeder LIVE LAUNCH Complete",
      description: [
        `Queued: **${data.queued_count ?? data.inserted_count ?? 0}**`,
        `Inserted: **${data.inserted_count ?? data.queued_count ?? 0}**`,
        `Offset: **${params.candidate_offset}**`,
        `First scheduled: **${data.first_scheduled_for || "—"}**`,
        `Last scheduled: **${data.last_scheduled_for || "—"}**`,
      ].join("\n"),
      fields: [
        {
          name: "TextGrid Markets",
          value: formatObjectCounts(data.selected_textgrid_market_counts),
          inline: true,
        },
        {
          name: "Routing Tiers",
          value: formatObjectCounts(data.routing_tier_counts),
          inline: true,
        },
        {
          name: "Sample Skip Reasons",
          value: formatSkipReasons(data.sample_skips),
          inline: false,
        },
      ],
    })],
    components: feederCockpitButtons(),
  };
}

async function handleFeederLaunchButton({ interaction, payload_token, dry_run = false }) {
  const params = decodeFeederPayloadFromCustomId(payload_token);
  if (!params) {
    return ephemeralMessage("Invalid feeder launch payload. Run a fresh auto scan.");
  }

  return runDeferredDiscordHandler(interaction, async () => {
    const body = buildCandidateFeederBody(params, { dry_run });
    const result = await callInternal("/api/internal/outbound/feed-candidates", {
      method: "POST",
      body,
      timeout_ms: 60_000,
    });
    if (!result.ok) {
      return { content: `Feeder launch failed: ${sanitizeUserVisibleErrorText(result.error)}.` };
    }
    return buildFeederLaunchResultPayload(result.data ?? {}, params, { dry_run });
  }, "button", dry_run ? "feeder_dry_launch" : "feeder_live_launch", {
    fallback_content: "Feeder launch unavailable. Check server logs.",
  });
}

function getQueueScheduledAt(row = null) {
  return (
    cleanText(row?.scheduled_for) ||
    cleanText(row?.scheduled_for_utc) ||
    cleanText(row?.scheduled_for_local) ||
    cleanText(row?.scheduled_at) ||
    null
  );
}

function normalizeQueueStatusLabel(value) {
  return cleanText(value || "unknown").toLowerCase();
}

async function buildFeederQueueStatusPayload() {
  const db = getDb();
  const { data, error } = await db
    .from("send_queue")
    .select("queue_status,scheduled_for,scheduled_for_utc,scheduled_for_local,scheduled_at,created_at,sent_at")
    .limit(10000);
  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const counts = {};
  const now_ms = Date.now();
  const today_key = new Date().toISOString().slice(0, 10);
  let first_future = null;
  let last_future = null;
  let next_due = null;
  let today_sent_count = 0;

  for (const row of rows) {
    const status = normalizeQueueStatusLabel(row.queue_status);
    counts[status] = (counts[status] || 0) + 1;
    const scheduled = getQueueScheduledAt(row);
    const scheduled_ms = scheduled ? new Date(scheduled).getTime() : null;
    const sent_at = cleanText(row.sent_at || row.created_at);

    if (status === "sent" && sent_at.slice(0, 10) === today_key) today_sent_count += 1;

    if (scheduled_ms && Number.isFinite(scheduled_ms) && scheduled_ms > now_ms) {
      if (!first_future || scheduled_ms < first_future.ms) first_future = { ms: scheduled_ms, iso: scheduled };
      if (!last_future || scheduled_ms > last_future.ms) last_future = { ms: scheduled_ms, iso: scheduled };
    }

    if (status === "queued" && (!scheduled_ms || scheduled_ms <= now_ms)) {
      const due_ms = scheduled_ms || new Date(row.created_at || 0).getTime() || 0;
      if (!next_due || due_ms < next_due.ms) next_due = { ms: due_ms, iso: scheduled || row.created_at || "due now" };
    }
  }

  const status_lines = [
    ["scheduled", counts.scheduled || 0],
    ["queued", counts.queued || 0],
    ["sending", counts.sending || 0],
    ["sent", counts.sent || 0],
    ["failed", counts.failed || 0],
    ["blocked", counts.blocked || 0],
    ["cancelled", counts.cancelled || counts.canceled || 0],
  ].map(([label, count]) => `${label}: **${count}**`);

  return {
    embeds: [buildSuccessEmbed({
      title: "SMS Feeder Queue Status",
      description: status_lines.join(" | "),
      fields: [
        {
          name: "Future Schedule",
          value: [
            `First future row: **${first_future?.iso || "—"}**`,
            `Last future row: **${last_future?.iso || "—"}**`,
          ].join("\n"),
          inline: false,
        },
        {
          name: "Due / Sent",
          value: [
            `Next due row: **${next_due?.iso || "—"}**`,
            `Today sent count: **${today_sent_count}**`,
          ].join("\n"),
          inline: false,
        },
      ],
    })],
    components: feederCockpitButtons(),
  };
}

async function handleFeederQueueStatus() {
  try {
    return cinematicMessage(await buildFeederQueueStatusPayload());
  } catch {
    return errorResponse("Failed to read feeder queue status.");
  }
}

async function handleFeederQueueRun({ interaction, dry_run = true, limit = 25 }) {
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/queue/run", {
      method: "POST",
      body: { limit, dry_run },
      timeout_ms: 30_000,
    });
    if (!result.ok) {
      return { content: `Queue run failed: ${sanitizeUserVisibleErrorText(result.error)}.` };
    }
    const run = result.data?.result ?? result.data ?? {};
    return {
      embeds: [buildSuccessEmbed({
        title: dry_run ? "Queue Run — Dry" : "Queue Run — Live",
        description: [
          `Processed: **${run.processed_count ?? run.started_count ?? "—"}**`,
          `Sent: **${run.sent_count ?? "—"}**`,
          `Failed: **${run.failed_count ?? "—"}**`,
          `Skipped: **${run.skipped_count ?? "—"}**`,
          `Future rows: **${run.future_rows ?? "—"}**`,
        ].join("\n"),
      })],
      components: feederCockpitButtons(),
    };
  }, "command", dry_run ? "queue_run_dry" : "queue_run_live", {
    fallback_content: "Queue run unavailable. Check server logs.",
  });
}

// ---------------------------------------------------------------------------
// Command: /lead inspect <phone_or_owner_id>
// ---------------------------------------------------------------------------

async function handleLeadInspect({ options_array, context }) {
  const query = String(getOption(options_array, "phone_or_owner_id") ?? "").trim();
  if (!query) return errorResponse("phone_or_owner_id is required.");

  const db = getDb();

  try {
    const is_numeric = /^\d+$/.test(query);

    let builder = db
      .from("message_events")
      .select("event_type, direction, delivery_status, created_at")
      .order("created_at", { ascending: false })
      .limit(200);

    if (is_numeric) {
      builder = builder.eq("master_owner_id", Number(query));
    } else {
      builder = builder.or(
        `metadata->>to_phone.eq.${query},metadata->>from_phone.eq.${query}`
      );
    }

    const { data, error } = await builder;
    if (error) throw error;

    const rows = data ?? [];
    if (rows.length === 0) {
      return cinematicMessage({
        embeds: [buildLeadInspectEmbed({ query, events_count: 0 })],
        ephemeral: true,
      });
    }

    const by_direction = { inbound: 0, outbound: 0 };
    for (const r of rows) {
      const d = String(r.direction ?? "").toLowerCase();
      if (d === "inbound")  by_direction.inbound++;
      if (d === "outbound") by_direction.outbound++;
    }

    const most_recent = rows[0]?.created_at
      ? new Date(rows[0].created_at).toISOString().slice(0, 10)
      : null;

    const latest_status = String(rows[0]?.delivery_status ?? rows[0]?.event_type ?? "").slice(0, 40) || null;

    return cinematicMessage({
      embeds: [buildLeadInspectEmbed({
        query,
        events_count: rows.length,
        by_direction,
        most_recent,
        lead_status: latest_status,
      })],
      components: leadInspectButtons({ ownerId: is_numeric ? query : "", phone: is_numeric ? "" : query }),
      ephemeral: true,
    });
  } catch {
    return errorResponse("Failed to fetch lead records.");
  }
}

// ---------------------------------------------------------------------------
// Command: /hotleads
// ---------------------------------------------------------------------------

async function handleHotleads({ options_array, context }) {
  const limit = Math.min(Math.max(1, Number(getOption(options_array, "limit")) || 10), 25);
  const db = getDb();

  try {
    const { data, error } = await db
      .from("message_events")
      .select("id, direction, created_at, podio_sync_status, metadata")
      .eq("direction", "inbound")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;

    const rows = data ?? [];

    const events = rows.map((r) => {
      const meta = r.metadata ?? {};
      return {
        phone:        String(meta.from_phone ?? meta.phone ?? "unknown").slice(0, 20),
        body_preview: String(meta.body ?? meta.message_body ?? "").slice(0, 80),
        created_at:   r.created_at ?? null,
        podio_synced: r.podio_sync_status === "synced" || r.podio_sync_status === "ok",
      };
    });

    return cinematicMessage({
      embeds: [buildHotLeadEmbed({ events, total: rows.length })],
    });
  } catch {
    return errorResponse("Failed to fetch inbound events.");
  }
}

// ---------------------------------------------------------------------------
// Command: /alerts mode
// ---------------------------------------------------------------------------

async function handleAlertsMode({ options_array, context }) {
  const mode = String(getOption(options_array, "mode") ?? "").trim().toLowerCase();
  if (!mode) {
    // No mode provided — show current mode and a selector.
    const db = getDb();
    let current_mode = "normal";
    try {
      const { data } = await db
        .from("app_config")
        .select("value")
        .eq("key", "alert_mode")
        .limit(1)
        .maybeSingle();
      if (data?.value) current_mode = String(data.value);
    } catch { /* non-fatal */ }
    return cinematicMessage({
      embeds: [{
        title:       "Alerts Mode",
        description: `Current mode: **${current_mode}**\n\nSelect a mode using the buttons below or \`/alerts mode:{normal|quiet|verbose}\`.`,
        color:       0x5865F2,
      }],
      components: [
        {
          type: 1,
          components: [
            { type: 2, style: 1, label: "Normal",  custom_id: "alerts:mode:normal",  disabled: false },
            { type: 2, style: 2, label: "Quiet",   custom_id: "alerts:mode:quiet",   disabled: false },
            { type: 2, style: 2, label: "Verbose", custom_id: "alerts:mode:verbose", disabled: false },
          ],
        },
      ],
      ephemeral: true,
    });
  }

  const db = getDb();

  // Attempt to read/write app_config table.  If it does not exist, return
  // not_configured rather than inventing persistence.
  try {
    const { data, error } = await db
      .from("app_config")
      .select("key")
      .eq("key", "alert_mode")
      .limit(1)
      .maybeSingle();

    // If Supabase returned a "relation does not exist" error, treat as not configured.
    if (error) {
      return cinematicMessage({
        embeds: [{
          title:       "Alerts Mode — Not Configured",
          description: "No `app_config` table found. Alert mode persistence is not available.",
          color:       0x95A5A6,
        }],
        ephemeral: true,
      });
    }

    // Table exists — upsert the alert_mode key.
    await db.from("app_config").upsert({
      key:        "alert_mode",
      value:      mode,
      updated_at: new Date().toISOString(),
      updated_by: context.username || context.user_id || "discord",
    });

    return cinematicMessage({
      embeds: [buildSuccessEmbed({
        title:       "Alerts Mode Updated",
        description: `Alert mode set to **${mode}**.`,
      })],
      ephemeral: true,
    });
  } catch {
    return errorResponse("Failed to update alert mode.");
  }
}

// ---------------------------------------------------------------------------
// Command: /target-build (interactive builder)
// ---------------------------------------------------------------------------

function createDefaultBuilderState() {
  return {
    scan_mode: "property_first",
    market_region: null,
    market: null,
    asset_class: null,
    strategy: null,
    property_tags: [],
    filters: {},
    limits: {
      max_scan_count: 100,
      target_eligible_count: 10,
    },
  };
}

function makeBuilderSessionKey() {
  return `tb_${crypto.randomBytes(8).toString("hex")}`;
}

async function createTargetBuilderSession({ context, interaction }) {
  const db = getDb();
  const session_key = makeBuilderSessionKey();
  const state = createDefaultBuilderState();

  const row = {
    session_key,
    discord_user_id: context.user_id,
    guild_id: context.guild_id,
    channel_id: context.channel_id,
    message_id: null,
    state,
    status: "active",
  };

  const { data, error } = await db
    .from("discord_targeting_sessions")
    .insert(row)
    .select("*")
    .maybeSingle();

  if (error) throw error;
  return data ?? row;
}

async function getTargetBuilderSession(session_key) {
  const db = getDb();
  const { data, error } = await db
    .from("discord_targeting_sessions")
    .select("*")
    .eq("session_key", session_key)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function updateTargetBuilderSessionState(session_key, next_state, next_status = null) {
  const db = getDb();
  const update = {
    state: next_state,
    updated_at: new Date().toISOString(),
  };
  if (next_status) update.status = next_status;

  const { data, error } = await db
    .from("discord_targeting_sessions")
    .update(update)
    .eq("session_key", session_key)
    .select("*")
    .maybeSingle();

  if (error) throw error;
  return data;
}

function buildBuilderComponents({ session_key, mode = "main", region = null, filter_category = null }) {
  const components = [];

  if (mode === "market_region") {
    components.push(marketRegionSelect(session_key));
  } else if (mode === "market") {
    components.push(marketSelect(session_key, region, getMarketsForRegion(region)));
  } else if (mode === "asset") {
    components.push(assetClassSelect(session_key));
  } else if (mode === "strategy") {
    components.push(strategySelect(session_key));
  } else if (mode === "tags") {
    components.push(propertyTagMultiSelect(session_key));
  } else if (mode === "filter_category") {
    components.push(propertyFilterCategorySelect(session_key));
  } else if (mode === "filter_values") {
    components.push(propertyFilterValueSelect(session_key, filter_category));
  }

  components.push(targetBuilderMainActionRow(session_key));
  components.push(targetBuilderRunActionRow(session_key));
  return components;
}

async function handleTargetBuild({ context, interaction }) {
  try {
    const session = await createTargetBuilderSession({ context, interaction });
    const embed = buildTargetBuilderEmbed(session.state, {});
    return cinematicMessage({
      embeds: [embed],
      components: buildBuilderComponents({ session_key: session.session_key }),
      ephemeral: true,
    });
  } catch {
    return errorResponse("Failed to create targeting builder session.");
  }
}

function applyFilterSelection(state, packed) {
  const next = { ...(state ?? createDefaultBuilderState()) };
  next.filters = { ...(next.filters ?? {}) };

  const [key, raw_value] = String(packed ?? "").split(":");
  if (!key) return next;
  const value = raw_value == null ? null : raw_value;
  if (value == null || value === "") return next;
  next.filters[key] = value;
  return next;
}

async function handleTargetBuilderComponent({ interaction, custom_id, context }) {
  const values = Array.isArray(interaction?.data?.values) ? interaction.data.values : [];
  const parts = custom_id.split(":");
  const action = parts[1] ?? "";
  const session_key = parts[2] ?? "";

  if (!session_key) {
    return updateMessage("⚠️ Missing builder session key.");
  }

  let session;
  try {
    session = await getTargetBuilderSession(session_key);
  } catch {
    return updateMessage("⚠️ Could not load builder session.");
  }

  if (!session || session.status !== "active") {
    return updateMessage("⚠️ Builder session expired or closed.");
  }

  if (session.discord_user_id && session.discord_user_id !== context.user_id) {
    return deniedResponse("This builder session belongs to another user.");
  }

  let state = { ...createDefaultBuilderState(), ...(session.state ?? {}) };
  let mode = "main";
  let region = state.market_region;
  let filter_category = null;
  let diagnostics = {};

  try {
    if (action === "open_market") {
      mode = "market_region";
    } else if (action === "open_asset") {
      mode = "asset";
    } else if (action === "open_strategy") {
      mode = "strategy";
    } else if (action === "open_tags") {
      mode = "tags";
    } else if (action === "open_filters") {
      mode = "filter_category";
    } else if (action === "region") {
      const selected = normalizeMarketRegion(values[0] ?? "other");
      state.market_region = selected;
      region = selected;
      mode = "market";
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "market") {
      state.market = normalizeMarketLabel(values[0] ?? "");
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "asset") {
      state.asset_class = String(values[0] ?? "").trim() || null;
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "strategy") {
      state.strategy = String(values[0] ?? "").trim() || null;
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "tags") {
      state.property_tags = values.map((v) => String(v)).slice(0, 3);
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "filter_category") {
      filter_category = String(values[0] ?? "score");
      mode = "filter_values";
    } else if (action === "filter") {
      state = applyFilterSelection(state, values[0]);
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "reset") {
      state = createDefaultBuilderState();
      await updateTargetBuilderSessionState(session_key, state);
    } else if (action === "close") {
      await updateTargetBuilderSessionState(session_key, state, "closed");
      return updateMessage("✅ Target builder closed.", {
        embeds: [buildSuccessEmbed({
          title: "Target Builder Closed",
          description: "Session closed. Run /target-build to start again.",
        })],
        components: [],
      });
    } else if (action === "run_scan") {
      const market_for_scan = resolveBuilderMarketToSystemSlug(state.market ?? "");
      const targeting = buildNormalizedTargeting({
        market: market_for_scan,
        asset: state.asset_class,
        strategy: state.strategy,
        tag_1: state.property_tags?.[0] ?? null,
        tag_2: state.property_tags?.[1] ?? null,
        tag_3: state.property_tags?.[2] ?? null,
      });

      for (const [k, v] of Object.entries(state.filters ?? {})) {
        if (v != null && v !== "") targeting[k] = v;
      }

      const max_scan_count = Number(state.limits?.max_scan_count ?? 100);
      const target_eligible_count = Number(state.limits?.target_eligible_count ?? 10);

      if (String(state.scan_mode) === "property_first") {
        const property_result = await scanPropertiesForTargeting({
          targeting,
          max_scan_count,
          target_eligible_count,
          dry_run: true,
        });
        diagnostics.last_result = property_result.ok
          ? `Property scan complete • scanned=${property_result.scanned_property_count ?? 0} • eligible=${property_result.final_eligible_count ?? 0}`
          : `Property scan error: ${property_result.error ?? "unknown"}`;
      } else {
        const source_view_name = resolveTargetSourceViewName({
          market: targeting.market_label,
          asset_type: targeting.asset_slug,
          strategy: targeting.strategy_slug,
        });

        const result = await callInternal("/api/internal/outbound/feed-master-owners", {
          method: "POST",
          body: {
            dry_run: true,
            limit: target_eligible_count,
            scan_limit: max_scan_count,
            source_view_name,
            targeting_filters: Object.keys(targeting.filters ?? {}).length > 0 ? targeting.filters : undefined,
            property_tags: targeting.tags?.length > 0 ? targeting.tags.map((t) => t.slug) : undefined,
          },
        });

        diagnostics.last_result = result.ok
          ? "Owner-first scan complete"
          : `Owner-first scan error: ${result.error ?? "unknown"}`;
      }
    } else if (action === "create_campaign") {
      const market_for_scan = resolveBuilderMarketToSystemSlug(state.market ?? "");
      const targeting = buildNormalizedTargeting({
        market: market_for_scan,
        asset: state.asset_class,
        strategy: state.strategy,
        tag_1: state.property_tags?.[0] ?? null,
        tag_2: state.property_tags?.[1] ?? null,
        tag_3: state.property_tags?.[2] ?? null,
      });

      const campaign_key = buildCampaignKey({
        market: targeting.market_slug,
        asset_type: targeting.asset_slug,
        strategy: targeting.strategy_slug,
      });

      const db = getDb();
      await db.from("campaign_targets").upsert({
        campaign_key,
        campaign_name: `${targeting.market_label} ${targeting.asset_label} ${targeting.strategy_label}`,
        market: targeting.market_slug,
        asset_type: targeting.asset_slug,
        strategy: targeting.strategy_slug,
        status: "draft",
        daily_cap: 50,
        source_view_name: resolveTargetSourceViewName({
          market: targeting.market_label,
          asset_type: targeting.asset_slug,
          strategy: targeting.strategy_slug,
        }),
        metadata: {
          source: "target_builder_v1",
          tags: targeting.tags,
          filters: {
            ...(targeting.filters ?? {}),
            ...(state.filters ?? {}),
          },
          builder_state: state,
        },
        updated_at: new Date().toISOString(),
      });

      diagnostics.last_result = `Campaign created as draft: ${campaign_key}`;
    }
  } catch {
    diagnostics.last_result = "Action failed. Check logs.";
  }

  const embed = buildTargetBuilderEmbed(state, diagnostics);
  const components = buildBuilderComponents({ session_key, mode, region, filter_category });
  return updateMessage(null, { embeds: [embed], components });
}

// ---------------------------------------------------------------------------
// Command: /target scan  (deferred, always dry_run=true)
// ---------------------------------------------------------------------------

async function handleTargetScan({
  options_array,
  context,
  interaction,
  scan_mode = "core",
  force_property_first = false,
}) {
  const opt = (...names) => {
    for (const name of names) {
      const value = getOption(options_array, name);
      if (value != null) return value;
    }
    return null;
  };

  const market_raw  = String(getOption(options_array, "market") ?? "").trim();
  const asset_raw   = String(opt("asset_class", "asset") ?? "").trim();
  const strategy_raw = String(getOption(options_array, "strategy") ?? "").trim();

  if (!market_raw || !asset_raw || !strategy_raw) {
    return errorResponse("market, asset, and strategy are required.");
  }

  // Validate market slug is a known choice (guard against unexpected values)
  const market_slug = normalizeMarketSlug(market_raw);
  if (!isKnownMarketSlug(market_slug)) {
    return errorResponse(`Unknown market: "${market_slug}". Use a supported market choice.`);
  }

  const target_eligible_count = Math.min(Math.max(1, Number(opt("target_eligible_count", "limit")) || 25), 5000);
  const max_scan_count = Math.min(Math.max(1, Number(opt("max_scan_count", "scan_limit")) || 100), 10000);

  // Tags and filters
  const tag_1         = opt("property_tag_1", "tag_1")            ?? null;
  const tag_2         = opt("property_tag_2", "tag_2")            ?? null;
  const tag_3         = opt("property_tag_3", "tag_3")            ?? null;
  const zip           = getOption(options_array, "zip")            ?? null;
  const county        = getOption(options_array, "county")         ?? null;
  const min_equity    = getOption(options_array, "min_equity")     ?? null;
  const max_year_built  = getOption(options_array, "max_year_built") ?? null;
  const owner_type    = getOption(options_array, "owner_type")     ?? null;
  const phone_status  = getOption(options_array, "phone_status")   ?? null;
  const language      = getOption(options_array, "language")       ?? null;
  const motivation_min = getOption(options_array, "motivation_min") ?? null;
  const priority_tier = getOption(options_array, "priority_tier")  ?? null;
  const phone_quality = getOption(options_array, "phone_quality")  ?? null;
  const contact_confidence = getOption(options_array, "contact_confidence") ?? null;
  const min_contactability_score = getOption(options_array, "min_contactability_score") ?? null;
  const min_financial_pressure_score = getOption(options_array, "min_financial_pressure_score") ?? null;
  const min_urgency_score = getOption(options_array, "min_urgency_score") ?? null;

  // Property-first filter options — Advanced v3
  const sq_ft_range                    = getOption(options_array, "sq_ft_range") ?? null;
  const units_range                    = getOption(options_array, "units_range") ?? null;
  const ownership_years_range          = getOption(options_array, "ownership_years_range") ?? null;
  const estimated_value_range          = getOption(options_array, "estimated_value_range") ?? null;
  const equity_percent_range           = getOption(options_array, "equity_percent_range") ?? null;
  const repair_cost_range              = getOption(options_array, "repair_cost_range") ?? null;
  const building_condition             = getOption(options_array, "building_condition") ?? null;
  const offer_vs_loan                  = getOption(options_array, "offer_vs_loan") ?? null;
  const offer_vs_last_purchase_price   = getOption(options_array, "offer_vs_last_purchase_price") ?? null;
  const year_built_range               = getOption(options_array, "year_built_range") ?? null;
  const min_property_score             = getOption(options_array, "min_property_score") ?? null;

  const targeting = buildNormalizedTargeting({
    market: market_raw, asset: asset_raw, strategy: strategy_raw,
    tag_1, tag_2, tag_3,
    zip, county, min_equity, max_year_built,
    owner_type, phone_status, language, motivation_min,
  });

  if (priority_tier) {
    targeting.priority_tier = String(priority_tier);
    targeting.filters.priority_tier = String(priority_tier);
  }
  if (phone_quality) {
    targeting.phone_quality = String(phone_quality);
    targeting.filters.phone_quality = String(phone_quality);
  }
  if (contact_confidence) {
    targeting.contact_confidence = String(contact_confidence);
    targeting.filters.contact_confidence = String(contact_confidence);
  }
  if (min_contactability_score != null) {
    targeting.min_contactability_score = Number(min_contactability_score);
    targeting.filters.min_contactability_score = Number(min_contactability_score);
  }
  if (min_financial_pressure_score != null) {
    targeting.min_financial_pressure_score = Number(min_financial_pressure_score);
    targeting.filters.min_financial_pressure_score = Number(min_financial_pressure_score);
  }
  if (min_urgency_score != null) {
    targeting.min_urgency_score = Number(min_urgency_score);
    targeting.filters.min_urgency_score = Number(min_urgency_score);
  }

  // Normalize and attach property filters to targeting object
  if (sq_ft_range) targeting.sq_ft_range = normalizeSqFtRange(sq_ft_range);
  if (units_range) targeting.units_range = normalizeUnitsRange(units_range);
  if (ownership_years_range) targeting.ownership_years_range = normalizeOwnershipYearsRange(ownership_years_range);
  if (estimated_value_range) targeting.estimated_value_range = normalizeEstimatedValueRange(estimated_value_range);
  if (equity_percent_range) targeting.equity_percent_range = normalizeEquityPercentRange(equity_percent_range);
  if (repair_cost_range) targeting.repair_cost_range = normalizeRepairCostRange(repair_cost_range);
  if (building_condition) targeting.building_condition = normalizeBuildingCondition(building_condition);
  if (offer_vs_loan) targeting.offer_vs_loan = normalizeOfferVsLoan(offer_vs_loan);
  if (offer_vs_last_purchase_price) targeting.offer_vs_last_purchase_price = normalizeOfferVsLastPurchasePrice(offer_vs_last_purchase_price);
  if (year_built_range) targeting.year_built_range = normalizeYearBuiltRange(year_built_range);
  if (min_property_score != null) targeting.min_property_score = Number(min_property_score);

  const source_view_name = resolveTargetSourceViewName({
    market:     targeting.market_label,
    asset_type: targeting.asset_slug,
    strategy:   targeting.strategy_slug,
  });
  const campaign_key = buildCampaignKey({
    market: targeting.market_slug, asset_type: targeting.asset_slug, strategy: targeting.strategy_slug,
  });

  const app_id = String(process.env.DISCORD_APPLICATION_ID ?? "");
  const token  = interaction.token;

  // Detect path: use property-first scan if any property filters selected
  const forced_property_first = force_property_first || scan_mode === "property_first";
  const use_property_first = forced_property_first || isPropertyFirstTargeting(targeting);

  info("discord.target.scan.started", {
    market: targeting.market_slug,
    asset:  targeting.asset_slug,
    strategy: targeting.strategy_slug,
    target_eligible_count,
    max_scan_count,
    tag_count:    targeting.tags.length,
    filter_count: Object.keys(targeting.filters).length,
    property_filter_count: Object.values({
      sq_ft_range,
      units_range,
      ownership_years_range,
      estimated_value_range,
      equity_percent_range,
      repair_cost_range,
      building_condition,
      offer_vs_loan,
      offer_vs_last_purchase_price,
      year_built_range,
      min_property_score,
    }).filter((v) => v != null).length,
    scan_mode,
    force_property_first: forced_property_first,
    use_property_first,
    user_id: context.user_id,
  });

  return runDeferredDiscordHandler(interaction, async () => {
    let scan_summary = {};
    let source_label = "Master Owner";
    let property_samples = [];
    
    if (use_property_first) {
      // Property-first targeting path
      const property_result = await scanPropertiesForTargeting({
        targeting,
        max_scan_count,
        target_eligible_count,
        dry_run: true,
      });
      
      if (!property_result.ok) {
        return { content: `Property scan error: ${property_result.error ?? "unknown"}.` };
      }
      
      source_label = "Property";
      property_samples = property_result.eligible_samples || [];
      scan_summary = {
        scanned:     property_result.scanned_property_count ?? 0,
        eligible:    property_result.final_eligible_count ?? 0,
        would_queue: 0,  // Property path doesn't create Send Queue
        skipped:    property_result.skipped_count ?? 0,
        property_samples,
        stopped_reason: property_result.stopped_reason,
        api_estimate: property_result.api_request_estimate,
      };
    } else {
      // Master Owner first path (existing v2 behavior)
      const result = await callInternal("/api/internal/outbound/feed-master-owners", {
        method: "POST",
        body: {
          dry_run:          true,
          limit: target_eligible_count,
          scan_limit: max_scan_count,
          source_view_name,
          targeting_filters: Object.keys(targeting.filters).length > 0 ? targeting.filters : undefined,
          property_tags:     targeting.tags.length > 0
            ? targeting.tags.map((t) => t.slug)
            : undefined,
        },
        timeout_ms: 30_000,
      });

      if (result.timed_out) {
        return { content: "Target scan is still running — check alerts channel." };
      }
      if (!result.ok) {
        return { content: `Target scan error: ${result.error ?? "unknown"}.` };
      }

      const payload = result.data ?? {};
      const feeder  = payload.result ?? {};
      source_label = "Master Owner";
      scan_summary = {
        scanned:    feeder.loaded_count   ?? 0,
        eligible:   feeder.eligible_count ?? 0,
        would_queue: feeder.inserted_count ?? 0,
        skipped:    feeder.skipped_count  ?? 0,
      };
    }

    try {
      const db = getDb();
      const { data: existing } = await db
        .from("campaign_targets")
        .select("id")
        .eq("campaign_key", campaign_key)
        .maybeSingle();
      if (existing) {
        await db
          .from("campaign_targets")
          .update({
            last_scan_summary: scan_summary,
            last_scan_at:  new Date().toISOString(),
            updated_at:    new Date().toISOString(),
          })
          .eq("campaign_key", campaign_key);
      }
    } catch {
      // Non-fatal
    }

    info("discord.target.scan.completed", {
      market:     targeting.market_slug,
      asset:      targeting.asset_slug,
      strategy:   targeting.strategy_slug,
      source:     source_label,
      ...scan_summary,
      user_id: context.user_id,
    });

    const embeds = [buildTargetScanEmbed({
      market:          targeting.market_slug,
      asset:           targeting.asset_slug,
      strategy:        targeting.strategy_slug,
      market_label:    targeting.market_label,
      asset_label:     targeting.asset_label,
      strategy_label:  targeting.strategy_label,
      theme:           targeting.theme,
      tags:            targeting.tags,
      filters:         targeting.filters,
      source_view_name,
      scan_source:     source_label,
      scanned:     scan_summary.scanned,
      eligible:    scan_summary.eligible,
      would_queue: scan_summary.would_queue ?? 0,
      skipped:     scan_summary.skipped ?? 0,
      property_samples: use_property_first ? property_samples : undefined,
      stopped_reason: scan_summary.stopped_reason,
      api_estimate: scan_summary.api_estimate,
    })];
    return { embeds, components: targetActionRow({ campaignKey: campaign_key }) };
  }, "command", "target_scan", {
    fallback_content: "Target scan encountered an error. Check server logs.",
  });
}

// ---------------------------------------------------------------------------
// Command: /campaign create
// ---------------------------------------------------------------------------

async function handleCampaignCreate({ options_array, context }) {
  const campaign_name = String(getOption(options_array, "name") ?? "").trim();
  const market_raw    = String(getOption(options_array, "market") ?? "").trim();
  const asset_raw     = String(getOption(options_array, "asset") ?? "").trim();
  const strategy_raw  = String(getOption(options_array, "strategy") ?? "").trim();

  if (!campaign_name || !market_raw || !asset_raw || !strategy_raw) {
    return errorResponse("name, market, asset, and strategy are required.");
  }

  // Validate market slug
  const market_slug = normalizeMarketSlug(market_raw);
  if (!isKnownMarketSlug(market_slug)) {
    return errorResponse(`Unknown market: "${market_slug}". Use a supported market choice.`);
  }

  // Bound daily_cap: min 1, max 500 for non-Owner; Owners may exceed
  const raw_cap  = Number(getOption(options_array, "daily_cap")) || 50;
  const max_cap  = isOwner(context.role_ids) ? 10000 : 500;
  const daily_cap = Math.min(Math.max(1, raw_cap), max_cap);

  const source_view_name_override = String(getOption(options_array, "source_view_name") ?? "").trim() || null;

  // Tags and filters
  const tag_1          = getOption(options_array, "tag_1")           ?? null;
  const tag_2          = getOption(options_array, "tag_2")           ?? null;
  const tag_3          = getOption(options_array, "tag_3")           ?? null;
  const zip            = getOption(options_array, "zip")             ?? null;
  const county         = getOption(options_array, "county")          ?? null;
  const min_equity     = getOption(options_array, "min_equity")      ?? null;
  const max_year_built = getOption(options_array, "max_year_built")  ?? null;
  const owner_type     = getOption(options_array, "owner_type")      ?? null;
  const phone_status   = getOption(options_array, "phone_status")    ?? null;
  const language       = getOption(options_array, "language")        ?? null;
  const motivation_min = getOption(options_array, "motivation_min")  ?? null;

  const targeting = buildNormalizedTargeting({
    market: market_raw, asset: asset_raw, strategy: strategy_raw,
    tag_1, tag_2, tag_3,
    zip, county, min_equity, max_year_built,
    owner_type, phone_status, language, motivation_min,
  });

  const source_view_name = source_view_name_override ?? resolveTargetSourceViewName({
    market:     targeting.market_label,
    asset_type: targeting.asset_slug,
    strategy:   targeting.strategy_slug,
  });

  const campaign_key = buildCampaignKey({
    market:     targeting.market_slug,
    asset_type: targeting.asset_slug,
    strategy:   targeting.strategy_slug,
  });

  info("discord.campaign.create.started", {
    campaign_key,
    market:   targeting.market_slug,
    asset:    targeting.asset_slug,
    strategy: targeting.strategy_slug,
    daily_cap,
    tag_count:    targeting.tags.length,
    filter_count: Object.keys(targeting.filters).length,
    user_id: context.user_id,
  });

  try {
    const db = getDb();
    const row = {
      campaign_key,
      campaign_name,
      market:     targeting.market_slug,
      asset_type: targeting.asset_slug,
      strategy:   targeting.strategy_slug,
      language:   language ?? "auto",
      source_view_name,
      daily_cap,
      status:     "draft",
      created_by_discord_user_id: context.user_id || null,
      metadata: {
        market_label:   targeting.market_label,
        asset_label:    targeting.asset_label,
        strategy_label: targeting.strategy_label,
        tags:    targeting.tags,
        filters: targeting.filters,
        theme:   targeting.theme,
      },
      updated_at: new Date().toISOString(),
    };

    const { error } = await db
      .from("campaign_targets")
      .upsert(row, { onConflict: "campaign_key" });
    if (error) throw error;

    info("discord.campaign.create.completed", {
      campaign_key,
      market:   targeting.market_slug,
      daily_cap,
      user_id: context.user_id,
    });

    return cinematicMessage({
      embeds: [buildCampaignCreatedEmbed({
        campaign_key,
        campaign_name,
        market:          targeting.market_slug,
        asset:           targeting.asset_slug,
        strategy:        targeting.strategy_slug,
        market_label:    targeting.market_label,
        asset_label:     targeting.asset_label,
        strategy_label:  targeting.strategy_label,
        theme:           targeting.theme,
        tags:            targeting.tags,
        filters:         targeting.filters,
        daily_cap,
        status: "draft",
        source_view_name,
      })],
      components: campaignActionRow({ campaignKey: campaign_key, paused: false }),
    });
  } catch {
    logError("discord.target.command.failed", { command: "campaign.create", user_id: context.user_id });
    return errorResponse("Failed to create campaign. Please try again.");
  }
}

// ---------------------------------------------------------------------------
// Command: /campaign inspect
// ---------------------------------------------------------------------------

async function handleCampaignInspect({ options_array, context }) {
  const campaign_key = String(getOption(options_array, "campaign") ?? "").trim();
  if (!campaign_key) return errorResponse("campaign key is required.");

  try {
    const db = getDb();
    const { data, error } = await db
      .from("campaign_targets")
      .select("*")
      .eq("campaign_key", campaign_key)
      .maybeSingle();

    if (error) throw error;
    if (!data) return errorResponse(`Campaign not found: ${campaign_key}`);

    return cinematicMessage({
      embeds: [buildCampaignInspectEmbed(data)],
      components: campaignActionRow({ campaignKey: campaign_key, paused: data.status === "paused" }),
    });
  } catch {
    return errorResponse("Failed to load campaign.");
  }
}

// ---------------------------------------------------------------------------
// Command: /campaign scale
// ---------------------------------------------------------------------------

async function handleCampaignScale({ options_array, context, interaction }) {
  const campaign_key = String(getOption(options_array, "campaign") ?? "").trim();
  const daily_cap = Math.max(1, Number(getOption(options_array, "daily_cap")) || 0);

  if (!campaign_key) return errorResponse("campaign key is required.");
  if (!daily_cap) return errorResponse("daily_cap must be a positive integer.");
  if (!checkPermission(context.role_ids, ["owner", "tech_ops", "sms_ops"])) {
    return deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
  }

  const needs_approval = daily_cap > 100 && !isOwner(context.role_ids) && !isTechOps(context.role_ids);
  if (needs_approval) {
    const approval_token = crypto.randomUUID();
    const action_payload = { command: "campaign_scale", campaign_key, daily_cap };

    await auditLog({
      interaction_id: interaction.id,
      guild_id: context.guild_id,
      channel_id: context.channel_id,
      user_id: context.user_id,
      username: context.username,
      command_name: "campaign",
      subcommand: "scale",
      options: action_payload,
      role_ids: context.role_ids,
      permission_outcome: "allowed",
      action_outcome: "approval_pending",
      approval_token,
    });

    return cinematicMessage({
      embeds: [buildCampaignScaleEmbed({
        campaign_key,
        current_cap: null,
        requested_cap: daily_cap,
        status: "pending",
        recommendation: "Requires Owner or Tech Ops approval for daily_cap > 100",
        risk_level: "high",
      })],
      components: approvalButtons({
        actionId: approval_token,
        approveLabel: `Scale to ${daily_cap}`,
        denyLabel: "Cancel",
      }),
    });
  }

  try {
    const db = getDb();
    const { data: existing } = await db
      .from("campaign_targets")
      .select("daily_cap, status")
      .eq("campaign_key", campaign_key)
      .maybeSingle();

    if (!existing) return errorResponse(`Campaign not found: ${campaign_key}`);

    await db
      .from("campaign_targets")
      .update({ daily_cap, updated_at: new Date().toISOString() })
      .eq("campaign_key", campaign_key);

    return cinematicMessage({
      embeds: [buildCampaignScaleEmbed({
        campaign_key,
        current_cap: existing.daily_cap,
        requested_cap: daily_cap,
        status: "applied",
        recommendation: daily_cap > 200 ? "High volume — monitor closely" : "Within safe range",
        risk_level: daily_cap > 200 ? "high" : daily_cap > 100 ? "medium" : "low",
      })],
    });
  } catch {
    return errorResponse("Failed to update campaign scale.");
  }
}

// ---------------------------------------------------------------------------
// Command: /territory map
// ---------------------------------------------------------------------------

async function handleTerritoryMap(context) {
  try {
    const db = getDb();
    const { data, error } = await db
      .from("campaign_targets")
      .select("*")
      .order("market", { ascending: true })
      .order("status", { ascending: true });

    if (error) throw error;

    const rows = data ?? [];
    if (rows.length === 0) {
      return cinematicMessage({
        embeds: [buildTerritoryMapEmbed({ grouped: {}, empty: true })],
        components: territoryActionRow(),
      });
    }

    const grouped = {};
    for (const row of rows) {
      const key = row.market || "unknown";
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(row);
    }

    return cinematicMessage({
      embeds: [buildTerritoryMapEmbed({ grouped, empty: false })],
      components: territoryActionRow(),
    });
  } catch {
    return errorResponse("Failed to load territory map.");
  }
}

// ---------------------------------------------------------------------------
// Command: /email
// ---------------------------------------------------------------------------

async function handleEmailCockpit({ context, interaction }) {
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/email/cockpit", { method: "GET" });
    const embed = buildEmailCockpitEmbed(result?.ok ? result : {});
    return { embeds: [embed], components: emailActionRow() };
  }, "command", "email_cockpit", {
    fallback_content: "Email cockpit unavailable. Check server logs.",
  });
}

async function handleEmailPreview({ options_array, context, interaction }) {
  const template_key = options_array?.find(o => o.name === "template_key")?.value ?? "";
  const owner_id     = options_array?.find(o => o.name === "owner_id")?.value     ?? null;
  if (!template_key) return errorResponse("template_key is required.");
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/email/preview", {
      method: "POST",
      body: { template_key, context: { owner_id } },
    });
    if (!result?.ok) {
      return { content: "Preview failed." };
    }
    const { preview = {} } = result;
    const embed = buildEmailPreviewEmbed({ template_key, ...preview });
    return { embeds: [embed] };
  }, "command", "email_preview", {
    fallback_content: "Email preview unavailable. Check server logs.",
  });
}

async function handleEmailSendTest({ options_array, context, interaction }) {
  const email_address = options_array?.find(o => o.name === "email_address")?.value ?? "";
  const template_key  = options_array?.find(o => o.name === "template_key")?.value  ?? "";
  if (!email_address || !template_key) return errorResponse("email_address and template_key are required.");
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/email/send-test", {
      method: "POST",
      body: { email_address, template_key, context: {} },
    });
    if (!result?.ok) {
      const embed = buildEmailSendTestEmbed({ sent: false, email_address, template_key, error: result?.error ?? "Send failed" });
      return { embeds: [embed] };
    }
    const embed = buildEmailSendTestEmbed({ sent: true, email_address, template_key, brevo_message_id: result.brevo_message_id });
    return { embeds: [embed] };
  }, "command", "email_send_test", {
    fallback_content: "Email send-test unavailable. Check server logs.",
  });
}

async function handleEmailQueueStatus({ options_array, context, interaction }) {
  const limit   = options_array?.find(o => o.name === "limit")?.value   ?? 20;
  const dry_run = options_array?.find(o => o.name === "dry_run")?.value  ?? true;
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/email/queue/run", {
      method: "POST",
      body: { limit, dry_run },
    });
    if (!result?.ok) {
      return { content: "Email queue run failed." };
    }
    const embed = buildEmailStatsEmbed({
      event_type_counts: { attempted: result.attempted_count ?? 0, sent: result.sent_count ?? 0, failed: result.failed_count ?? 0, skipped: result.skipped_count ?? 0 },
      suppression_total: 0,
      latest_event_at:   null,
    });
    return { embeds: [embed], components: emailActionRow() };
  }, "command", "email_queue", {
    fallback_content: "Email queue status unavailable. Check server logs.",
  });
}

async function handleEmailSuppression({ context, interaction }) {
  return runDeferredDiscordHandler(interaction, async () => {
    const db = getDb();
    const { count, data, error } = await db
      .from("email_suppression")
      .select("email_address, reason, suppressed_at", { count: "exact" })
      .order("suppressed_at", { ascending: false })
      .limit(10);
    if (error) throw error;
    const embed = buildEmailSuppressionEmbed({
      suppression_total:   count ?? 0,
      recent_suppressions: data  ?? [],
    });
    return { embeds: [embed] };
  }, "command", "email_suppression", {
    fallback_content: "Email suppression unavailable. Check server logs.",
  });
}

async function handleEmailStats({ context, interaction }) {
  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/email/cockpit", { method: "GET" });
    if (!result?.ok) {
      return { content: "Email stats unavailable." };
    }
    const embed = buildEmailStatsEmbed({
      event_type_counts: result.event_type_counts  ?? {},
      latest_event_at:   result.latest_event_at    ?? null,
      suppression_total: result.suppression_total  ?? 0,
    });
    return { embeds: [embed], components: emailActionRow() };
  }, "command", "email_stats", {
    fallback_content: "Email stats unavailable. Check server logs.",
  });
}

// ---------------------------------------------------------------------------
// Command: /replay
// ---------------------------------------------------------------------------

async function handleReplayInbound({ options_array, context, interaction }) {
  const text       = options_array?.find(o => o.name === "text")?.value       ?? "";
  const language   = options_array?.find(o => o.name === "language")?.value   ?? "English";
  const stage      = options_array?.find(o => o.name === "stage")?.value      ?? null;
  const property_type = options_array?.find(o => o.name === "property_type")?.value ?? null;
  const deal_strategy = options_array?.find(o => o.name === "deal_strategy")?.value ?? null;

  if (!text) return errorResponse("text is required.");

  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/testing/replay-inbound", {
      method: "POST",
      body: {
        message_body:   text,
        from_number:    null,
        to_number:      null,
        prior_language: language,
        prior_stage:    stage,
        property_type,
        deal_strategy,
        dry_run:        true,
      },
    });

    if (!result?.ok) {
      return { content: `Replay failed: ${sanitizeUserVisibleErrorText(result?.error)}` };
    }

    const embed = buildReplayInboundEmbed({
      message_body:            text,
      classification:          result.classification ?? {},
      previous_stage:          result.previous_stage,
      next_stage:              result.next_stage,
      selected_use_case:       result.selected_use_case,
      selected_template_source: result.selected_template_source,
      would_queue_reply:       result.would_queue_reply ?? false,
      underwriting_signals:    result.underwriting_signals ?? {},
      underwriting_route:      result.underwriting_route,
      alignment_passed:        result.alignment_passed ?? true,
    });

    return { embeds: [embed] };
  }, "command", "replay_inbound", {
    fallback_content: "Replay inbound unavailable. Check server logs.",
  });
}

async function handleReplayOwner({ options_array, context, interaction }) {
  const owner_id = options_array?.find(o => o.name === "owner_id")?.value ?? "";
  const text     = options_array?.find(o => o.name === "text")?.value     ?? "";

  if (!owner_id || !text) return errorResponse("owner_id and text are required.");

  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/testing/replay-inbound", {
      method: "POST",
      body: {
        message_body:   text,
        from_number:    null,
        to_number:      null,
        master_owner_id: owner_id,
        dry_run:        true,
      },
    });

    if (!result?.ok) {
      return { content: `Replay failed: ${sanitizeUserVisibleErrorText(result?.error)}` };
    }

    const embed = buildReplayOwnerEmbed({
      owner_id,
      owner_name:           null,
      property_address:     "unknown",
      property_type:        result.underwriting_signals?.property_type ?? "unknown",
      message_body:         text,
      classification:       result.classification ?? {},
      current_stage:        result.previous_stage,
      next_stage:           result.next_stage,
      selected_use_case:    result.selected_use_case,
      selected_template_source: result.selected_template_source,
      cash_offer_snapshot:  null,
      underwriting_route:   result.underwriting_route,
      would_queue:          result.would_queue_reply ?? false,
    });

    return { embeds: [embed] };
  }, "command", "replay_owner", {
    fallback_content: "Replay owner unavailable. Check server logs.",
  });
}

async function handleReplayTemplate({ options_array, context, interaction }) {
  const use_case      = options_array?.find(o => o.name === "use_case")?.value ?? "";
  const language      = options_array?.find(o => o.name === "language")?.value ?? "English";
  const property_type = options_array?.find(o => o.name === "property_type")?.value ?? "Single Family";

  if (!use_case) return errorResponse("use_case is required.");

  return runDeferredDiscordHandler(interaction, async () => {
    const result = await callInternal("/api/internal/testing/replay-inbound", {
      method: "POST",
      body: {
        message_body:   "[template preview]",
        from_number:    null,
        to_number:      null,
        prior_language: language,
        property_type,
        dry_run:        true,
      },
    });

    if (!result?.ok || !result.selected_template_id) {
      return { content: "Template resolution failed" };
    }

    const embed = buildReplayTemplateEmbed({
      use_case,
      template_id:            result.selected_template_id,
      template_source:        result.selected_template_source,
      stage_code:             result.selected_template_stage_code,
      language:               result.selected_template_language || language,
      template_text:          result.rendered_message_text || "(template not rendered)",
      property_type_resolved: property_type,
    });

    return { embeds: [embed] };
  }, "command", "replay_template", {
    fallback_content: "Template resolution unavailable. Check server logs.",
  });
}

async function handleReplayBatch({ options_array, context, interaction }) {
  const scenario = options_array?.find(o => o.name === "scenario")?.value ?? "all";

  const scenarios_map = {
    "ownership": [
      { text: "Hi who is this?", stage: "ownership_check" },
      { text: "Is this the right number?", stage: "ownership_check" },
      { text: "Wrong number", stage: "ownership_check" },
    ],
    "offer_requests": [
      { text: "What is your offer?", stage: "offer_reveal_cash" },
      { text: "Can you tell me more about the offer?", stage: "offer_reveal_cash" },
    ],
    "objections": [
      { text: "I do not want to sell", stage: "consider_selling" },
      { text: "Are you paying cash?", stage: "asking_price" },
    ],
    "underwriting": [
      { text: "How many units is this?", stage: "mf_confirm_units" },
      { text: "What kind of property is it?", stage: "property_discovery" },
    ],
    "compliance": [
      { text: "Stop calling me", stage: "stop_or_opt_out" },
      { text: "Do not contact me", stage: "stop_or_opt_out" },
    ],
  };

  let test_cases = [];
  if (scenario === "all") {
    for (const cases of Object.values(scenarios_map)) {
      test_cases.push(...cases);
    }
  } else {
    test_cases = scenarios_map[scenario] ?? [];
  }

  return runDeferredDiscordHandler(interaction, async () => {
    const results = [];
    let passed = 0;
    let warnings = 0;
    let failed = 0;

    for (const test_case of test_cases) {
      try {
        const result = await callInternal("/api/internal/testing/replay-inbound", {
          method: "POST",
          body: {
            message_body: test_case.text,
            from_number:  null,
            to_number:    null,
            prior_stage:  test_case.stage,
            dry_run:      true,
          },
        });

        if (result?.ok && result?.alignment_passed) {
          results.push({ name: test_case.text, status: "pass", note: result.selected_use_case });
          passed++;
        } else if (result?.ok) {
          results.push({ name: test_case.text, status: "warn", note: "alignment warning" });
          warnings++;
        } else {
          results.push({
            name: test_case.text,
            status: "fail",
            note: sanitizeUserVisibleErrorText(result?.error),
          });
          failed++;
        }
      } catch {
        results.push({ name: test_case.text, status: "fail", note: "error" });
        failed++;
      }
    }

    const embed = buildReplayBatchEmbed({
      scenario,
      tested:   test_cases.length,
      passed,
      warnings,
      failed,
      results,
    });

    return { embeds: [embed] };
  }, "command", "replay_batch", {
    fallback_content: "Replay batch unavailable. Check server logs.",
  });
}

// ---------------------------------------------------------------------------
// /briefing handlers
// ---------------------------------------------------------------------------

/**
 * Deferred handler for all /briefing subcommands.
 * Immediately returns type:5 ephemeral deferred, then edits with the full embed.
 *
 * @param {object}   interaction
 * @param {Function} asyncFn     - Async fn resolving to { embeds, components }
 * @param {string}   label       - short log tag (today/yesterday/week/market/agent)
 * @param {object}   meta        - Extra info for logging (range, market, agent)
 */
function runDeferredBriefingHandler(interaction, asyncFn, label, meta = {}) {
  const app_id  = String(process.env.DISCORD_APPLICATION_ID ?? "");
  const token   = interaction.token;
  const user_id = interaction.member?.user?.id ?? "unknown";
  const started = Date.now();

  info("discord.briefing.started", { label, user_id, ...meta });

  Promise.resolve()
    .then(async () => {
      let payload;
      try {
        payload = await asyncFn();
        const duration_ms = Date.now() - started;
        info("discord.briefing.completed", {
          label, user_id, duration_ms,
          partial: payload?.partial ?? false,
          ...meta,
        });
      } catch (err) {
        const duration_ms = Date.now() - started;
        logError("discord.briefing.failed", { label, user_id, duration_ms, ...meta });
        payload = {
          embeds: [{
            title:       "⚠️ Briefing Unavailable",
            description: "The briefing could not be generated. Check server logs.",
            color:       0xF1C40F,
          }],
          components: [],
        };
      }
      await doEditInteractionResponse({
        applicationId:    app_id,
        token,
        content:          "",
        embeds:           payload.embeds     ?? [],
        components:       payload.components ?? [],
        flags:            64,
        allowed_mentions: { parse: [] },
      }).catch(() => {});
    })
    .catch(() => {});

  return deferredEphemeralResponse();
}

/**
 * handleBriefingToday
 */
function handleBriefingToday({ options_array, context, interaction }) {
  const timezone = String(options_array?.find(o => o.name === "timezone")?.value ?? "America/Chicago");
  const market   = options_array?.find(o => o.name === "market")?.value ?? null;
  return runDeferredBriefingHandler(interaction, async () => {
    const metrics = await getDailyBriefing({ range: "today", timezone, market, supabase: getDb() });
    return { embeds: [buildDailyBriefingEmbed(metrics)], components: briefingActionRow(), partial: metrics.partial };
  }, "today", { timezone, market });
}

/**
 * handleBriefingYesterday
 */
function handleBriefingYesterday({ options_array, context, interaction }) {
  const timezone = String(options_array?.find(o => o.name === "timezone")?.value ?? "America/Chicago");
  const market   = options_array?.find(o => o.name === "market")?.value ?? null;
  return runDeferredBriefingHandler(interaction, async () => {
    const metrics = await getDailyBriefing({ range: "yesterday", timezone, market, supabase: getDb() });
    return { embeds: [buildDailyBriefingEmbed(metrics)], components: briefingActionRow(), partial: metrics.partial };
  }, "yesterday", { timezone, market });
}

/**
 * handleBriefingWeek
 */
function handleBriefingWeek({ options_array, context, interaction }) {
  const timezone = String(options_array?.find(o => o.name === "timezone")?.value ?? "America/Chicago");
  const market   = options_array?.find(o => o.name === "market")?.value ?? null;
  return runDeferredBriefingHandler(interaction, async () => {
    const metrics = await getDailyBriefing({ range: "week", timezone, market, supabase: getDb() });
    return { embeds: [buildDailyBriefingEmbed(metrics)], components: briefingActionRow(), partial: metrics.partial };
  }, "week", { timezone, market });
}

/**
 * handleBriefingMarket
 */
function handleBriefingMarket({ options_array, context, interaction }) {
  const market   = String(options_array?.find(o => o.name === "market")?.value ?? "");
  const range    = String(options_array?.find(o => o.name === "range")?.value ?? "today");
  if (!market) return errorResponse("market is required for /briefing market.");
  const timezone = "America/Chicago";
  return runDeferredBriefingHandler(interaction, async () => {
    const metrics = await getDailyBriefing({ range, timezone, market, supabase: getDb() });
    return { embeds: [buildDailyBriefingEmbed(metrics)], components: briefingActionRow(), partial: metrics.partial };
  }, "market", { market, range });
}

/**
 * handleBriefingAgent
 */
function handleBriefingAgent({ options_array, context, interaction }) {
  const agent  = String(options_array?.find(o => o.name === "agent")?.value ?? "").trim();
  const range  = String(options_array?.find(o => o.name === "range")?.value ?? "today");
  if (!agent) return errorResponse("agent is required for /briefing agent.");
  const timezone = "America/Chicago";
  return runDeferredBriefingHandler(interaction, async () => {
    const metrics = await getDailyBriefing({ range, timezone, agent, supabase: getDb() });
    return { embeds: [buildDailyBriefingEmbed(metrics)], components: briefingActionRow(), partial: metrics.partial };
  }, "agent", { agent, range });
}

// ---------------------------------------------------------------------------
// /wires handlers
// ---------------------------------------------------------------------------

/**
 * handleWiresCockpit — show wire command center summary (deferred)
 */
function handleWiresCockpit({ options_array, context, interaction }) {
  const days = options_array?.find(o => o.name === "days")?.value ?? 7;
  return runDeferredWiresHandler(interaction, async () => {
    const summary = await getWireSummary({ days, db: getDb() });
    return { embeds: [buildWireCockpitEmbed({ ...summary, days: String(days) })], components: wireCockpitButtons() };
  }, "cockpit");
}

/**
 * handleWiresExpected — create an expected wire event
 */
async function handleWiresExpected({ options_array, context }) {
  const amount = options_array?.find(o => o.name === "amount")?.value ?? "0";
  const account = options_array?.find(o => o.name === "account")?.value ?? "";
  const deal_key = options_array?.find(o => o.name === "deal_key")?.value ?? null;
  const property_id = options_array?.find(o => o.name === "property_id")?.value ?? null;
  const closing_id = options_array?.find(o => o.name === "closing_id")?.value ?? null;
  const expected_at = options_array?.find(o => o.name === "expected_at")?.value ?? null;
  const note = options_array?.find(o => o.name === "note")?.value ?? null;

  if (!amount || !account) {
    return errorResponse("amount and account are required.");
  }

  try {
    const wire = await createExpectedWire({
      amount: Number(amount),
      account_key: account,
      deal_key,
      property_id: property_id ? Number(property_id) : null,
      closing_id: closing_id ? Number(closing_id) : null,
      expected_at,
      metadata: note ? { note } : {},
      created_by_discord_user_id: context.user_id,
      db: getDb(),
    });

    const embed = buildWireExpectedEmbed({
      amount: Number(amount),
      account_display: account,
      deal_key,
      expected_at,
      wire_key: wire.wire_key,
    });
    return cinematicMessage([embed]);
  } catch (err) {
    return errorResponse(`Failed to create expected wire: ${err?.message ?? "error"}`);
  }
}

/**
 * handleWiresReceived — mark wire as received
 */
async function handleWiresReceived({ options_array, context }) {
  const wire_key = options_array?.find(o => o.name === "wire_key")?.value ?? "";
  const note = options_array?.find(o => o.name === "note")?.value ?? null;

  if (!wire_key) {
    return errorResponse("wire_key is required.");
  }

  try {
    const wire = await markWireReceived({
      wire_key,
      status_note: note,
      discord_user_id: context.user_id,
      db: getDb(),
    });

    const embed = buildWireReceivedEmbed({
      wire_key: wire.wire_key,
      amount: wire.amount,
      received_at: wire.received_at,
      note,
    });
    return cinematicMessage([embed]);
  } catch (err) {
    return errorResponse(`Failed to mark wire received: ${err?.message ?? "error"}`);
  }
}

/**
 * handleWiresCleared — mark wire as cleared
 */
async function handleWiresCleared({ options_array, context }) {
  const wire_key = options_array?.find(o => o.name === "wire_key")?.value ?? "";
  const note = options_array?.find(o => o.name === "note")?.value ?? null;

  if (!wire_key) {
    return errorResponse("wire_key is required.");
  }

  try {
    const wire = await markWireCleared({
      wire_key,
      status_note: note,
      discord_user_id: context.user_id,
      db: getDb(),
    });

    const embed = buildWireClearedEmbed({
      wire_key: wire.wire_key,
      amount: wire.amount,
      cleared_at: wire.cleared_at,
      note,
    });
    return cinematicMessage([embed]);
  } catch (err) {
    return errorResponse(`Failed to mark wire cleared: ${err?.message ?? "error"}`);
  }
}

/**
 * handleWiresForecast — show forecast of expected wires (deferred)
 */
function handleWiresForecast({ options_array, context, interaction }) {
  const days = options_array?.find(o => o.name === "days")?.value ?? 14;
  return runDeferredWiresHandler(interaction, async () => {
    const forecast = await buildWireForecast({ days, db: getDb() });
    return { embeds: [buildWireForecastEmbed(forecast)], components: wireCockpitButtons() };
  }, "forecast");
}

/**
 * handleWiresDeal — show wires linked to deal/property/closing (deferred)
 */
function handleWiresDeal({ options_array, context, interaction }) {
  const deal_key = options_array?.find(o => o.name === "deal_key")?.value ?? "";
  if (!deal_key) return errorResponse("deal_key is required.");

  return runDeferredWiresHandler(interaction, async () => {
    const wires = await listWireEvents({ db: getDb(), limit: 100 });
    const filtered_wires = wires.filter(w =>
      w.deal_key === deal_key ||
      String(w.property_id) === deal_key ||
      String(w.closing_id) === deal_key
    );
    return { embeds: [buildWireDealEmbed({ deal_key, wires: filtered_wires })] };
  }, "deal");
}

/**
 * handleWiresReconcile — show wire anomalies (deferred)
 */
function handleWiresReconcile({ options_array, context, interaction }) {
  const days = options_array?.find(o => o.name === "days")?.value ?? 30;
  return runDeferredWiresHandler(interaction, async () => {
    const wires = await listWireEvents({ db: getDb(), days, limit: 1000 });
    const missing_account_links = wires.filter(w => !w.account_key).length;
    const missing_deal_links    = wires.filter(w => !w.deal_key && !w.closing_id).length;
    const stale_pending         = wires.filter(w => {
      if (w.status !== "pending") return false;
      return (Date.now() - new Date(w.created_at).getTime()) / 86_400_000 > 7;
    }).length;
    return {
      embeds: [buildWireReconcileEmbed({
        missing_account_links,
        missing_deal_links,
        stale_pending,
        total_anomalies: missing_account_links + missing_deal_links + stale_pending,
        scope_days: String(days),
      })],
      components: wireCockpitButtons(),
    };
  }, "reconcile");
}

// ---------------------------------------------------------------------------
// Command: /conquest
// ---------------------------------------------------------------------------

async function handleConquest(context) {
  try {
    const db = getDb();
    const { data, error } = await db
      .from("campaign_targets")
      .select("status, daily_cap, market, last_scan_at");

    if (error) throw error;

    const rows = data ?? [];
    let active = 0;
    let draft = 0;
    let paused = 0;
    let total_daily_cap = 0;
    let last_scan = null;
    const markets = new Set();

    for (const row of rows) {
      const status = String(row.status ?? "").toLowerCase();
      if (status === "active") active++;
      if (status === "draft") draft++;
      if (status === "paused") paused++;
      total_daily_cap += Number(row.daily_cap) || 0;
      if (row.market) markets.add(row.market);
      if (row.last_scan_at && (!last_scan || row.last_scan_at > last_scan)) last_scan = row.last_scan_at;
    }

    const recommended_next_move =
      active === 0 && draft === 0
        ? "Create your first campaign with /campaign create"
        : active === 0
          ? "Run /target scan on draft campaigns before requesting launch"
          : !last_scan || Date.now() - new Date(last_scan).getTime() > 86_400_000
            ? "Refresh territory intel with /target scan"
            : "Monitor mission health and hot leads while campaigns run";

    return cinematicMessage({
      embeds: [buildConquestEmbed({
        active,
        draft,
        paused,
        total_daily_cap,
        markets_unlocked: markets.size,
        last_scan,
        recommended_next_move,
      })],
    });
  } catch {
    return errorResponse("Failed to load conquest overview.");
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
  } else if (action_payload.command === "feeder_launch") {
    const { limit, scan_limit } = action_payload;
    exec_result = await callInternal("/api/internal/outbound/feed-master-owners", {
      method: "POST",
      body:   { limit, scan_limit, dry_run: false },
    });
  } else if (action_payload.command === "campaign_scale") {
    const { campaign_key, daily_cap } = action_payload;
    try {
      const db_a = getDb();
      await db_a
        .from("campaign_targets")
        .update({ daily_cap: Number(daily_cap), updated_at: new Date().toISOString() })
        .eq("campaign_key", campaign_key);
      exec_result = { ok: true, data: { campaign_key, daily_cap } };
    } catch {
      exec_result = { ok: false, error: "campaign_scale_failed" };
    }
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
  // Discord's health-check ping must always pass.
  if (interaction?.type === 1) {
    return { type: 1 };
  }

  const discord_actions_enabled = await getSystemFlag("discord_actions_enabled");
  if (!discord_actions_enabled) {
    return ephemeralMessage("⛔ Discord actions are currently disabled via system control.");
  }

  const context = extractMemberContext(interaction);
  const role_ids = context.role_ids;

  // ── MESSAGE_COMPONENT (button click) ──────────────────────────────────────
  if (interaction.type === 3) {
    const custom_id = String(interaction?.data?.custom_id ?? "");

    if (custom_id.startsWith("target_builder:")) {
      return handleTargetBuilderComponent({ interaction, custom_id, context });
    }

    // Legacy approval prefix (existing approval flows)
    if (custom_id.startsWith(APPROVE_PREFIX) || custom_id.startsWith(REJECT_PREFIX)) {
      return handleApproval({ interaction, custom_id });
    }

    // New approval: prefix (cinematic approval buttons from discord-components.js)
    if (custom_id.startsWith("approval:approve:") || custom_id.startsWith("approval:deny:")) {
      const is_approve = custom_id.startsWith("approval:approve:");
      const new_token  = is_approve
        ? custom_id.slice("approval:approve:".length)
        : custom_id.slice("approval:deny:".length);
      // Delegate to handleApproval using legacy format so lookup works.
      return handleApproval({
        interaction,
        custom_id: is_approve
          ? `${APPROVE_PREFIX}${new_token}`
          : `${REJECT_PREFIX}${new_token}`,
      });
    }

    // Shortcut buttons — re-run commands inline.
    const ctx = extractMemberContext(interaction);
    if (custom_id === "mission:refresh") return handleMissionStatus(ctx);
    if (custom_id === "mission:preflight") return handleLaunchPreflight(ctx);
    if (custom_id === "queue:cockpit") return handleQueueCockpit(ctx);
    if (custom_id === "preflight:recheck") return handleLaunchPreflight(ctx);
    if (custom_id === "preflight:scan_feeder") return handleFeederScan({ options_array: [], context: ctx, interaction });
    if (custom_id === "templates:audit") return handleTemplatesAudit(ctx);
    if (custom_id === "templates:stage1") return handleTemplatesStage1(ctx);

    if (custom_id === "feeder:auto_scan") {
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleFeederAutoScan({ interaction, title: "SMS Feeder Auto Scan" });
    }
    if (custom_id === "feeder:scan_next") {
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleFeederAutoScan({
        interaction,
        offsets: FEEDER_SCAN_NEXT_OFFSETS,
        title: "SMS Feeder Scan Next Offsets",
      });
    }
    if (custom_id.startsWith("feeder:dry_launch:")) {
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleFeederLaunchButton({
        interaction,
        payload_token: custom_id.slice("feeder:dry_launch:".length),
        dry_run: true,
      });
    }
    if (custom_id.startsWith("feeder:launch:")) {
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleFeederLaunchButton({
        interaction,
        payload_token: custom_id.slice("feeder:launch:".length),
        dry_run: false,
      });
    }
    if (custom_id === "feeder:queue_status") return handleFeederQueueStatus();
    if (custom_id === "feeder:queue_run_dry") {
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleFeederQueueRun({ interaction, dry_run: true, limit: 25 });
    }
    if (custom_id === "feeder:queue_run_live") {
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleFeederQueueRun({ interaction, dry_run: false, limit: 25 });
    }
    if (custom_id.startsWith("feeder:")) {
      return ephemeralMessage(`Unsupported feeder action: \`${custom_id}\`.`);
    }

    if (custom_id.startsWith("target:create_campaign:")) {
      const ck = custom_id.slice("target:create_campaign:".length);
      return cinematicMessage({
        embeds: [buildSuccessEmbed({
          title: "Create Campaign",
          description: `Use \`/campaign create\` to create a campaign${ck ? ` for key \`${ck}\`` : ""}.`,
        })],
        ephemeral: true,
      });
    }
    if (custom_id.startsWith("target:run_again:")) return handleMissionStatus(ctx);
    if (custom_id === "target:template_audit") return handleTemplatesAudit(ctx);
    if (custom_id === "target:launch_preflight") return handleLaunchPreflight(ctx);

    if (custom_id === "campaign:preflight") return handleLaunchPreflight(ctx);
    if (custom_id.startsWith("campaign:scan:")) {
      const ck = custom_id.slice("campaign:scan:".length);
      return cinematicMessage({
        embeds: [buildSuccessEmbed({
          title: "Scan Campaign",
          description: `Use \`/target scan\` with the campaign's market/asset/strategy to run a dry-run scan for \`${ck}\`.`,
        })],
        ephemeral: true,
      });
    }
    if (custom_id.startsWith("campaign:scale:")) {
      const ck = custom_id.slice("campaign:scale:".length);
      return cinematicMessage({
        embeds: [buildSuccessEmbed({
          title: "Scale Campaign",
          description: `Use \`/campaign scale\` with campaign key \`${ck}\` to update the daily cap.`,
        })],
        ephemeral: true,
      });
    }
    if (custom_id.startsWith("campaign:pause:")) {
      const ck = custom_id.slice("campaign:pause:".length);
      if (!checkPermission(ctx.role_ids, ["owner", "sms_ops"])) {
        return deniedResponse("Requires **SMS Ops** or **Owner** to pause a campaign.");
      }
      return handleCampaignPause({ options_array: [{ name: "campaign_id", value: ck }], context: ctx });
    }
    if (custom_id.startsWith("campaign:resume:")) {
      const ck = custom_id.slice("campaign:resume:".length);
      if (!checkPermission(ctx.role_ids, ["owner", "sms_ops"])) {
        return deniedResponse("Requires **SMS Ops** or **Owner** to resume a campaign.");
      }
      return handleCampaignResume({
        options_array: [{ name: "campaign_id", value: ck }],
        context: ctx,
        interaction,
      });
    }
    const campaign_status_match = custom_id.match(/^campaign:(approve_launch|approve|launch|close):(.+)$/);
    if (campaign_status_match) {
      const [, action, ck] = campaign_status_match;
      const is_close = action === "close";
      if (!checkPermission(ctx.role_ids, ["owner", "sms_ops"])) {
        return ephemeralMessage(
          is_close
            ? "🚫 Only **Owner** or **SMS Ops** can close a campaign."
            : "🚫 Only **Owner** or **SMS Ops** can approve a campaign launch."
        );
      }
      const next_status = is_close ? "closed" : "active";
      try {
        const db = getDb();
        const { error } = await db
          .from("campaign_targets")
          .update({ status: next_status, updated_at: new Date().toISOString() })
          .eq("campaign_key", ck);
        if (error) throw error;
        return updateMessage(
          is_close
            ? `🔒 Campaign \`${ck}\` closed.`
            : `✅ Campaign \`${ck}\` approved and set to **active**.`
        );
      } catch {
        return updateMessage(
          is_close
            ? `⚠️ Could not close campaign \`${ck}\` — check logs or set manually.`
            : `⚠️ Could not activate campaign \`${ck}\` — check logs or set manually.`
        );
      }
    }
    if (custom_id.startsWith("campaign:")) {
      return ephemeralMessage(
        `⚠️ Unsupported campaign action: \`${custom_id}\`\nThis campaign button is not yet wired. Please report it to **Tech Ops**.`
      );
    }

    if (custom_id === "territory:create_target") {
      return cinematicMessage({
        embeds: [buildSuccessEmbed({
          title: "Create Target",
          description: "Use `/campaign create` to add a new territory target.",
        })],
        ephemeral: true,
      });
    }
    if (custom_id === "territory:scan_active") return handleTerritoryMap(ctx);
    if (custom_id === "territory:mission_status") return handleMissionStatus(ctx);

    if (custom_id.startsWith("queue:run:")) {
      const run_limit = Number(custom_id.split(":")[2]) || 10;
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return deniedResponse("Requires **Tech Ops** or **Owner**.");
      }
      return handleQueueRun({ options_array: [{ name: "limit", value: run_limit }], context: ctx });
    }

    // Ops OS button actions from notify-discord-ops
    if (custom_id.startsWith("ops_action:")) {
      const [, action, action_arg] = custom_id.split(":");

      if (action === "show_queue_health") {
        return handleQueueCockpit(ctx);
      }

      if (action === "run_queue_dry") {
        if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
          return deniedResponse("Requires **Tech Ops** or **Owner**.");
        }
        return handleQueueRun({
          options_array: [
            { name: "limit", value: 25 },
            { name: "dry_run", value: true },
          ],
          context: ctx,
        });
      }

      if (action === "run_queue_live") {
        if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
          return deniedResponse("Requires **Tech Ops** or **Owner**.");
        }
        return handleQueueRun({ options_array: [{ name: "limit", value: 25 }], context: ctx });
      }

      if (action === "sync_podio") {
        if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
          return deniedResponse("Requires **Tech Ops** or **Owner**.");
        }
        return handleSyncPodio({ options_array: [], context: ctx });
      }

      if (action === "approve_campaign" && action_arg) {
        return routeDiscordInteraction({
          ...interaction,
          data: {
            ...(interaction?.data || {}),
            custom_id: `approval:campaign_scale:${action_arg}`,
          },
        });
      }

      if (action === "reject_campaign" && action_arg) {
        return routeDiscordInteraction({
          ...interaction,
          data: {
            ...(interaction?.data || {}),
            custom_id: `approval:hold:${action_arg}`,
          },
        });
      }

      return updateMessage(`⚠️ Action not wired yet: ${action || "unknown"}`);
    }

    // SMS reply action buttons (legacy inbound alert cards)
    // custom_id format: sms_action:{action_type}:{message_event_id}
    if (custom_id.startsWith("sms_action:")) {
      const [, action, message_event_id] = custom_id.split(":");

      // Import handlers
      const {
        handleOpenSmsReplyModal,
        handleSendSuggestedSmsReply,
        handleManualSmsReply,
        handleSuppressNumber,
        handleMarkHotLead,
      } = await import("@/lib/discord/discord-action-handlers/handle-sms-reply.js").catch(() => ({}));

      if (action === "open_modal" && handleOpenSmsReplyModal) {
        return await handleOpenSmsReplyModal({
          interaction,
          message_event_id,
        });
      }

      if (action === "send_suggested" && handleSendSuggestedSmsReply) {
        // Approve template-based reply by message_event_id; endpoint performs selection/render.
        return await handleSendSuggestedSmsReply({
          interaction,
          message_event_id,
          template_id: interaction?.data?.template_id || "",
          discord_user_id: ctx.user_id,
          channel_id: ctx.channel_id,
          message_id: interaction?.message?.id,
        });
      }

      if (action === "suppress" && handleSuppressNumber) {
        return await handleSuppressNumber({
          interaction,
          message_event_id,
          from_phone_number: interaction?.data?.from_phone || "",
          discord_user_id: ctx.user_id,
          channel_id: ctx.channel_id,
        });
      }

      if (action === "mark_hot" && handleMarkHotLead) {
        return await handleMarkHotLead({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
          channel_id: ctx.channel_id,
        });
      }

      return updateMessage(`⚠️ SMS action not wired yet: ${action || "unknown"}`);
    }

    // Compact inbound SMS review actions
    // custom_id format: sr:{action}:{message_event_id}
    if (custom_id.startsWith("sr:")) {
      const [, action, message_event_id] = custom_id.split(":");
      const {
        handleApproveSendNowSmsReply,
        handleManualSmsReply,
        handleCancelAutopilotSmsReply,
        handleNotInterestedSmsReply,
        handleWrongNumberSmsReply,
        handleOptOutSmsReply,
        handleOpenRecord,
      } = await import("@/lib/discord/discord-action-handlers/handle-sms-reply.js").catch(() => ({}));

      if (action === "a" && handleApproveSendNowSmsReply) {
        return await handleApproveSendNowSmsReply({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
          channel_id: ctx.channel_id,
          message_id: interaction?.message?.id,
        });
      }

      if (action === "c" && handleCancelAutopilotSmsReply) {
        return await handleCancelAutopilotSmsReply({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
          channel_id: ctx.channel_id,
          message_id: interaction?.message?.id,
        });
      }

      if (action === "m" && handleManualSmsReply) {
        return await handleManualSmsReply({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
          channel_id: ctx.channel_id,
          message_id: interaction?.message?.id,
        });
      }

      if (action === "ni" && handleNotInterestedSmsReply) {
        return await handleNotInterestedSmsReply({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
        });
      }

      if (action === "wn" && handleWrongNumberSmsReply) {
        return await handleWrongNumberSmsReply({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
        });
      }

      if (action === "oo" && handleOptOutSmsReply) {
        return await handleOptOutSmsReply({
          interaction,
          message_event_id,
          discord_user_id: ctx.user_id,
        });
      }

      return updateMessage(`⚠️ Unsupported SMS review action: ${action || "unknown"}`);
    }

    if (custom_id.startsWith("context:open_record:")) {
      const message_event_id = custom_id.split(":").slice(2).join(":");
      const { handleOpenRecord } = await import("@/lib/discord/discord-action-handlers/handle-sms-reply.js").catch(() => ({}));
      if (handleOpenRecord) {
        return await handleOpenRecord({ message_event_id });
      }
      return updateMessage("⚠️ Open record handler not available.");
    }

    // Briefing quick-action buttons
    if (custom_id === "briefing:refresh") {
      return handleBriefingToday({ options_array: [], context: ctx, interaction });
    }
    if (custom_id === "briefing:hot_leads") {
      return handleHotleads({ options_array: [], context: ctx });
    }
    if (custom_id === "briefing:queue_scan") {
      return handleQueueCockpit(ctx);
    }
    if (custom_id === "briefing:scale_campaign" || custom_id === "briefing:export") {
      return cinematicMessage({
        embeds: [buildSuccessEmbed({
          title:       custom_id === "briefing:scale_campaign" ? "Scale Campaign" : "Export Summary",
          description: custom_id === "briefing:scale_campaign"
            ? "Use `/campaign scale` with a campaign key to update the daily cap."
            : "Export is not yet implemented. Use `/briefing today` to regenerate.",
        })],
        ephemeral: true,
      });
    }

    // ── Proactive ops approval buttons ──────────────────────────────────────
    // custom_id format: approval:campaign_scale:<requestKey>
    //                   approval:campaign_pause:<requestKey>
    //                   approval:hold:<requestKey>
    //                   approval:inspect:<requestKey>

    if (
      custom_id.startsWith("approval:campaign_scale:") ||
      custom_id.startsWith("approval:campaign_pause:") ||
      custom_id.startsWith("approval:hold:")           ||
      custom_id.startsWith("approval:inspect:")
    ) {
      const is_scale  = custom_id.startsWith("approval:campaign_scale:");
      const is_pause  = custom_id.startsWith("approval:campaign_pause:");
      const is_hold   = custom_id.startsWith("approval:hold:");
      const is_inspect = custom_id.startsWith("approval:inspect:");

      const prefix = is_scale   ? "approval:campaign_scale:"
                   : is_pause   ? "approval:campaign_pause:"
                   : is_hold    ? "approval:hold:"
                   : "approval:inspect:";
      const request_key = custom_id.slice(prefix.length);

      if (!request_key) {
        return updateMessage("❌ Invalid ops approval request key.");
      }

      // Permission check — scale and pause require Owner or SMS Ops.
      if ((is_scale || is_pause) && !checkPermission(ctx.role_ids, ["owner", "sms_ops"])) {
        await writeDiscordActionAudit({
          request_key,
          action_type:    is_scale ? "campaign_scale" : "campaign_pause",
          actor_user_id:  ctx.user_id,
          actor_username: ctx.username,
          guild_id:       ctx.guild_id,
          outcome:        "unauthorized",
        });
        return updateMessage("🚫 Only **Owner** or **SMS Ops** can approve this action.");
      }

      // Load the approval request.
      let approval_req = null;
      try {
        const db_ctx = _router_deps.supabase_override ?? supabase;
        const { data } = await db_ctx
          .from("campaign_approval_requests")
          .select("*")
          .eq("request_key", request_key)
          .maybeSingle();
        // Treat expired as unavailable.
        if (data && data.expires_at && new Date(data.expires_at) < new Date()) {
          approval_req = null;
        } else {
          approval_req = data;
        }
      } catch { /* non-fatal — approval_req stays null */ }

      if (is_inspect) {
        // Inspect is informational — return campaign snapshot without mutating.
        await writeDiscordActionAudit({
          request_key,
          action_type:    "inspect",
          actor_user_id:  ctx.user_id,
          actor_username: ctx.username,
          guild_id:       ctx.guild_id,
          outcome:        "inspected",
          details:        approval_req ? { campaign_key: approval_req.campaign_key } : null,
        });

        const inspect_embed = buildOpsNotificationEmbed({
          title:    `Inspection — ${approval_req?.campaign_key ?? request_key}`,
          message:  approval_req?.reason ?? "No additional context available.",
          severity: "info",
          campaign_key: approval_req?.campaign_key ?? null,
          metrics:      approval_req?.metrics ?? null,
        });
        return updateMessage(null, { embeds: [inspect_embed] });
      }

      if (!approval_req) {
        return updateMessage("⚠️ Approval request not found or has expired.");
      }

      if (approval_req.status !== "pending") {
        return updateMessage(`⚠️ This request has already been **${approval_req.status}**.`);
      }

      if (is_hold) {
        // Hold — record audit entry but don't mutate the campaign.
        await resolveApprovalRequest(request_key, "cancelled", {
          user_id:  ctx.user_id,
          username: ctx.username,
        });
        await writeDiscordActionAudit({
          request_key,
          action_type:    is_scale ? "campaign_scale" : "campaign_pause",
          actor_user_id:  ctx.user_id,
          actor_username: ctx.username,
          guild_id:       ctx.guild_id,
          outcome:        "held",
          details:        { campaign_key: approval_req.campaign_key },
        });
        return updateMessage(`⏸ Held by <@${ctx.user_id}> — no action taken on \`${approval_req.campaign_key}\`.`, {
          allowed_mentions: { parse: [] },
        });
      }

      // Scale approval — update daily_cap.
      if (is_scale) {
        const resolved = await resolveApprovalRequest(request_key, "approved", {
          user_id:  ctx.user_id,
          username: ctx.username,
        });

        if (!resolved) {
          return updateMessage("⚠️ Could not update approval status — request may have changed.");
        }

        try {
          const db_ctx = _router_deps.supabase_override ?? supabase;
          await db_ctx
            .from("campaign_targets")
            .update({
              daily_cap:  Number(approval_req.proposed_cap),
              updated_at: new Date().toISOString(),
            })
            .eq("campaign_key", approval_req.campaign_key);
        } catch { /* non-fatal — audit still written */ }

        await writeDiscordActionAudit({
          request_key,
          action_type:    "campaign_scale",
          actor_user_id:  ctx.user_id,
          actor_username: ctx.username,
          guild_id:       ctx.guild_id,
          outcome:        "approved",
          details: {
            campaign_key: approval_req.campaign_key,
            current_cap:  approval_req.current_cap,
            proposed_cap: approval_req.proposed_cap,
          },
        });

        const scale_embed = buildCampaignScaleApprovalEmbed({
          campaign_key: approval_req.campaign_key,
          market:       approval_req.market,
          asset:        approval_req.asset,
          strategy:     approval_req.strategy,
          current_cap:  approval_req.current_cap,
          proposed_cap: approval_req.proposed_cap,
          metrics:      approval_req.metrics,
          request_key,
          reason:       approval_req.reason,
        });

        return updateMessage(`✅ Scale approved by <@${ctx.user_id}>.`, {
          embeds:           [scale_embed],
          allowed_mentions: { parse: [] },
        });
      }

      // Pause approval — pause the campaign.
      if (is_pause) {
        const resolved = await resolveApprovalRequest(request_key, "approved", {
          user_id:  ctx.user_id,
          username: ctx.username,
        });

        if (!resolved) {
          return updateMessage("⚠️ Could not update approval status — request may have changed.");
        }

        try {
          const db_ctx = _router_deps.supabase_override ?? supabase;
          await db_ctx
            .from("campaign_targets")
            .update({ paused: true, updated_at: new Date().toISOString() })
            .eq("campaign_key", approval_req.campaign_key);
        } catch { /* non-fatal */ }

        await writeDiscordActionAudit({
          request_key,
          action_type:    "campaign_pause",
          actor_user_id:  ctx.user_id,
          actor_username: ctx.username,
          guild_id:       ctx.guild_id,
          outcome:        "approved",
          details:        { campaign_key: approval_req.campaign_key, reason: approval_req.reason },
        });

        const pause_embed = buildCampaignPauseAlertEmbed({
          campaign_key: approval_req.campaign_key,
          reason:       approval_req.reason,
          opt_out_rate: approval_req.metrics?.opted_out && approval_req.metrics?.delivered
            ? approval_req.metrics.opted_out / approval_req.metrics.delivered
            : null,
          failed_rate: approval_req.metrics?.failed && approval_req.metrics?.sent
            ? approval_req.metrics.failed / approval_req.metrics.sent
            : null,
          request_key,
        });

        return updateMessage(`⏸ Pause approved by <@${ctx.user_id}>.`, {
          embeds:           [pause_embed],
          allowed_mentions: { parse: [] },
        });
      }
    }

    // ── alerts mode buttons ───────────────────────────────────────────────────
    if (custom_id.startsWith("alerts:mode:")) {
      const mode = custom_id.slice("alerts:mode:".length);
      if (!checkPermission(ctx.role_ids, ["owner", "tech_ops"])) {
        return ephemeralMessage("🚫 Requires **Tech Ops** or **Owner**.");
      }
      return handleAlertsMode({ options_array: [{ name: "mode", value: mode }], context: ctx });
    }

    // ── morning check-in buttons ───────────────────────────────────────────
    if (custom_id.startsWith("checkin:")) {
      const action = custom_id.slice("checkin:".length);
      if (action === "review_hot_leads") {
        return handleHotleads({ options_array: [], context: ctx });
      }
      if (action === "run_feeder_dry_run") {
        return handleFeederScan({ options_array: [{ name: "limit", value: 10 }], context: ctx, interaction });
      }
      if (action === "scale_winning_campaign") {
        return cinematicMessage({
          embeds: [buildSuccessEmbed({ title: "Scale Campaign", description: "Use `/campaign scale` with a campaign key to increase the daily cap." })],
          ephemeral: true,
        });
      }
      if (action === "hold_outreach") {
        return cinematicMessage({
          embeds: [buildSuccessEmbed({ title: "Hold Outreach Today", description: "Post a `/queue status` check and set campaigns to paused via `/campaign pause` to hold outreach for today." })],
          ephemeral: true,
        });
      }
      if (action === "approve_suggested_move") {
        return cinematicMessage({
          embeds: [buildSuccessEmbed({ title: "Approve Suggested Move", description: "Review the recommended next moves above and use the appropriate command to execute." })],
          ephemeral: true,
        });
      }
      return ephemeralMessage(`⚠️ Unknown check-in action: \`${action}\``);
    }

    // ── Unknown button — structured diagnostic ephemeral ─────────────────────
    warn("discord.button.unknown_custom_id", { custom_id, user_id: ctx.user_id });
    try {
      await supabase.from("discord_command_events").insert({
        created_at:     new Date().toISOString(),
        guild_id:       ctx.guild_id,
        channel_id:     ctx.channel_id,
        user_id:        ctx.user_id,
        username:       ctx.username,
        command_name:   "button",
        subcommand:     custom_id,
        action_outcome: "unknown_button",
        result_summary: null,
      });
    } catch { /* non-fatal */ }
    return ephemeralMessage(`⚠️ Unsupported interaction: \`${custom_id}\`\nThis button is not yet wired. Please report it to **Tech Ops**.`);
  }

  // ── MODAL_SUBMIT (form submission) ────────────────────────────────────────
  if (interaction.type === 5) {
    const modal_custom_id = String(interaction?.data?.custom_id ?? "");

    // SMS reply modal submission
    if (modal_custom_id.startsWith("sms_reply_modal:") || modal_custom_id.startsWith("sms_reply_manual_modal:")) {
      const { handleSubmitSmsReplyModal } = await import(
        "@/lib/discord/discord-action-handlers/handle-sms-reply.js"
      ).catch(() => ({}));

      if (handleSubmitSmsReplyModal) {
        const result = await handleSubmitSmsReplyModal({
          interaction,
          discord_user_id: context.user_id,
          channel_id: context.channel_id,
          message_id: interaction?.message?.id,
        });

        if (result?.bridge_endpoint) {
          // Bridge to endpoint
          try {
            const bridge_response = await callInternal(
              result.bridge_endpoint,
              {
                method: result.method || "POST",
                body: result.bridge_payload,
              }
            );

            if (bridge_response?.ok) {
              return updateMessage(
                `✅ Reply queued to ${(
                  bridge_response?.to_phone_number || ""
                ).slice(-4).padStart(4, "*")}`
              );
            } else {
              return updateMessage(
                `🚫 Reply blocked: ${bridge_response?.message || "Unknown reason"}`
              );
            }
          } catch (err) {
            return updateMessage(`❌ Error queuing reply: ${err?.message}`);
          }
        }

        return result;
      }

      return updateMessage("⚠️ Modal handler not available.");
    }

    return updateMessage("⚠️ Unknown modal interaction.");
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

  const cmd_label = sub_name ? `${command_name}.${sub_name}` : command_name;
  const user_last4 = String(context.user_id ?? "").slice(-4) || "unknown";
  const guild_last4 = String(context.guild_id ?? "").slice(-4) || "unknown";
  const cmd_started = Date.now();

  info("discord.command.started", { command: cmd_label, user_last4, guild_last4 });

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
      } else if (sub_name === "cockpit") {
        response = await handleQueueCockpit(context);
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
      } else if (sub_name === "scan") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederScan({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "launch") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederLaunch({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "scan_offsets") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederAutoScan({
            options_array: sub_opts,
            context,
            interaction,
            title: "SMS Feeder Auto Scan",
          });
        }
      } else if (sub_name === "launch_batch") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederLaunchPreview({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "queue_status") {
        response = await handleFeederQueueStatus();
      } else if (sub_name === "run_queue_dry") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederQueueRun({ interaction, dry_run: true, limit: 25 });
        }
      } else if (sub_name === "run_queue_live") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleFeederQueueRun({ interaction, dry_run: false, limit: 25 });
        }
      } else {
        response = errorResponse(`Unknown /feeder subcommand: ${sub_name}`);
      }

    // ── /target ──────────────────────────────────────────────────────────────
    } else if (command_name === "target-build") {
      if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
        response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
      } else {
        response = await handleTargetBuild({ context, interaction });
      }

    // ── /target-scan ──────────────────────────────────────────────────────
    } else if (command_name === "target-scan") {
      if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
        response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
      } else {
        response = await handleTargetScan({
          options_array: top_options,
          context,
          interaction,
          scan_mode: "core",
          force_property_first: false,
        });
      }

    // ── /target-property ───────────────────────────────────────────────────
    } else if (command_name === "target-property") {
      if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
        response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
      } else {
        response = await handleTargetScan({
          options_array: top_options,
          context,
          interaction,
          scan_mode: "property_first",
          force_property_first: true,
        });
      }

    // ── /target (legacy compatibility) ────────────────────────────────────
    } else if (command_name === "target") {
      if (sub_name === "scan") {
        if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
          response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleTargetScan({
            options_array: sub_opts,
            context,
            interaction,
            scan_mode: "core",
            force_property_first: false,
          });
        }
      } else if (sub_name === "property") {
        if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
          response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleTargetScan({
            options_array: sub_opts,
            context,
            interaction,
            scan_mode: "property_first",
            force_property_first: true,
          });
        }
      } else {
        response = errorResponse(`Unknown /target subcommand: ${sub_name}`);
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
      } else if (sub_name === "create") {
        if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
          response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleCampaignCreate({ options_array: sub_opts, context });
        }
      } else if (sub_name === "inspect") {
        if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops", "acquisitions"])) {
          response = deniedResponse("Requires **Acquisitions**, **SMS Ops**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleCampaignInspect({ options_array: sub_opts, context });
        }
      } else if (sub_name === "scale") {
        response = await handleCampaignScale({ options_array: sub_opts, context, interaction });
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
      } else if (sub_name === "inspect") {
        if (!checkPermission(role_ids, ["owner", "acquisitions", "tech_ops"])) {
          response = deniedResponse("Requires **Acquisitions**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleLeadInspect({ options_array: sub_opts, context });
        }
      } else {
        response = errorResponse(`Unknown /lead subcommand: ${sub_name}`);
      }

    // ── /mission ─────────────────────────────────────────────────────────────
    } else if (command_name === "mission") {
      if (sub_name === "status") {
        response = await handleMissionStatus(context);
      } else {
        response = errorResponse(`Unknown /mission subcommand: ${sub_name}`);
      }

    // ── /launch ──────────────────────────────────────────────────────────────
    } else if (command_name === "launch") {
      if (sub_name === "preflight") {
        response = await handleLaunchPreflight(context);
      } else {
        response = errorResponse(`Unknown /launch subcommand: ${sub_name}`);
      }

    // ── /templates ───────────────────────────────────────────────────────────
    } else if (command_name === "templates") {
      if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
        response = deniedResponse("Requires **Tech Ops** or **Owner**.");
      } else if (sub_name === "audit") {
        response = await handleTemplatesAudit(context);
      } else if (sub_name === "stage1") {
        response = await handleTemplatesStage1(context);
      } else {
        response = errorResponse(`Unknown /templates subcommand: ${sub_name}`);
      }

    // ── /hotleads ────────────────────────────────────────────────────────────
    } else if (command_name === "hotleads") {
      if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops", "acquisitions"])) {
        response = deniedResponse("Requires a recognised team role.");
      } else {
        response = await handleHotleads({ options_array: top_options, context });
      }

    // ── /alerts ──────────────────────────────────────────────────────────────
    } else if (command_name === "alerts") {
      if (sub_name === "mode") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleAlertsMode({ options_array: sub_opts, context });
        }
      } else {
        response = errorResponse(`Unknown /alerts subcommand: ${sub_name}`);
      }

    // ── /territory ───────────────────────────────────────────────────────────
    } else if (command_name === "territory") {
      if (sub_name === "map") {
        if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops", "acquisitions"])) {
          response = deniedResponse("Requires a recognised team role.");
        } else {
          response = await handleTerritoryMap(context);
        }
      } else {
        response = errorResponse(`Unknown /territory subcommand: ${sub_name}`);
      }

    // ── /conquest ────────────────────────────────────────────────────────────
    } else if (command_name === "conquest") {
      if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops"])) {
        response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
      } else {
        response = await handleConquest(context);
      }

    // ── /email ────────────────────────────────────────────────────────────────
    } else if (command_name === "email") {
      if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
        response = deniedResponse("Requires **Tech Ops** or **Owner**.");
      } else if (sub_name === "cockpit") {
        response = await handleEmailCockpit({ context, interaction });
      } else if (sub_name === "preview") {
        response = await handleEmailPreview({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "send-test") {
        response = await handleEmailSendTest({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "queue") {
        response = await handleEmailQueueStatus({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "suppression") {
        response = await handleEmailSuppression({ context, interaction });
      } else if (sub_name === "stats") {
        response = await handleEmailStats({ context, interaction });
      } else {
        response = errorResponse(`Unknown /email subcommand: ${sub_name}`);
      }

    // ── /replay ────────────────────────────────────────────────────────────────
    } else if (command_name === "replay") {
      if (sub_name === "inbound") {
        if (!checkPermission(role_ids, ["sms_ops", "tech_ops", "owner"])) {
          response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleReplayInbound({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "owner") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleReplayOwner({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "template") {
        if (!checkPermission(role_ids, ["sms_ops", "tech_ops", "owner"])) {
          response = deniedResponse("Requires **SMS Ops**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleReplayTemplate({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "batch") {
        if (!checkPermission(role_ids, ["owner", "tech_ops"])) {
          response = deniedResponse("Requires **Tech Ops** or **Owner**.");
        } else {
          response = await handleReplayBatch({ options_array: sub_opts, context, interaction });
        }
      } else {
        response = errorResponse(`Unknown /replay subcommand: ${sub_name}`);
      }

    // ── /wires ─────────────────────────────────────────────────────────────────
    } else if (command_name === "wires") {
      if (sub_name === "cockpit") {
        if (!checkPermission(role_ids, ["owner", "closings", "tech_ops"])) {
          response = deniedResponse("Requires **Closings**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleWiresCockpit({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "expected") {
        if (!checkPermission(role_ids, ["owner", "closings"])) {
          response = deniedResponse("Requires **Closings** or **Owner**.");
        } else {
          response = await handleWiresExpected({ options_array: sub_opts, context });
        }
      } else if (sub_name === "received") {
        if (!checkPermission(role_ids, ["owner"])) {
          response = deniedResponse("Requires **Owner** role.");
        } else {
          response = await handleWiresReceived({ options_array: sub_opts, context });
        }
      } else if (sub_name === "cleared") {
        if (!checkPermission(role_ids, ["owner"])) {
          response = deniedResponse("Requires **Owner** role.");
        } else {
          response = await handleWiresCleared({ options_array: sub_opts, context });
        }
      } else if (sub_name === "forecast") {
        if (!checkPermission(role_ids, ["owner", "closings", "tech_ops"])) {
          response = deniedResponse("Requires **Closings**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleWiresForecast({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "deal") {
        if (!checkPermission(role_ids, ["owner", "closings", "tech_ops"])) {
          response = deniedResponse("Requires **Closings**, **Tech Ops**, or **Owner**.");
        } else {
          response = await handleWiresDeal({ options_array: sub_opts, context, interaction });
        }
      } else if (sub_name === "reconcile") {
        if (!checkPermission(role_ids, ["owner", "closings"])) {
          response = deniedResponse("Requires **Closings** or **Owner**.");
        } else {
          response = await handleWiresReconcile({ options_array: sub_opts, context, interaction });
        }
      } else {
        response = errorResponse(`Unknown /wires subcommand: ${sub_name}`);
      }

    // ── /briefing ─────────────────────────────────────────────────────────────
    } else if (command_name === "briefing") {
      if (!checkPermission(role_ids, ["owner", "tech_ops", "sms_ops", "closings", "acquisitions"])) {
        response = deniedResponse("Requires **Owner**, **Tech Ops**, **SMS Ops**, **Closings**, or **Acquisitions**.");
      } else if (sub_name === "today") {
        response = handleBriefingToday({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "yesterday") {
        response = handleBriefingYesterday({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "week") {
        response = handleBriefingWeek({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "market") {
        response = handleBriefingMarket({ options_array: sub_opts, context, interaction });
      } else if (sub_name === "agent") {
        response = handleBriefingAgent({ options_array: sub_opts, context, interaction });
      } else {
        response = errorResponse(`Unknown /briefing subcommand: ${sub_name}`);
      }

    // ── /reply-sms ────────────────────────────────────────────────────────────
    } else if (command_name === "reply-sms") {
      if (!checkPermission(role_ids, ["owner", "sms_ops"])) {
        response = deniedResponse("Requires **SMS Ops** or **Owner**.");
      } else {
        const { handleReplySmsCommand } = await import(
          "@/lib/discord/discord-slash-commands/reply-sms-command.js"
        ).catch(() => ({}));

        if (handleReplySmsCommand) {
          const cmd_opts = {};
          for (const opt of sub_opts) {
            cmd_opts[opt.name] = opt.value;
          }
          response = await handleReplySmsCommand(context, cmd_opts);
        } else {
          response = errorResponse("Reply command handler not available.");
        }
      }

    } else {
      response = errorResponse(`Unknown command: /${command_name}`);
    }
  } catch {
    logError("discord.command.failed", { command: cmd_label, user_last4, guild_last4 });
    response = errorResponse("Unexpected error processing your command.");
  }

  info("discord.command.completed", {
    command:    cmd_label,
    user_last4,
    guild_last4,
    duration_ms: Date.now() - cmd_started,
    response_type: response?.type ?? null,
  });
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
