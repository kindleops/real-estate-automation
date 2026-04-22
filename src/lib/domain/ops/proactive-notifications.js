/**
 * proactive-notifications.js
 *
 * Domain module for proactive ops notifications.
 *
 * Responsibilities:
 *   - Analyse campaign health metrics and produce scale/pause recommendations
 *   - Dedup-safe upsert of ops_notifications rows
 *   - Create approval requests in campaign_approval_requests
 *   - Dispatch Discord webhook notifications
 *   - Orchestrate a full proactive ops check run
 *
 * All Supabase calls use injectable deps so the module is unit-testable
 * without a live database connection.
 *
 * Safety constraints:
 *   - Never sends SMS (read-only analysis + Discord alerts only)
 *   - No campaign mutations without an explicit approval request
 *   - Notifications are deduped by notification_key
 */

import { supabase } from "@/lib/supabase/client.js";
import { child }    from "@/lib/logging/logger.js";

const logger = child({ module: "domain.ops.proactive-notifications" });

// ---------------------------------------------------------------------------
// Dependency injection (test support)
// ---------------------------------------------------------------------------

let _deps = { supabase_override: null, fetch_override: null };

export function __setProactiveNotificationsDeps(overrides) {
  _deps = { ..._deps, ...overrides };
}

export function __resetProactiveNotificationsDeps() {
  _deps = { supabase_override: null, fetch_override: null };
}

function getDb() {
  return _deps.supabase_override ?? supabase;
}

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

export const THRESHOLDS = {
  MIN_SAMPLE_SIZE:          50,
  POSITIVE_REPLY_RATE:      0.05,   // 5 % → scale recommended
  OPT_OUT_RATE_MAX:         0.03,   // 3 % → warning
  FAILED_RATE_MAX:          0.05,   // 5 % → warning
  PAUSE_OPT_OUT_THRESHOLD:  0.08,   // 8 % → pause recommended
  PAUSE_FAILED_THRESHOLD:   0.10,   // 10 % → pause recommended
  SCALE_COOLDOWN_HOURS:     24,     // Hours between scale recommendations for same campaign
  NOTIFICATION_EXPIRY_HOURS: 48,    // Hours before a notification is considered stale
  APPROVAL_EXPIRY_HOURS:    24,     // Hours before an approval request expires
};

// ---------------------------------------------------------------------------
// Campaign health analysis
// ---------------------------------------------------------------------------

/**
 * Analyse campaign metrics and return a health recommendation.
 *
 * @param {object} campaign - campaign_targets row
 * @param {object} metrics  - { sent, delivered, replied, opted_out, failed, sample_size }
 * @returns {{ scale_recommended: boolean, pause_recommended: boolean, reason: string, confidence: number }}
 */
export function analyzeCampaignHealth(campaign = {}, metrics = {}) {
  const {
    sent        = 0,
    delivered   = 0,
    replied     = 0,
    opted_out   = 0,
    failed      = 0,
  } = metrics;

  const sample_size = metrics.sample_size ?? sent;

  if (sample_size < THRESHOLDS.MIN_SAMPLE_SIZE) {
    return {
      scale_recommended: false,
      pause_recommended: false,
      reason:     `Sample size too small (${sample_size} < ${THRESHOLDS.MIN_SAMPLE_SIZE})`,
      confidence: 0,
    };
  }

  const base          = delivered > 0 ? delivered : sample_size;
  const reply_rate    = base > 0 ? replied    / base : 0;
  const opt_out_rate  = base > 0 ? opted_out  / base : 0;
  const failed_rate   = sent  > 0 ? failed    / sent  : 0;

  // Pause signals take priority.
  if (opt_out_rate >= THRESHOLDS.PAUSE_OPT_OUT_THRESHOLD) {
    return {
      scale_recommended: false,
      pause_recommended: true,
      reason:     `Opt-out rate critical: ${(opt_out_rate * 100).toFixed(1)}% (threshold ${THRESHOLDS.PAUSE_OPT_OUT_THRESHOLD * 100}%)`,
      confidence: Math.min(1, opt_out_rate / THRESHOLDS.PAUSE_OPT_OUT_THRESHOLD),
    };
  }

  if (failed_rate >= THRESHOLDS.PAUSE_FAILED_THRESHOLD) {
    return {
      scale_recommended: false,
      pause_recommended: true,
      reason:     `Failed delivery rate critical: ${(failed_rate * 100).toFixed(1)}% (threshold ${THRESHOLDS.PAUSE_FAILED_THRESHOLD * 100}%)`,
      confidence: Math.min(1, failed_rate / THRESHOLDS.PAUSE_FAILED_THRESHOLD),
    };
  }

  // Scale signal.
  if (
    reply_rate >= THRESHOLDS.POSITIVE_REPLY_RATE &&
    opt_out_rate < THRESHOLDS.OPT_OUT_RATE_MAX   &&
    failed_rate  < THRESHOLDS.FAILED_RATE_MAX
  ) {
    return {
      scale_recommended: true,
      pause_recommended: false,
      reason:     `Positive reply rate: ${(reply_rate * 100).toFixed(1)}% (threshold ${THRESHOLDS.POSITIVE_REPLY_RATE * 100}%) with low opt-outs and failures`,
      confidence: Math.min(1, reply_rate / (THRESHOLDS.POSITIVE_REPLY_RATE * 2)),
    };
  }

  return {
    scale_recommended: false,
    pause_recommended: false,
    reason:     "Metrics within normal range — no action needed",
    confidence: 0,
  };
}

/**
 * Build a scale recommendation object.
 *
 * @param {object} campaign  - campaign_targets row
 * @param {object} metrics   - aggregated send metrics
 * @param {object} analysis  - result of analyzeCampaignHealth
 * @returns {object}
 */
export function buildCampaignScaleRecommendation(campaign = {}, metrics = {}, analysis = {}) {
  const current_cap  = Number(campaign.daily_cap ?? campaign.cap ?? 0);
  const proposed_cap = Math.min(current_cap * 2, 10000);

  return {
    type:         "scale",
    campaign_key: String(campaign.campaign_key ?? campaign.key ?? ""),
    campaign_id:  String(campaign.id ?? ""),
    market:       String(campaign.market ?? ""),
    asset:        String(campaign.asset_type ?? campaign.asset ?? ""),
    strategy:     String(campaign.strategy ?? ""),
    current_cap,
    proposed_cap,
    reason:       String(analysis.reason ?? ""),
    confidence:   Number(analysis.confidence ?? 0),
    metrics,
  };
}

/**
 * Build a pause recommendation object.
 *
 * @param {object} campaign  - campaign_targets row
 * @param {object} metrics   - aggregated send metrics
 * @param {object} analysis  - result of analyzeCampaignHealth
 * @returns {object}
 */
export function buildCampaignPauseRecommendation(campaign = {}, metrics = {}, analysis = {}) {
  const opt_out_rate = metrics.delivered > 0
    ? (metrics.opted_out ?? 0) / metrics.delivered
    : 0;
  const failed_rate = metrics.sent > 0
    ? (metrics.failed ?? 0) / metrics.sent
    : 0;

  return {
    type:         "pause",
    campaign_key: String(campaign.campaign_key ?? campaign.key ?? ""),
    campaign_id:  String(campaign.id ?? ""),
    market:       String(campaign.market ?? ""),
    asset:        String(campaign.asset_type ?? campaign.asset ?? ""),
    strategy:     String(campaign.strategy ?? ""),
    reason:       String(analysis.reason ?? ""),
    confidence:   Number(analysis.confidence ?? 0),
    opt_out_rate,
    failed_rate,
    metrics,
  };
}

// ---------------------------------------------------------------------------
// Notification dedup key helpers
// ---------------------------------------------------------------------------

/**
 * Build a deterministic, window-scoped dedup key for an ops notification.
 * Key format: <type>:<campaign_key>:<YYYY-MM-DD>  (UTC date)
 *
 * @param {string} type         - e.g. "campaign_scale", "campaign_pause"
 * @param {string} campaign_key - e.g. "tx_sfr_cash"
 * @param {Date}   [ref_date]   - Defaults to today UTC
 * @returns {string}
 */
export function buildNotificationKey(type, campaign_key = "", ref_date = new Date()) {
  const date_str = ref_date.toISOString().slice(0, 10); // YYYY-MM-DD
  const safe_key = String(campaign_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 80);
  return `${type}:${safe_key}:${date_str}`;
}

/**
 * Build a deterministic dedup key for an approval request.
 * Key format: approval:<type>:<campaign_key>:<YYYY-MM-DD>
 */
export function buildApprovalRequestKey(type, campaign_key = "", ref_date = new Date()) {
  const date_str = ref_date.toISOString().slice(0, 10);
  const safe_key = String(campaign_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 80);
  return `approval:${type}:${safe_key}:${date_str}`;
}

// ---------------------------------------------------------------------------
// Supabase writes
// ---------------------------------------------------------------------------

/**
 * Upsert an ops_notification row (deduped by notification_key).
 * Returns the upserted row's id, or null on error.
 *
 * @param {object} fields - ops_notifications column values
 * @returns {Promise<number|null>}
 */
export async function createOpsNotification(fields = {}) {
  const db = getDb();
  const expiry = new Date();
  expiry.setUTCHours(expiry.getUTCHours() + THRESHOLDS.NOTIFICATION_EXPIRY_HOURS);

  const row = {
    notification_key:   String(fields.notification_key   ?? ""),
    notification_type:  String(fields.notification_type  ?? "info"),
    severity:           String(fields.severity           ?? "info"),
    campaign_key:       fields.campaign_key ?? null,
    title:              String(fields.title              ?? "Ops Notification"),
    message:            fields.message       != null ? String(fields.message)  : null,
    metrics:            fields.metrics       != null ? fields.metrics          : null,
    recommended_action: fields.recommended_action != null ? String(fields.recommended_action) : null,
    discord_channel_id: fields.discord_channel_id != null ? String(fields.discord_channel_id) : null,
    status:             String(fields.status ?? "pending"),
    expires_at:         fields.expires_at ?? expiry.toISOString(),
    updated_at:         new Date().toISOString(),
  };

  try {
    const { data, error } = await db
      .from("ops_notifications")
      .upsert(row, { onConflict: "notification_key", ignoreDuplicates: false })
      .select("id")
      .maybeSingle();

    if (error) {
      logger.warn("ops_notification.upsert_error", { error: error.message });
      return null;
    }
    return data?.id ?? null;
  } catch (err) {
    logger.warn("ops_notification.upsert_exception", { error: String(err?.message ?? err) });
    return null;
  }
}

/**
 * Insert a campaign approval request.
 * Returns the created row's id, or null on error.
 *
 * @param {object} fields - campaign_approval_requests column values
 * @returns {Promise<number|null>}
 */
export async function createCampaignApprovalRequest(fields = {}) {
  const db = getDb();
  const expiry = new Date();
  expiry.setUTCHours(expiry.getUTCHours() + THRESHOLDS.APPROVAL_EXPIRY_HOURS);

  const row = {
    request_key:     String(fields.request_key    ?? ""),
    request_type:    String(fields.request_type   ?? "scale"),
    campaign_key:    String(fields.campaign_key   ?? ""),
    campaign_id:     fields.campaign_id    != null ? String(fields.campaign_id)  : null,
    market:          fields.market         != null ? String(fields.market)        : null,
    asset:           fields.asset          != null ? String(fields.asset)         : null,
    strategy:        fields.strategy       != null ? String(fields.strategy)      : null,
    current_cap:     fields.current_cap    != null ? Number(fields.current_cap)   : null,
    proposed_cap:    fields.proposed_cap   != null ? Number(fields.proposed_cap)  : null,
    reason:          fields.reason         != null ? String(fields.reason)        : null,
    metrics:         fields.metrics        != null ? fields.metrics               : null,
    status:          "pending",
    requester:       fields.requester      != null ? String(fields.requester)     : "system",
    notification_id: fields.notification_id ?? null,
    expires_at:      fields.expires_at ?? expiry.toISOString(),
    updated_at:      new Date().toISOString(),
  };

  try {
    const { data, error } = await db
      .from("campaign_approval_requests")
      .upsert(row, { onConflict: "request_key", ignoreDuplicates: true })
      .select("id")
      .maybeSingle();

    if (error) {
      logger.warn("approval_request.upsert_error", { error: error.message });
      return null;
    }
    return data?.id ?? null;
  } catch (err) {
    logger.warn("approval_request.upsert_exception", { error: String(err?.message ?? err) });
    return null;
  }
}

/**
 * Write an immutable audit entry to discord_action_audit.
 *
 * @param {object} fields
 * @returns {Promise<void>}
 */
export async function writeDiscordActionAudit(fields = {}) {
  const db = getDb();
  try {
    await db.from("discord_action_audit").insert({
      request_key:    fields.request_key    ?? null,
      action_type:    String(fields.action_type   ?? ""),
      actor_user_id:  fields.actor_user_id  ?? null,
      actor_username: fields.actor_username ?? null,
      guild_id:       fields.guild_id       ?? null,
      channel_id:     fields.channel_id     ?? null,
      message_id:     fields.message_id     ?? null,
      outcome:        String(fields.outcome ?? ""),
      details:        fields.details        ?? null,
    });
  } catch { /* audit log failures are non-fatal */ }
}

/**
 * Mark a campaign_approval_requests row with an outcome.
 *
 * @param {string} request_key
 * @param {"approved"|"rejected"|"expired"|"cancelled"} outcome
 * @param {object} [actor]  - { user_id, username }
 * @returns {Promise<boolean>}  true if the row was updated
 */
export async function resolveApprovalRequest(request_key, outcome, actor = {}) {
  const db = getDb();
  const now_iso = new Date().toISOString();

  const is_approval = outcome === "approved";
  const is_rejection = outcome === "rejected";

  const update = {
    status:     outcome,
    updated_at: now_iso,
    ...(is_approval  ? { approved_by: actor.username ?? actor.user_id ?? "operator", approved_at: now_iso } : {}),
    ...(is_rejection ? { rejected_by: actor.username ?? actor.user_id ?? "operator", rejected_at: now_iso } : {}),
  };

  try {
    const { data, error } = await db
      .from("campaign_approval_requests")
      .update(update)
      .eq("request_key", request_key)
      .eq("status", "pending")
      .select("id")
      .maybeSingle();

    if (error) {
      logger.warn("approval_request.resolve_error", { error: error.message });
      return false;
    }
    return data != null;
  } catch {
    return false;
  }
}

/**
 * Load an approval request by request_key.
 * Returns null if not found or expired.
 *
 * @param {string} request_key
 * @returns {Promise<object|null>}
 */
export async function loadApprovalRequest(request_key) {
  const db = getDb();
  try {
    const { data, error } = await db
      .from("campaign_approval_requests")
      .select("*")
      .eq("request_key", request_key)
      .maybeSingle();

    if (error || !data) return null;

    // Treat expired requests as unavailable.
    if (data.expires_at && new Date(data.expires_at) < new Date()) return null;

    return data;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Aggregate metrics from message_events
// ---------------------------------------------------------------------------

/**
 * Aggregate send metrics for a campaign from message_events.
 * Looks at events in the last N hours.
 *
 * @param {string} campaign_key
 * @param {number} [hours=72]
 * @returns {Promise<{ sent: number, delivered: number, replied: number, opted_out: number, failed: number, sample_size: number }>}
 */
export async function fetchCampaignMetrics(campaign_key, hours = 72) {
  const db = getDb();
  const since = new Date();
  since.setUTCHours(since.getUTCHours() - hours);

  const blank = { sent: 0, delivered: 0, replied: 0, opted_out: 0, failed: 0, sample_size: 0 };

  try {
    const { data, error } = await db
      .from("message_events")
      .select("direction, status, body")
      .eq("campaign_key", campaign_key)
      .gte("created_at", since.toISOString())
      .limit(10000);

    if (error || !data) return blank;

    const metrics = { ...blank };
    for (const ev of data) {
      if (ev.direction === "outbound") {
        metrics.sent++;
        if (ev.status === "delivered")   metrics.delivered++;
        if (ev.status === "failed")      metrics.failed++;
        if (ev.status === "opted_out")   metrics.opted_out++;
      } else if (ev.direction === "inbound") {
        metrics.replied++;
      }
    }
    metrics.sample_size = metrics.sent;
    return metrics;
  } catch {
    return blank;
  }
}

/**
 * Fetch active campaigns from campaign_targets.
 *
 * @returns {Promise<object[]>}
 */
export async function fetchActiveCampaigns() {
  const db = getDb();
  try {
    const { data, error } = await db
      .from("campaign_targets")
      .select("id, campaign_key, market, asset_type, strategy, daily_cap, paused, created_at")
      .eq("paused", false)
      .order("created_at", { ascending: false })
      .limit(100);

    if (error) return [];
    return data ?? [];
  } catch {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Discord webhook dispatch
// ---------------------------------------------------------------------------

/**
 * Dispatch a proactive ops notification to a Discord webhook URL.
 * Fails silently — never throws.
 *
 * @param {object} opts
 * @param {string}   opts.webhook_url  - Discord webhook URL (from env)
 * @param {object[]} opts.embeds       - Discord embed objects
 * @param {object[]} [opts.components] - Discord component rows
 * @returns {Promise<{ ok: boolean, message_id?: string }>}
 */
export async function dispatchDiscordOpsNotification({ webhook_url, embeds = [], components = [] }) {
  if (!webhook_url) {
    logger.warn("ops_notification.dispatch_skipped", { reason: "no_webhook_url" });
    return { ok: false };
  }

  const fetch_fn = _deps.fetch_override ?? globalThis.fetch;

  try {
    const body = JSON.stringify({
      embeds:     embeds.slice(0, 10),
      components: components.slice(0, 5),
    });

    const res = await fetch_fn(`${webhook_url}?wait=true`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });

    if (!res.ok) {
      logger.warn("ops_notification.dispatch_failed", { status: res.status });
      return { ok: false };
    }

    const json = await res.json().catch(() => ({}));
    return { ok: true, message_id: json.id ?? null };
  } catch (err) {
    logger.warn("ops_notification.dispatch_exception", { error: String(err?.message ?? err) });
    return { ok: false };
  }
}

// ---------------------------------------------------------------------------
// Proactive ops check orchestrator
// ---------------------------------------------------------------------------

/**
 * Run a full proactive ops check:
 *   1. Fetch active campaigns
 *   2. Aggregate metrics for each
 *   3. Run health analysis
 *   4. Upsert notifications + approval requests for actioned campaigns
 *   5. Optionally dispatch Discord webhook messages
 *
 * @param {object} opts
 * @param {boolean} [opts.dry_run=true]          - If true, no writes or dispatches
 * @param {boolean} [opts.discord_dispatch=false] - If true, send Discord webhooks
 * @param {string}  [opts.webhook_url]            - Discord webhook URL
 * @param {Function} [opts.embed_builder]         - (notification, recommendation) → embed; injected in tests
 * @param {Function} [opts.component_builder]     - (request_key, type) → components; injected in tests
 * @returns {Promise<{ campaigns_checked: number, scale_recommendations: number, pause_recommendations: number, notifications_created: number, dry_run: boolean }>}
 */
export async function runProactiveOpsCheck({
  dry_run           = true,
  discord_dispatch  = false,
  webhook_url       = process.env.OPS_DISCORD_WEBHOOK_URL ?? "",
  embed_builder     = null,
  component_builder = null,
} = {}) {
  const result = {
    campaigns_checked:       0,
    scale_recommendations:   0,
    pause_recommendations:   0,
    notifications_created:   0,
    approvals_created:        0,
    dry_run,
  };

  let campaigns = [];
  try {
    campaigns = await fetchActiveCampaigns();
  } catch {
    return { ...result, source_error: "failed_to_fetch_campaigns" };
  }

  result.campaigns_checked = campaigns.length;

  for (const campaign of campaigns) {
    const campaign_key = String(campaign.campaign_key ?? campaign.id ?? "");
    if (!campaign_key) continue;

    let metrics;
    try {
      metrics = await fetchCampaignMetrics(campaign_key);
    } catch {
      continue;
    }

    const analysis = analyzeCampaignHealth(campaign, metrics);

    if (!analysis.scale_recommended && !analysis.pause_recommended) continue;

    const rec_type   = analysis.scale_recommended ? "scale" : "pause";
    const rec        = analysis.scale_recommended
      ? buildCampaignScaleRecommendation(campaign, metrics, analysis)
      : buildCampaignPauseRecommendation(campaign, metrics, analysis);

    if (analysis.scale_recommended) result.scale_recommendations++;
    if (analysis.pause_recommended) result.pause_recommendations++;

    if (dry_run) continue;

    const notification_key = buildNotificationKey(
      `campaign_${rec_type}`,
      campaign_key
    );
    const request_key = buildApprovalRequestKey(rec_type, campaign_key);

    const severity   = rec_type === "pause" ? "warning" : "info";
    const title      = rec_type === "scale"
      ? `Campaign Scale Opportunity — ${campaign_key}`
      : `Campaign Pause Alert — ${campaign_key}`;

    const notif_id = await createOpsNotification({
      notification_key:  notification_key,
      notification_type: `campaign_${rec_type}`,
      severity,
      campaign_key,
      title,
      message:           rec.reason,
      metrics,
      recommended_action: rec_type,
      discord_channel_id: process.env.OPS_DISCORD_CHANNEL_ID ?? null,
    });

    if (notif_id != null) result.notifications_created++;

    const approval_id = await createCampaignApprovalRequest({
      request_key,
      request_type:    rec_type,
      campaign_key,
      campaign_id:     rec.campaign_id,
      market:          rec.market,
      asset:           rec.asset,
      strategy:        rec.strategy,
      current_cap:     rec.current_cap,
      proposed_cap:    rec.proposed_cap,
      reason:          rec.reason,
      metrics,
      notification_id: notif_id,
    });

    if (approval_id != null) result.approvals_created++;

    if (discord_dispatch && webhook_url) {
      const embed = embed_builder
        ? embed_builder(rec, analysis)
        : null;

      const components = component_builder
        ? component_builder(request_key, rec_type)
        : [];

      if (embed) {
        await dispatchDiscordOpsNotification({
          webhook_url,
          embeds:     [embed],
          components,
        });
      }
    }
  }

  return result;
}
