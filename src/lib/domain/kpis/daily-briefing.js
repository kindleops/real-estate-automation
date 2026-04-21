/**
 * daily-briefing.js
 *
 * Daily Empire Briefing — KPI / metrics aggregation.
 *
 * Query strategy:
 *   - message_events   → outreach (SMS sent/delivered/replies)
 *   - send_queue       → system health queue ready/due/failed
 *   - wire_events      → revenue / wires
 *   - email_send_queue → email metrics
 *
 * All data sources fail open: errors are recorded in source_errors[] and the
 * function returns partial=true with the data that was available.
 * Never throws raw DB errors to the caller.
 */

// ---------------------------------------------------------------------------
// Window builder
// ---------------------------------------------------------------------------

/**
 * Compute the UTC timestamp that corresponds to midnight on a given calendar
 * date inside the target IANA timezone.
 *
 * Strategy: compare the "sv" locale format of noon-UTC vs noon-in-TZ to derive
 * the UTC offset at that date (capturing DST correctly), then apply the offset
 * to the UTC midnight epoch.
 *
 * @param {number} year
 * @param {number} month_1based  1 = January … 12 = December
 * @param {number} day           1–31 (overflow is handled by Date.UTC)
 * @param {string} timezone      IANA timezone string
 * @returns {Date}
 */
function toTzMidnight(year, month_1based, day, timezone) {
  // Use noon UTC as the reference point — avoids tricky DST edges at midnight.
  const noonUTC = new Date(Date.UTC(year, month_1based - 1, day, 12, 0, 0));

  try {
    // "sv" locale produces "YYYY-MM-DD HH:MM:SS" — parseable as ISO with a "Z" suffix.
    const utcStr = noonUTC.toLocaleString("sv", { timeZone: "UTC" });
    const tzStr  = noonUTC.toLocaleString("sv", { timeZone: timezone });

    const asUTC    = (s) => new Date(s.replace(" ", "T") + "Z");
    const offsetMs = asUTC(utcStr).getTime() - asUTC(tzStr).getTime();

    // Midnight in the target timezone = UTC midnight + offset
    return new Date(Date.UTC(year, month_1based - 1, day, 0, 0, 0) + offsetMs);
  } catch {
    // Fallback: treat as UTC midnight (safe for tests with synthetic timezones)
    return new Date(Date.UTC(year, month_1based - 1, day, 0, 0, 0));
  }
}

/**
 * Build a time window (ISO UTC strings) representing the requested range
 * anchored to the given IANA timezone.
 *
 * Supported ranges: "today" | "yesterday" | "week" | "month"
 *
 * @param {{ range?: string, timezone?: string }} opts
 * @returns {{ window_start: string, window_end: string }}
 */
export function buildBriefingWindow({ range = "today", timezone = "America/Chicago" } = {}) {
  let y, m, d;
  try {
    const now = new Date();
    // "sv" locale from toLocaleDateString gives "YYYY-MM-DD"
    const str = now.toLocaleDateString("sv", { timeZone: timezone });
    [y, m, d] = str.split("-").map(Number);
  } catch {
    const now = new Date();
    y = now.getUTCFullYear();
    m = now.getUTCMonth() + 1;
    d = now.getUTCDate();
  }

  let ws, we;
  switch (range) {
    case "yesterday":
      ws = toTzMidnight(y, m, d - 1, timezone);
      we = toTzMidnight(y, m, d,     timezone);
      break;
    case "week":
      ws = toTzMidnight(y, m, d - 6, timezone);
      we = toTzMidnight(y, m, d + 1, timezone);
      break;
    case "month":
      ws = toTzMidnight(y, m, 1,     timezone);
      we = toTzMidnight(y, m, d + 1, timezone);
      break;
    default: // "today"
      ws = toTzMidnight(y, m, d,     timezone);
      we = toTzMidnight(y, m, d + 1, timezone);
  }

  return {
    window_start: ws.toISOString(),
    window_end:   we.toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Empty metric shapes (guaranteed-safe defaults)
// ---------------------------------------------------------------------------

function emptyOutreach() {
  return {
    queued: 0, sending: 0, sent: 0, delivered: 0, failed: 0,
    replies: 0, positive_replies: 0, opt_outs: 0, wrong_numbers: 0,
    reply_rate: 0, delivery_rate: 0,
  };
}

function emptyEmail() {
  return {
    queued: 0, sent: 0, delivered: 0, opened: 0,
    clicked: 0, replied: 0, bounced: 0, suppressed: 0,
  };
}

function emptyAcquisitions() {
  return {
    hot_leads: 0, stage_advances: 0, offers_sent: 0, offers_created: 0,
    contracts_sent: 0, contracts_signed: 0, underwriting_transfers: 0, manual_reviews: 0,
  };
}

function emptyDispo() {
  return { buyer_matches: 0, buyer_replies: 0, jv_opportunities: 0, dispo_tasks: 0 };
}

function emptyRevenue() {
  return {
    closed_count: 0, closed_revenue: 0,
    pending_wires: 0, pending_wire_amount: 0,
    cleared_wires: 0, cleared_wire_amount: 0,
    projected_pipeline_value: 0,
  };
}

function emptySystemHealth() {
  return {
    queue_ready: 0, queue_due: 0, queue_failed_recent: 0,
    podio_status: "unknown", supabase_status: "unknown",
    textgrid_status: "unknown", email_status: "unknown",
  };
}

// ---------------------------------------------------------------------------
// Error sanitiser (never leaks tokens/secrets/stack traces)
// ---------------------------------------------------------------------------

function sanitizeError(err) {
  if (!err) return "unknown error";
  const msg = String(err?.message ?? err ?? "unknown error");
  return msg
    .replace(/sk[-_][\w]{20,}/gi, "[redacted]")
    .replace(/https?:\/\/[^\s]{50,}/gi, "[url]")
    .slice(0, 120);
}

/**
 * Run `fn()` and return its value; on error, push to source_errors and return null.
 */
async function safeQuery(fn, source_label, source_errors) {
  try {
    return await fn();
  } catch (err) {
    source_errors.push({ source: source_label, message: sanitizeError(err) });
    return null;
  }
}

// ---------------------------------------------------------------------------
// Main aggregation
// ---------------------------------------------------------------------------

/**
 * Aggregate KPI metrics for the given range and optional market/agent scope.
 *
 * @param {{
 *   range?:    string,
 *   timezone?: string,
 *   market?:   string | null,
 *   agent?:    string | null,
 *   supabase:  object,
 * }} opts
 * @returns {Promise<object>}  Always resolves — never rejects.
 */
export async function getDailyBriefing({
  range    = "today",
  timezone = "America/Chicago",
  market   = null,
  agent    = null,
  supabase: db,
} = {}) {
  const { window_start, window_end } = buildBriefingWindow({ range, timezone });
  const source_errors = [];

  // ── Outreach (message_events) ─────────────────────────────────────────────
  const outreach = emptyOutreach();

  const msg_data = await safeQuery(async () => {
    const { data, error } = await db
      .from("message_events")
      .select("status, direction, body")
      .gte("created_at", window_start)
      .lt("created_at", window_end)
      .limit(10000);
    if (error) throw error;
    return data ?? [];
  }, "message_events", source_errors);

  if (msg_data) {
    for (const row of msg_data) {
      if (row.direction === "outbound") {
        const s = String(row.status ?? "");
        if (s === "queued")    outreach.queued++;
        if (s === "sending")   outreach.sending++;
        if (s === "sent")      outreach.sent++;
        if (s === "delivered") outreach.delivered++;
        if (s === "failed")    outreach.failed++;
      } else if (row.direction === "inbound") {
        outreach.replies++;
        const b = String(row.body ?? "").toLowerCase().trim();
        if (b === "stop" || b === "unsubscribe" || b.startsWith("stop ")) {
          outreach.opt_outs++;
        } else if (b === "wrong number" || b.includes("wrong number")) {
          outreach.wrong_numbers++;
        } else if (b.length > 2) {
          outreach.positive_replies++;
        }
      }
    }
    const total_sent = outreach.sent + outreach.delivered;
    outreach.delivery_rate = total_sent > 0
      ? Math.round(outreach.delivered / total_sent * 100) : 0;
    outreach.reply_rate = total_sent > 0
      ? Math.round(outreach.replies / total_sent * 100) : 0;
  }

  // ── Email (email_send_queue) ──────────────────────────────────────────────
  const email = emptyEmail();

  const email_data = await safeQuery(async () => {
    const { data, error } = await db
      .from("email_send_queue")
      .select("status")
      .gte("created_at", window_start)
      .lt("created_at", window_end)
      .limit(10000);
    if (error) throw error;
    return data ?? [];
  }, "email_send_queue", source_errors);

  if (email_data) {
    for (const row of email_data) {
      const s = String(row.status ?? "");
      if (s === "queued")     email.queued++;
      if (s === "sent")       email.sent++;
      if (s === "delivered")  email.delivered++;
      if (s === "opened")     email.opened++;
      if (s === "clicked")    email.clicked++;
      if (s === "replied")    email.replied++;
      if (s === "bounced")    email.bounced++;
      if (s === "suppressed") email.suppressed++;
    }
  }

  // ── Revenue / Wires (wire_events) ─────────────────────────────────────────
  const revenue = emptyRevenue();

  const wire_data = await safeQuery(async () => {
    const { data, error } = await db
      .from("wire_events")
      .select("status, amount")
      .gte("created_at", window_start)
      .lt("created_at", window_end)
      .limit(5000);
    if (error) throw error;
    return data ?? [];
  }, "wire_events", source_errors);

  if (wire_data) {
    for (const row of wire_data) {
      const amt = Number(row.amount ?? 0);
      if (row.status === "cleared" || row.status === "received") {
        revenue.cleared_wires++;
        revenue.cleared_wire_amount += amt;
      } else if (row.status === "pending") {
        revenue.pending_wires++;
        revenue.pending_wire_amount += amt;
      }
    }
    revenue.projected_pipeline_value =
      revenue.cleared_wire_amount + revenue.pending_wire_amount;
  }

  // ── System Health (send_queue) ────────────────────────────────────────────
  const system_health = emptySystemHealth();

  const queue_data = await safeQuery(async () => {
    const { data, error } = await db
      .from("send_queue")
      .select("status, run_at")
      .limit(5000);
    if (error) throw error;
    return data ?? [];
  }, "send_queue", source_errors);

  if (queue_data) {
    const now_ms = Date.now();
    for (const row of queue_data) {
      const s = String(row.status ?? "");
      if (s === "ready")  system_health.queue_ready++;
      if (s === "failed") system_health.queue_failed_recent++;
      if (s === "ready" && row.run_at && new Date(row.run_at).getTime() <= now_ms) {
        system_health.queue_due++;
      }
    }
    system_health.supabase_status = "ok";
  }

  // External service status from env (presence indicates configuration)
  system_health.podio_status    = process.env.PODIO_CLIENT_ID    ? "configured" : "not_configured";
  system_health.textgrid_status = process.env.TEXTGRID_ACCOUNT_SID ? "configured" : "not_configured";
  system_health.email_status    = process.env.SENDGRID_API_KEY   ? "configured" : "not_configured";

  // ── Acquisitions (heuristic from inbound replies) ─────────────────────────
  const acquisitions = emptyAcquisitions();
  if (msg_data) {
    acquisitions.hot_leads = outreach.positive_replies;
  }

  // ── Dispo (placeholder — full Podio query out of scope) ──────────────────
  const dispo = emptyDispo();

  // ── Assemble ──────────────────────────────────────────────────────────────
  const markets = market ? [market] : [];
  const agents  = agent  ? [agent]  : [];
  const partial = source_errors.length > 0;

  const raw = {
    range, timezone, window_start, window_end,
    outreach, email, acquisitions, dispo, revenue, system_health,
    markets, agents, source_errors, partial,
  };

  const metrics = normalizeBriefingMetrics(raw);
  metrics.health = calculateBriefingHealth(metrics);
  metrics.next_recommended_action = calculateNextRecommendedAction(metrics);
  return metrics;
}

// ---------------------------------------------------------------------------
// Normalize
// ---------------------------------------------------------------------------

/**
 * Ensure every field in the metrics object has a safe default.
 * Safe to call on partial/null raw objects.
 *
 * @param {object} raw
 * @returns {object}
 */
export function normalizeBriefingMetrics(raw = {}) {
  return {
    range:        raw.range        ?? "today",
    timezone:     raw.timezone     ?? "America/Chicago",
    window_start: raw.window_start ?? new Date().toISOString(),
    window_end:   raw.window_end   ?? new Date().toISOString(),
    outreach:     { ...emptyOutreach(),     ...(raw.outreach     ?? {}) },
    email:        { ...emptyEmail(),        ...(raw.email        ?? {}) },
    acquisitions: { ...emptyAcquisitions(), ...(raw.acquisitions ?? {}) },
    dispo:        { ...emptyDispo(),        ...(raw.dispo        ?? {}) },
    revenue:      { ...emptyRevenue(),      ...(raw.revenue      ?? {}) },
    system_health: { ...emptySystemHealth(), ...(raw.system_health ?? {}) },
    markets:       raw.markets       ?? [],
    agents:        raw.agents        ?? [],
    source_errors: raw.source_errors ?? [],
    partial:       raw.partial       ?? false,
    health:        raw.health        ?? null,
    next_recommended_action: raw.next_recommended_action ?? null,
  };
}

// ---------------------------------------------------------------------------
// Health calculation
// ---------------------------------------------------------------------------

/**
 * Compute the overall health color for this briefing period.
 *
 * @param {object} metrics  (normalized)
 * @returns {"green" | "yellow" | "red" | "purple"}
 */
export function calculateBriefingHealth(metrics) {
  const { outreach, revenue, system_health, source_errors } = metrics;

  if ((outreach.failed   ?? 0) > 50)                return "red";
  if ((system_health.queue_failed_recent ?? 0) > 20) return "red";

  if ((source_errors?.length ?? 0) > 0)   return "yellow";
  if ((outreach.failed ?? 0) > 10)        return "yellow";

  // Strong revenue day → purple/gold
  if ((revenue.cleared_wire_amount ?? 0) > 50_000) return "purple";
  if ((revenue.cleared_wires       ?? 0) > 2)      return "purple";

  return "green";
}

// ---------------------------------------------------------------------------
// Next recommended action
// ---------------------------------------------------------------------------

/**
 * Generate a single, actionable "next move" string based on current metrics.
 *
 * @param {object} metrics  (normalized)
 * @returns {string}
 */
export function calculateNextRecommendedAction(metrics) {
  const { outreach, acquisitions, revenue, system_health } = metrics;

  if ((system_health.queue_due           ?? 0) > 10)  return "🚨 Run send queue — messages are overdue (`/queue run`)";
  if ((revenue.pending_wires             ?? 0) > 2)   return "💵 Follow up on pending wires — use `/wires cockpit`";
  if ((acquisitions.offers_created       ?? 0) > 0 &&
      (acquisitions.contracts_signed     ?? 0) === 0) return "📋 Push offers to contract — review hot leads";
  if ((outreach.positive_replies         ?? 0) > 5)   return "🔥 High reply volume — qualify leads with `/hotleads`";
  if ((outreach.sent                     ?? 0) === 0) return "📤 No outreach today — run the feeder (`/feeder run`)";
  if ((outreach.reply_rate               ?? 0) > 15)  return "🎯 Strong reply rate — push qualified leads to contract";
  return "✅ Operations nominal — monitor reply queue";
}
