/**
 * discord-embed-factory.js
 *
 * Reusable Discord embed builders for the operations command center.
 *
 * Color scheme (cinematic, professional):
 *   green  (0x2ECC71) – healthy / success
 *   yellow (0xF1C40F) – warning / caution
 *   red    (0xE74C3C) – critical / error
 *   blue   (0x3498DB) – informational
 *   purple (0x9B59B6) – informational alt
 *   gray   (0x95A5A6) – neutral / inactive
 *
 * All functions return plain Discord API embed objects.
 * The caller wraps them in a { type, data: { embeds: [...] } } response.
 */

// ---------------------------------------------------------------------------
// Color constants
// ---------------------------------------------------------------------------

const COLOR = {
  green:  0x2ECC71,
  yellow: 0xF1C40F,
  red:    0xE74C3C,
  blue:   0x3498DB,
  purple: 0x9B59B6,
  gray:   0x95A5A6,
};

/**
 * Map a status string to a Discord embed color.
 *
 * Accepted values (case-insensitive):
 *   healthy | go | success | active | ok  → green
 *   warning | warn | partial              → yellow
 *   critical | error | failed | hold      → red
 *   info | pending | informational        → blue
 *   (anything else)                       → gray
 *
 * @param {string} status
 * @returns {number}
 */
export function statusToColor(status) {
  const s = String(status ?? "").toLowerCase();
  if (["healthy", "go", "success", "active", "ok"].includes(s))   return COLOR.green;
  if (["warning", "warn", "partial"].includes(s))                  return COLOR.yellow;
  if (["critical", "error", "failed", "hold"].includes(s))         return COLOR.red;
  if (["info", "pending", "informational"].includes(s))            return COLOR.blue;
  return COLOR.gray;
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

/** Build a Discord embed field object. */
function f(name, value, inline = false) {
  return {
    name:   (String(name  ?? "").slice(0, 256))  || "\u200b",
    value:  (String(value ?? "").slice(0, 1024)) || "—",
    inline: Boolean(inline),
  };
}

/** ISO-8601 timestamp for embed footer (no milliseconds). */
function now() {
  return new Date().toISOString().slice(0, 19) + "Z";
}

// ---------------------------------------------------------------------------
// buildMissionStatusEmbed
// ---------------------------------------------------------------------------

/**
 * High-level mission health embed.
 *
 * @param {object} payload
 * @param {string}  payload.overall_status      - "healthy" | "warning" | "critical"
 * @param {object}  payload.queue_counts        - { queued?, sending?, sent?, failed?, ... }
 * @param {number}  [payload.active_templates]  - total active sms_templates rows
 * @param {number}  [payload.stage1_templates]  - active Stage 1 sms_templates rows
 * @param {number}  [payload.recent_events]     - message_events in last 24 h
 * @param {number}  [payload.failed_syncs]      - failed Podio sync events
 * @param {boolean} [payload.supabase_ok]
 * @param {boolean} [payload.podio_ok]
 * @param {boolean} [payload.textgrid_ok]
 * @returns {object}  Discord embed
 */
export function buildMissionStatusEmbed(payload = {}) {
  const {
    overall_status   = "info",
    queue_counts     = {},
    active_templates = null,
    stage1_templates = null,
    recent_events    = null,
    failed_syncs     = null,
    supabase_ok      = null,
    podio_ok         = null,
    textgrid_ok      = null,
  } = payload;

  const qv = (k) => queue_counts[k] != null ? String(queue_counts[k]) : "—";

  const icon = (ok) =>
    ok === true  ? "✓" :
    ok === false ? "✗" : "—";

  return {
    title:     "Mission Status",
    color:     statusToColor(overall_status),
    timestamp: now(),
    fields: [
      f(
        "Queue",
        `Queued: **${qv("queued")}**  |  Sending: **${qv("sending")}**  |  Failed: **${qv("failed")}**`
      ),
      f(
        "Templates",
        `Active: **${active_templates ?? "—"}**  |  Stage 1: **${stage1_templates ?? "—"}**`,
        true
      ),
      f(
        "Events (24 h)",
        `Count: **${recent_events ?? "—"}**  |  Failed syncs: **${failed_syncs ?? "—"}**`,
        true
      ),
      f(
        "Integrations",
        `Supabase: ${icon(supabase_ok)}  |  Podio: ${icon(podio_ok)}  |  TextGrid: ${icon(textgrid_ok)}`
      ),
    ],
    footer: { text: "Operations Command Center" },
  };
}

// ---------------------------------------------------------------------------
// buildLaunchPreflightEmbed
// ---------------------------------------------------------------------------

/**
 * Launch preflight check embed.
 *
 * @param {object}   payload
 * @param {string}   payload.overall_status  - "GO" | "WARN" | "HOLD"
 * @param {object[]} payload.checks          - [{ name, status: "pass"|"warn"|"fail", detail? }]
 * @returns {object}
 */
export function buildLaunchPreflightEmbed(payload = {}) {
  const { overall_status = "HOLD", checks = [] } = payload;

  const color =
    overall_status === "GO"   ? COLOR.green  :
    overall_status === "WARN" ? COLOR.yellow :
    COLOR.red;

  const STATUS_ICON = { pass: "✓", warn: "⚠", fail: "✗" };

  const check_lines = checks
    .map((c) => `${STATUS_ICON[c.status] ?? "—"} **${String(c.name).slice(0, 60)}**: ${String(c.detail ?? "").slice(0, 120)}`)
    .join("\n") || "No checks performed.";

  return {
    title:       `Launch Preflight — ${overall_status}`,
    description: check_lines.slice(0, 4096),
    color,
    timestamp:   now(),
    footer:      { text: "Read-only checks — no sends performed" },
  };
}

// ---------------------------------------------------------------------------
// buildQueueCockpitEmbed
// ---------------------------------------------------------------------------

/**
 * Queue cockpit embed.
 *
 * @param {object} payload
 * @param {object} payload.counts        - { [status_string]: count }
 * @param {number} [payload.due_now]     - rows eligible immediately
 * @param {number} [payload.future]      - rows scheduled in the future
 * @param {number} [payload.stuck_sending] - sending rows past threshold
 * @returns {object}
 */
export function buildQueueCockpitEmbed(payload = {}) {
  const { counts = {}, due_now = null, future = null, stuck_sending = null } = payload;

  const PRIORITY_ORDER = ["queued", "sending", "sent", "failed", "cancelled"];
  const priority  = PRIORITY_ORDER.filter((s) => counts[s] != null);
  const remaining = Object.keys(counts).filter((s) => !PRIORITY_ORDER.includes(s));

  const total = Object.values(counts).reduce((sum, v) => sum + (Number(v) || 0), 0);

  const color =
    (counts.failed   ?? 0) > 0 ? COLOR.yellow :
    (counts.sending  ?? 0) > 0 ? COLOR.blue   :
    COLOR.green;

  const status_fields = [...priority, ...remaining].map((s) =>
    f(s.charAt(0).toUpperCase() + s.slice(1), String(counts[s] ?? 0), true)
  );

  return {
    title:     "Queue Cockpit",
    color,
    timestamp: now(),
    fields: [
      ...status_fields,
      f("Total", String(total), true),
      ...(due_now       != null ? [f("Due Now",        String(due_now),       true)] : []),
      ...(future        != null ? [f("Future",         String(future),        true)] : []),
      ...(stuck_sending != null ? [f("Stuck Sending",  String(stuck_sending), true)] : []),
    ].slice(0, 25),
    footer: { text: `Total: ${total} rows in queue` },
  };
}

// ---------------------------------------------------------------------------
// buildTemplateAuditEmbed
// ---------------------------------------------------------------------------

/**
 * Template audit embed (reads from Supabase sms_templates).
 *
 * @param {object}   payload
 * @param {number}   payload.total
 * @param {number}   payload.active
 * @param {number}   payload.inactive
 * @param {object}   payload.by_language     - { [lang]: count }
 * @param {object}   payload.by_use_case     - { [use_case]: count }
 * @param {object}   payload.by_stage_code   - { [code]: count }
 * @param {number}   payload.active_first_touch
 * @param {number}   payload.active_ownership_check
 * @param {number}   payload.missing_template_body
 * @param {number}   payload.missing_language
 * @param {number}   payload.missing_use_case
 * @param {number}   payload.missing_stage_code
 * @param {string[]} payload.blockers         - human-readable descriptions (first 10 shown)
 * @returns {object}
 */
export function buildTemplateAuditEmbed(payload = {}) {
  const {
    total                  = 0,
    active                 = 0,
    inactive               = 0,
    by_language            = {},
    by_use_case            = {},
    by_stage_code          = {},
    active_first_touch     = 0,
    active_ownership_check = 0,
    missing_template_body  = 0,
    missing_language       = 0,
    missing_use_case       = 0,
    missing_stage_code     = 0,
    blockers               = [],
  } = payload;

  const has_issues =
    missing_template_body > 0 || missing_language > 0 ||
    missing_use_case > 0      || missing_stage_code > 0;

  const color =
    has_issues  ? COLOR.yellow :
    active > 0  ? COLOR.green  :
    COLOR.red;

  const compact = (obj) =>
    Object.entries(obj).map(([k, v]) => `${k}: ${v}`).join("  |  ") || "—";

  const blocker_text = blockers.length > 0
    ? blockers.slice(0, 10).map((b) => `• ${String(b).slice(0, 100)}`).join("\n")
    : "None";

  return {
    title:     "Template Audit — sms_templates",
    color,
    timestamp: now(),
    fields: [
      f("Inventory",      `Total: **${total}**  |  Active: **${active}**  |  Inactive: **${inactive}**`),
      f("Stage 1",        `First Touch: **${active_first_touch}**  |  Ownership Check: **${active_ownership_check}**`, true),
      f("By Language",    compact(by_language)),
      f("By Use Case",    compact(by_use_case)),
      f("By Stage Code",  compact(by_stage_code)),
      f(
        "Missing Fields",
        `Body: **${missing_template_body}**  |  Language: **${missing_language}**  |  Use Case: **${missing_use_case}**  |  Stage Code: **${missing_stage_code}**`
      ),
      f("Blockers (first 10)", blocker_text.slice(0, 1024)),
    ],
    footer: { text: "Source: sms_templates (Supabase)" },
  };
}

// ---------------------------------------------------------------------------
// buildLeadInspectEmbed
// ---------------------------------------------------------------------------

/**
 * Lead inspect embed.
 *
 * @param {object} payload
 * @param {string} payload.query          - phone or owner_id searched
 * @param {number} payload.events_count   - total events found
 * @param {object} payload.by_direction   - { inbound?: n, outbound?: n }
 * @param {string} [payload.most_recent]  - ISO date string of latest event
 * @param {string} [payload.lead_status]  - summary status label
 * @returns {object}
 */
export function buildLeadInspectEmbed(payload = {}) {
  const {
    query        = "",
    events_count = 0,
    by_direction = {},
    most_recent  = null,
    lead_status  = null,
  } = payload;

  return {
    title:     `Lead — ${String(query).slice(0, 60)}`,
    color:     events_count > 0 ? COLOR.blue : COLOR.gray,
    timestamp: now(),
    fields: [
      f(
        "Events",
        `Total: **${events_count}**  |  Inbound: **${by_direction.inbound ?? "—"}**  |  Outbound: **${by_direction.outbound ?? "—"}**`
      ),
      ...(most_recent ? [f("Most Recent", String(most_recent).slice(0, 30), true)] : []),
      ...(lead_status ? [f("Status",      String(lead_status).slice(0, 60),  true)] : []),
    ],
    footer: { text: "Read-only lead summary" },
  };
}

// ---------------------------------------------------------------------------
// buildHotLeadEmbed
// ---------------------------------------------------------------------------

/**
 * Hot leads embed — recent inbound SMS events.
 *
 * @param {object}   payload
 * @param {object[]} payload.events   - [{ phone, body_preview, created_at, podio_synced }]
 * @param {number}   payload.total    - total recent inbound count
 * @returns {object}
 */
export function buildHotLeadEmbed(payload = {}) {
  const { events = [], total = 0 } = payload;

  const fields = events.slice(0, 5).map((evt, i) => {
    const ts_str = evt.created_at
      ? new Date(evt.created_at).toISOString().slice(0, 16).replace("T", " ")
      : "—";
    return f(
      `${i + 1}. ${String(evt.phone ?? "unknown").slice(0, 30)}`,
      [
        `\`${String(evt.body_preview ?? "").slice(0, 80)}\``,
        ts_str,
        `Podio: ${evt.podio_synced ? "✓" : "pending"}`,
      ].join("  ·  ")
    );
  });

  if (fields.length === 0) {
    fields.push(f("No recent inbounds", "No hot leads in the current window."));
  }

  return {
    title:     `Hot Leads (${total} total)`,
    color:     total > 0 ? COLOR.purple : COLOR.gray,
    timestamp: now(),
    fields,
    footer:    { text: "Recent inbound SMS events" },
  };
}

// ---------------------------------------------------------------------------
// buildCampaignControlEmbed
// ---------------------------------------------------------------------------

/**
 * Campaign control embed.
 *
 * @param {object} payload
 * @param {string}  payload.campaign_id
 * @param {boolean} payload.paused
 * @param {string}  [payload.action]  - action requested or performed
 * @returns {object}
 */
export function buildCampaignControlEmbed(payload = {}) {
  const { campaign_id = "", paused = false, action = "" } = payload;

  return {
    title:     `Campaign — ${String(campaign_id).slice(0, 60)}`,
    color:     paused ? COLOR.yellow : COLOR.green,
    timestamp: now(),
    fields: [
      f("Status", paused ? "Paused" : "Active", true),
      ...(action ? [f("Action", String(action).slice(0, 100), true)] : []),
    ],
  };
}

// ---------------------------------------------------------------------------
// buildErrorEmbed
// ---------------------------------------------------------------------------

/**
 * Error embed. Never expose raw error details or secrets.
 *
 * @param {object} payload
 * @param {string} payload.message    - User-facing error message
 * @param {string} [payload.command]  - Command that failed
 * @returns {object}
 */
export function buildErrorEmbed(payload = {}) {
  const { message = "An unexpected error occurred.", command = null } = payload;

  return {
    title:       "Error",
    description: String(message).slice(0, 2048),
    color:       COLOR.red,
    timestamp:   now(),
    ...(command ? { footer: { text: `Command: ${String(command).slice(0, 100)}` } } : {}),
  };
}

// ---------------------------------------------------------------------------
// buildSuccessEmbed
// ---------------------------------------------------------------------------

/**
 * Generic success embed.
 *
 * @param {object}   payload
 * @param {string}   payload.title
 * @param {string}   [payload.description]
 * @param {object[]} [payload.fields]  - pre-built Discord field objects
 * @returns {object}
 */
export function buildSuccessEmbed(payload = {}) {
  const { title = "Success", description = null, fields = [] } = payload;

  return {
    title:       String(title).slice(0, 256),
    ...(description ? { description: String(description).slice(0, 4096) } : {}),
    color:       COLOR.green,
    timestamp:   now(),
    fields:      fields.slice(0, 25),
  };
}

// ---------------------------------------------------------------------------
// buildApprovalEmbed
// ---------------------------------------------------------------------------

/**
 * Approval required embed (approval gate for high-risk actions).
 *
 * @param {object} payload
 * @param {string}  payload.action      - What action is being requested
 * @param {string}  payload.requester   - Username or mention of requester
 * @param {string}  [payload.details]   - Additional safe context (no secrets)
 * @returns {object}
 */
export function buildApprovalEmbed(payload = {}) {
  const { action = "", requester = "", details = null } = payload;

  return {
    title:       "Approval Required",
    description: `**${String(action).slice(0, 200)}** requires Owner approval.`,
    color:       COLOR.yellow,
    timestamp:   now(),
    fields: [
      f("Requested by", String(requester).slice(0, 80), true),
      ...(details ? [f("Details", String(details).slice(0, 400))] : []),
    ],
    footer: { text: "Owner must click Approve to proceed" },
  };
}

// ---------------------------------------------------------------------------
// buildTargetScanEmbed
// ---------------------------------------------------------------------------

/**
 * Target scan result embed (always dry-run).
 *
 * @param {object} payload
 * @param {string}  payload.market
 * @param {string}  payload.asset
 * @param {string}  payload.strategy
 * @param {string}  [payload.source_view_name]
 * @param {number}  [payload.scanned]
 * @param {number}  [payload.eligible]
 * @param {number}  [payload.would_queue]
 * @param {number}  [payload.skipped]
 * @param {number}  [payload.no_phone]
 * @param {number}  [payload.dnc]
 * @param {string}  [payload.template_source]
 * @param {number}  [payload.stage1_errors]
 * @param {number}  [payload.recommended_batch]
 * @param {string}  [payload.risk_level]   - "low" | "medium" | "high"
 * @returns {object}
 */
export function buildTargetScanEmbed(payload = {}) {
  const {
    market            = "",
    asset             = "",
    strategy          = "",
    source_view_name  = "",
    scanned           = null,
    eligible          = null,
    would_queue       = null,
    skipped           = null,
    no_phone          = null,
    dnc               = null,
    template_source   = null,
    stage1_errors     = null,
    recommended_batch = null,
    risk_level        = null,
  } = payload;

  const rc = recommended_batch ?? eligible ?? 0;
  const rl = risk_level ?? (rc > 100 ? "high" : rc > 50 ? "medium" : "low");
  const color =
    rl === "high"   ? COLOR.red    :
    rl === "medium" ? COLOR.yellow :
    COLOR.green;

  return {
    title:     "🎯 Target Scan",
    color,
    timestamp: now(),
    fields: [
      f("Market",            String(market).slice(0, 100),           true),
      f("Asset",             String(asset).slice(0, 100),            true),
      f("Strategy",          String(strategy).slice(0, 100),         true),
      f("Source View",       String(source_view_name).slice(0, 100), false),
      f("Scanned",           scanned     != null ? String(scanned)     : "—", true),
      f("Eligible",          eligible    != null ? String(eligible)    : "—", true),
      f("Would Queue",       would_queue != null ? String(would_queue) : "—", true),
      f("Skipped",           skipped     != null ? String(skipped)     : "—", true),
      f("No Phone",          no_phone    != null ? String(no_phone)    : "—", true),
      f("DNC",               dnc         != null ? String(dnc)         : "—", true),
      ...(template_source  != null ? [f("Template Source",  String(template_source),  true)] : []),
      ...(stage1_errors    != null ? [f("Stage 1 Errors",   String(stage1_errors),    true)] : []),
      f("Recommended Batch", String(rc),                              true),
      f("Risk Level",        String(rl).toUpperCase(),                true),
    ].slice(0, 25),
    footer: { text: "Dry-run only — no SMS sent" },
  };
}

// ---------------------------------------------------------------------------
// buildCampaignCreatedEmbed
// ---------------------------------------------------------------------------

/**
 * Campaign created / upserted confirmation embed.
 *
 * @param {object} payload
 * @param {string}  payload.campaign_key
 * @param {string}  [payload.campaign_name]
 * @param {string}  payload.market
 * @param {string}  payload.asset
 * @param {string}  payload.strategy
 * @param {number}  [payload.daily_cap]
 * @param {string}  [payload.status]
 * @param {string}  [payload.source_view_name]
 * @returns {object}
 */
export function buildCampaignCreatedEmbed(payload = {}) {
  const {
    campaign_key     = "",
    campaign_name    = "",
    market           = "",
    asset            = "",
    strategy         = "",
    daily_cap        = 50,
    status           = "draft",
    source_view_name = "",
  } = payload;

  return {
    title:     "🎮 Campaign Created",
    color:     COLOR.blue,
    timestamp: now(),
    fields: [
      f("Campaign Key",  String(campaign_key).slice(0, 80),                    false),
      f("Name",          String(campaign_name || campaign_key).slice(0, 100),  true),
      f("Market",        String(market).slice(0, 60),                          true),
      f("Asset",         String(asset).slice(0, 60),                           true),
      f("Strategy",      String(strategy).slice(0, 60),                        true),
      f("Daily Cap",     String(daily_cap),                                    true),
      f("Status",        String(status).toUpperCase(),                         true),
      f("Source View",   String(source_view_name).slice(0, 100),               false),
    ],
    footer: { text: "Use /campaign inspect to view full details" },
  };
}

// ---------------------------------------------------------------------------
// buildCampaignInspectEmbed
// ---------------------------------------------------------------------------

/**
 * Campaign detail inspect embed.
 *
 * @param {object} payload  - campaign_targets row
 * @returns {object}
 */
export function buildCampaignInspectEmbed(payload = {}) {
  const {
    campaign_key      = "",
    campaign_name     = null,
    market            = "",
    asset_type        = "",
    strategy          = "",
    daily_cap         = null,
    status            = "",
    last_scan_at      = null,
    last_scan_summary = null,
    last_launched_at  = null,
    source_view_name  = null,
  } = payload;

  const scan = last_scan_summary ?? {};
  const scan_line =
    scan.eligible != null
      ? `Scanned: **${scan.scanned ?? "—"}** | Eligible: **${scan.eligible ?? "—"}** | Would Queue: **${scan.would_queue ?? "—"}**`
      : "No scan data yet";

  const status_color =
    status === "active" ? COLOR.green  :
    status === "paused" ? COLOR.yellow :
    COLOR.gray;

  return {
    title:     `📋 ${String(campaign_key).slice(0, 60)}`,
    color:     status_color,
    timestamp: now(),
    fields: [
      f("Campaign Key",  String(campaign_key).slice(0, 80),                   false),
      f("Name",          String(campaign_name || campaign_key).slice(0, 100), true),
      f("Market",        String(market).slice(0, 60),                         true),
      f("Asset Type",    String(asset_type).slice(0, 60),                     true),
      f("Strategy",      String(strategy).slice(0, 60),                       true),
      f("Daily Cap",     daily_cap != null ? String(daily_cap) : "—",         true),
      f("Status",        String(status).toUpperCase() || "—",                 true),
      f("Last Scan",     last_scan_at ? new Date(last_scan_at).toISOString().slice(0, 10) : "Never", true),
      f("Scan Summary",  scan_line,                                           false),
      f("Last Launch",   last_launched_at ? new Date(last_launched_at).toISOString().slice(0, 10) : "Never", true),
      ...(source_view_name ? [f("Source View", String(source_view_name).slice(0, 100), false)] : []),
    ].slice(0, 25),
    footer: { text: "Read-only campaign snapshot" },
  };
}

// ---------------------------------------------------------------------------
// buildCampaignScaleEmbed
// ---------------------------------------------------------------------------

/**
 * Campaign scale request or confirmation embed.
 *
 * @param {object}  payload
 * @param {string}  payload.campaign_key
 * @param {number}  [payload.current_cap]
 * @param {number}  [payload.requested_cap]
 * @param {string}  [payload.status]        - "applied" | "pending"
 * @param {string}  [payload.recommendation]
 * @param {string}  [payload.risk_level]    - "low" | "medium" | "high"
 * @returns {object}
 */
export function buildCampaignScaleEmbed(payload = {}) {
  const {
    campaign_key   = "",
    current_cap    = null,
    requested_cap  = null,
    status         = "applied",
    recommendation = "",
    risk_level     = "low",
  } = payload;

  const color =
    risk_level === "high"   ? COLOR.red    :
    risk_level === "medium" ? COLOR.yellow :
    COLOR.green;

  const title = status === "applied" ? "📈 Scale Applied" : "📈 Scale Request";

  return {
    title,
    color,
    timestamp: now(),
    fields: [
      f("Campaign",       String(campaign_key).slice(0, 80),                false),
      f("Current Cap",    current_cap   != null ? String(current_cap)   : "—", true),
      f("Requested Cap",  requested_cap != null ? String(requested_cap) : "—", true),
      f("Recommendation", String(recommendation).slice(0, 200),             false),
      f("Risk Level",     String(risk_level).toUpperCase(),                 true),
      f("Status",         String(status).toUpperCase(),                     true),
    ],
    footer: { text: status === "pending" ? "Owner approval required" : "Scale processed" },
  };
}

// ---------------------------------------------------------------------------
// buildTerritoryMapEmbed
// ---------------------------------------------------------------------------

/**
 * Territory map embed — shows all campaign_targets grouped by market.
 *
 * @param {object}   payload
 * @param {object}   payload.grouped  - { [market]: campaign_targets_row[] }
 * @param {boolean}  [payload.empty]  - true when no campaigns exist
 * @returns {object}
 */
export function buildTerritoryMapEmbed(payload = {}) {
  const { grouped = {}, empty = false } = payload;

  if (empty || Object.keys(grouped).length === 0) {
    return {
      title:       "🗺️ Territory Map",
      description: 'No territories unlocked yet. Create one with `/campaign create`.',
      color:       COLOR.gray,
      timestamp:   now(),
      footer:      { text: "Targeting Console v1" },
    };
  }

  const STATUS_ICONS = { active: "🟢", draft: "🟡", paused: "🔴" };

  const all_rows   = Object.values(grouped).flat();
  const total      = all_rows.length;
  const active     = all_rows.filter((r) => r.status === "active").length;
  const draft      = all_rows.filter((r) => r.status === "draft").length;
  const paused     = all_rows.filter((r) => r.status === "paused").length;

  const fields = Object.entries(grouped)
    .slice(0, 10)
    .map(([market, rows]) => {
      const summary = rows
        .map((r) => {
          const icon = STATUS_ICONS[r.status] ?? "⚪";
          return `${icon} ${String(r.asset_type ?? "").toUpperCase()} / ${String(r.strategy ?? "")} (cap: ${r.daily_cap ?? "—"})`;
        })
        .join("\n");
      return f(String(market).slice(0, 80), summary.slice(0, 1024));
    });

  return {
    title:       "🗺️ Territory Map",
    description: `**${total}** territories across **${Object.keys(grouped).length}** markets  |  Active: **${active}**  |  Draft: **${draft}**  |  Paused: **${paused}**`,
    color:       active > 0 ? COLOR.green : draft > 0 ? COLOR.blue : COLOR.gray,
    timestamp:   now(),
    fields:      fields.slice(0, 25),
    footer:      { text: "Targeting Console v1" },
  };
}

// ---------------------------------------------------------------------------
// buildConquestEmbed
// ---------------------------------------------------------------------------

/**
 * Empire-level conquest overview embed.
 *
 * @param {object}  payload
 * @param {number}  [payload.active]
 * @param {number}  [payload.draft]
 * @param {number}  [payload.paused]
 * @param {number}  [payload.total_daily_cap]
 * @param {number}  [payload.markets_unlocked]
 * @param {string}  [payload.last_scan]         - ISO date string
 * @param {string}  [payload.recommended_next_move]
 * @returns {object}
 */
export function buildConquestEmbed(payload = {}) {
  const {
    active                = 0,
    draft                 = 0,
    paused                = 0,
    total_daily_cap       = 0,
    markets_unlocked      = 0,
    last_scan             = null,
    recommended_next_move = "",
  } = payload;

  const total        = active + draft + paused;
  const color        = active > 0 ? COLOR.green : draft > 0 ? COLOR.blue : COLOR.gray;
  const last_scan_str = last_scan
    ? new Date(last_scan).toISOString().slice(0, 10)
    : "Never";

  return {
    title:     "⚔️ Conquest Overview",
    color,
    timestamp: now(),
    fields: [
      f("Active Campaigns",  String(active),          true),
      f("Draft Campaigns",   String(draft),           true),
      f("Paused Campaigns",  String(paused),          true),
      f("Total Daily Cap",   String(total_daily_cap), true),
      f("Markets Unlocked",  String(markets_unlocked),true),
      f("Total Campaigns",   String(total),           true),
      f("Last Scan",         last_scan_str,           true),
      f("Next Move",         String(recommended_next_move).slice(0, 200), false),
    ],
    footer: { text: "Empire Intelligence — Targeting Console v1" },
  };
}
