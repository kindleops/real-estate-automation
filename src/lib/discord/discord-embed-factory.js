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
  green:      0x2ECC71,
  yellow:     0xF1C40F,
  red:        0xE74C3C,
  blue:       0x3498DB,
  purple:     0x9B59B6,
  gray:       0x95A5A6,
  // v2 theme colors
  teal_green: 0x1ABC9C,
  gold_purple: 0x8E44AD,
  amber:      0xF39C12,
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
 * Target scan result embed (always dry-run). v2 — cinematic, themed.
 *
 * @param {object} payload
 * @param {string}  payload.market               - raw market value or label
 * @param {string}  payload.asset                - raw asset value or label
 * @param {string}  payload.strategy             - raw strategy value or label
 * @param {string}  [payload.market_label]       - resolved human-readable market
 * @param {string}  [payload.asset_label]        - resolved human-readable asset
 * @param {string}  [payload.strategy_label]     - resolved human-readable strategy
 * @param {object}  [payload.theme]              - { emoji, color, mode_label, intensity_label }
 * @param {object[]} [payload.tags]              - [{ slug, label }]
 * @param {object}  [payload.filters]            - { zip?, county?, min_equity?, ... }
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
 * @param {string}  [payload.risk_level]         - "low" | "medium" | "high"
 * @param {number}  [payload.readiness_score]    - 0–100
 * @param {string}  [payload.next_action]        - recommended next step
 * @returns {object}
 */
export function buildTargetScanEmbed(payload = {}) {
  const {
    market            = "",
    asset             = "",
    strategy          = "",
    market_label      = null,
    asset_label       = null,
    strategy_label    = null,
    theme             = null,
    tags              = [],
    filters           = {},
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
    readiness_score   = null,
    next_action       = null,
  } = payload;

  const rc = recommended_batch ?? eligible ?? 0;
  const rl = risk_level ?? (rc > 100 ? "high" : rc > 50 ? "medium" : "low");

  // Resolve theme color
  const theme_color_key = theme?.color ?? "blue";
  const base_color =
    rl === "high"   ? COLOR.red    :
    rl === "medium" ? COLOR.yellow :
    (COLOR[theme_color_key] ?? COLOR.green);

  const theme_emoji      = theme?.emoji ?? "🎯";
  const display_market   = market_label  ?? String(market).replace(/_/g, " ");
  const display_asset    = asset_label   ?? String(asset).replace(/_/g, " ");
  const display_strategy = strategy_label ?? String(strategy).replace(/_/g, " ");

  // Tags field
  const tags_value = Array.isArray(tags) && tags.length > 0
    ? tags.map((t) => `\`${t.label ?? t.slug}\``).join("  ·  ")
    : null;

  // Filters field — only show populated filters, never silently drop
  const filter_entries = Object.entries(filters ?? {})
    .filter(([, v]) => v != null && v !== "")
    .map(([k, v]) => `**${k.replace(/_/g, " ")}**: ${v}`);
  const filters_value = filter_entries.length > 0 ? filter_entries.join("  ·  ") : null;

  // Readiness score badge
  const readiness_str =
    readiness_score != null
      ? `${readiness_score}/100 ${readiness_score >= 70 ? "🟢" : readiness_score >= 40 ? "🟡" : "🔴"}`
      : "pending";

  const fields = [
    f("Territory",   `${theme_emoji}  ${String(display_market).slice(0, 80)}`, true),
    f("Asset Class", String(display_asset).slice(0, 60),                       true),
    f("Strategy",    String(display_strategy).slice(0, 60),                    true),
    ...(source_view_name ? [f("Source View", String(source_view_name).slice(0, 100), false)] : []),
    ...(tags_value   ? [f("Property Tags", tags_value.slice(0, 512),  false)] : []),
    ...(filters_value ? [f("Active Filters", filters_value.slice(0, 512), false)] : []),
    f("Scanned",           scanned     != null ? String(scanned)     : "—", true),
    f("Eligible",          eligible    != null ? String(eligible)    : "—", true),
    f("Would Queue",       would_queue != null ? String(would_queue) : "—", true),
    f("Skipped",           skipped     != null ? String(skipped)     : "—", true),
    f("No Phone",          no_phone    != null ? String(no_phone)    : "—", true),
    f("DNC",               dnc         != null ? String(dnc)         : "—", true),
    ...(template_source  != null ? [f("Template Source",  String(template_source),  true)] : []),
    ...(stage1_errors    != null ? [f("Stage 1 Errors",   String(stage1_errors),    true)] : []),
    f("Recommended Batch", String(rc),              true),
    f("Risk Level",        String(rl).toUpperCase(), true),
    f("Readiness",         readiness_str,            true),
    ...(next_action ? [f("Next Action", String(next_action).slice(0, 200), false)] : []),
  ].slice(0, 25);

  return {
    title:     `🎯 Target Scan — ${String(display_market).slice(0, 60)}`,
    color:     base_color,
    timestamp: now(),
    fields,
    footer:    { text: "Targeting Console v2 • Dry-run safe" },
  };
}

// ---------------------------------------------------------------------------
// buildCampaignCreatedEmbed
// ---------------------------------------------------------------------------

/**
 * Campaign created / upserted confirmation embed. v2 — cinematic, themed.
 *
 * @param {object} payload
 * @param {string}  payload.campaign_key
 * @param {string}  [payload.campaign_name]
 * @param {string}  payload.market
 * @param {string}  payload.asset
 * @param {string}  payload.strategy
 * @param {string}  [payload.market_label]
 * @param {string}  [payload.asset_label]
 * @param {string}  [payload.strategy_label]
 * @param {object}  [payload.theme]           - { emoji, color, mode_label }
 * @param {object[]} [payload.tags]           - [{ slug, label }]
 * @param {object}  [payload.filters]         - active filters map
 * @param {number}  [payload.daily_cap]
 * @param {string}  [payload.status]
 * @param {string}  [payload.source_view_name]
 * @returns {object}
 */
export function buildCampaignCreatedEmbed(payload = {}) {
  const {
    campaign_key      = "",
    campaign_name     = "",
    market            = "",
    asset             = "",
    strategy          = "",
    market_label      = null,
    asset_label       = null,
    strategy_label    = null,
    theme             = null,
    tags              = [],
    filters           = {},
    daily_cap         = 50,
    status            = "draft",
    source_view_name  = "",
  } = payload;

  const theme_emoji      = theme?.emoji ?? "🎮";
  const theme_color_key  = theme?.color ?? "blue";
  const color            = COLOR[theme_color_key] ?? COLOR.blue;

  const display_market   = market_label   ?? String(market).replace(/_/g, " ");
  const display_asset    = asset_label    ?? String(asset).replace(/_/g, " ");
  const display_strategy = strategy_label ?? String(strategy).replace(/_/g, " ");

  const tags_value = Array.isArray(tags) && tags.length > 0
    ? tags.map((t) => `\`${t.label ?? t.slug}\``).join("  ·  ")
    : null;

  const filter_entries = Object.entries(filters ?? {})
    .filter(([, v]) => v != null && v !== "")
    .map(([k, v]) => `**${k.replace(/_/g, " ")}**: ${v}`);
  const filters_value = filter_entries.length > 0 ? filter_entries.join("  ·  ") : null;

  return {
    title:     `${theme_emoji} Campaign Created`,
    color,
    timestamp: now(),
    fields: [
      f("Campaign Key",  String(campaign_key).slice(0, 80),                    false),
      f("Name",          String(campaign_name || campaign_key).slice(0, 100),  true),
      f("Territory",     String(display_market).slice(0, 60),                  true),
      f("Asset Class",   String(display_asset).slice(0, 60),                   true),
      f("Strategy",      String(display_strategy).slice(0, 60),                true),
      f("Daily Cap",     String(daily_cap),                                    true),
      f("Status",        String(status).toUpperCase(),                         true),
      ...(source_view_name ? [f("Source View", String(source_view_name).slice(0, 100), false)] : []),
      ...(tags_value   ? [f("Property Tags", tags_value.slice(0, 512), false)] : []),
      ...(filters_value ? [f("Active Filters", filters_value.slice(0, 512), false)] : []),
    ].slice(0, 25),
    footer: { text: "Targeting Console v2 • Use /campaign inspect to view full details" },
  };
}

// ---------------------------------------------------------------------------
// buildCampaignInspectEmbed
// ---------------------------------------------------------------------------

/**
 * Campaign detail inspect embed. v2 — shows tags and filters from metadata.
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
    metadata          = {},
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

  // Surface tags and filters from metadata if present
  const meta_tags    = metadata?.tags    ?? [];
  const meta_filters = metadata?.filters ?? {};

  const tags_value = Array.isArray(meta_tags) && meta_tags.length > 0
    ? meta_tags.map((t) => `\`${t.label ?? t.slug ?? t}\``).join("  ·  ")
    : null;

  const filter_entries = Object.entries(meta_filters)
    .filter(([, v]) => v != null && v !== "")
    .map(([k, v]) => `**${k.replace(/_/g, " ")}**: ${v}`);
  const filters_value = filter_entries.length > 0 ? filter_entries.join("  ·  ") : null;

  return {
    title:     `📋 ${String(campaign_key).slice(0, 60)}`,
    color:     status_color,
    timestamp: now(),
    fields: [
      f("Campaign Key",  String(campaign_key).slice(0, 80),                   false),
      f("Name",          String(campaign_name || campaign_key).slice(0, 100), true),
      f("Territory",     String(market).slice(0, 60),                         true),
      f("Asset Class",   String(asset_type).slice(0, 60),                     true),
      f("Strategy",      String(strategy).slice(0, 60),                       true),
      f("Daily Cap",     daily_cap != null ? String(daily_cap) : "—",         true),
      f("Status",        String(status).toUpperCase() || "—",                 true),
      f("Last Scan",     last_scan_at ? new Date(last_scan_at).toISOString().slice(0, 10) : "Never", true),
      f("Scan Summary",  scan_line,                                           false),
      f("Last Launch",   last_launched_at ? new Date(last_launched_at).toISOString().slice(0, 10) : "Never", true),
      ...(source_view_name ? [f("Source View", String(source_view_name).slice(0, 100), false)] : []),
      ...(tags_value    ? [f("Property Tags",  tags_value.slice(0, 512),    false)] : []),
      ...(filters_value ? [f("Active Filters", filters_value.slice(0, 512), false)] : []),
    ].slice(0, 25),
    footer: { text: "Targeting Console v2 • Read-only campaign snapshot" },
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
      footer:      { text: "Targeting Console v2" },
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
    footer:      { text: "Targeting Console v2" },
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
    footer: { text: "Empire Intelligence — Targeting Console v2" },
  };
}

// ---------------------------------------------------------------------------
// buildEmailCockpitEmbed
// ---------------------------------------------------------------------------

/**
 * Email Layer v1 cockpit overview embed.
 *
 * @param {object} payload
 * @param {object} [payload.queue_status_counts]   - { queued, sent, failed, delivered, opened, clicked }
 * @param {object} [payload.event_type_counts]     - { delivered, opened, clicked, hard_bounce, ... }
 * @param {number} [payload.queue_total]
 * @param {number} [payload.active_templates]
 * @param {number} [payload.suppression_total]
 * @param {string} [payload.latest_event_at]
 * @returns {object}
 */
export function buildEmailCockpitEmbed(payload = {}) {
  const {
    queue_status_counts  = {},
    event_type_counts    = {},
    queue_total          = 0,
    active_templates     = 0,
    suppression_total    = 0,
    latest_event_at      = null,
  } = payload;

  const queued    = queue_status_counts.queued    ?? 0;
  const sent      = queue_status_counts.sent      ?? 0;
  const failed    = queue_status_counts.failed    ?? 0;
  const delivered = queue_status_counts.delivered ?? 0;
  const opened    = queue_status_counts.opened    ?? 0;
  const clicked   = queue_status_counts.clicked   ?? 0;

  const ev_delivered = event_type_counts.delivered  ?? 0;
  const ev_opened    = event_type_counts.opened      ?? 0;
  const ev_bounced   = event_type_counts.hard_bounce ?? 0;
  const ev_spam      = event_type_counts.spam        ?? 0;

  const color = failed > 0 ? COLOR.red : queued > 0 ? COLOR.blue : COLOR.green;

  const last_event_str = latest_event_at
    ? new Date(latest_event_at).toISOString().slice(0, 16).replace("T", " ") + " UTC"
    : "No events yet";

  return {
    title:     "📧 Email Cockpit",
    color,
    timestamp: now(),
    fields: [
      f("Queued",       String(queued),    true),
      f("Sent",         String(sent),      true),
      f("Failed",       String(failed),    true),
      f("Delivered",    String(delivered), true),
      f("Opened",       String(opened),    true),
      f("Clicked",      String(clicked),   true),
      f("Bounces",      String(ev_bounced), true),
      f("Spam Reports", String(ev_spam),    true),
      f("Ev. Delivered", String(ev_delivered), true),
      f("Ev. Opened",   String(ev_opened), true),
      f("Total Queued", String(queue_total), true),
      f("Active Templates", String(active_templates), true),
      f("Suppressed",   String(suppression_total), true),
      f("Latest Event", last_event_str, false),
    ],
    footer: { text: "Email Layer v1 — Brevo" },
  };
}

// ---------------------------------------------------------------------------
// buildEmailPreviewEmbed
// ---------------------------------------------------------------------------

/**
 * @param {object} payload
 * @param {string} [payload.template_key]
 * @param {string} [payload.subject]
 * @param {string} [payload.html_body]
 * @param {string} [payload.text_body]
 * @param {string[]} [payload.missing_variables]
 * @returns {object}
 */
export function buildEmailPreviewEmbed(payload = {}) {
  const {
    template_key      = "unknown",
    subject           = "(no subject)",
    html_body         = "",
    text_body         = "",
    missing_variables = [],
  } = payload;

  const preview_text = (text_body || html_body.replace(/<[^>]+>/g, " "))
    .trim()
    .slice(0, 300);

  const color = missing_variables.length > 0 ? COLOR.yellow : COLOR.blue;

  const fields = [
    f("Template Key", template_key, true),
    f("Subject",      subject,      false),
    f("Preview",      preview_text || "(empty)", false),
  ];

  if (missing_variables.length > 0) {
    fields.push(f("⚠️ Missing Variables", missing_variables.join(", "), false));
  }

  return {
    title:     "🔍 Email Template Preview",
    color,
    timestamp: now(),
    fields,
    footer: { text: "Preview only — will NOT send" },
  };
}

// ---------------------------------------------------------------------------
// buildEmailSendTestEmbed
// ---------------------------------------------------------------------------

/**
 * @param {object} payload
 * @param {boolean} [payload.sent]
 * @param {string}  [payload.email_address]
 * @param {string}  [payload.template_key]
 * @param {string}  [payload.brevo_message_id]
 * @param {string}  [payload.error]
 * @returns {object}
 */
export function buildEmailSendTestEmbed(payload = {}) {
  const {
    sent             = false,
    email_address    = "unknown",
    template_key     = "unknown",
    brevo_message_id = null,
    error            = null,
  } = payload;

  const color = sent ? COLOR.green : COLOR.red;

  const fields = [
    f("Recipient",    email_address, true),
    f("Template Key", template_key,  true),
    f("Status",       sent ? "✅ Sent" : "❌ Failed", true),
  ];

  if (brevo_message_id) fields.push(f("Brevo Message ID", brevo_message_id, false));
  if (error)            fields.push(f("Error", String(error).slice(0, 200), false));

  return {
    title:     "📤 Email Send Test",
    color,
    timestamp: now(),
    fields,
    footer: { text: "Test send via Brevo transactional API" },
  };
}

// ---------------------------------------------------------------------------
// buildEmailStatsEmbed
// ---------------------------------------------------------------------------

/**
 * @param {object} payload
 * @param {object} [payload.event_type_counts]
 * @param {string} [payload.latest_event_at]
 * @param {number} [payload.suppression_total]
 * @returns {object}
 */
export function buildEmailStatsEmbed(payload = {}) {
  const {
    event_type_counts  = {},
    latest_event_at    = null,
    suppression_total  = 0,
  } = payload;

  const last_event_str = latest_event_at
    ? new Date(latest_event_at).toISOString().slice(0, 16).replace("T", " ") + " UTC"
    : "No events yet";

  const fields = Object.entries(event_type_counts).map(([k, v]) =>
    f(k.replace(/_/g, " "), String(v), true)
  );

  fields.push(f("Suppressed Addresses", String(suppression_total), true));
  fields.push(f("Latest Event", last_event_str, false));

  return {
    title:     "📊 Email Stats",
    color:     COLOR.purple,
    timestamp: now(),
    fields:    fields.slice(0, 25),
    footer:    { text: "Email Layer v1 — event log stats" },
  };
}

// ---------------------------------------------------------------------------
// buildEmailSuppressionEmbed
// ---------------------------------------------------------------------------

/**
 * @param {object} payload
 * @param {number} [payload.suppression_total]
 * @param {object[]} [payload.recent_suppressions]  - array of { email_address, reason, suppressed_at }
 * @returns {object}
 */
export function buildEmailSuppressionEmbed(payload = {}) {
  const {
    suppression_total    = 0,
    recent_suppressions  = [],
  } = payload;

  const fields = [f("Total Suppressed", String(suppression_total), false)];

  if (recent_suppressions.length > 0) {
    const list = recent_suppressions.slice(0, 10).map(r => {
      const date = r.suppressed_at
        ? new Date(r.suppressed_at).toISOString().slice(0, 10)
        : "unknown";
      return `\`${r.email_address}\`  —  ${r.reason ?? "unknown"}  (${date})`;
    }).join("\n");
    fields.push(f("Recent Suppressions", list, false));
  }

  return {
    title:     "🚫 Email Suppression List",
    color:     COLOR.red,
    timestamp: now(),
    fields,
    footer:    { text: "Hard-bounce / spam / unsubscribe auto-suppression" },
  };
}

// ---------------------------------------------------------------------------
// buildReplayInboundEmbed
// ---------------------------------------------------------------------------

/**
 * Inbound seller reply simulation result embed.
 *
 * @param {object} payload
 * @param {string} [payload.message_body]
 * @param {object} [payload.classification]
 * @param {string} [payload.previous_stage]
 * @param {string} [payload.next_stage]
 * @param {string} [payload.selected_use_case]
 * @param {string} [payload.selected_template_source]
 * @param {boolean} [payload.would_queue_reply]
 * @param {object} [payload.underwriting_signals]
 * @param {string} [payload.underwriting_route]
 * @param {boolean} [payload.alignment_passed]
 * @returns {object}
 */
export function buildReplayInboundEmbed(payload = {}) {
  const {
    message_body           = "(no text)",
    classification         = {},
    previous_stage         = null,
    next_stage             = null,
    selected_use_case      = null,
    selected_template_source = null,
    would_queue_reply      = false,
    underwriting_signals   = {},
    underwriting_route     = null,
    alignment_passed       = true,
  } = payload;

  const color = alignment_passed ? COLOR.green : COLOR.yellow;
  const msg_preview = String(message_body).slice(0, 150).trim();

  const fields = [
    f("Seller Text",  msg_preview || "(empty)", false),
    f("Language",     classification.language ?? "unknown", true),
    f("Objection",    classification.objection ?? "no", true),
    f("Emotion",      classification.emotion ?? "neutral", true),
    f("Current Stage",   previous_stage ?? "unknown", true),
    f("Next Stage",      next_stage ?? "none", true),
    f("Selected Use Case", selected_use_case ?? "unknown", true),
    f("Template Source",  selected_template_source ?? "unresolved", true),
    f("Property Type",    underwriting_signals?.property_type ?? "unknown", true),
    f("Strategy",        underwriting_signals?.creative_strategy ?? "unknown", true),
    f("Would Queue",      would_queue_reply ? "✅ Yes" : "❌ No", true),
    f("Underwriting Route", underwriting_route ?? "none", true),
    f("Alignment",        alignment_passed ? "✅ Pass" : "⚠️ Warning", true),
  ];

  return {
    title:     "🎮 Inbound Replay",
    color,
    timestamp: now(),
    fields,
    footer:    { text: "Dry-run simulation — no SMS sent, no queue writes" },
  };
}

// ---------------------------------------------------------------------------
// buildReplayOwnerEmbed
// ---------------------------------------------------------------------------

/**
 * Owner-specific replay result with real context.
 *
 * @param {object} payload
 * @param {string} [payload.owner_id]
 * @param {string} [payload.owner_name]
 * @param {string} [payload.property_address]
 * @param {string} [payload.property_type]
 * @param {string} [payload.message_body]
 * @param {object} [payload.classification]
 * @param {string} [payload.current_stage]
 * @param {string} [payload.next_stage]
 * @param {string} [payload.selected_use_case]
 * @param {string} [payload.selected_template_source]
 * @param {string} [payload.cash_offer_snapshot]
 * @param {string} [payload.underwriting_route]
 * @param {boolean} [payload.would_queue]
 * @returns {object}
 */
export function buildReplayOwnerEmbed(payload = {}) {
  const {
    owner_id                = "unknown",
    owner_name              = null,
    property_address        = "unknown address",
    property_type           = "unknown",
    message_body            = "(no text)",
    classification          = {},
    current_stage           = null,
    next_stage              = null,
    selected_use_case       = null,
    selected_template_source = null,
    cash_offer_snapshot     = null,
    underwriting_route      = null,
    would_queue             = false,
  } = payload;

  const msg_preview = String(message_body).slice(0, 100).trim();

  const fields = [
    f("Owner",            owner_name || owner_id, true),
    f("Property",         property_address, true),
    f("Property Type",    property_type, true),
    f("Latest Stage",     current_stage ?? "unknown", true),
    f("Next Stage",       next_stage ?? "none", true),
    f("Seller Text",      msg_preview || "(empty)", false),
    f("Classification",   classification.stage_hint ?? "unknown", true),
    f("Route",            next_stage || "no route", true),
    f("Template Use Case", selected_use_case ?? "unknown", true),
    f("Template Source",  selected_template_source ?? "unresolved", true),
    f("Cash Offer Snapshot", cash_offer_snapshot ?? "—", true),
    f("Underwriting",     underwriting_route ?? "none", true),
    f("Would Queue",      would_queue ? "✅ Yes" : "❌ No", true),
  ];

  return {
    title:     "🧠 Owner Replay",
    color:     COLOR.blue,
    timestamp: now(),
    fields,
    footer:    { text: `Owner ${owner_id} — dry-run only` },
  };
}

// ---------------------------------------------------------------------------
// buildReplayTemplateEmbed
// ---------------------------------------------------------------------------

/**
 * Template resolution and preview embed.
 *
 * @param {object} payload
 * @param {string} [payload.use_case]
 * @param {string} [payload.template_id]
 * @param {string} [payload.template_source]
 * @param {string} [payload.stage_code]
 * @param {string} [payload.language]
 * @param {string} [payload.template_text]
 * @param {string} [payload.property_type_resolved]
 * @returns {object}
 */
export function buildReplayTemplateEmbed(payload = {}) {
  const {
    use_case              = "unknown",
    template_id           = "unknown",
    template_source       = "unknown",
    stage_code            = null,
    language              = "English",
    template_text         = "(no template)",
    property_type_resolved = "Residential",
  } = payload;

  const text_preview = String(template_text).slice(0, 200).trim();

  const fields = [
    f("Use Case",         use_case, true),
    f("Template ID",      template_id, true),
    f("Stage Code",       stage_code || "—", true),
    f("Source",           template_source, true),
    f("Language",         language, true),
    f("Property Type",    property_type_resolved, true),
    f("Preview",          text_preview || "(empty)", false),
  ];

  return {
    title:     "📋 Template Resolution",
    color:     COLOR.purple,
    timestamp: now(),
    fields,
    footer:    { text: "Template preview with safe mock context" },
  };
}

// ---------------------------------------------------------------------------
// buildReplayBatchEmbed
// ---------------------------------------------------------------------------

/**
 * Batch scenario test results summary.
 *
 * @param {object} payload
 * @param {string} [payload.scenario]
 * @param {number} [payload.tested]
 * @param {number} [payload.passed]
 * @param {number} [payload.warnings]
 * @param {number} [payload.failed]
 * @param {object[]} [payload.results] - array of { name, status, note }
 * @returns {object}
 */
export function buildReplayBatchEmbed(payload = {}) {
  const {
    scenario  = "unknown",
    tested    = 0,
    passed    = 0,
    warnings  = 0,
    failed    = 0,
    results   = [],
  } = payload;

  const status_line = `✅ ${passed} pass  ⚠️ ${warnings} warn  ❌ ${failed} fail  (${tested} total)`;

  const result_list = results.slice(0, 10).map(r => {
    const icon = r.status === "pass" ? "✅" : r.status === "warn" ? "⚠️" : "❌";
    return `${icon} ${String(r.name).slice(0, 40)}`;
  }).join("\n");

  const fields = [
    f("Scenario",  scenario, true),
    f("Tested",    String(tested), true),
    f("Status",    status_line, false),
  ];

  if (result_list) {
    fields.push(f("Results",  result_list, false));
  }

  const color = failed > 0 ? COLOR.red : warnings > 0 ? COLOR.yellow : COLOR.green;

  return {
    title:     "📊 Batch Test Results",
    color,
    timestamp: now(),
    fields,
    footer:    { text: "Scenario batch dry-run simulation" },
  };
}

// ---------------------------------------------------------------------------
// buildWireCockpitEmbed
// ---------------------------------------------------------------------------

/**
 * Wire command center cockpit — summary of expected, pending, received, cleared.
 *
 * @param {object} payload
 * @param {number} [payload.expected]
 * @param {number} [payload.pending]
 * @param {number} [payload.received]
 * @param {number} [payload.cleared]
 * @param {number} [payload.total_amount]
 * @param {string} [payload.days]
 * @returns {object}
 */
export function buildWireCockpitEmbed(payload = {}) {
  const {
    expected      = 0,
    pending       = 0,
    received      = 0,
    cleared       = 0,
    total_amount  = 0,
    days          = "7",
  } = payload;

  const total = expected + pending + received + cleared;
  const summary = `📩 ${expected} expected  ⏳ ${pending} pending  ✓ ${received} received  ✅ ${cleared} cleared`;

  const fields = [
    f("Summary",          summary, false),
    f("Scope",            `Last ${days} days`, true),
    f("Total Wires",      String(total), true),
    f("Total Amount",     `$${Number(total_amount).toLocaleString("en-US", { maximumFractionDigits: 2 })}`, true),
    f("Next 5 Expected",  "(use forecast command)", false),
  ];

  const url_emoji = "https://cdn.discordapp.com/emojis/879519779192045579.png";
  return {
    title:     "💸 Wire Command Center",
    color:     COLOR.blue,
    timestamp: now(),
    fields,
    footer:    { text: "Wire tracking cockpit" },
  };
}

// ---------------------------------------------------------------------------
// buildWireExpectedEmbed
// ---------------------------------------------------------------------------

/**
 * Expected wire event created confirmation.
 *
 * @param {object}  payload
 * @param {number}  [payload.amount]
 * @param {string}  [payload.account_display]
 * @param {string}  [payload.deal_key]
 * @param {string}  [payload.expected_at]
 * @param {string}  [payload.wire_key]
 * @returns {object}
 */
export function buildWireExpectedEmbed(payload = {}) {
  const {
    amount          = 0,
    account_display = "—",
    deal_key        = null,
    expected_at     = null,
    wire_key        = "pending",
  } = payload;

  const fields = [
    f("Amount",           `$${Number(amount).toLocaleString("en-US", { maximumFractionDigits: 2 })}`, true),
    f("Account",          account_display, true),
    f("Expected",         expected_at ? new Date(expected_at).toLocaleString() : "—", true),
    f("Wire Key",         `\`${wire_key}\``, false),
  ];

  if (deal_key) {
    fields.push(f("Deal",  deal_key, true));
  }

  return {
    title:     "📨 Expected Wire Created",
    color:     COLOR.blue,
    timestamp: now(),
    fields,
    footer:    { text: "Wire event created — no bank movement initiated" },
  };
}

// ---------------------------------------------------------------------------
// buildWireReceivedEmbed
// ---------------------------------------------------------------------------

/**
 * Wire marked as received.
 *
 * @param {object} payload
 * @param {string} [payload.wire_key]
 * @param {number} [payload.amount]
 * @param {string} [payload.received_at]
 * @param {string} [payload.note]
 * @returns {object}
 */
export function buildWireReceivedEmbed(payload = {}) {
  const {
    wire_key    = "—",
    amount      = 0,
    received_at = null,
    note        = null,
  } = payload;

  const fields = [
    f("Wire Key",     `\`${wire_key}\``, true),
    f("Amount",       `$${Number(amount).toLocaleString("en-US", { maximumFractionDigits: 2 })}`, true),
    f("Received",     received_at ? new Date(received_at).toLocaleString() : new Date().toLocaleString(), true),
  ];

  if (note) {
    fields.push(f("Note", note, false));
  }

  return {
    title:     "✓ Wire Received",
    color:     COLOR.green,
    timestamp: now(),
    fields,
    footer:    { text: "Wire marked as in-transit" },
  };
}

// ---------------------------------------------------------------------------
// buildWireClearedEmbed
// ---------------------------------------------------------------------------

/**
 * Wire marked as cleared.
 *
 * @param {object} payload
 * @param {string} [payload.wire_key]
 * @param {number} [payload.amount]
 * @param {string} [payload.cleared_at]
 * @param {string} [payload.note]
 * @returns {object}
 */
export function buildWireClearedEmbed(payload = {}) {
  const {
    wire_key    = "—",
    amount      = 0,
    cleared_at  = null,
    note        = null,
  } = payload;

  const fields = [
    f("Wire Key",   `\`${wire_key}\``, true),
    f("Amount",     `$${Number(amount).toLocaleString("en-US", { maximumFractionDigits: 2 })}`, true),
    f("Cleared",    cleared_at ? new Date(cleared_at).toLocaleString() : new Date().toLocaleString(), true),
  ];

  if (note) {
    fields.push(f("Note", note, false));
  }

  return {
    title:     "✅ Wire Cleared",
    color:     COLOR.green,
    timestamp: now(),
    fields,
    footer:    { text: "Wire fully cleared and settled" },
  };
}

// ---------------------------------------------------------------------------
// buildWireForecastEmbed
// ---------------------------------------------------------------------------

/**
 * Wire forecast — expected wires over next N days.
 *
 * @param {object}  payload
 * @param {number}  [payload.days_ahead]
 * @param {number}  [payload.total_expected]
 * @param {number}  [payload.total_pending]
 * @param {number}  [payload.total_amount]
 * @param {object[]} [payload.wires] - [{ expected_at, days_until, amount, account_key }]
 * @param {number}  [payload.confidence_score]
 * @returns {object}
 */
export function buildWireForecastEmbed(payload = {}) {
  const {
    days_ahead      = 14,
    total_expected  = 0,
    total_pending   = 0,
    total_amount    = 0,
    wires           = [],
    confidence_score = 0,
  } = payload;

  const forecast_list = wires.slice(0, 5).map(w => {
    const days = w.days_until > 0 ? `in ${w.days_until}d` : "today";
    const amt = `$${Number(w.amount).toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
    return `${w.expected_at ? new Date(w.expected_at).toLocaleDateString() : "—"} · ${amt} (${days})`;
  }).join("\n");

  const fields = [
    f("Horizon",           `${days_ahead} days`, true),
    f("Confidence",        `${confidence_score}%`, true),
    f("Total Expected",    String(total_expected), true),
    f("Total Pending",     String(total_pending), true),
    f("Total Amount",      `$${Number(total_amount).toLocaleString("en-US", { maximumFractionDigits: 2 })}`, true),
    f("Next 5 Wires",      forecast_list || "(none expected)", false),
  ];

  const color = confidence_score >= 80 ? COLOR.green : confidence_score >= 60 ? COLOR.yellow : COLOR.red;

  return {
    title:     "📅 Wire Forecast",
    color,
    timestamp: now(),
    fields,
    footer:    { text: "7-day and 14-day forecasts available" },
  };
}

// ---------------------------------------------------------------------------
// buildWireDealEmbed
// ---------------------------------------------------------------------------

/**
 * Wires linked to a deal / property / closing.
 *
 * @param {object}  payload
 * @param {string}  [payload.deal_key]
 * @param {object[]} [payload.wires] - [{ wire_key, status, amount, expected_at }]
 * @param {string}  [payload.property_address]
 * @param {string}  [payload.closing_status]
 * @returns {object}
 */
export function buildWireDealEmbed(payload = {}) {
  const {
    deal_key         = "—",
    wires            = [],
    property_address = null,
    closing_status   = null,
  } = payload;

  const wire_links = wires.slice(0, 10).map(w => {
    const icon = w.status === "cleared" ? "✅" : w.status === "received" ? "✓" : "📨";
    const amt = `$${Number(w.amount).toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
    return `${icon} ${w.status} · ${amt}`;
  }).join("\n");

  const fields = [
    f("Deal Key",      deal_key, true),
    f("Wire Count",    String(wires.length), true),
  ];

  if (property_address) {
    fields.push(f("Property",  property_address, false));
  }
  if (closing_status) {
    fields.push(f("Closing Status",  closing_status, true));
  }

  fields.push(f("Wires",  wire_links || "(no wires linked)", false));

  return {
    title:     "🔗 Deal Wire Summary",
    color:     COLOR.purple,
    timestamp: now(),
    fields,
    footer:    { text: "Wires linked to deal/property/closing" },
  };
}

// ---------------------------------------------------------------------------
// buildWireReconcileEmbed
// ---------------------------------------------------------------------------

/**
 * Wire reconciliation anomalies.
 *
 * @param {object}  payload
 * @param {number}  [payload.missing_account_links]
 * @param {number}  [payload.missing_deal_links]
 * @param {number}  [payload.stale_pending]
 * @param {number}  [payload.total_anomalies]
 * @param {string}  [payload.scope_days]
 * @returns {object}
 */
export function buildWireReconcileEmbed(payload = {}) {
  const {
    missing_account_links = 0,
    missing_deal_links    = 0,
    stale_pending         = 0,
    total_anomalies       = 0,
    scope_days            = "30",
  } = payload;

  const issues = [];
  if (missing_account_links > 0) issues.push(`⚠️ ${missing_account_links} wires missing account link`);
  if (missing_deal_links > 0)    issues.push(`⚠️ ${missing_deal_links} wires missing deal/closing link`);
  if (stale_pending > 0)         issues.push(`⚠️ ${stale_pending} wires pending > 7 days`);

  const anomaly_text = issues.length > 0
    ? issues.join("\n")
    : "✅ No anomalies detected";

  const fields = [
    f("Scope",            `Last ${scope_days} days`, true),
    f("Total Issues",     String(total_anomalies), true),
    f("Issues",           anomaly_text, false),
  ];

  const color = total_anomalies > 5 ? COLOR.red : total_anomalies > 0 ? COLOR.yellow : COLOR.green;

  return {
    title:     "🔍 Wire Reconciliation",
    color,
    timestamp: now(),
    fields,
    footer:    { text: "Anomaly detection and reconciliation report" },
  };
}

// ---------------------------------------------------------------------------
// buildWireSetupRequiredEmbed
// ---------------------------------------------------------------------------

/**
 * Shown when wire_events table is missing or schema cache is stale.
 * No sensitive data is included.
 *
 * @returns {object}
 */
export function buildWireSetupRequiredEmbed() {
  return {
    title:       "⚠️ Wire Command Center Setup Required",
    description: "Wire tables are not available yet. Apply the Supabase migration and reload schema cache.",
    color:       COLOR.yellow,
    timestamp:   now(),
    fields: [
      {
        name:   "Step 1 — Apply Migration",
        value:  "Run pending wire migrations:\n`create wire_accounts`\n`create wire_events`",
        inline: false,
      },
      {
        name:   "Step 2 — Reload Schema Cache",
        value:  "Run in Supabase SQL editor:\n```sql\nselect pg_notify('pgrst', 'reload schema');\n```",
        inline: false,
      },
      {
        name:   "Step 3 — Retry",
        value:  "Re-run the `/wires` command after the migration completes.",
        inline: false,
      },
    ],
    footer: { text: "Wire Command Center v1 • Setup required" },
  };
}

// ---------------------------------------------------------------------------
// buildDailyBriefingEmbed
// ---------------------------------------------------------------------------

/**
 * Cinematic Daily Empire Briefing embed.
 *
 * Accepts the metrics object returned by getDailyBriefing / normalizeBriefingMetrics.
 * Displays all KPI sections concisely without raw DB errors or secrets.
 *
 * Color rules:
 *   red    → critical failures or high send-queue errors
 *   yellow → partial data / unavailable sections
 *   purple → strong revenue day
 *   green  → nominal operations
 *
 * @param {object} metrics
 * @returns {object}  Discord embed object
 */
export function buildDailyBriefingEmbed(metrics = {}) {
  const {
    range         = "today",
    timezone      = "America/Chicago",
    window_start,
    window_end,
    outreach      = {},
    email         = {},
    acquisitions  = {},
    dispo         = {},
    revenue       = {},
    system_health = {},
    markets       = [],
    agents        = [],
    source_errors = [],
    partial       = false,
    health        = "green",
    next_recommended_action = null,
  } = metrics;

  // ── Color ────────────────────────────────────────────────────────────────
  const colorMap = {
    red:    COLOR.red,
    yellow: COLOR.yellow,
    purple: COLOR.gold_purple,
    green:  COLOR.teal_green,
  };
  const color = colorMap[health] ?? COLOR.teal_green;

  // ── Description ──────────────────────────────────────────────────────────
  const rangeLabel = {
    today:     "Today",
    yesterday: "Yesterday",
    week:      "Last 7 Days",
    month:     "This Month",
  }[range] ?? range;

  const scopeParts = [];
  if (markets.length > 0) scopeParts.push(`Market: ${markets.join(", ")}`);
  if (agents.length  > 0) scopeParts.push(`Agent: ${agents.join(", ")}`);
  const scopeStr = scopeParts.length > 0 ? `  •  ${scopeParts.join("  •  ")}` : "";

  const dateStr = window_start
    ? new Date(window_start).toLocaleDateString("en-US", {
        timeZone: timezone,
        month: "short", day: "numeric", year: "numeric",
      })
    : "";
  const description = `📅 ${rangeLabel}${dateStr ? `  •  ${dateStr}` : ""}  •  ${timezone}${scopeStr}`;

  // ── Helper: compact number format ───────────────────────────────────────
  const n = (v) => Number(v ?? 0).toLocaleString("en-US");
  const pct = (v) => `${Number(v ?? 0)}%`;
  const usd = (v) => {
    const amt = Number(v ?? 0);
    if (amt >= 1_000_000) return `$${(amt / 1_000_000).toFixed(2)}M`;
    if (amt >= 1_000)     return `$${(amt / 1_000).toFixed(1)}K`;
    return `$${amt.toLocaleString("en-US")}`;
  };
  const na = (v, fmt = n) => (v == null || Number(v) === 0 && !v) ? "—" : fmt(v);

  // ── Fields ───────────────────────────────────────────────────────────────
  const fields = [];

  // 1. Outreach Engine
  const o = outreach;
  fields.push(f(
    "📨 Outreach Engine",
    [
      `Sent: **${n(o.sent)}**  |  Delivered: **${n(o.delivered)}**  |  Failed: **${n(o.failed)}**`,
      `Replies: **${n(o.replies)}**  |  Reply Rate: **${pct(o.reply_rate)}**  |  Delivery Rate: **${pct(o.delivery_rate)}**`,
      o.opt_outs > 0 || o.wrong_numbers > 0
        ? `Opt-outs: **${n(o.opt_outs)}**  |  Wrong #: **${n(o.wrong_numbers)}**`
        : null,
    ].filter(Boolean).join("\n"),
    false,
  ));

  // 2. Email
  const em = email;
  const emailUnavailable = source_errors.some(e => e.source === "email_send_queue");
  fields.push(f(
    "✉️ Email",
    emailUnavailable
      ? "Unavailable — email data not configured"
      : `Sent: **${n(em.sent)}**  |  Delivered: **${n(em.delivered)}**  |  Opened: **${n(em.opened)}**  |  Clicked: **${n(em.clicked)}**`,
    false,
  ));

  // 3. Lead Flow / Hot Leads
  const aq = acquisitions;
  fields.push(f(
    "🔥 Lead Flow",
    [
      `Hot Leads: **${n(aq.hot_leads)}**  |  Stage Advances: **${n(aq.stage_advances)}**`,
      `Offers Created: **${n(aq.offers_created)}**  |  Offers Sent: **${n(aq.offers_sent)}**`,
    ].join("\n"),
    false,
  ));

  // 4. Acquisitions
  fields.push(f(
    "💵 Acquisitions",
    [
      `Contracts Sent: **${n(aq.contracts_sent)}**  |  Contracts Signed: **${n(aq.contracts_signed)}**`,
      aq.underwriting_transfers > 0
        ? `Underwriting Transfers: **${n(aq.underwriting_transfers)}**  |  Manual Reviews: **${n(aq.manual_reviews)}**`
        : `Underwriting Transfers: **${n(aq.underwriting_transfers)}**`,
    ].join("\n"),
    false,
  ));

  // 5. Dispo / Buyers / JV
  const dp = dispo;
  fields.push(f(
    "🤝 Dispo / Buyers / JV",
    `Buyer Matches: **${n(dp.buyer_matches)}**  |  Buyer Replies: **${n(dp.buyer_replies)}**  |  JV Opportunities: **${n(dp.jv_opportunities)}**`,
    false,
  ));

  // 6. Revenue / Wires
  const rv = revenue;
  const wireUnavailable = source_errors.some(e => e.source === "wire_events");
  fields.push(f(
    "🏦 Revenue / Wires",
    wireUnavailable
      ? "Unavailable — wire data not configured or migration pending"
      : [
          `Cleared: **${n(rv.cleared_wires)}** (${usd(rv.cleared_wire_amount)})  |  Pending: **${n(rv.pending_wires)}** (${usd(rv.pending_wire_amount)})`,
          `Pipeline: **${usd(rv.projected_pipeline_value)}**`,
        ].join("\n"),
    false,
  ));

  // 7. System Health
  const sh = system_health;
  const queueUnavailable = source_errors.some(e => e.source === "send_queue");
  const queueLine = queueUnavailable
    ? "Queue: Unavailable"
    : `Queue: **${n(sh.queue_ready)}** ready  |  **${n(sh.queue_due)}** due  |  **${n(sh.queue_failed_recent)}** failed`;
  fields.push(f(
    "🧠 System Health",
    [
      queueLine,
      `Supabase: **${sh.supabase_status}**  |  Podio: **${sh.podio_status}**`,
      `TextGrid: **${sh.textgrid_status}**  |  Email: **${sh.email_status}**`,
    ].join("\n"),
    false,
  ));

  // 8. Next Move
  fields.push(f(
    "🎯 Next Move",
    next_recommended_action ?? "✅ Operations nominal",
    false,
  ));

  // 9. Partial warning (if any sources errored)
  if (partial && source_errors.length > 0) {
    // Map internal source IDs to user-friendly labels — never expose raw table names.
    const SOURCE_LABELS = {
      message_events:  "SMS events",
      email_send_queue: "Email queue",
      wire_events:     "Wire data",
      send_queue:      "Send queue",
      campaign_targets: "Campaign targets",
    };
    const friendly = [...new Set(
      source_errors.map(e => SOURCE_LABELS[e.source] ?? "one or more data sources")
    )].join(", ");
    fields.push(f(
      "⚠️ Partial Data",
      `Some data was unavailable: ${friendly}. Metrics may be incomplete.`,
      false,
    ));
  }

  return {
    title:       "👑 Daily Empire Briefing",
    description: description.slice(0, 256),
    color,
    timestamp:   now(),
    fields,
    footer: { text: "Empire Briefing • Real Estate Automation" },
  };
}
