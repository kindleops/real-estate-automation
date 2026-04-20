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
