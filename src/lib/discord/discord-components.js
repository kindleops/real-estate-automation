/**
 * discord-components.js
 *
 * Button component builders for Discord interaction responses.
 *
 * custom_id prefix registry (safe, colon-separated):
 *   mission:    – /mission subcommand shortcuts
 *   queue:      – /queue subcommand shortcuts
 *   preflight:  – /launch preflight shortcuts
 *   templates:  – /templates subcommand shortcuts
 *   lead:       – /lead subcommand shortcuts
 *   campaign:   – campaign control actions
 *   approval:   – approval / deny gate (new style)
 *
 * Button styles:
 *   PRIMARY   1  blurple
 *   SECONDARY 2  grey
 *   SUCCESS   3  green
 *   DANGER    4  red
 */

const STYLE = {
  PRIMARY:   1,
  SECONDARY: 2,
  SUCCESS:   3,
  DANGER:    4,
};

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

/**
 * Build a Discord button component object.
 *
 * @param {object} opts
 * @param {string}  opts.label
 * @param {string}  opts.custom_id  - max 100 chars
 * @param {number}  [opts.style]    - STYLE constant
 * @param {boolean} [opts.disabled]
 * @returns {object}
 */
function button({ label, custom_id, style = STYLE.PRIMARY, disabled = false }) {
  return {
    type:      2,   // BUTTON
    style,
    label:     String(label).slice(0, 80),
    custom_id: String(custom_id).slice(0, 100),
    disabled:  Boolean(disabled),
  };
}

/**
 * Wrap buttons in a Discord ACTION_ROW (max 5 buttons per row).
 * @param {object[]} buttons
 * @returns {object}
 */
function actionRow(buttons) {
  return { type: 1, components: buttons.slice(0, 5) };
}

// ---------------------------------------------------------------------------
// Public button builders
// ---------------------------------------------------------------------------

/**
 * Buttons for /mission status output.
 * @returns {object[]}  Array of action rows
 */
export function missionButtons() {
  return [
    actionRow([
      button({ label: "Refresh Status",   custom_id: "mission:refresh",   style: STYLE.SECONDARY }),
      button({ label: "Launch Preflight", custom_id: "mission:preflight", style: STYLE.PRIMARY   }),
    ]),
  ];
}

/**
 * Buttons for /queue cockpit output.
 * @returns {object[]}
 */
export function queueButtons() {
  return [
    actionRow([
      button({ label: "Cockpit",        custom_id: "queue:cockpit",  style: STYLE.SECONDARY }),
      button({ label: "Run Queue (10)", custom_id: "queue:run:10",   style: STYLE.PRIMARY   }),
    ]),
  ];
}

/**
 * Buttons for /launch preflight output.
 * @returns {object[]}
 */
export function preflightButtons() {
  return [
    actionRow([
      button({ label: "Recheck",     custom_id: "preflight:recheck",     style: STYLE.PRIMARY   }),
      button({ label: "Feeder Scan", custom_id: "preflight:scan_feeder", style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for /templates audit output.
 * @returns {object[]}
 */
export function templateAuditButtons() {
  return [
    actionRow([
      button({ label: "Stage 1 Detail", custom_id: "templates:stage1", style: STYLE.PRIMARY   }),
      button({ label: "Full Audit",     custom_id: "templates:audit",  style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for /lead inspect output.
 *
 * @param {object} opts
 * @param {string} [opts.ownerId]  - owner ID to embed in custom_id
 * @param {string} [opts.phone]    - phone number (E.164) to embed in custom_id
 * @returns {object[]}
 */
export function leadInspectButtons({ ownerId = "", phone = "" } = {}) {
  // Strip characters that are unsafe in custom_ids.
  const safe_owner = String(ownerId).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  const safe_phone = String(phone).replace(/[^0-9+]/g, "").slice(0, 20);

  return [
    actionRow([
      button({ label: "Inspect",      custom_id: `lead:inspect:${safe_owner}`,  style: STYLE.PRIMARY   }),
      button({ label: "Mark Handled", custom_id: `lead:handled:${safe_phone}`,  style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for campaign control.
 *
 * @param {object}  opts
 * @param {string}  opts.campaignId
 * @param {boolean} opts.paused      - true → show Resume; false → show Pause
 * @returns {object[]}
 */
export function campaignControlButtons({ campaignId = "", paused = false } = {}) {
  const safe_id = String(campaignId).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);

  return [
    actionRow([
      paused
        ? button({ label: "Resume Campaign", custom_id: `campaign:resume:${safe_id}`, style: STYLE.SUCCESS })
        : button({ label: "Pause Campaign",  custom_id: `campaign:pause:${safe_id}`,  style: STYLE.DANGER  }),
      button({ label: "Details", custom_id: `campaign:details:${safe_id}`, style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Approval / deny button pair (new-style approval: prefix).
 *
 * @param {object} opts
 * @param {string} opts.actionId       - opaque token, embedded in custom_id
 * @param {string} [opts.approveLabel]
 * @param {string} [opts.denyLabel]
 * @returns {object[]}
 */
export function approvalButtons({ actionId = "", approveLabel = "Approve", denyLabel = "Deny" } = {}) {
  // Strip non-safe characters from the token.
  const safe_id = String(actionId).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 50);

  return [
    actionRow([
      button({ label: String(approveLabel).slice(0, 60), custom_id: `approval:approve:${safe_id}`, style: STYLE.SUCCESS }),
      button({ label: String(denyLabel).slice(0, 60),    custom_id: `approval:deny:${safe_id}`,    style: STYLE.DANGER  }),
    ]),
  ];
}

// ---------------------------------------------------------------------------
// Targeting Console button builders
// ---------------------------------------------------------------------------

/**
 * Buttons for /target scan output.
 *
 * custom_id prefix: target:
 *
 * @param {object} [opts]
 * @param {string} [opts.campaignKey]  - campaign key to embed in shortcut custom_ids
 * @returns {object[]}
 */
export function targetActionRow({ campaignKey = "" } = {}) {
  const safe_key = String(campaignKey).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return [
    actionRow([
      button({ label: "Create Campaign",  custom_id: `target:create_campaign:${safe_key}`, style: STYLE.PRIMARY   }),
      button({ label: "Run Again",        custom_id: `target:run_again:${safe_key}`,       style: STYLE.SECONDARY }),
      button({ label: "Template Audit",   custom_id: "target:template_audit",             style: STYLE.SECONDARY }),
      button({ label: "Launch Preflight", custom_id: "target:launch_preflight",           style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for /campaign create, inspect, and management output.
 *
 * custom_id prefix: campaign:
 *
 * @param {object}  opts
 * @param {string}  opts.campaignKey
 * @param {boolean} [opts.paused]   - true → show Resume; false → show Pause
 * @returns {object[]}
 */
export function campaignActionRow({ campaignKey = "", paused = false } = {}) {
  const safe_key = String(campaignKey).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return [
    actionRow([
      button({ label: "Scan Campaign", custom_id: `campaign:scan:${safe_key}`,  style: STYLE.PRIMARY   }),
      button({ label: "Preflight",     custom_id: "campaign:preflight",         style: STYLE.SECONDARY }),
      button({ label: "Scale",         custom_id: `campaign:scale:${safe_key}`, style: STYLE.SECONDARY }),
      paused
        ? button({ label: "Resume", custom_id: `campaign:resume:${safe_key}`, style: STYLE.SUCCESS })
        : button({ label: "Pause",  custom_id: `campaign:pause:${safe_key}`,  style: STYLE.DANGER  }),
    ]),
  ];
}

/**
 * Buttons for /territory map output.
 *
 * custom_id prefix: territory:
 *
 * @returns {object[]}
 */
export function territoryActionRow() {
  return [
    actionRow([
      button({ label: "Create Target",  custom_id: "territory:create_target",  style: STYLE.PRIMARY   }),
      button({ label: "Scan Active",    custom_id: "territory:scan_active",    style: STYLE.SECONDARY }),
      button({ label: "Mission Status", custom_id: "territory:mission_status", style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for /email subcommands.
 *
 * custom_id prefix: email:
 *
 * @returns {object[]}
 */
export function emailActionRow() {
  return [
    actionRow([
      button({ label: "Cockpit",     custom_id: "email:cockpit",     style: STYLE.PRIMARY   }),
      button({ label: "Queue",       custom_id: "email:queue",       style: STYLE.SECONDARY }),
      button({ label: "Stats",       custom_id: "email:stats",       style: STYLE.SECONDARY }),
      button({ label: "Suppression", custom_id: "email:suppression", style: STYLE.DANGER    }),
    ]),
  ];
}

/**
 * Buttons for /wires cockpit output.
 *
 * custom_id prefix: wires:
 *
 * @returns {object[]}
 */
export function wireCockpitButtons() {
  return [
    actionRow([
      button({ label: "Refresh",    custom_id: "wires:refresh",    style: STYLE.PRIMARY   }),
      button({ label: "Forecast",   custom_id: "wires:forecast",   style: STYLE.SECONDARY }),
      button({ label: "Reconcile",  custom_id: "wires:reconcile",  style: STYLE.DANGER    }),
      button({ label: "Close",      custom_id: "wires:close",      style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for individual wire event interactions.
 *
 * custom_id prefix: wires:
 *
 * @returns {object[]}
 */
export function wireEventButtons() {
  return [
    actionRow([
      button({ label: "Mark Received", custom_id: "wires:mark_received", style: STYLE.SUCCESS }),
      button({ label: "Mark Cleared",  custom_id: "wires:mark_cleared",  style: STYLE.SUCCESS }),
      button({ label: "View Deal",     custom_id: "wires:view_deal",     style: STYLE.SECONDARY }),
      button({ label: "Close",         custom_id: "wires:close",         style: STYLE.SECONDARY }),
    ]),
  ];
}
