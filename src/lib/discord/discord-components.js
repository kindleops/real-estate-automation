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

function stringSelect({ custom_id, placeholder, options = [], min_values = 1, max_values = 1 }) {
  return {
    type: 1,
    components: [
      {
        type: 3,
        custom_id: String(custom_id).slice(0, 100),
        placeholder: String(placeholder ?? "Choose an option").slice(0, 150),
        min_values,
        max_values,
        options: options.slice(0, 25).map((opt) => ({
          label: String(opt.label ?? opt.name ?? opt.value ?? "Option").slice(0, 100),
          value: String(opt.value ?? opt.label ?? "option").slice(0, 100),
          description: opt.description ? String(opt.description).slice(0, 100) : undefined,
          emoji: opt.emoji,
        })),
      },
    ],
  };
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
 * Buttons for /target scan output. v2 — cinematic targeting console.
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
      button({ label: "Launch Dry Run",   custom_id: `target:launch_dry_run:${safe_key}`,  style: STYLE.SECONDARY }),
      button({ label: "Refine Filters",   custom_id: `target:refine_filters:${safe_key}`,  style: STYLE.SECONDARY }),
      button({ label: "View Territory",   custom_id: "target:view_territory",              style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for /campaign create, inspect, and management output. v2.
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
      button({ label: "Approve Launch", custom_id: `campaign:approve_launch:${safe_key}`, style: STYLE.SUCCESS   }),
      button({ label: "Scale",          custom_id: `campaign:scale:${safe_key}`,          style: STYLE.PRIMARY   }),
      paused
        ? button({ label: "Resume", custom_id: `campaign:resume:${safe_key}`, style: STYLE.SUCCESS })
        : button({ label: "Pause",  custom_id: `campaign:pause:${safe_key}`,  style: STYLE.DANGER  }),
      button({ label: "Close",          custom_id: `campaign:close:${safe_key}`,          style: STYLE.SECONDARY }),
    ]),
  ];
}

/**
 * Buttons for /territory map output. v2.
 *
 * custom_id prefix: territory:
 *
 * @returns {object[]}
 */
export function territoryActionRow() {
  return [
    actionRow([
      button({ label: "Create Campaign",  custom_id: "target:create_campaign",   style: STYLE.PRIMARY   }),
      button({ label: "Launch Dry Run",   custom_id: "target:launch_dry_run",    style: STYLE.SECONDARY }),
      button({ label: "Mission Status",   custom_id: "territory:mission_status", style: STYLE.SECONDARY }),
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

// ---------------------------------------------------------------------------
// briefingActionRow — Daily Empire Briefing quick-actions
// ---------------------------------------------------------------------------

/**
 * Action buttons for the Daily Empire Briefing embed.
 *
 * custom_id prefix: briefing:
 *
 * @returns {object[]}
 */
export function briefingActionRow() {
  return [
    actionRow([
      button({ label: "🔄 Refresh",        custom_id: "briefing:refresh",        style: STYLE.PRIMARY   }),
      button({ label: "🔥 Hot Leads",       custom_id: "briefing:hot_leads",      style: STYLE.SUCCESS   }),
      button({ label: "📈 Scale Campaign",  custom_id: "briefing:scale_campaign", style: STYLE.SECONDARY }),
      button({ label: "📬 Queue Scan",      custom_id: "briefing:queue_scan",     style: STYLE.SECONDARY }),
      button({ label: "📤 Export",          custom_id: "briefing:export",         style: STYLE.SECONDARY }),
    ]),
  ];
}

// ---------------------------------------------------------------------------
// Proactive ops approval actions
// ---------------------------------------------------------------------------

/**
 * Ops approval action row for proactive campaign scale/pause notifications.
 *
 * custom_id format:
 *   approval:campaign_scale:<requestKey>   — approve a scale (Owner / SMS Ops)
 *   approval:campaign_pause:<requestKey>   — approve a pause (Owner / SMS Ops)
 *   approval:hold:<requestKey>             — hold without action (any team)
 *   approval:inspect:<requestKey>          — inspect campaign (any team)
 *
 * Note: custom_ids are truncated to 100 chars per Discord spec.
 *
 * @param {object} opts
 * @param {string} opts.requestKey - Dedup key for the approval request
 * @param {"scale"|"pause"} [opts.type="scale"] - Alert type
 * @returns {object[]}  Array of action rows
 */
export function opsApprovalActionRow({ requestKey = "", type = "scale" } = {}) {
  const safe_key = String(requestKey).replace(/[^a-zA-Z0-9_:-]/g, "").slice(0, 60);

  const action_custom_id = type === "pause"
    ? `approval:campaign_pause:${safe_key}`
    : `approval:campaign_scale:${safe_key}`;

  const approve_label = type === "pause" ? "⏸ Approve Pause" : "✅ Approve Scale";

  return [
    actionRow([
      button({ label: approve_label,   custom_id: action_custom_id,              style: STYLE.SUCCESS   }),
      button({ label: "⏸ Hold",        custom_id: `approval:hold:${safe_key}`,   style: STYLE.SECONDARY }),
      button({ label: "🔍 Inspect",    custom_id: `approval:inspect:${safe_key}`, style: STYLE.SECONDARY }),
    ]),
  ];
}

// ---------------------------------------------------------------------------
// Target Builder v1 components
// ---------------------------------------------------------------------------

export function targetBuilderMainActionRow(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return actionRow([
    button({ label: "🌎 Market",   custom_id: `target_builder:open_market:${sk}`,   style: STYLE.PRIMARY }),
    button({ label: "🏠 Asset",    custom_id: `target_builder:open_asset:${sk}`,    style: STYLE.SECONDARY }),
    button({ label: "⚡ Strategy", custom_id: `target_builder:open_strategy:${sk}`, style: STYLE.SECONDARY }),
    button({ label: "🏷️ Tags",     custom_id: `target_builder:open_tags:${sk}`,     style: STYLE.SECONDARY }),
    button({ label: "⚙️ Filters",  custom_id: `target_builder:open_filters:${sk}`,  style: STYLE.SECONDARY }),
  ]);
}

export function targetBuilderRunActionRow(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return actionRow([
    button({ label: "🧪 Run Scan",        custom_id: `target_builder:run_scan:${sk}`,        style: STYLE.PRIMARY }),
    button({ label: "🚀 Create Campaign", custom_id: `target_builder:create_campaign:${sk}`, style: STYLE.SUCCESS }),
    button({ label: "🔄 Reset",           custom_id: `target_builder:reset:${sk}`,           style: STYLE.SECONDARY }),
    button({ label: "❌ Close",            custom_id: `target_builder:close:${sk}`,           style: STYLE.DANGER }),
  ]);
}

export function marketRegionSelect(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return stringSelect({
    custom_id: `target_builder:region:${sk}`,
    placeholder: "Choose a market region",
    options: [
      { label: "Texas",           value: "texas" },
      { label: "Florida",         value: "florida" },
      { label: "California",      value: "california" },
      { label: "Southeast",       value: "southeast" },
      { label: "Midwest",         value: "midwest" },
      { label: "Northeast",       value: "northeast" },
      { label: "West / Mountain", value: "west_mountain" },
      { label: "Other",           value: "other" },
    ],
  });
}

export function marketSelect(session_key = "", region = "other", markets = []) {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  const list = Array.isArray(markets) ? markets : [];
  return stringSelect({
    custom_id: `target_builder:market:${sk}`,
    placeholder: `Choose market (${String(region).replace(/_/g, " ")})`,
    options: list.map((m) => ({ label: m, value: m })),
  });
}

export function assetClassSelect(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return stringSelect({
    custom_id: `target_builder:asset:${sk}`,
    placeholder: "Choose asset class",
    options: [
      { label: "🏠 Single Family",           value: "sfr" },
      { label: "🏘️ Multifamily",             value: "multifamily" },
      { label: "🏚️ Duplex / Small MF",       value: "duplex" },
      { label: "🌾 Land",                    value: "vacant_land" },
      { label: "🏢 Commercial",              value: "commercial" },
      { label: "🏨 Hotel / Motel",           value: "hotel_motel" },
      { label: "📦 Self Storage",            value: "self_storage" },
      { label: "🔥 Distressed Residential",  value: "distressed_residential" },
    ],
  });
}

export function strategySelect(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return stringSelect({
    custom_id: `target_builder:strategy:${sk}`,
    placeholder: "Choose strategy",
    options: [
      { label: "💰 High Equity",             value: "high_equity" },
      { label: "🏚️ Distress Stack",         value: "distress_stack" },
      { label: "🧓 Tired Landlord",          value: "tired_landlord" },
      { label: "🏛️ Probate",                value: "probate" },
      { label: "⚠️ Pre-Foreclosure",         value: "pre_foreclosure" },
      { label: "🏚️ Vacant",                 value: "vacant" },
      { label: "🧾 Tax Delinquent",          value: "tax_delinquent" },
      { label: "🔓 Free And Clear",          value: "free_and_clear" },
      { label: "🎯 Acquisition Score",       value: "acquisition_score" },
      { label: "🇪🇸 Spanish Seller",         value: "spanish_seller" },
      { label: "🤝 Creative Finance",        value: "creative" },
      { label: "🏢 Multifamily Underwrite",  value: "multifamily_underwrite" },
    ],
  });
}

export function propertyTagMultiSelect(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return stringSelect({
    custom_id: `target_builder:tags:${sk}`,
    placeholder: "Choose up to 3 property tags",
    min_values: 0,
    max_values: 3,
    options: [
      { label: "Absentee Owner",      value: "absentee_owner" },
      { label: "High Equity",         value: "high_equity" },
      { label: "Tired Landlord",      value: "tired_landlord" },
      { label: "Vacant Home",         value: "vacant_home" },
      { label: "Tax Delinquent",      value: "tax_delinquent" },
      { label: "Probate",             value: "probate" },
      { label: "Active Lien",         value: "active_lien" },
      { label: "Preforeclosure",      value: "pre_foreclosure" },
      { label: "Free And Clear",      value: "free_and_clear" },
      { label: "Senior Owner",        value: "senior_owner" },
      { label: "Out Of State Owner",  value: "out_of_state_owner" },
      { label: "Long Term Owner",     value: "long_term_owner" },
      { label: "Major Repairs Needed",value: "major_repairs_needed" },
      { label: "Moderate Repairs",    value: "moderate_repairs" },
      { label: "Corporate Owner",     value: "corporate_owner" },
      { label: "Likely To Move",      value: "likely_to_move" },
    ],
  });
}

export function propertyFilterCategorySelect(session_key = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  return stringSelect({
    custom_id: `target_builder:filter_category:${sk}`,
    placeholder: "Choose property filter category",
    options: [
      { label: "📐 Size / Units",        value: "size_units" },
      { label: "💎 Value / Equity",       value: "value_equity" },
      { label: "🛠️ Condition / Repairs",  value: "condition_repairs" },
      { label: "⏳ Ownership / Purchase", value: "ownership_purchase" },
      { label: "🎯 Score",               value: "score" },
    ],
  });
}

export function propertyFilterValueSelect(session_key = "", category = "") {
  const sk = String(session_key).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40);
  const catalog = {
    size_units: [
      { label: "Sq Ft 0-1000", value: "sq_ft_range:0_1000" },
      { label: "Sq Ft 1000-1500", value: "sq_ft_range:1000_1500" },
      { label: "Sq Ft 1500+", value: "sq_ft_range:1500_plus" },
      { label: "Units 1", value: "units_range:1" },
      { label: "Units 2", value: "units_range:2" },
      { label: "Units 3-4", value: "units_range:3_4" },
      { label: "Units 5+", value: "units_range:5_plus" },
    ],
    value_equity: [
      { label: "Value $0-$200k", value: "estimated_value_range:0_200k" },
      { label: "Value $200k-$500k", value: "estimated_value_range:200k_500k" },
      { label: "Value $500k+", value: "estimated_value_range:500k_plus" },
      { label: "Equity 0-50%", value: "equity_percent_range:0_50" },
      { label: "Equity 50-90%", value: "equity_percent_range:50_90" },
      { label: "Equity 90-100%", value: "equity_percent_range:90_100" },
    ],
    condition_repairs: [
      { label: "Repairs $0-$25k", value: "repair_cost_range:0_25k" },
      { label: "Repairs $25k-$50k", value: "repair_cost_range:25k_50k" },
      { label: "Repairs $50k+", value: "repair_cost_range:50k_plus" },
      { label: "Condition Good", value: "building_condition:Good" },
      { label: "Condition Fair", value: "building_condition:Fair" },
      { label: "Condition Poor", value: "building_condition:Poor" },
    ],
    ownership_purchase: [
      { label: "Ownership 0-5y", value: "ownership_years_range:0_5" },
      { label: "Ownership 5-15y", value: "ownership_years_range:5_15" },
      { label: "Ownership 15y+", value: "ownership_years_range:15_plus" },
      { label: "Offer < Loan", value: "offer_vs_loan:offer_less_loan" },
      { label: "Offer > Loan", value: "offer_vs_loan:offer_greater_loan" },
      { label: "Offer ≈ Loan", value: "offer_vs_loan:offer_equal_loan" },
      { label: "Year Built pre-1940", value: "year_built_range:pre_1940" },
      { label: "Year Built 1940-1980", value: "year_built_range:1940_1980" },
      { label: "Year Built 1980+", value: "year_built_range:1980_plus" },
    ],
    score: [
      { label: "Score 40+", value: "min_property_score:40" },
      { label: "Score 50+", value: "min_property_score:50" },
      { label: "Score 60+", value: "min_property_score:60" },
      { label: "Score 70+", value: "min_property_score:70" },
      { label: "Score 80+", value: "min_property_score:80" },
    ],
  };

  return stringSelect({
    custom_id: `target_builder:filter:${sk}`,
    placeholder: "Choose a filter preset",
    options: catalog[String(category)] ?? catalog.score,
  });
}
