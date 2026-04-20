/**
 * register-discord-commands.mjs
 *
 * Registers (upserts) all guild slash commands for the real-estate-automation
 * Discord bot via PUT /applications/{id}/guilds/{guild_id}/commands.
 *
 * PUT replaces the full guild command set atomically — safe to run any time
 * commands are added or changed.  Deleted entries from the array will be
 * removed from Discord automatically.
 *
 * Usage:
 *   DISCORD_APPLICATION_ID=... DISCORD_GUILD_ID=... DISCORD_BOT_TOKEN=... \
 *     node scripts/register-discord-commands.mjs
 *
 * Or via npm:
 *   npm run discord:register
 *
 * Note: DISCORD_BOT_TOKEN is read from env and never logged.
 */

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const APPLICATION_ID = String(process.env.DISCORD_APPLICATION_ID ?? "").trim();
const GUILD_ID       = String(process.env.DISCORD_GUILD_ID       ?? "").trim();
const BOT_TOKEN      = String(process.env.DISCORD_BOT_TOKEN      ?? "").trim();

if (!APPLICATION_ID) {
  console.error("Error: DISCORD_APPLICATION_ID is not set.");
  process.exit(1);
}
if (!GUILD_ID) {
  console.error("Error: DISCORD_GUILD_ID is not set.");
  process.exit(1);
}
if (!BOT_TOKEN) {
  console.error("Error: DISCORD_BOT_TOKEN is not set.");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Option type constants (Discord ApplicationCommandOptionType)
// ---------------------------------------------------------------------------

const OPT = {
  SUB_COMMAND: 1,
  STRING:      3,
  INTEGER:     4,
  BOOLEAN:     5,
};

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

const COMMANDS = [
  // ── /queue ─────────────────────────────────────────────────────────────
  {
    name:        "queue",
    description: "Send queue operations",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "status",
        description: "Show send queue row counts grouped by status",
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "run",
        description: "Process the send queue (Tech Ops or Owner)",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Maximum messages to process (1–50)",
            required:    false,
            min_value:   1,
            max_value:   50,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "cockpit",
        description: "Rich queue cockpit — status counts, due now, stuck rows",
      },
    ],
  },

  // ── /sync ──────────────────────────────────────────────────────────────
  {
    name:        "sync",
    description: "Data synchronisation operations",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "podio",
        description: "Sync un-synced message events to Podio (Tech Ops or Owner)",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Maximum rows to sync in this batch (1–100)",
            required:    false,
            min_value:   1,
            max_value:   100,
          },
        ],
      },
    ],
  },

  // ── /diagnostic ────────────────────────────────────────────────────────
  {
    name:        "diagnostic",
    description: "System diagnostics (Tech Ops or Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "inbound",
        description: "Run the inbound SMS diagnostic query",
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "podio-sync",
        description: "Run the Podio sync eligibility diagnostic",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Rows to inspect (1–100)",
            required:    false,
            min_value:   1,
            max_value:   100,
          },
        ],
      },
    ],
  },

  // ── /lock ──────────────────────────────────────────────────────────────
  {
    name:        "lock",
    description: "Run-lock management (Tech Ops or Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "release",
        description: "Force-release a stale feeder or run lock",
        options: [
          {
            type:        OPT.STRING,
            name:        "scope",
            description: "Lock scope to release (e.g. feeder)",
            required:    true,
          },
        ],
      },
    ],
  },

  // ── /feeder ────────────────────────────────────────────────────────────
  {
    name:        "feeder",
    description: "Outbound feeder operations (Tech Ops or Owner; >25 needs Owner approval)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "run",
        description: "Run the master-owner outbound feeder",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Max owners to enqueue (default 10; >25 requires Owner approval)",
            required:    false,
            min_value:   1,
            max_value:   200,
          },
          {
            type:        OPT.INTEGER,
            name:        "scan_limit",
            description: "Max Podio owners to scan (default 500)",
            required:    false,
            min_value:   1,
          },
          {
            type:        OPT.BOOLEAN,
            name:        "dry_run",
            description: "If true, simulate without enqueueing (default false)",
            required:    false,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "scan",
        description: "Dry-run scan — show eligible owners without enqueueing (deferred, Tech Ops+)",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Max owners to evaluate (default 50)",
            required:    false,
            min_value:   1,
            max_value:   200,
          },
          {
            type:        OPT.INTEGER,
            name:        "scan_limit",
            description: "Max Podio owners to scan (default 500)",
            required:    false,
            min_value:   1,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "launch",
        description: "Live feeder launch — enqueue owners (>25 requires Owner approval)",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Max owners to enqueue (default 10; >25 requires Owner approval)",
            required:    false,
            min_value:   1,
            max_value:   200,
          },
          {
            type:        OPT.INTEGER,
            name:        "scan_limit",
            description: "Max Podio owners to scan (default 500)",
            required:    false,
            min_value:   1,
          },
        ],
      },
    ],
  },

  // ── /campaign ──────────────────────────────────────────────────────────
  {
    name:        "campaign",
    description: "Campaign management (SMS Ops or Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "pause",
        description: "Pause a campaign (SMS Ops or Owner)",
        options: [
          {
            type:        OPT.STRING,
            name:        "campaign_id",
            description: "ID of the campaign to pause",
            required:    true,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "resume",
        description: "Resume a paused campaign (requires Owner approval)",
        options: [
          {
            type:        OPT.STRING,
            name:        "campaign_id",
            description: "ID of the campaign to resume",
            required:    true,
          },
        ],
      },
    ],
  },

  // ── /lead ──────────────────────────────────────────────────────────────
  {
    name:        "lead",
    description: "Lead information (Acquisitions or Owner, read-only)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "summarize",
        description: "Summarise message event history for a lead",
        options: [
          {
            type:        OPT.STRING,
            name:        "phone_or_owner_id",
            description: "Phone number (E.164) or numeric master_owner_id",
            required:    true,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "inspect",
        description: "Deep-inspect a lead's message event history (ephemeral)",
        options: [
          {
            type:        OPT.STRING,
            name:        "phone_or_owner_id",
            description: "Phone number (E.164) or numeric master_owner_id",
            required:    true,
          },
        ],
      },
    ],
  },

  // ── /mission ───────────────────────────────────────────────────────────
  {
    name:        "mission",
    description: "Operations command center",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "status",
        description: "Show full mission health — queue, templates, integrations",
      },
    ],
  },

  // ── /launch ────────────────────────────────────────────────────────────
  {
    name:        "launch",
    description: "Launch readiness checks",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "preflight",
        description: "Run read-only preflight checks — GO / WARN / HOLD",
      },
    ],
  },

  // ── /templates ─────────────────────────────────────────────────────────
  {
    name:        "templates",
    description: "SMS template inspection (Tech Ops or Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "audit",
        description: "Full audit of sms_templates — counts, blockers, missing fields",
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "stage1",
        description: "Show active Stage 1 / first-touch ownership templates",
      },
    ],
  },

  // ── /hotleads ──────────────────────────────────────────────────────────
  {
    name:        "hotleads",
    description: "Show recent inbound SMS lead responses",
    options: [
      {
        type:        OPT.INTEGER,
        name:        "limit",
        description: "Max leads to show (1–25, default 10)",
        required:    false,
        min_value:   1,
        max_value:   25,
      },
    ],
  },

  // ── /alerts ────────────────────────────────────────────────────────────
  {
    name:        "alerts",
    description: "Alert mode configuration (Tech Ops or Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "mode",
        description: "Get or set the active alert mode",
        options: [
          {
            type:        OPT.STRING,
            name:        "value",
            description: "New mode value (e.g. verbose, silent, normal) — omit to read current",
            required:    false,
          },
        ],
      },
    ],
  },
];

// ---------------------------------------------------------------------------
// Targeting Console command definitions (Targeting Console v1)
// ---------------------------------------------------------------------------

const ASSET_CHOICES = [
  { name: "SFR",             value: "sfr"             },
  { name: "Multifamily",     value: "multifamily"     },
  { name: "Duplex",          value: "duplex"          },
  { name: "Vacant",          value: "vacant"          },
  { name: "Absentee",        value: "absentee"        },
  { name: "Probate",         value: "probate"         },
  { name: "Tax Delinquent",  value: "tax_delinquent"  },
  { name: "Corporate Owner", value: "corporate_owner" },
  { name: "High Equity",     value: "high_equity"     },
  { name: "Free & Clear",    value: "free_and_clear"  },
];

const STRATEGY_CHOICES = [
  { name: "Cash",                   value: "cash"                   },
  { name: "Creative",               value: "creative"               },
  { name: "Multifamily Underwrite", value: "multifamily_underwrite" },
];

const TARGETING_COMMANDS = [
  // ── /target ────────────────────────────────────────────────────────────
  {
    name:        "target",
    description: "Market targeting operations (SMS Ops, Tech Ops, or Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "scan",
        description: "Dry-run scan a target territory — no SMS sent",
        options: [
          {
            type:        OPT.STRING,
            name:        "market",
            description: "Market to scan (e.g. Los Angeles, Miami)",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "asset",
            description: "Asset type",
            required:    true,
            choices:     ASSET_CHOICES,
          },
          {
            type:        OPT.STRING,
            name:        "strategy",
            description: "Acquisition strategy",
            required:    true,
            choices:     STRATEGY_CHOICES,
          },
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Max owners to evaluate (default 25, max 500)",
            required:    false,
            min_value:   1,
            max_value:   500,
          },
          {
            type:        OPT.INTEGER,
            name:        "scan_limit",
            description: "Max Podio owners to scan (default 100, max 5000)",
            required:    false,
            min_value:   1,
            max_value:   5000,
          },
          {
            type:        OPT.STRING,
            name:        "source_view_name",
            description: "Podio view name override (auto-derived if omitted)",
            required:    false,
          },
        ],
      },
    ],
  },

  // ── /territory ─────────────────────────────────────────────────────────
  {
    name:        "territory",
    description: "Territory map and campaign overview",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "map",
        description: "Show all territories grouped by market and status",
      },
    ],
  },

  // ── /conquest ──────────────────────────────────────────────────────────
  {
    name:        "conquest",
    description: "Empire-level campaign overview — active, draft, paused, and recommended next move",
  },

  // ── /email ─────────────────────────────────────────────────────────────
  {
    name:        "email",
    description: "Email cockpit — preview, send-test, queue, suppression, and stats (Tech Ops / Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "cockpit",
        description: "Full email layer dashboard — queue status, event counts, templates, suppression",
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "preview",
        description: "Preview a rendered email template without sending",
        options: [
          {
            type:        OPT.STRING,
            name:        "template_key",
            description: "Template key to render (e.g. seller_intro)",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "owner_id",
            description: "Owner ID for context variable substitution (optional)",
            required:    false,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "send-test",
        description: "Send a live test email via Brevo (allowlisted addresses only)",
        options: [
          {
            type:        OPT.STRING,
            name:        "email_address",
            description: "Recipient email address (must be on EMAIL_TEST_ALLOWLIST)",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "template_key",
            description: "Template key to send",
            required:    true,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "queue",
        description: "Run the email send queue (dry_run=true by default)",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "limit",
            description: "Max rows to process (default 20, max 200)",
            required:    false,
          },
          {
            type:        OPT.BOOLEAN,
            name:        "dry_run",
            description: "If true, simulate without sending (default: true)",
            required:    false,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "suppression",
        description: "Show suppressed email addresses (hard-bounce / spam / unsubscribe)",
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "stats",
        description: "Email event statistics — delivered, opened, clicked, bounced",
      },
    ],
  },
];

// Extend /campaign with create, inspect, scale (preserve pause/resume).
const campaign_cmd = COMMANDS.find((c) => c.name === "campaign");
if (campaign_cmd) {
  campaign_cmd.options.push(
    {
      type:        OPT.SUB_COMMAND,
      name:        "create",
      description: "Create or update a campaign target (SMS Ops, Tech Ops, or Owner)",
      options: [
        {
          type:        OPT.STRING,
          name:        "name",
          description: "Human-readable campaign name",
          required:    true,
        },
        {
          type:        OPT.STRING,
          name:        "market",
          description: "Market (e.g. Los Angeles, Miami)",
          required:    true,
        },
        {
          type:        OPT.STRING,
          name:        "asset",
          description: "Asset type",
          required:    true,
          choices:     ASSET_CHOICES,
        },
        {
          type:        OPT.STRING,
          name:        "strategy",
          description: "Acquisition strategy",
          required:    true,
          choices:     STRATEGY_CHOICES,
        },
        {
          type:        OPT.INTEGER,
          name:        "daily_cap",
          description: "Max messages per day (default 50)",
          required:    false,
          min_value:   1,
          max_value:   10000,
        },
        {
          type:        OPT.STRING,
          name:        "source_view_name",
          description: "Podio view name override (auto-derived if omitted)",
          required:    false,
        },
        {
          type:        OPT.STRING,
          name:        "language",
          description: "Message language (default auto)",
          required:    false,
          choices: [
            { name: "Auto",    value: "auto"    },
            { name: "English", value: "English" },
            { name: "Spanish", value: "Spanish" },
          ],
        },
      ],
    },
    {
      type:        OPT.SUB_COMMAND,
      name:        "inspect",
      description: "Inspect a campaign target — status, cap, last scan, last launch",
      options: [
        {
          type:        OPT.STRING,
          name:        "campaign",
          description: "Campaign key (e.g. los_angeles_sfr_cash)",
          required:    true,
        },
      ],
    },
    {
      type:        OPT.SUB_COMMAND,
      name:        "scale",
      description: "Update the daily cap for a campaign (>100 requires Owner/Tech Ops approval for SMS Ops)",
      options: [
        {
          type:        OPT.STRING,
          name:        "campaign",
          description: "Campaign key (e.g. los_angeles_sfr_cash)",
          required:    true,
        },
        {
          type:        OPT.INTEGER,
          name:        "daily_cap",
          description: "New daily message cap",
          required:    true,
          min_value:   1,
          max_value:   10000,
        },
      ],
    }
  );
}

// ───────────────────────────────────────────────────────────────────────────
// /replay — Inbound conversation and template replay/simulation (testing)
// ───────────────────────────────────────────────────────────────────────────

const REPLAY_COMMANDS = [
  {
    name:        "replay",
    description: "Simulate inbound seller replies and test routing/template alignment (SMS Ops / Tech Ops / Owner)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "inbound",
        description: "Simulate arbitrary seller inbound reply — classify, route, and show template selection",
        options: [
          {
            type:        OPT.STRING,
            name:        "text",
            description: "Seller message text to simulate",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "language",
            description: "Seller language preference (English, Spanish)",
            required:    false,
            choices: [
              { name: "English", value: "English" },
              { name: "Spanish", value: "Spanish" },
            ],
          },
          {
            type:        OPT.STRING,
            name:        "stage",
            description: "Current seller stage (e.g. ownership_check, offer_reveal_cash)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "property_type",
            description: "Property type (e.g. Single Family, Multifamily)",
            required:    false,
            choices: [
              { name: "Single Family",       value: "Single Family" },
              { name: "Multifamily (2-4)",   value: "Multifamily (2-4)" },
              { name: "Multifamily (5+)",    value: "Multifamily (5+)" },
              { name: "Commercial",          value: "Commercial" },
            ],
          },
          {
            type:        OPT.STRING,
            name:        "deal_strategy",
            description: "Deal strategy (cash, creative, etc)",
            required:    false,
            choices: [
              { name: "Cash",                  value: "cash" },
              { name: "Creative Financing",    value: "creative" },
              { name: "Wholesale",             value: "wholesale" },
            ],
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "owner",
        description: "Simulate inbound reply for a real owner — shows real property, offer, routing context",
        options: [
          {
            type:        OPT.STRING,
            name:        "owner_id",
            description: "Master owner ID (numeric or string)",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "text",
            description: "Seller message text to simulate for this owner",
            required:    true,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "template",
        description: "Resolve and preview template for a specific use case",
        options: [
          {
            type:        OPT.STRING,
            name:        "use_case",
            description: "Template use case (e.g. offer_reveal_cash, ownership_confirmation)",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "language",
            description: "Language (default English)",
            required:    false,
            choices: [
              { name: "English", value: "English" },
              { name: "Spanish", value: "Spanish" },
            ],
          },
          {
            type:        OPT.STRING,
            name:        "property_type",
            description: "Property type scope (default Residential)",
            required:    false,
            choices: [
              { name: "Single Family",       value: "Single Family" },
              { name: "Multifamily (2-4)",   value: "Multifamily (2-4)" },
              { name: "Multifamily (5+)",    value: "Multifamily (5+)" },
            ],
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "batch",
        description: "Run multiple preset replay scenarios and show pass/fail summary",
        options: [
          {
            type:        OPT.STRING,
            name:        "scenario",
            description: "Predefined scenario batch (e.g. ownership, offer_requests, all)",
            required:    true,
            choices: [
              { name: "Ownership Checks",      value: "ownership" },
              { name: "Offer Requests",       value: "offer_requests" },
              { name: "Objections & Concerns", value: "objections" },
              { name: "Underwriting Replies",  value: "underwriting" },
              { name: "Compliance Edge Cases", value: "compliance" },
              { name: "All Scenarios",         value: "all" },
            ],
          },
        ],
      },
    ],
  },
];

// ── /wires ─────────────────────────────────────────────────────────────

const WIRES_COMMANDS = [
  {
    name:        "wires",
    description: "Wire/closing command center — track expected, received, cleared wires (Owner / Closings / Tech Ops)",
    options: [
      {
        type:        OPT.SUB_COMMAND,
        name:        "cockpit",
        description: "Show wire command center summary — expected, pending, received, cleared",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "days",
            description: "Look back N days (default 7)",
            required:    false,
            min_value:   1,
            max_value:   90,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "expected",
        description: "Create an expected wire event (Owner / Closings)",
        options: [
          {
            type:        OPT.STRING,
            name:        "amount",
            description: "Wire amount (e.g. 50000)",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "account",
            description: "Account key or display name",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "deal_key",
            description: "Deal identifier (optional)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "property_id",
            description: "Podio property item ID (optional)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "closing_id",
            description: "Podio closing item ID (optional)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "expected_at",
            description: "Expected arrival date ISO format (optional)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "note",
            description: "Additional notes",
            required:    false,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "received",
        description: "Mark wire as received (Owner only)",
        options: [
          {
            type:        OPT.STRING,
            name:        "wire_key",
            description: "Wire identifier",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "note",
            description: "Received note",
            required:    false,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "cleared",
        description: "Mark wire as cleared (Owner only)",
        options: [
          {
            type:        OPT.STRING,
            name:        "wire_key",
            description: "Wire identifier",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "note",
            description: "Clearance note",
            required:    false,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "forecast",
        description: "Show wire forecast — expected wires over next N days",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "days",
            description: "Forecast horizon (default 14)",
            required:    false,
            min_value:   1,
            max_value:   90,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "deal",
        description: "Show all wires linked to a deal / property / closing",
        options: [
          {
            type:        OPT.STRING,
            name:        "deal_key",
            description: "Deal identifier or property/closing lookup",
            required:    true,
          },
        ],
      },
      {
        type:        OPT.SUB_COMMAND,
        name:        "reconcile",
        description: "Show wire anomalies — missing account links, stale pending, mismatches",
        options: [
          {
            type:        OPT.INTEGER,
            name:        "days",
            description: "Scope (default 30)",
            required:    false,
            min_value:   1,
            max_value:   365,
          },
        ],
      },
    ],
  },
];

// Final command set = existing + targeting console additions + replay additions + wires additions.
const ALL_COMMANDS = [...COMMANDS, ...TARGETING_COMMANDS, ...REPLAY_COMMANDS, ...WIRES_COMMANDS];

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

const url = `https://discord.com/api/v10/applications/${APPLICATION_ID}/guilds/${GUILD_ID}/commands`;

console.log(`Registering ${ALL_COMMANDS.length} commands for guild ${GUILD_ID} …`);

let response;
try {
  response = await fetch(url, {
    method:  "PUT",
    headers: {
      "Authorization": `Bot ${BOT_TOKEN}`,
      "Content-Type":  "application/json",
    },
    body: JSON.stringify(ALL_COMMANDS),
  });
} catch (err) {
  console.error("Network error calling Discord API:", err.message);
  process.exit(1);
}

if (!response.ok) {
  const body = await response.text().catch(() => "(unreadable body)");
  console.error(`Discord API returned HTTP ${response.status}: ${body}`);
  process.exit(1);
}

const registered = await response.json();

console.log("\nRegistered commands:");
for (const cmd of registered) {
  console.log(`  /${cmd.name}  (id: ${cmd.id})`);
}
console.log(`\n✅ Done — ${registered.length} command(s) registered.`);
