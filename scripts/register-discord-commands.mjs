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

import {
  validateCommandOptionCounts,
  validateCommandPayloadSizes,
} from "../src/lib/discord/command-registration-validation.js";

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

const MARKET_CHOICES = [
  { name: "Los Angeles",         value: "los_angeles"       },
  { name: "Miami",               value: "miami"             },
  { name: "Dallas / Fort Worth", value: "dallas_fort_worth" },
  { name: "Houston",             value: "houston"           },
  { name: "Jacksonville",        value: "jacksonville"      },
  { name: "New Orleans",         value: "new_orleans"       },
  { name: "Atlanta",             value: "atlanta"           },
  { name: "Tampa",               value: "tampa"             },
  { name: "Orlando",             value: "orlando"           },
  { name: "Phoenix",             value: "phoenix"           },
  { name: "Las Vegas",           value: "las_vegas"         },
  { name: "Cleveland",           value: "cleveland"         },
  { name: "Detroit",             value: "detroit"           },
  { name: "Memphis",             value: "memphis"           },
  { name: "Birmingham",          value: "birmingham"        },
  { name: "Indianapolis",        value: "indianapolis"      },
  { name: "Charlotte",           value: "charlotte"         },
  { name: "San Antonio",         value: "san_antonio"       },
  { name: "Austin",              value: "austin"            },
  { name: "Chicago",             value: "chicago"           },
  { name: "St. Louis",           value: "st_louis"          },
  { name: "Kansas City",         value: "kansas_city"       },
  { name: "Minneapolis",         value: "minneapolis"       },
  { name: "Nashville",           value: "nashville"         },
  { name: "Philadelphia",        value: "philadelphia"      },
];

const ASSET_CHOICES = [
  { name: "\u{1F3E0} SFR / Single Family",    value: "sfr"                    },
  { name: "\u{1F3E2} Multifamily",             value: "multifamily"            },
  { name: "\u{1F3D8}\uFE0F Duplex",            value: "duplex"                 },
  { name: "\u{1F33E} Vacant Land",             value: "vacant_land"            },
  { name: "\u{1F3DA}\uFE0F Distressed Residential", value: "distressed_residential" },
  { name: "\u{1F3EC} Commercial",              value: "commercial"             },
  { name: "\u{1F3E8} Hotel / Motel",           value: "hotel_motel"            },
  { name: "\u{1F4E6} Self Storage",            value: "self_storage"           },
];

const STRATEGY_CHOICES = [
  { name: "\u{1F4B5} Cash Offer",              value: "cash"                   },
  { name: "\u{1F9E0} Creative Finance",        value: "creative"               },
  { name: "\u{1F3E2} Multifamily Underwrite",  value: "multifamily_underwrite" },
  { name: "\u{1F3DA}\uFE0F Distress Stack",    value: "distress_stack"         },
  { name: "\u{1F9FE} Probate / Inherited",     value: "probate"                },
  { name: "\u{1F3D8}\uFE0F Tired Landlord",    value: "tired_landlord"         },
  { name: "\u{1F3E6} Pre-Foreclosure",         value: "pre_foreclosure"        },
  { name: "\u{1F3AF} High Equity",             value: "high_equity"            },
];

const PROPERTY_TAG_CHOICES = [
  { name: "Absentee Owner",       value: "absentee_owner"      },
  { name: "Out of State Owner",   value: "out_of_state_owner"  },
  { name: "Vacant",               value: "vacant"              },
  { name: "High Equity",          value: "high_equity"         },
  { name: "Free and Clear",       value: "free_and_clear"      },
  { name: "Tax Delinquent",       value: "tax_delinquent"      },
  { name: "Pre-Foreclosure",      value: "pre_foreclosure"     },
  { name: "Probate / Inherited",  value: "probate"             },
  { name: "Tired Landlord",       value: "tired_landlord"      },
  { name: "Senior Owner",         value: "senior_owner"        },
  { name: "Empty Nester",         value: "empty_nester"        },
  { name: "Corporate Owner",      value: "corporate_owner"     },
  { name: "Low Equity",           value: "low_equity"          },
  { name: "Active Lien",          value: "active_lien"         },
  { name: "Likely To Move",       value: "likely_to_move"      },
  { name: "Distressed Property",  value: "distressed_property" },
  { name: "Unknown Equity",       value: "unknown_equity"      },
];

const OWNER_TYPE_CHOICES = [
  { name: "Individual", value: "individual" },
  { name: "Corporate",  value: "corporate"  },
  { name: "Trust",      value: "trust"      },
  { name: "LLC",        value: "llc"        },
  { name: "Investor",   value: "investor"   },
  { name: "Unknown",    value: "unknown"    },
];

const PHONE_STATUS_CHOICES = [
  { name: "Active 12+ Months", value: "active_12_plus"   },
  { name: "Active",            value: "active"           },
  { name: "Unknown",           value: "unknown"          },
  { name: "Exclude Inactive",  value: "exclude_inactive" },
];

const LANGUAGE_CHOICES = [
  { name: "Auto",    value: "auto"    },
  { name: "English", value: "english" },
  { name: "Spanish", value: "spanish" },
];

const PRIORITY_TIER_CHOICES = [
  { name: "Tier 1", value: "tier_1" },
  { name: "Tier 2", value: "tier_2" },
  { name: "Tier 3", value: "tier_3" },
];

const PHONE_QUALITY_CHOICES = [
  { name: "Excellent", value: "excellent" },
  { name: "Good",      value: "good"      },
  { name: "Fair",      value: "fair"      },
  { name: "Poor",      value: "poor"      },
  { name: "Unknown",   value: "unknown"   },
];

const CONTACT_CONFIDENCE_CHOICES = [
  { name: "High",   value: "high"   },
  { name: "Medium", value: "medium" },
  { name: "Low",    value: "low"    },
];

// ───────────────────────────────────────────────────────────────────────────
// Property Filter Choices — v3
// ───────────────────────────────────────────────────────────────────────────

const SQ_FT_RANGE_CHOICES = [
  { name: "0–1,000 sq ft",        value: "0_1000"      },
  { name: "1,000–1,250 sq ft",    value: "1000_1250"   },
  { name: "1,250–1,500 sq ft",    value: "1250_1500"   },
  { name: "1,500–1,750 sq ft",    value: "1500_1750"   },
  { name: "1,750–2,000 sq ft",    value: "1750_2000"   },
  { name: "2,000–2,500 sq ft",    value: "2000_2500"   },
  { name: "2,500–3,000 sq ft",    value: "2500_3000"   },
  { name: "3,000+ sq ft",          value: "3000_plus"   },
  { name: "Non-SFR",              value: "non_sfr"     },
];

const UNITS_RANGE_CHOICES = [
  { name: "1 unit",     value: "1"       },
  { name: "2 units",    value: "2"       },
  { name: "3–4 units",  value: "3_4"     },
  { name: "5–10 units", value: "5_10"    },
  { name: "11–25 units", value: "11_25"  },
  { name: "26–50 units", value: "26_50"  },
  { name: "51+ units",  value: "51_plus" },
];

const OWNERSHIP_YEARS_RANGE_CHOICES = [
  { name: "0–2 years",   value: "0_2"   },
  { name: "3–5 years",   value: "3_5"   },
  { name: "6–10 years",  value: "6_10"  },
  { name: "11–20 years", value: "11_20" },
  { name: "21+ years",   value: "21_plus" },
];

const ESTIMATED_VALUE_RANGE_CHOICES = [
  { name: "$0–$100k",      value: "0_100k"      },
  { name: "$100k–$200k",   value: "100k_200k"   },
  { name: "$200k–$350k",   value: "200k_350k"   },
  { name: "$350k–$500k",   value: "350k_500k"   },
  { name: "$500k–$1M",     value: "500k_1m"     },
  { name: "$1M+",          value: "1m_plus"     },
];

const EQUITY_PERCENT_RANGE_CHOICES = [
  { name: "0–25%",   value: "0_25"   },
  { name: "25–50%",  value: "25_50"  },
  { name: "50–70%",  value: "50_70"  },
  { name: "70–90%",  value: "70_90"  },
  { name: "90–100%", value: "90_100" },
];

const REPAIR_COST_RANGE_CHOICES = [
  { name: "$0–$10k",    value: "0_10k"    },
  { name: "$10k–$25k",  value: "10k_25k"  },
  { name: "$25k–$50k",  value: "25k_50k"  },
  { name: "$50k–$100k", value: "50k_100k" },
  { name: "$100k+",     value: "100k_plus" },
];

const BUILDING_CONDITION_CHOICES = [
  { name: "Excellent", value: "Excellent" },
  { name: "Very Good", value: "Very Good" },
  { name: "Good",      value: "Good"      },
  { name: "Average",   value: "Average"   },
  { name: "Fair",      value: "Fair"      },
  { name: "Poor",      value: "Poor"      },
  { name: "Unsound",   value: "Unsound"   },
  { name: "Unknown",   value: "Unknown"   },
];

const OFFER_VS_LOAN_CHOICES = [
  { name: "Free and Clear",       value: "free_and_clear"    },
  { name: "Offer < Loan",          value: "offer_less_loan"   },
  { name: "Offer > Loan (Clear)",  value: "offer_greater_loan" },
  { name: "Offer ≈ Loan",          value: "offer_equal_loan"  },
  { name: "No Purchase Data",      value: "no_purchase_data"  },
];

const OFFER_VS_PURCHASE_PRICE_CHOICES = [
  { name: "No Purchase Data",        value: "no_purchase_data"        },
  { name: "Offer < Purchase",        value: "offer_less_purchase"     },
  { name: "Offer > Purchase (Win)",  value: "offer_greater_purchase"  },
  { name: "Offer ≈ Purchase",        value: "offer_equal_purchase"    },
];

const YEAR_BUILT_RANGE_CHOICES = [
  { name: "Pre-1940",         value: "pre_1940"   },
  { name: "1940–1960",        value: "1940_1960"  },
  { name: "1960–1980",        value: "1960_1980"  },
  { name: "1980–2000",        value: "1980_2000"  },
  { name: "2000+",            value: "2000_plus"  },
];

const TARGETING_COMMANDS = [
  // ── /target-scan ───────────────────────────────────────────────────────
  {
    name:        "target-scan",
    description: "Run a core owner targeting scan",
    options: [
      {
        type:        OPT.STRING,
        name:        "market",
        description: "Market to scan",
        required:    true,
        choices:     MARKET_CHOICES,
      },
      {
        type:        OPT.STRING,
        name:        "asset_class",
        description: "Asset class",
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
        type:        OPT.STRING,
        name:        "property_tag_1",
        description: "Property tag filter (primary)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "property_tag_2",
        description: "Property tag filter (secondary)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "property_tag_3",
        description: "Property tag filter (tertiary)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "priority_tier",
        description: "Priority tier filter",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "language",
        description: "Outreach language (default auto)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "phone_quality",
        description: "Phone quality bucket",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "contact_confidence",
        description: "Contact confidence bucket",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "owner_type",
        description: "Owner entity type filter",
        required:    false,
      },
      {
        type:        OPT.INTEGER,
        name:        "min_contactability_score",
        description: "Minimum contactability score (0-100)",
        required:    false,
        min_value:   0,
        max_value:   100,
      },
      {
        type:        OPT.INTEGER,
        name:        "min_financial_pressure_score",
        description: "Minimum financial pressure score (0-100)",
        required:    false,
        min_value:   0,
        max_value:   100,
      },
      {
        type:        OPT.INTEGER,
        name:        "min_urgency_score",
        description: "Minimum urgency score (0-100)",
        required:    false,
        min_value:   0,
        max_value:   100,
      },
      {
        type:        OPT.INTEGER,
        name:        "min_equity",
        description: "Minimum equity percentile (0–100)",
        required:    false,
        min_value:   0,
        max_value:   100,
      },
      {
        type:        OPT.INTEGER,
        name:        "max_scan_count",
        description: "Maximum records to scan (default 100, max 5000)",
        required:    false,
        min_value:   1,
        max_value:   5000,
      },
      {
        type:        OPT.INTEGER,
        name:        "target_eligible_count",
        description: "Stop after this many eligible owners (default 25, max 500)",
        required:    false,
        min_value:   1,
        max_value:   500,
      },
    ],
  },

  // ── /target-property ───────────────────────────────────────────────────
  {
    name:        "target-property",
    description: "Run an advanced property-first targeting scan",
    options: [
      {
        type:        OPT.STRING,
        name:        "market",
        description: "Market to scan",
        required:    true,
        choices:     MARKET_CHOICES,
      },
      {
        type:        OPT.STRING,
        name:        "asset_class",
        description: "Asset class",
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
        type:        OPT.STRING,
        name:        "property_tag_1",
        description: "Property tag filter (primary)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "property_tag_2",
        description: "Property tag filter (secondary)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "property_tag_3",
        description: "Property tag filter (tertiary)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "sq_ft_range",
        description: "Square footage range (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "units_range",
        description: "Number of units range (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "ownership_years_range",
        description: "Ownership tenure range (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "estimated_value_range",
        description: "Current estimated property value (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "equity_percent_range",
        description: "Equity percentage range (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "repair_cost_range",
        description: "Estimated repair cost range (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "building_condition",
        description: "Building condition assessment (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "offer_vs_loan",
        description: "Smart offer vs mortgage balance (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "offer_vs_last_purchase_price",
        description: "Smart offer vs last purchase price (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "year_built_range",
        description: "Property year-built range (property-first filter)",
        required:    false,
      },
      {
        type:        OPT.INTEGER,
        name:        "min_property_score",
        description: "Minimum FINAL Aquisition Score (0–100)",
        required:    false,
        min_value:   0,
        max_value:   100,
      },
      {
        type:        OPT.STRING,
        name:        "priority_tier",
        description: "Priority tier filter",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "language",
        description: "Outreach language (default auto)",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "phone_quality",
        description: "Phone quality bucket",
        required:    false,
      },
      {
        type:        OPT.STRING,
        name:        "contact_confidence",
        description: "Contact confidence bucket",
        required:    false,
      },
      {
        type:        OPT.INTEGER,
        name:        "max_scan_count",
        description: "Maximum records to scan (default 5000)",
        required:    false,
        min_value:   1,
        max_value:   10000,
      },
      {
        type:        OPT.INTEGER,
        name:        "target_eligible_count",
        description: "Stop after this many eligible owners (default 250)",
        required:    false,
        min_value:   1,
        max_value:   5000,
      },
    ],
  },

  {
    name:        "target-build",
    description: "Open the campaign target builder",
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
        // — Required —
        {
          type:        OPT.STRING,
          name:        "name",
          description: "Human-readable campaign name",
          required:    true,
        },
        {
          type:        OPT.STRING,
          name:        "market",
          description: "Market",
          required:    true,
          choices:     MARKET_CHOICES,
        },
        {
          type:        OPT.STRING,
          name:        "asset",
          description: "Asset class",
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
        // — Property tags (optional) —
        {
          type:        OPT.STRING,
          name:        "tag_1",
          description: "Property tag filter (primary)",
          required:    false,
          choices:     PROPERTY_TAG_CHOICES,
        },
        {
          type:        OPT.STRING,
          name:        "tag_2",
          description: "Property tag filter (secondary)",
          required:    false,
          choices:     PROPERTY_TAG_CHOICES,
        },
        {
          type:        OPT.STRING,
          name:        "tag_3",
          description: "Property tag filter (tertiary)",
          required:    false,
          choices:     PROPERTY_TAG_CHOICES,
        },
        // — Geographic filters (optional) —
        {
          type:        OPT.STRING,
          name:        "zip",
          description: "Zip code filter",
          required:    false,
        },
        {
          type:        OPT.STRING,
          name:        "county",
          description: "County filter",
          required:    false,
        },
        // — Property filters (optional) —
        {
          type:        OPT.INTEGER,
          name:        "min_equity",
          description: "Minimum equity percentile (0–100)",
          required:    false,
          min_value:   0,
          max_value:   100,
        },
        {
          type:        OPT.INTEGER,
          name:        "max_year_built",
          description: "Maximum year built (e.g. 1980)",
          required:    false,
          min_value:   1800,
          max_value:   2030,
        },
        // — Owner filters (optional) —
        {
          type:        OPT.STRING,
          name:        "owner_type",
          description: "Owner entity type filter",
          required:    false,
          choices:     OWNER_TYPE_CHOICES,
        },
        {
          type:        OPT.STRING,
          name:        "phone_status",
          description: "Phone activity filter",
          required:    false,
          choices:     PHONE_STATUS_CHOICES,
        },
        // — Outreach settings (optional) —
        {
          type:        OPT.STRING,
          name:        "language",
          description: "Outreach language (default auto)",
          required:    false,
          choices:     LANGUAGE_CHOICES,
        },
        {
          type:        OPT.INTEGER,
          name:        "motivation_min",
          description: "Minimum motivation score (0–100)",
          required:    false,
          min_value:   0,
          max_value:   100,
        },
        // — Volume controls (optional) —
        {
          type:        OPT.INTEGER,
          name:        "daily_cap",
          description: "Max messages per day (default 50, max 500 for non-Owner)",
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

// ── /briefing ─────────────────────────────────────────────────────────────
const BRIEFING_COMMANDS = [
  {
    name:        "briefing",
    description: "Daily KPI briefing for outreach, offers, contracts, wires, and system health.",    
    options: [
      // /briefing today
      {
        type:        OPT.SUB_COMMAND,
        name:        "today",
        description: "Empire briefing for today",
        options: [
          {
            type:        OPT.STRING,
            name:        "timezone",
            description: "IANA timezone (default America/Chicago)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "market",
            description: "Filter by market",
            required:    false,
            choices:     MARKET_CHOICES,
          },
          {
            type:        OPT.BOOLEAN,
            name:        "include_system_health",
            description: "Include system health section (default true)",
            required:    false,
          },
        ],
      },
      // /briefing yesterday
      {
        type:        OPT.SUB_COMMAND,
        name:        "yesterday",
        description: "Empire briefing for yesterday",
        options: [
          {
            type:        OPT.STRING,
            name:        "timezone",
            description: "IANA timezone (default America/Chicago)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "market",
            description: "Filter by market",
            required:    false,
            choices:     MARKET_CHOICES,
          },
        ],
      },
      // /briefing week
      {
        type:        OPT.SUB_COMMAND,
        name:        "week",
        description: "Empire briefing for the last 7 days",
        options: [
          {
            type:        OPT.STRING,
            name:        "timezone",
            description: "IANA timezone (default America/Chicago)",
            required:    false,
          },
          {
            type:        OPT.STRING,
            name:        "market",
            description: "Filter by market",
            required:    false,
            choices:     MARKET_CHOICES,
          },
        ],
      },
      // /briefing market
      {
        type:        OPT.SUB_COMMAND,
        name:        "market",
        description: "Empire briefing scoped to a specific market",
        options: [
          {
            type:        OPT.STRING,
            name:        "market",
            description: "Market to scope the briefing to",
            required:    true,
            choices:     MARKET_CHOICES,
          },
          {
            type:        OPT.STRING,
            name:        "range",
            description: "Date range (default today)",
            required:    false,
            choices: [
              { name: "Today",     value: "today"     },
              { name: "Yesterday", value: "yesterday" },
              { name: "Week",      value: "week"      },
              { name: "Month",     value: "month"     },
            ],
          },
        ],
      },
      // /briefing agent
      {
        type:        OPT.SUB_COMMAND,
        name:        "agent",
        description: "Empire briefing scoped to a specific agent",
        options: [
          {
            type:        OPT.STRING,
            name:        "agent",
            description: "Agent name or ID",
            required:    true,
          },
          {
            type:        OPT.STRING,
            name:        "range",
            description: "Date range (default today)",
            required:    false,
            choices: [
              { name: "Today",     value: "today"     },
              { name: "Yesterday", value: "yesterday" },
              { name: "Week",      value: "week"      },
            ],
          },
        ],
      },
    ],
  },
];

// Final command set = existing + targeting console additions + replay additions + wires additions + briefing.
const ALL_COMMANDS = [...COMMANDS, ...TARGETING_COMMANDS, ...REPLAY_COMMANDS, ...WIRES_COMMANDS, ...BRIEFING_COMMANDS];

try {
  validateCommandOptionCounts(ALL_COMMANDS, 25);
  validateCommandPayloadSizes(ALL_COMMANDS, 8000);
} catch (err) {
  console.error("Command registration schema error:");
  console.error(err?.message ?? String(err));
  process.exit(1);
}

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
