/**
 * nexus-service.js
 *
 * Orchestrates live data fetching from Podio and builds the full
 * LiveDashboardModel response shape consumed by the NEXUS frontend.
 *
 * Sources used:
 *   - TextGrid Numbers    → per-market delivery stats, system health
 *   - MasterOwners        → lead pipeline (top N by priority)
 *   - AI Conversation Brain → lead enrichment, recommended actions, risk flags
 *   - Properties          → lead geo-coordinates and addresses
 *   - Message Events      → activity timeline, opt-out rate
 *   - Send Queue          → queue health, summary metrics
 *   - Markets             → market metadata (name, hotness score)
 *
 * All raw Podio field names are contained here and in nexus-adapters.js.
 * The response is ready for direct consumption by the NEXUS frontend.
 */

import APP_IDS from "@/lib/config/app-ids.js";
import {
  filterAppItems,
  getItem,
  getFirstAppReferenceId,
  getCategoryValue,
  getNumberValue,
  getTextValue,
  getDateValue,
} from "@/lib/providers/podio.js";
import { MASTER_OWNER_FIELDS, findSmsEligibleMasterOwnerItems } from "@/lib/podio/apps/master-owners.js";
import { BRAIN_FIELDS, findBrainItems } from "@/lib/podio/apps/ai-conversation-brain.js";
import { TEXTGRID_NUMBER_FIELDS, findTextgridNumbers } from "@/lib/podio/apps/textgrid-numbers.js";
import { MARKET_FIELDS } from "@/lib/podio/apps/markets.js";
import { findSendQueueItems } from "@/lib/podio/apps/send-queue.js";
import { findMessageEvents } from "@/lib/podio/apps/message-events.js";
import { MARKET_CENTROIDS } from "@/lib/dashboard/ops-config.js";
import { readThroughCache } from "@/lib/dashboard/ops-cache.js";
import { child } from "@/lib/logging/logger.js";
import {
  aggregateTextgridByMarket,
  adaptMarketRecord,
  adaptLeadRecord,
  adaptAgentRecord,
  adaptTextgridAlert,
  adaptQueueAlert,
  adaptMessageEventToActivity,
  buildNexusSystemHealth,
  toMarketId,
} from "@/lib/dashboard/nexus/nexus-adapters.js";

const logger = child({ module: "dashboard.nexus.service" });

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────

const SNAPSHOT_TTL_MS    = 18_000;   // 18 s — live polling cycle
const TEXTGRID_TTL_MS    = 30_000;   // 30 s — TextGrid numbers change slowly
const QUEUE_COUNTS_TTL_MS = 8_000;   // 8 s  — queue counts are volatile

const MAX_LEADS          = 20;       // Top N MasterOwners fetched per refresh
const MAX_TIMELINE_ITEMS = 30;       // Activity events in feed
const MAX_TEXTGRID_ITEMS = 80;       // TextGrid numbers fetched in one call

// Podio message-event direction field
const ME_DIRECTION_FIELD = "direction";
const ME_STATUS_FIELD    = "status-3";
const ME_TIMESTAMP_FIELD = "timestamp";
const ME_ROUTE_FIELD     = "ai-route";
const ME_SOURCE_FIELD    = "source-app";
const ME_AI_OUTPUT_FIELD = "ai-output";
const ME_FAILURE_BUCKET  = "failure-bucket";

// ─────────────────────────────────────────────────────────────────────────────
// PRIVATE UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toItems(result) {
  if (Array.isArray(result))        return result;
  if (Array.isArray(result?.items)) return result.items;
  return [];
}

function sortByItemIdDesc(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function getItemCreatedAt(item) {
  return item?.created_on ?? item?.last_edit_on ?? item?.last_event_on ?? null;
}

function parseJson(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function compactValue(value) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(Number(value) || 0);
}

function currencyValue(value) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(Number(value) || 0);
}

function percentValue(value) {
  return `${Number(value || 0).toFixed(1)}%`;
}

// ─────────────────────────────────────────────────────────────────────────────
// SEND QUEUE STATUS COUNTS
// ─────────────────────────────────────────────────────────────────────────────

async function fetchQueueStatusCounts() {
  return readThroughCache("dashboard:nexus:queue-counts", QUEUE_COUNTS_TTL_MS, async () => {
    const statuses = ["Queued", "Sending", "Sent", "Failed", "Blocked"];
    const counts = await Promise.all(
      statuses.map(async (status) => {
        try {
          const response = await findSendQueueItems({ "queue-status": status }, 1, 0);
          const count = Number(
            response?.filtered ?? response?.total ?? toItems(response).length ?? 0
          );
          return { status, count };
        } catch (err) {
          logger.warn("nexus.queue_count_failed", { status, message: err?.message });
          return { status, count: 0 };
        }
      })
    );
    return counts;
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// TEXTGRID NUMBERS
// ─────────────────────────────────────────────────────────────────────────────

async function fetchTextgridNumbers() {
  return readThroughCache("dashboard:nexus:textgrid-numbers", TEXTGRID_TTL_MS, async () => {
    try {
      const items = await findTextgridNumbers({}, MAX_TEXTGRID_ITEMS, 0).then(toItems);
      return sortByItemIdDesc(items);
    } catch (err) {
      logger.warn("nexus.textgrid_fetch_failed", { message: err?.message });
      return [];
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// MARKET ITEMS  (one call — all markets up front)
// ─────────────────────────────────────────────────────────────────────────────

async function fetchAllMarketItems() {
  return readThroughCache("dashboard:nexus:market-items", 60_000, async () => {
    try {
      const items = await filterAppItems(APP_IDS.markets, {}, { limit: 100, offset: 0 }).then(toItems);
      // Build map of item_id → market_item
      const by_id = new Map();
      for (const item of items) {
        const mid = String(item?.item_id ?? "");
        if (mid) by_id.set(mid, item);
      }
      return by_id;
    } catch (err) {
      logger.warn("nexus.market_items_fetch_failed", { message: err?.message });
      return new Map();
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// TOP MASTER OWNERS
// ─────────────────────────────────────────────────────────────────────────────

async function fetchTopMasterOwners() {
  try {
    const items = await findSmsEligibleMasterOwnerItems({
      limit: MAX_LEADS,
      offset: 0,
      sort_by: MASTER_OWNER_FIELDS.master_owner_priority_score,
      sort_desc: true,
    }).then(toItems);
    return sortByItemIdDesc(items);
  } catch (err) {
    logger.warn("nexus.master_owners_fetch_failed", { message: err?.message });
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// BRAIN ENRICHMENT  (parallel lookups per owner)
// ─────────────────────────────────────────────────────────────────────────────

async function fetchBrainForOwner(owner_id) {
  if (!owner_id) return null;
  try {
    const items = await findBrainItems(
      { [BRAIN_FIELDS.master_owner]: owner_id },
      5,
      0
    );
    if (!items?.length) return null;
    // Take the most recently created brain item (highest item_id)
    return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0] ?? null;
  } catch (err) {
    logger.warn("nexus.brain_fetch_failed", { owner_id, message: err?.message });
    return null;
  }
}

async function fetchBrainItemsForOwners(owners = []) {
  const results = await Promise.all(
    owners.map((owner) => fetchBrainForOwner(owner?.item_id))
  );
  // Return map of owner_item_id → brain_item
  const by_owner = new Map();
  for (let i = 0; i < owners.length; i++) {
    const oid = String(owners[i]?.item_id ?? "");
    if (oid) by_owner.set(oid, results[i] ?? null);
  }
  return by_owner;
}

// ─────────────────────────────────────────────────────────────────────────────
// PROPERTY ITEMS  (fetched via Brain's first property reference)
// ─────────────────────────────────────────────────────────────────────────────

async function fetchPropertyForBrain(brain_item) {
  if (!brain_item) return null;
  const property_id = getFirstAppReferenceId(brain_item, BRAIN_FIELDS.properties, null);
  if (!property_id) return null;
  try {
    return await getItem(property_id);
  } catch (err) {
    logger.warn("nexus.property_fetch_failed", { property_id, message: err?.message });
    return null;
  }
}

async function fetchPropertyItemsForBrains(brain_items_by_owner) {
  const by_owner = new Map();
  const tasks = [...brain_items_by_owner.entries()].map(async ([owner_id, brain_item]) => {
    const prop = await fetchPropertyForBrain(brain_item);
    by_owner.set(owner_id, prop);
  });
  await Promise.all(tasks);
  return by_owner;
}

// ─────────────────────────────────────────────────────────────────────────────
// RECENT MESSAGE EVENTS  (timeline + opt-out rate)
// ─────────────────────────────────────────────────────────────────────────────

async function fetchRecentMessageEvents() {
  try {
    const items = await findMessageEvents({}, MAX_TIMELINE_ITEMS * 2, 0).then(toItems);
    return sortByItemIdDesc(items);
  } catch (err) {
    logger.warn("nexus.message_events_fetch_failed", { message: err?.message });
    return [];
  }
}

/**
 * Converts raw message event Podio items into lightweight event records
 * suitable for the NEXUS activity timeline.
 *
 * This is a simplified version of ops-service's buildMessageEventRecords —
 * it omits related item lookups and produces only what LiveActivity needs.
 */
function parseMessageEventsToRecords(items = [], market_by_podio_id = new Map()) {
  const records = [];

  for (const item of items) {
    const timestamp       = getDateValue(item, ME_TIMESTAMP_FIELD, null) || getItemCreatedAt(item);
    const direction       = clean(getCategoryValue(item, ME_DIRECTION_FIELD, ""));
    const delivery_status = clean(getCategoryValue(item, ME_STATUS_FIELD, ""));
    const route           = clean(getCategoryValue(item, ME_ROUTE_FIELD, ""));
    const source_app      = clean(getTextValue(item, ME_SOURCE_FIELD, ""));
    const failure_bucket  = clean(getCategoryValue(item, ME_FAILURE_BUCKET, ""));
    const meta            = parseJson(getTextValue(item, ME_AI_OUTPUT_FIELD, ""));
    const market_id_ref   = getFirstAppReferenceId(item, "market", null);
    const market_item     = market_id_ref ? market_by_podio_id.get(String(market_id_ref)) : null;

    let event_type = null;
    let title      = null;

    if (source_app.toLowerCase() === "system alert" || clean(meta?.subsystem)) {
      event_type = "system_alert";
      title      = clean(meta?.summary) || "System alert";
    } else if (lower(direction) === "inbound") {
      event_type = "inbound_reply";
      title      = "Inbound SMS received";
    } else if (lower(direction) === "outbound" && lower(delivery_status) === "delivered") {
      event_type = "delivered";
      title      = "Delivery confirmed";
    } else if (lower(direction) === "outbound" && lower(delivery_status) === "sent") {
      event_type = "outbound_sent";
      title      = "Message sent";
    } else if (lower(delivery_status) === "failed") {
      event_type = "queue_failure";
      title      = "Carrier failure";
    }

    if (!event_type) continue;

    const market_record = market_item
      ? { id: toMarketId(clean(getTextValue(market_item, MARKET_FIELDS.title, "") || market_item?.title || "")), label: clean(getTextValue(market_item, MARKET_FIELDS.title, "") || market_item?.title || "") }
      : null;

    records.push({
      id:          `nexus-event:${item.item_id}:${event_type}`,
      event_type,
      timestamp,
      title,
      detail:      clean(meta?.summary) || clean(failure_bucket) || route || null,
      market_id:   market_record?.id ?? null,
      market_name: market_record?.label ?? null,
    });
  }

  return records;
}

// Count opt-out events from message events
function countOptOutsFromEvents(items = []) {
  let opt_outs = 0;
  for (const item of items) {
    const bucket = lower(getCategoryValue(item, ME_FAILURE_BUCKET, ""));
    if (bucket.includes("opt out") || bucket.includes("dnc")) opt_outs++;
  }
  return opt_outs;
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN SNAPSHOT BUILDER
// ─────────────────────────────────────────────────────────────────────────────

async function buildNexusDashboardSnapshot() {
  const snapshot_ts = new Date().toISOString();

  // Phase 1 — parallel primary fetches (all independent)
  const [
    textgrid_items,
    owners,
    message_event_items,
    queue_status_counts,
    market_by_podio_id,
  ] = await Promise.all([
    fetchTextgridNumbers(),
    fetchTopMasterOwners(),
    fetchRecentMessageEvents(),
    fetchQueueStatusCounts(),
    fetchAllMarketItems(),
  ]);

  // Phase 2 — enrichment fetches (brain + property, parallel per owner)
  const brain_by_owner    = await fetchBrainItemsForOwners(owners);
  const property_by_owner = await fetchPropertyItemsForBrains(brain_by_owner);

  // Phase 3 — aggregate TextGrid numbers by market_podio_item_id
  const tg_by_market = aggregateTextgridByMarket(textgrid_items);

  // ── Build market records ──────────────────────────────────────────────────
  // Collect all distinct market podio IDs referenced by TextGrid numbers + owners
  const market_podio_ids = new Set();
  for (const mid of tg_by_market.keys()) market_podio_ids.add(mid);
  for (const owner of owners) {
    const mid = getFirstAppReferenceId(owner, MASTER_OWNER_FIELDS.markets, null);
    if (mid) market_podio_ids.add(String(mid));
  }

  // We'll build lead records first (without final market records) so we can
  // pass leads_for_market to adaptMarketRecord in a second pass.
  // Resolve market_podio_id → market_record (partial — no leads yet)
  const market_record_by_podio_id = new Map();
  for (const podio_id of market_podio_ids) {
    const market_item = market_by_podio_id.get(podio_id);
    if (!market_item) continue;
    const title = clean(getTextValue(market_item, MARKET_FIELDS.title, "") || market_item?.title || "");
    if (!title) continue;
    // Skip markets not in our MARKET_CENTROIDS (likely test/unmapped records)
    if (!MARKET_CENTROIDS[title]) continue;
    market_record_by_podio_id.set(podio_id, {
      id:        toMarketId(title),
      label:     title,
      name:      title.split(",")[0]?.trim() ?? title,
      stateCode: title.match(/,\s*([A-Z]{2})\s*$/)?.[1] ?? "",
      lat:       MARKET_CENTROIDS[title].lat,
      lng:       MARKET_CENTROIDS[title].lng,
    });
  }

  // ── Build lead records ────────────────────────────────────────────────────
  const leads = [];
  for (const owner of owners) {
    const owner_id    = String(owner?.item_id ?? "");
    const brain_item  = brain_by_owner.get(owner_id) ?? null;
    const prop_item   = property_by_owner.get(owner_id) ?? null;
    const market_podio_id = String(
      getFirstAppReferenceId(owner, MASTER_OWNER_FIELDS.markets, "") ?? ""
    );
    const market_record = market_record_by_podio_id.get(market_podio_id) ?? null;

    try {
      leads.push(adaptLeadRecord(owner, brain_item, prop_item, market_record));
    } catch (err) {
      logger.warn("nexus.lead_adapt_failed", { owner_id, message: err?.message });
    }
  }

  // ── Build full market records (with lead stats) ───────────────────────────
  const markets = [];
  for (const [podio_id, partial_market] of market_record_by_podio_id) {
    const market_item    = market_by_podio_id.get(podio_id);
    const tg_agg         = tg_by_market.get(podio_id) ?? null;
    const leads_for_mkt  = leads.filter((l) => l.marketId === partial_market.id);

    try {
      const market_record = adaptMarketRecord(market_item, tg_agg, leads_for_mkt);
      // Apply opt-out rate from message events
      const sent_today_mkt = market_record.outboundToday;
      const opt_outs = countOptOutsFromEvents(
        message_event_items.slice(0, 50) // sample from recent events
      );
      market_record.optOutRate = sent_today_mkt > 0
        ? Math.round((opt_outs / sent_today_mkt) * 1000) / 10
        : 0;
      markets.push(market_record);
    } catch (err) {
      logger.warn("nexus.market_adapt_failed", { podio_id, message: err?.message });
    }
  }

  // ── Build agent records (one AI agent per active Brain session) ───────────
  const agents = [];
  for (const [owner_id, brain_item] of brain_by_owner) {
    if (!brain_item) continue;
    const owner = owners.find((o) => String(o?.item_id) === owner_id);
    if (!owner) continue;
    const market_podio_id = String(
      getFirstAppReferenceId(owner, MASTER_OWNER_FIELDS.markets, "") ?? ""
    );
    const market_record = market_record_by_podio_id.get(market_podio_id) ?? null;
    try {
      agents.push(adaptAgentRecord(brain_item, owner, market_record));
    } catch (err) {
      logger.warn("nexus.agent_adapt_failed", { owner_id, message: err?.message });
    }
  }

  // ── Build alerts ──────────────────────────────────────────────────────────
  const alerts = [];
  for (const tg_item of textgrid_items) {
    const market_podio_id = String(
      getFirstAppReferenceId(tg_item, TEXTGRID_NUMBER_FIELDS.markets, null) ??
      getFirstAppReferenceId(tg_item, TEXTGRID_NUMBER_FIELDS.market, null) ?? ""
    );
    const market_record = market_record_by_podio_id.get(market_podio_id) ?? null;
    const alert = adaptTextgridAlert(tg_item, market_record);
    if (alert) alerts.push(alert);
  }

  const queue_map = queue_status_counts.reduce((acc, { status, count }) => {
    acc[status] = count;
    return acc;
  }, {});
  const queue_alert = adaptQueueAlert(Number(queue_map.Failed || 0), Number(queue_map.Queued || 0));
  if (queue_alert) alerts.push(queue_alert);

  // Sort: P0 critical first
  const priority_order = { P0: 0, P1: 1, P2: 2, P3: 3 };
  alerts.sort((a, b) => (priority_order[a.priority] ?? 3) - (priority_order[b.priority] ?? 3));

  // ── Build activity timeline ───────────────────────────────────────────────
  const event_records   = parseMessageEventsToRecords(message_event_items, market_by_podio_id);
  const timeline = event_records
    .slice(0, MAX_TIMELINE_ITEMS)
    .map((rec) => {
      const market_record = rec.market_id
        ? (markets.find((m) => m.id === rec.market_id) ?? null)
        : null;
      return adaptMessageEventToActivity(rec, market_record);
    })
    .filter(Boolean);

  // ── Build summary metrics ─────────────────────────────────────────────────
  const total_outbound   = markets.reduce((sum, m) => sum + m.outboundToday, 0);
  const total_replies    = markets.reduce((sum, m) => sum + m.repliesToday, 0);
  const total_hot_leads  = markets.reduce((sum, m) => sum + m.hotLeads, 0);
  const total_pipeline   = leads.reduce((sum, l) => sum + (l.offerAmount || l.estimatedValue || 0), 0);
  const pending_fu       = markets.reduce((sum, m) => sum + m.pendingFollowUps, 0);
  const avg_deliverability = markets.length > 0
    ? markets.reduce((sum, m) => sum + m.deliverability, 0) / markets.length
    : 0;
  const avg_health       = markets.length > 0
    ? Math.round(markets.reduce((sum, m) => sum + m.healthScore, 0) / markets.length)
    : 0;
  const reply_rate       = total_outbound > 0 ? (total_replies / total_outbound) * 100 : 0;
  const avg_positive     = markets.length > 0
    ? markets.reduce((sum, m) => sum + m.positiveRate, 0) / markets.length : 0;
  const highest_alert    = alerts[0] ?? null;

  const summary_metrics = [
    {
      id:     "total-outbound",
      label:  "Total Outbound",
      value:  compactValue(total_outbound),
      tone:   "primary",
      detail: `${queue_map.Queued ?? 0} queued`,
    },
    {
      id:     "replies-today",
      label:  "Replies Today",
      value:  compactValue(total_replies),
      tone:   "success",
      detail: `${total_hot_leads} hot lead${total_hot_leads === 1 ? "" : "s"}`,
    },
    {
      id:     "reply-rate",
      label:  "Reply Rate",
      value:  percentValue(reply_rate),
      tone:   "primary",
      detail: `${percentValue(avg_positive)} positive`,
    },
    {
      id:     "opt-out-rate",
      label:  "Opt-Out Rate",
      value:  percentValue(
        markets.length > 0
          ? markets.reduce((sum, m) => sum + m.optOutRate, 0) / markets.length
          : 0
      ),
      tone:   (highest_alert?.severity === "critical" ? "warning" : "muted"),
      detail: highest_alert?.title ?? "No active alerts",
    },
    {
      id:     "active-markets",
      label:  "Active Markets",
      value:  `${markets.length}`,
      tone:   "muted",
      detail: `${markets.filter((m) => m.campaignStatus === "live").length} live`,
    },
    {
      id:     "pending-followups",
      label:  "Pending Follow-ups",
      value:  `${pending_fu}`,
      tone:   "warning",
      detail: `${alerts.length} alerting`,
    },
    {
      id:     "pipeline-value",
      label:  "Pipeline Value",
      value:  currencyValue(total_pipeline),
      tone:   "success",
      detail: `${agents.filter((a) => a.status === "active").length} AI agents active`,
    },
    {
      id:     "deliverability",
      label:  "Deliverability",
      value:  percentValue(avg_deliverability),
      tone:   "primary",
      detail: `Health ${avg_health}`,
    },
  ];

  // ── Build system health ───────────────────────────────────────────────────
  const total_sent      = markets.reduce((sum, m) => sum + m.outboundToday, 0);
  const total_delivered = textgrid_items.reduce(
    (sum, item) => sum + Number(getNumberValue(item, TEXTGRID_NUMBER_FIELDS.delivered_today, 0) || 0),
    0
  );
  const delivery_rate_pct = total_sent > 0 ? (total_delivered / total_sent) * 100 : 0;

  const system_health = buildNexusSystemHealth({
    textgrid_items,
    queue_status_counts,
    brain_count:         [...brain_by_owner.values()].filter(Boolean).length,
    active_markets_count: markets.filter((m) => m.campaignStatus === "live").length,
    delivery_rate_pct,
  });

  // ── Build filter options ──────────────────────────────────────────────────
  const buildFilterOptions = (values) =>
    Array.from(new Set(values))
      .sort((a, b) => a.localeCompare(b))
      .map((value) => ({
        value,
        label: value.split("-").map((s) => s.charAt(0).toUpperCase() + s.slice(1)).join(" "),
      }));

  const filters = {
    propertyTypes:  buildFilterOptions(leads.map((l) => l.propertyType)),
    sentiments:     buildFilterOptions(leads.map((l) => l.sentiment)),
    pipelineStages: buildFilterOptions(leads.map((l) => l.pipelineStage)),
    ownerTypes:     buildFilterOptions(leads.map((l) => l.ownerType)),
  };

  const health_label = highest_alert
    ? `HOME BASE • ${highest_alert.title}`
    : "HOME BASE • NOMINAL";

  return {
    generatedAtIso: snapshot_ts,
    appName:        "NEXUS",
    dataSource:     "live",
    summaryMetrics: summary_metrics,
    markets,
    leads,
    agents,
    alerts,
    timeline,
    mapLinks:       [],
    systemHealth:   system_health,
    filters,
    defaults:       {
      marketId: markets[0]?.id ?? "",
      leadId:   leads[0]?.id   ?? "",
      agentId:  agents[0]?.id  ?? "",
    },
    healthLabel: health_label,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC API
// ─────────────────────────────────────────────────────────────────────────────

export async function getNexusDashboard() {
  return readThroughCache(
    "dashboard:nexus:snapshot",
    SNAPSHOT_TTL_MS,
    buildNexusDashboardSnapshot
  );
}

export default {
  getNexusDashboard,
};
