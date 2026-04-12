/**
 * nexus-adapters.js
 *
 * Pure field-mapping functions: raw Podio items → NEXUS domain types.
 * No Podio external_ids appear in any return value — all field names stay
 * inside this file and nexus-service.js.
 *
 * All functions are synchronous and stateless.
 * Callers are responsible for null / missing item graceful handling.
 */

import {
  getCategoryValue,
  getCategoryValues,
  getNumberValue,
  getMoneyValue,
  getTextValue,
  getDateValue,
  getFirstAppReferenceId,
  getAppReferenceIds,
  normalizeBooleanLabel,
} from "@/lib/providers/podio.js";
import { MASTER_OWNER_FIELDS } from "@/lib/podio/apps/master-owners.js";
import { BRAIN_FIELDS } from "@/lib/podio/apps/ai-conversation-brain.js";
import { TEXTGRID_NUMBER_FIELDS } from "@/lib/podio/apps/textgrid-numbers.js";
import { MARKET_FIELDS } from "@/lib/podio/apps/markets.js";
import { MARKET_CENTROIDS } from "@/lib/dashboard/ops-config.js";

// ─────────────────────────────────────────────────────────────────────────────
// PRIVATE UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, Number(value) || 0));
}

function toIso(raw) {
  if (!raw) return null;
  const ts = new Date(raw).getTime();
  return Number.isNaN(ts) ? null : new Date(ts).toISOString();
}

function daysSince(iso) {
  if (!iso) return 0;
  const ms = Date.now() - new Date(iso).getTime();
  return Math.max(0, Math.floor(ms / 86_400_000));
}

// ─────────────────────────────────────────────────────────────────────────────
// PIPELINE STAGE MAPPING
// ─────────────────────────────────────────────────────────────────────────────

const BRAIN_STAGE_MAP = {
  "ownership confirmation":         "new",
  "offer interest confirmation":    "contacted",
  "seller price discovery":         "responding",
  "condition timeline discovery":   "responding",
  "offer positioning":              "negotiating",
  "negotiation":                    "negotiating",
  "verbal acceptance lock":         "negotiating",
  "contract out":                   "under-contract",
  "signed closing":                 "under-contract",
  "closed dead outcome":            "new",
};

const CONTACT_STATUS_MAP = {
  "new":                "new",
  "never contacted":    "new",
  "pristine":           "new",
  "not contacted":      "new",
  "attempted contact":  "contacted",
  "left message":       "contacted",
  "contacted":          "contacted",
  "follow up":          "contacted",
  "in conversation":    "responding",
  "responding":         "responding",
  "active":             "responding",
  "hot":                "responding",
  "warm":               "responding",
  "negotiating":        "negotiating",
  "offer submitted":    "negotiating",
  "under contract":     "under-contract",
  "contracted":         "under-contract",
  "closed":             "under-contract",
};

export function toNexusPipelineStage(brain_stage_raw, contact_status_raw) {
  const from_brain = BRAIN_STAGE_MAP[lower(brain_stage_raw)];
  if (from_brain) return from_brain;

  const contact_lower = lower(contact_status_raw);
  for (const [key, stage] of Object.entries(CONTACT_STATUS_MAP)) {
    if (contact_lower.includes(key)) return stage;
  }
  return "new";
}

// ─────────────────────────────────────────────────────────────────────────────
// SENTIMENT MAPPING
// ─────────────────────────────────────────────────────────────────────────────

const EMOTIONAL_TONE_MAP = [
  { keys: ["hot", "highly motivated", "urgently selling", "ready to sell", "motivated"], tone: "hot" },
  { keys: ["warm", "receptive", "interested", "open", "potentially interested"], tone: "warm" },
  { keys: ["neutral", "considering", "maybe", "unsure", "undecided"], tone: "neutral" },
  { keys: ["cold", "declined", "resistant", "not interested", "dnc", "wrong number", "disagreeable"], tone: "cold" },
];

export function toNexusSentiment(emotional_tone_raw, motivation_score) {
  const tone = lower(emotional_tone_raw);
  for (const { keys, tone: result } of EMOTIONAL_TONE_MAP) {
    if (keys.some((k) => tone.includes(k))) return result;
  }
  const score = clamp(motivation_score, 0, 100);
  if (score >= 75) return "hot";
  if (score >= 50) return "warm";
  if (score >= 25) return "neutral";
  return "cold";
}

// ─────────────────────────────────────────────────────────────────────────────
// ALERT PRIORITY MAPPING
// ─────────────────────────────────────────────────────────────────────────────

export function toNexusAlertPriority(priority_tier, deal_tag) {
  const raw = lower(priority_tier || deal_tag || "");
  if (/^s-?tier|^a-?tier|priority.?1|^s$/.test(raw)) return "P0";
  if (/^b-?tier|priority.?2|^b$/.test(raw))            return "P1";
  if (/^c-?tier|priority.?3|^c$/.test(raw))            return "P2";
  return "P3";
}

// ─────────────────────────────────────────────────────────────────────────────
// PROPERTY / OWNER TYPE MAPPING
// ─────────────────────────────────────────────────────────────────────────────

export function toNexusPropertyType(raw) {
  const v = lower(raw);
  if (v.includes("multi") || v.includes("mfr")) return "Multi-Family";
  if (v.includes("duplex"))                       return "Duplex";
  if (v.includes("mobile") || v.includes("manufactured") || v === "mh") return "Mobile Home";
  if (v.includes("land") || v.includes("vacant") || v.includes("lot"))  return "Vacant Land";
  return "SFR";
}

export function toNexusOwnerType(owner_name, tax_delinquent_count) {
  if (Number(tax_delinquent_count) > 0) return "tax-delinquent";
  const name = lower(owner_name);
  if (/\b(estate|heir|heirs|attn|deceas|probate|trust)\b/.test(name))          return "estate";
  if (/\b(llc|corp|inc|ltd|company|holding|holdings|management|properties|investments|enterprise|group|partners)\b/.test(name)) return "corporate";
  return "absentee";
}

// ─────────────────────────────────────────────────────────────────────────────
// MARKET HEALTH MAPPING
// ─────────────────────────────────────────────────────────────────────────────

export function toNexusMarketHeat(hotness_score) {
  const score = clamp(hotness_score, 0, 100);
  if (score >= 70) return "hot";
  if (score >= 45) return "warm";
  return "steady";
}

export function toNexusCampaignStatus(any_hard_paused, any_risk_spike) {
  if (any_hard_paused)   return "paused";
  if (any_risk_spike)    return "warning";
  return "live";
}

export function toNexusOperationalRisk(deliverability_pct, reply_rate_pct, any_risk_spike) {
  if (any_risk_spike || deliverability_pct < 80 || reply_rate_pct < 1.0) return "elevated";
  if (deliverability_pct < 90 || reply_rate_pct < 2.5)                    return "moderate";
  return "nominal";
}

export function toNexusStageMomentum(brain_stage_raw, days_in_pipeline) {
  const days = Number(days_in_pipeline) || 0;
  const nexus_stage = BRAIN_STAGE_MAP[lower(brain_stage_raw)] ?? "new";
  if (["negotiating", "under-contract"].includes(nexus_stage) && days < 10) return "accelerating";
  if (days > 25) return "stalling";
  return "steady";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEXTGRID NUMBER AGGREGATION
// Returns: Map<market_podio_item_id_string, AggregatedTextgridStats>
// ─────────────────────────────────────────────────────────────────────────────

export function aggregateTextgridByMarket(textgrid_items = []) {
  const by_market = new Map();

  for (const item of textgrid_items) {
    // Numbers may reference one primary market or multiple — try plural first
    const multi_ids = getAppReferenceIds(item, TEXTGRID_NUMBER_FIELDS.markets);
    const single_id = getFirstAppReferenceId(item, TEXTGRID_NUMBER_FIELDS.market, null);
    const market_ids = multi_ids.length > 0
      ? multi_ids
      : single_id ? [single_id] : [];

    if (market_ids.length === 0) continue;

    const sent_today      = Number(getNumberValue(item, TEXTGRID_NUMBER_FIELDS.sent_today, 0) || 0);
    const delivered_today = Number(getNumberValue(item, TEXTGRID_NUMBER_FIELDS.delivered_today, 0) || 0);
    const replies_today   = Number(getNumberValue(item, TEXTGRID_NUMBER_FIELDS.replies_today, 0) || 0);
    const sent_last_hour  = Number(getNumberValue(item, TEXTGRID_NUMBER_FIELDS.sent_last_hour, 0) || 0);
    const daily_cap       = Number(getNumberValue(item, TEXTGRID_NUMBER_FIELDS.daily_send_cap, 500) || 500);
    const hard_paused     = normalizeBooleanLabel(getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.hard_pause, "no")) === "yes";
    const risk_spike      = normalizeBooleanLabel(getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.risk_spike_flag, "no")) === "yes";
    const ai_risk_level   = clean(getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.ai_risk_level, ""));
    const last_used_at    = getDateValue(item, TEXTGRID_NUMBER_FIELDS.last_used_at, null);
    const tg_status       = lower(getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.status, ""));
    const is_active       = !hard_paused && !tg_status.includes("retired") && !tg_status.includes("decommission");

    for (const market_id of market_ids) {
      const mid = String(market_id);
      const existing = by_market.get(mid);

      if (existing) {
        existing.sent_today       += sent_today;
        existing.delivered_today  += delivered_today;
        existing.replies_today    += replies_today;
        existing.sent_last_hour   += sent_last_hour;
        existing.daily_cap        += daily_cap;
        existing.number_count     += 1;
        existing.active_count     += is_active ? 1 : 0;
        existing.paused_count     += hard_paused ? 1 : 0;
        existing.risk_spike_count += risk_spike ? 1 : 0;
        existing.hard_paused_any  = existing.hard_paused_any || hard_paused;
        existing.risk_spike_any   = existing.risk_spike_any  || risk_spike;
        if (ai_risk_level) existing.ai_risk_levels.push(ai_risk_level);
        if (last_used_at && (!existing.last_used_at || last_used_at > existing.last_used_at)) {
          existing.last_used_at = last_used_at;
        }
      } else {
        by_market.set(mid, {
          market_podio_item_id: mid,
          sent_today,
          delivered_today,
          replies_today,
          sent_last_hour,
          daily_cap,
          number_count:     1,
          active_count:     is_active ? 1 : 0,
          paused_count:     hard_paused ? 1 : 0,
          risk_spike_count: risk_spike ? 1 : 0,
          hard_paused_any:  hard_paused,
          risk_spike_any:   risk_spike,
          ai_risk_levels:   ai_risk_level ? [ai_risk_level] : [],
          last_used_at:     last_used_at ?? null,
        });
      }
    }
  }

  return by_market;
}

// ─────────────────────────────────────────────────────────────────────────────
// MARKET RECORD ADAPTER
// ─────────────────────────────────────────────────────────────────────────────

export function toMarketId(title) {
  return "m-" + lower(title)
    .replace(/[,\s]+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-{2,}/g, "-")
    .replace(/^-|-$/g, "");
}

// Distributes daily sent_today across 24 hours using a business-hours bell curve
function estimateHourlyOutbound(sent_today) {
  const total = Number(sent_today) || 0;
  const weights = [
    0.01, 0.01, 0.01, 0.01, 0.01, 0.02,
    0.03, 0.05, 0.07, 0.09, 0.10, 0.10,
    0.09, 0.08, 0.07, 0.06, 0.06, 0.05,
    0.04, 0.03, 0.02, 0.01, 0.01, 0.01,
  ];
  return weights.map((w) => Math.round(total * w));
}

const EMPTY_TG = {
  sent_today: 0, delivered_today: 0, replies_today: 0,
  sent_last_hour: 0, daily_cap: 500, number_count: 0,
  active_count: 0, paused_count: 0, risk_spike_count: 0,
  hard_paused_any: false, risk_spike_any: false,
  ai_risk_levels: [], last_used_at: null,
};

export function adaptMarketRecord(market_item, textgrid_agg, leads_for_market = []) {
  const title    = clean(getTextValue(market_item, MARKET_FIELDS.title, "") || market_item?.title || "");
  const hotness  = Number(getNumberValue(market_item, MARKET_FIELDS.market_hotness_score, 50) || 50);
  const centroid = MARKET_CENTROIDS[title] ?? null;
  const lat      = centroid?.lat ?? 35.0;
  const lng      = centroid?.lng ?? -95.0;

  const state_match = title.match(/,\s*([A-Z]{2})\s*$/);
  const state_code  = state_match?.[1] ?? "";
  const city_name   = title.split(",")[0]?.trim() ?? title;
  const slug        = lower(city_name).replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");

  const tg = textgrid_agg ?? EMPTY_TG;
  const sent_today     = tg.sent_today;
  const replies_today  = tg.replies_today;
  const reply_rate     = sent_today > 0 ? (replies_today / sent_today) * 100 : 0;
  const deliverability = sent_today > 0
    ? clamp((tg.delivered_today / sent_today) * 100, 0, 100)
    : 98;

  const hot_leads       = leads_for_market.filter((l) => l.urgencyScore >= 75).length;
  const pipeline_value  = leads_for_market.reduce((sum, l) => sum + (l.offerAmount || l.estimatedValue || 0), 0);
  const pending_fu      = leads_for_market.filter((l) => l.urgencyScore >= 60).length;

  const stage_counts = { new: 0, contacted: 0, responding: 0, negotiating: 0, underContract: 0 };
  for (const lead of leads_for_market) {
    if      (lead.pipelineStage === "new")            stage_counts.new++;
    else if (lead.pipelineStage === "contacted")      stage_counts.contacted++;
    else if (lead.pipelineStage === "responding")     stage_counts.responding++;
    else if (lead.pipelineStage === "negotiating")    stage_counts.negotiating++;
    else if (lead.pipelineStage === "under-contract") stage_counts.underContract++;
  }
  const total_leads = leads_for_market.length || 1;
  const pipeline_distribution = {
    new:           Math.round((stage_counts.new / total_leads) * 100),
    contacted:     Math.round((stage_counts.contacted / total_leads) * 100),
    responding:    Math.round((stage_counts.responding / total_leads) * 100),
    negotiating:   Math.round((stage_counts.negotiating / total_leads) * 100),
    underContract: Math.round((stage_counts.underContract / total_leads) * 100),
  };

  const capacity_strain = tg.daily_cap > 0 ? clamp((sent_today / tg.daily_cap) * 100, 0, 100) : 0;
  const health_score = clamp(
    Math.round(
      deliverability * 0.5 +
      Math.min(reply_rate * 10, 30) +
      (tg.risk_spike_any ? -15 : 0) +
      (tg.hard_paused_any ? -30 : 0) +
      20
    ),
    0, 100
  );

  const relative_tf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
  const scan_label = tg.last_used_at
    ? `SWEEP ${relative_tf.format(-Math.round((Date.now() - new Date(tg.last_used_at).getTime()) / 60_000), "minute").toUpperCase()}`
    : "SCAN IDLE";

  return {
    id:              toMarketId(title),
    slug,
    name:            city_name,
    stateCode:       state_code,
    label:           title,
    lat,
    lng,
    heat:            toNexusMarketHeat(hotness),
    campaignStatus:  toNexusCampaignStatus(tg.hard_paused_any, tg.risk_spike_any),
    scanLabel:       scan_label,
    activeProperties: leads_for_market.length,
    totalOutbound:   sent_today,
    outboundToday:   sent_today,
    repliesToday:    replies_today,
    hotLeads:        hot_leads,
    pipelineValue:   pipeline_value,
    deliverability:  Math.round(deliverability * 10) / 10,
    healthScore:     health_score,
    activeCampaigns: tg.active_count,
    replyRate:       Math.round(reply_rate * 10) / 10,
    positiveRate:    Math.round(Math.min(reply_rate * 0.42, 100) * 10) / 10,
    optOutRate:      0, // derived in nexus-service from message events
    pendingFollowUps: pending_fu,
    hourlyOutbound:  estimateHourlyOutbound(sent_today),
    recentReplyRate: Array(8).fill(Math.round(reply_rate * 10) / 10),
    topZips:         [], // enriched separately
    pipelineDistribution: pipeline_distribution,
    lastSweepIso:    toIso(tg.last_used_at) ?? new Date().toISOString(),
    operationalRisk: toNexusOperationalRisk(deliverability, reply_rate, tg.risk_spike_any),
    capacityStrain:  Math.round(capacity_strain),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// LEAD RECORD ADAPTER  (MasterOwner + Brain + Property → LiveLead)
// ─────────────────────────────────────────────────────────────────────────────

function buildHeatFactors(owner_item, brain_item) {
  const factors = [];
  const urgency          = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.urgency_score, 0) || 0);
  const financial_pres   = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.financial_pressure_score, 0) || 0);
  const tax_delinquent   = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_tax_delinquent_count, 0) || 0);
  const lien_count       = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_lien_count, 0) || 0);
  const portfolio_count  = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_property_count, 0) || 0);

  if (urgency >= 75)         factors.push("High urgency score");
  if (financial_pres >= 70)  factors.push("Financial pressure detected");
  if (tax_delinquent > 0)    factors.push(`${tax_delinquent} tax-delinquent propert${tax_delinquent === 1 ? "y" : "ies"}`);
  if (lien_count > 0)        factors.push(`${lien_count} active lien${lien_count === 1 ? "" : "s"}`);
  if (portfolio_count >= 3)  factors.push(`Multi-property portfolio (${portfolio_count})`);

  if (brain_item) {
    const deal_tag   = lower(getCategoryValue(brain_item, BRAIN_FIELDS.deal_priority_tag, ""));
    const motivation = Number(getNumberValue(brain_item, BRAIN_FIELDS.seller_motivation_score, 0) || 0);
    const ai_route   = lower(getCategoryValue(brain_item, BRAIN_FIELDS.ai_route, ""));
    const stage      = lower(getCategoryValue(brain_item, BRAIN_FIELDS.conversation_stage, ""));

    if (deal_tag && !deal_tag.includes("c-tier") && !deal_tag.includes("d-tier")) {
      factors.push(`${clean(getCategoryValue(brain_item, BRAIN_FIELDS.deal_priority_tag, ""))} priority`);
    }
    if (motivation >= 70) factors.push(`AI motivation ${motivation}/100`);
    if (ai_route === "negotiation" || ai_route === "offer") factors.push("Active offer negotiation");
    if (stage === "verbal acceptance lock") factors.push("Verbal acceptance reached");
    if (stage === "contract out")           factors.push("Contract under review");
  }

  return factors.slice(0, 5);
}

function buildRiskFlags(owner_item, brain_item) {
  const flags = [];

  if (brain_item) {
    const raw = getTextValue(brain_item, BRAIN_FIELDS.risk_flags_ai, "");
    if (raw) {
      const parsed = raw.split(/[,;|\n]+/).map((s) => clean(s)).filter((s) => s.length > 3 && s.length < 100);
      flags.push(...parsed);
    }
  }

  const lien_count     = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_lien_count, 0) || 0);
  const contactability = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.contactability_score, 100) || 100);

  if (lien_count > 0 && !flags.some((f) => lower(f).includes("lien"))) {
    flags.push("Active liens on portfolio");
  }
  if (contactability < 40) flags.push("Low contactability score");

  return flags.slice(0, 6);
}

function buildMessageThread(brain_item) {
  if (!brain_item) return [];
  const messages = [];
  const last_contact = getDateValue(brain_item, BRAIN_FIELDS.last_contact_timestamp, null);
  const ts = toIso(last_contact) ?? new Date().toISOString();

  const last_outbound = getTextValue(brain_item, BRAIN_FIELDS.last_outbound_message, "");
  const last_inbound  = getTextValue(brain_item, BRAIN_FIELDS.last_inbound_message, "");

  if (last_outbound) {
    messages.push({
      id: `msg-out-${brain_item.item_id}`,
      direction: "outbound",
      message: last_outbound,
      timestampIso: ts,
      aiGenerated: true,
    });
  }
  if (last_inbound) {
    messages.push({
      id: `msg-in-${brain_item.item_id}`,
      direction: "inbound",
      message: last_inbound,
      timestampIso: ts,
      aiGenerated: false,
    });
  }
  return messages;
}

export function adaptLeadRecord(owner_item, brain_item, property_item, market_record) {
  const owner_id   = String(owner_item?.item_id ?? "");
  const owner_name = clean(
    getTextValue(owner_item, MASTER_OWNER_FIELDS.owner_full_name, "") ||
    owner_item?.title ||
    "Unknown Owner"
  );

  // Owner scores
  const urgency_score  = clamp(getNumberValue(owner_item, MASTER_OWNER_FIELDS.urgency_score, 0), 0, 100);
  const priority_score = clamp(getNumberValue(owner_item, MASTER_OWNER_FIELDS.master_owner_priority_score, 0), 0, 100);
  const financial_pres = clamp(getNumberValue(owner_item, MASTER_OWNER_FIELDS.financial_pressure_score, 0), 0, 100);
  const contactability = clamp(getNumberValue(owner_item, MASTER_OWNER_FIELDS.contactability_score, 50), 0, 100);
  const tax_delinquent = Number(getNumberValue(owner_item, MASTER_OWNER_FIELDS.portfolio_tax_delinquent_count, 0) || 0);

  const last_outbound_raw  = getDateValue(owner_item, MASTER_OWNER_FIELDS.last_outbound, null);
  const last_inbound_raw   = getDateValue(owner_item, MASTER_OWNER_FIELDS.last_inbound, null);
  const priority_tier      = clean(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.priority_tier, ""));
  const property_type_raw  = clean(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.property_type_majority, "SFR"));
  const contact_status_raw = clean(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.contact_status, ""));

  // Brain enrichment
  let brain_stage_raw    = "";
  let seller_state_raw   = "";
  let motivation_score   = urgency_score;
  let emotional_tone_raw = "";
  let ai_summary         = "";
  let recommended_action = "";
  let objections         = [];
  let deal_tag           = priority_tier;
  let cash_offer_target  = 0;

  if (brain_item) {
    brain_stage_raw    = clean(getCategoryValue(brain_item, BRAIN_FIELDS.conversation_stage, ""));
    seller_state_raw   = clean(getCategoryValue(brain_item, BRAIN_FIELDS.current_seller_state, ""));
    motivation_score   = clamp(getNumberValue(brain_item, BRAIN_FIELDS.seller_motivation_score, urgency_score), 0, 100);
    emotional_tone_raw = clean(getCategoryValue(brain_item, BRAIN_FIELDS.seller_emotional_tone, ""));
    // full_conversation_summary_ai maps to external_id "title" (a standard text field)
    ai_summary         = clean(getTextValue(brain_item, BRAIN_FIELDS.full_conversation_summary_ai, "") || brain_item?.title || "");
    recommended_action = clean(getTextValue(brain_item, BRAIN_FIELDS.ai_recommended_next_move, ""));
    deal_tag           = clean(getCategoryValue(brain_item, BRAIN_FIELDS.deal_priority_tag, priority_tier));
    objections         = getCategoryValues(brain_item, BRAIN_FIELDS.primary_objection_type).filter(Boolean).slice(0, 3);
    cash_offer_target  = Number(getMoneyValue(brain_item, BRAIN_FIELDS.cash_offer_target, 0) || 0);
  }

  // Property data — provides address, coordinates, estimated value
  let address         = "";
  let city            = market_record?.name ?? "";
  let state_code      = market_record?.stateCode ?? "";
  let zip             = "";
  let lat             = market_record?.lat ?? 35.0;
  let lng             = market_record?.lng ?? -95.0;
  let estimated_value = 0;

  if (property_item) {
    address = clean(getTextValue(property_item, "property-address", "") || property_item?.title || "");
    // Property location field contains geocoordinates
    const loc_values = property_item?.fields?.["property-address"]?.values ?? [];
    const loc_val    = loc_values[0]?.value;
    if (loc_val?.lat && loc_val?.lng) {
      lat = Number(loc_val.lat);
      lng = Number(loc_val.lng);
    } else {
      // Fall back to parsed lat/lng text fields on some property records
      const p_lat = Number(getTextValue(property_item, "latitude", "") || NaN);
      const p_lng = Number(getTextValue(property_item, "longitude", "") || NaN);
      if (Number.isFinite(p_lat) && Number.isFinite(p_lng)) {
        lat = p_lat;
        lng = p_lng;
      }
    }
    estimated_value = Number(getMoneyValue(property_item, "estimated-value", 0) ||
      getNumberValue(property_item, "estimated-value", 0) || 0);

    // Extract city/state/zip from address string when present
    const addr_parts = address.split(",").map((s) => s.trim());
    if (addr_parts.length >= 2) city = addr_parts[1] || city;
    if (addr_parts.length >= 3) {
      const state_zip = addr_parts[2].trim().split(/\s+/);
      state_code = state_zip[0] ?? state_code;
      zip        = state_zip[1] ?? zip;
    }
  }

  // Add deterministic jitter when falling back to market centroid
  // so leads from the same market scatter visually on the map
  if (lat === (market_record?.lat ?? 35.0) && lng === (market_record?.lng ?? -95.0)) {
    const seed = owner_id.split("").reduce((acc, c) => acc + c.charCodeAt(0), 0);
    lat = lat + ((seed % 41) - 20) * 0.012;
    lng = lng + ((Math.floor(seed / 41) % 41) - 20) * 0.014;
  }

  const pipeline_stage  = toNexusPipelineStage(brain_stage_raw, contact_status_raw);
  const days_in_pipeline = last_outbound_raw ? daysSince(last_outbound_raw) : 0;
  const risk_summary = brain_item
    ? clean(getTextValue(brain_item, BRAIN_FIELDS.last_message_summary_ai, "")) ||
      (objections.length > 0 ? `Primary objection: ${objections[0]}` : "No active objections")
    : "Awaiting AI analysis";

  return {
    id:           `lead-${owner_id}`,
    marketId:     market_record?.id ?? "m-unknown",
    marketLabel:  market_record?.label ?? "",
    address:      address || `Lead #${owner_id}`,
    city,
    stateCode:    state_code,
    zip,
    lat,
    lng,
    ownerName:    owner_name,
    ownerType:    toNexusOwnerType(owner_name, tax_delinquent),
    propertyType: toNexusPropertyType(property_type_raw),
    sentiment:    toNexusSentiment(emotional_tone_raw, motivation_score),
    pipelineStage:     pipeline_stage,
    currentIntent:     seller_state_raw || contact_status_raw || "Pending qualification",
    estimatedValue:    estimated_value,
    offerAmount:       cash_offer_target,
    pipelineDays:      days_in_pipeline,
    outboundAttempts:  0, // enriched in service layer
    lastOutboundIso:   toIso(last_outbound_raw) ?? new Date().toISOString(),
    lastInboundIso:    toIso(last_inbound_raw),
    aiSummary:         ai_summary || `${owner_name} — ${pipeline_stage} stage via automated SMS outreach.`,
    heatFactors:       buildHeatFactors(owner_item, brain_item),
    urgencyScore:      Math.round(clamp(Math.max(urgency_score, motivation_score), 0, 100)),
    opportunityScore:  Math.round(clamp(priority_score, 0, 100)),
    actionConfidence:  Math.round(clamp(contactability, 0, 100)),
    conversationTemperature: Math.round(clamp(financial_pres, 0, 100)),
    stageMomentum:     toNexusStageMomentum(brain_stage_raw, days_in_pipeline),
    riskSummary:       risk_summary,
    riskFlags:         buildRiskFlags(owner_item, brain_item),
    objectionsDetected: objections,
    recommendedAction: recommended_action || "Review lead and schedule follow-up touch",
    messages:          buildMessageThread(brain_item),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// AGENT RECORD ADAPTER  (AI Brain session → LiveAgent)
// ─────────────────────────────────────────────────────────────────────────────

const ACTIVE_BRAIN_STAGES = new Set([
  "negotiation", "verbal acceptance lock", "contract out",
  "offer positioning", "offer interest confirmation",
]);

export function adaptAgentRecord(brain_item, owner_item, market_record) {
  const brain_id   = String(brain_item?.item_id ?? "");
  const owner_name = clean(
    getTextValue(owner_item, MASTER_OWNER_FIELDS.owner_full_name, "") ||
    owner_item?.title || "Unknown"
  );
  const ai_route    = clean(getCategoryValue(brain_item, BRAIN_FIELDS.ai_route, "follow up"));
  const sms_agent   = clean(getCategoryValue(brain_item, BRAIN_FIELDS.sms_agent, ""));
  const stage       = lower(getCategoryValue(brain_item, BRAIN_FIELDS.conversation_stage, ""));
  const last_contact = getDateValue(brain_item, BRAIN_FIELDS.last_contact_timestamp, null);
  const agent_name  = sms_agent || `AI Agent ${brain_id.slice(-4)}`;
  const specialty   = ai_route.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()) || "General Follow-up";

  const status = ACTIVE_BRAIN_STAGES.has(stage)
    ? "active"
    : last_contact && daysSince(last_contact) < 1
      ? "active"
      : last_contact && daysSince(last_contact) < 3
        ? "watching"
        : "queued";

  return {
    id:                `agent-brain-${brain_id}`,
    name:              agent_name,
    specialty,
    status,
    handledToday:      1,
    avgResponseMinutes: 4,
    successRate:       76,
    load:              0.65,
    marketId:          market_record?.id ?? "m-unknown",
    marketLabel:       market_record?.label ?? "",
    focusLeadId:       `lead-${owner_item?.item_id ?? ""}`,
    focusLeadLabel:    `${owner_name} • ${market_record?.name ?? ""}`,
    activityLabel:     stage || ai_route || "Active conversation",
    aiSummary:
      clean(getTextValue(brain_item, BRAIN_FIELDS.full_conversation_summary_ai, "") || brain_item?.title || "") ||
      `${agent_name} managing ${ai_route} stage with ${owner_name}`,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// ALERT ADAPTERS
// ─────────────────────────────────────────────────────────────────────────────

export function adaptTextgridAlert(tg_item, market_record) {
  const hard_paused  = normalizeBooleanLabel(getCategoryValue(tg_item, TEXTGRID_NUMBER_FIELDS.hard_pause, "no")) === "yes";
  const risk_spike   = normalizeBooleanLabel(getCategoryValue(tg_item, TEXTGRID_NUMBER_FIELDS.risk_spike_flag, "no")) === "yes";

  if (!hard_paused && !risk_spike) return null;

  const number_title = clean(
    getCategoryValue(tg_item, TEXTGRID_NUMBER_FIELDS.friendly_name, "") ||
    getTextValue(tg_item, TEXTGRID_NUMBER_FIELDS.title, "") ||
    tg_item?.title || "Number"
  );
  const risk_level   = clean(getCategoryValue(tg_item, TEXTGRID_NUMBER_FIELDS.ai_risk_level, ""));
  const pause_reason = clean(getCategoryValue(tg_item, TEXTGRID_NUMBER_FIELDS.pause_reason, ""));
  const sent_today   = Number(getNumberValue(tg_item, TEXTGRID_NUMBER_FIELDS.sent_today, 0) || 0);
  const daily_cap    = Number(getNumberValue(tg_item, TEXTGRID_NUMBER_FIELDS.daily_send_cap, 500) || 500);
  const last_used    = getDateValue(tg_item, TEXTGRID_NUMBER_FIELDS.last_used_at, null);
  const tg_id        = String(tg_item?.item_id ?? Math.random().toString(36).slice(2));
  const market_id    = market_record?.id ?? "m-unknown";
  const market_label = market_record?.label ?? "";
  const timestamp    = toIso(last_used) ?? new Date().toISOString();

  if (hard_paused) {
    return {
      id:          `alert-tg-pause-${tg_id}`,
      marketId:    market_id,
      marketLabel: market_label,
      severity:    "critical",
      priority:    "P0",
      title:       `Number hard-paused: ${number_title}`,
      detail:      pause_reason || "Hard pause active — outbound suspended",
      metricLabel: "Sent Today",
      metricValue: `${sent_today}`,
      timestampIso: timestamp,
    };
  }

  return {
    id:          `alert-tg-spike-${tg_id}`,
    marketId:    market_id,
    marketLabel: market_label,
    severity:    "warning",
    priority:    "P1",
    title:       `Risk spike detected: ${number_title}`,
    detail:      risk_level ? `AI risk level: ${risk_level}` : "Delivery risk spike flagged by AI",
    metricLabel: "Sent / Cap",
    metricValue: `${sent_today} / ${daily_cap}`,
    timestampIso: timestamp,
  };
}

export function adaptQueueAlert(failed_count, queued_count) {
  if (failed_count === 0) return null;
  const severity = failed_count >= 20 ? "critical" : failed_count >= 5 ? "warning" : "info";
  const priority = failed_count >= 20 ? "P0"        : failed_count >= 5 ? "P1"      : "P2";
  return {
    id:          `alert-queue-failures-${Date.now()}`,
    marketId:    "m-global",
    marketLabel: "Global",
    severity,
    priority,
    title:       `${failed_count} send failure${failed_count === 1 ? "" : "s"} in queue`,
    detail:      `${queued_count} message${queued_count === 1 ? "" : "s"} awaiting delivery`,
    metricLabel: "Failed",
    metricValue: `${failed_count}`,
    timestampIso: new Date().toISOString(),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// ACTIVITY TIMELINE ADAPTER  (Message Event record → LiveActivity)
// ─────────────────────────────────────────────────────────────────────────────

const EVENT_KIND_MAP = {
  outbound_sent:       "ai",
  outbound_queued:     "ai",
  delivered:           "ai",
  inbound_reply:       "conversation",
  queue_failure:       "alert",
  offer_created:       "deal",
  contract_sent:       "deal",
  closing_scheduled:   "deal",
  title_opened:        "deal",
  system_alert:        "system",
  buyer_package_sent:  "deal",
  buyer_selected:      "deal",
  buyer_interested:    "conversation",
  buyer_passed:        "conversation",
};

const EVENT_SEVERITY_MAP = {
  queue_failure:  "warning",
  system_alert:   "critical",
  inbound_reply:  "info",
  buyer_selected: "info",
  offer_created:  "info",
  contract_sent:  "info",
};

export function adaptMessageEventToActivity(event_record, market_record) {
  if (!event_record?.event_type) return null;
  return {
    id:          event_record.id,
    marketId:    market_record?.id ?? "m-global",
    marketLabel: market_record?.label ?? event_record.market_name ?? "Active Campaign",
    kind:        EVENT_KIND_MAP[event_record.event_type] ?? "system",
    severity:    EVENT_SEVERITY_MAP[event_record.event_type] ?? "info",
    title:       event_record.title ?? "Activity",
    detail:      clean(event_record.detail ?? ""),
    timestampIso: toIso(event_record.timestamp) ?? new Date().toISOString(),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// SYSTEM HEALTH BUILDER
// ─────────────────────────────────────────────────────────────────────────────

export function buildNexusSystemHealth({
  textgrid_items = [],
  queue_status_counts = [],
  brain_count = 0,
  active_markets_count = 0,
  delivery_rate_pct = 0,
}) {
  const queue_map = queue_status_counts.reduce((acc, { status, count }) => {
    acc[status] = count;
    return acc;
  }, {});

  const failed_count  = Number(queue_map.Failed  || 0);
  const queued_count  = Number(queue_map.Queued   || 0);
  const sending_count = Number(queue_map.Sending  || 0);

  const paused_numbers     = textgrid_items.filter((i) => normalizeBooleanLabel(getCategoryValue(i, TEXTGRID_NUMBER_FIELDS.hard_pause, "no")) === "yes");
  const risk_spike_numbers = textgrid_items.filter((i) => normalizeBooleanLabel(getCategoryValue(i, TEXTGRID_NUMBER_FIELDS.risk_spike_flag, "no")) === "yes");

  const queue_health = failed_count >= 20 ? "critical"
    : failed_count >= 5 ? "warning"
    : queued_count > 1000 ? "degraded"
    : "healthy";

  const delivery_health = delivery_rate_pct > 0
    ? (delivery_rate_pct < 75 ? "critical"
      : delivery_rate_pct < 85 ? "warning"
      : delivery_rate_pct < 92 ? "degraded"
      : "healthy")
    : "healthy"; // no data → assume healthy

  const textgrid_health = paused_numbers.length > 0 ? "critical"
    : risk_spike_numbers.length >= 3 ? "warning"
    : risk_spike_numbers.length >= 1 ? "degraded"
    : "healthy";

  const brain_health = brain_count === 0 ? "degraded"
    : brain_count < 3 ? "warning"
    : "healthy";

  const now_ts = new Date().toISOString();
  const fmt_time = new Intl.DateTimeFormat("en-US", { hour: "numeric", minute: "2-digit" }).format(new Date());

  return [
    {
      id:             "send-queue",
      label:          "Send Queue",
      status:         queue_health,
      value:          `${queued_count + sending_count} active`,
      detail:         failed_count > 0 ? `${failed_count} failed` : "Nominal",
      lastUpdatedIso: now_ts,
    },
    {
      id:             "textgrid",
      label:          "TextGrid Delivery",
      status:         textgrid_health,
      value:          delivery_rate_pct > 0 ? `${delivery_rate_pct.toFixed(1)}%` : "—",
      detail:         risk_spike_numbers.length > 0
        ? `${risk_spike_numbers.length} risk spike${risk_spike_numbers.length === 1 ? "" : "s"}`
        : "Nominal",
      lastUpdatedIso: now_ts,
    },
    {
      id:             "ai-brain",
      label:          "AI Brain",
      status:         brain_health,
      value:          `${brain_count} active`,
      detail:         brain_count > 0 ? "Conversations managed" : "No active brain sessions",
      lastUpdatedIso: now_ts,
    },
    {
      id:             "podio-api",
      label:          "Podio API",
      status:         "healthy",
      value:          "Connected",
      detail:         `Synced at ${fmt_time}`,
      lastUpdatedIso: now_ts,
    },
    {
      id:             "campaigns",
      label:          "Active Campaigns",
      status:         paused_numbers.length > 0 ? "warning" : "healthy",
      value:          `${active_markets_count} market${active_markets_count === 1 ? "" : "s"}`,
      detail:         paused_numbers.length > 0
        ? `${paused_numbers.length} number${paused_numbers.length === 1 ? "" : "s"} paused`
        : "All campaigns active",
      lastUpdatedIso: now_ts,
    },
    {
      id:             "message-events",
      label:          "Message Events",
      status:         "healthy",
      value:          "Live polling",
      detail:         "15s refresh cadence",
      lastUpdatedIso: now_ts,
    },
  ];
}

export default {
  aggregateTextgridByMarket,
  adaptMarketRecord,
  adaptLeadRecord,
  adaptAgentRecord,
  adaptTextgridAlert,
  adaptQueueAlert,
  adaptMessageEventToActivity,
  buildNexusSystemHealth,
  toMarketId,
  toNexusPipelineStage,
  toNexusSentiment,
  toNexusAlertPriority,
  toNexusPropertyType,
  toNexusOwnerType,
  toNexusMarketHeat,
  toNexusCampaignStatus,
  toNexusOperationalRisk,
  toNexusStageMomentum,
};
