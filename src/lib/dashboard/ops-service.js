import APP_IDS from "@/lib/config/app-ids.js";
import { BUYER_MATCH_FIELDS, findBuyerMatchItems } from "@/lib/podio/apps/buyer-match.js";
import { MASTER_OWNER_FIELDS, listMasterOwnerViews } from "@/lib/podio/apps/master-owners.js";
import { findSendQueueItems } from "@/lib/podio/apps/send-queue.js";
import { findMessageEvents } from "@/lib/podio/apps/message-events.js";
import { OFFER_FIELDS, findOfferItems } from "@/lib/podio/apps/offers.js";
import { UNDERWRITING_FIELDS, findUnderwritingItems } from "@/lib/podio/apps/underwriting.js";
import { CONTRACT_FIELDS, findContractItems } from "@/lib/podio/apps/contracts.js";
import { TITLE_ROUTING_FIELDS, findTitleRoutingItems } from "@/lib/podio/apps/title-routing.js";
import { CLOSING_FIELDS, findClosingItems } from "@/lib/podio/apps/closings.js";
import { runMasterOwnerOutboundFeeder } from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import {
  getCategoryValue,
  getDateValue,
  getFieldValues,
  getFirstAppReferenceId,
  getItem,
  getTextValue,
} from "@/lib/providers/podio.js";
import { getAttachedFieldSchema } from "@/lib/podio/schema.js";
import { readThroughCache } from "@/lib/dashboard/ops-cache.js";
import {
  MARKET_CENTROIDS,
  OPS_TIME_RANGES,
  EVENT_TYPE_META,
  getEventTypeMeta,
} from "@/lib/dashboard/ops-config.js";
import { child } from "@/lib/logging/logger.js";

const logger = child({
  module: "dashboard.ops.service",
});

const OPS_TIMEZONE = "America/Chicago";
const DEFAULT_TIME_RANGE = "24h";
const DEFAULT_FEED_LIMIT = 40;
const DEFAULT_MAP_LIMIT = 50;
const DEFAULT_SAMPLE_LIMIT = 140;
const DEFAULT_QUEUE_SAMPLE_LIMIT = 180;
const DEFAULT_FEEDER_SCAN_LIMIT = 100;
const DEFAULT_FEEDER_LIMIT = 10;

const SNAPSHOT_TTL_MS = 15_000;
const MAP_TTL_MS = 20_000;
const FEED_TTL_MS = 8_000;
const QUEUE_TTL_MS = 12_000;
const QUEUE_COUNTS_TTL_MS = 8_000;
const FEEDER_TTL_MS = 60_000;
const FILTER_OPTIONS_TTL_MS = 5 * 60_000;

const ACTIVE_CONVERSATION_EVENT_TYPES = new Set([
  "outbound_sent",
  "delivered",
  "inbound_reply",
]);

const SYSTEM_HEALTH_NOTES = Object.freeze([
  "Polling-based live view. No WebSocket transport is used in this version.",
  "Queue status counts are global operational counts from Send Queue.",
  "Timeline and map are generated from recent operational samples and can omit older historical items.",
  "Feeder visibility is view-driven and cached separately to keep Podio cost bounded.",
]);

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toItems(result) {
  if (Array.isArray(result)) return result;
  if (Array.isArray(result?.items)) return result.items;
  return [];
}

function parsePositiveInteger(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function sortByItemIdDesc(items = []) {
  return [...items].sort((left, right) => Number(right?.item_id || 0) - Number(left?.item_id || 0));
}

function sortByTimestampDesc(items = []) {
  return [...items].sort((left, right) => {
    const left_ts = toTimestamp(left?.timestamp) ?? 0;
    const right_ts = toTimestamp(right?.timestamp) ?? 0;
    return right_ts - left_ts;
  });
}

function dayKey(value, timeZone = OPS_TIMEZONE) {
  if (!value) return null;

  return new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value));
}

function isToday(value, timeZone = OPS_TIMEZONE) {
  if (!value) return false;
  return dayKey(value, timeZone) === dayKey(Date.now(), timeZone);
}

function withinRange(value, range_ms) {
  const ts = toTimestamp(value);
  if (ts === null) return false;
  return ts >= Date.now() - range_ms;
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (clean(value)) return clean(value);
  }
  return "";
}

function compactCount(value) {
  const numeric = Number(value || 0);
  if (numeric >= 1000) {
    return `${(numeric / 1000).toFixed(numeric >= 10_000 ? 0 : 1)}k`;
  }
  return String(numeric);
}

function normalizeOptionValue(value, allowed_values = []) {
  const raw = clean(value);
  if (!raw) return null;

  const direct = allowed_values.find((entry) => clean(entry) === raw);
  if (direct) return direct;

  const by_lower = allowed_values.find((entry) => lower(entry) === lower(raw));
  return by_lower || raw;
}

export function parseOpsFilters(input = {}) {
  const normalized_time_range = normalizeOptionValue(
    input?.time_range,
    Object.keys(OPS_TIME_RANGES)
  ) || DEFAULT_TIME_RANGE;

  return {
    source_view_id: clean(input?.source_view_id) || null,
    source_view_name: clean(input?.source_view_name) || null,
    priority_tier: clean(input?.priority_tier) || null,
    file: clean(input?.file) || null,
    market: clean(input?.market) || null,
    event_type: clean(input?.event_type) || null,
    time_range: normalized_time_range,
    time_range_label: OPS_TIME_RANGES[normalized_time_range]?.label || OPS_TIME_RANGES[DEFAULT_TIME_RANGE].label,
    range_ms: OPS_TIME_RANGES[normalized_time_range]?.ms || OPS_TIME_RANGES[DEFAULT_TIME_RANGE].ms,
    feed_limit: parsePositiveInteger(input?.feed_limit, DEFAULT_FEED_LIMIT),
    map_limit: parsePositiveInteger(input?.map_limit, DEFAULT_MAP_LIMIT),
    sample_limit: parsePositiveInteger(input?.sample_limit, DEFAULT_SAMPLE_LIMIT),
    queue_sample_limit: parsePositiveInteger(
      input?.queue_sample_limit,
      DEFAULT_QUEUE_SAMPLE_LIMIT
    ),
    feeder_scan_limit: parsePositiveInteger(
      input?.scan_limit,
      DEFAULT_FEEDER_SCAN_LIMIT
    ),
    feeder_limit: parsePositiveInteger(input?.limit, DEFAULT_FEEDER_LIMIT),
  };
}

function serializeOperationalFilterKey(filters = {}) {
  return JSON.stringify({
    priority_tier: filters.priority_tier || null,
    file: filters.file || null,
    market: filters.market || null,
    event_type: filters.event_type || null,
    time_range: filters.time_range || DEFAULT_TIME_RANGE,
    feed_limit: filters.feed_limit || DEFAULT_FEED_LIMIT,
    map_limit: filters.map_limit || DEFAULT_MAP_LIMIT,
    sample_limit: filters.sample_limit || DEFAULT_SAMPLE_LIMIT,
    queue_sample_limit: filters.queue_sample_limit || DEFAULT_QUEUE_SAMPLE_LIMIT,
  });
}

function serializeFeederFilterKey(filters = {}) {
  return JSON.stringify({
    source_view_id: filters.source_view_id || null,
    source_view_name: filters.source_view_name || null,
    feeder_scan_limit: filters.feeder_scan_limit || DEFAULT_FEEDER_SCAN_LIMIT,
    feeder_limit: filters.feeder_limit || DEFAULT_FEEDER_LIMIT,
  });
}

function getItemCreatedAt(item) {
  return (
    item?.created_on ||
    item?.last_edit_on ||
    item?.last_event_on ||
    item?.initial_revision?.created_on ||
    null
  );
}

function getLocationValue(item, external_id) {
  const first = getFieldValues(item, external_id)[0];
  return first?.value || null;
}

function getLocationCoordinates(item, external_id) {
  const value = getLocationValue(item, external_id);
  if (!value) return null;

  const lat = Number(value?.lat ?? value?.latitude ?? NaN);
  const lng = Number(value?.lng ?? value?.longitude ?? NaN);

  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    return { lat, lng };
  }

  return null;
}

function getPropertyLatitudeLongitude(property_item) {
  const direct_location = getLocationCoordinates(property_item, "property-address");
  if (direct_location) return direct_location;

  const lat = Number(getTextValue(property_item, "latitude", "") || NaN);
  const lng = Number(getTextValue(property_item, "longitude", "") || NaN);

  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    return { lat, lng };
  }

  return null;
}

function getMarketLabel({ market_item = null, property_item = null } = {}) {
  return (
    firstNonEmpty(
      getTextValue(market_item, "title", ""),
      getCategoryValue(property_item, "market-3", "")
    ) || null
  );
}

function getFileLabel({ owner_item = null, property_item = null } = {}) {
  return (
    firstNonEmpty(
      getCategoryValue(property_item, "file", ""),
      getCategoryValue(owner_item, "file", "")
    ) || null
  );
}

function getPriorityTierLabel(owner_item = null) {
  return clean(getCategoryValue(owner_item, MASTER_OWNER_FIELDS.priority_tier, null)) || null;
}

function deriveGeoPoint({ property_item = null, market_item = null, market_label = null } = {}) {
  const from_property = getPropertyLatitudeLongitude(property_item);
  if (from_property) return from_property;

  const fallback_market = clean(market_label) || getMarketLabel({ market_item, property_item });
  if (fallback_market && MARKET_CENTROIDS[fallback_market]) {
    return MARKET_CENTROIDS[fallback_market];
  }

  return null;
}

function normalizeEventType(value = null) {
  const raw = clean(value);
  return raw && EVENT_TYPE_META[raw] ? raw : null;
}

function parseJson(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function normalizeRecord(record, related = {}) {
  const owner_item =
    related.owner_map.get(String(record.master_owner_id || "")) || null;
  const property_item =
    related.property_map.get(String(record.property_id || "")) || null;
  const market_item =
    related.market_map.get(String(record.market_id || "")) || null;

  const owner_name =
    firstNonEmpty(
      getTextValue(owner_item, MASTER_OWNER_FIELDS.owner_full_name, ""),
      owner_item?.title
    ) || null;
  const property_address =
    firstNonEmpty(
      getTextValue(property_item, "property-address", ""),
      property_item?.title
    ) || null;
  const market_name = getMarketLabel({ market_item, property_item });
  const file = getFileLabel({ owner_item, property_item });
  const priority_tier = getPriorityTierLabel(owner_item);
  const geo = deriveGeoPoint({
    property_item,
    market_item,
    market_label: market_name,
  });
  const meta = getEventTypeMeta(record.event_type);

  return {
    ...record,
    meta,
    owner_name,
    property_address,
    market_name,
    file,
    priority_tier,
    geo,
  };
}

function matchesRecordFilters(record, filters) {
  if (filters.event_type && record.event_type !== filters.event_type) return false;
  if (filters.market && !lower(record.market_name).includes(lower(filters.market))) return false;
  if (filters.file && lower(record.file) !== lower(filters.file)) return false;
  if (
    filters.priority_tier &&
    lower(record.priority_tier) !== lower(filters.priority_tier)
  ) {
    return false;
  }
  if (!withinRange(record.timestamp, filters.range_ms)) return false;
  return true;
}

function groupStatusCounts(items = [], external_id, limit = 4) {
  const counts = new Map();

  for (const item of items) {
    const value = clean(getCategoryValue(item, external_id, null)) || "Unknown";
    counts.set(value, (counts.get(value) || 0) + 1);
  }

  return [...counts.entries()]
    .map(([label, count]) => ({ label, count }))
    .sort((left, right) => right.count - left.count)
    .slice(0, limit);
}

function makeRecord({
  id,
  source,
  source_item_id,
  event_type,
  timestamp,
  title,
  detail = null,
  status = null,
  master_owner_id = null,
  property_id = null,
  market_id = null,
  phone_item_id = null,
  route = null,
}) {
  return {
    id,
    source,
    source_item_id,
    event_type,
    timestamp,
    title,
    detail,
    status,
    master_owner_id,
    property_id,
    market_id,
    phone_item_id,
    route,
  };
}

function buildQueueRecords(items = []) {
  const records = [];

  for (const item of items) {
    const status = clean(getCategoryValue(item, "queue-status", ""));
    const timestamp =
      getDateValue(item, "scheduled-for-utc", null) ||
      getDateValue(item, "scheduled-for-local", null) ||
      getItemCreatedAt(item);

    if (status === "Queued") {
      records.push(
        makeRecord({
          id: `queue:${item.item_id}:queued`,
          source: "send_queue",
          source_item_id: item.item_id,
          event_type: "outbound_queued",
          timestamp,
          title: "Owner touch queued",
          detail: clean(getCategoryValue(item, "message-type", "")) || null,
          status,
          master_owner_id: getFirstAppReferenceId(item, "master-owner", null),
          property_id: getFirstAppReferenceId(item, "properties", null),
          market_id: getFirstAppReferenceId(item, "market", null),
          phone_item_id: getFirstAppReferenceId(item, "phone-number", null),
        })
      );
    }

    if (status === "Failed") {
      records.push(
        makeRecord({
          id: `queue:${item.item_id}:failed`,
          source: "send_queue",
          source_item_id: item.item_id,
          event_type: "queue_failure",
          timestamp:
            getDateValue(item, "sent-at", null) ||
            getItemCreatedAt(item),
          title: "Queue failure",
          detail: clean(getTextValue(item, "failed-reason", "")) || null,
          status,
          master_owner_id: getFirstAppReferenceId(item, "master-owner", null),
          property_id: getFirstAppReferenceId(item, "properties", null),
          market_id: getFirstAppReferenceId(item, "market", null),
          phone_item_id: getFirstAppReferenceId(item, "phone-number", null),
        })
      );
    }
  }

  return records;
}

function buildMessageEventRecords(items = []) {
  const records = [];

  for (const item of items) {
    const timestamp =
      getDateValue(item, "timestamp", null) ||
      getItemCreatedAt(item);
    const direction = clean(getCategoryValue(item, "direction", ""));
    const delivery_status = clean(getCategoryValue(item, "status-3", ""));
    const route = clean(getCategoryValue(item, "ai-route", ""));
    const source_app = clean(getTextValue(item, "source-app", ""));
    const meta = parseJson(getTextValue(item, "ai-output", ""));
    const normalized_response = lower(meta?.classification?.normalized_response || "");

    let event_type = null;
    let title = null;

    if (source_app === "System Alert" || meta?.subsystem) {
      event_type = "system_alert";
      title = clean(meta?.summary) || "System alert";
    } else if (meta?.event_kind === "buyer_blast" && direction === "Outbound" && delivery_status === "Sent") {
      event_type = "buyer_package_sent";
      title = "Buyer package sent";
    } else if (meta?.event_kind === "buyer_response") {
      if (normalized_response === "chosen") {
        event_type = "buyer_selected";
        title = "Buyer selected";
      } else if (normalized_response === "passed") {
        event_type = "buyer_passed";
        title = "Buyer passed";
      } else if (["interested", "needs_more_info"].includes(normalized_response)) {
        event_type = "buyer_interested";
        title = "Buyer engaged";
      } else {
        event_type = "inbound_reply";
        title = source_app === "Buyer Disposition" ? "Buyer reply received" : "Inbound SMS received";
      }
    } else if (direction === "Inbound") {
      event_type = "inbound_reply";
      title = "Inbound SMS received";
    } else if (direction === "Outbound" && delivery_status === "Delivered") {
      event_type = "delivered";
      title = "Delivery confirmed";
    } else if (direction === "Outbound" && delivery_status === "Sent") {
      event_type = "outbound_sent";
      title = "Message sent";
    } else if (delivery_status === "Failed") {
      event_type = "queue_failure";
      title = "Carrier failure";
    }

    if (!event_type) continue;

    records.push(
      makeRecord({
        id: `message_event:${item.item_id}:${event_type}`,
        source: "message_events",
        source_item_id: item.item_id,
        event_type,
        timestamp,
        title,
        detail:
          clean(meta?.summary) ||
          clean(meta?.severity) ||
          clean(meta?.company_name) ||
          clean(getTextValue(item, "trigger-name", "")) ||
          clean(getTextValue(item, "message", "")) ||
          null,
        status: clean(meta?.severity) || delivery_status || direction || null,
        master_owner_id: getFirstAppReferenceId(item, "master-owner", null),
        property_id: getFirstAppReferenceId(item, "property", null),
        market_id: getFirstAppReferenceId(item, "market", null),
        phone_item_id: getFirstAppReferenceId(item, "phone-number", null),
        route: route || null,
      })
    );
  }

  return records;
}

function extractSystemAlerts(items = []) {
  return items
    .map((item) => {
      const source_app = clean(getTextValue(item, "source-app", ""));
      const meta = parseJson(getTextValue(item, "ai-output", ""));
      if (source_app !== "System Alert" && !clean(meta?.subsystem)) return null;

      return {
        id: `system-alert:${item?.item_id || meta?.signature || meta?.code || "unknown"}`,
        item_id: item?.item_id || null,
        subsystem: clean(meta?.subsystem) || null,
        code: clean(meta?.code) || null,
        severity: clean(meta?.severity) || "warning",
        retryable: Boolean(meta?.retryable),
        status: clean(meta?.status) || "open",
        operator_state: clean(meta?.operator_state) || "open",
        summary:
          clean(meta?.summary) ||
          clean(getTextValue(item, "message", "")) ||
          "System alert",
        affected_ids: Array.isArray(meta?.affected_ids) ? meta.affected_ids.filter(Boolean) : [],
        occurrence_count: Number(meta?.occurrence_count || 0) || 0,
        acknowledged_at: clean(meta?.acknowledged_at) || null,
        acknowledged_by: clean(meta?.acknowledged_by) || null,
        silenced_until: clean(meta?.silenced_until) || null,
        silenced_by: clean(meta?.silenced_by) || null,
        silenced_reason: clean(meta?.silenced_reason) || null,
        first_seen_at: clean(meta?.first_seen_at) || null,
        last_seen_at:
          clean(meta?.last_seen_at) ||
          getDateValue(item, "timestamp", null) ||
          getItemCreatedAt(item),
      };
    })
    .filter(Boolean)
    .sort((left, right) => (toTimestamp(right?.last_seen_at) || 0) - (toTimestamp(left?.last_seen_at) || 0));
}

function extractBuyerThreads(items = []) {
  return items
    .map((item) => {
      const source_app = clean(getTextValue(item, "source-app", ""));
      const meta = parseJson(getTextValue(item, "ai-output", ""));
      if (source_app !== "Buyer Thread" && meta?.event_kind !== "buyer_thread") return null;

      return {
        id: `buyer-thread:${item?.item_id || meta?.buyer_match_item_id || "unknown"}:${meta?.company_item_id || "unknown"}`,
        item_id: item?.item_id || null,
        buyer_match_item_id: clean(meta?.buyer_match_item_id) || null,
        company_item_id: clean(meta?.company_item_id) || null,
        company_name: clean(meta?.company_name) || "Partner",
        current_state: clean(meta?.current_state) || "Candidate",
        last_channel: clean(meta?.last_channel) || null,
        last_contact_at:
          clean(meta?.last_contact_at) ||
          getDateValue(item, "timestamp", null) ||
          getItemCreatedAt(item),
        selected_buyer: Boolean(meta?.selected_buyer),
        primary_email: clean(meta?.primary_email) || null,
        primary_phone: clean(meta?.primary_phone) || null,
        interaction_counts:
          meta?.interaction_counts && typeof meta.interaction_counts === "object"
            ? meta.interaction_counts
            : {},
      };
    })
    .filter(Boolean)
    .sort((left, right) => (toTimestamp(right?.last_contact_at) || 0) - (toTimestamp(left?.last_contact_at) || 0));
}

function buildOfferRecords(items = []) {
  return items.map((item) =>
    makeRecord({
      id: `offer:${item.item_id}`,
      source: "offers",
      source_item_id: item.item_id,
      event_type: "offer_created",
      timestamp:
        getDateValue(item, OFFER_FIELDS.offer_date, null) ||
        getItemCreatedAt(item),
      title: "Offer created",
      detail: clean(getCategoryValue(item, OFFER_FIELDS.offer_status, "")) || null,
      status: clean(getCategoryValue(item, OFFER_FIELDS.offer_status, "")) || null,
      master_owner_id: getFirstAppReferenceId(item, OFFER_FIELDS.master_owner, null),
      property_id: getFirstAppReferenceId(item, OFFER_FIELDS.property, null),
      market_id: getFirstAppReferenceId(item, OFFER_FIELDS.market, null),
      phone_item_id: getFirstAppReferenceId(item, OFFER_FIELDS.phone_number, null),
    })
  );
}

function buildContractRecords(items = []) {
  return items
    .filter(
      (item) =>
        getDateValue(item, CONTRACT_FIELDS.contract_sent_timestamp, null) ||
        lower(getCategoryValue(item, CONTRACT_FIELDS.contract_status, "")) === "sent"
    )
    .map((item) =>
      makeRecord({
        id: `contract:${item.item_id}`,
        source: "contracts",
        source_item_id: item.item_id,
        event_type: "contract_sent",
        timestamp:
          getDateValue(item, CONTRACT_FIELDS.contract_sent_timestamp, null) ||
          getItemCreatedAt(item),
        title: "Contract sent",
        detail: clean(getCategoryValue(item, CONTRACT_FIELDS.contract_status, "")) || null,
        status: clean(getCategoryValue(item, CONTRACT_FIELDS.contract_status, "")) || null,
        master_owner_id: getFirstAppReferenceId(item, CONTRACT_FIELDS.master_owner, null),
        property_id: getFirstAppReferenceId(item, CONTRACT_FIELDS.property, null),
        market_id: getFirstAppReferenceId(item, CONTRACT_FIELDS.market, null),
        phone_item_id: getFirstAppReferenceId(item, CONTRACT_FIELDS.phone, null),
      })
    );
}

function buildTitleRecords(items = []) {
  return items
    .filter((item) => {
      const status = clean(getCategoryValue(item, TITLE_ROUTING_FIELDS.routing_status, ""));
      const opened_date = getDateValue(item, TITLE_ROUTING_FIELDS.title_opened_date, null);

      return Boolean(
        opened_date ||
          ["Opened", "Clear to Close", "Closed"].includes(status)
      );
    })
    .map((item) =>
      makeRecord({
        id: `title:${item.item_id}`,
        source: "title_routing",
        source_item_id: item.item_id,
        event_type: "title_opened",
        timestamp:
          getDateValue(item, TITLE_ROUTING_FIELDS.title_opened_date, null) ||
          getDateValue(item, TITLE_ROUTING_FIELDS.file_routed_date, null) ||
          getItemCreatedAt(item),
        title: "Title file opened",
        detail: clean(getCategoryValue(item, TITLE_ROUTING_FIELDS.routing_status, "")) || null,
        status: clean(getCategoryValue(item, TITLE_ROUTING_FIELDS.routing_status, "")) || null,
        master_owner_id: getFirstAppReferenceId(item, TITLE_ROUTING_FIELDS.master_owner, null),
        property_id: getFirstAppReferenceId(item, TITLE_ROUTING_FIELDS.property, null),
        market_id: getFirstAppReferenceId(item, TITLE_ROUTING_FIELDS.market, null),
      })
    );
}

function buildClosingRecords(items = []) {
  return items
    .filter((item) => {
      const status = clean(getCategoryValue(item, CLOSING_FIELDS.closing_status, ""));
      return ["Scheduled", "Confirmed", "Rescheduled"].includes(status);
    })
    .map((item) =>
      makeRecord({
        id: `closing:${item.item_id}`,
        source: "closings",
        source_item_id: item.item_id,
        event_type: "closing_scheduled",
        timestamp:
          getDateValue(item, CLOSING_FIELDS.rescheduled_date, null) ||
          getDateValue(item, CLOSING_FIELDS.confirmed_date, null) ||
          getDateValue(item, CLOSING_FIELDS.closing_date_time, null) ||
          getItemCreatedAt(item),
        title: "Closing scheduled",
        detail: clean(getCategoryValue(item, CLOSING_FIELDS.closing_status, "")) || null,
        status: clean(getCategoryValue(item, CLOSING_FIELDS.closing_status, "")) || null,
        master_owner_id: getFirstAppReferenceId(item, CLOSING_FIELDS.master_owner, null),
        property_id: getFirstAppReferenceId(item, CLOSING_FIELDS.property, null),
        market_id: getFirstAppReferenceId(item, CLOSING_FIELDS.market, null),
      })
    );
}

async function loadRelatedMaps(records = []) {
  const owner_ids = new Set();
  const property_ids = new Set();
  const market_ids = new Set();

  for (const record of records) {
    if (record.master_owner_id) owner_ids.add(String(record.master_owner_id));
    if (record.property_id) property_ids.add(String(record.property_id));
    if (record.market_id) market_ids.add(String(record.market_id));
  }

  async function loadMap(ids) {
    const map = new Map();

    await Promise.all(
      [...ids].map(async (item_id) => {
        try {
          map.set(String(item_id), await getItem(item_id));
        } catch (error) {
          logger.warn("dashboard.ops.related_item_failed", {
            item_id,
            message: error?.message || "Unknown Podio item error",
          });
          map.set(String(item_id), null);
        }
      })
    );

    return map;
  }

  const [owner_map, property_map, market_map] = await Promise.all([
    loadMap(owner_ids),
    loadMap(property_ids),
    loadMap(market_ids),
  ]);

  return {
    owner_map,
    property_map,
    market_map,
  };
}

async function fetchRecentData(filters) {
  const [
    queue_items,
    message_events,
    offers,
    underwriting,
    contracts,
    title_files,
    closings,
    buyer_matches,
  ] = await Promise.all([
    Promise.resolve(findSendQueueItems({}, filters.queue_sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findMessageEvents({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findOfferItems({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findUnderwritingItems({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findContractItems({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findTitleRoutingItems({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findClosingItems({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
    Promise.resolve(findBuyerMatchItems({}, filters.sample_limit, 0)).then(toItems).then(sortByItemIdDesc),
  ]);

  return {
    queue_items,
    message_events,
    offers,
    underwriting,
    contracts,
    title_files,
    closings,
    buyer_matches,
    buyer_threads: extractBuyerThreads(message_events),
    system_alerts: extractSystemAlerts(message_events),
  };
}

async function buildOperationalSnapshot(filters) {
  const data = await fetchRecentData(filters);

  const raw_records = [
    ...buildQueueRecords(data.queue_items),
    ...buildMessageEventRecords(data.message_events),
    ...buildOfferRecords(data.offers),
    ...buildContractRecords(data.contracts),
    ...buildTitleRecords(data.title_files),
    ...buildClosingRecords(data.closings),
  ];

  const related_maps = await loadRelatedMaps(raw_records);
  const records = sortByTimestampDesc(
    raw_records.map((record) => normalizeRecord(record, related_maps))
  );

  const filtered_records = records.filter((record) => matchesRecordFilters(record, filters));

  return {
    generated_at: new Date().toISOString(),
    filters,
    queue_items: data.queue_items,
    message_events: data.message_events,
    offers: data.offers,
    underwriting: data.underwriting,
    contracts: data.contracts,
    title_files: data.title_files,
    closings: data.closings,
    buyer_matches: data.buyer_matches,
    buyer_threads: data.buyer_threads,
    system_alerts: data.system_alerts,
    records,
    filtered_records,
    partials: [
      "Timeline, map, and day-based metrics are calculated from recent operational samples.",
      "Queue status counts are direct current counts from Send Queue filters.",
    ],
  };
}

async function getOperationalSnapshot(filters) {
  return readThroughCache(
    `dashboard:ops:snapshot:${serializeOperationalFilterKey(filters)}`,
    SNAPSHOT_TTL_MS,
    () => buildOperationalSnapshot(filters)
  );
}

async function getQueueStatusCounts() {
  return readThroughCache(
    "dashboard:ops:queue-status-counts",
    QUEUE_COUNTS_TTL_MS,
    async () => {
      const statuses = ["Queued", "Sending", "Sent", "Failed", "Blocked"];

      const counts = await Promise.all(
        statuses.map(async (status) => {
          const response = await findSendQueueItems({ "queue-status": status }, 1, 0);
          const count = Number(
            response?.filtered ??
              response?.total ??
              toItems(response).length ??
              0
          );
          return { status, count };
        })
      );

      return counts;
    }
  );
}

function buildKpis(snapshot, queue_status_counts) {
  const recent_records = snapshot.filtered_records;
  const active_conversation_keys = new Set();

  for (const record of recent_records) {
    if (!ACTIVE_CONVERSATION_EVENT_TYPES.has(record.event_type)) continue;
    const conversation_key =
      String(record.phone_item_id || record.master_owner_id || record.id);
    if (conversation_key) active_conversation_keys.add(conversation_key);
  }

  const offers_today = recent_records.filter(
    (record) => record.event_type === "offer_created" && isToday(record.timestamp)
  ).length;
  const contracts_today = recent_records.filter(
    (record) => record.event_type === "contract_sent" && isToday(record.timestamp)
  ).length;
  const delivered_today = recent_records.filter(
    (record) => record.event_type === "delivered" && isToday(record.timestamp)
  ).length;
  const inbound_today = recent_records.filter(
    (record) => record.event_type === "inbound_reply" && isToday(record.timestamp)
  ).length;
  const sent_today = recent_records.filter(
    (record) => record.event_type === "outbound_sent" && isToday(record.timestamp)
  ).length;
  const failures_today = recent_records.filter(
    (record) => record.event_type === "queue_failure" && isToday(record.timestamp)
  ).length;

  const closings_active = snapshot.closings.filter((item) => {
    const status = lower(getCategoryValue(item, CLOSING_FIELDS.closing_status, ""));
    return !["completed", "cancelled"].includes(status);
  }).length;
  const buyer_packages_sent = recent_records.filter(
    (record) => record.event_type === "buyer_package_sent" && isToday(record.timestamp)
  ).length;
  const buyers_interested = recent_records.filter(
    (record) => record.event_type === "buyer_interested" && isToday(record.timestamp)
  ).length;
  const buyers_selected = recent_records.filter(
    (record) => record.event_type === "buyer_selected" && isToday(record.timestamp)
  ).length;

  const queue_counts = queue_status_counts.reduce((acc, item) => {
    acc[item.status] = item.count;
    return acc;
  }, {});

  return [
    {
      id: "queued",
      label: "Queued",
      value: queue_counts.Queued || 0,
      display_value: compactCount(queue_counts.Queued || 0),
      scope: "global",
    },
    {
      id: "sending",
      label: "Sending",
      value: queue_counts.Sending || 0,
      display_value: compactCount(queue_counts.Sending || 0),
      scope: "global",
    },
    {
      id: "sent_today",
      label: "Sent Today",
      value: sent_today,
      display_value: compactCount(sent_today),
      scope: "sample",
    },
    {
      id: "delivered_today",
      label: "Delivered Today",
      value: delivered_today,
      display_value: compactCount(delivered_today),
      scope: "sample",
    },
    {
      id: "failed_today",
      label: "Failed Today",
      value: failures_today,
      display_value: compactCount(failures_today),
      scope: "sample",
    },
    {
      id: "inbound_replies_today",
      label: "Inbound Replies",
      value: inbound_today,
      display_value: compactCount(inbound_today),
      scope: "sample",
    },
    {
      id: "active_conversations",
      label: "Active Conversations",
      value: active_conversation_keys.size,
      display_value: compactCount(active_conversation_keys.size),
      scope: "sample",
    },
    {
      id: "offers_created",
      label: "Offers Created",
      value: offers_today,
      display_value: compactCount(offers_today),
      scope: "sample",
    },
    {
      id: "contracts_sent",
      label: "Contracts Sent",
      value: contracts_today,
      display_value: compactCount(contracts_today),
      scope: "sample",
    },
    {
      id: "closings_active",
      label: "Closings Active",
      value: closings_active,
      display_value: compactCount(closings_active),
      scope: "sample",
    },
    {
      id: "buyer_packages_sent",
      label: "Buyer Packages",
      value: buyer_packages_sent,
      display_value: compactCount(buyer_packages_sent),
      scope: "sample",
    },
    {
      id: "buyers_interested",
      label: "Buyer Interest",
      value: buyers_interested,
      display_value: compactCount(buyers_interested),
      scope: "sample",
    },
    {
      id: "buyers_selected",
      label: "Buyers Chosen",
      value: buyers_selected,
      display_value: compactCount(buyers_selected),
      scope: "sample",
    },
  ];
}

function buildFlowSummary(snapshot) {
  return {
    offers: {
      label: "Offers",
      total_recent: snapshot.offers.length,
      statuses: groupStatusCounts(snapshot.offers, OFFER_FIELDS.offer_status),
    },
    underwriting: {
      label: "Underwriting",
      total_recent: snapshot.underwriting.length,
      statuses: groupStatusCounts(snapshot.underwriting, UNDERWRITING_FIELDS.underwriting_status),
    },
    contracts: {
      label: "Contracts",
      total_recent: snapshot.contracts.length,
      statuses: groupStatusCounts(snapshot.contracts, CONTRACT_FIELDS.contract_status),
    },
    title: {
      label: "Title",
      total_recent: snapshot.title_files.length,
      statuses: groupStatusCounts(snapshot.title_files, TITLE_ROUTING_FIELDS.routing_status),
    },
    closings: {
      label: "Closings",
      total_recent: snapshot.closings.length,
      statuses: groupStatusCounts(snapshot.closings, CLOSING_FIELDS.closing_status),
    },
    buyer_disposition: {
      label: "Buyer Dispo",
      total_recent: snapshot.buyer_matches.length,
      statuses: groupStatusCounts(snapshot.buyer_matches, BUYER_MATCH_FIELDS.match_status),
    },
  };
}

function buildBuyerDispositionSummary(snapshot) {
  const thread_states = [...snapshot.buyer_threads]
    .reduce((acc, thread) => {
      const label = clean(thread?.current_state) || "Candidate";
      acc.set(label, (acc.get(label) || 0) + 1);
      return acc;
    }, new Map());

  return {
    total_recent: snapshot.buyer_matches.length,
    threads_total: snapshot.buyer_threads.length,
    statuses: groupStatusCounts(snapshot.buyer_matches, BUYER_MATCH_FIELDS.match_status, 5),
    response_statuses: groupStatusCounts(
      snapshot.buyer_matches,
      BUYER_MATCH_FIELDS.buyer_response_status,
      5
    ),
    thread_states: [...thread_states.entries()]
      .map(([label, count]) => ({ label, count }))
      .sort((left, right) => right.count - left.count)
      .slice(0, 5),
    recent_threads: snapshot.buyer_threads.slice(0, 8),
    recent_events: snapshot.filtered_records
      .filter((record) =>
        ["buyer_package_sent", "buyer_interested", "buyer_passed", "buyer_selected"].includes(
          record.event_type
        )
      )
      .slice(0, 8),
  };
}

function buildAttentionSummary(snapshot) {
  const recent_alerts = snapshot.system_alerts.filter((alert) => {
    if (lower(alert?.status) === "resolved") return false;
    return withinRange(alert?.last_seen_at, snapshot.filters.range_ms);
  });
  const actionable_alerts = recent_alerts.filter(
    (alert) => lower(alert?.operator_state) !== "silenced"
  );

  const severity_counts = recent_alerts.reduce(
    (acc, alert) => {
      const severity = lower(alert?.severity);
      if (severity === "critical") acc.critical += 1;
      else if (severity === "high") acc.high += 1;
      else if (severity === "warning") acc.warning += 1;
      else acc.info += 1;

      if (alert?.retryable) acc.retryable += 1;
      else acc.non_retryable += 1;

      const operator_state = lower(alert?.operator_state) || "open";
      if (operator_state === "acknowledged") acc.acknowledged += 1;
      else if (operator_state === "silenced") acc.silenced += 1;
      else acc.open += 1;

      return acc;
    },
    {
      critical: 0,
      high: 0,
      warning: 0,
      info: 0,
      open: 0,
      acknowledged: 0,
      silenced: 0,
      retryable: 0,
      non_retryable: 0,
    }
  );

  return {
    needs_attention: severity_counts.open > 0,
    tracked_alerts_count: recent_alerts.length,
    open_alerts_count: severity_counts.open,
    acknowledged_count: severity_counts.acknowledged,
    silenced_count: severity_counts.silenced,
    actionable_alerts_count: actionable_alerts.length,
    critical_count: severity_counts.critical,
    high_count: severity_counts.high,
    warning_count: severity_counts.warning,
    retryable_count: severity_counts.retryable,
    non_retryable_count: severity_counts.non_retryable,
    recent_alerts: recent_alerts.slice(0, 8),
  };
}

function buildHealthSummary(snapshot) {
  const latest_event_ts = snapshot.filtered_records[0]?.timestamp || null;
  const attention = buildAttentionSummary(snapshot);

  return {
    snapshot_generated_at: snapshot.generated_at,
    latest_activity_at: latest_event_ts,
    time_range: snapshot.filters.time_range,
    notes: SYSTEM_HEALTH_NOTES,
    partials: [
      ...snapshot.partials,
      attention.needs_attention
        ? `${attention.open_alerts_count} open alert(s), ${attention.acknowledged_count} acknowledged, and ${attention.silenced_count} silenced in the selected window.`
        : attention.tracked_alerts_count
          ? `${attention.acknowledged_count} acknowledged and ${attention.silenced_count} silenced alert(s) are currently being tracked.`
          : "No active system alerts in the selected window.",
    ],
    attention,
  };
}

function bucketMapPoints(records = []) {
  const buckets = new Map();

  for (const record of records) {
    if (!record.geo) continue;

    const key = `${record.geo.lat.toFixed(2)}:${record.geo.lng.toFixed(2)}:${record.event_type}`;
    const existing = buckets.get(key);

    if (existing) {
      existing.count += 1;
      existing.timestamps.push(record.timestamp);
      continue;
    }

    buckets.set(key, {
      id: key,
      event_type: record.event_type,
      lat: record.geo.lat,
      lng: record.geo.lng,
      count: 1,
      market_name: record.market_name || null,
      label: record.title,
      timestamps: [record.timestamp],
      meta: getEventTypeMeta(record.event_type),
    });
  }

  return [...buckets.values()].sort((left, right) => right.count - left.count);
}

function buildQueueSummary(snapshot, queue_status_counts) {
  const recent_failures = snapshot.filtered_records
    .filter((record) => record.event_type === "queue_failure")
    .slice(0, 8);

  return {
    generated_at: snapshot.generated_at,
    status_breakdown: queue_status_counts.map((entry) => ({
      ...entry,
      meta: getEventTypeMeta(
        entry.status === "Failed" ? "queue_failure" : "outbound_queued"
      ),
    })),
    recent_failures,
    queue_sample_size: snapshot.queue_items.length,
  };
}

function pickDefaultFeederView(views = []) {
  const preferred = views.find((view) => /sms/i.test(view?.name || ""));
  return preferred || views[0] || null;
}

async function buildFilterOptions() {
  const raw_master_owner_views = await listMasterOwnerViews().catch(() => []);
  const master_owner_views = Array.isArray(raw_master_owner_views)
    ? raw_master_owner_views
    : raw_master_owner_views?.views || [];
  const priority_field = getAttachedFieldSchema(
    APP_IDS.master_owners,
    MASTER_OWNER_FIELDS.priority_tier
  );
  const file_field = getAttachedFieldSchema(APP_IDS.master_owners, "file");

  return {
    views: master_owner_views.map((view) => ({
      view_id: view?.view_id ?? null,
      name: view?.name || null,
      type: view?.type || null,
    })),
    priority_tiers: (priority_field?.options || []).map((option) => option.text),
    files: (file_field?.options || []).map((option) => option.text),
    time_ranges: Object.values(OPS_TIME_RANGES).map((entry) => ({
      id: entry.id,
      label: entry.label,
    })),
    event_types: Object.entries(EVENT_TYPE_META).map(([id, meta]) => ({
      id,
      label: meta.label,
    })),
    market_suggestions: Object.keys(MARKET_CENTROIDS).sort(),
  };
}

export async function getOpsFilterOptions() {
  return readThroughCache("dashboard:ops:filter-options", FILTER_OPTIONS_TTL_MS, buildFilterOptions);
}

export async function getOpsKpiSnapshot(input = {}) {
  const filters = parseOpsFilters(input);

  return readThroughCache(
    `dashboard:ops:kpis:${serializeOperationalFilterKey(filters)}`,
    SNAPSHOT_TTL_MS,
    async () => {
      const [snapshot, queue_status_counts] = await Promise.all([
        getOperationalSnapshot(filters),
        getQueueStatusCounts(),
      ]);

      return {
        generated_at: snapshot.generated_at,
        filters,
        kpis: buildKpis(snapshot, queue_status_counts),
        flow: buildFlowSummary(snapshot),
        buyer_dispo: buildBuyerDispositionSummary(snapshot),
        health: buildHealthSummary(snapshot),
      };
    }
  );
}

export async function getOpsFeedSnapshot(input = {}) {
  const filters = parseOpsFilters(input);

  return readThroughCache(
    `dashboard:ops:feed:${serializeOperationalFilterKey(filters)}`,
    FEED_TTL_MS,
    async () => {
      const snapshot = await getOperationalSnapshot(filters);
      return {
        generated_at: snapshot.generated_at,
        filters,
        total_matching_events: snapshot.filtered_records.length,
        events: snapshot.filtered_records.slice(0, filters.feed_limit),
      };
    }
  );
}

export async function getOpsMapSnapshot(input = {}) {
  const filters = parseOpsFilters(input);

  return readThroughCache(
    `dashboard:ops:map:${serializeOperationalFilterKey(filters)}`,
    MAP_TTL_MS,
    async () => {
      const snapshot = await getOperationalSnapshot(filters);
      const relevant = snapshot.filtered_records
        .filter((record) => record.geo)
        .slice(0, Math.max(filters.map_limit * 2, filters.map_limit));

      return {
        generated_at: snapshot.generated_at,
        filters,
        marker_count: relevant.length,
        points: bucketMapPoints(relevant).slice(0, filters.map_limit),
      };
    }
  );
}

export async function getOpsQueueSnapshot(input = {}) {
  const filters = parseOpsFilters(input);

  return readThroughCache(
    `dashboard:ops:queue:${serializeOperationalFilterKey(filters)}`,
    QUEUE_TTL_MS,
    async () => {
      const [snapshot, queue_status_counts] = await Promise.all([
        getOperationalSnapshot(filters),
        getQueueStatusCounts(),
      ]);

      return buildQueueSummary(snapshot, queue_status_counts);
    }
  );
}

export async function getOpsFeederSnapshot(input = {}) {
  const filters = parseOpsFilters(input);

  return readThroughCache(
    `dashboard:ops:feeder:${serializeFeederFilterKey(filters)}`,
    FEEDER_TTL_MS,
    async () => {
      const options = await getOpsFilterOptions();
      const source_view =
        options.views.find(
          (view) =>
            String(view.view_id || "") === String(filters.source_view_id || "") ||
            lower(view.name) === lower(filters.source_view_name)
        ) || pickDefaultFeederView(options.views);

      if (!source_view?.view_id && !source_view?.name) {
        return {
          generated_at: new Date().toISOString(),
          filters,
          ok: false,
          reason: "no_master_owner_views_available",
          view_options: options.views,
        };
      }

      const feeder_result = await runMasterOwnerOutboundFeeder({
        dry_run: true,
        source_view_id: source_view?.view_id || null,
        source_view_name: source_view?.name || null,
        scan_limit: filters.feeder_scan_limit,
        limit: filters.feeder_limit,
      });

      return {
        generated_at: new Date().toISOString(),
        filters,
        ok: feeder_result?.ok !== false,
        source_view: {
          view_id: source_view?.view_id ?? null,
          name: source_view?.name ?? null,
        },
        raw_items_pulled: feeder_result?.raw_items_pulled ?? 0,
        eligible_owner_count: feeder_result?.eligible_owner_count ?? 0,
        queued_count: feeder_result?.queued_count ?? 0,
        queued_owner_ids: feeder_result?.queued_owner_ids ?? [],
        skip_reason_counts: feeder_result?.skip_reason_counts ?? [],
        deferred_resolution: feeder_result?.deferred_resolution ?? null,
        reason: feeder_result?.reason ?? null,
        view_options: options.views,
      };
    }
  );
}

export default {
  parseOpsFilters,
  getOpsFilterOptions,
  getOpsKpiSnapshot,
  getOpsFeedSnapshot,
  getOpsMapSnapshot,
  getOpsQueueSnapshot,
  getOpsFeederSnapshot,
};
