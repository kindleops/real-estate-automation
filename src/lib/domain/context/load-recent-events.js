// ─── load-recent-events.js ───────────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  filterAppItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";
import { parseMessageEventMetadata } from "@/lib/domain/events/message-event-metadata.js";

const DEFAULT_LIMIT = 10;
const MESSAGE_EVENT_CACHE_TTL_MS = 15_000;

function toTimestamp(value) {
  if (!value) return 0;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? 0 : ts;
}

function sortByTimestampDesc(items = []) {
  return [...items].sort((a, b) => {
    const aTs = toTimestamp(getDateValue(a, "timestamp", null));
    const bTs = toTimestamp(getDateValue(b, "timestamp", null));
    return bTs - aTs;
  });
}

function normalizeMessageEvent(item) {
  const metadata = parseMessageEventMetadata(item);

  return {
    item_id: item?.item_id ?? null,
    message_id: getTextValue(item, "message-id", ""),
    direction: getCategoryValue(item, "direction", null),
    timestamp: getDateValue(item, "timestamp", null),
    message: getTextValue(item, "message", ""),
    delivery_status: getCategoryValue(item, "status-3", null),
    raw_carrier_status: getTextValue(item, "status-2", null),
    failure_bucket: getCategoryValue(item, "failure-bucket", null),
    processed_by: getCategoryValue(item, "processed-by", null),
    source_app: getCategoryValue(item, "source-app", null),
    trigger_name: getTextValue(item, "trigger-name", null),
    message_variant:
      getNumberValue(item, "message-variant", null) ??
      metadata?.message_variant ??
      null,
    phone_item_id: getFirstAppReferenceId(item, "phone-number", null),
    textgrid_number_item_id: getFirstAppReferenceId(item, "textgrid-number", null),
    master_owner_id: getFirstAppReferenceId(item, "master-owner", null),
    prospect_id: getFirstAppReferenceId(item, "linked-seller", null),
    property_id: getFirstAppReferenceId(item, "property", null),
    market_id: getFirstAppReferenceId(item, "market", null),
    conversation_item_id: getFirstAppReferenceId(item, "conversation", null),
    template_id:
      getFirstAppReferenceId(item, "template-selected", null) ??
      metadata?.template_id ??
      null,
    metadata,
    selected_use_case:
      metadata?.selected_use_case ||
      metadata?.canonical_use_case ||
      null,
    template_use_case: metadata?.template_use_case || null,
    next_expected_stage: metadata?.next_expected_stage || null,
    selected_variant_group: metadata?.selected_variant_group || null,
    selected_tone: metadata?.selected_tone || null,
    raw: item,
  };
}

export async function loadRecentEvents({
  phone_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  limit = DEFAULT_LIMIT,
} = {}, deps = {}) {
  const {
    filterAppItemsImpl = filterAppItems,
  } = deps;
  const queries = [];

  if (phone_item_id) {
    queries.push({
      filters: { "phone-number": phone_item_id },
    });
  }

  if (master_owner_id) {
    queries.push({
      filters: { "master-owner": master_owner_id },
    });
  }

  if (prospect_id) {
    queries.push({
      filters: { "linked-seller": prospect_id },
    });
  }

  if (!queries.length) {
    return {
      ok: true,
      count: 0,
      events: [],
    };
  }

  const seen = new Set();
  const deduped = [];
  const page_size = Math.max(Number(limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);

  for (const query of queries) {
    const response = await filterAppItemsImpl(
      APP_IDS.message_events,
      query.filters,
      {
        limit: page_size,
        offset: 0,
        sort_by: "timestamp",
        sort_desc: true,
        cache_ttl_ms: MESSAGE_EVENT_CACHE_TTL_MS,
      }
    );

    const items = Array.isArray(response?.items) ? response.items : [];

    for (const item of items) {
      const key = item?.item_id;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      deduped.push(item);
    }

    if (deduped.length >= limit) {
      break;
    }
  }

  const events = sortByTimestampDesc(deduped)
    .slice(0, limit)
    .map(normalizeMessageEvent);

  return {
    ok: true,
    count: events.length,
    events,
  };
}

export default loadRecentEvents;
