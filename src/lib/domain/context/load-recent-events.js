// ─── load-recent-events.js ───────────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";

import {
  fetchAllItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";

const DEFAULT_LIMIT = 10;

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
    phone_item_id: getFirstAppReferenceId(item, "phone-number", null),
    textgrid_number_item_id: getFirstAppReferenceId(item, "textgrid-number", null),
    master_owner_id: getFirstAppReferenceId(item, "master-owner", null),
    prospect_id: getFirstAppReferenceId(item, "linked-seller", null),
    property_id: getFirstAppReferenceId(item, "property", null),
    raw: item,
  };
}

export async function loadRecentEvents({
  phone_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  limit = DEFAULT_LIMIT,
} = {}) {
  const batches = [];

  if (phone_item_id) {
    batches.push(
      fetchAllItems(
        APP_IDS.message_events,
        { "phone-number": phone_item_id },
        { page_size: Math.max(limit, 25) }
      )
    );
  }

  if (master_owner_id) {
    batches.push(
      fetchAllItems(
        APP_IDS.message_events,
        { "master-owner": master_owner_id },
        { page_size: Math.max(limit, 25) }
      )
    );
  }

  if (prospect_id) {
    batches.push(
      fetchAllItems(
        APP_IDS.message_events,
        { "linked-seller": prospect_id },
        { page_size: Math.max(limit, 25) }
      )
    );
  }

  if (!batches.length) {
    return {
      ok: true,
      count: 0,
      events: [],
    };
  }

  const results = await Promise.all(batches);
  const all_items = results.flat().filter(Boolean);

  const seen = new Set();
  const deduped = all_items.filter((item) => {
    const key = item?.item_id;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });

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
