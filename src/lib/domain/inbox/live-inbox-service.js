import { supabase as defaultSupabase, hasSupabaseConfig } from "@/lib/supabase/client.js";
import { classifyInboxMessage, findMatchedKeywords, KEYWORD_GROUPS } from "@/lib/domain/inbox/keywords.js";

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 500;

function clean(value) { return String(value ?? "").trim(); }
function lower(value) { return clean(value).toLowerCase(); }
function int(value, fallback) { const n = Number(value); return Number.isFinite(n) && n > 0 ? Math.min(Math.trunc(n), MAX_LIMIT) : fallback; }
function bool(value) { return ["1", "true", "yes", "on"].includes(lower(value)); }
function object(value) { return value && typeof value === "object" && !Array.isArray(value) ? value : {}; }
function asTime(value) { const t = new Date(value || 0).getTime(); return Number.isFinite(t) ? t : 0; }
function latestAt(row = {}) { return row.latest_activity_at || row.event_timestamp || row.received_at || row.sent_at || row.created_at || row.updated_at || null; }
function cursorFor(row = {}) { return Buffer.from(JSON.stringify({ t: latestAt(row), id: row.id || row.message_event_key || row.provider_message_sid || "" })).toString("base64url"); }
function parseCursor(cursor) { try { return JSON.parse(Buffer.from(clean(cursor), "base64url").toString("utf8")); } catch { return null; } }
function normalizeDirection(value) { const d = lower(value); if (d.startsWith("in")) return "inbound"; if (d.startsWith("out")) return "outbound"; return d || null; }
function msgId(row) { return row.id || row.message_event_key || row.provider_message_sid || row.provider_message_id || null; }
function threadKey(row = {}) {
  const meta = object(row.metadata);
  const enrichment = object(meta.enrichment);
  if (row.thread_key || enrichment.thread_key) return row.thread_key || enrichment.thread_key;
  const phones = [row.from_phone_number, row.to_phone_number].map(clean).filter(Boolean).sort().join(":");
  return [row.property_id || row.master_owner_id || "unknown", phones || "no_phone"].join(":");
}
function displayName(row = {}) { return row.seller_display_name || object(row.metadata)?.enrichment?.seller_name || object(row.metadata)?.seller_display_name || null; }

export function applyInboxRowComputedFields(row = {}, query = {}) {
  const keywordGroups = [];
  if (query.keyword_group && KEYWORD_GROUPS[lower(query.keyword_group)]) keywordGroups.push(lower(query.keyword_group));
  const searchTerms = clean(query.q) ? clean(query.q).split(/\s+/).filter(Boolean) : [];
  const groupMatches = keywordGroups.length ? findMatchedKeywords(row.message_body || "", keywordGroups) : [];
  const searchMatches = searchTerms.length ? findMatchedKeywords(row.message_body || "", searchTerms) : [];
  const flags = classifyInboxMessage(row);
  return {
    ...row,
    id: msgId(row),
    direction: normalizeDirection(row.direction),
    thread_key: threadKey(row),
    latest_activity_at: latestAt(row),
    seller_display_name: displayName(row),
    property_address: row.property_address || object(row.metadata)?.enrichment?.property_address || null,
    market: row.market || object(row.metadata)?.enrichment?.market || null,
    flags,
    matched_keywords: [...new Set([...flags.matched_keywords, ...groupMatches.map((m) => m.term), ...searchMatches.map((m) => m.term)])],
    highlight_ranges: [...groupMatches, ...searchMatches].map(({ start, end, term }) => ({ start, end, term })),
  };
}

function rowMatchesFilter(row = {}, filter = "all") {
  const f = lower(filter) || "all";
  const dir = normalizeDirection(row.direction);
  const flags = row.flags || classifyInboxMessage(row);
  const ar = lower(row.auto_reply_status || object(row.metadata).auto_reply_status);
  if (f === "all") return true;
  if (f === "inbound_only") return dir === "inbound";
  if (f === "outbound_only") return dir === "outbound";
  if (f === "needs_reply") return dir === "inbound" && !["queued", "sent", "suppressed"].includes(ar);
  if (f === "auto_replied") return ["queued", "sent", "delivered"].includes(ar);
  if (f === "auto_reply_failed") return ["failed", "error"].includes(ar);
  if (f === "positive_hot") return flags.positive_hot;
  if (f === "offer_requested") return flags.offer_requested;
  if (f === "wrong_number") return flags.wrong_number;
  if (f === "opt_out") return flags.opt_out;
  if (f === "missing_context") return !row.property_id && !row.master_owner_id && !object(row.metadata)?.enrichment?.property_id;
  if (f === "manual_review") return flags.manual_review || ar === "manual_review";
  return true;
}

function buildThreads(messages = []) {
  const byThread = new Map();
  for (const message of messages) {
    const key = message.thread_key;
    const existing = byThread.get(key) || { thread_key: key, messages: [], message_count: 0 };
    existing.messages.push(message.id);
    existing.message_count += 1;
    if (!existing.latest_activity_at || asTime(message.latest_activity_at) > asTime(existing.latest_activity_at)) {
      Object.assign(existing, {
        latest_activity_at: message.latest_activity_at,
        latest_message_direction: message.direction,
        latest_message_body: message.message_body || null,
        property_id: message.property_id || existing.property_id || null,
        master_owner_id: message.master_owner_id || existing.master_owner_id || null,
        seller_display_name: message.seller_display_name || existing.seller_display_name || null,
        property_address: message.property_address || existing.property_address || null,
        market: message.market || existing.market || null,
        needs_reply: rowMatchesFilter(message, "needs_reply"),
        positive_hot: Boolean(message.flags?.positive_hot || existing.positive_hot),
        auto_reply_status: message.auto_reply_status || existing.auto_reply_status || null,
      });
    }
    byThread.set(key, existing);
  }
  return [...byThread.values()].sort((a, b) => asTime(b.latest_activity_at) - asTime(a.latest_activity_at));
}

function buildCounts(rows = []) {
  const counts = { all: rows.length, inbound_only: 0, outbound_only: 0, needs_reply: 0, auto_replied: 0, auto_reply_failed: 0, positive_hot: 0, offer_requested: 0, wrong_number: 0, opt_out: 0, missing_context: 0, manual_review: 0 };
  for (const row of rows) for (const key of Object.keys(counts)) if (key !== "all" && rowMatchesFilter(row, key)) counts[key] += 1;
  return counts;
}

async function loadMapPins(supabase, query, filteredRows = []) {
  const filteredPropertyIds = [...new Set(filteredRows.map((r) => r.property_id).filter(Boolean))];
  let q = supabase.from("properties").select("id,property_id,latitude,longitude,address,property_address,market,lead_status,stage,seller_name,master_owner_id").not("latitude", "is", null).not("longitude", "is", null).limit(10000);
  if (bool(query.filtered_pins) && filteredPropertyIds.length) q = q.in("id", filteredPropertyIds);
  const { data, error } = await q;
  if (error) return [];
  const latestByProperty = new Map();
  for (const row of filteredRows) if (row.property_id && (!latestByProperty.has(String(row.property_id)) || asTime(row.latest_activity_at) > asTime(latestByProperty.get(String(row.property_id)).latest_activity_at))) latestByProperty.set(String(row.property_id), row);
  return (Array.isArray(data) ? data : []).map((p) => {
    const id = p.id || p.property_id;
    const latest = latestByProperty.get(String(id)) || {};
    return { property_id: id, thread_key: latest.thread_key || null, latitude: Number(p.latitude), longitude: Number(p.longitude), address: p.address || p.property_address, market: p.market || latest.market || null, seller_name: p.seller_name || latest.seller_display_name || null, lead_status: p.lead_status || p.stage || null, stage: p.stage || latest.stage_after || latest.stage_before || null, latest_message_direction: latest.direction || null, latest_message_at: latest.latest_activity_at || null, needs_reply: latest.id ? rowMatchesFilter(latest, "needs_reply") : false, positive_hot: Boolean(latest.flags?.positive_hot), auto_reply_status: latest.auto_reply_status || null };
  });
}

export async function getLiveInbox(params = {}, deps = {}) {
  if (!deps.supabase && !hasSupabaseConfig()) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  const supabase = deps.supabase || defaultSupabase;
  const limit = int(params.limit, DEFAULT_LIMIT);
  const direction = lower(params.direction || "all");
  const cursor = parseCursor(params.cursor);
  const wantsMap = bool(params.map);
  let scanLimit = Math.max(limit * 4, 500);
  if (params.filter && params.filter !== "all") scanLimit = Math.max(scanLimit, 2000);

  let q = supabase.from("inbox_chat_timeline_hydrated").select("*").order("event_timestamp", { ascending: false }).limit(scanLimit);
  if (direction === "inbound" || direction === "outbound") q = q.eq("direction", direction);
  if (cursor?.t) q = q.lt("event_timestamp", cursor.t);
  if (clean(params.q)) q = q.ilike("message_body", `%${clean(params.q)}%`);
  const { data, error } = await q;
  if (error) throw error;
  let rows = (Array.isArray(data) ? data : []).map((row) => applyInboxRowComputedFields(row, params));
  if (params.keyword_group) rows = rows.filter((row) => findMatchedKeywords(row.message_body || "", [params.keyword_group]).length > 0);
  rows = rows.filter((row) => rowMatchesFilter(row, params.filter || "all"));
  rows.sort((a, b) => asTime(b.latest_activity_at) - asTime(a.latest_activity_at));
  const pageRows = rows.slice(0, limit);
  const mapPins = wantsMap ? await loadMapPins(supabase, params, rows) : [];
  return { threads: buildThreads(pageRows), messages: pageRows, counts: buildCounts(rows), mapPins, pagination: { limit, returned: pageRows.length, has_more: rows.length > limit || (Array.isArray(data) && data.length === scanLimit), next_cursor: pageRows.length ? cursorFor(pageRows[pageRows.length - 1]) : null } };
}
