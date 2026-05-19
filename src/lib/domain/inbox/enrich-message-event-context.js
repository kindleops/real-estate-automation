import { normalizePhone } from "@/lib/providers/textgrid.js";

function clean(value) { return String(value ?? "").trim(); }
function pickFirst(...values) { for (const v of values) { if (v !== null && v !== undefined && clean(v) !== "") return v; } return null; }
function object(value) { return value && typeof value === "object" && !Array.isArray(value) ? value : {}; }
function nowIso() { return new Date().toISOString(); }
function threadKeyFor(direction, from, to) {
  const dir = clean(direction).toLowerCase();
  const from_norm = normalizePhone(from);
  const to_norm = normalizePhone(to);
  if (dir === "inbound") return from_norm || to_norm || null;
  if (dir === "outbound") return to_norm || from_norm || null;
  return from_norm || to_norm || null;
}

async function maybeSingle(query) {
  const { data, error } = await query.maybeSingle();
  if (error) return null;
  return data || null;
}

async function latestFrom(table, supabase, build) {
  try {
    return await maybeSingle(build(supabase.from(table).select("*")));
  } catch { return null; }
}

function fromQueue(row = {}) {
  const metadata = object(row.metadata);
  const snapshot = object(metadata.candidate_snapshot);
  const seller = object(metadata.seller_identity);
  const qctx = object(metadata.queue_context);
  return {
    property_id: pickFirst(row.property_id, snapshot.property_id, qctx.property_id),
    master_owner_id: pickFirst(row.master_owner_id, row.owner_id, snapshot.master_owner_id, qctx.master_owner_id),
    prospect_id: pickFirst(row.prospect_id, snapshot.prospect_id, qctx.prospect_id),
    textgrid_number_id: pickFirst(row.textgrid_number_id, qctx.textgrid_number_id),
    template_id: pickFirst(row.template_id, metadata.selected_template_id, metadata.template_id),
    seller_first_name: pickFirst(row.seller_first_name, metadata.seller_first_name, snapshot.seller_first_name, seller.first_name),
    seller_display_name: pickFirst(row.seller_display_name, metadata.seller_display_name, snapshot.seller_full_name, snapshot.display_name, seller.display_name),
    owner_display_name: pickFirst(snapshot.owner_display_name, snapshot.owner_name, row.seller_display_name),
    owner_type: pickFirst(row.owner_type, snapshot.owner_type),
    property_address: pickFirst(row.property_address, snapshot.property_address, qctx.property_address),
    property_city: pickFirst(row.property_city, snapshot.property_city),
    property_state: pickFirst(row.property_state, snapshot.property_state),
    property_zip: pickFirst(row.property_zip, snapshot.property_zip),
    market: pickFirst(row.market, snapshot.market, metadata.market),
    timezone: pickFirst(row.timezone, snapshot.timezone, metadata.timezone),
    thread_key: pickFirst(row.thread_key, metadata.thread_key),
  };
}

function fromEvent(row = {}) {
  const metadata = object(row.metadata);
  const enrichment = object(metadata.enrichment);
  return {
    property_id: pickFirst(row.property_id, metadata.property_id, enrichment.property_id),
    master_owner_id: pickFirst(row.master_owner_id, metadata.master_owner_id, enrichment.master_owner_id),
    prospect_id: pickFirst(row.prospect_id, metadata.prospect_id),
    textgrid_number_id: pickFirst(row.textgrid_number_id, metadata.textgrid_number_id),
    template_id: pickFirst(row.template_id, metadata.template_id),
    seller_display_name: pickFirst(row.seller_display_name, enrichment.seller_name),
    owner_display_name: pickFirst(row.owner_display_name, enrichment.seller_name),
    owner_type: pickFirst(row.owner_type),
    property_address: pickFirst(row.property_address, enrichment.property_address),
    market: pickFirst(row.market, enrichment.market),
    timezone: pickFirst(row.timezone, enrichment.timezone),
    thread_key: pickFirst(row.thread_key, enrichment.thread_key),
  };
}

function merge(...sources) {
  const out = {};
  for (const source of sources) for (const [k, v] of Object.entries(source || {})) if (out[k] === undefined || out[k] === null || clean(out[k]) === "") out[k] = v ?? null;
  return out;
}

export async function enrichMessageEventContext(eventOrPayload = {}, supabase) {
  const event = object(eventOrPayload);
  const metadata = object(event.metadata);
  const queueId = pickFirst(event.queue_id, event.queue_item_id, event.source_queue_id, metadata.queue_id, metadata.queue_item_id);
  const providerSid = pickFirst(event.provider_message_sid, event.provider_message_id, event.message_id, metadata.provider_message_id);
  const from = pickFirst(event.from_phone_number, event.from, metadata.inbound_from);
  const to = pickFirst(event.to_phone_number, event.to, metadata.inbound_to);
  const base = fromEvent(event);
  const sources = [];
  let source = "event";

  if (queueId) {
    const row = await latestFrom("send_queue", supabase, (q) => q.eq("id", queueId).order("created_at", { ascending: false }).limit(1));
    if (row) { sources.push(fromQueue(row)); source = "send_queue.queue_id"; }
  }
  if (providerSid) {
    const row = await latestFrom("send_queue", supabase, (q) => q.or(`provider_message_id.eq.${providerSid},textgrid_message_id.eq.${providerSid}`).order("created_at", { ascending: false }).limit(1));
    if (row) { sources.push(fromQueue(row)); source = source === "event" ? "send_queue.provider_message" : source; }
  }
  if (from && to) {
    const a = normalizePhone(from); const b = normalizePhone(to);
    const queueRow = await latestFrom("send_queue", supabase, (q) => q.or(`and(from_phone_number.eq.${a},to_phone_number.eq.${b}),and(from_phone_number.eq.${b},to_phone_number.eq.${a})`).order("created_at", { ascending: false }).limit(1));
    if (queueRow) { sources.push(fromQueue(queueRow)); source = source === "event" ? "send_queue.phone_pair" : source; }
    const eventRow = await latestFrom("message_events", supabase, (q) => q.or(`and(from_phone_number.eq.${a},to_phone_number.eq.${b}),and(from_phone_number.eq.${b},to_phone_number.eq.${a})`).order("created_at", { ascending: false }).limit(1));
    if (eventRow) { sources.push(fromEvent(eventRow)); source = source === "event" ? "message_events.phone_pair" : source; }
  }

  let enriched = merge(base, ...sources);
  if (enriched.property_id) {
    const property = await latestFrom("properties", supabase, (q) => q.eq("id", enriched.property_id).limit(1));
    if (property) enriched = merge(enriched, { property_address: property.address || property.property_address, property_city: property.city, property_state: property.state, property_zip: property.zip, market: property.market, timezone: property.timezone, latitude: property.latitude, longitude: property.longitude });
  }
  if (enriched.master_owner_id) {
    const owner = await latestFrom("master_owners", supabase, (q) => q.eq("id", enriched.master_owner_id).limit(1));
    if (owner) enriched = merge(enriched, { seller_display_name: owner.seller_display_name || owner.owner_display_name || owner.name, owner_display_name: owner.owner_display_name || owner.name, owner_type: owner.owner_type });
  }

  // Force canonical thread_key: outbound = normalizePhone(to), inbound = normalizePhone(from).
  // Never inherit null/composite/pipe thread keys from legacy enrichment sources.
  const canonical_thread_key = threadKeyFor(event.direction, from, to);
  enriched.thread_key = canonical_thread_key || null;
  enriched.enrichment_source = source;
  enriched.enriched_at = nowIso();
  enriched.metadata = {
    ...metadata,
    enrichment: {
      ...(object(metadata.enrichment)),
      source,
      enriched_at: enriched.enriched_at,
      thread_key: canonical_thread_key || null,
      property_id: enriched.property_id || null,
      master_owner_id: enriched.master_owner_id || null,
      seller_name: pickFirst(enriched.seller_display_name, enriched.owner_display_name, enriched.seller_first_name),
      property_address: enriched.property_address || null,
      market: enriched.market || null,
      timezone: enriched.timezone || null,
    },
  };
  return enriched;
}

export function buildMessageEventEnrichmentUpdate(enrichment = {}) {
  return {
    thread_key: enrichment.thread_key || null,
    property_id: enrichment.property_id || null,
    master_owner_id: enrichment.master_owner_id || null,
    prospect_id: enrichment.prospect_id || null,
    textgrid_number_id: enrichment.textgrid_number_id || null,
    template_id: enrichment.template_id || null,
    seller_display_name: enrichment.seller_display_name || enrichment.owner_display_name || null,
    property_address: enrichment.property_address || null,
    market: enrichment.market || null,
    metadata: enrichment.metadata || { enrichment },
    updated_at: enrichment.enriched_at || nowIso(),
  };
}
