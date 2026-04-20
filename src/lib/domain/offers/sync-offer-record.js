/**
 * sync-offer-record.js
 *
 * Creates or updates a Podio Offers app record immediately after the
 * automation sends an SMS message that contains a real cash offer price
 * from the property_cash_offer_snapshots table.
 *
 * Design rules enforced here:
 *
 *   1. Only runs when queue_row.cash_offer_snapshot_id is set.
 *      That field is populated at queue-creation time when a real cash offer
 *      number is present in the outbound message.
 *
 *   2. Stage 1 / ownership_check messages NEVER create an Offer record.
 *      The guard is: use_case_template === "ownership_check".
 *
 *   3. Multifamily / creative deals are routed to the Podio Underwriting app
 *      upstream (via transfer-to-underwriting.js).  They never have a
 *      cash_offer_snapshot_id on the queue row, so this module silently skips.
 *
 *   4. This function NEVER throws.  All failures are caught, logged via warn(),
 *      the send_queue row is marked offer_record_sync_status='failed', and a
 *      Discord critical alert is fired.  The inbound/send pipeline continues.
 *
 *   5. On success, the send_queue row is updated with offer_podio_item_id and
 *      offer_record_sync_status='synced'.  The snapshot row's podio_offer_item_id
 *      is also back-filled.
 *
 * Exported API:
 *   shouldSyncOfferRecord(queue_row) → boolean
 *   buildOfferPayload({ queue_row, snapshot, outbound_event_id?, now? }) → fields
 *   syncOfferRecord({ queue_row, outbound_event_id?, now? })
 *     → { ok, skipped, created, updated, offer_item_id?, diagnostics }
 *
 * Test injection:
 *   __setSyncOfferRecordDeps / __resetSyncOfferRecordDeps
 */

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import {
  OFFER_FIELDS,
  createOfferItem,
  updateOfferItem,
  findLatestOpenOfferByPropertyId,
  findLatestOpenOfferByMasterOwnerId,
} from "@/lib/podio/apps/offers.js";
import { updateMessageEvent } from "@/lib/podio/apps/message-events.js";
import { sendCriticalAlert } from "@/lib/alerts/discord.js";
import { warn } from "@/lib/logging/logger.js";

const TABLE_QUEUE     = "send_queue";
const TABLE_SNAPSHOTS = "property_cash_offer_snapshots";
const MESSAGE_EVENT_SYNC_NOTE_FIELD = "ai-output";
const OFFER_SYNC_FAILURE_PREFIX = "offer_record_sync_failed";

// ---------------------------------------------------------------------------
// Test dependency injection
// ---------------------------------------------------------------------------

let _deps = {
  supabase_override:          null,
  create_offer_item:          null,
  update_offer_item:          null,
  find_offer_by_property:     null,
  find_offer_by_master_owner: null,
  update_message_event:       null,
  send_critical_alert:        null,
};

/** Override production dependencies for unit tests. */
export function __setSyncOfferRecordDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

/** Reset all injected dependencies. */
export function __resetSyncOfferRecordDeps() {
  _deps = {
    supabase_override:          null,
    create_offer_item:          null,
    update_offer_item:          null,
    find_offer_by_property:     null,
    find_offer_by_master_owner: null,
    update_message_event:       null,
    send_critical_alert:        null,
  };
}

function getDb()           { return _deps.supabase_override         ?? defaultSupabase; }
function getCreate()       { return _deps.create_offer_item         ?? createOfferItem; }
function getUpdate()       { return _deps.update_offer_item         ?? updateOfferItem; }
function getFindByProp()   { return _deps.find_offer_by_property    ?? findLatestOpenOfferByPropertyId; }
function getFindByOwner()  { return _deps.find_offer_by_master_owner ?? findLatestOpenOfferByMasterOwnerId; }
function getUpdateEvent()  { return _deps.update_message_event       ?? updateMessageEvent; }
function getAlert()        { return _deps.send_critical_alert        ?? sendCriticalAlert; }

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function clean(value) {
  return String(value ?? "").trim();
}

function safeNum(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toRef(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? [Math.trunc(n)] : undefined;
}

function formatCurrency(value) {
  const n = safeNum(value);
  if (n === null) return "N/A";
  return `$${n.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function nowIso() {
  return new Date().toISOString();
}

function includesAny(text = "", keywords = []) {
  const hay = clean(text).toLowerCase();
  return keywords.some((kw) => hay.includes(clean(kw).toLowerCase()));
}

function getQueueUseCase(queue_row = {}) {
  return clean(
    queue_row.use_case_template ||
      queue_row.metadata?.selected_use_case ||
      queue_row.metadata?.template_use_case ||
      queue_row.metadata?.use_case ||
      ""
  ).toLowerCase();
}

function getQueueMessageBody(queue_row = {}) {
  return clean(queue_row?.message_body || queue_row?.message_text || "");
}

function extractNumericTokens(message = "") {
  const text = String(message || "");
  const matches = text.match(/\$?\s*-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|\$?\s*-?\d+(?:\.\d+)?/g) || [];
  return matches
    .map((raw) => Number(String(raw).replace(/[$,\s]/g, "")))
    .filter((n) => Number.isFinite(n));
}

function messageContainsOfferAmount(message = "", offer_amount = null) {
  const target = safeNum(offer_amount);
  if (target === null) return false;

  const rounded_target = Math.round(target);
  return extractNumericTokens(message).some((candidate) => {
    const rounded_candidate = Math.round(candidate);
    return rounded_candidate === rounded_target || Math.abs(candidate - target) < 0.01;
  });
}

function looksLikeOfferMessage(message = "") {
  const body = clean(message);
  if (!body) return false;
  if (!includesAny(body, ["offer", "cash", "$", "price", "number"])) return false;
  return extractNumericTokens(body).some((n) => Math.abs(n) >= 10000);
}

function isUnderwritingUseCase(queue_row = {}) {
  const use_case = getQueueUseCase(queue_row);
  if (!use_case) return false;

  return includesAny(use_case, [
    "underwriting",
    "multifamily",
    "mf_",
    "creative",
    "subject_to",
    "subject-to",
    "seller_finance",
    "owner_finance",
    "novation",
    "lease_option",
  ]);
}

/** Returns current time as "YYYY-MM-DD HH:MM:SS" in America/Chicago. */
function toCentral(iso = nowIso()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false,
  }).formatToParts(new Date(iso));
  const get = (t) => parts.find((p) => p.type === t)?.value ?? "00";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

// ---------------------------------------------------------------------------
// Guard
// ---------------------------------------------------------------------------

/**
 * Returns true if this queued send should create/update a Podio Offer record.
 *
 * Conditions (ALL must hold):
 *   1. queue_row.cash_offer_snapshot_id is set — a real cash offer was in the message.
 *   2. The message use_case is NOT "ownership_check" (Stage 1 first-touch).
 *
 * Multifamily / creative deals are pre-screened upstream; they are never
 * assigned a cash_offer_snapshot_id, so condition 1 already blocks them.
 */
export function shouldSyncOfferRecord(queue_row = {}) {
  const use_case = getQueueUseCase(queue_row);

  if (use_case === "ownership_check") return false;
  if (isUnderwritingUseCase(queue_row)) return false;

  // Primary path: queue row already linked to a snapshot at queue time.
  if (queue_row?.cash_offer_snapshot_id) return true;

  // Fallback path: message contains what looks like a real offer amount.
  return looksLikeOfferMessage(getQueueMessageBody(queue_row));
}

// ---------------------------------------------------------------------------
// Payload builder
// ---------------------------------------------------------------------------

/**
 * Build the Podio Offers field map from a normalized send_queue row
 * and a property_cash_offer_snapshots snapshot object.
 *
 * Fields that hit standard OFFER_FIELDS columns are mapped directly.
 * Financial details, diagnostics, and message_text are written to `notes`
 * since the Offers app has no dedicated columns for them.
 *
 * @param {{
 *   queue_row?: object,
 *   snapshot?: object,
 *   outbound_event_id?: number|null,
 *   now?: string,
 * }}
 * @returns {object} — Podio field map
 */
export function buildOfferPayload({
  queue_row         = {},
  snapshot          = {},
  outbound_event_id = null,
  now               = nowIso(),
} = {}) {
  const cash_offer            = safeNum(snapshot?.cash_offer);
  const repair_estimate       = safeNum(snapshot?.repair_estimate);
  const estimated_value       = safeNum(snapshot?.estimated_value);
  const calculated_value      = safeNum(snapshot?.calculated_value);
  const estimated_equity      = safeNum(snapshot?.estimated_equity);
  const est_mortgage_balance  = safeNum(snapshot?.estimated_mortgage_balance);
  const est_mortgage_payment  = safeNum(snapshot?.estimated_mortgage_payment);

  const address        = clean(queue_row?.property_address || snapshot?.property_address || "");
  const offer_label    = cash_offer !== null ? formatCurrency(cash_offer) : "Cash Offer";
  const sent_central   = toCentral(now);

  // ---------- structured notes block ----------
  const notes_lines = [
    "Offer Source: SMS Automation",
    `Sent At: ${sent_central}`,
    "",
    "=== Financials ===",
    `Offer Amount: ${formatCurrency(cash_offer)}`,
    repair_estimate      !== null ? `Repair Estimate: ${formatCurrency(repair_estimate)}` : null,
    estimated_value      !== null ? `Estimated Value: ${formatCurrency(estimated_value)}` : null,
    calculated_value     !== null ? `Calculated Value: ${formatCurrency(calculated_value)}` : null,
    estimated_equity     !== null ? `Estimated Equity: ${formatCurrency(estimated_equity)}` : null,
    est_mortgage_balance !== null ? `Est. Mortgage Balance: ${formatCurrency(est_mortgage_balance)}` : null,
    est_mortgage_payment !== null ? `Est. Mortgage Payment: ${formatCurrency(est_mortgage_payment)}/mo` : null,
    "",
    "=== Source Diagnostics ===",
    queue_row?.id                     ? `Queue Row ID: ${queue_row.id}`                                         : null,
    outbound_event_id                 ? `Message Event ID: ${outbound_event_id}`                               : null,
    queue_row?.template_id            ? `Template ID: ${queue_row.template_id}`                                : null,
    queue_row?.cash_offer_snapshot_id ? `Snapshot ID (Supabase): ${queue_row.cash_offer_snapshot_id}`         : null,
    snapshot?.version  != null        ? `Snapshot Version: ${snapshot.version}`                               : null,
    snapshot?.offer_source            ? `Snapshot Source: ${snapshot.offer_source}`                           : null,
    queue_row?.use_case_template      ? `Use Case: ${queue_row.use_case_template}`                             : null,
    queue_row?.current_stage          ? `Stage: ${queue_row.current_stage}`                                    : null,
    queue_row?.metadata?.route_reason ? `Route Reason: ${queue_row.metadata.route_reason}`                     : null,
  ].filter(Boolean);

  const message_body = clean(queue_row?.message_body || queue_row?.message_text || "");
  if (message_body) {
    notes_lines.push("", "=== Message Text ===", message_body.slice(0, 500));
  }

  const notes = notes_lines.join("\n");

  // ---------- core fields ----------
  const fields = {};

  fields[OFFER_FIELDS.title] = address
    ? `SMS Offer — ${address} — ${offer_label}`
    : `SMS Offer — ${offer_label}`;

  fields[OFFER_FIELDS.offer_status] = "Offer Sent";
  fields[OFFER_FIELDS.offer_type]   = "Cash";
  fields[OFFER_FIELDS.offer_date]   = sent_central;
  fields[OFFER_FIELDS.notes]        = notes;

  if (cash_offer !== null) {
    fields[OFFER_FIELDS.offer_sent_price] = cash_offer;
  }

  // ---------- relationship fields ----------
  const master_owner_ref = toRef(
    queue_row?.master_owner_id ?? snapshot?.master_owner_id ?? null
  );
  if (master_owner_ref) fields[OFFER_FIELDS.master_owner] = master_owner_ref;

  const property_ref = toRef(
    queue_row?.property_id ?? snapshot?.podio_property_item_id ?? null
  );
  if (property_ref) fields[OFFER_FIELDS.property] = property_ref;

  const prospect_ref = toRef(queue_row?.prospect_id ?? null);
  if (prospect_ref) fields[OFFER_FIELDS.prospect] = prospect_ref;

  const market_ref = toRef(queue_row?.market_id ?? null);
  if (market_ref) fields[OFFER_FIELDS.market] = market_ref;

  const agent_ref = toRef(queue_row?.sms_agent_id ?? null);
  if (agent_ref) fields[OFFER_FIELDS.assigned_agent] = agent_ref;

  // phone_number — may live in the queue row or its metadata
  const phone_id = Number(
    queue_row?.phone_item_id ??
    queue_row?.metadata?.phone_item_id ??
    null
  );
  if (Number.isFinite(phone_id) && phone_id > 0) {
    fields[OFFER_FIELDS.phone_number] = [phone_id];
  }

  return fields;
}

// ---------------------------------------------------------------------------
// Supabase bookkeeping helpers
// ---------------------------------------------------------------------------

/** Best-effort: mark send_queue row with 'failed' offer sync status. */
async function markSyncFailed({ queue_row_id, error_message, now }) {
  if (!queue_row_id) return;
  try {
    await getDb()
      .from(TABLE_QUEUE)
      .update({
        offer_record_sync_status: "failed",
        offer_record_sync_error:  String(error_message ?? "unknown").slice(0, 500),
        offer_record_synced_at:   now,
      })
      .eq("id", String(queue_row_id));
  } catch (_) { /* suppress secondary errors */ }
}

/** Best-effort: annotate message event when offer sync fails post-send. */
async function markMessageEventSyncFailed({ outbound_event_id, error_message }) {
  if (!outbound_event_id) return;
  try {
    await getUpdateEvent()(Number(outbound_event_id), {
      [MESSAGE_EVENT_SYNC_NOTE_FIELD]: `${OFFER_SYNC_FAILURE_PREFIX}:${String(error_message || "unknown").slice(0, 250)}`,
    });
  } catch (_) { /* suppress secondary errors */ }
}

/** Best-effort: mark send_queue row synced and back-fill snapshot row. */
async function markSyncSuccess({ queue_row_id, snapshot_id, offer_item_id, now }) {
  if (queue_row_id) {
    try {
      await getDb()
        .from(TABLE_QUEUE)
        .update({
          offer_podio_item_id:      offer_item_id,
          cash_offer_snapshot_id:   snapshot_id || null,
          offer_record_sync_status: "synced",
          offer_record_sync_error:  null,
          offer_record_synced_at:   now,
        })
        .eq("id", String(queue_row_id));
    } catch (_) { /* suppress secondary errors */ }
  }

  if (snapshot_id && offer_item_id) {
    try {
      await getDb()
        .from(TABLE_SNAPSHOTS)
        .update({
          podio_offer_item_id: offer_item_id,
          podio_synced_at:     now,
        })
        .eq("id", String(snapshot_id));
    } catch (_) { /* suppress secondary errors */ }
  }
}

// ---------------------------------------------------------------------------
// Internal Podio helpers
// ---------------------------------------------------------------------------

/** Load the cash offer snapshot by UUID. Throws on DB error. */
async function loadSnapshot(snapshot_id) {
  if (!snapshot_id) return null;

  const { data, error } = await getDb()
    .from(TABLE_SNAPSHOTS)
    .select("*")
    .eq("id", String(snapshot_id))
    .maybeSingle();

  if (error) throw error;
  return data ?? null;
}

/**
 * Fallback snapshot resolution when queue rows don't yet carry snapshot UUIDs.
 * Tries: podio_property_item_id -> master_owner_id.
 */
async function loadActiveSnapshotForQueueRow(queue_row = {}) {
  const property_item_id = safeNum(queue_row?.property_id ?? null);
  if (property_item_id !== null) {
    const { data, error } = await getDb()
      .from(TABLE_SNAPSHOTS)
      .select("*")
      .eq("podio_property_item_id", Math.trunc(property_item_id))
      .eq("status", "active")
      .order("version", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) throw error;
    if (data) return data;
  }

  const master_owner_id = safeNum(queue_row?.master_owner_id ?? null);
  if (master_owner_id !== null) {
    const { data, error } = await getDb()
      .from(TABLE_SNAPSHOTS)
      .select("*")
      .eq("master_owner_id", Math.trunc(master_owner_id))
      .eq("status", "active")
      .order("version", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) throw error;
    if (data) return data;
  }

  return null;
}

/**
 * Find the most recent open Offer in Podio linked to this deal.
 * Resolution: property first, then master_owner.
 * Returns null (without throwing) if both lookups fail or return nothing.
 */
async function findExistingOffer({ property_id, master_owner_id }) {
  const by_property = await getFindByProp()(property_id).catch(() => null);
  if (by_property?.item_id) return by_property;

  const by_owner = await getFindByOwner()(master_owner_id).catch(() => null);
  if (by_owner?.item_id) return by_owner;

  return null;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Sync a Podio Offer record after a successful SMS send.
 *
 * This function NEVER throws — errors are caught, logged, and returned as
 * { ok: false }.  The calling send pipeline must continue regardless.
 *
 * @param {{
 *   queue_row:          object  — normalized send_queue row (must have cash_offer_snapshot_id)
 *   outbound_event_id?: number  — Podio message event item_id from the send
 *   now?:               string  — ISO timestamp for tests
 * }}
 * @returns {Promise<{
 *   ok: boolean,
 *   skipped: boolean,
 *   created: boolean,
 *   updated: boolean,
 *   offer_item_id: number|null,
 *   diagnostics: object,
 * }>}
 */
export async function syncOfferRecord({
  queue_row         = {},
  outbound_event_id = null,
  now               = nowIso(),
} = {}) {
  const queue_row_id = queue_row?.id ?? queue_row?.queue_row_id ?? null;
  const provided_snapshot_id  = queue_row?.cash_offer_snapshot_id ?? null;

  const diagnostics = {
    queue_row_id,
    snapshot_id: provided_snapshot_id,
    outbound_event_id,
    use_case: getQueueUseCase(queue_row) || null,
    attempted_at: now,
  };

  // ------------------------------------------------------------------
  // Guard: nothing to sync
  // ------------------------------------------------------------------
  if (!shouldSyncOfferRecord(queue_row)) {
    if (diagnostics.use_case === "ownership_check") {
      diagnostics.skip_reason = "ownership_check_use_case";
    } else if (isUnderwritingUseCase(queue_row)) {
      diagnostics.skip_reason = "underwriting_route";
    } else {
      diagnostics.skip_reason = "no_offer_amount_detected";
    }
    return {
      ok: true, skipped: true,
      created: false, updated: false,
      offer_item_id: null,
      diagnostics,
    };
  }

  try {
    // ------------------------------------------------------------------
    // 1. Load snapshot
    // ------------------------------------------------------------------
    const snapshot = provided_snapshot_id
      ? await loadSnapshot(provided_snapshot_id)
      : await loadActiveSnapshotForQueueRow(queue_row);

    if (!snapshot) {
      diagnostics.skip_reason = "snapshot_not_found";
      return {
        ok: true, skipped: true,
        created: false, updated: false,
        offer_item_id: null,
        diagnostics,
      };
    }

    const resolved_snapshot_id = clean(snapshot?.id || provided_snapshot_id || "") || null;
    diagnostics.snapshot_id = resolved_snapshot_id;
    diagnostics.snapshot_resolution = provided_snapshot_id
      ? "queue_row_snapshot_id"
      : "active_snapshot_lookup";
    diagnostics.cash_offer = snapshot.cash_offer;

    const message_body = getQueueMessageBody(queue_row);
    const has_matching_offer_amount = messageContainsOfferAmount(
      message_body,
      snapshot.cash_offer
    );
    diagnostics.message_contains_offer_amount = has_matching_offer_amount;

    if (!has_matching_offer_amount) {
      diagnostics.skip_reason = "message_missing_snapshot_offer_amount";
      return {
        ok: true, skipped: true,
        created: false, updated: false,
        offer_item_id: null,
        diagnostics,
      };
    }

    const queue_row_with_snapshot = {
      ...queue_row,
      cash_offer_snapshot_id: resolved_snapshot_id,
    };

    // ------------------------------------------------------------------
    // 2. Build Podio payload
    // ------------------------------------------------------------------
    const payload = buildOfferPayload({
      queue_row: queue_row_with_snapshot,
      snapshot,
      outbound_event_id,
      now,
    });

    // ------------------------------------------------------------------
    // 3. Find or create/update Podio Offer
    // ------------------------------------------------------------------
    const existing = await findExistingOffer({
      property_id:     queue_row_with_snapshot?.property_id     ?? snapshot?.podio_property_item_id ?? null,
      master_owner_id: queue_row_with_snapshot?.master_owner_id ?? snapshot?.master_owner_id        ?? null,
    });
    diagnostics.existing_offer_id = existing?.item_id ?? null;

    let offer_item_id;
    let created = false;
    let updated = false;

    if (existing?.item_id) {
      await getUpdate()(existing.item_id, payload);
      offer_item_id = existing.item_id;
      updated = true;
    } else {
      const result = await getCreate()(payload);
      offer_item_id = result?.item_id ?? null;
      created = true;
    }

    // ------------------------------------------------------------------
    // 4. Back-fill IDs in Supabase (best-effort)
    // ------------------------------------------------------------------
    await markSyncSuccess({
      queue_row_id,
      snapshot_id: resolved_snapshot_id,
      offer_item_id,
      now,
    });

    diagnostics.offer_item_id = offer_item_id;

    return {
      ok: true, skipped: false,
      created, updated,
      offer_item_id,
      diagnostics,
    };
  } catch (err) {
    const error_message = String(err?.message ?? err);

    warn("sync_offer_record.failed", {
      error:        error_message,
      queue_row_id,
      snapshot_id: diagnostics.snapshot_id,
    });

    // Mark queue row as failed (best-effort — suppresses further errors)
    await markSyncFailed({ queue_row_id, error_message, now });
    await markMessageEventSyncFailed({ outbound_event_id, error_message });

    // Discord critical alert (best-effort — suppresses further errors)
    await getAlert()({
      title: "Offer Record Sync Failed",
      description:
        "Failed to create/update Podio Offer after SMS send. " +
        "The offer data is preserved in send_queue and property_cash_offer_snapshots for manual recovery.",
      color: 0xFF4444,
      fields: [
        { name: "Queue Row ID",    value: String(queue_row_id ?? "unknown"),                  inline: true  },
        { name: "Snapshot ID",     value: String(diagnostics.snapshot_id ?? "unknown"),       inline: true  },
        { name: "Message Event ID",value: String(outbound_event_id ?? "unknown"),             inline: true  },
        { name: "Master Owner ID", value: String(queue_row?.master_owner_id ?? "unknown"),    inline: true  },
        { name: "Property ID",     value: String(queue_row?.property_id ?? "unknown"),        inline: true  },
        { name: "Error",           value: error_message.slice(0, 250),                        inline: false },
      ],
      timestamp: now,
    }).catch(() => {});

    return {
      ok: false, skipped: false,
      created: false, updated: false,
      offer_item_id: null,
      error: error_message,
      diagnostics: { ...diagnostics, error: error_message },
    };
  }
}
