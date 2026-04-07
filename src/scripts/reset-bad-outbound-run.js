/**
 * reset-bad-outbound-run.js
 *
 * One-shot production reset for the malformed one-word send incident.
 *
 * The problem:
 *   Multiline template bodies were stored in Podio's single-line message-text
 *   field without collapsing newlines first.  Podio kept only the first line,
 *   truncating messages to one-word greetings ("Hi", "Hola", "Ciao").  Those
 *   junk bodies passed validation, were accepted by TextGrid, and the system
 *   treated them as real outreach:
 *     - Master Owner contact_status_2 → "Follow-Up Scheduled"
 *     - next_follow_up_at was set
 *     - Message events with delivery_status "Sent" were created
 *
 *   TextGrid's Content Filter then blocked delivery asynchronously, but the
 *   owner state was never rolled back.
 *
 * What this script does (dry_run = true by default):
 *   1. Scans Send Queue for items in status "Sent" whose message body has
 *      fewer than 3 words (the junk threshold).
 *   2. For each bad queue item, resolves the linked Master Owner.
 *   3. Resets the Master Owner:
 *        - contact_status_2  → cleared (null)
 *        - next_follow_up_at → cleared (null)
 *   4. Marks each bad queue item as "Cancelled" (removes it from history
 *      queries that filter by Queued/Sending/Sent).
 *   5. Prints a full summary of affected records.
 *
 * Usage:
 *   node --experimental-vm-modules src/scripts/reset-bad-outbound-run.js
 *
 *   To actually apply changes:
 *   DRY_RUN=false node --experimental-vm-modules src/scripts/reset-bad-outbound-run.js
 *
 *   To restrict to a specific time window (ISO strings):
 *   SINCE=2026-04-04T00:00:00Z UNTIL=2026-04-06T00:00:00Z DRY_RUN=false node ...
 */

import { fetchAllItems, updateItem } from "@/lib/providers/podio.js";
import {
  MASTER_OWNER_FIELDS,
  updateMasterOwnerItem,
} from "@/lib/podio/apps/master-owners.js";
import APP_IDS from "@/lib/config/app-ids.js";

const DRY_RUN = process.env.DRY_RUN !== "false";

// Optional time-window filter (defaults: last 7 days → today)
const SINCE_ISO =
  process.env.SINCE ||
  new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
const UNTIL_ISO = process.env.UNTIL || new Date().toISOString();

const SINCE_TS = new Date(SINCE_ISO).getTime();
const UNTIL_TS = new Date(UNTIL_ISO).getTime();

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

function clean(value) {
  return String(value ?? "").trim();
}

function getTextValue(item, external_id, fallback = "") {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  if (!field?.values?.length) return fallback;
  return String(field.values[0]?.value ?? fallback);
}

function getCategoryValue(item, external_id, fallback = null) {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  if (!field?.values?.length) return fallback;
  return String(field.values[0]?.value?.text ?? fallback);
}

function getFirstAppReferenceId(item, external_id, fallback = null) {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  if (!field?.values?.length) return fallback;
  return field.values[0]?.value?.item_id ?? fallback;
}

function getDateStart(item, external_id) {
  const field = item?.fields?.find((f) => f.external_id === external_id);
  if (!field?.values?.length) return null;
  return field.values[0]?.start ?? field.values[0]?.value?.start ?? null;
}

function wordCount(text) {
  return clean(text).split(/\s+/).filter(Boolean).length;
}

function isJunkBody(message_text) {
  return wordCount(message_text) < 3;
}

function isInWindow(item) {
  // Use sent-at date when available, fall back to item creation date.
  const sent_at_raw = getDateStart(item, "sent-at");
  if (sent_at_raw) {
    const ts = new Date(sent_at_raw).getTime();
    return !isNaN(ts) && ts >= SINCE_TS && ts <= UNTIL_TS;
  }
  // If no sent-at, use item_id heuristic (skip)
  return true;
}

// ------------------------------------------------------------------
// Core
// ------------------------------------------------------------------

async function findBadQueueItems() {
  console.log(
    `[reset] Scanning Send Queue for "Sent" items with junk bodies (window: ${SINCE_ISO} → ${UNTIL_ISO})…`
  );

  const all_sent = await fetchAllItems(
    APP_IDS.send_queue,
    { "queue-status": ["Sent"] },
    { page_size: 500 }
  );

  console.log(`[reset] Total "Sent" queue items fetched: ${all_sent.length}`);

  const bad = all_sent.filter((item) => {
    const body = getTextValue(item, "message-text", "");
    return isJunkBody(body) && isInWindow(item);
  });

  console.log(`[reset] Bad (junk body) items in window: ${bad.length}`);
  return bad;
}

async function resetOwner(master_owner_id) {
  if (DRY_RUN) {
    console.log(
      `  [DRY RUN] Would clear contact_status_2 + next_follow_up_at for owner ${master_owner_id}`
    );
    return;
  }

  await updateMasterOwnerItem(master_owner_id, {
    [MASTER_OWNER_FIELDS.contact_status_2]: null,
    [MASTER_OWNER_FIELDS.next_follow_up_at]: null,
  });

  console.log(`  [RESET] Cleared owner ${master_owner_id}`);
}

async function cancelQueueItem(queue_item_id) {
  if (DRY_RUN) {
    console.log(
      `  [DRY RUN] Would mark queue item ${queue_item_id} as Cancelled`
    );
    return;
  }

  await updateItem(queue_item_id, {
    "queue-status": "Cancelled",
    "failed-reason": "Content Filter",
    "delivery-confirmed": "❌ Failed",
  });

  console.log(`  [RESET] Cancelled queue item ${queue_item_id}`);
}

async function run() {
  console.log("");
  console.log("=== reset-bad-outbound-run ===");
  console.log(`DRY_RUN: ${DRY_RUN}`);
  console.log(`Window : ${SINCE_ISO} → ${UNTIL_ISO}`);
  console.log("");

  const bad_items = await findBadQueueItems();

  if (bad_items.length === 0) {
    console.log("[reset] Nothing to do.");
    return;
  }

  // Collect unique master owner IDs
  const owner_id_set = new Set();
  for (const item of bad_items) {
    const owner_id = getFirstAppReferenceId(item, "master-owner", null);
    if (owner_id) owner_id_set.add(String(owner_id));
  }

  console.log(
    `[reset] Unique Master Owners affected: ${owner_id_set.size}`
  );
  console.log("");

  // Print the full list for audit
  console.log("--- Affected queue items ---");
  for (const item of bad_items) {
    const body = getTextValue(item, "message-text", "");
    const owner_id = getFirstAppReferenceId(item, "master-owner", null);
    const status = getCategoryValue(item, "queue-status", null);
    console.log(
      `  Queue ${item.item_id} | owner=${owner_id} | status=${status} | body="${body}" (${wordCount(body)} words)`
    );
  }
  console.log("");

  // Step 1: Reset each owner (deduplicated)
  console.log("--- Resetting Master Owners ---");
  for (const owner_id of owner_id_set) {
    await resetOwner(Number(owner_id));
  }
  console.log("");

  // Step 2: Cancel each bad queue item
  console.log("--- Cancelling bad queue items ---");
  for (const item of bad_items) {
    await cancelQueueItem(item.item_id);
  }
  console.log("");

  console.log(
    `[reset] Done. ${owner_id_set.size} owner(s) reset, ${bad_items.length} queue item(s) cancelled.`
  );
  if (DRY_RUN) {
    console.log(
      "[reset] This was a DRY RUN — no changes were written to Podio."
    );
    console.log(
      "[reset] Re-run with DRY_RUN=false to apply."
    );
  }
}

run().catch((err) => {
  console.error("[reset] Fatal error:", err);
  process.exit(1);
});
