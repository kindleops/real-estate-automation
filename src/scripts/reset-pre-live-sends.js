/**
 * reset-pre-live-sends.js
 *
 * Resets Master Owner contact state for ALL sends made before the system
 * was fully live and able to auto-respond.  Those old sends set
 * contact_status_2 → "Sent" / "Follow-Up Scheduled" which causes the feeder
 * to classify them as reengagement instead of first-touch.
 *
 * What this script does (DRY_RUN = true by default):
 *   1. Scans Send Queue for items with status "Sent" in the time window.
 *   2. For each, resolves the linked Master Owner.
 *   3. Only resets owners whose contact-status is blank AND
 *      contact-status-2 is "Sent" or "Follow-Up Scheduled"
 *      (i.e. auto-set by the system, not manually engaged).
 *   4. Clears: contact_status_2, next_follow_up_at, last_contacted_at, last_outbound
 *   5. Marks the queue items as "Cancelled" with reason "Pre-Live Reset".
 *
 * Usage:
 *   node --experimental-vm-modules src/scripts/reset-pre-live-sends.js
 *
 *   To actually apply changes:
 *   DRY_RUN=false node --experimental-vm-modules src/scripts/reset-pre-live-sends.js
 *
 *   To customize time window (defaults: all time up to now):
 *   SINCE=2026-01-01T00:00:00Z UNTIL=2026-04-15T23:59:59Z DRY_RUN=false node ...
 */

import { fetchAllItems, updateItem } from "@/lib/providers/podio.js";
import {
  MASTER_OWNER_FIELDS,
  updateMasterOwnerItem,
} from "@/lib/podio/apps/master-owners.js";
import APP_IDS from "@/lib/config/app-ids.js";

const DRY_RUN = process.env.DRY_RUN !== "false";

const SINCE_ISO =
  process.env.SINCE || "2025-01-01T00:00:00Z";
const UNTIL_ISO = process.env.UNTIL || new Date().toISOString();

const SINCE_TS = new Date(SINCE_ISO).getTime();
const UNTIL_TS = new Date(UNTIL_ISO).getTime();

// contact-status-2 values that were auto-set by the send pipeline
const AUTO_SET_STATUSES = new Set(["sent", "follow-up scheduled"]);

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

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

function isInWindow(item) {
  const sent_at_raw = getDateStart(item, "sent-at");
  if (sent_at_raw) {
    const ts = new Date(sent_at_raw).getTime();
    return !isNaN(ts) && ts >= SINCE_TS && ts <= UNTIL_TS;
  }
  return true;
}

function lower(val) {
  return val == null ? null : String(val).toLowerCase().trim();
}

// ------------------------------------------------------------------
// Core
// ------------------------------------------------------------------

async function findSentQueueItems() {
  console.log(
    `[reset] Scanning Send Queue for "Sent" items (window: ${SINCE_ISO} → ${UNTIL_ISO})…`
  );

  const all_sent = await fetchAllItems(
    APP_IDS.send_queue,
    { "queue-status": ["Sent"] },
    { page_size: 500 }
  );

  console.log(`[reset] Total "Sent" queue items fetched: ${all_sent.length}`);

  const in_window = all_sent.filter((item) => isInWindow(item));
  console.log(`[reset] Items in time window: ${in_window.length}`);
  return in_window;
}

async function resolveOwnerStatus(owner_id) {
  const { getItem } = await import("@/lib/providers/podio.js");
  const owner = await getItem(owner_id);
  if (!owner) return null;

  const contact_status = lower(getCategoryValue(owner, "contact-status", null));
  const contact_status_2 = lower(getCategoryValue(owner, "contact-status-2", null));

  return { owner, contact_status, contact_status_2 };
}

async function resetOwner(master_owner_id) {
  if (DRY_RUN) {
    console.log(
      `  [DRY RUN] Would clear contact_status_2, next_follow_up_at, last_contacted_at, last_outbound for owner ${master_owner_id}`
    );
    return;
  }

  await updateMasterOwnerItem(master_owner_id, {
    [MASTER_OWNER_FIELDS.contact_status_2]: null,
    [MASTER_OWNER_FIELDS.next_follow_up_at]: null,
    [MASTER_OWNER_FIELDS.last_contacted_at]: null,
    [MASTER_OWNER_FIELDS.last_outbound]: null,
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
  });

  console.log(`  [RESET] Cancelled queue item ${queue_item_id}`);
}

async function run() {
  console.log("");
  console.log("=== reset-pre-live-sends ===");
  console.log(`DRY_RUN: ${DRY_RUN}`);
  console.log(`Window : ${SINCE_ISO} → ${UNTIL_ISO}`);
  console.log("");

  const sent_items = await findSentQueueItems();

  if (sent_items.length === 0) {
    console.log("[reset] No Sent queue items found in window.");
    return;
  }

  // Collect unique master owner IDs from queue items
  const owner_queue_map = new Map(); // owner_id → [queue_item_ids]
  for (const item of sent_items) {
    const owner_id = getFirstAppReferenceId(item, "master-owner", null);
    if (!owner_id) continue;
    const key = String(owner_id);
    if (!owner_queue_map.has(key)) owner_queue_map.set(key, []);
    owner_queue_map.get(key).push(item);
  }

  console.log(`[reset] Unique Master Owners found: ${owner_queue_map.size}`);
  console.log("");

  // Check each owner's status and only reset those auto-set by pipeline
  const owners_to_reset = [];
  const owners_skipped = [];
  const queue_items_to_cancel = [];

  console.log("--- Checking Master Owner statuses ---");
  for (const [owner_id, queue_items] of owner_queue_map) {
    const result = await resolveOwnerStatus(Number(owner_id));
    if (!result) {
      console.log(`  Owner ${owner_id}: NOT FOUND — skipping`);
      owners_skipped.push({ owner_id, reason: "not_found" });
      continue;
    }

    const { contact_status, contact_status_2 } = result;

    // Only reset if contact-status is blank AND contact-status-2 was auto-set
    if (contact_status != null) {
      console.log(
        `  Owner ${owner_id}: contact_status="${contact_status}" — skipping (manually engaged)`
      );
      owners_skipped.push({ owner_id, reason: "has_contact_status", contact_status });
      continue;
    }

    if (contact_status_2 != null && !AUTO_SET_STATUSES.has(contact_status_2)) {
      console.log(
        `  Owner ${owner_id}: contact_status_2="${contact_status_2}" — skipping (not auto-set)`
      );
      owners_skipped.push({ owner_id, reason: "manual_status_2", contact_status_2 });
      continue;
    }

    if (contact_status_2 == null) {
      console.log(
        `  Owner ${owner_id}: already blank — skipping reset (will still cancel queue items)`
      );
    } else {
      console.log(
        `  Owner ${owner_id}: contact_status_2="${contact_status_2}" — WILL RESET`
      );
      owners_to_reset.push(owner_id);
    }

    // Always cancel the queue items for this owner
    queue_items_to_cancel.push(...queue_items);
  }

  console.log("");
  console.log(`[reset] Owners to reset: ${owners_to_reset.length}`);
  console.log(`[reset] Owners skipped: ${owners_skipped.length}`);
  console.log(`[reset] Queue items to cancel: ${queue_items_to_cancel.length}`);
  console.log("");

  // Print affected queue items
  console.log("--- Queue items to cancel ---");
  for (const item of queue_items_to_cancel) {
    const body = getTextValue(item, "message-text", "");
    const owner_id = getFirstAppReferenceId(item, "master-owner", null);
    const preview = body.length > 60 ? body.slice(0, 60) + "…" : body;
    console.log(
      `  Queue ${item.item_id} | owner=${owner_id} | body="${preview}"`
    );
  }
  console.log("");

  // Step 1: Reset owners
  if (owners_to_reset.length > 0) {
    console.log("--- Resetting Master Owners ---");
    for (const owner_id of owners_to_reset) {
      await resetOwner(Number(owner_id));
    }
    console.log("");
  }

  // Step 2: Cancel queue items
  if (queue_items_to_cancel.length > 0) {
    console.log("--- Cancelling queue items ---");
    for (const item of queue_items_to_cancel) {
      await cancelQueueItem(item.item_id);
    }
    console.log("");
  }

  console.log(
    `[reset] Done. ${owners_to_reset.length} owner(s) reset, ${queue_items_to_cancel.length} queue item(s) cancelled.`
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
