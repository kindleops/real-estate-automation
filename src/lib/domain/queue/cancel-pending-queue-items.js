// ─── cancel-pending-queue-items.js ───────────────────────────────────────
// Cancels all Queued/Sending send-queue rows for a given master owner and/or
// phone item.  Called immediately after a confirmed negative inbound reply so
// that no further outbound touches fire before the suppression flag propagates.
//
// Scope: "Queued" and "Sending" items only.  Already-sent, blocked, or failed
// rows are left untouched — they represent historical record, not future sends.

import APP_IDS from "@/lib/config/app-ids.js";
import {
  filterAppItems,
  getCategoryValue,
  getFirstAppReferenceId,
  updateItem,
} from "@/lib/providers/podio.js";
import { info, warn } from "@/lib/logging/logger.js";

const CANCELABLE_STATUSES = new Set(["queued", "sending"]);

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function extractItems(response) {
  if (Array.isArray(response)) return response;
  return Array.isArray(response?.items) ? response.items : [];
}

/**
 * Cancel all pending queue items for a master owner + phone combination.
 *
 * @param {{ master_owner_id?: number|string|null, phone_item_id?: number|string|null, reason?: string }} options
 * @param {{ filterAppItemsImpl?: Function, updateItemImpl?: Function }} deps
 * @returns {Promise<{ ok: boolean, canceled_count: number, items_checked: number, cancelable_count?: number }>}
 */
export async function cancelPendingQueueItemsForOwner({
  master_owner_id = null,
  phone_item_id = null,
  reason = "inbound_negative_reply",
} = {}, deps = {}) {
  const {
    filterAppItemsImpl = filterAppItems,
    updateItemImpl = updateItem,
  } = deps;

  if (!master_owner_id && !phone_item_id) {
    return {
      ok: true,
      canceled_count: 0,
      items_checked: 0,
      skipped: true,
      reason: "no_owner_or_phone_provided",
    };
  }

  // Build the tightest filter available — prefer phone (more specific) when
  // both are present, then fall back to master owner.
  const filters = {};
  if (master_owner_id) {
    filters["master-owner"] = Number(master_owner_id);
  }
  if (phone_item_id) {
    filters["phone-number"] = Number(phone_item_id);
  }

  let all_items = [];

  try {
    // Fetch up to 100 pending rows — a buffer this large should never have
    // that many pending rows for one owner, but it's a safe upper bound.
    const response = await filterAppItemsImpl(
      APP_IDS.send_queue,
      filters,
      { limit: 100, offset: 0, sort_desc: true }
    );
    all_items = extractItems(response);
  } catch (error) {
    warn("queue.cancel_pending_fetch_failed", {
      master_owner_id: master_owner_id ?? null,
      phone_item_id: phone_item_id ?? null,
      reason,
      message: error?.message || "unknown_error",
    });
    return {
      ok: false,
      reason: "fetch_failed",
      canceled_count: 0,
      items_checked: 0,
    };
  }

  // Filter to rows that can still be stopped
  const cancelable = all_items.filter((item) => {
    const status = lower(getCategoryValue(item, "queue-status", "") || "");
    return CANCELABLE_STATUSES.has(status);
  });

  if (!cancelable.length) {
    return {
      ok: true,
      canceled_count: 0,
      items_checked: all_items.length,
      cancelable_count: 0,
      reason: "no_cancelable_items_found",
    };
  }

  let canceled_count = 0;
  const cancel_errors = [];

  for (const item of cancelable) {
    const item_id = item?.item_id;
    if (!item_id) continue;

    try {
      await updateItemImpl(item_id, {
        "queue-status": "Blocked",
        "failed-reason": "Opt-Out",
      });
      canceled_count += 1;
    } catch (error) {
      cancel_errors.push({
        item_id,
        message: clean(error?.message) || "unknown_error",
      });
    }
  }

  info("queue.pending_items_canceled_after_negative_reply", {
    master_owner_id: master_owner_id ?? null,
    phone_item_id: phone_item_id ?? null,
    reason,
    canceled_count,
    items_checked: all_items.length,
    cancelable_count: cancelable.length,
    error_count: cancel_errors.length,
  });

  return {
    ok: cancel_errors.length === 0,
    canceled_count,
    items_checked: all_items.length,
    cancelable_count: cancelable.length,
    errors: cancel_errors.length ? cancel_errors : undefined,
  };
}

export default cancelPendingQueueItemsForOwner;
