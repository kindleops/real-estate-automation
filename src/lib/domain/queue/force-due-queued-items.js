import APP_IDS from "@/lib/config/app-ids.js";
import { toPodioDateTimeString, toPodioDateField } from "@/lib/utils/dates.js";
import { info, warn } from "@/lib/logging/logger.js";

// Podio helpers are loaded lazily so this module can be imported in tests
// without requiring a live axios installation (same pattern as queue-run-request.js).
async function loadPodio(overrides = {}) {
  if (
    overrides.fetchAllItems &&
    overrides.updateItem &&
    overrides.getCategoryValue &&
    overrides.getDateValue &&
    overrides.getFirstAppReferenceId
  ) {
    return overrides;
  }
  const podio = await import("@/lib/providers/podio.js");
  return {
    fetchAllItems: overrides.fetchAllItems ?? podio.fetchAllItems,
    updateItem: overrides.updateItem ?? podio.updateItem,
    getCategoryValue: overrides.getCategoryValue ?? podio.getCategoryValue,
    getDateValue: overrides.getDateValue ?? podio.getDateValue,
    getFirstAppReferenceId:
      overrides.getFirstAppReferenceId ?? podio.getFirstAppReferenceId,
  };
}

// ──────────────────────────────────────────────────────────────────────────────
// CONSTANTS
// ──────────────────────────────────────────────────────────────────────────────

const FORCE_DUE_MAX_ROWS = 25;

// ──────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ──────────────────────────────────────────────────────────────────────────────

function nowIso() {
  return new Date().toISOString();
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function mapTimezoneToIana(value) {
  const raw = lower(value);

  if (raw === "eastern" || raw === "et" || raw === "est" || raw === "edt") {
    return "America/New_York";
  }
  if (raw === "central" || raw === "ct" || raw === "cst" || raw === "cdt") {
    return "America/Chicago";
  }
  if (raw === "mountain" || raw === "mt" || raw === "mst" || raw === "mdt") {
    return "America/Denver";
  }
  if (raw === "pacific" || raw === "pt" || raw === "pst" || raw === "pdt") {
    return "America/Los_Angeles";
  }
  if (raw === "alaska") {
    return "America/Anchorage";
  }
  if (raw === "hawaii") {
    return "Pacific/Honolulu";
  }

  return "America/Chicago";
}

/**
 * Format a Date as "YYYY-MM-DD HH:MM:SS" in the given IANA timezone.
 * Falls back to the UTC Podio format if the timezone is invalid.
 */
function toPodioLocalDateTimeString(date, timezone_iana) {
  try {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone_iana,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });

    const parts = formatter.formatToParts(date);
    const get = (type) => parts.find((p) => p.type === type)?.value ?? "00";

    const year = get("year");
    const month = get("month");
    const day = get("day");
    // hour12: false may return "24" for midnight in some Node versions
    const raw_hour = get("hour");
    const hour = raw_hour === "24" ? "00" : raw_hour;
    const minute = get("minute");
    const second = get("second");

    return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
  } catch {
    // fallback: reuse the UTC Podio formatter
    return toPodioDateTimeString(date);
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// DOMAIN FUNCTION
// ──────────────────────────────────────────────────────────────────────────────

/**
 * Force-reschedule stuck Queued rows to be due immediately.
 *
 * @param {object} options
 * @param {number}  [options.limit=25]              Max rows to reschedule (hard cap 25).
 * @param {boolean} [options.dry_run=true]          When true, no writes are made.
 * @param {number}  [options.master_owner_id=null]  Scope to a single master owner.
 * @param {number}  [options.older_than_minutes]    When set, only target rows that are
 *                                                   either scheduled in the future OR
 *                                                   overdue by more than this many minutes.
 * @param {string}  [options.now]                   ISO timestamp override (for tests).
 * @param {object}  [deps]                          Injectable dependencies.
 */
export async function forceDueQueuedItems(
  {
    limit = FORCE_DUE_MAX_ROWS,
    dry_run = true,
    master_owner_id = null,
    older_than_minutes = null,
    now = null,
  } = {},
  deps = {}
) {
  const podio = await loadPodio(deps);
  const fetch_all_items = podio.fetchAllItems;
  const update_item = podio.updateItem;
  const get_category_value = podio.getCategoryValue;
  const get_date_value = podio.getDateValue;
  const get_first_app_reference_id = podio.getFirstAppReferenceId;
  const info_log = deps.info || info;
  const warn_log = deps.warn || warn;

  const run_started_at = now || nowIso();
  const now_ts = toTimestamp(run_started_at) ?? Date.now();
  const now_date = new Date(now_ts);
  const scoped_master_owner_id = Number(master_owner_id || 0) || null;
  const effective_limit = Math.min(
    Math.max(1, Number(limit) || FORCE_DUE_MAX_ROWS),
    FORCE_DUE_MAX_ROWS
  );
  const older_than_ms =
    older_than_minutes !== null && older_than_minutes !== undefined
      ? Number(older_than_minutes) * 60_000
      : null;

  info_log("queue_force_due.started", {
    limit: effective_limit,
    dry_run,
    master_owner_id: scoped_master_owner_id,
    older_than_minutes: older_than_minutes ?? null,
    run_started_at,
  });

  // ── Fetch all Queued rows ──────────────────────────────────────────────────

  const queued_items = await fetch_all_items(
    APP_IDS.send_queue,
    { "queue-status": "Queued" },
    {
      page_size: Math.max(effective_limit * 4, 50),
      sort_by: "scheduled-for-utc",
      sort_desc: false,
    }
  );

  info_log("queue_force_due.rows_loaded", {
    total_rows_loaded: queued_items.length,
    master_owner_id: scoped_master_owner_id,
    run_started_at,
  });

  // ── Filter eligible rows ───────────────────────────────────────────────────

  const eligible = [];

  for (const item of queued_items) {
    if (eligible.length >= effective_limit) break;

    const item_id = item?.item_id;
    const status = get_category_value(item, "queue-status", null);

    // Must be Queued
    if (lower(status) !== "queued") {
      continue;
    }

    // master_owner scope filter
    if (
      scoped_master_owner_id &&
      Number(get_first_app_reference_id(item, "master-owner", 0) || 0) !==
        scoped_master_owner_id
    ) {
      continue;
    }

    // older_than_minutes filter
    if (older_than_ms !== null) {
      const scheduled_utc = get_date_value(item, "scheduled-for-utc", null);
      const scheduled_ts = toTimestamp(scheduled_utc);

      if (scheduled_ts !== null) {
        const is_future = scheduled_ts > now_ts;
        const is_long_overdue = scheduled_ts < now_ts - older_than_ms;

        if (!is_future && !is_long_overdue) {
          // Row is recent-past but not overdue by threshold — skip it;
          // the normal runner should handle it.
          continue;
        }
      }
      // null scheduled_ts → no schedule set, always eligible
    }

    eligible.push(item);
  }

  // ── Build per-row action plan ──────────────────────────────────────────────

  const now_utc_podio = toPodioDateField(now_date); // { start: "YYYY-MM-DD HH:MM:SS" }
  const new_scheduled_utc = now_utc_podio?.start ?? toPodioDateTimeString(now_date);

  const actions = eligible.map((item) => {
    const timezone_label = get_category_value(item, "timezone", "Central");
    const timezone_iana = mapTimezoneToIana(timezone_label);

    return {
      queue_item_id: item?.item_id,
      old_scheduled_utc: get_date_value(item, "scheduled-for-utc", null),
      new_scheduled_utc,
      old_scheduled_local: get_date_value(item, "scheduled-for-local", null),
      new_scheduled_local: toPodioLocalDateTimeString(now_date, timezone_iana),
      timezone: timezone_label,
      timezone_iana,
      reason: "force_due_rescheduled",
    };
  });

  // ── Mutate (only when dry_run=false) ──────────────────────────────────────

  let rescheduled_count = 0;

  if (!dry_run) {
    for (const action of actions) {
      try {
        await update_item(action.queue_item_id, {
          "scheduled-for-utc": now_utc_podio,
          "scheduled-for-local": { start: action.new_scheduled_local },
        });

        rescheduled_count += 1;

        info_log("queue_force_due.row_rescheduled", {
          queue_item_id: action.queue_item_id,
          old_scheduled_utc: action.old_scheduled_utc,
          new_scheduled_utc: action.new_scheduled_utc,
          old_scheduled_local: action.old_scheduled_local,
          new_scheduled_local: action.new_scheduled_local,
          timezone: action.timezone,
          dry_run: false,
        });
      } catch (err) {
        warn_log("queue_force_due.row_reschedule_failed", {
          queue_item_id: action.queue_item_id,
          error: err?.message ?? "unknown",
          dry_run: false,
        });
      }
    }
  }

  const summary = {
    ok: true,
    dry_run,
    run_started_at,
    total_rows_loaded: queued_items.length,
    eligible_rows: eligible.length,
    rescheduled_count: dry_run ? 0 : rescheduled_count,
    skipped_count: queued_items.length - eligible.length,
    master_owner_id: scoped_master_owner_id,
    older_than_minutes: older_than_minutes ?? null,
    first_10_candidate_item_ids: eligible.slice(0, 10).map((i) => i?.item_id),
    first_10_actions: actions.slice(0, 10),
  };

  info_log("queue_force_due.completed", {
    dry_run,
    total_rows_loaded: queued_items.length,
    eligible_rows: eligible.length,
    rescheduled_count: summary.rescheduled_count,
    skipped_count: summary.skipped_count,
    master_owner_id: scoped_master_owner_id,
    run_started_at,
  });

  return summary;
}

export default forceDueQueuedItems;
