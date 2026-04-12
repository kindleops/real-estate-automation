import { toPodioDateTimeString } from "@/lib/utils/dates.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function hashString(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

export function mapQueueTimezoneToIana(value) {
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

function getLocalDateTimeParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

  const parts = formatter.formatToParts(date);
  const get = (type) => parts.find((entry) => entry.type === type)?.value || "00";

  const year = Number(get("year"));
  const month = Number(get("month"));
  const day = Number(get("day"));
  const hour = Number(get("hour"));
  const minute = Number(get("minute"));
  const second = Number(get("second"));

  return {
    year,
    month,
    day,
    hour,
    minute,
    second,
    minutes_since_midnight: hour * 60 + minute,
  };
}

function parseTimeToken(token) {
  const raw = clean(token).toUpperCase();
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)$/);

  if (!match) return null;

  let hour = Number(match[1]);
  const minute = Number(match[2] || "0");
  const meridiem = match[3];

  if (hour === 12) hour = 0;
  if (meridiem === "PM") hour += 12;

  return hour * 60 + minute;
}

function formatTimeToken(total_minutes = 0) {
  const clamped = Math.max(0, Math.min(24 * 60 - 1, Number(total_minutes) || 0));
  const hour24 = Math.floor(clamped / 60);
  const minute = clamped % 60;
  const meridiem = hour24 >= 12 ? "PM" : "AM";
  let hour12 = hour24 % 12;
  if (hour12 === 0) hour12 = 12;

  return minute === 0 ? `${hour12}${meridiem}` : `${hour12}:${pad2(minute)}${meridiem}`;
}

function timezoneLabelToWindowSuffix(timezone_label = "Central") {
  switch (clean(timezone_label)) {
    case "Eastern":
      return "ET";
    case "Mountain":
      return "MT";
    case "Pacific":
      return "PT";
    case "Alaska":
      return "AT";
    case "Hawaii":
      return "HT";
    case "Central":
    default:
      return "CT";
  }
}

function extractContactWindowSuffix(contact_window = "", timezone_label = "Central") {
  const match = clean(contact_window).match(/\b(Local|CT|ET|MT|PT|AT|HT)\s*$/i);
  return match?.[1] || timezoneLabelToWindowSuffix(timezone_label);
}

export function parseQueueContactWindow(window_value) {
  const raw = clean(window_value);
  if (!raw) return null;
  const suffix = extractContactWindowSuffix(raw);

  const normalized = raw.toUpperCase();

  const range_match = normalized.match(
    /(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*-\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))/
  );

  if (!range_match) return null;

  const start = parseTimeToken(range_match[1]);
  const end = parseTimeToken(range_match[2]);

  if (start === null || end === null) return null;

  return { start, end, suffix };
}

export function buildAlwaysOnContactWindow(timezone_label = "Central") {
  return `12AM-11:59PM ${timezoneLabelToWindowSuffix(timezone_label)}`;
}

export function buildFirstContactWindow({
  contact_window = null,
  timezone_label = "Central",
  min_minutes = 8 * 60,
  max_minutes = 21 * 60,
} = {}) {
  const suffix = extractContactWindowSuffix(contact_window, timezone_label);
  const parsed_window = parseQueueContactWindow(contact_window);

  if (!parsed_window) {
    return `${formatTimeToken(min_minutes)}-${formatTimeToken(max_minutes)} ${suffix}`;
  }

  return `${formatTimeToken(parsed_window.start)}-${formatTimeToken(parsed_window.end)} ${suffix}`;
}

export function resolveSchedulingContactWindow({
  contact_window = null,
  timezone_label = "Central",
  is_first_contact = false,
} = {}) {
  return buildFirstContactWindow({
    contact_window,
    timezone_label,
  });
}

function formatPodioLocalDateTime(parts) {
  return [
    `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)}`,
    `${pad2(parts.hour)}:${pad2(parts.minute)}:${pad2(parts.second ?? 0)}`,
  ].join(" ");
}

function addLocalDays(parts, days = 0) {
  const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day));
  date.setUTCDate(date.getUTCDate() + Number(days || 0));

  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function getTimeZoneOffsetMinutes(date, timeZone) {
  const parts = getLocalDateTimeParts(date, timeZone);
  const local_as_utc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second
  );

  return Math.round((local_as_utc - date.getTime()) / 60_000);
}

function zonedLocalDateTimeToUtcDate(parts, timeZone) {
  const utc_guess = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second ?? 0
  );

  const guess_date = new Date(utc_guess);
  const guess_offset = getTimeZoneOffsetMinutes(guess_date, timeZone);
  const actual_date = new Date(utc_guess - guess_offset * 60_000);
  const actual_offset = getTimeZoneOffsetMinutes(actual_date, timeZone);

  if (actual_offset !== guess_offset) {
    return new Date(utc_guess - actual_offset * 60_000);
  }

  return actual_date;
}

function shouldSendNow(current_minutes, window) {
  if (!window) return true;

  if (window.end >= window.start) {
    return current_minutes >= window.start && current_minutes <= window.end;
  }

  return current_minutes >= window.start || current_minutes <= window.end;
}

function clampToPositiveInteger(value, fallback = 0) {
  const numeric = Math.round(Number(value));
  if (!Number.isFinite(numeric) || numeric < 0) {
    return Math.max(0, Math.round(Number(fallback) || 0));
  }
  return numeric;
}

function pickDeterministicDelayMinutes(
  min_minutes = 0,
  max_minutes = 0,
  distribution_key = null
) {
  const lower_bound = clampToPositiveInteger(min_minutes, 0);
  const upper_bound = clampToPositiveInteger(max_minutes, lower_bound);
  const min = Math.min(lower_bound, upper_bound);
  const max = Math.max(lower_bound, upper_bound);

  if (max <= min) return min;
  if (!distribution_key) return min;

  const span = max - min + 1;
  return min + (Math.abs(hashString(String(distribution_key))) % span);
}

function pickDeterministicSecond(distribution_key = null, salt = "second") {
  if (!distribution_key) return 0;
  return Math.abs(hashString(`${distribution_key}:${salt}`)) % 60;
}

function pickMinuteInRange(min_minutes, max_minutes, distribution_key = null, salt = "minute") {
  const min = clampToPositiveInteger(min_minutes, 0);
  const max = clampToPositiveInteger(max_minutes, min);

  if (max <= min) return min;
  if (!distribution_key) return min;

  const span = max - min + 1;
  return min + (Math.abs(hashString(`${distribution_key}:${salt}`)) % span);
}

function addWindowSpread(base_minute, window, distribution_key = null) {
  const spread = pickDeterministicDelayMinutes(0, 20, distribution_key);
  const tentative = base_minute + spread;

  if (window.end >= window.start) {
    return Math.min(tentative, window.end);
  }

  return tentative >= 24 * 60 ? tentative % (24 * 60) : tentative;
}

function assertScheduledMinuteWithinWindow(scheduled_minute, window, metadata = {}) {
  if (shouldSendNow(scheduled_minute, window)) return;

  const error = new Error("INVALID_CONTACT_WINDOW");
  error.code = "INVALID_CONTACT_WINDOW";
  error.schedule_metadata = metadata;
  throw error;
}

function buildScheduledResult({
  target_date,
  scheduled_minute,
  scheduled_second = 0,
  parsed_window,
  timeZone,
  timezone_label,
  contact_window,
  reason,
  within_contact_window,
}) {
  assertScheduledMinuteWithinWindow(scheduled_minute, parsed_window, {
    scheduled_minute,
    parsed_window,
    timezone_label,
    contact_window,
    reason,
  });

  const scheduled_local_parts = {
    ...target_date,
    hour: Math.floor(scheduled_minute / 60),
    minute: scheduled_minute % 60,
    second: scheduled_second,
  };
  const scheduled_utc_date = zonedLocalDateTimeToUtcDate(scheduled_local_parts, timeZone);

  return {
    scheduled_for_local: formatPodioLocalDateTime(scheduled_local_parts),
    scheduled_for_utc: toPodioDateTimeString(scheduled_utc_date),
    timeZone,
    timezone_label: clean(timezone_label) || "Central",
    contact_window: clean(contact_window) || null,
    reason,
    within_contact_window,
  };
}

export function resolveQueueSchedule({
  now = new Date().toISOString(),
  timezone_label = "Central",
  contact_window = null,
  distribution_key = null,
  distribute_when_inside_window = true,
} = {}) {
  const base_date = new Date(now || Date.now());
  const safe_now = Number.isNaN(base_date.getTime()) ? new Date() : base_date;
  const timeZone = mapQueueTimezoneToIana(timezone_label);
  const local_now_parts = getLocalDateTimeParts(safe_now, timeZone);
  const parsed_window = parseQueueContactWindow(contact_window);
  const timezone_suffix = extractContactWindowSuffix(contact_window, timezone_label);
  const normalized_contact_window =
    clean(contact_window) ||
    `${formatTimeToken(8 * 60)}-${formatTimeToken(21 * 60)} ${timezone_suffix}`;

  if (!parsed_window) {
    return {
      scheduled_for_local: formatPodioLocalDateTime(local_now_parts),
      scheduled_for_utc: toPodioDateTimeString(safe_now),
      timeZone,
      timezone_label: clean(timezone_label) || "Central",
      contact_window: normalized_contact_window,
      reason: contact_window ? "unparseable_contact_window_schedule_now" : "missing_contact_window_schedule_now",
      within_contact_window: true,
    };
  }

  const current_minutes = local_now_parts.minutes_since_midnight;
  const within_contact_window = shouldSendNow(current_minutes, parsed_window);
  const scheduled_second = pickDeterministicSecond(distribution_key);

  if (within_contact_window) {
    let earliest_minute = current_minutes + 2;
    let latest_minute = parsed_window.end;
    let target_date = addLocalDays(local_now_parts, 0);

    if (parsed_window.end < parsed_window.start) {
      if (current_minutes >= parsed_window.start) {
        latest_minute = 24 * 60 - 1;
      }
    }

    if (earliest_minute > latest_minute) {
      target_date = addLocalDays(local_now_parts, 1);
      const delayed_start = addWindowSpread(
        parsed_window.start,
        parsed_window,
        distribution_key ? `${distribution_key}:next-window-start` : null
      );

      return buildScheduledResult({
        target_date,
        scheduled_minute: delayed_start,
        scheduled_second,
        parsed_window,
        timeZone,
        timezone_label,
        contact_window: normalized_contact_window,
        reason: "inside_contact_window_roll_to_next_window",
        within_contact_window,
      });
    }

    const scheduled_minute =
      distribute_when_inside_window
        ? pickMinuteInRange(
            earliest_minute,
            latest_minute,
            distribution_key,
            "inside-window"
          )
        : earliest_minute;

    return buildScheduledResult({
      target_date,
      scheduled_minute,
      scheduled_second,
      parsed_window,
      timeZone,
      timezone_label,
      contact_window: normalized_contact_window,
      reason: "inside_contact_window_schedule_with_spread",
      within_contact_window,
    });
  }

  const before_window_start =
    parsed_window.end >= parsed_window.start
      ? current_minutes < parsed_window.start
      : current_minutes > parsed_window.end && current_minutes < parsed_window.start;
  const target_date = before_window_start
    ? addLocalDays(local_now_parts, 0)
    : addLocalDays(local_now_parts, 1);
  const scheduled_minute = addWindowSpread(
    parsed_window.start,
    parsed_window,
    distribution_key ? `${distribution_key}:window-start` : null
  );

  return buildScheduledResult({
    target_date,
    scheduled_minute,
    scheduled_second,
    parsed_window,
    timeZone,
    timezone_label,
    contact_window: normalized_contact_window,
    reason: before_window_start
      ? "before_contact_window_schedule_at_window_start"
      : "after_contact_window_schedule_next_window",
    within_contact_window,
  });
}

export function resolveLatencyAwareQueueSchedule({
  now = new Date().toISOString(),
  timezone_label = "Central",
  contact_window = null,
  distribution_key = null,
  delay_min_minutes = 0,
  delay_max_minutes = 0,
} = {}) {
  const agent_delay_minutes = pickDeterministicDelayMinutes(
    delay_min_minutes,
    delay_max_minutes,
    distribution_key
  );

  const base_date = new Date(now || Date.now());
  const safe_now = Number.isNaN(base_date.getTime()) ? new Date() : base_date;
  const delayed_now = new Date(safe_now.getTime() + agent_delay_minutes * 60_000);

  const schedule = resolveQueueSchedule({
    now: delayed_now.toISOString(),
    timezone_label,
    contact_window,
    distribution_key,
    // Latency already provides per-row variation; skip inside-window redistribution.
    distribute_when_inside_window: false,
  });

  return {
    ...schedule,
    agent_delay_minutes,
    delay_min_minutes: clampToPositiveInteger(delay_min_minutes, 0),
    delay_max_minutes: clampToPositiveInteger(delay_max_minutes, 0),
    delayed_now_utc: toPodioDateTimeString(delayed_now),
  };
}

export default {
  buildAlwaysOnContactWindow,
  buildFirstContactWindow,
  mapQueueTimezoneToIana,
  parseQueueContactWindow,
  resolveQueueSchedule,
  resolveSchedulingContactWindow,
  resolveLatencyAwareQueueSchedule,
};
