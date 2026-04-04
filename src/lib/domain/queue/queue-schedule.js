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

  const normalized = raw.toUpperCase();

  const range_match = normalized.match(
    /(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*-\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))/
  );

  if (!range_match) return null;

  const start = parseTimeToken(range_match[1]);
  const end = parseTimeToken(range_match[2]);

  if (start === null || end === null) return null;

  return { start, end };
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

  const intervals =
    parsed_window.end >= parsed_window.start
      ? [[parsed_window.start, parsed_window.end]]
      : [
          [parsed_window.start, 24 * 60 - 1],
          [0, parsed_window.end],
        ];

  const overlaps = intervals
    .map(([start, end]) => [Math.max(start, min_minutes), Math.min(end, max_minutes)])
    .filter(([start, end]) => end > start);

  if (!overlaps.length) {
    return `${formatTimeToken(min_minutes)}-${formatTimeToken(max_minutes)} ${suffix}`;
  }

  overlaps.sort((left, right) => {
    const left_span = left[1] - left[0];
    const right_span = right[1] - right[0];
    if (right_span !== left_span) return right_span - left_span;
    return left[0] - right[0];
  });

  const [start, end] = overlaps[0];
  return `${formatTimeToken(start)}-${formatTimeToken(end)} ${suffix}`;
}

export function resolveSchedulingContactWindow({
  contact_window = null,
  timezone_label = "Central",
  is_first_contact = false,
} = {}) {
  if (is_first_contact) {
    return buildFirstContactWindow({
      contact_window,
      timezone_label,
    });
  }

  return buildAlwaysOnContactWindow(timezone_label);
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

function getWindowDurationMinutes(window) {
  if (!window) return 0;

  if (window.end >= window.start) {
    return Math.max(0, window.end - window.start);
  }

  return Math.max(0, (24 * 60 - window.start) + window.end);
}

function pickDistributedWindowMinute(window, distribution_key = null) {
  if (!window) return 0;

  const duration = getWindowDurationMinutes(window);
  if (!distribution_key || duration <= 0) {
    return window.start;
  }

  const guard_band = duration >= 30 ? 5 : 0;
  const usable_span = Math.max(1, duration - guard_band * 2);
  const offset = guard_band + (Math.abs(hashString(String(distribution_key))) % usable_span);

  return (window.start + offset) % (24 * 60);
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

export function resolveQueueSchedule({
  now = new Date().toISOString(),
  timezone_label = "Central",
  contact_window = null,
  distribution_key = null,
} = {}) {
  const base_date = new Date(now || Date.now());
  const safe_now = Number.isNaN(base_date.getTime()) ? new Date() : base_date;
  const timeZone = mapQueueTimezoneToIana(timezone_label);
  const local_now_parts = getLocalDateTimeParts(safe_now, timeZone);
  const parsed_window = parseQueueContactWindow(contact_window);

  if (!parsed_window) {
    return {
      scheduled_for_local: formatPodioLocalDateTime(local_now_parts),
      scheduled_for_utc: toPodioDateTimeString(safe_now),
      timeZone,
      timezone_label: clean(timezone_label) || "Central",
      contact_window: clean(contact_window) || null,
      reason: contact_window ? "unparseable_contact_window_schedule_now" : "missing_contact_window_schedule_now",
      within_contact_window: true,
    };
  }

  if (shouldSendNow(local_now_parts.minutes_since_midnight, parsed_window)) {
    return {
      scheduled_for_local: formatPodioLocalDateTime(local_now_parts),
      scheduled_for_utc: toPodioDateTimeString(safe_now),
      timeZone,
      timezone_label: clean(timezone_label) || "Central",
      contact_window: clean(contact_window) || null,
      reason: "inside_contact_window_schedule_now",
      within_contact_window: true,
    };
  }

  const target_date =
    parsed_window.end >= parsed_window.start &&
    local_now_parts.minutes_since_midnight > parsed_window.end
      ? addLocalDays(local_now_parts, 1)
      : parsed_window.end < parsed_window.start &&
          local_now_parts.minutes_since_midnight > parsed_window.end &&
          local_now_parts.minutes_since_midnight < parsed_window.start
        ? addLocalDays(local_now_parts, 0)
        : addLocalDays(local_now_parts, 0);

  const scheduled_minute_of_day = pickDistributedWindowMinute(
    parsed_window,
    distribution_key
  );
  const start_hour = Math.floor(scheduled_minute_of_day / 60);
  const start_minute = scheduled_minute_of_day % 60;
  const scheduled_local_parts = {
    ...target_date,
    hour: start_hour,
    minute: start_minute,
    second: 0,
  };

  if (
    parsed_window.end >= parsed_window.start &&
    local_now_parts.minutes_since_midnight > parsed_window.end
  ) {
    Object.assign(scheduled_local_parts, addLocalDays(local_now_parts, 1));
  }

  const scheduled_utc_date = zonedLocalDateTimeToUtcDate(scheduled_local_parts, timeZone);

  return {
    scheduled_for_local: formatPodioLocalDateTime(scheduled_local_parts),
    scheduled_for_utc: toPodioDateTimeString(scheduled_utc_date),
    timeZone,
    timezone_label: clean(timezone_label) || "Central",
    contact_window: clean(contact_window) || null,
    reason: distribution_key
      ? "outside_contact_window_schedule_within_window"
      : "outside_contact_window_schedule_at_window_start",
    within_contact_window: false,
  };
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
