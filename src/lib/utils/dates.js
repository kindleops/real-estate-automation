export function nowIso() {
  return new Date().toISOString();
}

// Returns the current time formatted as "YYYY-MM-DD HH:MM:SS" in America/Chicago.
// Used for operational Podio date fields (Sent At, Delivered At) where ops expects
// Central time display.  Podio stores date strings as-is without timezone conversion,
// so writing UTC ISO strings (the nowIso() default) causes UTC hours to appear in
// the ops UI.  Writing a Central time string solves this without requiring workspace
// timezone reconfiguration.
export function nowPodioDateTimeCentral() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const get = (type) => parts.find((p) => p.type === type)?.value ?? "00";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

export function toPodioDateTimeString(value) {
  if (!value) return null;

  const text = typeof value === "string" ? value.trim() : "";

  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return text;
  }

  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text)) {
    return text;
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;

  return [
    `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`,
    `${pad2(date.getUTCHours())}:${pad2(date.getUTCMinutes())}:${pad2(date.getUTCSeconds())}`,
  ].join(" ");
}

export function nowPodioDateTime() {
  return toPodioDateTimeString(new Date());
}

export function toIso(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

export function addMinutes(value, minutes = 0) {
  const date = new Date(value || Date.now());
  date.setMinutes(date.getMinutes() + Number(minutes || 0));
  return date.toISOString();
}

export function addHours(value, hours = 0) {
  const date = new Date(value || Date.now());
  date.setHours(date.getHours() + Number(hours || 0));
  return date.toISOString();
}

export function addDays(value, days = 0) {
  const date = new Date(value || Date.now());
  date.setDate(date.getDate() + Number(days || 0));
  return date.toISOString();
}

export function isPast(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return false;
  return date.getTime() < Date.now();
}

export function toPodioDateField(value) {
  const formatted = toPodioDateTimeString(value);
  return formatted ? { start: formatted } : null;
}

export default {
  nowIso,
  nowPodioDateTimeCentral,
  toIso,
  toPodioDateTimeString,
  nowPodioDateTime,
  addMinutes,
  addHours,
  addDays,
  isPast,
  toPodioDateField,
};
