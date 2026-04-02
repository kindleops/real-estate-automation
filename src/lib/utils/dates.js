export function nowIso() {
  return new Date().toISOString();
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
  toIso,
  toPodioDateTimeString,
  nowPodioDateTime,
  addMinutes,
  addHours,
  addDays,
  isPast,
  toPodioDateField,
};
