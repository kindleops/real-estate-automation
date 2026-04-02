export function toNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function toInteger(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

export function clamp(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.min(max, Math.max(min, n));
}

export function percent(part, whole, fallback = 0) {
  const p = Number(part);
  const w = Number(whole);

  if (!Number.isFinite(p) || !Number.isFinite(w) || w === 0) {
    return fallback;
  }

  return (p / w) * 100;
}

export default {
  toNumber,
  toInteger,
  clamp,
  percent,
};