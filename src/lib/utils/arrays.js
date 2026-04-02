export function ensureArray(value) {
  if (value == null) return [];
  return Array.isArray(value) ? value : [value];
}

export function compact(list = []) {
  return ensureArray(list).filter(Boolean);
}

export function unique(list = []) {
  return [...new Set(compact(list))];
}

export function uniqueBy(list = [], getKey) {
  const seen = new Set();
  const out = [];

  for (const item of ensureArray(list)) {
    const key = getKey(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
}

export function chunk(list = [], size = 50) {
  const out = [];
  const arr = ensureArray(list);

  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }

  return out;
}

export function first(list = [], fallback = null) {
  return ensureArray(list)[0] ?? fallback;
}

export function last(list = [], fallback = null) {
  const arr = ensureArray(list);
  return arr.length ? arr[arr.length - 1] : fallback;
}

export default {
  ensureArray,
  compact,
  unique,
  uniqueBy,
  chunk,
  first,
  last,
};