export function clean(value) {
  return String(value ?? "").trim();
}

export function lower(value) {
  return clean(value).toLowerCase();
}

export function upper(value) {
  return clean(value).toUpperCase();
}

export function collapseWhitespace(value) {
  return clean(value).replace(/\s+/g, " ");
}

export function includesAny(value, phrases = []) {
  const text = lower(value);
  return phrases.some((phrase) => text.includes(lower(phrase)));
}

export function truncate(value, max = 200) {
  const text = String(value ?? "");
  if (text.length <= max) return text;
  return `${text.slice(0, max)}...`;
}

export function slugify(value) {
  return lower(value)
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

export default {
  clean,
  lower,
  upper,
  collapseWhitespace,
  includesAny,
  truncate,
  slugify,
};