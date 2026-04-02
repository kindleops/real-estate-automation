export function assert(condition, message = "Assertion failed") {
  if (!condition) {
    throw new Error(message);
  }
}

export function requireNonEmptyString(value, label = "value") {
  const output = String(value ?? "").trim();
  if (!output) {
    throw new Error(`Missing required ${label}`);
  }
  return output;
}

export function requireItemId(value, label = "item_id") {
  if (!value) {
    throw new Error(`Missing required ${label}`);
  }
  return value;
}

export function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export default {
  assert,
  requireNonEmptyString,
  requireItemId,
  isObject,
};