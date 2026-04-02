import PODIO_ATTACHED_BASE_SCHEMA from "@/lib/podio/schema-attached.generated.js";
import PODIO_ATTACHED_SCHEMA_SUPPLEMENT from "@/lib/podio/schema-attached-supplement.generated.js";
import { toPodioDateTimeString } from "@/lib/utils/dates.js";

export const PODIO_ATTACHED_SCHEMA = Object.freeze({
  ...PODIO_ATTACHED_BASE_SCHEMA,
  ...PODIO_ATTACHED_SCHEMA_SUPPLEMENT,
});

function normalizeCategoryText(value) {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function toFiniteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toAppReferenceIds(value) {
  if (value === null || value === undefined || value === "") return value;

  const list = Array.isArray(value) ? value : [value];

  return list
    .map((entry) => {
      if (typeof entry === "number") return entry;
      if (typeof entry === "string" && entry.trim() !== "") {
        const parsed = Number(entry);
        return Number.isFinite(parsed) ? parsed : null;
      }
      if (typeof entry?.item_id === "number") return entry.item_id;
      if (typeof entry?.value?.item_id === "number") return entry.value.item_id;
      if (typeof entry?.value === "number") return entry.value;
      return null;
    })
    .filter(Boolean);
}

export function hasAttachedSchema(app_id) {
  return Boolean(PODIO_ATTACHED_SCHEMA[String(app_id)]);
}

export function getAttachedAppSchema(app_id) {
  return PODIO_ATTACHED_SCHEMA[String(app_id)] || null;
}

export function getAttachedFieldSchema(app_id, external_id) {
  return getAttachedAppSchema(app_id)?.fields?.[external_id] || null;
}

export function getCategoryOptionId(app_id, external_id, value) {
  const field = getAttachedFieldSchema(app_id, external_id);
  if (!field || field.type !== "category") return null;

  const numeric = toFiniteNumber(value);
  if (numeric !== null) {
    return field.options.some((option) => option.id === numeric) ? numeric : null;
  }

  const normalized = normalizeCategoryText(value);
  if (!normalized) return null;

  return (
    field.options.find((option) => normalizeCategoryText(option.text) === normalized)?.id ||
    null
  );
}

function normalizeCategoryValue(app_id, external_id, value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;

  const field = getAttachedFieldSchema(app_id, external_id);
  if (!field) return value;

  const rawValues = Array.isArray(value) ? value : [value];
  const option_ids = rawValues
    .map((entry) => getCategoryOptionId(app_id, external_id, entry))
    .filter((entry) => entry !== null);

  if (!option_ids.length) {
    throw new Error(
      `[Podio] Invalid category value "${value}" for ${getAttachedAppSchema(app_id)?.app_name}::${external_id}`
    );
  }

  return field.multiple ? option_ids : option_ids[0];
}

function normalizeDateValue(value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;
  if (typeof value === "string") {
    return { start: toPodioDateTimeString(value) || value };
  }
  if (value instanceof Date) {
    return { start: toPodioDateTimeString(value) || value };
  }
  if (typeof value === "object") {
    if (value.start || value.end || value.start_date || value.end_date) {
      return {
        ...value,
        ...(value.start
          ? { start: toPodioDateTimeString(value.start) || value.start }
          : {}),
        ...(value.end
          ? { end: toPodioDateTimeString(value.end) || value.end }
          : {}),
        ...(value.start_date
          ? {
              start_date:
                toPodioDateTimeString(value.start_date) || value.start_date,
            }
          : {}),
        ...(value.end_date
          ? { end_date: toPodioDateTimeString(value.end_date) || value.end_date }
          : {}),
      };
    }
  }
  return value;
}

function normalizeMoneyValue(app_id, external_id, value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;

  if (typeof value === "object" && value !== null && "value" in value) {
    return {
      value: toFiniteNumber(value.value) ?? value.value,
      currency:
        value.currency ||
        getAttachedFieldSchema(app_id, external_id)?.allowed_currencies?.[0] ||
        "USD",
    };
  }

  const numeric = toFiniteNumber(value);
  if (numeric === null) {
    throw new Error(
      `[Podio] Invalid money value "${value}" for ${getAttachedAppSchema(app_id)?.app_name}::${external_id}`
    );
  }

  return {
    value: numeric,
    currency:
      getAttachedFieldSchema(app_id, external_id)?.allowed_currencies?.[0] || "USD",
  };
}

function normalizeNumberValue(value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;
  return toFiniteNumber(value) ?? value;
}

function normalizeAppValue(value) {
  if (value === null) return null;
  if (value === undefined || value === "") return undefined;
  return toAppReferenceIds(value);
}

export function normalizePodioFieldValue(app_id, external_id, value) {
  const field = getAttachedFieldSchema(app_id, external_id);
  if (!field) return value;

  switch (field.type) {
    case "category":
      return normalizeCategoryValue(app_id, external_id, value);
    case "date":
      return normalizeDateValue(value);
    case "money":
      return normalizeMoneyValue(app_id, external_id, value);
    case "number":
    case "progress":
      return normalizeNumberValue(value);
    case "app":
    case "contact":
    case "member":
      return normalizeAppValue(value);
    default:
      return value;
  }
}

export function normalizePodioFieldMap(app_id, fields = {}) {
  if (!fields || typeof fields !== "object") return fields;

  const appSchema = getAttachedAppSchema(app_id);
  if (!appSchema) return fields;

  const normalized = {};

  for (const [external_id, rawValue] of Object.entries(fields)) {
    if (!appSchema.fields[external_id]) {
      throw new Error(`[Podio] Unknown field for ${appSchema.app_name}: ${external_id}`);
    }

    normalized[external_id] = normalizePodioFieldValue(app_id, external_id, rawValue);
  }

  return normalized;
}

export default {
  PODIO_ATTACHED_SCHEMA,
  hasAttachedSchema,
  getAttachedAppSchema,
  getAttachedFieldSchema,
  getCategoryOptionId,
  normalizePodioFieldValue,
  normalizePodioFieldMap,
};
