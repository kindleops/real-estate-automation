import crypto from "node:crypto";

export function textField(value) {
  return { value: String(value ?? "") };
}

export function categoryField(value) {
  return { value: { text: String(value ?? "") } };
}

export function numberField(value) {
  return { value: Number(value ?? 0) };
}

export function dateField(start) {
  return { start: String(start ?? "") };
}

export function appRefField(item_id) {
  return { value: { item_id: Number(item_id) } };
}

export function phoneField(value) {
  return { value: String(value ?? "") };
}

// Creates a Podio location field value with structured sub-fields.
// Mirrors the geocoded shape returned by the Podio API so that
// extractStreetAddress() and formatPropertyAddress() can reach the sub-fields
// directly rather than falling back to the pre-formatted string.
export function locationField({ street_address = "", city = "", state = "", postal_code = "", formatted = "" } = {}) {
  return {
    street_address,
    city,
    state,
    postal_code,
    formatted: formatted || [street_address, city, state, postal_code].filter(Boolean).join(", "),
    value: {
      street_address,
      city,
      state,
      postal_code,
      formatted: formatted || [street_address, city, state, postal_code].filter(Boolean).join(", "),
    },
  };
}

export function createPodioItem(item_id, fields = {}) {
  return {
    item_id: Number(item_id),
    fields: Object.entries(fields).map(([external_id, raw_values]) => ({
      external_id,
      values: Array.isArray(raw_values) ? raw_values : [raw_values],
    })),
  };
}

export function createInMemoryIdempotencyLedger() {
  const records = new Map();

  function normalizeKey(scope, key) {
    return `${String(scope || "").trim()}:${String(key || "").trim()}`;
  }

  return {
    records,
    hash(value) {
      return crypto
        .createHash("sha256")
        .update(typeof value === "string" ? value : JSON.stringify(value), "utf8")
        .digest("hex");
    },
    async begin({ scope, key, metadata = {} } = {}) {
      const record_key = normalizeKey(scope, key);
      const existing = records.get(record_key);

      if (existing?.status === "completed") {
        return {
          ok: true,
          duplicate: true,
          reason: "duplicate_event_ignored",
          record_item_id: record_key,
          key,
          scope,
          meta: existing,
        };
      }

      if (existing?.status === "processing") {
        return {
          ok: true,
          duplicate: true,
          reason: "event_already_processing",
          record_item_id: record_key,
          key,
          scope,
          meta: existing,
        };
      }

      records.set(record_key, {
        ...metadata,
        scope,
        key,
        status: "processing",
      });

      return {
        ok: true,
        duplicate: false,
        reason: "event_claimed",
        record_item_id: record_key,
        key,
        scope,
      };
    },
    async complete({ record_item_id, scope, key, metadata = {} } = {}) {
      const record_key = record_item_id || normalizeKey(scope, key);
      records.set(record_key, {
        ...metadata,
        scope,
        key,
        status: "completed",
      });

      return {
        ok: true,
        record_item_id: record_key,
      };
    },
    async fail({ record_item_id, scope, key, error = null, metadata = {} } = {}) {
      const record_key = record_item_id || normalizeKey(scope, key);
      records.set(record_key, {
        ...metadata,
        scope,
        key,
        status: "failed",
        last_error: error?.message || String(error || "unknown_error"),
      });

      return {
        ok: true,
        record_item_id: record_key,
      };
    },
  };
}
