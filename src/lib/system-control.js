/**
 * system-control.js
 *
 * Runtime feature-flag helpers backed by the public.system_control Supabase table.
 * Every critical send path should call getSystemFlag() / requireSystemFlag() before
 * executing destructive work.
 *
 * Usage:
 *   import { requireSystemFlag } from '@/lib/system-control.js';
 *   await requireSystemFlag('outbound_sms_enabled', 'sms-send');
 *
 * getSystemFlag() returns true when the flag is enabled (or when Supabase is
 * unreachable and the caller does not opt-in to fail-closed).
 */

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { warn, info } from "@/lib/logging/logger.js";

const TABLE = "system_control";

// In-process cache so we don't hammer Supabase on every message send.
const CACHE_TTL_MS = 30_000; // 30 seconds

const _cache = new Map(); // key -> { enabled, expires_at }

function clean(value) {
  return String(value ?? "").trim();
}

function getCachedFlag(key) {
  const entry = _cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expires_at) {
    _cache.delete(key);
    return null;
  }
  return entry.enabled;
}

function setCachedFlag(key, enabled) {
  _cache.set(key, { enabled, expires_at: Date.now() + CACHE_TTL_MS });
}

/**
 * Fetch a single system_control flag from Supabase.
 *
 * @param {string} key - The flag key (e.g. "outbound_sms_enabled").
 * @param {{ failClosedOnError?: boolean, supabase?: object }} [opts]
 *   - failClosedOnError: when true, DB errors treat the flag as disabled (false).
 *     Defaults to false so a Supabase outage does not accidentally stop sends.
 * @returns {Promise<boolean>}
 */
export async function getSystemFlag(key, opts = {}) {
  const { failClosedOnError = false, supabase = defaultSupabase } = opts;
  const normalized_key = clean(key);

  const cached = getCachedFlag(normalized_key);
  if (cached !== null) return cached;

  try {
    const { data, error } = await supabase
      .from(TABLE)
      .select("enabled")
      .eq("key", normalized_key)
      .maybeSingle();

    if (error) {
      warn("system_control.fetch_error", { key: normalized_key, message: error.message });
      const fallback = !failClosedOnError;
      setCachedFlag(normalized_key, fallback);
      return fallback;
    }

    // Row missing ⇒ treat as enabled (unknown flags default to on).
    const enabled = data == null ? true : Boolean(data.enabled);
    setCachedFlag(normalized_key, enabled);
    return enabled;
  } catch (err) {
    warn("system_control.unexpected_error", { key: normalized_key, message: err?.message });
    const fallback = !failClosedOnError;
    setCachedFlag(normalized_key, fallback);
    return fallback;
  }
}

/**
 * Fetch multiple flags in a single query.
 *
 * @param {string[]} keys
 * @param {{ failClosedOnError?: boolean, supabase?: object }} [opts]
 * @returns {Promise<Record<string, boolean>>}
 */
export async function getSystemFlags(keys, opts = {}) {
  const { failClosedOnError = false, supabase = defaultSupabase } = opts;
  const normalized_keys = keys.map(clean).filter(Boolean);
  if (!normalized_keys.length) return {};

  // Check cache first — only fetch missing keys.
  const result = {};
  const to_fetch = [];

  for (const key of normalized_keys) {
    const cached = getCachedFlag(key);
    if (cached !== null) {
      result[key] = cached;
    } else {
      to_fetch.push(key);
    }
  }

  if (!to_fetch.length) return result;

  try {
    const { data, error } = await supabase
      .from(TABLE)
      .select("key, enabled")
      .in("key", to_fetch);

    if (error) {
      warn("system_control.fetch_multi_error", { message: error.message });
      const fallback = !failClosedOnError;
      for (const key of to_fetch) {
        result[key] = fallback;
        setCachedFlag(key, fallback);
      }
      return result;
    }

    const found = new Map((data ?? []).map((row) => [row.key, Boolean(row.enabled)]));

    for (const key of to_fetch) {
      const enabled = found.has(key) ? found.get(key) : true; // unknown → enabled
      result[key] = enabled;
      setCachedFlag(key, enabled);
    }
  } catch (err) {
    warn("system_control.unexpected_multi_error", { message: err?.message });
    const fallback = !failClosedOnError;
    for (const key of to_fetch) {
      result[key] = fallback;
      setCachedFlag(key, fallback);
    }
  }

  return result;
}

/**
 * Assert a flag is enabled.  Throws a structured error with status 423 when disabled.
 *
 * @param {string} key - Flag key.
 * @param {string} context - Human-readable caller context for logs (e.g. "queue-runner").
 * @param {{ failClosedOnError?: boolean, supabase?: object }} [opts]
 * @throws {SystemControlDisabledError} when the flag is false.
 */
export async function requireSystemFlag(key, context = "unknown", opts = {}) {
  const enabled = await getSystemFlag(key, opts);

  if (!enabled) {
    info("system_control.disabled", { key, context });
    const err = new SystemControlDisabledError(key, context);
    throw err;
  }
}

/**
 * Structured error thrown by requireSystemFlag().
 * Callers can catch this and return HTTP 423.
 */
export class SystemControlDisabledError extends Error {
  constructor(key, context = "unknown") {
    super(`System flag '${key}' is disabled. Context: ${context}`);
    this.name = "SystemControlDisabledError";
    this.status = 423;
    this.flag_key = key;
    this.context = context;
  }
}

/**
 * Build a NextResponse-compatible 423 JSON body for disabled routes.
 */
export function buildDisabledResponse(key, context = "unknown") {
  return {
    ok: false,
    status: 423,
    error: "system_control_disabled",
    flag_key: key,
    context,
    message: `Operation '${context}' is currently disabled via system_control['${key}'].`,
  };
}

/** Invalidate the in-process cache (useful in tests or after manual flag updates). */
export function clearSystemControlCache() {
  _cache.clear();
}
