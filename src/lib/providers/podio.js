import fs from "node:fs/promises";
import path from "node:path";
import axios from "axios";
import APP_IDS from "@/lib/config/app-ids.js";
import { child } from "@/lib/logging/logger.js";
import {
  hasAttachedSchema,
  normalizePodioFieldMap,
  normalizePodioFilterMap,
} from "@/lib/podio/schema.js";

function clean(value) {
  return String(value ?? "").trim();
}

const logger = child({
  module: "providers.podio",
});

function toDebugHeaders(headers = null) {
  if (!headers) return null;
  if (typeof headers?.toJSON === "function") {
    return headers.toJSON();
  }
  if (typeof headers === "object") {
    return { ...headers };
  }
  return headers;
}

function sanitizeDebugHeaders(headers = null) {
  const normalized = toDebugHeaders(headers);
  if (!normalized || typeof normalized !== "object") return normalized;

  const cloned = { ...normalized };
  for (const key of Object.keys(cloned)) {
    if (String(key).toLowerCase() === "authorization") {
      cloned[key] = "[redacted]";
    }
  }

  return cloned;
}

function logPodioAxiosFailure(err, fallback = {}) {
  console.error("🚨 PODIO REQUEST FAILED", {
    url: err?.config?.url ?? fallback.url ?? null,
    method: err?.config?.method ?? fallback.method ?? null,
    data: err?.config?.data ?? fallback.data ?? null,
    params: err?.config?.params ?? fallback.params ?? null,
    headers: sanitizeDebugHeaders(err?.config?.headers ?? fallback.headers ?? null),
    response_status: err?.response?.status ?? null,
    response_data: err?.response?.data ?? null,
  });
}

// ══════════════════════════════════════════════════════════════════════════
// CONFIG & ENV VALIDATION
// ══════════════════════════════════════════════════════════════════════════

const PODIO_CLIENT_ID = process.env.PODIO_CLIENT_ID;
const PODIO_CLIENT_SECRET = process.env.PODIO_CLIENT_SECRET;
const PODIO_USERNAME = process.env.PODIO_USERNAME;
const PODIO_PASSWORD = process.env.PODIO_PASSWORD;

const PODIO_API_BASE = "https://api.podio.com";
const PODIO_OAUTH_URL = "https://podio.com/oauth/token";

const REQUEST_TIMEOUT_MS = 20_000;
const TOKEN_REFRESH_BUFFER_MS = 15_000;
const MAX_RETRIES = 4;
const RETRY_BASE_DELAY_MS = 300;
const RETRY_MAX_DELAY_MS = 15_000;
const LOW_RATE_LIMIT_THRESHOLDS = [250, 100, 50];
const FILTER_RESULT_CACHE_MAX_TTL_MS = 30_000;
const PODIO_COOLDOWN_STATE_FILE =
  process.env.PODIO_COOLDOWN_STATE_FILE ||
  "/tmp/real-estate-automation-podio-rate-limit-cooldown.json";
const PODIO_RATE_LIMIT_STATUS_FILE =
  process.env.PODIO_RATE_LIMIT_STATUS_FILE ||
  "/tmp/real-estate-automation-podio-rate-limit-status.json";

const _filter_result_cache =
  globalThis.__rea_podio_filter_result_cache__ || new Map();
if (!globalThis.__rea_podio_filter_result_cache__) {
  globalThis.__rea_podio_filter_result_cache__ = _filter_result_cache;
}

const _podio_cooldown_runtime = globalThis.__rea_podio_cooldown_runtime__ || {
  loaded: false,
  state: null,
  read_promise: null,
};
if (!globalThis.__rea_podio_cooldown_runtime__) {
  globalThis.__rea_podio_cooldown_runtime__ = _podio_cooldown_runtime;
}

const _podio_rate_limit_status_runtime =
  globalThis.__rea_podio_rate_limit_status_runtime__ || {
    loaded: false,
    state: null,
    read_promise: null,
  };
if (!globalThis.__rea_podio_rate_limit_status_runtime__) {
  globalThis.__rea_podio_rate_limit_status_runtime__ =
    _podio_rate_limit_status_runtime;
}

const REQUIRED_ENV = {
  PODIO_CLIENT_ID,
  PODIO_CLIENT_SECRET,
  PODIO_USERNAME,
  PODIO_PASSWORD,
};

for (const [key, value] of Object.entries(REQUIRED_ENV)) {
  if (!value) throw new Error(`[Podio] Missing required env var: ${key}`);
}

// ══════════════════════════════════════════════════════════════════════════
// STRUCTURED ERROR
// ══════════════════════════════════════════════════════════════════════════

export class PodioError extends Error {
  constructor(
    message,
    {
      method,
      path,
      status,
      data,
      headers = null,
      operation = null,
      retry_after_seconds = null,
      rate_limit_limit = null,
      rate_limit_remaining = null,
      code = null,
      cooldown_until = null,
      cooldown_active = false,
      cause = null,
    } = {}
  ) {
    super(message);
    this.name = "PodioError";
    this.method = method ?? null;
    this.path = path ?? null;
    this.status = status ?? null;
    this.data = data ?? null;
    this.headers = headers ?? null;
    this.operation = operation ?? null;
    this.retry_after_seconds = retry_after_seconds ?? null;
    this.rate_limit_limit = rate_limit_limit ?? null;
    this.rate_limit_remaining = rate_limit_remaining ?? null;
    this.code = code ?? null;
    this.cooldown_until = cooldown_until ?? null;
    this.cooldown_active = Boolean(cooldown_active);
    this.cause = cause ?? null;
  }
}

function toPodioError(err, method, path) {
  const status = err?.response?.status ?? null;
  const data = err?.response?.data ?? null;
  const headers = err?.response?.headers ?? null;
  const retry_after_seconds = getPodioRetryAfterSeconds(err, null);
  const rate_limit = extractRateLimitMeta(headers);
  const message =
    data?.error_description ??
    data?.error ??
    err?.message ??
    "Unknown Podio error";

  return new PodioError(message, {
    method,
    path,
    status,
    data,
    headers,
    operation: derivePodioOperation(method, path),
    retry_after_seconds,
    rate_limit_limit: rate_limit.rate_limit_limit,
    rate_limit_remaining: rate_limit.rate_limit_remaining,
    code: err?.code ?? null,
    cause: err ?? null,
  });
}

// ══════════════════════════════════════════════════════════════════════════
// TOKEN MANAGEMENT
// ══════════════════════════════════════════════════════════════════════════

let _access_token = null;
let _expires_at = 0;
let _refresh_promise = null;
const _item_app_id_cache = new Map();
let _latest_rate_limit_status = buildEmptyRateLimitStatus();
let _last_low_rate_limit_warning_threshold = null;

function buildEmptyRateLimitStatus() {
  return {
    observed: false,
    observed_at: null,
    method: null,
    path: null,
    operation: null,
    status: null,
    duration_ms: null,
    attempt: null,
    rate_limit_limit: null,
    rate_limit_remaining: null,
    retry_after_seconds: null,
    low_remaining_threshold: null,
  };
}

function _isTokenExpired() {
  return !_access_token || Date.now() >= _expires_at - TOKEN_REFRESH_BUFFER_MS;
}

export function invalidateToken() {
  _access_token = null;
  _expires_at = 0;
}

async function _doRefresh() {
  const form = new URLSearchParams({
    grant_type: "password",
    client_id: PODIO_CLIENT_ID,
    client_secret: PODIO_CLIENT_SECRET,
    username: PODIO_USERNAME,
    password: PODIO_PASSWORD,
  });

  try {
    const res = await axios.post(PODIO_OAUTH_URL, form, {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      timeout: REQUEST_TIMEOUT_MS,
    });

    _access_token = res.data.access_token;
    _expires_at = Date.now() + res.data.expires_in * 1000;

    return _access_token;
  } catch (err) {
    logPodioAxiosFailure(err, {
      url: PODIO_OAUTH_URL,
      method: "post",
      data: form.toString(),
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    });
    throw err;
  }
}

async function getToken() {
  if (!_isTokenExpired()) return _access_token;
  if (_refresh_promise) return _refresh_promise;

  _refresh_promise = _doRefresh().finally(() => {
    _refresh_promise = null;
  });

  return _refresh_promise;
}

export function getPodioCredentialStatus() {
  const missing = Object.entries(REQUIRED_ENV)
    .filter(([, value]) => !value)
    .map(([key]) => key);

  return {
    configured: missing.length === 0,
    missing,
    client_id_present: Boolean(PODIO_CLIENT_ID),
    client_secret_present: Boolean(PODIO_CLIENT_SECRET),
    username_present: Boolean(PODIO_USERNAME),
    password_present: Boolean(PODIO_PASSWORD),
  };
}

export function hasPodioCredentials() {
  return getPodioCredentialStatus().configured;
}

export async function verifyPodioAuth() {
  try {
    const access_token = await getToken();
    return {
      ok: true,
      reason: "podio_auth_ready",
      access_token_present: Boolean(clean(access_token)),
    };
  } catch (error) {
    return {
      ok: false,
      reason: clean(error?.message) || "podio_auth_failed",
    };
  }
}

// ══════════════════════════════════════════════════════════════════════════
// RETRY ENGINE — Exponential Backoff + Full Jitter
// ══════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Podio uses 420 for rate limiting. 429 included for safety.
const RETRYABLE_STATUSES = new Set([408, 409, 420, 425, 429, 500, 502, 503, 504]);

function isRetryable(status) {
  return RETRYABLE_STATUSES.has(status);
}

function toHeaderNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function getHeaderValue(headers = {}, target = "") {
  if (!headers || typeof headers !== "object") return null;
  const wanted = clean(target).toLowerCase();
  if (!wanted) return null;

  for (const [key, value] of Object.entries(headers)) {
    if (clean(key).toLowerCase() === wanted) {
      if (Array.isArray(value)) return value[0] ?? null;
      return value ?? null;
    }
  }

  return null;
}

function derivePodioOperation(method = "", path = "") {
  const normalized_method = clean(method).toUpperCase();
  const normalized_path = clean(path);

  if (normalized_method === "GET" && /^\/item\/\d+$/.test(normalized_path)) {
    return "get_item";
  }

  if (normalized_method === "DELETE" && /^\/item\/\d+$/.test(normalized_path)) {
    return "delete_item";
  }

  if (normalized_method === "PUT" && /^\/item\/\d+$/.test(normalized_path)) {
    return "update_item";
  }

  if (normalized_method === "POST" && /^\/item\/app\/\d+\/$/.test(normalized_path)) {
    return "create_item";
  }

  if (normalized_method === "POST" && /^\/item\/app\/\d+\/filter\/[^/]+\/$/.test(normalized_path)) {
    return "filter_items_by_view";
  }

  if (normalized_method === "POST" && /^\/item\/app\/\d+\/filter\/$/.test(normalized_path)) {
    return "filter_items";
  }

  if (normalized_method === "GET" && /^\/view\/app\/\d+\/$/.test(normalized_path)) {
    return "list_app_views";
  }

  if (normalized_method === "GET" && /^\/view\/app\/\d+\/[^/]+$/.test(normalized_path)) {
    return "get_app_view";
  }

  return `${normalized_method.toLowerCase() || "request"}:${normalized_path || "/"}`;
}

function extractRateLimitMeta(headers = {}) {
  return {
    rate_limit_limit: toHeaderNumber(getHeaderValue(headers, "x-rate-limit-limit")),
    rate_limit_remaining: toHeaderNumber(getHeaderValue(headers, "x-rate-limit-remaining")),
  };
}

function resolveLowRateLimitThreshold(remaining = null) {
  if (!Number.isFinite(Number(remaining))) return null;

  for (const threshold of [...LOW_RATE_LIMIT_THRESHOLDS].sort((left, right) => left - right)) {
    if (Number(remaining) <= threshold) {
      return threshold;
    }
  }

  return null;
}

export function resetPodioRateLimitObservability() {
  _latest_rate_limit_status = buildEmptyRateLimitStatus();
  _podio_rate_limit_status_runtime.state = _latest_rate_limit_status;
  _podio_rate_limit_status_runtime.loaded = true;
  _last_low_rate_limit_warning_threshold = null;
}

export function getLatestPodioRateLimitStatus() {
  return { ..._latest_rate_limit_status };
}

function normalizeRateLimitStatus(value = null) {
  if (!value || typeof value !== "object") {
    return buildEmptyRateLimitStatus();
  }

  return {
    ...buildEmptyRateLimitStatus(),
    ...value,
    observed: Boolean(value?.observed),
    observed_at: clean(value?.observed_at) || null,
    method: clean(value?.method).toUpperCase() || null,
    path: clean(value?.path) || null,
    operation: clean(value?.operation) || null,
    status: toHeaderNumber(value?.status),
    duration_ms: toHeaderNumber(value?.duration_ms),
    attempt: toHeaderNumber(value?.attempt),
    rate_limit_limit: toHeaderNumber(value?.rate_limit_limit),
    rate_limit_remaining: toHeaderNumber(value?.rate_limit_remaining),
    retry_after_seconds: normalizePositiveInteger(value?.retry_after_seconds, null),
    low_remaining_threshold: toHeaderNumber(value?.low_remaining_threshold),
  };
}

async function persistLatestRateLimitStatus(observation) {
  const file_path = PODIO_RATE_LIMIT_STATUS_FILE;
  const directory = path.dirname(file_path);
  await fs.mkdir(directory, { recursive: true }).catch(() => {});
  await fs.writeFile(
    file_path,
    JSON.stringify(normalizeRateLimitStatus(observation), null, 2),
    "utf8"
  );
}

async function loadLatestRateLimitStatus(force_reload = false) {
  if (_podio_rate_limit_status_runtime.loaded && !force_reload) {
    return normalizeRateLimitStatus(_podio_rate_limit_status_runtime.state);
  }

  if (_podio_rate_limit_status_runtime.read_promise && !force_reload) {
    return _podio_rate_limit_status_runtime.read_promise;
  }

  const read_promise = fs
    .readFile(PODIO_RATE_LIMIT_STATUS_FILE, "utf8")
    .then((raw) => {
      try {
        return normalizeRateLimitStatus(JSON.parse(raw));
      } catch {
        return buildEmptyRateLimitStatus();
      }
    })
    .catch((error) => {
      if (error?.code !== "ENOENT") {
        logger.warn("podio.rate_limit_status_read_failed", {
          message: error?.message || null,
          file_path: PODIO_RATE_LIMIT_STATUS_FILE,
        });
      }
      return buildEmptyRateLimitStatus();
    })
    .finally(() => {
      _podio_rate_limit_status_runtime.read_promise = null;
    });

  _podio_rate_limit_status_runtime.read_promise = read_promise;
  const state = await read_promise;
  _podio_rate_limit_status_runtime.state = state;
  _podio_rate_limit_status_runtime.loaded = true;
  return state;
}

function observationIsFresh(observation, max_age_ms = 5 * 60_000) {
  const observed_at_ts = toTimestamp(observation?.observed_at);
  if (!observed_at_ts) return false;
  return Date.now() - observed_at_ts <= Math.max(Number(max_age_ms) || 0, 1);
}

export async function getPodioRateLimitPressureState({
  min_remaining = 100,
  max_age_ms = 5 * 60_000,
  force_reload = false,
} = {}) {
  const cooldown = await getPodioRateLimitCooldown({ force_reload });
  if (cooldown.active) {
    return {
      active: true,
      reason: "podio_rate_limit_cooldown_active",
      observation: null,
      cooldown,
      min_remaining,
      max_age_ms,
    };
  }

  const observation = force_reload
    ? await loadLatestRateLimitStatus(true)
    : normalizeRateLimitStatus(
        _latest_rate_limit_status?.observed
          ? _latest_rate_limit_status
          : await loadLatestRateLimitStatus(false)
      );

  if (!observation.observed || !observationIsFresh(observation, max_age_ms)) {
    return {
      active: false,
      reason: null,
      observation,
      cooldown,
      min_remaining,
      max_age_ms,
    };
  }

  if (normalizePositiveInteger(observation.retry_after_seconds, null)) {
    return {
      active: true,
      reason: "podio_rate_limit_retry_after_observed",
      observation,
      cooldown,
      min_remaining,
      max_age_ms,
    };
  }

  if (
    Number.isFinite(Number(observation.rate_limit_remaining)) &&
    Number(observation.rate_limit_remaining) <= Math.max(Number(min_remaining) || 0, 0)
  ) {
    return {
      active: true,
      reason: "podio_rate_limit_low_remaining",
      observation,
      cooldown,
      min_remaining,
      max_age_ms,
    };
  }

  return {
    active: false,
    reason: null,
    observation,
    cooldown,
    min_remaining,
    max_age_ms,
  };
}

export async function buildPodioBackpressureSkipResult(
  base = {},
  {
    min_remaining = 100,
    max_age_ms = 5 * 60_000,
    force_reload = false,
  } = {}
) {
  const pressure = await getPodioRateLimitPressureState({
    min_remaining,
    max_age_ms,
    force_reload,
  });

  if (!pressure.active) return null;

  if (pressure.reason === "podio_rate_limit_cooldown_active") {
    return buildPodioCooldownSkipResult(base);
  }

  return {
    ok: true,
    skipped: true,
    reason: pressure.reason,
    retry_after_seconds:
      pressure.observation?.retry_after_seconds ??
      pressure.cooldown?.retry_after_seconds_remaining ??
      pressure.cooldown?.retry_after_seconds ??
      null,
    retry_after_at: pressure.cooldown?.cooldown_until || null,
    podio_backpressure: {
      active: true,
      reason: pressure.reason,
      min_remaining,
      max_age_ms,
      observation: pressure.observation,
    },
    ...base,
  };
}

function cleanRetryMessage(value) {
  return String(value ?? "").trim().toLowerCase();
}

const RETRYABLE_MESSAGE_PATTERNS = [
  /the server took too long to respond/i,
  /timeout of \d+ms exceeded/i,
  /\betimedout\b/i,
  /\beconnreset\b/i,
  /\beconnaborted\b/i,
  /socket hang up/i,
];

export function isRetryablePodioRequestError(err) {
  const status = err?.response?.status ?? 0;
  if (isRetryable(status)) return true;

  const code = cleanRetryMessage(err?.code);
  if (["etimedout", "econnreset", "econnaborted"].includes(code)) {
    return true;
  }

  const message_candidates = [
    err?.response?.data?.error_description,
    err?.response?.data?.error,
    err?.message,
    err?.cause?.message,
  ];

  return message_candidates.some((candidate) => {
    const message = cleanRetryMessage(candidate);
    if (!message) return false;
    return RETRYABLE_MESSAGE_PATTERNS.some((pattern) => pattern.test(message));
  });
}

export function isPodioRateLimitError(err) {
  if (err?.cooldown_active) return true;

  const status = err?.response?.status ?? err?.status ?? 0;
  if (status === 420 || status === 429) return true;

  if (
    clean(err?.data?.error).toLowerCase() === "podio_rate_limit_cooldown_active" ||
    clean(err?.response?.data?.error).toLowerCase() === "podio_rate_limit_cooldown_active"
  ) {
    return true;
  }

  const message_candidates = [
    err?.response?.data?.error_description,
    err?.response?.data?.error,
    err?.message,
    err?.cause?.message,
  ];

  return message_candidates.some((candidate) =>
    /hit the rate limit/i.test(String(candidate ?? ""))
  );
}

export function getPodioRetryAfterSeconds(err, fallback = null) {
  const direct_candidates = [
    err?.retry_after_seconds,
    err?.response?.data?.retry_after_seconds,
    getHeaderValue(err?.headers || err?.response?.headers || {}, "retry-after"),
  ];

  for (const candidate of direct_candidates) {
    const seconds = Number(candidate);
    if (Number.isFinite(seconds) && seconds > 0) {
      return seconds;
    }
  }

  const message_candidates = [
    err?.response?.data?.error_description,
    err?.response?.data?.error,
    err?.message,
    err?.cause?.message,
  ];

  for (const candidate of message_candidates) {
    const match = String(candidate ?? "").match(/wait\s+(\d+)\s+seconds/i);
    if (match) {
      const seconds = Number(match[1]);
      if (Number.isFinite(seconds) && seconds > 0) {
        return seconds;
      }
    }
  }

  return fallback;
}

function buildEmptyPodioCooldownState() {
  return {
    active: false,
    cooldown_started_at: null,
    cooldown_until: null,
    retry_after_seconds: null,
    retry_after_seconds_remaining: null,
    method: null,
    path: null,
    operation: null,
    status: null,
    rate_limit_limit: null,
    rate_limit_remaining: null,
    error_message: null,
    updated_at: null,
  };
}

function toTimestamp(value) {
  if (!value) return null;
  const timestamp = new Date(value).getTime();
  return Number.isNaN(timestamp) ? null : timestamp;
}

function normalizePositiveInteger(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function normalizePodioCooldownState(value = null) {
  if (!value || typeof value !== "object") {
    return buildEmptyPodioCooldownState();
  }

  return {
    ...buildEmptyPodioCooldownState(),
    ...value,
    active: Boolean(value?.active),
    retry_after_seconds: normalizePositiveInteger(value?.retry_after_seconds, null),
    rate_limit_limit: toHeaderNumber(value?.rate_limit_limit),
    rate_limit_remaining: toHeaderNumber(value?.rate_limit_remaining),
    status: toHeaderNumber(value?.status),
    method: clean(value?.method).toUpperCase() || null,
    path: clean(value?.path) || null,
    operation: clean(value?.operation) || null,
    cooldown_started_at: clean(value?.cooldown_started_at) || null,
    cooldown_until: clean(value?.cooldown_until) || null,
    error_message: clean(value?.error_message) || null,
    updated_at: clean(value?.updated_at) || null,
  };
}

function finalizePodioCooldownState(value = null) {
  const state = normalizePodioCooldownState(value);
  const cooldown_until_ts = toTimestamp(state.cooldown_until);

  if (!state.active || cooldown_until_ts === null) {
    return buildEmptyPodioCooldownState();
  }

  const remaining_ms = cooldown_until_ts - Date.now();
  if (remaining_ms <= 0) {
    return buildEmptyPodioCooldownState();
  }

  return {
    ...state,
    active: true,
    retry_after_seconds_remaining: Math.max(1, Math.ceil(remaining_ms / 1000)),
  };
}

async function persistPodioCooldownState(state) {
  const file_path = PODIO_COOLDOWN_STATE_FILE;
  const directory = path.dirname(file_path);
  await fs.mkdir(directory, { recursive: true }).catch(() => {});
  await fs.writeFile(
    file_path,
    JSON.stringify(normalizePodioCooldownState(state), null, 2),
    "utf8"
  );
}

async function loadPodioCooldownState(force_reload = false) {
  if (_podio_cooldown_runtime.loaded && !force_reload) {
    return normalizePodioCooldownState(_podio_cooldown_runtime.state);
  }

  if (_podio_cooldown_runtime.read_promise && !force_reload) {
    return _podio_cooldown_runtime.read_promise;
  }

  const read_promise = fs
    .readFile(PODIO_COOLDOWN_STATE_FILE, "utf8")
    .then((raw) => {
      try {
        return normalizePodioCooldownState(JSON.parse(raw));
      } catch {
        return buildEmptyPodioCooldownState();
      }
    })
    .catch((error) => {
      if (error?.code !== "ENOENT") {
        logger.warn("podio.cooldown_state_read_failed", {
          message: error?.message || null,
          file_path: PODIO_COOLDOWN_STATE_FILE,
        });
      }
      return buildEmptyPodioCooldownState();
    })
    .finally(() => {
      _podio_cooldown_runtime.read_promise = null;
    });

  _podio_cooldown_runtime.read_promise = read_promise;
  const state = await read_promise;
  _podio_cooldown_runtime.state = state;
  _podio_cooldown_runtime.loaded = true;
  return state;
}

export async function clearPodioRateLimitCooldown({
  reason = "manual_clear",
  suppress_log = false,
} = {}) {
  const next_state = {
    ...buildEmptyPodioCooldownState(),
    updated_at: new Date().toISOString(),
  };

  _podio_cooldown_runtime.state = next_state;
  _podio_cooldown_runtime.loaded = true;

  try {
    await persistPodioCooldownState(next_state);
  } catch (error) {
    logger.warn("podio.cooldown_state_write_failed", {
      message: error?.message || null,
      file_path: PODIO_COOLDOWN_STATE_FILE,
      reason,
    });
  }

  if (!suppress_log) {
    logger.info("podio.cooldown_cleared", {
      reason,
    });
  }

  return buildEmptyPodioCooldownState();
}

export async function getPodioRateLimitCooldown({ force_reload = false } = {}) {
  const stored_state = await loadPodioCooldownState(force_reload);
  const current_state = finalizePodioCooldownState(stored_state);

  if (!current_state.active && stored_state?.active) {
    await clearPodioRateLimitCooldown({
      reason: "cooldown_expired",
      suppress_log: true,
    });
    return buildEmptyPodioCooldownState();
  }

  return current_state;
}

export async function activatePodioRateLimitCooldown({
  method = "",
  path = "",
  status = 420,
  headers = {},
  retry_after_seconds = null,
  error = null,
} = {}) {
  const seconds = normalizePositiveInteger(
    retry_after_seconds,
    getPodioRetryAfterSeconds(error, null)
  );

  if (!seconds) {
    return getPodioRateLimitCooldown();
  }

  const existing = await getPodioRateLimitCooldown();
  const now = Date.now();
  const next_until_ts = now + seconds * 1000;
  const existing_until_ts = toTimestamp(existing.cooldown_until) ?? 0;
  const cooldown_until_ts = Math.max(existing_until_ts, next_until_ts);
  const rate_limit = extractRateLimitMeta(headers);
  const next_state = normalizePodioCooldownState({
    active: true,
    cooldown_started_at: existing.active
      ? existing.cooldown_started_at || new Date(now).toISOString()
      : new Date(now).toISOString(),
    cooldown_until: new Date(cooldown_until_ts).toISOString(),
    retry_after_seconds: Math.max(
      seconds,
      normalizePositiveInteger(existing.retry_after_seconds_remaining, 0)
    ),
    method: clean(method).toUpperCase() || null,
    path: clean(path) || null,
    operation: derivePodioOperation(method, path),
    status: Number.isFinite(Number(status)) ? Number(status) : 420,
    rate_limit_limit: rate_limit.rate_limit_limit,
    rate_limit_remaining:
      rate_limit.rate_limit_remaining ?? 0,
    error_message: clean(error?.message) || null,
    updated_at: new Date().toISOString(),
  });

  _podio_cooldown_runtime.state = next_state;
  _podio_cooldown_runtime.loaded = true;

  try {
    await persistPodioCooldownState(next_state);
  } catch (persist_error) {
    logger.warn("podio.cooldown_state_write_failed", {
      message: persist_error?.message || null,
      file_path: PODIO_COOLDOWN_STATE_FILE,
      status: next_state.status,
      path: next_state.path,
      operation: next_state.operation,
    });
  }

  logger.warn("podio.cooldown_activated", {
    ...finalizePodioCooldownState(next_state),
  });

  return getPodioRateLimitCooldown();
}

export async function buildPodioCooldownSkipResult(base = {}) {
  const cooldown = await getPodioRateLimitCooldown();
  return {
    ok: true,
    skipped: true,
    reason: "podio_rate_limit_cooldown_active",
    retry_after_seconds:
      cooldown.retry_after_seconds_remaining ??
      cooldown.retry_after_seconds ??
      null,
    retry_after_at: cooldown.cooldown_until || null,
    podio_cooldown: cooldown,
    ...base,
  };
}

function deriveErrorPath(error) {
  const direct_path =
    clean(error?.path) ||
    clean(error?.config?.path) ||
    clean(error?.response?.config?.path);
  if (direct_path) return direct_path;

  const raw_url =
    clean(error?.config?.url) ||
    clean(error?.response?.config?.url) ||
    "";
  if (!raw_url) return null;

  try {
    const parsed = new URL(raw_url);
    return `${parsed.pathname}${parsed.search || ""}` || null;
  } catch {
    return raw_url.replace(PODIO_API_BASE, "") || null;
  }
}

export function serializePodioError(error) {
  const headers = error?.headers || error?.response?.headers || {};
  const rate_limit = extractRateLimitMeta(headers);
  const method =
    clean(error?.method || error?.config?.method || error?.response?.config?.method)
      .toUpperCase() || null;
  const path = deriveErrorPath(error);
  const retry_after_seconds = getPodioRetryAfterSeconds(error, null);

  return {
    name: clean(error?.name) || null,
    message: clean(error?.message) || "Unknown Podio error",
    stack: error?.stack || error?.cause?.stack || null,
    code: clean(error?.code) || null,
    status:
      Number.isFinite(Number(error?.status)) ||
      Number.isFinite(Number(error?.response?.status))
        ? Number(error?.status ?? error?.response?.status)
        : null,
    method,
    path,
    operation:
      clean(error?.operation) ||
      (method && path ? derivePodioOperation(method, path) : null),
    retry_after_seconds,
    rate_limit_limit:
      toHeaderNumber(error?.rate_limit_limit) ?? rate_limit.rate_limit_limit,
    rate_limit_remaining:
      toHeaderNumber(error?.rate_limit_remaining) ?? rate_limit.rate_limit_remaining,
    cooldown_until: clean(error?.cooldown_until) || null,
    cooldown_active: Boolean(error?.cooldown_active),
    data_error: clean(error?.data?.error || error?.response?.data?.error) || null,
    data_error_description:
      clean(
        error?.data?.error_description || error?.response?.data?.error_description
      ) || null,
  };
}

function stableSerialize(value) {
  if (value === null || value === undefined) return "null";
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableSerialize(entry)).join(",")}]`;
  }
  if (typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableSerialize(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function normalizeCacheTtlMs(value) {
  const ttl = Number(value);
  if (!Number.isFinite(ttl) || ttl <= 0) return 0;
  return Math.min(Math.floor(ttl), FILTER_RESULT_CACHE_MAX_TTL_MS);
}

async function readThroughFilterResultCache(cache_key, ttl_ms, loader) {
  const ttl = normalizeCacheTtlMs(ttl_ms);
  if (!ttl) {
    return loader();
  }

  const now = Date.now();
  const cached = _filter_result_cache.get(cache_key);

  if (cached?.value && cached.expires_at > now) {
    return cached.value;
  }

  if (cached?.promise) {
    return cached.promise;
  }

  if (cached && cached.expires_at <= now) {
    _filter_result_cache.delete(cache_key);
  }

  const promise = Promise.resolve()
    .then(loader)
    .then((value) => {
      _filter_result_cache.set(cache_key, {
        value,
        expires_at: Date.now() + ttl,
      });
      return value;
    })
    .catch((error) => {
      _filter_result_cache.delete(cache_key);
      throw error;
    });

  _filter_result_cache.set(cache_key, {
    promise,
    expires_at: now + ttl,
  });

  return promise;
}

export function clearPodioFilterResultCache() {
  _filter_result_cache.clear();
}

export function recordPodioRateLimitObservation({
  method = "",
  path = "",
  status = null,
  duration_ms = null,
  attempt = null,
  headers = {},
  retry_after_seconds = null,
} = {}) {
  const operation = derivePodioOperation(method, path);
  const rate_limit = extractRateLimitMeta(headers);
  const low_remaining_threshold = resolveLowRateLimitThreshold(rate_limit.rate_limit_remaining);
  const observation = {
    observed: Boolean(
      rate_limit.rate_limit_limit !== null ||
        rate_limit.rate_limit_remaining !== null ||
        retry_after_seconds !== null
    ),
    observed_at: new Date().toISOString(),
    method: clean(method).toUpperCase() || null,
    path: clean(path) || null,
    operation,
    status: Number.isFinite(Number(status)) ? Number(status) : null,
    duration_ms: Number.isFinite(Number(duration_ms)) ? Number(duration_ms) : null,
    attempt: Number.isFinite(Number(attempt)) ? Number(attempt) : null,
    rate_limit_limit: rate_limit.rate_limit_limit,
    rate_limit_remaining: rate_limit.rate_limit_remaining,
    retry_after_seconds:
      Number.isFinite(Number(retry_after_seconds)) ? Number(retry_after_seconds) : null,
    low_remaining_threshold,
  };

  if (observation.observed) {
    _latest_rate_limit_status = observation;
    _podio_rate_limit_status_runtime.state = observation;
    _podio_rate_limit_status_runtime.loaded = true;

    if (
      observation.retry_after_seconds !== null ||
      observation.low_remaining_threshold !== null
    ) {
      void persistLatestRateLimitStatus(observation).catch((error) => {
        logger.warn("podio.rate_limit_status_write_failed", {
          message: error?.message || null,
          file_path: PODIO_RATE_LIMIT_STATUS_FILE,
        });
      });
    }
  }

  if (
    observation.rate_limit_remaining !== null &&
    observation.rate_limit_remaining > LOW_RATE_LIMIT_THRESHOLDS[0]
  ) {
    _last_low_rate_limit_warning_threshold = null;
  }

  return observation;
}

function maybeWarnOnLowRateLimit(observation) {
  const threshold = observation?.low_remaining_threshold ?? null;
  if (threshold === null) return;

  if (
    _last_low_rate_limit_warning_threshold === null ||
    threshold < _last_low_rate_limit_warning_threshold
  ) {
    _last_low_rate_limit_warning_threshold = threshold;
    logger.warn("podio.rate_limit_low", observation);
  }
}

function logPodioResponse({
  level = "info",
  event = "podio.request_completed",
  method = "",
  path = "",
  status = null,
  duration_ms = null,
  attempt = null,
  headers = {},
  retry_after_seconds = null,
  retryable = false,
  will_retry = false,
  error = null,
} = {}) {
  const observation = recordPodioRateLimitObservation({
    method,
    path,
    status,
    duration_ms,
    attempt,
    headers,
    retry_after_seconds,
  });

  const meta = {
    ...observation,
    retryable: Boolean(retryable),
    will_retry: Boolean(will_retry),
    ...(error ? { error } : {}),
  };

  if (level === "warn") {
    logger.warn(event, meta);
  } else if (level === "error") {
    logger.error(event, meta);
  } else {
    logger.info(event, meta);
  }

  maybeWarnOnLowRateLimit(observation);
}

function calcBackoff(attempt) {
  const exponential = RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
  const capped = Math.min(RETRY_MAX_DELAY_MS, exponential);
  return Math.floor(Math.random() * capped);
}

async function _executeWithRetry(buildConfig, attempt = 0) {
  const config = await buildConfig();
  const started_at = Date.now();

  try {
    const response = await axios({ timeout: REQUEST_TIMEOUT_MS, ...config });
    logPodioResponse({
      level: "info",
      event: "podio.request_completed",
      method: config.method,
      path: config.path,
      status: response?.status ?? null,
      duration_ms: Date.now() - started_at,
      attempt: attempt + 1,
      headers: response?.headers || {},
    });
    return response;
  } catch (err) {
    logPodioAxiosFailure(err, {
      url: config.url,
      method: config.method,
      data: config.data ?? null,
      params: config.params ?? null,
      headers: config.headers ?? null,
    });

    const retry_after_seconds = getPodioRetryAfterSeconds(err, null);
    const retryable = isRetryablePodioRequestError(err);
    const rate_limited = isPodioRateLimitError(err);
    const will_retry = attempt < MAX_RETRIES && retryable && !rate_limited;

    logPodioResponse({
      level: "warn",
      event: "podio.request_failed",
      method: config.method,
      path: config.path,
      status: err?.response?.status ?? null,
      duration_ms: Date.now() - started_at,
      attempt: attempt + 1,
      headers: err?.response?.headers || {},
      retry_after_seconds,
      retryable,
      will_retry,
      error: {
        message: err?.message || null,
        code: err?.code || null,
      },
    });

    if (rate_limited) {
      await activatePodioRateLimitCooldown({
        method: config.method,
        path: config.path,
        status: err?.response?.status ?? 420,
        headers: err?.response?.headers || {},
        retry_after_seconds,
        error: err,
      });
      throw err;
    }

    if (will_retry) {
      await sleep(calcBackoff(attempt));
      return _executeWithRetry(buildConfig, attempt + 1);
    }

    throw err;
  }
}

// ══════════════════════════════════════════════════════════════════════════
// CORE REQUEST
// ══════════════════════════════════════════════════════════════════════════

export async function podioRequest(method, path, data = null, params = null) {
  const active_cooldown = await getPodioRateLimitCooldown();
  if (active_cooldown.active) {
    logger.warn("podio.request_skipped_cooldown", {
      method: clean(method).toUpperCase() || null,
      path: clean(path) || null,
      operation: derivePodioOperation(method, path),
      retry_after_seconds:
        active_cooldown.retry_after_seconds_remaining ??
        active_cooldown.retry_after_seconds ??
        null,
      cooldown_until: active_cooldown.cooldown_until || null,
      rate_limit_remaining: active_cooldown.rate_limit_remaining ?? null,
      rate_limit_limit: active_cooldown.rate_limit_limit ?? null,
    });

    throw new PodioError(
      `Podio cooldown active until ${active_cooldown.cooldown_until}`,
      {
        method,
        path,
        status: active_cooldown.status || 420,
        data: {
          error: "podio_rate_limit_cooldown_active",
        },
        headers: {
          "x-rate-limit-limit": active_cooldown.rate_limit_limit,
          "x-rate-limit-remaining": active_cooldown.rate_limit_remaining,
        },
        operation: derivePodioOperation(method, path),
        retry_after_seconds:
          active_cooldown.retry_after_seconds_remaining ??
          active_cooldown.retry_after_seconds ??
          null,
        rate_limit_limit: active_cooldown.rate_limit_limit ?? null,
        rate_limit_remaining:
          active_cooldown.rate_limit_remaining ?? null,
        cooldown_until: active_cooldown.cooldown_until || null,
        cooldown_active: true,
      }
    );
  }

  const buildConfig = async () => ({
    method,
    url: `${PODIO_API_BASE}${path}`,
    path,
    headers: {
      Authorization: `OAuth2 ${await getToken()}`,
    },
    ...(data && { data }),
    ...(params && { params }),
  });

  try {
    const res = await _executeWithRetry(buildConfig);
    return res.data;
  } catch (err) {
    if (err?.response?.status === 401) {
      invalidateToken();

      try {
        const res = await _executeWithRetry(buildConfig);
        return res.data;
      } catch (retryErr) {
        throw toPodioError(retryErr, method, path);
      }
    }

    throw toPodioError(err, method, path);
  }
}

// ══════════════════════════════════════════════════════════════════════════
// CRUD
// ══════════════════════════════════════════════════════════════════════════

export async function getItem(item_id) {
  const item = await podioRequest("get", `/item/${item_id}`);
  if (item?.item_id && item?.app?.app_id) {
    _item_app_id_cache.set(String(item.item_id), Number(item.app.app_id));
  }
  return item;
}

export async function deleteItem(item_id) {
  _item_app_id_cache.delete(String(item_id));
  return podioRequest("delete", `/item/${item_id}`);
}

export async function createItem(app_id, fields) {
  const normalized_fields = hasAttachedSchema(app_id)
    ? normalizePodioFieldMap(app_id, fields)
    : fields;

  const result = await podioRequest("post", `/item/app/${app_id}/`, { fields: normalized_fields });
  if (result?.item_id) {
    _item_app_id_cache.set(String(result.item_id), Number(app_id));
  }
  return result;
}

async function resolveItemAppId(item_id) {
  const cache_key = String(item_id);
  if (_item_app_id_cache.has(cache_key)) {
    return _item_app_id_cache.get(cache_key);
  }

  const item = await getItem(item_id);
  return Number(item?.app?.app_id || 0) || null;
}

export async function updateItem(item_id, fields, revision = null) {
  const app_id = await resolveItemAppId(item_id);
  const normalized_fields =
    app_id && hasAttachedSchema(app_id)
      ? normalizePodioFieldMap(app_id, fields)
      : fields;

  const payload = {
    fields: normalized_fields,
    ...(revision !== null && { revision }),
  };

  return podioRequest("put", `/item/${item_id}`, payload);
}

// ══════════════════════════════════════════════════════════════════════════
// FILTER & SEARCH
// ══════════════════════════════════════════════════════════════════════════

export function filterAppItems(app_id, filters = {}, limitOrOptions = {}, maybeOffset = 0) {
  let limit = 50;
  let offset = 0;
  let sort_by;
  let sort_desc;
  let remember;
  let cache_ttl_ms = 0;

  if (typeof limitOrOptions === "number") {
    limit = limitOrOptions;
    offset = Number.isFinite(Number(maybeOffset)) ? Number(maybeOffset) : 0;
  } else {
    const options = limitOrOptions || {};
    limit = options.limit ?? 50;
    offset = options.offset ?? 0;
    sort_by = options.sort_by;
    sort_desc = options.sort_desc;
    remember = options.remember;
    cache_ttl_ms = options.cache_ttl_ms ?? 0;
  }

  const payload = {
    filters: hasAttachedSchema(app_id)
      ? normalizePodioFilterMap(app_id, filters)
      : filters,
    limit,
    offset,
    ...(sort_by && { sort_by }),
    ...(typeof sort_desc === "boolean" && { sort_desc }),
    ...(typeof remember === "boolean" && { remember }),
  };

  const path = `/item/app/${app_id}/filter/`;
  const cache_key = `filter:${app_id}:${stableSerialize(payload)}`;

  return readThroughFilterResultCache(cache_key, cache_ttl_ms, () =>
    podioRequest("post", path, payload)
  );
}

export function filterAppItemsByView(
  app_id,
  view_id,
  limitOrOptions = {},
  maybeOffset = 0
) {
  let limit = 50;
  let offset = 0;
  let sort_by;
  let sort_desc;
  let remember;
  let cache_ttl_ms = 0;

  if (typeof limitOrOptions === "number") {
    limit = limitOrOptions;
    offset = Number.isFinite(Number(maybeOffset)) ? Number(maybeOffset) : 0;
  } else {
    const options = limitOrOptions || {};
    limit = options.limit ?? 50;
    offset = options.offset ?? 0;
    sort_by = options.sort_by;
    sort_desc = options.sort_desc;
    remember = options.remember;
    cache_ttl_ms = options.cache_ttl_ms ?? 0;
  }

  const payload = {
    limit,
    offset,
    ...(sort_by && { sort_by }),
    ...(typeof sort_desc === "boolean" && { sort_desc }),
    ...(typeof remember === "boolean" && { remember }),
  };

  const path = `/item/app/${app_id}/filter/${view_id}/`;
  const cache_key = `filter_view:${app_id}:${view_id}:${stableSerialize(payload)}`;

  return readThroughFilterResultCache(cache_key, cache_ttl_ms, () =>
    podioRequest("post", path, payload)
  );
}

export function getAppViews(app_id, { include_standard_views = false } = {}) {
  return podioRequest("get", `/view/app/${app_id}/`, null, {
    ...(include_standard_views ? { include_standard_views: true } : {}),
  });
}

export function getAppView(app_id, view_id_or_name) {
  return podioRequest(
    "get",
    `/view/app/${app_id}/${encodeURIComponent(String(view_id_or_name))}`
  );
}

export async function fetchAllItems(app_id, filters = {}, options = {}) {
  const PAGE_SIZE = options.page_size ?? 500;
  let offset = 0;
  let all = [];

  while (true) {
    const res = await filterAppItems(app_id, filters, {
      ...options,
      limit: PAGE_SIZE,
      offset,
    });

    const items = res?.items ?? [];
    all = all.concat(items);

    if (items.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return all;
}

export async function getFirstMatchingItem(app_id, filters = {}, options = {}) {
  const res = await filterAppItems(app_id, filters, {
    limit: 1,
    offset: 0,
    ...options,
  });

  return res?.items?.[0] ?? null;
}

export function findByField(app_id, external_id, value, options = {}) {
  return getFirstMatchingItem(app_id, { [external_id]: value }, options);
}

export function createMessageEvent(fields = {}) {
  return createItem(APP_IDS.message_events, fields);
}

export function updateMessageEvent(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export function updateBrain(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

// ══════════════════════════════════════════════════════════════════════════
// FIELD READERS
// ══════════════════════════════════════════════════════════════════════════

export function getFieldMap(item) {
  if (!Array.isArray(item?.fields)) return {};

  return item.fields.reduce((acc, field) => {
    if (field?.external_id) acc[field.external_id] = field;
    return acc;
  }, {});
}

export function getField(item, external_id) {
  if (!item?.fields) return null;

  if (Array.isArray(item.fields)) {
    return item.fields.find((f) => f?.external_id === external_id) ?? null;
  }

  return item.fields[external_id] ?? null;
}

export function getFieldValues(item, external_id) {
  const field = getField(item, external_id);
  return Array.isArray(field?.values) ? field.values : [];
}

export function getTextValue(item, external_id, fallback = "") {
  const first = getFieldValues(item, external_id)[0];
  if (!first) return fallback;

  return (
    (typeof first.value === "string" && first.value) ||
    (typeof first.value?.text === "string" && first.value.text) ||
    (typeof first.value?.title === "string" && first.value.title) ||
    (typeof first.value?.formatted === "string" && first.value.formatted) ||
    (typeof first.value?.value === "string" && first.value.value) ||
    (typeof first.formatted === "string" && first.formatted) ||
    fallback
  );
}

export function getNumberValue(item, external_id, fallback = null) {
  const first = getFieldValues(item, external_id)[0];
  const candidates = [first?.value, first?.value?.value];

  for (const raw of candidates) {
    if (typeof raw === "number") return raw;
    if (typeof raw === "string" && raw.trim() !== "") {
      const n = Number(raw);
      if (!Number.isNaN(n)) return n;
    }
  }

  return fallback;
}

export function getMoneyValue(item, external_id, fallback = null) {
  const first = getFieldValues(item, external_id)[0];
  const raw = first?.value?.value;

  if (typeof raw === "number") return raw;
  if (typeof raw === "string" && raw.trim() !== "") {
    const n = Number(raw);
    return Number.isNaN(n) ? fallback : n;
  }

  return fallback;
}

export function getDateValue(item, external_id, fallback = null) {
  const first = getFieldValues(item, external_id)[0];
  return first?.start ?? first?.value?.start ?? fallback;
}

export function getCategoryValues(item, external_id) {
  return getFieldValues(item, external_id)
    .map((v) => v?.value?.text ?? (typeof v?.value === "string" ? v.value : null))
    .filter(Boolean);
}

export function getCategoryValue(item, external_id, fallback = null) {
  return getCategoryValues(item, external_id)[0] ?? fallback;
}

export function getAppReferenceIds(item, external_id) {
  return getFieldValues(item, external_id)
    .map((v) => v?.value?.item_id ?? v?.item_id ?? null)
    .filter(Boolean);
}

export function getFirstAppReferenceId(item, external_id, fallback = null) {
  return getAppReferenceIds(item, external_id)[0] ?? fallback;
}

export function getPhoneValue(item, external_id, fallback = "") {
  const first = getFieldValues(item, external_id)[0];
  const value = first?.value;

  if (!value) return fallback;
  if (typeof value === "string") return value;
  if (typeof value?.value === "string") return value.value;
  if (Array.isArray(value) && typeof value[0]?.value === "string") return value[0].value;

  return fallback;
}

// ══════════════════════════════════════════════════════════════════════════
// NORMALIZATION
// ══════════════════════════════════════════════════════════════════════════

export function normalizeBooleanLabel(value) {
  const raw = String(value ?? "").trim().toLowerCase();

  if (["yes", "true", "✅ confirmed", "✅ cleared", "active"].includes(raw)) return "yes";
  if (["no", "false", "❌ failed", "blocked", "paused", "retired"].includes(raw)) return "no";

  return raw;
}

const LANGUAGE_MAP = {
  english: "English",
  spanish: "Spanish",
  portuguese: "Portuguese",
  italian: "Italian",
  hebrew: "Hebrew",
  mandarin: "Mandarin",
  korean: "Korean",
  vietnamese: "Vietnamese",
  polish: "Polish",
  arabic: "Arabic",
  hindi: "Hindi",
  french: "French",
  russian: "Russian",
  japanese: "Japanese",
  farsi: "Farsi",
  persian: "Persian",
  german: "German",
  greek: "Greek",
  thai: "Thai",
  pashto: "Pashto",
  tagalog: "Tagalog",
  cantonese: "Cantonese",
  turkish: "Turkish",
  swahili: "Swahili",
  somali: "Somali",
  amharic: "Amharic",
  yoruba: "Yoruba",
  hindi: "Asian Indian (Hindi or Other)",
  "asian indian (hindi or other)": "Asian Indian (Hindi or Other)",
};

export function normalizeLanguage(value) {
  return LANGUAGE_MAP[String(value ?? "").trim().toLowerCase()] ?? value ?? "English";
}

export function normalizeStage(value) {
  return normalizeConversationStage(value);
}

export function normalizeUsPhone10(value) {
  const digits = String(value ?? "").replace(/\D/g, "");
  return digits.length === 11 && digits.startsWith("1") ? digits.slice(1) : digits;
}

export function toCanonicalUsE164(value) {
  const digits = normalizeUsPhone10(value);
  return digits.length === 10 ? `+1${digits}` : null;
}

export function safeCategoryEquals(value, expected) {
  return (
    String(value ?? "").trim().toLowerCase() ===
    String(expected ?? "").trim().toLowerCase()
  );
}

export function isRevisionLimitExceeded(error) {
  if (!(error instanceof PodioError)) return false;
  return String(error?.message || "")
    .toLowerCase()
    .includes("this item has exceeded the maximum number of revisions");
}

export default {
  PodioError,
  invalidateToken,
  getLatestPodioRateLimitStatus,
  getPodioRateLimitPressureState,
  getPodioRetryAfterSeconds,
  isPodioRateLimitError,
  isRetryablePodioRequestError,
  podioRequest,
  buildPodioBackpressureSkipResult,
  recordPodioRateLimitObservation,
  resetPodioRateLimitObservability,
  getItem,
  deleteItem,
  createItem,
  updateItem,
  filterAppItems,
  filterAppItemsByView,
  getAppViews,
  getAppView,
  fetchAllItems,
  getFirstMatchingItem,
  findByField,
  createMessageEvent,
  updateMessageEvent,
  updateBrain,
  getFieldMap,
  getField,
  getFieldValues,
  getTextValue,
  getNumberValue,
  getMoneyValue,
  getDateValue,
  getCategoryValues,
  getCategoryValue,
  getAppReferenceIds,
  getFirstAppReferenceId,
  getPhoneValue,
  normalizeBooleanLabel,
  normalizeLanguage,
  normalizeStage,
  normalizeUsPhone10,
  toCanonicalUsE164,
  safeCategoryEquals,
  isRevisionLimitExceeded,
};
import { normalizeStage as normalizeConversationStage } from "@/lib/config/stages.js";
