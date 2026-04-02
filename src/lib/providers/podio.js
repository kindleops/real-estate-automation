import axios from "axios";
import APP_IDS from "@/lib/config/app-ids.js";
import {
  hasAttachedSchema,
  normalizePodioFieldMap,
} from "@/lib/podio/schema.js";

function clean(value) {
  return String(value ?? "").trim();
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
  constructor(message, { method, path, status, data } = {}) {
    super(message);
    this.name = "PodioError";
    this.method = method ?? null;
    this.path = path ?? null;
    this.status = status ?? null;
    this.data = data ?? null;
  }
}

function toPodioError(err, method, path) {
  const status = err?.response?.status ?? null;
  const data = err?.response?.data ?? null;
  const message =
    data?.error_description ??
    data?.error ??
    err?.message ??
    "Unknown Podio error";

  return new PodioError(message, { method, path, status, data });
}

// ══════════════════════════════════════════════════════════════════════════
// TOKEN MANAGEMENT
// ══════════════════════════════════════════════════════════════════════════

let _access_token = null;
let _expires_at = 0;
let _refresh_promise = null;
const _item_app_id_cache = new Map();

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

  const res = await axios.post(PODIO_OAUTH_URL, form, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: REQUEST_TIMEOUT_MS,
  });

  _access_token = res.data.access_token;
  _expires_at = Date.now() + res.data.expires_in * 1000;

  return _access_token;
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

function calcBackoff(attempt) {
  const exponential = RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
  const capped = Math.min(RETRY_MAX_DELAY_MS, exponential);
  return Math.floor(Math.random() * capped);
}

async function _executeWithRetry(buildConfig, attempt = 0) {
  const config = await buildConfig();

  try {
    return await axios({ timeout: REQUEST_TIMEOUT_MS, ...config });
  } catch (err) {
    if (attempt < MAX_RETRIES && isRetryablePodioRequestError(err)) {
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
  const buildConfig = async () => ({
    method,
    url: `${PODIO_API_BASE}${path}`,
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

  return podioRequest("post", `/item/app/${app_id}/`, { fields: normalized_fields });
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
  }

  const payload = {
    filters: hasAttachedSchema(app_id)
      ? normalizePodioFieldMap(app_id, filters)
      : filters,
    limit,
    offset,
    ...(sort_by && { sort_by }),
    ...(typeof sort_desc === "boolean" && { sort_desc }),
    ...(typeof remember === "boolean" && { remember }),
  };

  return podioRequest("post", `/item/app/${app_id}/filter/`, payload);
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
  }

  const payload = {
    limit,
    offset,
    ...(sort_by && { sort_by }),
    ...(typeof sort_desc === "boolean" && { sort_desc }),
    ...(typeof remember === "boolean" && { remember }),
  };

  return podioRequest("post", `/item/app/${app_id}/filter/${view_id}/`, payload);
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

export default {
  PodioError,
  invalidateToken,
  podioRequest,
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
};
import { normalizeStage as normalizeConversationStage } from "@/lib/config/stages.js";
