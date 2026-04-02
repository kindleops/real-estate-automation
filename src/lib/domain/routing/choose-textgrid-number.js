// ─── choose-textgrid-number.js ────────────────────────────────────────────
import APP_IDS from "@/lib/config/app-ids.js";
import {
  normalizeMarketLabel,
  resolveMarketSendingProfile,
} from "@/lib/config/market-sending-zones.js";
import { TEXTGRID_NUMBER_FIELDS } from "@/lib/podio/apps/textgrid-numbers.js";

import {
  fetchAllItems,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getNumberValue,
  getPhoneValue,
  getTextValue,
} from "@/lib/providers/podio.js";

import { normalizePhone } from "@/lib/providers/textgrid.js";
import { info, warn } from "@/lib/logging/logger.js";

const DEFAULT_FETCH_LIMIT = 200;

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function hashString(value) {
  let hash = 0;
  const text = String(value ?? "");
  for (let i = 0; i < text.length; i += 1) {
    hash = (hash << 5) - hash + text.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

function rotateCandidate(items, rotation_key = null) {
  if (!Array.isArray(items) || items.length === 0) return null;
  if (!rotation_key) return items[0];

  const index = Math.abs(hashString(rotation_key)) % items.length;
  return items[index];
}

function isPositiveCategory(value) {
  const raw = lower(value);
  return [
    "yes",
    "true",
    "active",
    "enabled",
    "available",
    "on",
    "_ active",
    "_ warming up",
  ].includes(raw);
}

function isNegativeCategory(value) {
  const raw = lower(value);
  return [
    "no",
    "false",
    "inactive",
    "disabled",
    "retired",
    "blocked",
    "off",
    "_ paused",
    "_ flagged",
    "⚫ retired",
  ].includes(raw);
}

function isPaused(record, now = new Date()) {
  if (!record) return true;
  if (record.hard_pause && isPositiveCategory(record.hard_pause)) return true;
  if (!record.pause_until) return false;

  const pause_until_ts = new Date(record.pause_until).getTime();
  if (Number.isNaN(pause_until_ts)) return false;

  return pause_until_ts > now.getTime();
}

function extractNumberRecord(item) {
  const outbound_phone =
    getPhoneValue(item, TEXTGRID_NUMBER_FIELDS.title, "") ||
    getTextValue(item, TEXTGRID_NUMBER_FIELDS.title, "");

  const normalized_phone = normalizePhone(outbound_phone);

  return {
    item_id: item?.item_id ?? null,
    raw: item,

    title: getTextValue(item, TEXTGRID_NUMBER_FIELDS.title, ""),
    friendly_name: getTextValue(item, TEXTGRID_NUMBER_FIELDS.friendly_name, ""),
    phone_number: outbound_phone,
    normalized_phone,

    market_name: getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.market, null),
    market_id: getFirstAppReferenceId(item, TEXTGRID_NUMBER_FIELDS.markets, null) || null,

    status: getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.status, null),
    hard_pause: getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.hard_pause, null),
    pause_reason: getTextValue(item, TEXTGRID_NUMBER_FIELDS.pause_reason, ""),
    pause_until: getDateValue(item, TEXTGRID_NUMBER_FIELDS.pause_until, null),

    priority:
      getNumberValue(item, TEXTGRID_NUMBER_FIELDS.rotation_weight, null) ??
      0,

    daily_limit:
      getNumberValue(item, TEXTGRID_NUMBER_FIELDS.daily_send_cap, null) ??
      null,

    daily_sent:
      getNumberValue(item, TEXTGRID_NUMBER_FIELDS.sent_today, null) ??
      0,

    allowed_send_window_start_local: getTextValue(
      item,
      TEXTGRID_NUMBER_FIELDS.allowed_send_window_start_local,
      ""
    ),
    allowed_send_window_end_local: getTextValue(
      item,
      TEXTGRID_NUMBER_FIELDS.allowed_send_window_end_local,
      ""
    ),
    area_code: normalized_phone.replace(/^\+1/, "").slice(0, 3),
  };
}

function isUsableNumber(record) {
  if (!record?.item_id) return false;
  if (!record?.normalized_phone) return false;

  if (record.status && isNegativeCategory(record.status)) return false;
  if (isPaused(record)) return false;

  if (
    record.daily_limit !== null &&
    Number(record.daily_limit) > 0 &&
    Number(record.daily_sent || 0) >= Number(record.daily_limit)
  ) {
    return false;
  }

  return true;
}

function scoreNumber(record, {
  preferred_market_name = null,
  preferred_area_code = null,
} = {}) {
  let score = 0;

  if (record.status && isPositiveCategory(record.status)) score += 30;
  if (record.priority) score += Number(record.priority) * 5;

  if (
    preferred_market_name &&
    record.market_name &&
    normalizeMarketLabel(record.market_name) === normalizeMarketLabel(preferred_market_name)
  ) {
    score += 30;
  }

  if (
    preferred_area_code &&
    record.area_code &&
    clean(record.area_code) === clean(preferred_area_code)
  ) {
    score += 12;
  }

  if (
    record.daily_limit !== null &&
    Number(record.daily_limit) > 0
  ) {
    const used = Number(record.daily_sent || 0);
    const cap = Number(record.daily_limit);
    const remaining_ratio = Math.max(0, (cap - used) / cap);
    score += remaining_ratio * 15;
  } else {
    score += 5;
  }

  return score;
}

export async function loadUsableTextgridNumbers() {
  const items = await fetchAllItems(
    APP_IDS.textgrid_numbers,
    {},
    {
      page_size: DEFAULT_FETCH_LIMIT,
    }
  );

  return uniq(items)
    .map(extractNumberRecord)
    .filter((record) => isUsableNumber(record));
}

function chooseBestCandidate({
  candidates,
  rotation_key = null,
}) {
  if (!candidates.length) return null;

  const sorted = [...candidates].sort((a, b) => b.score - a.score);
  const topScore = sorted[0]?.score ?? 0;
  const topCluster = sorted.filter((x) => x.score >= topScore - 8);

  return rotateCandidate(topCluster, rotation_key);
}

function buildSelectionDiagnostics({
  market_id = null,
  raw_seller_market = null,
  resolution = null,
  all_numbers = [],
  allowed_candidates = [],
  allowed_market_counts = [],
  selected = null,
  selection_reason = null,
  fallback_reason = null,
} = {}) {
  return {
    market_id: market_id || null,
    raw_seller_market: clean(raw_seller_market) || null,
    normalized_seller_market: resolution?.normalized_raw_market || null,
    resolved_sending_zone: resolution?.sending_zone || null,
    allowed_phone_markets: Array.isArray(resolution?.allowed_phone_markets)
      ? [...resolution.allowed_phone_markets]
      : [],
    allowed_market_counts,
    available_number_count: Array.isArray(all_numbers) ? all_numbers.length : 0,
    allowed_candidate_count: Array.isArray(allowed_candidates) ? allowed_candidates.length : 0,
    selected_item_id: selected?.item_id || null,
    selected_phone_number: selected?.normalized_phone || null,
    selected_phone_market: selected?.market_name || null,
    selection_reason: clean(selection_reason) || null,
    fallback_reason: clean(fallback_reason) || null,
  };
}

function buildNoSelectionResult({
  market_id = null,
  raw_seller_market = null,
  resolution = null,
  all_numbers = [],
  allowed_candidates = [],
  allowed_market_counts = [],
  selection_reason = null,
} = {}) {
  return {
    item_id: null,
    id: null,
    textgrid_number_item_id: null,
    normalized_phone: "",
    phone_number: "",
    market_name: null,
    selection_reason: clean(selection_reason) || null,
    fallback_reason: null,
    selection_diagnostics: buildSelectionDiagnostics({
      market_id,
      raw_seller_market,
      resolution,
      all_numbers,
      allowed_candidates,
      allowed_market_counts,
      selected: null,
      selection_reason,
      fallback_reason: null,
    }),
  };
}

export async function chooseTextgridNumber({
  context = null,
  classification = null,
  route = null,
  preferred_language = null,
  rotation_key = null,
  candidate_records = null,
} = {}) {
  const market_id =
    context?.ids?.market_id ||
    null;

  const raw_seller_market =
    context?.summary?.market_name ||
    null;

  const market_area_code =
    context?.summary?.market_area_code ||
    null;

  const language =
    preferred_language ||
    route?.language ||
    classification?.language ||
    context?.summary?.language_preference ||
    "English";

  info("routing.choose_textgrid_number_started", {
    phone_item_id: context?.ids?.phone_item_id || null,
    market_id,
    raw_seller_market,
    language,
  });

  const all_numbers = Array.isArray(candidate_records)
    ? candidate_records.filter((record) => isUsableNumber(record))
    : await loadUsableTextgridNumbers();

  if (!all_numbers.length) {
    warn("routing.choose_textgrid_number_none_available", {
      market_id,
      raw_seller_market,
      language,
    });
    return buildNoSelectionResult({
      market_id,
      raw_seller_market,
      resolution: null,
      all_numbers,
      allowed_candidates: [],
      allowed_market_counts: [],
      selection_reason: "no_textgrid_numbers_available",
    });
  }

  const resolution = resolveMarketSendingProfile(raw_seller_market);

  if (!resolution.ok) {
    warn("routing.choose_textgrid_number_market_unmapped", {
      market_id,
      raw_seller_market,
      reason: resolution.reason,
      language,
    });
    return buildNoSelectionResult({
      market_id,
      raw_seller_market,
      resolution,
      all_numbers,
      allowed_candidates: [],
      allowed_market_counts: [],
      selection_reason: resolution.reason,
    });
  }

  const scored = all_numbers.map((record) => ({
    ...record,
    score: scoreNumber(record, {
      preferred_market_name: raw_seller_market,
      preferred_area_code: market_area_code,
    }),
  }));

  const allowed_markets_by_key = new Map(
    resolution.allowed_phone_markets.map((market_name, index) => [
      lower(normalizeMarketLabel(market_name)),
      {
        market_name,
        index,
      },
    ])
  );
  const allowed_candidates = scored.filter((record) =>
    allowed_markets_by_key.has(lower(normalizeMarketLabel(record.market_name)))
  );
  const allowed_market_counts = resolution.allowed_phone_markets.map((market_name) => ({
    market_name,
    candidate_count: allowed_candidates.filter(
      (record) => lower(normalizeMarketLabel(record.market_name)) === lower(normalizeMarketLabel(market_name))
    ).length,
  }));

  let selected = null;
  let selection_reason = "no_usable_phone_numbers_in_allowed_markets";
  let fallback_reason = null;

  for (const { market_name, candidate_count } of allowed_market_counts) {
    if (!candidate_count) continue;

    const market_pool = allowed_candidates.filter(
      (record) => lower(normalizeMarketLabel(record.market_name)) === lower(normalizeMarketLabel(market_name))
    );

    selected = chooseBestCandidate({
      candidates: market_pool,
      rotation_key:
        rotation_key ||
        `${context?.ids?.phone_item_id || "no-phone"}:${resolution.sending_zone}:${market_name}`,
    });

    if (selected) {
      const selected_market_index = allowed_markets_by_key.get(
        lower(normalizeMarketLabel(selected.market_name))
      )?.index ?? 0;
      selection_reason =
        selected_market_index === 0
          ? "primary_allowed_phone_market_match"
          : "fallback_allowed_phone_market_match";
      fallback_reason =
        selected_market_index === 0 ? null : "higher_priority_allowed_phone_markets_unavailable";
      break;
    }
  }

  if (!selected) {
    warn("routing.choose_textgrid_number_no_match", {
      market_id,
      raw_seller_market,
      resolved_sending_zone: resolution.sending_zone,
      allowed_phone_markets: resolution.allowed_phone_markets,
      language,
      available_count: scored.length,
      allowed_candidate_count: allowed_candidates.length,
    });
    return buildNoSelectionResult({
      market_id,
      raw_seller_market,
      resolution,
      all_numbers,
      allowed_candidates,
      allowed_market_counts,
      selection_reason,
    });
  }

  const selection_diagnostics = buildSelectionDiagnostics({
    market_id,
    raw_seller_market,
    resolution,
    all_numbers,
    allowed_candidates,
    allowed_market_counts,
    selected,
    selection_reason,
    fallback_reason,
  });

  info("routing.choose_textgrid_number_completed", {
    selected_item_id: selected.item_id,
    market_id: selected.market_id,
    market_name: selected.market_name,
    raw_seller_market,
    resolved_sending_zone: resolution.sending_zone,
    allowed_phone_markets: resolution.allowed_phone_markets,
    language,
    score: selected.score,
    phone_number: selected.normalized_phone,
    selection_reason,
    fallback_reason,
  });

  return {
    ...selected,
    selection_reason,
    fallback_reason,
    selection_diagnostics,
  };
}

export default chooseTextgridNumber;
