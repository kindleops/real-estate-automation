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

function parseTimeToMinutes(value) {
  const raw = clean(value);
  const matched = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!matched) return null;
  const hours = Number(matched[1]);
  const minutes = Number(matched[2]);
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return null;
  return hours * 60 + minutes;
}

function minutesSinceMidnight(value = new Date()) {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) return null;
  return value.getHours() * 60 + value.getMinutes();
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
    hourly_limit:
      getNumberValue(item, TEXTGRID_NUMBER_FIELDS.hourly_send_cap, null) ??
      null,

    daily_sent:
      getNumberValue(item, TEXTGRID_NUMBER_FIELDS.sent_today, null) ??
      0,
    hourly_sent:
      getNumberValue(item, TEXTGRID_NUMBER_FIELDS.sent_last_hour, null) ??
      0,
    risk_spike_flag: getCategoryValue(item, TEXTGRID_NUMBER_FIELDS.risk_spike_flag, null),
    last_used_at: getDateValue(item, TEXTGRID_NUMBER_FIELDS.last_used_at, null),

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

function isWithinLocalSendWindow(record, now = new Date()) {
  const start = parseTimeToMinutes(record?.allowed_send_window_start_local);
  const end = parseTimeToMinutes(record?.allowed_send_window_end_local);
  if (start === null || end === null) return true;

  const current = minutesSinceMidnight(now);
  if (current === null) return true;

  if (start <= end) return current >= start && current <= end;
  return current >= start || current <= end;
}

function hasRiskSpike(record) {
  return isPositiveCategory(record?.risk_spike_flag);
}

function isUsableNumber(record, { now = new Date() } = {}) {
  if (!record?.item_id) return false;
  if (!record?.normalized_phone) return false;

  if (record.status && isNegativeCategory(record.status)) return false;
  if (isPaused(record)) return false;
  if (!isWithinLocalSendWindow(record, now)) return false;
  if (hasRiskSpike(record)) return false;

  if (
    record.hourly_limit !== null &&
    Number(record.hourly_limit) > 0 &&
    Number(record.hourly_sent || 0) >= Number(record.hourly_limit)
  ) {
    return false;
  }

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
  preferred_area_code = null,
} = {}) {
  const daily_cap = Number(record?.daily_limit);
  const daily_used = Number(record?.daily_sent || 0);
  const hourly_cap = Number(record?.hourly_limit);
  const hourly_used = Number(record?.hourly_sent || 0);
  const daily_ratio =
    Number.isFinite(daily_cap) && daily_cap > 0
      ? Math.min(1, Math.max(0, daily_used / daily_cap))
      : daily_used > 0
        ? 1
        : 0;
  const hourly_ratio =
    Number.isFinite(hourly_cap) && hourly_cap > 0
      ? Math.min(1, Math.max(0, hourly_used / hourly_cap))
      : hourly_used > 0
        ? 1
        : 0;
  const utilization_ratio = Math.max(daily_ratio, hourly_ratio);
  const rotation_weight = Number(record?.priority || 0);
  const last_used_ts = record?.last_used_at ? new Date(record.last_used_at).getTime() : 0;
  const recency_rank = Number.isNaN(last_used_ts) ? 0 : last_used_ts;
  const area_code_bonus =
    preferred_area_code &&
    record.area_code &&
    clean(record.area_code) === clean(preferred_area_code)
      ? 1
      : 0;

  return {
    utilization_ratio,
    area_code_bonus,
    rotation_weight,
    recency_rank,
  };
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

  const sorted = [...candidates].sort((left, right) => {
    if (left.score.utilization_ratio !== right.score.utilization_ratio) {
      return left.score.utilization_ratio - right.score.utilization_ratio;
    }
    if (left.score.area_code_bonus !== right.score.area_code_bonus) {
      return right.score.area_code_bonus - left.score.area_code_bonus;
    }
    if (left.score.rotation_weight !== right.score.rotation_weight) {
      return right.score.rotation_weight - left.score.rotation_weight;
    }
    return left.score.recency_rank - right.score.recency_rank;
  });
  const top = sorted[0];
  const topCluster = sorted.filter(
    (candidate) =>
      candidate.score.utilization_ratio === top.score.utilization_ratio &&
      candidate.score.area_code_bonus === top.score.area_code_bonus &&
      candidate.score.rotation_weight === top.score.rotation_weight &&
      candidate.score.recency_rank === top.score.recency_rank
  );

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
    resolved_sending_zone: resolution?.primary_cluster || null,
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
      preferred_area_code: market_area_code,
    }),
  }));

  const normalizedExact = lower(normalizeMarketLabel(resolution.normalized_raw_market));
  const normalizedAlias = lower(normalizeMarketLabel(resolution.normalized_market));
  const clusterMarkets = resolution.allowed_phone_markets.map((market_name) =>
    lower(normalizeMarketLabel(market_name))
  );
  const allowed_candidates = scored.filter((record) => {
    const key = lower(normalizeMarketLabel(record.market_name));
    return key === normalizedExact || key === normalizedAlias || clusterMarkets.includes(key);
  });
  const allowed_market_counts = resolution.priority_chain.map((entry) => ({
    tier: entry.tier,
    market_name: entry.market || null,
    candidate_count:
      entry.tier === "regional_cluster_fallback"
        ? scored.filter((record) =>
            clusterMarkets.includes(lower(normalizeMarketLabel(record.market_name)))
          ).length
        : scored.filter(
            (record) =>
              lower(normalizeMarketLabel(record.market_name)) ===
              lower(normalizeMarketLabel(entry.market))
          ).length,
  }));

  let selected = null;
  let selection_reason = "routing_unmapped";
  let fallback_reason = null;
  const deterministic_tiers = [
    {
      tier: "exact_market_match",
      reason: "exact_market_match",
      candidates: scored.filter(
        (record) => lower(normalizeMarketLabel(record.market_name)) === normalizedExact
      ),
    },
    {
      tier: "alias_market_match",
      reason: "alias_market_match",
      candidates:
        normalizedAlias === normalizedExact
          ? []
          : scored.filter(
              (record) => lower(normalizeMarketLabel(record.market_name)) === normalizedAlias
            ),
    },
    {
      tier: "regional_cluster_fallback",
      reason: "regional_cluster_fallback",
      candidates: scored.filter((record) =>
        clusterMarkets.includes(lower(normalizeMarketLabel(record.market_name)))
      ),
    },
  ];

  for (const tier of deterministic_tiers) {
    if (!tier.candidates.length) continue;

    selected = chooseBestCandidate({
      candidates: tier.candidates,
      rotation_key:
        rotation_key ||
        `${context?.ids?.phone_item_id || "no-phone"}:${resolution.primary_cluster}:${tier.tier}`,
    });

    if (selected) {
      selection_reason = tier.reason;
      fallback_reason = tier.tier === "regional_cluster_fallback"
        ? "higher_priority_market_tiers_unavailable"
        : null;
      break;
    }
  }

  if (!selected) {
    warn("routing.choose_textgrid_number_no_match", {
      market_id,
      raw_seller_market,
      resolved_sending_zone: resolution.primary_cluster,
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
    resolved_sending_zone: resolution.primary_cluster,
    allowed_phone_markets: resolution.allowed_phone_markets,
    language,
    score: selected.score.utilization_ratio,
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
