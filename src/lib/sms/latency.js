// ─── latency.js ───────────────────────────────────────────────────────────
// Compute scheduled send time based on assigned SMS Agent latency ranges.
// Deterministic randomization via seeded hashing.
// Enforces local contact window and timezone safety.

import crypto from "node:crypto";

// ══════════════════════════════════════════════════════════════════════════
// AGENT FIELD EXTERNAL IDS
// ══════════════════════════════════════════════════════════════════════════

export const AGENT_LATENCY_FIELDS = Object.freeze({
  hot_min: "latency-hot-min",
  hot_max: "latency-hot-max",
  neutral_min: "latency-neutral-min",
  neutral_max: "latency-neutral-max",
  cold_min: "latency-cold-min",
  cold_max: "latency-cold-max",
  response_min: "number-5",
  response_max: "number-4",
  // NOTE: number-6, number-7, number-8 do NOT exist in the live Agents app.
  // readAgentNumber will always fall back to DEFAULT_LATENCY values (1, 2, 3 days).
  // Kept here so that if these fields are re-added to Podio, they'll be picked up
  // automatically without a code change.
  stage_2_delay_days: "number-6",
  stage_3_delay_days: "number-7",
  stage_5_delay_days: "number-8",
});

// ══════════════════════════════════════════════════════════════════════════
// DEFAULTS (seconds) — used only when agent fields are missing
// ══════════════════════════════════════════════════════════════════════════

const DEFAULT_LATENCY = Object.freeze({
  hot_min: 30,     hot_max: 120,
  neutral_min: 120, neutral_max: 300,
  cold_min: 300,   cold_max: 900,
  response_min: 60, response_max: 180,
  stage_2_delay_days: 1,
  stage_3_delay_days: 2,
  stage_5_delay_days: 3,
});

// Default contact window: 9 AM – 8 PM local
const DEFAULT_WINDOW_START_HOUR = 9;
const DEFAULT_WINDOW_END_HOUR = 20;

// ══════════════════════════════════════════════════════════════════════════
// SEEDED RANDOM
// ══════════════════════════════════════════════════════════════════════════

function seededRandom(parts = []) {
  const input = parts.map((p) => String(p ?? "")).join("|");
  const hash = crypto.createHash("sha256").update(input, "utf8").digest();
  return hash.readUInt32BE(0) / 0xffffffff; // 0..1
}

function randomInRange(min_val, max_val, seed_parts) {
  const r = seededRandom(seed_parts);
  return min_val + r * (max_val - min_val);
}

// ══════════════════════════════════════════════════════════════════════════
// TIMEZONE HELPERS
// ══════════════════════════════════════════════════════════════════════════

const TZ_MAP = Object.freeze({
  "eastern": "America/New_York",
  "central": "America/Chicago",
  "mountain": "America/Denver",
  "pacific": "America/Los_Angeles",
  "hawaii": "Pacific/Honolulu",
  "alaska": "America/Anchorage",
  "america/new_york": "America/New_York",
  "america/chicago": "America/Chicago",
  "america/denver": "America/Denver",
  "america/los_angeles": "America/Los_Angeles",
  "pacific/honolulu": "Pacific/Honolulu",
  "america/anchorage": "America/Anchorage",
});

function resolveTimezone(tz) {
  if (!tz) return "America/New_York";
  const normalized = String(tz).toLowerCase().trim();
  return TZ_MAP[normalized] || tz;
}

function toLocalHour(utc_date, iana_tz) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: iana_tz,
      hour: "numeric",
      minute: "numeric",
      hour12: false,
    }).formatToParts(utc_date);
    const hour = Number(parts.find((p) => p.type === "hour")?.value ?? 0);
    const minute = Number(parts.find((p) => p.type === "minute")?.value ?? 0);
    return hour + minute / 60;
  } catch {
    return utc_date.getUTCHours() + utc_date.getUTCMinutes() / 60;
  }
}

function toLocalDateString(utc_date, iana_tz) {
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone: iana_tz }).format(utc_date);
  } catch {
    return utc_date.toISOString().slice(0, 10);
  }
}

function toLocalISOString(utc_date, iana_tz) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: iana_tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).formatToParts(utc_date);

    const get = (type) => parts.find((p) => p.type === type)?.value ?? "00";
    return `${get("year")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}:${get("second")}`;
  } catch {
    return utc_date.toISOString().replace("Z", "");
  }
}

// ══════════════════════════════════════════════════════════════════════════
// CONTACT WINDOW PARSING
// ══════════════════════════════════════════════════════════════════════════

const WINDOW_PATTERN = /^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?\s*[-–]\s*(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?/i;

function parseContactWindow(window_str) {
  if (!window_str) return { start_hour: DEFAULT_WINDOW_START_HOUR, end_hour: DEFAULT_WINDOW_END_HOUR };

  const match = String(window_str).match(WINDOW_PATTERN);
  if (!match) return { start_hour: DEFAULT_WINDOW_START_HOUR, end_hour: DEFAULT_WINDOW_END_HOUR };

  let start_h = Number(match[1]);
  const start_ampm = (match[3] || "").toUpperCase();
  let end_h = Number(match[4]);
  const end_ampm = (match[6] || "").toUpperCase();

  if (start_ampm === "PM" && start_h < 12) start_h += 12;
  if (start_ampm === "AM" && start_h === 12) start_h = 0;
  if (end_ampm === "PM" && end_h < 12) end_h += 12;
  if (end_ampm === "AM" && end_h === 12) end_h = 0;

  return { start_hour: start_h, end_hour: end_h };
}

// ══════════════════════════════════════════════════════════════════════════
// READ AGENT LATENCY
// ══════════════════════════════════════════════════════════════════════════

function readAgentNumber(agent_item, field_id, fallback) {
  if (!agent_item) return fallback;
  // Support both raw Podio item shape and pre-extracted numbers
  if (typeof agent_item === "object" && typeof agent_item[field_id] === "number") {
    return agent_item[field_id];
  }
  // Try Podio item field extraction
  if (agent_item.fields) {
    for (const f of agent_item.fields) {
      if (f.external_id === field_id) {
        const val = f.values?.[0]?.value;
        const n = typeof val === "number" ? val : Number(val);
        return Number.isFinite(n) ? n : fallback;
      }
    }
  }
  return fallback;
}

function extractAgentLatency(agent_item) {
  return {
    hot_min: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.hot_min, DEFAULT_LATENCY.hot_min),
    hot_max: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.hot_max, DEFAULT_LATENCY.hot_max),
    neutral_min: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.neutral_min, DEFAULT_LATENCY.neutral_min),
    neutral_max: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.neutral_max, DEFAULT_LATENCY.neutral_max),
    cold_min: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.cold_min, DEFAULT_LATENCY.cold_min),
    cold_max: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.cold_max, DEFAULT_LATENCY.cold_max),
    response_min: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.response_min, DEFAULT_LATENCY.response_min),
    response_max: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.response_max, DEFAULT_LATENCY.response_max),
    stage_2_delay_days: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.stage_2_delay_days, DEFAULT_LATENCY.stage_2_delay_days),
    stage_3_delay_days: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.stage_3_delay_days, DEFAULT_LATENCY.stage_3_delay_days),
    stage_5_delay_days: readAgentNumber(agent_item, AGENT_LATENCY_FIELDS.stage_5_delay_days, DEFAULT_LATENCY.stage_5_delay_days),
  };
}

// ══════════════════════════════════════════════════════════════════════════
// DELAY TYPE RESOLUTION
// ══════════════════════════════════════════════════════════════════════════

const STAGE_DELAY_MAP = Object.freeze({
  S2F: "stage_2_delay_days",
  S3F: "stage_3_delay_days",
  S5F: "stage_5_delay_days",
});

function isFollowUpStage(stage_code) {
  return stage_code && STAGE_DELAY_MAP[String(stage_code).toUpperCase()] != null;
}

// ══════════════════════════════════════════════════════════════════════════
// MAIN SCHEDULING
// ══════════════════════════════════════════════════════════════════════════

/**
 * Compute the scheduled send time.
 *
 * @param {object} params
 * @param {Date|string} params.now_utc - Current UTC time
 * @param {string} params.timezone - Contact timezone name
 * @param {object} params.assigned_agent - Agent Podio item or extracted latency object
 * @param {string} params.message_kind - "reply" | "follow_up" | "cold_outbound"
 * @param {string} [params.stage_code] - Current stage code
 * @param {object} [params.classify_result] - Classify output (for delay profile)
 * @param {string} [params.contact_window] - Contact window string
 * @param {string} [params.delay_profile] - "hot" | "neutral" | "cold"
 * @param {string[]} [params.seeded_key] - Parts for deterministic seed
 * @returns {{ scheduled_utc: string, scheduled_local: string, timezone: string, latency_seconds: number, delay_source: string }}
 */
export function computeScheduledSend({
  now_utc,
  timezone,
  assigned_agent = null,
  message_kind = "reply",
  stage_code = null,
  classify_result = null,
  contact_window = null,
  delay_profile = null,
  seeded_key = [],
} = {}) {
  const now = now_utc instanceof Date ? now_utc : new Date(now_utc || Date.now());
  const iana_tz = resolveTimezone(timezone);
  const agent = extractAgentLatency(assigned_agent);
  const window = parseContactWindow(contact_window);
  const profile = delay_profile || resolveDelayProfileFromClassify(classify_result);

  let delay_seconds;
  let delay_source;

  // Stage-based day delays for follow-ups
  const upper_stage = String(stage_code ?? "").toUpperCase();
  if (message_kind === "follow_up" && isFollowUpStage(upper_stage)) {
    const day_field = STAGE_DELAY_MAP[upper_stage];
    const delay_days = agent[day_field] || DEFAULT_LATENCY[day_field] || 1;
    // Base delay in seconds + intra-day jitter
    delay_seconds = delay_days * 86400;
    const jitter = randomInRange(0, 3600, [...seeded_key, "intraday_jitter"]);
    delay_seconds += jitter;
    delay_source = `stage_delay:${day_field}:${delay_days}d`;
  } else {
    // Immediate conversational reply — use latency buckets
    const min_key = `${profile}_min`;
    const max_key = `${profile}_max`;
    const min_s = agent[min_key] ?? DEFAULT_LATENCY[min_key] ?? 60;
    const max_s = agent[max_key] ?? DEFAULT_LATENCY[max_key] ?? 300;
    delay_seconds = Math.round(randomInRange(min_s, max_s, [...seeded_key, "latency"]));
    delay_source = `${profile}_latency:${min_s}-${max_s}s`;
  }

  let target = new Date(now.getTime() + delay_seconds * 1000);

  // Contact window enforcement
  target = enforceContactWindow(target, iana_tz, window, seeded_key);

  return {
    scheduled_utc: target.toISOString(),
    scheduled_local: toLocalISOString(target, iana_tz),
    timezone: iana_tz,
    latency_seconds: Math.round((target.getTime() - now.getTime()) / 1000),
    delay_source,
  };
}

function resolveDelayProfileFromClassify(classify_result) {
  if (!classify_result) return "neutral";
  const emotion = String(classify_result.emotion ?? "").toLowerCase();
  const signals = new Set((classify_result.positive_signals || []).map((s) => String(s).toLowerCase()));

  if (emotion === "motivated" || signals.has("urgency") || signals.has("affirmative") || signals.has("price_curious")) {
    return "hot";
  }
  if (emotion === "skeptical" || emotion === "frustrated" || emotion === "guarded") {
    return "cold";
  }
  return "neutral";
}

function enforceContactWindow(target, iana_tz, window, seeded_key) {
  const local_hour = toLocalHour(target, iana_tz);

  if (local_hour >= window.start_hour && local_hour < window.end_hour) {
    return target; // within window
  }

  // Roll forward to next valid window opening
  const local_date_str = toLocalDateString(target, iana_tz);
  let next_day = local_hour >= window.end_hour;

  // Build a target at window start of the same or next day
  const base_date = new Date(target);
  if (next_day || local_hour >= window.end_hour) {
    base_date.setTime(base_date.getTime() + 86400000);
  }

  // Set to window start in local time by iterating
  // We use a simple approach: set to midnight + start_hour, adjust for TZ
  const target_date_str = next_day
    ? toLocalDateString(base_date, iana_tz)
    : local_date_str;

  // Construct target time: date + start_hour with some jitter
  const jitter_minutes = Math.round(randomInRange(0, 30, [...(seeded_key || []), "window_jitter"]));
  const target_local_iso = `${target_date_str}T${String(window.start_hour).padStart(2, "0")}:${String(jitter_minutes).padStart(2, "0")}:00`;

  // Parse back to UTC via the timezone
  try {
    // Use a roundtrip: format a known UTC moment in this TZ, compute offset
    const ref_utc = new Date(`${target_date_str}T12:00:00Z`);
    const ref_local_str = toLocalISOString(ref_utc, iana_tz);
    const ref_local = new Date(ref_local_str + "Z");
    const offset_ms = ref_local.getTime() - ref_utc.getTime();

    const local_as_utc = new Date(target_local_iso + "Z");
    return new Date(local_as_utc.getTime() - offset_ms);
  } catch {
    // Fallback: add 12 hours and hope for the best
    return new Date(target.getTime() + 43200000);
  }
}

export {
  extractAgentLatency,
  parseContactWindow,
  resolveTimezone,
  seededRandom,
  randomInRange,
  toLocalHour,
  toLocalISOString,
  DEFAULT_LATENCY,
  STAGE_DELAY_MAP,
};

export default { computeScheduledSend };
