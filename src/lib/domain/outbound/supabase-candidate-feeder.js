import crypto from "node:crypto";

import { renderTemplate } from "@/lib/domain/templates/render-template.js";
import { loadBestSupabaseSmsTemplate } from "@/lib/domain/master-owners/supabase-feeder-support.js";
import { child } from "@/lib/logging/logger.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import { hasSupabaseConfig, supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { evaluateContactWindow, insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";

const SEND_QUEUE_TABLE = "send_queue";
const TEXTGRID_NUMBERS_TABLE = "textgrid_numbers";
const FEEDER_SOURCE_CANDIDATE_TABLES = [
  "outbound_candidate_snapshot",
  "outbound_candidates",
  "vw_outbound_candidates",
  "v_outbound_candidates",
  "master_owner_outbound_candidates",
  "master_owners",
];

const REASON_CODES = Object.freeze({
  NO_VALID_PHONE: "NO_VALID_PHONE",
  NO_PROPERTY: "NO_PROPERTY",
  TRUE_OPT_OUT: "TRUE_OPT_OUT",
  SUPPRESSED: "SUPPRESSED",
  OUTSIDE_CONTACT_WINDOW: "OUTSIDE_CONTACT_WINDOW",
  PENDING_PRIOR_TOUCH: "PENDING_PRIOR_TOUCH",
  DUPLICATE_QUEUE_ITEM: "DUPLICATE_QUEUE_ITEM",
  NO_TEMPLATE: "NO_TEMPLATE",
  TEMPLATE_RENDER_FAILED: "TEMPLATE_RENDER_FAILED",
  NO_VALID_TEXTGRID_NUMBER: "NO_VALID_TEXTGRID_NUMBER",
  ROUTING_BLOCKED: "ROUTING_BLOCKED",
  CAMPAIGN_LIMIT_REACHED: "CAMPAIGN_LIMIT_REACHED",
});

const logger = child({ module: "domain.outbound.supabase_candidate_feeder" });

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = lower(value);
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function asNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asPositiveInteger(value, fallback = null) {
  const parsed = asNumber(value, fallback);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (!clean(value)) return [];
  return clean(value)
    .split(",")
    .map((entry) => clean(entry))
    .filter(Boolean);
}

function isTruthyDataFlag(value) {
  const normalized = lower(value);
  return ["1", "true", "yes", "active", "confirmed", "suppressed"].includes(normalized);
}

function pick(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && clean(value) !== "") return value;
  }
  return null;
}

function maskPhone(value) {
  const phone = normalizePhone(value);
  if (!phone) return null;
  return `${phone.slice(0, 2)}******${phone.slice(-2)}`;
}

function getSupabase(deps = {}) {
  if (deps.supabase) return deps.supabase;
  if (!hasSupabaseConfig()) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  return defaultSupabase;
}

function normalizeMarket(value) {
  return lower(value).replace(/\s+/g, "_");
}

function buildIdempotencyKey({
  master_owner_id,
  property_id,
  phone_id,
  template_use_case,
  touch_number,
  campaign_session_id,
}) {
  return [
    clean(master_owner_id),
    clean(property_id),
    clean(phone_id),
    clean(template_use_case),
    clean(touch_number),
    clean(campaign_session_id),
  ].join(":");
}

function buildQueueKeyFromIdempotencyKey(idempotency_key) {
  return `feed:${crypto.createHash("sha1").update(clean(idempotency_key)).digest("hex")}`;
}

function normalizeCandidateRow(row = {}, defaults = {}) {
  const master_owner_id = asPositiveInteger(
    pick(row.master_owner_id, row.owner_id, row.master_owner_item_id),
    null
  );
  const property_id = asPositiveInteger(
    pick(row.property_id, row.primary_property_id, row.property_item_id),
    null
  );
  const phone_id = asPositiveInteger(
    pick(row.best_phone_id, row.phone_number_id, row.phone_id, row.primary_phone_id),
    null
  );

  const canonical_e164 =
    normalizePhone(
      pick(
        row.canonical_e164,
        row.phone_e164,
        row.best_phone_e164,
        row.phone_hidden,
        row.phone_number
      )
    ) || "";

  const market = clean(
    pick(row.seller_market, row.market_name, row.market, row.market_label, defaults.market)
  );
  const state = clean(pick(row.seller_state, row.state, row.property_state, defaults.state));

  return {
    raw: row,
    master_owner_id,
    property_id,
    phone_id,
    canonical_e164,
    market,
    state,
    timezone: clean(pick(row.timezone, row.seller_timezone, "America/Chicago")) || "America/Chicago",
    contact_window: clean(
      pick(row.contact_window, row.contact_window_local, row.allowed_contact_window, "")
    ),
    true_post_contact_suppression: asBoolean(
      pick(row.true_post_contact_suppression, row.post_contact_suppression, row.is_suppressed),
      false
    ),
    active_opt_out: asBoolean(pick(row.active_opt_out, row.opt_out_active, row.is_opted_out), false),
    pending_prior_touch: asBoolean(pick(row.pending_prior_touch), false),
    template_use_case:
      clean(pick(row.template_use_case, row.use_case, row.selected_use_case, defaults.template_use_case)) ||
      "ownership_check",
    touch_number: asPositiveInteger(pick(row.touch_number, row.next_touch_number, defaults.touch_number), 1),
    campaign_session_id:
      clean(pick(row.campaign_session_id, defaults.campaign_session_id)) ||
      `session-${new Date().toISOString().slice(0, 10)}`,
    property_address: clean(pick(row.property_address, row.address, row.title)),
    owner_first_name: clean(pick(row.owner_first_name, row.first_name)),
    owner_last_name: clean(pick(row.owner_last_name, row.last_name)),
    language: clean(pick(row.language, row.preferred_language, "English")) || "English",
    seller_name: clean(pick(row.seller_name, row.owner_name, "there")) || "there",
    market_id: asPositiveInteger(pick(row.market_id), null),
  };
}

function mapReasonToDiagnosticCounter(reason) {
  if (reason === REASON_CODES.SUPPRESSED || reason === REASON_CODES.TRUE_OPT_OUT) {
    return "suppression_block_count";
  }
  if (reason === REASON_CODES.OUTSIDE_CONTACT_WINDOW) {
    return "contact_window_block_count";
  }
  if (reason === REASON_CODES.PENDING_PRIOR_TOUCH) {
    return "pending_prior_touch_block_count";
  }
  if (reason === REASON_CODES.DUPLICATE_QUEUE_ITEM) {
    return "duplicate_queue_block_count";
  }
  if (reason === REASON_CODES.NO_TEMPLATE || reason === REASON_CODES.TEMPLATE_RENDER_FAILED) {
    return "template_block_count";
  }
  if (reason === REASON_CODES.ROUTING_BLOCKED || reason === REASON_CODES.NO_VALID_TEXTGRID_NUMBER) {
    return "routing_block_count";
  }
  return null;
}

function buildTemplateContext(candidate = {}) {
  return {
    owner_name: candidate.seller_name || "there",
    owner_first_name: candidate.owner_first_name || null,
    owner_last_name: candidate.owner_last_name || null,
    property_address: candidate.property_address || "your property",
    market_name: candidate.market || null,
    state: candidate.state || null,
  };
}

function parseWindowTime(value) {
  const matched = clean(value).match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!matched) return null;
  let hours = Number(matched[1]);
  const minutes = Number(matched[2]);
  const period = matched[3].toUpperCase();
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return null;
  if (hours < 1 || hours > 12 || minutes < 0 || minutes > 59) return null;
  if (period === "AM" && hours === 12) hours = 0;
  if (period === "PM" && hours !== 12) hours += 12;
  return hours * 60 + minutes;
}

function parseContactWindowRange(window_text = "") {
  const cleaned = clean(window_text).replace(/\s+/g, " ");
  if (!cleaned) return null;
  const parts = cleaned.split(/\s*-\s*|\s+to\s+/i);
  if (!Array.isArray(parts) || parts.length !== 2) return null;
  const start = parseWindowTime(parts[0]);
  const end = parseWindowTime(parts[1]);
  if (start === null || end === null) return null;
  return { start, end };
}

function getLocalMinutesNow(timezone) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone || "America/Chicago",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).formatToParts(new Date());
    const hh = Number(parts.find((p) => p.type === "hour")?.value ?? 0);
    const mm = Number(parts.find((p) => p.type === "minute")?.value ?? 0);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    return hh * 60 + mm;
  } catch {
    return null;
  }
}

function computeNextSchedulableTime(candidate = {}, now_iso = new Date().toISOString()) {
  const range = parseContactWindowRange(candidate.contact_window);
  if (!range) return now_iso;
  const local_minutes = getLocalMinutesNow(candidate.timezone);
  if (local_minutes === null) return now_iso;
  if (local_minutes <= range.start) {
    const now = new Date();
    now.setUTCMinutes(now.getUTCMinutes() + Math.max(range.start - local_minutes, 0));
    return now.toISOString();
  }
  const next_day = new Date();
  next_day.setUTCDate(next_day.getUTCDate() + 1);
  next_day.setUTCHours(14, 0, 0, 0);
  return next_day.toISOString();
}

export async function getSupabaseFeederCandidates(
  {
    scan_limit = 500,
    market = null,
    state = null,
    template_use_case = null,
    touch_number = 1,
    campaign_session_id = null,
  } = {},
  deps = {}
) {
  const supabase = getSupabase(deps);

  for (const source_name of FEEDER_SOURCE_CANDIDATE_TABLES) {
    const { data, error } = await supabase
      .from(source_name)
      .select("*")
      .limit(Math.max(1, Math.min(asPositiveInteger(scan_limit, 500), 5000)));

    if (error) {
      if (error.code === "42P01") continue;
      throw error;
    }

    const rows = Array.isArray(data) ? data : [];
    const normalized = rows
      .map((row) =>
        normalizeCandidateRow(row, {
          template_use_case,
          touch_number,
          campaign_session_id,
          market,
          state,
        })
      )
      .filter((row) => {
        if (market && normalizeMarket(row.market) !== normalizeMarket(market)) return false;
        if (state && lower(row.state) !== lower(state)) return false;
        return true;
      });

    return {
      source: source_name,
      scanned_count: normalized.length,
      rows: normalized,
    };
  }

  return {
    source: null,
    scanned_count: 0,
    rows: [],
  };
}

async function hasDuplicateQueueItem(candidate = {}, options = {}, deps = {}) {
  if (typeof deps.hasDuplicateQueueItem === "function") {
    return deps.hasDuplicateQueueItem(candidate, options);
  }

  const supabase = getSupabase(deps);
  const statuses = ["queued", "sending", "sent"];

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .select("id,queue_status,touch_number,to_phone_number,metadata")
    .eq("master_owner_id", candidate.master_owner_id)
    .eq("property_id", candidate.property_id)
    .in("queue_status", statuses)
    .eq("touch_number", candidate.touch_number)
    .limit(20);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const phone = normalizePhone(candidate.canonical_e164);

  const matched = rows.find((row) => {
    const row_phone = normalizePhone(row?.to_phone_number);
    const template_use_case = clean(
      row?.metadata?.template_use_case || row?.metadata?.selected_use_case || row?.use_case_template
    );
    return row_phone === phone && template_use_case === clean(options.template_use_case);
  });

  return Boolean(matched);
}

export async function evaluateCandidateEligibility(candidate = {}, options = {}, deps = {}) {
  if (!candidate.master_owner_id) {
    return { ok: false, reason_code: REASON_CODES.NO_PROPERTY, reason: "missing_master_owner_id" };
  }
  if (!candidate.property_id) {
    return { ok: false, reason_code: REASON_CODES.NO_PROPERTY, reason: "missing_property_id" };
  }
  if (!candidate.phone_id || !candidate.canonical_e164) {
    return { ok: false, reason_code: REASON_CODES.NO_VALID_PHONE, reason: "missing_phone" };
  }
  if (candidate.true_post_contact_suppression) {
    return { ok: false, reason_code: REASON_CODES.SUPPRESSED, reason: "true_post_contact_suppression" };
  }
  if (candidate.active_opt_out) {
    return { ok: false, reason_code: REASON_CODES.TRUE_OPT_OUT, reason: "active_opt_out" };
  }
  if (candidate.pending_prior_touch) {
    return { ok: false, reason_code: REASON_CODES.PENDING_PRIOR_TOUCH, reason: "pending_prior_touch" };
  }

  const duplicate = await hasDuplicateQueueItem(candidate, options, deps);
  if (duplicate) {
    return { ok: false, reason_code: REASON_CODES.DUPLICATE_QUEUE_ITEM, reason: "duplicate_queue_item" };
  }

  const window_check = evaluateContactWindow(
    {
      contact_window: candidate.contact_window || null,
      timezone: candidate.timezone || "America/Chicago",
      scheduled_for: options.now || new Date().toISOString(),
    },
    { now: options.now }
  );

  if (options.within_contact_window_now && !window_check.allowed) {
    return {
      ok: false,
      reason_code: REASON_CODES.OUTSIDE_CONTACT_WINDOW,
      reason: "outside_contact_window",
      contact_window: window_check,
    };
  }

  return {
    ok: true,
    reason_code: "OK",
    reason: "eligible",
    contact_window: window_check,
    scheduled_for: options.within_contact_window_now
      ? options.now || new Date().toISOString()
      : computeNextSchedulableTime(candidate, options.now || new Date().toISOString()),
  };
}

function isRoutingConfigEnabled(value) {
  return asBoolean(value, false) || isTruthyDataFlag(value);
}

function normalizeRoutingAliases(value) {
  return asArray(value).map((entry) => normalizeMarket(entry));
}

function normalizeTextgridNumberRow(row = {}) {
  const market = clean(pick(row.market_name, row.market, row.seller_market));
  const state_aliases = asArray(pick(row.allowed_states, row.cluster_states, row.routing_states));
  return {
    raw: row,
    id: pick(row.id, row.textgrid_number_id),
    phone_number: normalizePhone(pick(row.phone_number, row.number, row.e164)),
    market,
    market_normalized: normalizeMarket(market),
    aliases: normalizeRoutingAliases(pick(row.approved_market_aliases, row.routing_aliases)),
    status: lower(pick(row.status, row.number_status, "active")),
    allow_nationwide_fallback: isRoutingConfigEnabled(row.allow_nationwide_fallback),
    allow_cluster_fallback: isRoutingConfigEnabled(row.allow_cluster_fallback),
    is_nationwide: isRoutingConfigEnabled(pick(row.is_nationwide, row.nationwide, false)),
    state_aliases: state_aliases.map((entry) => lower(entry)),
    messages_sent_today: asNumber(pick(row.messages_sent_today, row.sent_today), 0),
    last_used_at: clean(row.last_used_at) || null,
  };
}

function byUsageThenRecency(left, right) {
  const left_sent = asNumber(left.messages_sent_today, 0);
  const right_sent = asNumber(right.messages_sent_today, 0);
  if (left_sent !== right_sent) return left_sent - right_sent;

  const left_ts = left.last_used_at ? new Date(left.last_used_at).getTime() : 0;
  const right_ts = right.last_used_at ? new Date(right.last_used_at).getTime() : 0;
  return left_ts - right_ts;
}

export async function chooseTextgridNumber(candidate = {}, options = {}, deps = {}) {
  if (typeof deps.chooseTextgridNumber === "function") {
    return deps.chooseTextgridNumber(candidate, options);
  }

  const supabase = getSupabase(deps);
  const { data, error } = await supabase
    .from(TEXTGRID_NUMBERS_TABLE)
    .select("*")
    .limit(200);

  if (error) throw error;

  const numbers = (Array.isArray(data) ? data : [])
    .map(normalizeTextgridNumberRow)
    .filter((row) => row.id && row.phone_number && (!row.status || row.status === "active"));

  if (!numbers.length) {
    return {
      ok: false,
      reason_code: REASON_CODES.NO_VALID_TEXTGRID_NUMBER,
      routing_allowed: false,
      routing_tier: "none",
      routing_block_reason: "no_active_textgrid_numbers",
      selected: null,
    };
  }

  const seller_market = normalizeMarket(candidate.market);
  const seller_state = lower(candidate.state);

  const exact = numbers.filter((row) => row.market_normalized === seller_market).sort(byUsageThenRecency);
  const alias = numbers
    .filter((row) => row.aliases.includes(seller_market) && row.market_normalized !== seller_market)
    .sort(byUsageThenRecency);
  const cluster = numbers
    .filter((row) => row.allow_cluster_fallback && row.state_aliases.includes(seller_state))
    .sort(byUsageThenRecency);
  const nationwide = numbers
    .filter((row) => row.is_nationwide && row.allow_nationwide_fallback)
    .sort(byUsageThenRecency);

  let selected = null;
  let routing_tier = "none";
  let selection_reason = "none";

  if (exact.length) {
    selected = exact[0];
    routing_tier = "exact_market_match";
    selection_reason = "exact_market_match";
  } else if (alias.length) {
    selected = alias[0];
    routing_tier = "approved_market_alias";
    selection_reason = "approved_market_alias";
  } else if (!options.routing_safe_only && cluster.length) {
    selected = cluster[0];
    routing_tier = "approved_cluster_fallback";
    selection_reason = "approved_cluster_fallback";
  } else if (!options.routing_safe_only && nationwide.length) {
    selected = nationwide[0];
    routing_tier = "approved_nationwide_fallback";
    selection_reason = "approved_nationwide_fallback";
  }

  if (!selected) {
    return {
      ok: false,
      reason_code: REASON_CODES.ROUTING_BLOCKED,
      routing_allowed: false,
      routing_tier: "blocked",
      routing_block_reason: "no_approved_routing_path",
      selected: null,
    };
  }

  return {
    ok: true,
    reason_code: "OK",
    routing_allowed: true,
    routing_tier,
    selection_reason,
    routing_block_reason: null,
    selected: {
      id: selected.id,
      phone_number: selected.phone_number,
      market: selected.market,
    },
  };
}

export async function renderOutboundTemplate(candidate = {}, options = {}, deps = {}) {
  if (typeof deps.renderOutboundTemplate === "function") {
    return deps.renderOutboundTemplate(candidate, options);
  }

  const selector = {
    use_case: options.template_use_case || candidate.template_use_case || "ownership_check",
    touch_type: Number(candidate.touch_number || 1) <= 1 ? "First Touch" : "Follow-Up",
    language: candidate.language || "English",
    property_type_scope: clean(candidate.raw?.property_type_scope) || null,
    deal_strategy: clean(candidate.raw?.deal_strategy) || null,
  };

  const template = await loadBestSupabaseSmsTemplate(selector, deps);
  if (!template?.text) {
    return {
      ok: false,
      reason_code: REASON_CODES.NO_TEMPLATE,
      reason: "no_renderable_template",
      template: null,
      rendered_message_body: null,
    };
  }

  const rendered = renderTemplate({
    template_text: template.text,
    context: buildTemplateContext(candidate),
    use_case: selector.use_case,
    variant_group: template.variant_group || null,
  });

  if (!rendered.ok || !clean(rendered.rendered_text)) {
    return {
      ok: false,
      reason_code: REASON_CODES.TEMPLATE_RENDER_FAILED,
      reason: "template_render_failed",
      template,
      render_result: rendered,
      rendered_message_body: null,
    };
  }

  return {
    ok: true,
    reason_code: "OK",
    template,
    template_use_case: selector.use_case,
    rendered_message_body: rendered.rendered_text,
    render_result: rendered,
  };
}

export async function createSendQueueItem(candidate = {}, options = {}, deps = {}) {
  const idempotency_key = buildIdempotencyKey({
    master_owner_id: candidate.master_owner_id,
    property_id: candidate.property_id,
    phone_id: candidate.phone_id,
    template_use_case: options.template_use_case,
    touch_number: candidate.touch_number,
    campaign_session_id: candidate.campaign_session_id,
  });

  const queue_key = buildQueueKeyFromIdempotencyKey(idempotency_key);
  const scheduled_for = options.scheduled_for || new Date().toISOString();
  const queue_status = new Date(scheduled_for).getTime() > Date.now() ? "scheduled" : "queued";

  const payload = {
    queue_key,
    queue_id: queue_key,
    queue_status,
    scheduled_for,
    scheduled_for_utc: scheduled_for,
    scheduled_for_local: scheduled_for,
    message_body: options.rendered_message_body,
    message_text: options.rendered_message_body,
    to_phone_number: candidate.canonical_e164,
    from_phone_number: options.selected_textgrid_number,
    textgrid_number_id: options.selected_textgrid_number_id,
    master_owner_id: candidate.master_owner_id,
    property_id: candidate.property_id,
    market_id: candidate.market_id,
    template_id: options.template_id,
    touch_number: candidate.touch_number,
    timezone: candidate.timezone,
    contact_window: candidate.contact_window || null,
    use_case_template: options.template_use_case,
    metadata: {
      idempotency_key,
      campaign_session_id: candidate.campaign_session_id,
      template_use_case: options.template_use_case,
      selected_textgrid_number_id: options.selected_textgrid_number_id,
      selected_textgrid_number: options.selected_textgrid_number,
      selected_textgrid_market: options.selected_textgrid_market,
      seller_market: candidate.market,
      seller_state: candidate.state,
      routing_tier: options.routing_tier,
      selection_reason: options.selection_reason,
      routing_allowed: options.routing_allowed,
      routing_block_reason: options.routing_block_reason,
      candidate_snapshot: {
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
        phone_id: candidate.phone_id,
        canonical_phone_masked: maskPhone(candidate.canonical_e164),
        seller_market: candidate.market,
        seller_state: candidate.state,
        touch_number: candidate.touch_number,
      },
      template: {
        id: options.template_id,
        source: options.template_source || "supabase",
        use_case: options.template_use_case,
      },
    },
  };

  if (options.dry_run) {
    return {
      ok: true,
      dry_run: true,
      queued: false,
      queue_key,
      queue_id: queue_key,
      idempotency_key,
      payload,
    };
  }

  if (typeof deps.createSendQueueItem === "function") {
    return deps.createSendQueueItem(payload, { idempotency_key, queue_key });
  }

  const inserted = await insertSupabaseSendQueueRow(payload, deps);
  if (!inserted?.ok && inserted?.reason === "duplicate_blocked") {
    return {
      ok: false,
      reason_code: REASON_CODES.DUPLICATE_QUEUE_ITEM,
      reason: "duplicate_queue_item",
      duplicate: true,
      queue_key,
      idempotency_key,
      inserted,
    };
  }

  return {
    ok: true,
    dry_run: false,
    queued: true,
    queue_key,
    queue_id: inserted?.queue_id || queue_key,
    queue_row_id: inserted?.queue_row_id || null,
    idempotency_key,
    inserted,
  };
}

export function buildFeederDiagnostics(summary = {}) {
  const diagnostics = {
    ok: summary.ok !== false,
    dry_run: Boolean(summary.dry_run),
    source: summary.source || null,
    scanned_count: Number(summary.scanned_count || 0),
    eligible_count: Number(summary.eligible_count || 0),
    queued_count: Number(summary.queued_count || 0),
    skipped_count: Number(summary.skipped_count || 0),
    routing_block_count: Number(summary.routing_block_count || 0),
    suppression_block_count: Number(summary.suppression_block_count || 0),
    contact_window_block_count: Number(summary.contact_window_block_count || 0),
    pending_prior_touch_block_count: Number(summary.pending_prior_touch_block_count || 0),
    duplicate_queue_block_count: Number(summary.duplicate_queue_block_count || 0),
    template_block_count: Number(summary.template_block_count || 0),
    selected_textgrid_market_counts: summary.selected_textgrid_market_counts || {},
    routing_tier_counts: summary.routing_tier_counts || {},
    sample_created_queue_items: Array.isArray(summary.sample_created_queue_items)
      ? summary.sample_created_queue_items.slice(0, 10)
      : [],
    sample_skips: Array.isArray(summary.sample_skips) ? summary.sample_skips.slice(0, 20) : [],
  };

  return diagnostics;
}

export async function runSupabaseCandidateFeeder(input = {}, deps = {}) {
  const now = input.now || new Date().toISOString();
  const limit = Math.max(1, Math.min(asPositiveInteger(input.limit, 25), 500));
  const scan_limit = Math.max(limit, Math.min(asPositiveInteger(input.scan_limit, 500), 5000));

  const options = {
    dry_run: asBoolean(input.dry_run, false),
    limit,
    scan_limit,
    market: clean(input.market) || null,
    state: clean(input.state) || null,
    routing_safe_only: asBoolean(input.routing_safe_only, true),
    within_contact_window_now: asBoolean(input.within_contact_window_now, true),
    template_use_case: clean(input.template_use_case) || "ownership_check",
    touch_number: asPositiveInteger(input.touch_number, 1),
    campaign_session_id: clean(input.campaign_session_id) || `session-${now.slice(0, 10)}`,
    now,
  };

  const source = await getSupabaseFeederCandidates(options, deps);
  const summary = {
    ok: true,
    dry_run: options.dry_run,
    source: source.source,
    scanned_count: source.scanned_count,
    eligible_count: 0,
    queued_count: 0,
    skipped_count: 0,
    routing_block_count: 0,
    suppression_block_count: 0,
    contact_window_block_count: 0,
    pending_prior_touch_block_count: 0,
    duplicate_queue_block_count: 0,
    template_block_count: 0,
    selected_textgrid_market_counts: {},
    routing_tier_counts: {},
    sample_created_queue_items: [],
    sample_skips: [],
  };

  for (const candidate of source.rows) {
    if (summary.queued_count >= options.limit) {
      summary.skipped_count += 1;
      summary.sample_skips.push({
        reason_code: REASON_CODES.CAMPAIGN_LIMIT_REACHED,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
      });
      continue;
    }

    candidate.touch_number = options.touch_number;
    candidate.template_use_case = options.template_use_case;
    candidate.campaign_session_id = options.campaign_session_id;

    const eligibility = await evaluateCandidateEligibility(candidate, options, deps);
    if (!eligibility.ok) {
      summary.skipped_count += 1;
      const counter = mapReasonToDiagnosticCounter(eligibility.reason_code);
      if (counter) summary[counter] += 1;
      summary.sample_skips.push({
        reason_code: eligibility.reason_code,
        reason: eligibility.reason,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
      });
      continue;
    }

    summary.eligible_count += 1;

    const routing = await chooseTextgridNumber(candidate, options, deps);
    if (!routing.ok) {
      summary.skipped_count += 1;
      summary.routing_block_count += 1;
      summary.sample_skips.push({
        reason_code: routing.reason_code,
        reason: routing.routing_block_reason,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
      });
      continue;
    }

    const rendered = await renderOutboundTemplate(candidate, options, deps);
    if (!rendered.ok) {
      summary.skipped_count += 1;
      summary.template_block_count += 1;
      summary.sample_skips.push({
        reason_code: rendered.reason_code,
        reason: rendered.reason,
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
      });
      continue;
    }

    const queue_result = await createSendQueueItem(
      candidate,
      {
        ...options,
        scheduled_for: eligibility.scheduled_for,
        rendered_message_body: rendered.rendered_message_body,
        template_id: rendered.template?.template_id || rendered.template?.item_id || null,
        template_source: rendered.template?.source || "supabase",
        template_use_case: rendered.template_use_case,
        selected_textgrid_number_id: routing.selected.id,
        selected_textgrid_number: routing.selected.phone_number,
        selected_textgrid_market: routing.selected.market,
        routing_tier: routing.routing_tier,
        selection_reason: routing.selection_reason,
        routing_allowed: routing.routing_allowed,
        routing_block_reason: routing.routing_block_reason,
      },
      deps
    );

    if (!queue_result.ok) {
      summary.skipped_count += 1;
      const reason_code = queue_result.reason_code || REASON_CODES.DUPLICATE_QUEUE_ITEM;
      const counter = mapReasonToDiagnosticCounter(reason_code);
      if (counter) summary[counter] += 1;
      summary.sample_skips.push({
        reason_code,
        reason: queue_result.reason || "queue_create_failed",
        master_owner_id: candidate.master_owner_id,
        property_id: candidate.property_id,
      });
      continue;
    }

    summary.queued_count += 1;
    summary.selected_textgrid_market_counts[routing.selected.market || "unknown"] =
      Number(summary.selected_textgrid_market_counts[routing.selected.market || "unknown"] || 0) + 1;
    summary.routing_tier_counts[routing.routing_tier || "unknown"] =
      Number(summary.routing_tier_counts[routing.routing_tier || "unknown"] || 0) + 1;

    summary.sample_created_queue_items.push({
      queue_row_id: queue_result.queue_row_id || null,
      queue_key: queue_result.queue_key,
      master_owner_id: candidate.master_owner_id,
      property_id: candidate.property_id,
      phone_masked: maskPhone(candidate.canonical_e164),
      selected_textgrid_number_id: routing.selected.id,
      selected_textgrid_number: routing.selected.phone_number,
      selected_textgrid_market: routing.selected.market,
      seller_market: candidate.market,
      seller_state: candidate.state,
      routing_tier: routing.routing_tier,
      selection_reason: routing.selection_reason,
      routing_allowed: true,
      routing_block_reason: null,
      template_use_case: rendered.template_use_case,
      dry_run: options.dry_run,
    });
  }

  const diagnostics = buildFeederDiagnostics(summary);
  logger.info("outbound.supabase_feeder_completed", {
    source: diagnostics.source,
    dry_run: diagnostics.dry_run,
    scanned_count: diagnostics.scanned_count,
    eligible_count: diagnostics.eligible_count,
    queued_count: diagnostics.queued_count,
    skipped_count: diagnostics.skipped_count,
  });

  return diagnostics;
}

export { REASON_CODES };
