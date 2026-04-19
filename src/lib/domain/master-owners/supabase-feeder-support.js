import {
  canonicalizeTemplateUseCase,
  expandSelectorUseCases,
  normalizeSelectorText,
} from "@/lib/domain/templates/template-selector.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import {
  hasSupabaseConfig,
  supabase as defaultSupabase,
} from "@/lib/supabase/client.js";

const SEND_QUEUE_TABLE = "send_queue";
const SMS_TEMPLATES_TABLE = "sms_templates";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function asNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function ensureObject(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) return value;

  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      return {};
    }
  }

  return {};
}

function getSupabase(deps = {}) {
  if (!deps.supabase && !deps.supabaseClient && !hasSupabaseConfig()) {
    return null;
  }

  return deps.supabase || deps.supabaseClient || defaultSupabase;
}

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

export function hasSupabaseFeederSupport(deps = {}) {
  return Boolean(getSupabase(deps));
}

export function normalizeSupabaseFeederQueueRow(row = {}) {
  const metadata = ensureObject(row?.metadata);

  return {
    ...row,
    item_id: row?.id || null,
    queue_item_id: row?.id || null,
    queue_row_id: row?.id || null,
    queue_id: clean(row?.queue_id || row?.queue_key) || null,
    queue_key: clean(row?.queue_key || row?.queue_id) || null,
    queue_status: lower(row?.queue_status),
    touch_number: asNumber(row?.touch_number ?? row?.queue_sequence, null),
    phone_item_id: asNumber(
      metadata?.phone_item_id ?? row?.phone_item_id,
      null
    ),
    property_item_id: asNumber(row?.property_id, null),
    master_owner_id: asNumber(row?.master_owner_id, null),
    to_phone_number: normalizePhone(row?.to_phone_number) || null,
    from_phone_number: normalizePhone(row?.from_phone_number) || null,
    scheduled_for:
      row?.scheduled_for ||
      row?.scheduled_for_utc ||
      row?.scheduled_for_local ||
      row?.created_at ||
      null,
    metadata,
    raw: row,
  };
}

export async function loadSupabaseQueueRowsForMasterOwners(
  master_owner_ids = [],
  deps = {}
) {
  const supabase = getSupabase(deps);
  const owner_ids = uniq(
    master_owner_ids.map((value) => clean(value)).filter(Boolean)
  );

  if (!supabase || !owner_ids.length) {
    return [];
  }

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .select("*")
    .in("master_owner_id", owner_ids)
    .in("queue_status", ["queued", "sending", "sent"])
    .order("scheduled_for", { ascending: false, nullsFirst: false })
    .limit(Math.min(Math.max(owner_ids.length * 20, 100), 500));

  if (error) throw error;

  return (Array.isArray(data) ? data : []).map(normalizeSupabaseFeederQueueRow);
}

export async function loadSupabaseQueueRowsForMasterOwner(
  master_owner_id,
  deps = {}
) {
  if (!clean(master_owner_id)) return [];
  return loadSupabaseQueueRowsForMasterOwners([master_owner_id], deps);
}

export async function findSupabaseQueueRowsByQueueKey(queue_key, deps = {}) {
  const supabase = getSupabase(deps);
  const normalized_queue_key = clean(queue_key);

  if (!supabase || !normalized_queue_key) {
    return [];
  }

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .select("*")
    .eq("queue_key", normalized_queue_key)
    .order("created_at", { ascending: true, nullsFirst: true })
    .limit(10);

  if (error) throw error;

  return (Array.isArray(data) ? data : []).map(normalizeSupabaseFeederQueueRow);
}

export async function cancelSupabaseQueueRows(row_ids = [], deps = {}) {
  const supabase = getSupabase(deps);
  const ids = uniq(row_ids.map((value) => clean(value)).filter(Boolean));

  if (!supabase || !ids.length) {
    return [];
  }

  const { data, error } = await supabase
    .from(SEND_QUEUE_TABLE)
    .update({
      queue_status: "cancelled",
      updated_at: new Date().toISOString(),
    })
    .in("id", ids)
    .select("*");

  if (error) throw error;

  return (Array.isArray(data) ? data : []).map(normalizeSupabaseFeederQueueRow);
}

export async function inspectSupabaseQueueBuffer(
  {
    now = new Date().toISOString(),
    critical_low_threshold = 25,
    replenish_target = 50,
    healthy_target = 75,
    ideal_target = 100,
  } = {},
  deps = {}
) {
  const supabase = getSupabase(deps);

  if (!supabase) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const [queued_result, sending_result, failed_result] = await Promise.all([
    supabase
      .from(SEND_QUEUE_TABLE)
      .select("id,scheduled_for", { count: "exact" })
      .eq("queue_status", "queued")
      .limit(500),
    supabase
      .from(SEND_QUEUE_TABLE)
      .select("id", { count: "exact" })
      .eq("queue_status", "sending")
      .limit(200),
    supabase
      .from(SEND_QUEUE_TABLE)
      .select("id", { count: "exact" })
      .eq("queue_status", "failed")
      .limit(200),
  ]);

  const results = [queued_result, sending_result, failed_result];
  const first_error = results.find((result) => result?.error)?.error || null;
  if (first_error) throw first_error;

  const queued_rows = Array.isArray(queued_result.data) ? queued_result.data : [];
  const now_ts = new Date(now).getTime();
  let queued_future_count = 0;
  let queued_due_now_count = 0;

  for (const row of queued_rows) {
    const scheduled_ts = row?.scheduled_for
      ? new Date(row.scheduled_for).getTime()
      : null;

    if (scheduled_ts !== null && !Number.isNaN(scheduled_ts) && scheduled_ts > now_ts) {
      queued_future_count += 1;
    } else {
      queued_due_now_count += 1;
    }
  }

  const queued_inventory_count =
    Number(queued_result.count) || queued_rows.length;
  const sending_count =
    Number(sending_result.count) ||
    (Array.isArray(sending_result.data) ? sending_result.data.length : 0);
  const failed_recent_count =
    Number(failed_result.count) ||
    (Array.isArray(failed_result.data) ? failed_result.data.length : 0);
  const available_inventory_count = queued_inventory_count + sending_count;
  const desired_buffer_target =
    available_inventory_count >= ideal_target
      ? ideal_target
      : available_inventory_count < critical_low_threshold
        ? ideal_target
        : available_inventory_count < replenish_target
          ? healthy_target
          : ideal_target;

  return {
    queued_inventory_count,
    available_inventory_count,
    future_inventory_count: queued_future_count,
    due_inventory_count: queued_due_now_count,
    queued_future_count,
    queued_due_now_count,
    sending_count,
    failed_recent_count,
    critical_low_threshold,
    replenish_target,
    healthy_target,
    ideal_target,
    desired_buffer_target,
    critical_low_threshold_breached:
      available_inventory_count < critical_low_threshold,
    replenish_threshold_met: available_inventory_count >= replenish_target,
    healthy_buffer_threshold_met: available_inventory_count >= healthy_target,
    ideal_buffer_threshold_met: available_inventory_count >= ideal_target,
    buffer_target: desired_buffer_target,
    buffer_deficit: Math.max(desired_buffer_target - available_inventory_count, 0),
    buffer_satisfied:
      desired_buffer_target > 0 &&
      available_inventory_count >= desired_buffer_target,
    snapshot_limit: 500,
  };
}

function normalizeRequestedTemplateUseCases(use_case = null, variant_group = null) {
  return new Set(
    expandSelectorUseCases(use_case, variant_group)
      .map((value) => normalizeSelectorText(value))
      .filter(Boolean)
  );
}

function normalizeTemplateScope(value = null) {
  return normalizeSelectorText(value);
}

function isPropertyTypeScopeCompatible(requested_scope = null, template_scope = null) {
  const requested = normalizeTemplateScope(requested_scope);
  const actual = normalizeTemplateScope(template_scope);

  if (!requested || !actual) return true;
  if (requested === actual) return true;
  if (actual.includes("any residential") && requested.includes("residential")) return true;
  if (requested.includes(actual) || actual.includes(requested)) return true;
  return false;
}

function isDealStrategyCompatible(requested_strategy = null, template_strategy = null) {
  const requested = normalizeSelectorText(requested_strategy);
  const actual = normalizeSelectorText(template_strategy);

  if (!requested || !actual) return true;
  return requested === actual || actual.includes(requested) || requested.includes(actual);
}

function scoreSupabaseSmsTemplate(row = {}, selector = {}) {
  const template_text = clean(row?.template_body || row?.english_translation);
  if (!row?.is_active || !template_text) return Number.NEGATIVE_INFINITY;

  const requested_use_cases = normalizeRequestedTemplateUseCases(
    selector?.use_case,
    selector?.variant_group
  );
  const template_use_case = normalizeSelectorText(
    canonicalizeTemplateUseCase(row?.use_case, row?.stage_label || row?.stage_code)
  );

  if (requested_use_cases.size && !requested_use_cases.has(template_use_case)) {
    return Number.NEGATIVE_INFINITY;
  }

  const requested_touch_type = normalizeSelectorText(selector?.touch_type);
  const is_first_touch = row?.is_first_touch === true;
  const is_follow_up = row?.is_follow_up === true;

  if (requested_touch_type === normalizeSelectorText("First Touch") && !is_first_touch) {
    return Number.NEGATIVE_INFINITY;
  }

  if (
    requested_touch_type === normalizeSelectorText("Follow-Up") &&
    !is_follow_up &&
    is_first_touch
  ) {
    return Number.NEGATIVE_INFINITY;
  }

  if (
    !isPropertyTypeScopeCompatible(
      selector?.property_type_scope,
      row?.property_type_scope
    )
  ) {
    return Number.NEGATIVE_INFINITY;
  }

  if (!isDealStrategyCompatible(selector?.deal_strategy, row?.deal_strategy)) {
    return Number.NEGATIVE_INFINITY;
  }

  const requested_language = normalizeSelectorText(selector?.language || "English");
  const template_language = normalizeSelectorText(row?.language || "English");

  let score = 0;

  if (template_use_case && requested_use_cases.has(template_use_case)) score += 300;
  if (is_first_touch) score += 60;
  if (is_follow_up) score += 40;
  if (template_language === requested_language) {
    score += 80;
  } else if (template_language === normalizeSelectorText("English")) {
    score += 30;
  } else {
    score -= 100;
  }

  if (normalizeTemplateScope(row?.property_type_scope)) score += 20;
  if (normalizeSelectorText(row?.deal_strategy)) score += 20;

  score += Math.round(asNumber(row?.success_rate, 0) * 10);
  score += Math.min(asNumber(row?.usage_count, 0), 100);
  score += Math.min(asNumber(row?.version, 0), 20);

  return score;
}

export function normalizeSupabaseSmsTemplateRow(row = {}) {
  const template_id =
    clean(row?.template_id) ||
    clean(row?.podio_template_id) ||
    clean(row?.id);

  return {
    raw: row,
    item_id: template_id || null,
    supabase_template_id: clean(row?.id) || null,
    template_id: template_id || null,
    title: clean(row?.template_name) || null,
    text: clean(row?.template_body || row?.english_translation) || "",
    use_case: clean(row?.use_case) || null,
    use_case_label: clean(row?.use_case) || null,
    canonical_routing_slug: clean(row?.use_case) || null,
    variant_group: clean(row?.stage_label || row?.stage_code) || null,
    stage_code: clean(row?.stage_code) || null,
    stage_label: clean(row?.stage_label) || null,
    language: clean(row?.language || "English") || "English",
    active: row?.is_active ? "Yes" : "No",
    is_first_touch: row?.is_first_touch ? "Yes" : "No",
    is_follow_up: row?.is_follow_up ? "Yes" : "No",
    property_type_scope: clean(row?.property_type_scope) || null,
    deal_strategy: clean(row?.deal_strategy) || null,
    tone: clean(row?.agent_persona) || null,
    paired_with_agent_type: clean(row?.agent_persona) || null,
    source: "supabase",
    template_resolution_source: "supabase_template",
    template_fallback_reason: null,
    template_selection_diagnostics: {
      source: "supabase",
      template_name: clean(row?.template_name) || null,
      supabase_template_id: clean(row?.id) || null,
    },
  };
}

export async function loadBestSupabaseSmsTemplate(
  template_selection_inputs = {},
  deps = {}
) {
  const supabase = getSupabase(deps);

  if (!supabase) {
    return null;
  }

  const { data, error } = await supabase
    .from(SMS_TEMPLATES_TABLE)
    .select("*")
    .eq("is_active", true)
    .order("success_rate", { ascending: false, nullsFirst: false })
    .order("usage_count", { ascending: false, nullsFirst: false })
    .limit(250);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const ranked = rows
    .map((row) => ({
      row,
      score: scoreSupabaseSmsTemplate(row, template_selection_inputs),
    }))
    .filter((entry) => Number.isFinite(entry.score))
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return String(left.row?.id || "").localeCompare(String(right.row?.id || ""));
    });

  if (!ranked.length) {
    return null;
  }

  const best = normalizeSupabaseSmsTemplateRow(ranked[0].row);
  best.template_selection_diagnostics = {
    ...best.template_selection_diagnostics,
    score: ranked[0].score,
    selector: {
      use_case: template_selection_inputs?.use_case || null,
      touch_type: template_selection_inputs?.touch_type || null,
      language: template_selection_inputs?.language || null,
      property_type_scope: template_selection_inputs?.property_type_scope || null,
      deal_strategy: template_selection_inputs?.deal_strategy || null,
    },
  };

  return best;
}
