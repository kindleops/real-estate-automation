import {
  createMessageEvent,
  findMessageEventByMessageId,
  findMessageEvents,
  updateMessageEvent,
} from "@/lib/podio/apps/message-events.js";
import {
  BUYER_MATCH_FIELDS,
  getBuyerMatchItem,
} from "@/lib/podio/apps/buyer-match.js";
import {
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";

const THREAD_SOURCE_APP = "Buyer Thread";
const MAX_THREAD_HISTORY = 30;

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function parseJson(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function uniq(values = []) {
  return [...new Set(safeArray(values).map((value) => clean(value)).filter(Boolean))];
}

function takeNewest(items = [], limit = MAX_THREAD_HISTORY) {
  return [...items]
    .filter(Boolean)
    .sort((left, right) => (toTimestamp(right?.at) || 0) - (toTimestamp(left?.at) || 0))
    .slice(0, limit);
}

function chooseLatestValue({
  existing_value = "",
  existing_at = null,
  next_value = "",
  next_at = null,
} = {}) {
  const normalized_next = clean(next_value);
  if (!normalized_next) return clean(existing_value) || null;

  const existing_ts = toTimestamp(existing_at);
  const next_ts = toTimestamp(next_at);
  if (existing_ts !== null && next_ts !== null && next_ts < existing_ts) {
    return clean(existing_value) || null;
  }

  return normalized_next;
}

function determineThreadState({
  existing_state = "",
  existing_state_at = null,
  next_state = "",
  next_state_at = null,
} = {}) {
  return chooseLatestValue({
    existing_value: existing_state,
    existing_at: existing_state_at,
    next_value: next_state,
    next_at: next_state_at,
  }) || "Candidate";
}

function inferThreadState({
  interaction_kind = "",
  interaction_status = "",
  classification = null,
  send_ok = null,
} = {}) {
  const normalized_response = lower(classification?.normalized_response || "");
  const normalized_status = lower(
    classification?.buyer_response_status || interaction_status
  );
  const normalized_kind = lower(interaction_kind);

  if (normalized_response === "chosen" || normalized_status === "selected") {
    return "Selected";
  }

  if (normalized_response === "passed" || normalized_status === "passed") {
    return "Passed";
  }

  if (
    ["interested", "needs_more_info"].includes(normalized_response) ||
    ["interested", "needs more info"].includes(normalized_status)
  ) {
    return normalized_response === "needs_more_info" || normalized_status === "needs more info"
      ? "Needs More Info"
      : "Interested";
  }

  if (normalized_response === "opened" || normalized_status === "opened") {
    return "Opened";
  }

  if (normalized_kind === "blast_sent" || send_ok === true) {
    return "Sent";
  }

  return "";
}

function summarizeInteraction({
  subject = "",
  message = "",
  interaction_kind = "",
  interaction_status = "",
  classification = null,
} = {}) {
  const parts = [
    clean(subject),
    clean(message).replace(/\s+/g, " ").slice(0, 220),
    clean(classification?.buyer_response_status),
    clean(interaction_status),
    clean(interaction_kind),
  ].filter(Boolean);

  return parts[0] || "Buyer thread activity";
}

function buildThreadMessage({
  company_name = "",
  current_state = "",
  last_channel = "",
  last_contact_at = null,
} = {}) {
  return [
    clean(company_name) || "Buyer Thread",
    clean(current_state) || "Candidate",
    clean(last_channel) ? `${clean(last_channel).toUpperCase()} thread` : "",
    clean(last_contact_at) ? `Last contact ${clean(last_contact_at)}` : "",
  ]
    .filter(Boolean)
    .join(" | ");
}

function buildThreadTriggerName(buyer_match_item_id = null, company_item_id = null) {
  return `buyer-thread:${clean(buyer_match_item_id)}:${clean(company_item_id)}`;
}

export function buildBuyerThreadMessageId({
  buyer_match_item_id = null,
  company_item_id = null,
} = {}) {
  return `buyer-thread:${clean(buyer_match_item_id)}:${clean(company_item_id)}`;
}

export function parseBuyerThreadMeta(item = null) {
  return parseJson(getTextValue(item, "ai-output", ""));
}

function buildThreadHistoryEntry({
  at = nowIso(),
  channel = "",
  direction = "",
  interaction_kind = "",
  interaction_status = "",
  subject = "",
  message = "",
  provider_message_id = null,
  related_event_item_id = null,
  classification = null,
} = {}) {
  return {
    at: clean(at) || nowIso(),
    channel: clean(channel) || null,
    direction: clean(direction) || null,
    interaction_kind: clean(interaction_kind) || null,
    interaction_status: clean(interaction_status) || null,
    subject: clean(subject) || null,
    preview: clean(message).replace(/\s+/g, " ").slice(0, 220) || null,
    provider_message_id: clean(provider_message_id) || null,
    related_event_item_id: clean(related_event_item_id) || null,
    response: clean(classification?.buyer_response_status) || null,
    normalized_response: clean(classification?.normalized_response) || null,
  };
}

export async function upsertBuyerDispositionThread({
  buyer_match_item = null,
  buyer_match_item_id = null,
  company_item = null,
  company_item_id = null,
  company_name = "",
  recipient_email = null,
  recipient_phone = null,
  channel = "email",
  direction = "Outbound",
  interaction_kind = "",
  interaction_status = "",
  subject = "",
  message = "",
  provider_message_id = null,
  related_event_item_id = null,
  classification = null,
  metadata = {},
  timestamp = nowIso(),
  send_ok = null,
} = {}) {
  const resolved_buyer_match_item_id =
    clean(buyer_match_item?.item_id || buyer_match_item_id) || null;
  const resolved_company_item_id =
    clean(company_item?.item_id || company_item_id) || null;

  if (!resolved_buyer_match_item_id || !resolved_company_item_id) {
    return {
      ok: false,
      reason: "missing_buyer_thread_identifiers",
      buyer_match_item_id: resolved_buyer_match_item_id,
      company_item_id: resolved_company_item_id,
    };
  }

  const buyer_match_record =
    buyer_match_item?.item_id
      ? buyer_match_item
      : await getBuyerMatchItem(resolved_buyer_match_item_id).catch(() => null);
  const message_id = buildBuyerThreadMessageId({
    buyer_match_item_id: resolved_buyer_match_item_id,
    company_item_id: resolved_company_item_id,
  });
  const existing = await findMessageEventByMessageId(message_id);
  const existing_meta = parseBuyerThreadMeta(existing);

  const resolved_company_name =
    clean(company_name) ||
    clean(company_item?.title) ||
    clean(existing_meta?.company_name) ||
    "Partner";
  const interaction_at = clean(timestamp) || nowIso();
  const inferred_state = inferThreadState({
    interaction_kind,
    interaction_status,
    classification,
    send_ok,
  });
  const existing_state_at =
    clean(existing_meta?.last_state_change_at) ||
    clean(existing_meta?.last_inbound_at) ||
    clean(existing_meta?.last_outbound_at) ||
    null;
  const current_state = determineThreadState({
    existing_state: existing_meta?.current_state,
    existing_state_at,
    next_state: inferred_state,
    next_state_at: interaction_at,
  });

  const last_outbound_at =
    lower(direction) === "outbound"
      ? interaction_at
      : clean(existing_meta?.last_outbound_at) || null;
  const last_inbound_at =
    lower(direction) === "inbound"
      ? interaction_at
      : clean(existing_meta?.last_inbound_at) || null;
  const last_contact_at =
    chooseLatestValue({
      existing_value: existing_meta?.last_contact_at,
      existing_at: existing_meta?.last_contact_at,
      next_value: interaction_at,
      next_at: interaction_at,
    }) || interaction_at;

  const history = takeNewest([
    ...safeArray(existing_meta?.history),
    buildThreadHistoryEntry({
      at: interaction_at,
      channel,
      direction,
      interaction_kind,
      interaction_status,
      subject,
      message,
      provider_message_id,
      related_event_item_id,
      classification,
    }),
  ]);

  const next_meta = {
    version: 1,
    event_kind: "buyer_thread",
    buyer_match_item_id: resolved_buyer_match_item_id,
    company_item_id: resolved_company_item_id,
    company_name: resolved_company_name,
    current_state,
    last_state_change_at: inferred_state ? interaction_at : clean(existing_meta?.last_state_change_at) || null,
    last_channel: clean(channel) || clean(existing_meta?.last_channel) || null,
    last_contact_at,
    last_outbound_at,
    last_inbound_at,
    primary_email:
      chooseLatestValue({
        existing_value: existing_meta?.primary_email,
        existing_at: last_contact_at,
        next_value: recipient_email,
        next_at: interaction_at,
      }) || null,
    primary_phone:
      chooseLatestValue({
        existing_value: existing_meta?.primary_phone,
        existing_at: last_contact_at,
        next_value: recipient_phone,
        next_at: interaction_at,
      }) || null,
    emails: uniq([...(existing_meta?.emails || []), recipient_email]),
    phones: uniq([...(existing_meta?.phones || []), recipient_phone]),
    channels: uniq([...(existing_meta?.channels || []), channel]),
    interaction_counts: {
      outbound:
        Number(existing_meta?.interaction_counts?.outbound || 0) +
        (lower(direction) === "outbound" ? 1 : 0),
      inbound:
        Number(existing_meta?.interaction_counts?.inbound || 0) +
        (lower(direction) === "inbound" ? 1 : 0),
      email:
        Number(existing_meta?.interaction_counts?.email || 0) +
        (lower(channel) === "email" ? 1 : 0),
      sms:
        Number(existing_meta?.interaction_counts?.sms || 0) +
        (lower(channel) === "sms" ? 1 : 0),
    },
    related_property_item_id: getFirstAppReferenceId(
      buyer_match_record,
      BUYER_MATCH_FIELDS.property,
      null
    ),
    related_master_owner_item_id: getFirstAppReferenceId(
      buyer_match_record,
      BUYER_MATCH_FIELDS.master_owner,
      null
    ),
    related_contract_item_id: getFirstAppReferenceId(
      buyer_match_record,
      BUYER_MATCH_FIELDS.contract,
      null
    ),
    selected_buyer:
      getFirstAppReferenceId(buyer_match_record, BUYER_MATCH_FIELDS.selected_buyer, null) ===
        Number(resolved_company_item_id) ||
      lower(current_state) === "selected",
    buyer_response_status:
      clean(classification?.buyer_response_status) ||
      clean(existing_meta?.buyer_response_status) ||
      null,
    match_status:
      clean(getCategoryValue(buyer_match_record, BUYER_MATCH_FIELDS.match_status, "")) ||
      clean(existing_meta?.match_status) ||
      null,
    assignment_status:
      clean(getCategoryValue(buyer_match_record, BUYER_MATCH_FIELDS.assignment_status, "")) ||
      clean(existing_meta?.assignment_status) ||
      null,
    last_subject: clean(subject) || clean(existing_meta?.last_subject) || null,
    last_message_preview: summarizeInteraction({
      subject,
      message,
      interaction_kind,
      interaction_status,
      classification,
    }),
    last_provider_message_id:
      clean(provider_message_id) || clean(existing_meta?.last_provider_message_id) || null,
    last_related_event_item_id:
      clean(related_event_item_id) || clean(existing_meta?.last_related_event_item_id) || null,
    metadata: {
      ...(existing_meta?.metadata && typeof existing_meta.metadata === "object"
        ? existing_meta.metadata
        : {}),
      ...(metadata && typeof metadata === "object" ? metadata : {}),
    },
    history,
  };

  const fields = {
    "message-id": message_id,
    "timestamp": { start: interaction_at },
    "source-app": THREAD_SOURCE_APP,
    "trigger-name": buildThreadTriggerName(
      resolved_buyer_match_item_id,
      resolved_company_item_id
    ),
    "message": buildThreadMessage({
      company_name: resolved_company_name,
      current_state,
      last_channel: next_meta.last_channel,
      last_contact_at,
    }),
    "status-3": current_state || "Candidate",
    "status-2": clean(interaction_status) || clean(channel) || undefined,
    "property": next_meta.related_property_item_id
      ? [next_meta.related_property_item_id]
      : undefined,
    "master-owner": next_meta.related_master_owner_item_id
      ? [next_meta.related_master_owner_item_id]
      : undefined,
    "ai-output": JSON.stringify(next_meta),
  };

  if (existing?.item_id) {
    await updateMessageEvent(existing.item_id, fields);
    return {
      ok: true,
      updated: true,
      thread_item_id: existing.item_id,
      meta: next_meta,
    };
  }

  const created = await createMessageEvent(fields);
  return {
    ok: true,
    created: true,
    thread_item_id: created?.item_id || null,
    meta: next_meta,
  };
}

export async function listBuyerDispositionThreads({
  buyer_match_item_id = null,
  company_item_id = null,
  limit = 120,
} = {}) {
  const items = await findMessageEvents({ "source-app": THREAD_SOURCE_APP }, limit, 0);
  return safeArray(Array.isArray(items?.items) ? items.items : items)
    .map((item) => ({
      item_id: item?.item_id || null,
      ...parseBuyerThreadMeta(item),
      timestamp:
        getDateValue(item, "timestamp", null) ||
        clean(parseBuyerThreadMeta(item)?.last_contact_at) ||
        null,
    }))
    .filter((thread) => {
      if (clean(buyer_match_item_id) && clean(thread?.buyer_match_item_id) !== clean(buyer_match_item_id)) {
        return false;
      }
      if (clean(company_item_id) && clean(thread?.company_item_id) !== clean(company_item_id)) {
        return false;
      }
      return clean(thread?.buyer_match_item_id);
    })
    .sort((left, right) => (toTimestamp(right?.timestamp) || 0) - (toTimestamp(left?.timestamp) || 0));
}

export default {
  buildBuyerThreadMessageId,
  parseBuyerThreadMeta,
  upsertBuyerDispositionThread,
  listBuyerDispositionThreads,
};
