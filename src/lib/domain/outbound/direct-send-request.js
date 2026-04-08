import APP_IDS from "@/lib/config/app-ids.js";
import { deriveContextSummary } from "@/lib/domain/context/derive-context-summary.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import { processSendQueue } from "@/lib/domain/queue/process-send-queue.js";
import {
  fetchAllItems,
  getFirstAppReferenceId,
  getItem,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import { findPhoneRecord } from "@/lib/podio/apps/phone-numbers.js";

function clean(value) {
  return String(value ?? "").trim();
}

function asNullablePositiveInteger(value, fallback = null) {
  const normalized = clean(value);
  if (!normalized) return fallback;

  const n = Number(normalized);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function previewMessage(value, max = 160) {
  const normalized = clean(value);
  if (!normalized) return null;
  return normalized.length > max ? `${normalized.slice(0, max)}...` : normalized;
}

function statusForResult(result) {
  return result?.queued?.ok === false || result?.processed?.ok === false ? 400 : 200;
}

function serializeRouteError(error) {
  return {
    name: error?.name || "Error",
    message: error?.message || "Unknown error",
    stack: error?.stack || null,
  };
}

function isDirectSendInputError(message = "") {
  return [
    "missing_to_number",
    "invalid_to_number",
    "missing_message_text",
    "missing_from_number",
    "phone_item_not_found:",
    "phone_not_found_for_to_number",
    "phone_item_to_number_mismatch",
    "textgrid_number_item_not_found:",
    "textgrid_number_from_number_mismatch",
    "textgrid_number_not_found_for_from_number",
    "master_owner_item_not_found:",
    "prospect_item_not_found:",
    "property_item_not_found:",
    "market_item_not_found:",
  ].some((candidate) => String(message || "").startsWith(candidate));
}

function normalizePhoneItemTarget(phone_item = null) {
  return normalizePhone(
    getTextValue(phone_item, "canonical-e164", "") ||
      getTextValue(phone_item, "phone-hidden", "")
  );
}

function buildDirectSendContext({
  phone_item,
  master_owner_item = null,
  prospect_item = null,
  property_item = null,
  market_item = null,
}) {
  const touch_count = Number(getNumberValue(phone_item, "total-messages-sent", 0) || 0);
  const master_owner_id =
    master_owner_item?.item_id ||
    getFirstAppReferenceId(phone_item, "linked-master-owner", null) ||
    null;
  const prospect_id =
    prospect_item?.item_id ||
    getFirstAppReferenceId(phone_item, "linked-contact", null) ||
    null;
  const property_id =
    property_item?.item_id ||
    getFirstAppReferenceId(phone_item, "primary-property", null) ||
    null;
  const market_id =
    market_item?.item_id ||
    getFirstAppReferenceId(property_item, "market-2", null) ||
    getFirstAppReferenceId(property_item, "market", null) ||
    null;

  return {
    found: true,
    ids: {
      phone_item_id: phone_item?.item_id || null,
      master_owner_id,
      prospect_id,
      property_id,
      market_id,
      assigned_agent_id: null,
    },
    items: {
      phone_item,
      brain_item: null,
      master_owner_item,
      prospect_item,
      property_item,
      market_item,
      agent_item: null,
    },
    recent: {
      touch_count,
      recently_used_template_ids: [],
    },
    summary: deriveContextSummary({
      phone_item,
      brain_item: null,
      master_owner_item,
      prospect_item,
      property_item,
      market_item,
      touch_count,
    }),
  };
}

export function normalizeDirectSendInput(input = {}) {
  return {
    from_number: clean(input?.from_number) || null,
    to_number: clean(input?.to_number) || null,
    message_text: clean(input?.message_text) || null,
    master_owner_id: asNullablePositiveInteger(input?.master_owner_id, null),
    prospect_id: asNullablePositiveInteger(input?.prospect_id, null),
    property_id: asNullablePositiveInteger(input?.property_id, null),
    phone_item_id: asNullablePositiveInteger(input?.phone_item_id, null),
    textgrid_number_item_id: asNullablePositiveInteger(
      input?.textgrid_number_item_id,
      null
    ),
    message_preview: previewMessage(input?.message_text),
  };
}

async function loadExplicitItem(item_id, label, getItemImpl) {
  if (!item_id) return null;
  const item = await getItemImpl(item_id);
  if (!item?.item_id) {
    throw new Error(`${label}_not_found:${item_id}`);
  }
  return item;
}

async function resolvePhoneItem(
  {
    phone_item_id = null,
    to_number = null,
  },
  {
    getItemImpl,
    findPhoneRecordImpl,
  }
) {
  const normalized_to = normalizePhone(to_number);

  if (!normalized_to) {
    throw new Error("invalid_to_number");
  }

  const phone_item = phone_item_id
    ? await loadExplicitItem(phone_item_id, "phone_item", getItemImpl)
    : await findPhoneRecordImpl(to_number);

  if (!phone_item?.item_id) {
    throw new Error("phone_not_found_for_to_number");
  }

  const resolved_target = normalizePhoneItemTarget(phone_item);
  if (resolved_target && resolved_target !== normalized_to) {
    throw new Error("phone_item_to_number_mismatch");
  }

  return phone_item;
}

async function resolveTextgridNumberItem(
  {
    textgrid_number_item_id = null,
    from_number = null,
  },
  {
    getItemImpl,
    fetchAllItemsImpl,
  }
) {
  if (textgrid_number_item_id) {
    const explicit_item = await loadExplicitItem(
      textgrid_number_item_id,
      "textgrid_number_item",
      getItemImpl
    );
    const normalized_from = normalizePhone(from_number);
    const resolved_from = normalizePhone(
      getTextValue(explicit_item, "title", "") ||
        getTextValue(explicit_item, "friendly-name", "")
    );

    if (normalized_from && resolved_from && normalized_from !== resolved_from) {
      throw new Error("textgrid_number_from_number_mismatch");
    }

    return explicit_item;
  }

  const normalized_from = normalizePhone(from_number);
  if (!normalized_from) {
    throw new Error("missing_from_number");
  }

  const candidates = await fetchAllItemsImpl(
    APP_IDS.textgrid_numbers,
    {},
    { page_size: 200 }
  );
  const matched =
    candidates.find((item) => {
      const candidate_number = normalizePhone(
        getTextValue(item, "title", "") ||
          getTextValue(item, "friendly-name", "")
      );
      return candidate_number === normalized_from;
    }) || null;

  if (!matched?.item_id) {
    throw new Error("textgrid_number_not_found_for_from_number");
  }

  return matched;
}

export async function buildAndProcessDirectSend(input, deps = {}) {
  const {
    getItemImpl = getItem,
    findPhoneRecordImpl = findPhoneRecord,
    fetchAllItemsImpl = fetchAllItems,
    buildSendQueueItemImpl = buildSendQueueItem,
    processSendQueueImpl = processSendQueue,
  } = deps;
  const normalized = normalizeDirectSendInput(input);

  if (!normalized.to_number) {
    throw new Error("missing_to_number");
  }
  if (!normalized.message_text) {
    throw new Error("missing_message_text");
  }
  if (!normalized.from_number && !normalized.textgrid_number_item_id) {
    throw new Error("missing_from_number");
  }

  const phone_item = await resolvePhoneItem(normalized, {
    getItemImpl,
    findPhoneRecordImpl,
  });
  const textgrid_number_item = await resolveTextgridNumberItem(normalized, {
    getItemImpl,
    fetchAllItemsImpl,
  });
  const master_owner_item = await loadExplicitItem(
    normalized.master_owner_id ||
      getFirstAppReferenceId(phone_item, "linked-master-owner", null),
    "master_owner_item",
    getItemImpl
  );
  const prospect_item = await loadExplicitItem(
    normalized.prospect_id ||
      getFirstAppReferenceId(phone_item, "linked-contact", null),
    "prospect_item",
    getItemImpl
  );
  const property_item = await loadExplicitItem(
    normalized.property_id ||
      getFirstAppReferenceId(phone_item, "primary-property", null),
    "property_item",
    getItemImpl
  );
  const market_item = await loadExplicitItem(
    getFirstAppReferenceId(property_item, "market-2", null) ||
      getFirstAppReferenceId(property_item, "market", null),
    "market_item",
    getItemImpl
  );

  const context = buildDirectSendContext({
    phone_item,
    master_owner_item,
    prospect_item,
    property_item,
    market_item,
  });
  const now = new Date().toISOString();
  const queued = await buildSendQueueItemImpl({
    context,
    rendered_message_text: normalized.message_text,
    textgrid_number_item_id: textgrid_number_item.item_id,
    scheduled_for_local: now,
    scheduled_for_utc: now,
    queue_status: "Queued",
    message_type: context.recent.touch_count > 0 ? "Follow-Up" : "Cold Outbound",
    dnc_check: "✅ Cleared",
    delivery_confirmed: "⏳ Pending",
  });

  const queue_item_id = queued?.queue_item_id || null;
  const processed = queue_item_id
    ? await processSendQueueImpl({ queue_item_id })
    : {
        ok: false,
        sent: false,
        reason: "missing_queue_item_id_after_queue",
      };

  return {
    queued,
    processed,
    phone_item_id: phone_item.item_id,
    textgrid_number_item_id: textgrid_number_item.item_id,
    master_owner_id: master_owner_item?.item_id || null,
    prospect_id: prospect_item?.item_id || null,
    property_id: property_item?.item_id || null,
  };
}

export async function handleDirectSendRequestData(request, method = "GET", deps = {}) {
  const {
    logger,
  } = deps;
  let request_meta = {
    method,
    from_number: null,
    to_number: null,
    phone_item_id: null,
    textgrid_number_item_id: null,
    master_owner_id: null,
    prospect_id: null,
    property_id: null,
    message_preview: null,
  };

  try {
    let normalized_input = null;

    if (method === "GET") {
      const { searchParams } = new URL(request.url);
      normalized_input = normalizeDirectSendInput({
        from_number: searchParams.get("from_number"),
        to_number: searchParams.get("to_number"),
        message_text: searchParams.get("message_text"),
        master_owner_id: searchParams.get("master_owner_id"),
        prospect_id: searchParams.get("prospect_id"),
        property_id: searchParams.get("property_id"),
        phone_item_id: searchParams.get("phone_item_id"),
        textgrid_number_item_id: searchParams.get("textgrid_number_item_id"),
      });
    } else {
      const body = await request.json().catch(() => ({}));
      normalized_input = normalizeDirectSendInput(body);
    }

    request_meta = {
      method,
      from_number: normalized_input.from_number,
      to_number: normalized_input.to_number,
      phone_item_id: normalized_input.phone_item_id,
      textgrid_number_item_id: normalized_input.textgrid_number_item_id,
      master_owner_id: normalized_input.master_owner_id,
      prospect_id: normalized_input.prospect_id,
      property_id: normalized_input.property_id,
      message_preview: normalized_input.message_preview,
    };

    logger?.info?.("outbound_direct_send.requested", request_meta);

    const result = await buildAndProcessDirectSend(normalized_input, deps);

    logger?.info?.("outbound_direct_send.completed", {
      ...request_meta,
      ok: result?.queued?.ok === true && result?.processed?.ok === true,
      queue_item_id: result?.queued?.queue_item_id || null,
      processed_ok: result?.processed?.ok !== false,
      sent: result?.processed?.sent || false,
    });

    return {
      status: statusForResult(result),
      payload: {
        ok: result?.queued?.ok === true && result?.processed?.ok === true,
        route: "internal/outbound/direct-send",
        result,
      },
    };
  } catch (error) {
    const diagnostics = serializeRouteError(error);

    logger?.error?.("outbound_direct_send.failed", {
      ...request_meta,
      error: diagnostics,
    });

    return {
      status: isDirectSendInputError(diagnostics.message) ? 400 : 500,
      payload: {
        ok: false,
        error: "outbound_direct_send_failed",
        message: diagnostics.message,
      },
    };
  }
}

export default handleDirectSendRequestData;
