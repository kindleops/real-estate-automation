import crypto from "node:crypto";

import {
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";
import {
  parseMessageEventMetadata,
  serializeMessageEventMetadata,
} from "@/lib/domain/events/message-event-metadata.js";
import { toPodioDateField, toPodioDateTimeString } from "@/lib/utils/dates.js";

export const SELLER_MESSAGE_EVENT_FIELDS = {
  message_event_key: "message-id",
  provider_message_sid: "text-2",
  timestamp: "timestamp",
  direction: "direction",
  event_type: "category",
  message_variant: "message-variant",
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
  sms_agent: "sms-agent",
  conversation: "conversation",
  market: "market",
  ai_route: "ai-route",
  processed_by: "processed-by",
  source_app: "source-app",
  trigger_name: "trigger-name",
  message: "message",
  template: "template",
  property_address: "property-address",
  character_count: "character-count",
  segment_count: "number-2",
  delivery_status: "status-3",
  raw_carrier_status: "status-2",
  provider_delivery_status: "delivery-status",
  latency_ms: "latency-ms",
  failure_bucket: "failure-bucket",
  is_final_failure: "is-final-failure",
  ai_output: "ai-output",
  is_opt_out: "is-opt-out",
  opt_out_keyword: "opt-out-keyword",
  opt_out_message: "text-5",
  prior_message_id: "prior-message-id",
  response_to_message_id: "response-to-message-id",
  stage_before: "stage-before",
  stage_after: "stage-after",
};

const OPT_OUT_KEYWORDS = [
  "stop",
  "stopall",
  "end",
  "cancel",
  "quit",
  "unsubscribe",
  "remove me",
  "opt out",
  "opt-out",
  "do not contact",
  "do not text",
  "stop texting",
];

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function asArrayAppRef(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? [parsed] : undefined;
}

function segmentCountForMessage(message_body = "") {
  const length = String(message_body || "").length;
  if (!length) return 0;
  return Math.max(1, Math.ceil(length / 160));
}

function normalizeProviderDeliveryStatus(value = null) {
  const raw = lower(value);

  if (!raw) return undefined;
  if (["queued", "pending", "accepted"].includes(raw)) return "Queued";
  if (["sending"].includes(raw)) return "Sending";
  if (["sent"].includes(raw)) return "Sent";
  if (["delivered", "delivery_confirmed", "confirmed"].includes(raw)) return "Delivered";
  if (["failed", "error", "delivery_failed"].includes(raw)) return "Failed";
  if (["undelivered"].includes(raw)) return "Undelivered";
  return "Unknown";
}

export function normalizeSellerDeliveryStatus(value = null, fallback = "Sent") {
  const raw = lower(value);

  if (["queued", "pending", "accepted"].includes(raw)) return "Pending";
  if (["sending", "sent"].includes(raw)) return "Sent";
  if (["delivered", "delivery_confirmed", "confirmed"].includes(raw)) return "Delivered";
  if (["failed", "undelivered", "delivery_failed", "error"].includes(raw)) return "Failed";
  if (["received"].includes(raw)) return "Received";

  return fallback;
}

function buildFallbackKeySeed(payload = {}) {
  const seed = JSON.stringify({
    direction: clean(payload.direction) || null,
    event_type: clean(payload.event_type) || null,
    queue_item_id: clean(payload.queue_item_id) || null,
    client_reference_id: clean(payload.client_reference_id) || null,
    provider_message_id: clean(payload.provider_message_id) || null,
    body: clean(payload.message_body).slice(0, 120) || null,
    timestamp: clean(payload.timestamp) || null,
  });

  return crypto.createHash("sha1").update(seed, "utf8").digest("hex").slice(0, 16);
}

export function buildOutboundMessageEventKey({
  queue_item_id = null,
  client_reference_id = null,
  provider_message_id = null,
} = {}) {
  const suffix =
    clean(client_reference_id) ||
    (queue_item_id ? `queue-${clean(queue_item_id)}` : "") ||
    clean(provider_message_id);

  return `outbound:${suffix || buildFallbackKeySeed({
    direction: "Outbound",
    event_type: "Seller Outbound SMS",
    queue_item_id,
    client_reference_id,
    provider_message_id,
  })}`;
}

export function buildFailedMessageEventKey({
  queue_item_id = null,
  client_reference_id = null,
  provider_message_id = null,
} = {}) {
  const suffix =
    clean(client_reference_id) ||
    (queue_item_id ? `queue-${clean(queue_item_id)}` : "") ||
    clean(provider_message_id);

  return `failure:${suffix || buildFallbackKeySeed({
    direction: "Outbound",
    event_type: "Send Failure",
    queue_item_id,
    client_reference_id,
    provider_message_id,
  })}`;
}

export function buildInboundMessageEventKey({
  provider_message_id = null,
  from = null,
  to = null,
  message_body = "",
  received_at = null,
} = {}) {
  const suffix =
    clean(provider_message_id) ||
    buildFallbackKeySeed({
      direction: "Inbound",
      event_type: "Seller Inbound SMS",
      provider_message_id,
      message_body,
      timestamp: received_at,
      client_reference_id: `${clean(from)}:${clean(to)}`,
    });

  return `inbound:${suffix}`;
}

export function buildRelationshipFields({
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  market_id = null,
  phone_item_id = null,
  textgrid_number_item_id = null,
  sms_agent_id = null,
  conversation_item_id = null,
  template_id = null,
} = {}) {
  return {
    ...(asArrayAppRef(master_owner_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.master_owner]: asArrayAppRef(master_owner_id) }
      : {}),
    ...(asArrayAppRef(prospect_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.prospect]: asArrayAppRef(prospect_id) }
      : {}),
    ...(asArrayAppRef(property_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.property]: asArrayAppRef(property_id) }
      : {}),
    ...(asArrayAppRef(market_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.market]: asArrayAppRef(market_id) }
      : {}),
    ...(asArrayAppRef(phone_item_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.phone_number]: asArrayAppRef(phone_item_id) }
      : {}),
    ...(asArrayAppRef(textgrid_number_item_id)
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.textgrid_number]:
            asArrayAppRef(textgrid_number_item_id),
        }
      : {}),
    ...(asArrayAppRef(sms_agent_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.sms_agent]: asArrayAppRef(sms_agent_id) }
      : {}),
    ...(asArrayAppRef(conversation_item_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.conversation]: asArrayAppRef(conversation_item_id) }
      : {}),
    ...(asArrayAppRef(template_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.template]: asArrayAppRef(template_id) }
      : {}),
  };
}

export function extractOptOutDetails(message_body = "") {
  const normalized = lower(message_body);
  if (!normalized) return {};

  const matched_keyword = OPT_OUT_KEYWORDS.find((keyword) => normalized.includes(keyword));
  if (!matched_keyword) return {};

  return {
    [SELLER_MESSAGE_EVENT_FIELDS.is_opt_out]: "Yes",
    [SELLER_MESSAGE_EVENT_FIELDS.opt_out_keyword]: matched_keyword.toUpperCase(),
    [SELLER_MESSAGE_EVENT_FIELDS.opt_out_message]: String(message_body || ""),
  };
}

export function buildBaseSellerMessageEventFields({
  message_event_key,
  provider_message_id = null,
  timestamp = null,
  direction,
  event_type,
  message_body = "",
  delivery_status = null,
  provider_delivery_status = null,
  raw_carrier_status = null,
  message_variant = null,
  latency_ms = null,
  property_address = null,
  ai_route = null,
  processed_by = null,
  source_app = null,
  trigger_name = null,
  failure_bucket = null,
  is_final_failure = null,
  prior_message_id = null,
  response_to_message_id = null,
  stage_before = null,
  stage_after = null,
  opt_out = false,
  opt_out_keyword = null,
  metadata = {},
  relationship_ids = {},
} = {}) {
  const normalized_message_body = String(message_body || "");
  const fields = {
    [SELLER_MESSAGE_EVENT_FIELDS.message_event_key]:
      clean(message_event_key) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.provider_message_sid]:
      clean(provider_message_id) || null,
    [SELLER_MESSAGE_EVENT_FIELDS.timestamp]:
      toPodioDateField(timestamp || new Date()) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.direction]: clean(direction) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.event_type]: clean(event_type) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.message]: normalized_message_body,
    [SELLER_MESSAGE_EVENT_FIELDS.character_count]: normalized_message_body.length,
    [SELLER_MESSAGE_EVENT_FIELDS.segment_count]:
      segmentCountForMessage(normalized_message_body),
    [SELLER_MESSAGE_EVENT_FIELDS.processed_by]: clean(processed_by) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.source_app]: clean(source_app) || undefined,
    [SELLER_MESSAGE_EVENT_FIELDS.trigger_name]: clean(trigger_name) || undefined,
    ...(delivery_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.delivery_status]:
            normalizeSellerDeliveryStatus(delivery_status),
        }
      : {}),
    ...(provider_delivery_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.provider_delivery_status]:
            normalizeProviderDeliveryStatus(provider_delivery_status),
        }
      : {}),
    ...(raw_carrier_status
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.raw_carrier_status]:
            clean(raw_carrier_status),
        }
      : {}),
    ...(message_variant !== null && message_variant !== undefined
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.message_variant]:
            Number(message_variant) || 0,
        }
      : {}),
    ...(latency_ms !== null && latency_ms !== undefined
      ? { [SELLER_MESSAGE_EVENT_FIELDS.latency_ms]: Number(latency_ms) || 0 }
      : {}),
    ...(clean(property_address)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.property_address]: clean(property_address) }
      : {}),
    ...(clean(ai_route)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.ai_route]: clean(ai_route) }
      : {}),
    ...(clean(failure_bucket)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.failure_bucket]: clean(failure_bucket) }
      : {}),
    ...(is_final_failure !== null && is_final_failure !== undefined
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.is_final_failure]:
            is_final_failure ? "Yes" : "No",
        }
      : {}),
    ...(clean(prior_message_id)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.prior_message_id]: clean(prior_message_id) }
      : {}),
    ...(clean(response_to_message_id)
      ? {
          [SELLER_MESSAGE_EVENT_FIELDS.response_to_message_id]:
            clean(response_to_message_id),
        }
      : {}),
    ...(clean(stage_before)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.stage_before]: clean(stage_before) }
      : {}),
    ...(clean(stage_after)
      ? { [SELLER_MESSAGE_EVENT_FIELDS.stage_after]: clean(stage_after) }
      : {}),
    ...buildRelationshipFields(relationship_ids),
  };

  if (opt_out) {
    fields[SELLER_MESSAGE_EVENT_FIELDS.is_opt_out] = "Yes";
    if (clean(opt_out_keyword)) {
      fields[SELLER_MESSAGE_EVENT_FIELDS.opt_out_keyword] = clean(opt_out_keyword);
    }
    if (normalized_message_body) {
      fields[SELLER_MESSAGE_EVENT_FIELDS.opt_out_message] = normalized_message_body;
    }
  }

  const normalized_metadata =
    metadata && typeof metadata === "object" ? metadata : {};
  if (Object.keys(normalized_metadata).length) {
    fields[SELLER_MESSAGE_EVENT_FIELDS.ai_output] =
      serializeMessageEventMetadata(normalized_metadata);
  }

  return Object.fromEntries(
    Object.entries(fields).filter(([, value]) => value !== undefined)
  );
}

export function mergeMessageEventMetadata(event_item = null, patch = {}) {
  const existing = parseMessageEventMetadata(event_item);
  return {
    ...existing,
    ...(patch && typeof patch === "object" ? patch : {}),
  };
}

export function getProviderMessageIdFromEvent(event_item = null) {
  return (
    clean(
      getTextValue(
        event_item,
        SELLER_MESSAGE_EVENT_FIELDS.provider_message_sid,
        ""
      )
    ) ||
    clean(parseMessageEventMetadata(event_item)?.provider_message_id) ||
    clean(
      getTextValue(
        event_item,
        SELLER_MESSAGE_EVENT_FIELDS.message_event_key,
        ""
      )
    ) ||
    null
  );
}

export function getMessageEventKey(event_item = null) {
  return clean(
    getTextValue(event_item, SELLER_MESSAGE_EVENT_FIELDS.message_event_key, "")
  ) || null;
}

export function getMessageEventConversationId(event_item = null) {
  return getFirstAppReferenceId(
    event_item,
    SELLER_MESSAGE_EVENT_FIELDS.conversation,
    null
  );
}

export function getMessageEventTimestamp(event_item = null) {
  return getDateValue(event_item, SELLER_MESSAGE_EVENT_FIELDS.timestamp, null);
}

export function getMessageEventAiRoute(event_item = null) {
  return getCategoryValue(event_item, SELLER_MESSAGE_EVENT_FIELDS.ai_route, null);
}

export function getMessageEventPropertyAddress(event_item = null) {
  return clean(
    getTextValue(event_item, SELLER_MESSAGE_EVENT_FIELDS.property_address, "")
  ) || null;
}

export function toStoredTimestamp(value = null) {
  return toPodioDateTimeString(value || new Date());
}

export default {
  SELLER_MESSAGE_EVENT_FIELDS,
  normalizeSellerDeliveryStatus,
  buildOutboundMessageEventKey,
  buildFailedMessageEventKey,
  buildInboundMessageEventKey,
  buildRelationshipFields,
  extractOptOutDetails,
  buildBaseSellerMessageEventFields,
  mergeMessageEventMetadata,
  getProviderMessageIdFromEvent,
  getMessageEventKey,
  getMessageEventConversationId,
  getMessageEventTimestamp,
  getMessageEventAiRoute,
  getMessageEventPropertyAddress,
  toStoredTimestamp,
};
