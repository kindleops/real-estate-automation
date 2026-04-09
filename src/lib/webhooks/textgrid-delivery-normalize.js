function clean(value) {
  return String(value ?? "").trim();
}

function buildLookup(body = {}) {
  const lookup = new Map();

  for (const [key, value] of Object.entries(body || {})) {
    const normalized = clean(key).toLowerCase();
    if (!normalized || lookup.has(normalized)) continue;
    lookup.set(normalized, value);
  }

  return lookup;
}

function pickValue(body = {}, lookup = null, keys = []) {
  const resolved_lookup = lookup || buildLookup(body);

  for (const key of keys) {
    const exact = body?.[key];
    if (exact !== undefined && exact !== null && clean(exact)) {
      return exact;
    }

    const normalized = clean(key).toLowerCase();
    if (normalized && resolved_lookup.has(normalized)) {
      const value = resolved_lookup.get(normalized);
      if (value !== undefined && value !== null && clean(value)) {
        return value;
      }
    }
  }

  return "";
}

export function normalizeTextgridDeliveryPayload(
  body = {},
  headers = new Headers()
) {
  const http_received_at = new Date().toISOString();
  const lookup = buildLookup(body);

  return {
    provider: "textgrid",
    raw: body,

    message_id: clean(
      pickValue(body, lookup, [
        "message_id",
        "message_sid",
        "messageId",
        "id",
        "sms_id",
        "sms_sid",
        "sms_message_sid",
        "smsid",
        "smsmessagesid",
        "SmsMessageSid",
        "SmsSid",
        "MessageSid",
      ])
    ),

    status: clean(
      pickValue(body, lookup, [
        "status",
        "message_status",
        "sms_status",
        "messageStatus",
        "delivery_status",
        "smsstatus",
        "SmsStatus",
        "MessageStatus",
      ])
    ),

    error_code: clean(
      pickValue(body, lookup, [
        "error_code",
        "errorCode",
        "code",
        "ErrorCode",
      ])
    ),

    error_message: clean(
      pickValue(body, lookup, [
        "error_message",
        "errorMessage",
        "reason",
        "ErrorMessage",
      ])
    ),

    delivered_at: clean(
      pickValue(body, lookup, [
        "delivered_at",
        "timestamp",
        "updated_at",
        "DateUpdated",
        "date_updated",
        "MessageDateUpdated",
      ])
    ),

    client_reference_id: clean(
      pickValue(body, lookup, [
        "client_reference_id",
        "clientReferenceId",
        "external_id",
        "externalId",
      ])
    ),

    from: clean(
      pickValue(body, lookup, [
        "from",
        "from_number",
        "fromNumber",
        "sender",
        "From",
      ])
    ),

    to: clean(
      pickValue(body, lookup, [
        "to",
        "to_number",
        "toNumber",
        "recipient",
        "To",
      ])
    ),

    account_id: clean(
      pickValue(body, lookup, [
        "account_id",
        "account_sid",
        "accountId",
        "AccountSid",
      ])
    ),
    conversation_id: clean(
      pickValue(body, lookup, ["conversation_id", "conversationId"])
    ),
    api_version: clean(
      pickValue(body, lookup, ["api_version", "apiVersion", "ApiVersion"])
    ),
    segments: clean(
      pickValue(body, lookup, ["segments", "num_segments", "NumSegments"])
    ),

    header_signature: clean(
      headers.get("x-textgrid-signature") ||
        headers.get("x-twilio-signature") ||
        headers.get("x-signature") ||
        ""
    ),
    header_signature_name: (
      headers.get("x-textgrid-signature") ? "x-textgrid-signature" :
      headers.get("x-twilio-signature")   ? "x-twilio-signature"   :
      headers.get("x-signature")          ? "x-signature"          :
      null
    ),
    header_event: clean(
      headers.get("x-textgrid-event") ||
        headers.get("x-event-type") ||
        "delivery"
    ),
    http_received_at,
  };
}

export default normalizeTextgridDeliveryPayload;
