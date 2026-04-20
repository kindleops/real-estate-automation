function clean(value) {
  return String(value ?? "").trim();
}

function firstNonEmpty(candidates) {
  for (const [key, val] of candidates) {
    const s = String(val ?? "").trim();
    if (s) return { value: s, source: key };
  }
  return { value: "", source: null };
}

export function normalizeTextgridInboundPayload(body = {}, headers = new Headers()) {
  const http_received_at = new Date().toISOString();
  const raw_body_keys = Object.keys(body || {});

  const message_result = firstNonEmpty([
    ["Body",         body?.Body],
    ["body",         body?.body],
    ["MessageBody",  body?.MessageBody],
    ["message_body", body?.message_body],
    ["Message",      body?.Message],
    ["message",      body?.message],
    ["Text",         body?.Text],
    ["text",         body?.text],
    ["content",      body?.content],
    ["payload.Body", body?.payload?.Body],
    ["payload.body", body?.payload?.body],
    ["data.Body",    body?.data?.Body],
    ["data.body",    body?.data?.body],
  ]);

  return {
    provider: "textgrid",
    raw: body,
    raw_body_keys,

    message_id: clean(
      body?.MessageSid ||
      body?.SmsMessageSid ||
      body?.SmsSid ||
      body?.sid ||
      body?.message_sid ||
      body?.provider_message_sid ||
      body?.message_id ||
      body?.messageId ||
      body?.id ||
      body?.sms_id
    ),

    from: clean(
      body?.From ||
      body?.from ||
      body?.from_phone_number ||
      body?.from_number ||
      body?.fromNumber ||
      body?.sender ||
      body?.phone ||
      body?.payload?.From ||
      body?.payload?.from ||
      body?.data?.From ||
      body?.data?.from
    ),

    to: clean(
      body?.To ||
      body?.to ||
      body?.to_phone_number ||
      body?.to_number ||
      body?.toNumber ||
      body?.recipient ||
      body?.payload?.To ||
      body?.payload?.to ||
      body?.data?.To ||
      body?.data?.to
    ),

    message: message_result.value || null,
    message_body: message_result.value || null,
    body_source: message_result.source,

    direction: clean(body?.direction || "inbound"),
    received_at: clean(
      body?.received_at ||
      body?.timestamp ||
      body?.created_at ||
      body?.http_received_at ||
      http_received_at
    ),
    conversation_id: clean(body?.conversation_id || body?.conversationId),
    account_id: clean(body?.account_id || body?.accountId),
    status: clean(
      body?.status ||
      body?.SmsStatus ||
      "received"
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
      "inbound"
    ),
    http_received_at,
  };
}

export default normalizeTextgridInboundPayload;
