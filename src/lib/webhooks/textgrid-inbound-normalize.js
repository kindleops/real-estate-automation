function clean(value) {
  return String(value ?? "").trim();
}

export function normalizeTextgridInboundPayload(body = {}, headers = new Headers()) {
  const http_received_at = new Date().toISOString();

  return {
    provider: "textgrid",
    raw: body,

    message_id: clean(
      body?.message_id ||
      body?.messageId ||
      body?.id ||
      body?.sms_id ||
      body?.SmsMessageSid ||
      body?.SmsSid ||
      body?.MessageSid
    ),

    from: clean(
      body?.from ||
      body?.from_number ||
      body?.fromNumber ||
      body?.sender ||
      body?.phone ||
      body?.From
    ),

    to: clean(
      body?.to ||
      body?.to_number ||
      body?.toNumber ||
      body?.recipient ||
      body?.To
    ),

    message: clean(
      body?.message ||
      body?.body ||
      body?.text ||
      body?.content ||
      body?.Body
    ),

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
