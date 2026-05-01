function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function metadataValue(row = null, key = "") {
  const metadata =
    row && typeof row.metadata === "object" && !Array.isArray(row.metadata)
      ? row.metadata
      : {};
  return metadata[key];
}

export function isManualInboxSend(queue_item = null) {
  const queue_key = clean(
    queue_item?.queue_key ||
      queue_item?.queue_id ||
      metadataValue(queue_item, "queue_key") ||
      metadataValue(queue_item, "queue_id")
  );
  const message_type = lower(
    queue_item?.message_type || metadataValue(queue_item, "message_type")
  );
  const use_case_template = lower(
    queue_item?.use_case_template ||
      metadataValue(queue_item, "use_case_template") ||
      metadataValue(queue_item, "selected_use_case")
  );
  const action = lower(metadataValue(queue_item, "action"));
  const source = lower(metadataValue(queue_item, "source"));
  const created_from = lower(metadataValue(queue_item, "created_from"));

  return (
    queue_key.startsWith("inbox:send_now:") ||
    message_type === "manual_reply" ||
    use_case_template === "inbox_manual_send_now" ||
    action === "send_now" ||
    source === "inbox" ||
    created_from === "leadcommand_inbox"
  );
}

export function isUnknownAutoReply(queue_item = null) {
  const message_type = lower(
    queue_item?.message_type || metadataValue(queue_item, "message_type")
  );
  const use_case_template = lower(
    queue_item?.use_case_template ||
      metadataValue(queue_item, "use_case_template")
  );
  const source = lower(metadataValue(queue_item, "source"));
  const unknown_inbound = metadataValue(queue_item, "unknown_inbound") === true;

  return (
    use_case_template === "unknown_inbound_auto_reply" ||
    message_type === "unknown inbound auto reply" ||
    source === "textgrid_inbound_unknown_router" ||
    unknown_inbound === true
  );
}

export default isManualInboxSend;
