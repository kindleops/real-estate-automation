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

  return (
    queue_key.startsWith("inbox:send_now:") ||
    message_type === "manual_reply" ||
    use_case_template === "inbox_manual_send_now"
  );
}

export default isManualInboxSend;
