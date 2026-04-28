// ─── validate-send-queue-item.js ─────────────────────────────────────────
import {
  getCategoryValue,
  getFirstAppReferenceId,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function isTerminalStatus(status) {
  return ["sent", "failed", "cancelled", "blocked"].includes(lower(status));
}

function hasBlankSellerGreeting(text = "") {
  const normalized = clean(text).replace(/\s+/g, " ");
  return /^(hi|hey|hello|hola)\s+,/i.test(normalized);
}

export function validateSendQueueItem(queue_item = null) {
  const queue_item_id = queue_item?.item_id || queue_item?.id || queue_item?.queue_row_id;
  if (!queue_item_id) {
    return {
      ok: false,
      reason: "missing_queue_item",
    };
  }

  const queue_status = getCategoryValue(queue_item, "queue-status", null) || clean(queue_item.queue_status);
  const phone_item_id =
    getFirstAppReferenceId(queue_item, "phone-number", null) ||
    queue_item.phone_item_id ||
    queue_item.phone_id ||
    null;
  const textgrid_number_item_id =
    getFirstAppReferenceId(queue_item, "textgrid-number", null) ||
    queue_item.textgrid_number_item_id ||
    queue_item.textgrid_number_id ||
    null;
  const message_text =
    getTextValue(queue_item, "message-text", "") ||
    clean(queue_item.message_text || queue_item.message_body || queue_item.rendered_message_text || "");
  const retry_count = Number(getNumberValue(queue_item, "retry-count", 0) || 0);
  const max_retries = Number(getNumberValue(queue_item, "max-retries", 3) || 3);
  const touch_number = Number(getNumberValue(queue_item, "touch-number", 0) || 0);
  const use_case_template = getCategoryValue(queue_item, "use-case-template", null);

  if (queue_status && isTerminalStatus(queue_status)) {
    return {
      ok: false,
      reason: `terminal_status:${queue_status}`,
      queue_status,
      skipped: true,
    };
  }

  if (!phone_item_id) {
    return {
      ok: false,
      reason: "missing_phone_item",
      queue_status,
    };
  }

  if (!textgrid_number_item_id) {
    return {
      ok: false,
      reason: "missing_textgrid_number",
      queue_status,
    };
  }

  if (!clean(message_text)) {
    return {
      ok: false,
      reason: "empty_message_body",
      queue_status,
    };
  }

  // Reject one-word / too-short bodies.  These are truncation artifacts caused by
  // multiline template text being stored in a single-line Podio field — Podio keeps
  // only the first line, collapsing "Hi\nDear {owner}…" to "Hi".  A legitimate
  // outbound SMS must contain at least 3 whitespace-separated words.  Anything
  // shorter is never intentional outreach and would be caught by carrier content
  // filters anyway.
  const normalized_body = clean(message_text);
  if (hasBlankSellerGreeting(normalized_body)) {
    return {
      ok: false,
      reason: "blank_greeting_message_body",
      queue_status,
      message_body: normalized_body,
    };
  }

  const word_count = normalized_body.split(/\s+/).filter(Boolean).length;
  if (word_count < 3) {
    return {
      ok: false,
      reason: "junk_message_body",
      queue_status,
      word_count,
      message_body: normalized_body,
    };
  }

  if (retry_count >= max_retries) {
    return {
      ok: false,
      reason: "max_retries_exceeded",
      queue_status,
      retry_count,
      max_retries,
    };
  }

  // FIX 10: Touch 1 send-time validation — the queue runner must never deliver
  // a Touch 1 message that was somehow written with a wrong use case.  This is
  // a safety net for rows that were created before the pipeline lock was active.
  if (touch_number === 1) {
    const normalized_use_case = lower(use_case_template);
    if (normalized_use_case && normalized_use_case !== "ownership_check") {
      return {
        ok: false,
        reason: "invalid_touch_one_use_case",
        queue_status,
        use_case_template,
        touch_number,
      };
    }
  }

  // ── Template attachment quarantine ──────────────────────────────────────────
  // Every live queue row must be backed by a real Podio template with a valid
  // template relation.  Rows created from local_registry fallback templates
  // have no template relation (the field is empty) and must not be sent.
  // This catches rows that slipped through before the require_podio_template
  // gate was added to the feeder.
  const template_relation_id = getFirstAppReferenceId(queue_item, "template-2", null);
  if (!template_relation_id) {
    return {
      ok: false,
      reason: "unattached_template",
      queue_status,
      touch_number,
      use_case_template,
      note: "queue row has no template relation — likely created from local_registry fallback",
    };
  }

  return {
    ok: true,
    queue_status,
    phone_item_id,
    textgrid_number_item_id,
    message_text: clean(message_text),
    retry_count,
    max_retries,
  };
}

export default validateSendQueueItem;
